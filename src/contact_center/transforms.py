"""Transformaciones Beam para la analítica del contact center.

Dos agregaciones incrementales por clave producen un único changelog:

- `queue_metrics`      métricas operativas por cola y ventana (eventos de contacto)
- `agent_state_counts` transiciones por estado destino y ventana (eventos de agente)
"""

from __future__ import annotations

import json
import math
from datetime import UTC
from typing import Any

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms import trigger, window
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.windowed_value import PaneInfoTiming

from contact_center.config import Settings
from contact_center.contracts import decode_event, parse_iso_utc

CONTACT_EVENT_TYPES = ("contact_queued", "contact_connected", "contact_ended")
AGENT_EVENT_TYPE = "agent_state_changed"

METRIC_QUEUE = "queue_metrics"
METRIC_AGENT_STATE = "agent_state_counts"


def _percentile(values: list[float], fraction: float) -> float:
    """Percentil por rango más cercano, sin interpolación."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[min(index, len(ordered) - 1)]


def iso_utc_from_timestamp(timestamp: Any) -> str:
    """Serializar un `Timestamp` de Beam como ISO-8601 UTC terminado en Z.

    `Timestamp.to_utc_datetime()` devuelve un datetime **naive**: sin reponerle
    la zona, `isoformat()` produce `2026-07-24T13:00:00`, que no identifica un
    instante. Es el mismo detalle que rompe el contrato de salida en el
    laboratorio de la clase 7, donde el `.replace("+00:00", "Z")` nunca llega a
    aplicarse porque no hay sufijo que reemplazar.
    """
    moment = timestamp.to_utc_datetime()
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.isoformat().replace("+00:00", "Z")


class ParseEvent(beam.DoFn):
    """Decodificar el JSON y desviar los registros que violan el contrato."""

    INVALID = "invalid"

    def process(self, element: tuple[bytes | None, bytes | None]):
        raw_key, raw_value = element
        try:
            event = decode_event(raw_value or b"")
        except ValueError as error:
            yield pvalue.TaggedOutput(
                self.INVALID,
                {
                    "error": str(error),
                    "kafka_key": raw_key.decode(errors="replace") if raw_key else None,
                    "payload": (raw_value or b"").decode(errors="replace"),
                },
            )
            return
        event["kafka_key"] = raw_key.decode(errors="replace") if raw_key else None
        yield event


def assign_event_timestamp(event: dict[str, Any]) -> TimestampedValue:
    """Asignar el tiempo del dominio, no el de llegada a Kafka."""
    return TimestampedValue(event, parse_iso_utc(event["event_time"]).timestamp())


def windowed(events, settings: Settings, *, streaming_triggers: bool = True):
    """Ventana fija en tiempo de evento, con acumulación y lateness."""
    kwargs: dict[str, Any] = {
        "windowfn": window.FixedWindows(settings.window_seconds),
        "allowed_lateness": settings.allowed_lateness_seconds,
        "accumulation_mode": trigger.AccumulationMode.ACCUMULATING,
    }
    if streaming_triggers:
        # El disparador temprano observa el reloj de *processing time*. El
        # `DirectRunner` no lo provee, así que se activa solo cuando
        # `early_firing_seconds` es positivo; con un runner portable (Flink) el
        # reloj existe y las emisiones tempranas reducen la latencia.
        kwargs["trigger"] = trigger.AfterWatermark(
            early=trigger.AfterProcessingTime(settings.early_firing_seconds)
            if settings.early_firing_seconds > 0
            else None,
            late=trigger.AfterCount(1),
        )
    return events | "Ventana fija por evento" >> beam.WindowInto(**kwargs)


class DeduplicateByEventId(beam.DoFn):
    """Emitir cada `event_id` una sola vez dentro de su clave y ventana.

    El estado está aislado por clave y por ventana, y un timer de tiempo de
    evento lo libera al cerrar la ventana más la lateness permitida. Sin esa
    expiración el conjunto de identificadores crecería sin límite: el runner
    guardaría en memoria y en los checkpoints identificadores de eventos que ya
    no pueden volver a aparecer.
    """

    SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
    EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

    def process(
        self,
        element: tuple[str, dict[str, Any]],
        seen_ids=beam.DoFn.StateParam(SEEN_IDS),
        window_param=beam.DoFn.WindowParam,
        expiry=beam.DoFn.TimerParam(EXPIRY),
    ):
        key, event = element
        event_id = event["event_id"]
        if event_id in set(seen_ids.read() or ()):
            return
        seen_ids.add(event_id)
        expiry.set(window_param.end)
        yield key, event

    @on_timer(EXPIRY)
    def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
        """Liberar el estado cuando el watermark supera el cierre de la ventana."""
        seen_ids.clear()


class QueueMetricsCombineFn(beam.CombineFn):
    """Métricas por cola calculadas de forma incremental.

    El acumulador es una tupla inmutable, así que `merge_accumulators` puede
    combinar los resúmenes parciales de cada worker antes del shuffle. La única
    parte que no se resume es la lista de esperas, que se conserva para poder
    calcular el p90: es un costo acotado por el tamaño de la ventana.
    """

    def __init__(self, service_level_seconds: int = 20) -> None:
        self.service_level_seconds = service_level_seconds

    def create_accumulator(self):
        return (0, 0, 0, 0, 0, 0, 0.0, ())

    def add_input(self, accumulator, event: dict[str, Any]):
        offered, connected, abandoned, transferred, resolved, within_sl, talk, waits = (
            accumulator
        )
        payload = event["payload"]
        event_type = event["event_type"]

        if event_type == "contact_queued":
            offered += 1
        elif event_type == "contact_connected":
            connected += 1
            wait = float(payload.get("wait_seconds", 0.0))
            if wait <= self.service_level_seconds:
                within_sl += 1
        elif event_type == "contact_ended":
            wait = float(payload.get("wait_seconds", 0.0))
            waits = waits + (wait,)
            disposition = payload.get("disposition")
            if disposition == "abandoned":
                abandoned += 1
            else:
                talk += float(payload.get("talk_seconds", 0.0))
                if disposition == "transferred":
                    transferred += 1
                else:
                    resolved += 1

        return (offered, connected, abandoned, transferred, resolved, within_sl, talk, waits)

    def merge_accumulators(self, accumulators):
        offered = connected = abandoned = transferred = resolved = within_sl = 0
        talk = 0.0
        waits: tuple[float, ...] = ()
        for accumulator in accumulators:
            offered += accumulator[0]
            connected += accumulator[1]
            abandoned += accumulator[2]
            transferred += accumulator[3]
            resolved += accumulator[4]
            within_sl += accumulator[5]
            talk += accumulator[6]
            waits += accumulator[7]
        return (offered, connected, abandoned, transferred, resolved, within_sl, talk, waits)

    def extract_output(self, accumulator):
        offered, connected, abandoned, transferred, resolved, within_sl, talk, waits = (
            accumulator
        )
        finished = connected + abandoned
        wait_list = list(waits)
        return {
            "offered": offered,
            "connected": connected,
            "abandoned": abandoned,
            "transferred": transferred,
            "resolved": resolved,
            "answered_within_sl": within_sl,
            # Cociente sobre la misma población: los atendidos en la ventana.
            "ns20": round(within_sl / connected, 4) if connected else 0.0,
            # Cociente sobre la misma población: los resueltos en la ventana.
            "abandon_rate": round(abandoned / finished, 4) if finished else 0.0,
            "avg_wait_seconds": round(sum(wait_list) / len(wait_list), 3)
            if wait_list
            else 0.0,
            "p90_wait_seconds": round(_percentile(wait_list, 0.90), 3),
            "max_wait_seconds": round(max(wait_list), 3) if wait_list else 0.0,
            "total_talk_seconds": round(talk, 3),
        }


class StateCountCombineFn(beam.CombineFn):
    """Cuenta transiciones por estado destino; un contador es suficiente."""

    def create_accumulator(self) -> int:
        return 0

    def add_input(self, accumulator: int, _event) -> int:
        return accumulator + 1

    def merge_accumulators(self, accumulators) -> int:
        return sum(accumulators)

    def extract_output(self, accumulator: int) -> dict[str, int]:
        return {"transitions": accumulator}


class FormatAggregate(beam.DoFn):
    """Adjuntar metadatos de ventana y pane, y construir la clave idempotente."""

    def __init__(self, metric_type: str) -> None:
        self.metric_type = metric_type

    def process(
        self,
        element,
        window_param=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        dimension_id, metrics = element
        start = iso_utc_from_timestamp(window_param.start)
        end = iso_utc_from_timestamp(window_param.end)
        yield {
            "schema_version": 1,
            # La clave lógica del agregado: métrica, dimensión y ventana.
            "aggregate_id": f"{self.metric_type}|{dimension_id}|{start}",
            "metric_type": self.metric_type,
            "dimension_id": str(dimension_id),
            "window_start": start,
            "window_end": end,
            "pane_index": pane_info.index,
            # `timing` es un entero, no un enum con nombre: `str()` publica "1"
            # en lugar de "ON_TIME". El laboratorio de la clase 7 usa
            # `str(pane_info.timing)` y por eso emite el número.
            "pane_timing": PaneInfoTiming.to_string(pane_info.timing),
            "is_first": pane_info.is_first,
            "is_last": pane_info.is_last,
            **metrics,
        }


def build_analytics(events, settings: Settings, *, streaming_triggers: bool = True):
    """Construir las dos agregaciones y unificarlas en un changelog."""
    timestamped = events | "Asignar tiempo de evento" >> beam.Map(assign_event_timestamp)
    windowed_events = windowed(timestamped, settings, streaming_triggers=streaming_triggers)

    contacts = windowed_events | "Solo eventos de contacto" >> beam.Filter(
        lambda event: event["event_type"] in CONTACT_EVENT_TYPES
    )
    queue_metrics = (
        contacts
        | "Clave por cola"
        >> beam.Map(lambda event: (event["payload"]["queue_id"], event))
        | "Deduplicar contactos" >> beam.ParDo(DeduplicateByEventId())
        | "Combinar métricas por cola"
        >> beam.CombinePerKey(
            QueueMetricsCombineFn(settings.service_level_seconds)
        )
        | "Formatear métricas de cola" >> beam.ParDo(FormatAggregate(METRIC_QUEUE))
    )

    agent_events = windowed_events | "Solo eventos de agente" >> beam.Filter(
        lambda event: event["event_type"] == AGENT_EVENT_TYPE
    )
    state_counts = (
        agent_events
        | "Clave por estado destino"
        >> beam.Map(lambda event: (event["payload"]["to_state"], event))
        | "Deduplicar transiciones" >> beam.ParDo(DeduplicateByEventId())
        | "Contar transiciones" >> beam.CombinePerKey(StateCountCombineFn())
        | "Formatear conteo de estados" >> beam.ParDo(FormatAggregate(METRIC_AGENT_STATE))
    )

    return (queue_metrics, state_counts) | "Unificar changelog" >> beam.Flatten()


def aggregate_to_kafka_record(aggregate: dict[str, Any]) -> tuple[bytes, bytes]:
    """Clave de partición = dimensión; el valor lleva el agregado completo."""
    return (
        str(aggregate["dimension_id"]).encode(),
        json.dumps(aggregate, sort_keys=True, separators=(",", ":")).encode(),
    )


def invalid_to_kafka_record(record: dict[str, Any]) -> tuple[bytes, bytes]:
    key = record.get("kafka_key") or "sin-clave"
    return key.encode(), json.dumps(record, sort_keys=True).encode()
