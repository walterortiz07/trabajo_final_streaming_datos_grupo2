"""Pruebas de tiempo de evento y ventanas con TestStream.

`TestStream` permite controlar eventos y watermark de forma determinista. Estas
pruebas verifican lo que el arnés resuelve de manera fiable:

- la ventana se decide por `event_time` y no por el orden de llegada;
- un duplicado dentro de la ventana no cambia el total;
- la acumulación publica el **total**, no el incremento;
- claves y ventanas distintas no se mezclan.

**Lo que estas pruebas no verifican, y conviene decirlo.** En
`apache-beam==2.74` con `DirectRunner`, `TestStream` acumula los elementos en la
ventana y emite un único pane `ON_TIME`: el disparo del trigger es diferido, así
que un elemento añadido después de `advance_watermark_to` se pliega dentro de la
misma emisión en lugar de producir un pane `LATE` aparte. Un elemento muy por
detrás del horizonte de lateness tampoco se descarta en ese arnés. Comprobado
experimentalmente con el watermark en 1000 s y un horizonte de 360 s.

La secuencia real de panes `EARLY` / `ON_TIME` / `LATE` y la aplicación del
horizonte de lateness se demuestran sobre el pipeline vivo, con Kafka y reloj
real, y quedan registradas en `evidence/`. Esa evidencia es más fuerte que una
simulada: es el comportamiento que el sistema tiene en operación.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows, TimestampedValue

from contact_center.transforms import (
    DeduplicateByEventId,
    FormatAggregate,
    QueueMetricsCombineFn,
)

BASE = datetime(2026, 7, 24, 13, 0, 0, tzinfo=UTC)
QUEUE = "soporte-n1"

VENTANA_13_00 = "2026-07-24T13:00:00Z"
VENTANA_13_01 = "2026-07-24T13:01:00Z"


def ts(seconds: float) -> float:
    return (BASE + timedelta(seconds=seconds)).timestamp()


def evento(event_id: str, tipo: str, seconds: float, **payload) -> dict:
    cuerpo = {"contact_id": event_id.split(":")[0], "queue_id": QUEUE}
    cuerpo.update(payload)
    return {
        "event_id": event_id,
        "event_type": tipo,
        "event_time": (BASE + timedelta(seconds=seconds)).isoformat(),
        "key": event_id.split(":")[0],
        "schema_version": 1,
        "payload": cuerpo,
    }


def queued(contact: str, seconds: float, queue_id: str = QUEUE) -> dict:
    return evento(
        f"{contact}:queued",
        "contact_queued",
        seconds,
        queue_id=queue_id,
        channel="voice",
        priority="normal",
    )


def connected(contact: str, seconds: float, wait: float) -> dict:
    return evento(
        f"{contact}:connected",
        "contact_connected",
        seconds,
        wait_seconds=wait,
        agent_id="ag-01",
    )


def ended(contact: str, seconds: float, wait: float, disposition: str = "resolved") -> dict:
    return evento(
        f"{contact}:ended",
        "contact_ended",
        seconds,
        disposition=disposition,
        wait_seconds=wait,
        talk_seconds=0.0 if disposition == "abandoned" else 120.0,
    )


def _pipeline(pipeline, stream, *, con_formato: bool = False):
    """Aplicar la misma política temporal que el pipeline de producción."""
    coleccion = (
        pipeline
        | stream
        | "Ventana"
        >> beam.WindowInto(
            FixedWindows(60),
            trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
            allowed_lateness=300,
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        )
        | "Clave" >> beam.Map(lambda e: (e["payload"]["queue_id"], e))
        | "Dedup" >> beam.ParDo(DeduplicateByEventId())
        | "Combinar" >> beam.CombinePerKey(QueueMetricsCombineFn(20))
    )
    if con_formato:
        return coleccion | "Formato" >> beam.ParDo(FormatAggregate("queue_metrics"))
    return coleccion


def _stream(*instrucciones) -> TestStream:
    stream = TestStream().advance_watermark_to(ts(0))
    for tipo, carga in instrucciones:
        if tipo == "wm":
            stream = stream.advance_watermark_to(ts(carga))
        else:
            stream = stream.add_elements(
                [TimestampedValue(valor, ts(segundos)) for valor, segundos in carga]
            )
    return stream.advance_watermark_to_infinity()


# --- Deduplicación -----------------------------------------------------------


def test_el_duplicado_en_la_misma_ventana_no_cambia_el_total():
    duplicado = [
        (queued("ct-a", 5), 5),
        (connected("ct-a", 10, 5.0), 10),
        (ended("ct-a", 50, 5.0), 50),
        # Reintento del origen: mismo event_id y mismo contenido.
        (queued("ct-a", 5), 12),
        (connected("ct-a", 10, 5.0), 13),
        (ended("ct-a", 50, 5.0), 51),
    ]
    stream = _stream(("add", duplicado))

    with BeamTestPipeline() as pipeline:
        resultado = _pipeline(pipeline, stream) | "Solo metricas" >> beam.Map(lambda kv: kv[1])
        assert_that(resultado, _un_solo_contacto)


def _un_solo_contacto(actual):
    filas = list(actual)
    assert filas, "la ventana debería haberse emitido"
    for fila in filas:
        assert fila["offered"] == 1, f"el duplicado infló el total: {fila['offered']}"
        assert fila["connected"] == 1
        # La espera se toma de `contact_ended`, que es el único evento que
        # existe tanto para atendidos como para abandonados.
        assert fila["avg_wait_seconds"] == 5.0
        assert fila["resolved"] == 1


# --- Ventana por tiempo de evento -------------------------------------------


def test_la_ventana_se_decide_por_event_time_no_por_orden_de_llegada():
    """El contacto ocurrido a las 13:00:40 cae en la ventana de las 13:00."""
    stream = _stream(
        ("add", [(queued("ct-a", 5), 5)]),
        ("wm", 70),
        # Este contacto ocurrió a las 13:00:40 pero entra después de que el
        # watermark pasó el fin de la ventana.
        ("add", [(queued("ct-b", 40), 40)]),
        ("wm", 400),
    )

    with BeamTestPipeline() as pipeline:
        resultado = _pipeline(pipeline, stream, con_formato=True)
        assert_that(resultado, _todo_en_la_ventana_13_00)


def _todo_en_la_ventana_13_00(actual):
    filas = list(actual)
    assert filas, "no se emitió ninguna revisión"
    fuera = [f for f in filas if f["window_start"] != VENTANA_13_00]
    assert not fuera, f"aparecieron eventos en otra ventana: {[f['window_start'] for f in fuera]}"


def test_ventanas_distintas_no_se_mezclan():
    stream = _stream(
        ("add", [(queued("ct-a", 5), 5), (connected("ct-a", 10, 5.0), 10)]),
        ("add", [(queued("ct-b", 70), 70), (connected("ct-b", 75, 5.0), 75)]),
    )

    with BeamTestPipeline() as pipeline:
        resultado = _pipeline(pipeline, stream, con_formato=True)
        assert_that(resultado, _dos_ventanas)


def _dos_ventanas(actual):
    ventanas = {f["window_start"] for f in actual}
    assert ventanas == {VENTANA_13_00, VENTANA_13_01}, ventanas
    for fila in actual:
        assert fila["offered"] == 1, f"las ventanas se mezclaron: {fila}"


# --- Acumulación -------------------------------------------------------------


def test_la_acumulacion_publica_el_total_no_el_incremento():
    """Dos contactos en la misma ventana dan 2, no 1 y luego 1."""
    stream = _stream(
        ("add", [(queued("ct-a", 5), 5), (queued("ct-b", 20), 20)]),
    )

    with BeamTestPipeline() as pipeline:
        resultado = _pipeline(pipeline, stream) | "Solo metricas" >> beam.Map(lambda kv: kv[1])
        assert_that(resultado, _total_acumulado)


def _total_acumulado(actual):
    for fila in actual:
        assert fila["offered"] == 2, (
            f"con panes acumulativos el valor es el total, no el delta: {fila['offered']}"
        )


# --- Contrato de salida ------------------------------------------------------


def test_el_contrato_de_salida_expone_los_metadatos_de_ventana():
    stream = _stream(("add", [(queued("ct-a", 5), 5), (connected("ct-a", 10, 5.0), 10)]))

    with BeamTestPipeline() as pipeline:
        resultado = _pipeline(pipeline, stream, con_formato=True)
        assert_that(resultado, _contrato_completo)


def _contrato_completo(actual):
    campos = {
        "schema_version",
        "aggregate_id",
        "metric_type",
        "dimension_id",
        "window_start",
        "window_end",
        "pane_index",
        "pane_timing",
        "is_first",
        "is_last",
        "offered",
        "connected",
        "ns20",
    }
    for fila in actual:
        faltantes = campos - set(fila)
        assert not faltantes, f"faltan campos en el contrato: {sorted(faltantes)}"
        assert fila["window_start"] == VENTANA_13_00
        assert fila["window_end"] == VENTANA_13_01
        # `window_start` termina en Z: identifica un instante sin ambigüedad.
        assert fila["window_start"].endswith("Z")
        # `pane_timing` es un nombre, no el número del enum.
        assert fila["pane_timing"] in {"EARLY", "ON_TIME", "LATE", "UNKNOWN"}, fila["pane_timing"]
        assert fila["aggregate_id"] == f"queue_metrics|{QUEUE}|{VENTANA_13_00}"
        assert fila["metric_type"] == "queue_metrics"
        assert fila["dimension_id"] == QUEUE
