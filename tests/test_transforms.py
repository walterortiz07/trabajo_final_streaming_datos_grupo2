"""Pruebas de las transformaciones Beam con TestPipeline."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import FixedWindows

from contact_center.transforms import (
    DeduplicateByEventId,
    FormatAggregate,
    ParseEvent,
    QueueMetricsCombineFn,
    StateCountCombineFn,
    assign_event_timestamp,
    build_analytics,
    iso_utc_from_timestamp,
)

BASE = datetime(2026, 7, 24, 13, 0, 0, tzinfo=UTC)
QUEUE = "soporte-n1"


def momento(seconds: float) -> str:
    return (BASE + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def queued(contact_id: str, seconds: float, queue_id: str = QUEUE) -> dict[str, Any]:
    return {
        "event_id": f"{contact_id}:queued",
        "event_type": "contact_queued",
        "event_time": momento(seconds),
        "key": contact_id,
        "schema_version": 1,
        "payload": {
            "contact_id": contact_id,
            "queue_id": queue_id,
            "channel": "voice",
            "priority": "normal",
        },
    }


def connected(contact_id: str, seconds: float, wait: float, agent: str = "ag-01") -> dict[str, Any]:
    return {
        "event_id": f"{contact_id}:connected",
        "event_type": "contact_connected",
        "event_time": momento(seconds),
        "key": contact_id,
        "schema_version": 1,
        "payload": {
            "contact_id": contact_id,
            "queue_id": QUEUE,
            "agent_id": agent,
            "wait_seconds": wait,
        },
    }


def ended(
    contact_id: str, seconds: float, wait: float, disposition: str = "resolved"
) -> dict[str, Any]:
    return {
        "event_id": f"{contact_id}:ended",
        "event_type": "contact_ended",
        "event_time": momento(seconds),
        "key": contact_id,
        "schema_version": 1,
        "payload": {
            "contact_id": contact_id,
            "queue_id": QUEUE,
            "disposition": disposition,
            "wait_seconds": wait,
            "talk_seconds": 0.0 if disposition == "abandoned" else 120.0,
        },
    }


def agent_state(agent: str, seconds: float, to_state: str) -> dict[str, Any]:
    return {
        "event_id": f"{agent}:{to_state}:{seconds}",
        "event_type": "agent_state_changed",
        "event_time": momento(seconds),
        "key": agent,
        "schema_version": 1,
        "payload": {"agent_id": agent, "from_state": "ready", "to_state": to_state},
    }


# --- Validación del contrato -------------------------------------------------


def test_parse_event_separa_los_invalidos():
    valido = json.dumps(queued("ct-1", 5)).encode()
    roto = b'{"event_type": "contact_queued"}'

    with BeamTestPipeline() as pipeline:
        resultado = (
            pipeline
            | "Entrada" >> beam.Create([(b"ct-1", valido), (b"ct-1", roto)])
            | "Validar" >> beam.ParDo(ParseEvent()).with_outputs("invalid", main="valid")
        )
        # `decode_event` normaliza `event_time` a milisegundos, así que se
        # comparan los campos estables y no el JSON crudo.
        assert_that(
            resultado.valid
            | "Resumen"
            >> beam.Map(lambda e: {"event_id": e["event_id"], "kafka_key": e["kafka_key"]}),
            equal_to([{"event_id": "ct-1:queued", "kafka_key": "ct-1"}]),
        )
        assert_that(resultado.invalid | "Contar" >> beam.combiners.Count.Globally(), equal_to([1]))


def test_parse_event_no_falla_con_payload_vacio():
    with BeamTestPipeline() as pipeline:
        resultado = (
            pipeline
            | beam.Create([(None, None)])
            | beam.ParDo(ParseEvent()).with_outputs("invalid", main="valid")
        )
        assert_that(resultado.invalid | "Contar" >> beam.combiners.Count.Globally(), equal_to([1]))


# --- Ventanas y agregación ---------------------------------------------------


def test_metricas_por_cola_en_una_ventana():
    eventos = [
        queued("ct-1", 5),
        connected("ct-1", 10, wait=5.0),
        ended("ct-1", 50, wait=5.0),
        queued("ct-2", 20),
        ended("ct-2", 40, wait=18.0, disposition="abandoned"),
    ]

    with BeamTestPipeline() as pipeline:
        resultado = (
            pipeline
            | beam.Create(eventos)
            | beam.Map(assign_event_timestamp)
            | beam.WindowInto(FixedWindows(60))
            | beam.Map(lambda e: (e["payload"]["queue_id"], e))
            | beam.ParDo(DeduplicateByEventId())
            | beam.CombinePerKey(QueueMetricsCombineFn(20))
        )
        assert_that(resultado, equal_to([(QUEUE, _metricas_esperadas())]))


def _metricas_esperadas() -> dict[str, Any]:
    return {
        "offered": 2,
        "connected": 1,
        "abandoned": 1,
        "transferred": 0,
        "resolved": 1,
        "answered_within_sl": 1,
        "ns20": 1.0,
        "abandon_rate": 0.5,
        "avg_wait_seconds": 11.5,
        "p90_wait_seconds": 18.0,
        "max_wait_seconds": 18.0,
        "total_talk_seconds": 120.0,
    }


def test_los_eventos_de_otra_ventana_no_se_mezclan():
    eventos = [
        queued("ct-1", 5),
        connected("ct-1", 10, wait=5.0),
        queued("ct-2", 70),
        connected("ct-2", 75, wait=5.0),
    ]

    with BeamTestPipeline() as pipeline:
        resultado = (
            pipeline
            | beam.Create(eventos)
            | beam.Map(assign_event_timestamp)
            | beam.WindowInto(FixedWindows(60))
            | beam.Map(lambda e: (e["payload"]["queue_id"], e))
            | beam.ParDo(DeduplicateByEventId())
            | beam.CombinePerKey(QueueMetricsCombineFn(20))
            | beam.Map(lambda kv: kv[1]["connected"])
        )
        assert_that(resultado, equal_to([1, 1]))


def test_el_tiempo_de_evento_decide_la_ventana_no_el_de_llegada():
    """Un evento ocurrido en el minuto 13:00 cuenta en esa ventana."""
    eventos = [queued("ct-1", 5), connected("ct-1", 10, wait=5.0)]

    with BeamTestPipeline() as pipeline:
        resultado = (
            pipeline
            | beam.Create(eventos)
            | beam.Map(assign_event_timestamp)
            | beam.WindowInto(FixedWindows(60))
            | beam.Map(lambda e: (e["payload"]["queue_id"], e))
            | beam.ParDo(DeduplicateByEventId())
            | beam.CombinePerKey(QueueMetricsCombineFn(20))
            # Se devuelve una lista y no un dict: un dict devuelto desde un
            # lambda de ParDo se interpreta como salidas etiquetadas.
            | beam.Map(
                lambda kv, w=beam.DoFn.WindowParam: [
                    iso_utc_from_timestamp(w.start),
                    kv[1]["offered"],
                ]
            )
        )
        assert_that(resultado, equal_to([["2026-07-24T13:00:00Z", 1]]))


# --- Deduplicación -----------------------------------------------------------


def test_el_duplicado_no_cambia_el_total():
    eventos = [
        queued("ct-1", 5),
        connected("ct-1", 10, wait=5.0),
        queued("ct-1", 5),
        connected("ct-1", 10, wait=5.0),
    ]

    with BeamTestPipeline() as pipeline:
        resultado = (
            pipeline
            | beam.Create(eventos)
            | beam.Map(assign_event_timestamp)
            | beam.WindowInto(FixedWindows(60))
            | beam.Map(lambda e: (e["payload"]["queue_id"], e))
            | beam.ParDo(DeduplicateByEventId())
            | beam.CombinePerKey(QueueMetricsCombineFn(20))
            | beam.Map(lambda kv: kv[1]["offered"])
        )
        assert_that(resultado, equal_to([1]))


def test_la_deduplicacion_esta_aislada_por_clave():
    eventos = [
        queued("ct-1", 5, queue_id="soporte-n1"),
        queued("ct-1", 5, queue_id="ventas"),
    ]

    with BeamTestPipeline() as pipeline:
        resultado = (
            pipeline
            | beam.Create(eventos)
            | beam.Map(assign_event_timestamp)
            | beam.WindowInto(FixedWindows(60))
            | beam.Map(lambda e: (e["payload"]["queue_id"], e))
            | beam.ParDo(DeduplicateByEventId())
            | beam.Map(lambda kv: kv[0])
        )
        assert_that(resultado, equal_to(["soporte-n1", "ventas"]))


def test_el_estado_del_duplicado_no_mezcla_ventanas():
    """El mismo event_id en ventanas distintas son dos hechos distintos."""
    eventos = [queued("ct-1", 5), queued("ct-2", 5)]
    eventos[1]["event_id"] = eventos[0]["event_id"]  # mismo id, distinta ventana
    eventos[1]["event_time"] = momento(70)

    with BeamTestPipeline() as pipeline:
        resultado = (
            pipeline
            | beam.Create(eventos)
            | beam.Map(assign_event_timestamp)
            | beam.WindowInto(FixedWindows(60))
            | beam.Map(lambda e: (e["payload"]["queue_id"], e))
            | beam.ParDo(DeduplicateByEventId())
            | beam.Map(lambda kv: kv[1]["payload"]["contact_id"])
        )
        assert_that(resultado, equal_to(["ct-1", "ct-2"]))


# --- Segunda agregación y changelog -----------------------------------------


def test_conteo_de_estados_de_agente():
    eventos = [
        agent_state("ag-01", 5, "busy"),
        agent_state("ag-02", 6, "busy"),
        agent_state("ag-01", 40, "wrapup"),
    ]

    with BeamTestPipeline() as pipeline:
        resultado = (
            pipeline
            | beam.Create(eventos)
            | beam.Map(assign_event_timestamp)
            | beam.WindowInto(FixedWindows(60))
            | beam.Map(lambda e: (e["payload"]["to_state"], e))
            | beam.ParDo(DeduplicateByEventId())
            | beam.CombinePerKey(StateCountCombineFn())
        )
        assert_that(
            resultado,
            equal_to([("busy", {"transitions": 2}), ("wrapup", {"transitions": 1})]),
        )


def test_el_changelog_unifica_las_dos_metricas():
    from contact_center.config import Settings

    eventos = [queued("ct-1", 5), agent_state("ag-01", 6, "busy")]

    with BeamTestPipeline() as pipeline:
        # `build_analytics` asigna el tiempo de evento y aplica la ventana: por
        # eso recibe la PCollection cruda.
        resultado = build_analytics(
            pipeline | beam.Create(eventos),
            Settings(kafka_bootstrap_servers="localhost:29092"),
            streaming_triggers=False,
        )
        assert_that(resultado, _dos_tipos_de_metrica)


def _dos_tipos_de_metrica(actual):
    """El changelog debe traer las dos métricas, sin importar el orden ni los panes."""
    tipos = {row["metric_type"] for row in actual}
    assert tipos == {"queue_metrics", "agent_state_counts"}, tipos


# --- Contraste contra el oráculo en Python puro ------------------------------

# Campos que ambas implementaciones calculan. Si el pipeline y el oráculo
# difieren en alguno, una de las dos está mal.
CAMPOS_COMPARABLES = (
    "offered",
    "connected",
    "abandoned",
    "transferred",
    "resolved",
    "answered_within_sl",
    "ns20",
    "abandon_rate",
    "avg_wait_seconds",
    "p90_wait_seconds",
    "max_wait_seconds",
    "total_talk_seconds",
)


def test_el_pipeline_coincide_con_el_oraculo():
    """El pipeline Beam y el agregador en Python puro deben dar lo mismo.

    Son dos implementaciones independientes de la misma métrica: `metrics.py`
    recorre los eventos a mano, y `transforms.py` los agrega con un combinador
    de Beam. Contrastarlas detecta errores que una prueba contra valores fijos
    dejaría pasar.
    """
    from contact_center.metrics import MetricsAggregator

    eventos = [
        queued("ct-1", 5),
        connected("ct-1", 10, wait=5.0),
        ended("ct-1", 50, wait=5.0),
        queued("ct-2", 20),
        connected("ct-2", 25, wait=18.0),
        ended("ct-2", 45, wait=18.0),
        queued("ct-3", 30),
        ended("ct-3", 55, wait=25.0, disposition="abandoned"),
    ]

    oraculo = MetricsAggregator(window_seconds=60, service_level_seconds=20)
    for evento in eventos:
        oraculo.observe(evento)
    esperado = {fila.queue_id: fila.as_dict() for fila in oraculo.metrics()}

    with BeamTestPipeline() as pipeline:
        resultado = (
            pipeline
            | beam.Create(eventos)
            | beam.Map(assign_event_timestamp)
            | beam.WindowInto(FixedWindows(60))
            | beam.Map(lambda e: (e["payload"]["queue_id"], e))
            | beam.ParDo(DeduplicateByEventId())
            | beam.CombinePerKey(QueueMetricsCombineFn(20))
            | beam.ParDo(FormatAggregate("queue_metrics"))
        )
        assert_that(resultado, _comparar_con_oraculo(esperado))


def _comparar_con_oraculo(esperado_por_cola):
    def _matcher(actual):
        filas = list(actual)
        assert len(filas) == len(esperado_por_cola), (
            f"el pipeline emitió {len(filas)} filas y el oráculo {len(esperado_por_cola)}"
        )
        for fila in filas:
            referencia = esperado_por_cola[fila["dimension_id"]]
            for campo in CAMPOS_COMPARABLES:
                assert fila[campo] == referencia[campo], (
                    f"{campo} en {fila['dimension_id']}: "
                    f"pipeline={fila[campo]!r} oráculo={referencia[campo]!r}"
                )

    return _matcher
