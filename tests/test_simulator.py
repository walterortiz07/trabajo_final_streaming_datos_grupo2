"""Pruebas del generador: determinismo, ciclo de vida y coherencia de las claves."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

from contact_center.contracts import CONTACT_EVENT_TYPES, decode_event, encode_event
from contact_center.simulator import Scenario, simulate

START = datetime(2026, 9, 28, 14, 0, 0, tzinfo=UTC)


def scenario(**overrides) -> Scenario:
    params = {"start": START, "duration_minutes": 20, "seed": 11}
    params.update(overrides)
    return Scenario(**params)


def test_misma_semilla_produce_los_mismos_eventos():
    primero = simulate(scenario())
    segundo = simulate(scenario())
    assert encode_event(primero.contacts[0]) == encode_event(segundo.contacts[0])
    assert [e.as_dict() for e in primero.contacts] == [e.as_dict() for e in segundo.contacts]


def test_semillas_distintas_cambian_la_linea_de_tiempo():
    """La semilla mueve llegadas y atributos; los identificadores son estables."""
    primero = simulate(scenario())
    segundo = simulate(scenario(seed=99))
    assert [e.event_time for e in primero.contacts] != [
        e.event_time for e in segundo.contacts
    ]


def test_todos_los_eventos_pasan_el_contrato():
    result = simulate(scenario())
    for event in result.all_events:
        assert decode_event(encode_event(event))["event_id"] == event.event_id


def test_cada_contacto_tiene_encolado_y_finalizacion():
    result = simulate(scenario())
    por_tipo: dict[str, set[str]] = defaultdict(set)
    for event in result.contacts:
        por_tipo[event.event_type].add(event.key)

    assert por_tipo["contact_queued"], "la simulación no generó contactos"
    assert por_tipo["contact_queued"] == por_tipo["contact_ended"]
    # Todo contacto conectado también terminó, pero hay abandonados sin conexión.
    assert por_tipo["contact_connected"] <= por_tipo["contact_ended"]


def test_event_id_de_contacto_es_unico_por_tipo():
    result = simulate(scenario())
    ids = [e.event_id for e in result.contacts]
    assert len(ids) == len(set(ids))


def test_clave_de_contacto_es_el_contact_id():
    result = simulate(scenario())
    for event in result.contacts:
        assert event.key == event.payload["contact_id"]
        assert event.event_type in CONTACT_EVENT_TYPES


def test_clave_de_agente_es_el_agent_id():
    result = simulate(scenario())
    assert result.agent_states
    for event in result.agent_states:
        assert event.key == event.payload["agent_id"]


def test_transiciones_de_agente_encadenan_estado():
    """Cada transición arranca en el estado en que terminó la anterior."""
    result = simulate(scenario())
    ultimo: dict[str, str] = {}
    for event in result.agent_states:
        agent_id = event.key
        esperado = ultimo.get(agent_id, "logged_out")
        assert event.payload["from_state"] == esperado, (
            f"{agent_id}: se esperaba {esperado} y llegó {event.payload['from_state']}"
        )
        ultimo[agent_id] = event.payload["to_state"]


def test_tiempo_de_evento_no_retrocede_dentro_de_cada_topico():
    result = simulate(scenario())
    for events in (result.contacts, result.agent_states):
        tiempos = [e.event_time for e in events]
        assert tiempos == sorted(tiempos)


def test_la_linea_de_tiempo_es_causal():
    """Reordenar por event_id dentro de un mismo instante rompería el ciclo de vida."""
    result = simulate(scenario())
    posicion: dict[tuple[str, str], int] = {}
    for index, event in enumerate(result.all_events):
        posicion.setdefault((event.key, event.event_type), index)

    for (contact, tipo), index in posicion.items():
        if tipo in {"contact_connected", "contact_ended"}:
            assert posicion[(contact, "contact_queued")] < index, (
                f"{contact}: {tipo} se emitió antes de contact_queued"
            )
        if tipo == "contact_connected":
            assert posicion[(contact, "contact_ended")] > index


def test_la_linea_de_tiempo_no_retrocede():
    result = simulate(scenario())
    tiempos = [e.event_time for e in result.all_events]
    assert tiempos == sorted(tiempos)


def test_la_linea_de_tiempo_incluye_los_dos_topicos():
    result = simulate(scenario())
    assert len(result.all_events) == len(result.contacts) + len(result.agent_states)


def test_un_contacto_conectado_siempre_tiene_agente():
    result = simulate(scenario())
    for event in result.contacts:
        if event.event_type == "contact_connected":
            assert event.payload["agent_id"].startswith("ag-")
            assert event.payload["wait_seconds"] >= 0


def test_duracion_de_la_jornada_acota_las_llegadas():
    result = simulate(scenario(duration_minutes=5))
    encolados = [e for e in result.contacts if e.event_type == "contact_queued"]
    assert encolados
    assert all(e.event_time < "2026-09-28T14:05:00" for e in encolados)


def test_resumen_reporta_contactos_y_tipos():
    resumen = simulate(scenario()).summary()
    assert resumen["contactos"] > 0
    assert resumen["total"] == sum(
        v for k, v in resumen.items() if k not in {"contactos", "total"}
    )
