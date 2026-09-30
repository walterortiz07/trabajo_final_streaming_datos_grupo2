"""Pruebas del contrato de eventos: construcción, codificación y validación."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from contact_center.contracts import (
    EVENT_TYPES,
    Event,
    build_event,
    decode_event,
    encode_event,
    event_id_for,
    iso_utc,
)

AT = datetime(2026, 9, 28, 14, 3, 21, tzinfo=UTC)


def queued_event(**overrides) -> Event:
    payload = {
        "contact_id": "ct-8f2a1b3c",
        "queue_id": "soporte-n1",
        "channel": "voice",
        "priority": "normal",
    }
    payload.update(overrides.pop("payload", {}))
    return build_event(
        event_type=overrides.pop("event_type", "contact_queued"),
        event_time=overrides.pop("event_time", AT),
        key=overrides.pop("key", payload["contact_id"]),
        payload=payload,
        **overrides,
    )


def test_evento_tiene_los_seis_campos_del_sobre():
    event = queued_event()
    assert set(event.as_dict()) == {
        "event_id",
        "event_type",
        "event_time",
        "schema_version",
        "key",
        "payload",
    }


def test_roundtrip_encode_decode():
    event = queued_event()
    decoded = decode_event(encode_event(event))
    assert decoded == json.loads(encode_event(event))


def test_event_id_es_estable_y_reproducible():
    primero = build_event(
        event_type="contact_queued",
        event_time=AT,
        key="ct-1",
        payload={"contact_id": "ct-1", "queue_id": "q", "channel": "voice", "priority": "normal"},
    )
    segundo = build_event(
        event_type="contact_queued",
        event_time=AT,
        key="ct-1",
        payload={"contact_id": "ct-1", "queue_id": "q", "channel": "voice", "priority": "normal"},
    )
    assert primero.event_id == segundo.event_id


def test_event_id_for_distingue_partes():
    assert event_id_for("ct-1", "contact_queued") != event_id_for("ct-1", "contact_ended")


def test_event_type_desconocido_es_rechazado():
    with pytest.raises(ValueError, match="event_type no soportado"):
        build_event(event_type="contact_paused", event_time=AT, key="ct-1", payload={})


def test_decode_rechaza_campo_faltante_del_sobre():
    raw = json.loads(encode_event(queued_event()))
    del raw["schema_version"]
    with pytest.raises(ValueError, match="faltan campos obligatorios del sobre"):
        decode_event(json.dumps(raw))


def test_decode_rechaza_campo_faltante_del_payload():
    raw = json.loads(encode_event(queued_event()))
    del raw["payload"]["queue_id"]
    with pytest.raises(ValueError, match="sin campos obligatorios en payload"):
        decode_event(json.dumps(raw))


def test_decode_rechaza_event_type_desconocido():
    raw = json.loads(encode_event(queued_event()))
    raw["event_type"] = "contact_paused"
    with pytest.raises(ValueError, match="event_type no soportado"):
        decode_event(json.dumps(raw))


def test_decode_rechaza_event_time_no_iso():
    raw = json.loads(encode_event(queued_event()))
    raw["event_time"] = "28/09/2026 14:03"
    with pytest.raises(ValueError, match="event_time no es ISO-8601"):
        decode_event(json.dumps(raw))


def test_decode_rechaza_key_que_no_es_el_contact_id():
    raw = json.loads(encode_event(queued_event()))
    raw["key"] = "otra-cosa"
    with pytest.raises(ValueError, match="debe ser el contact_id"):
        decode_event(json.dumps(raw))


def test_decode_normaliza_event_time_a_utc():
    event = queued_event(event_time=datetime(2026, 9, 28, 10, 3, 21, tzinfo=UTC))
    raw = json.loads(encode_event(event))
    raw["event_time"] = "2026-09-28T12:03:21+02:00"
    assert decode_event(json.dumps(raw))["event_time"] == "2026-09-28T10:03:21.000Z"


def test_decode_rechaza_json_invalido():
    with pytest.raises(ValueError, match="no es JSON válido"):
        decode_event(b"{esto no es json")


def test_decode_rechaza_payload_que_no_es_objeto():
    raw = json.loads(encode_event(queued_event()))
    raw["payload"] = ["lista", "no", "objeto"]
    with pytest.raises(ValueError, match="payload debe ser un objeto JSON"):
        decode_event(json.dumps(raw))


def test_cada_tipo_de_evento_exige_sus_campos():
    """Un evento de agente no puede colarse con los campos de un contacto."""
    raw = {
        "event_id": "x",
        "event_type": "agent_state_changed",
        "event_time": "2026-09-28T14:03:21Z",
        "schema_version": 1,
        "key": "ag-1",
        "payload": {"contact_id": "ct-1", "queue_id": "soporte-n1"},
    }
    with pytest.raises(ValueError, match="agent_state_changed sin campos obligatorios"):
        decode_event(json.dumps(raw))


def test_el_tiempo_de_evento_tiene_ancho_constante():
    """Ordenar por string debe dar el mismo resultado que ordenar por tiempo."""
    momentos = [
        datetime(2026, 9, 28, 14, 0, 0, tzinfo=UTC),
        datetime(2026, 9, 28, 14, 0, 0, 81763, tzinfo=UTC),
        datetime(2026, 9, 28, 14, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 28, 23, 59, 59, 999999, tzinfo=UTC),
    ]
    serializados = [iso_utc(m) for m in momentos]
    assert len({len(s) for s in serializados}) == 1
    assert serializados == sorted(serializados)


def test_tipos_de_evento_soportados():
    assert set(EVENT_TYPES) == {
        "contact_queued",
        "contact_connected",
        "contact_ended",
        "agent_state_changed",
    }
