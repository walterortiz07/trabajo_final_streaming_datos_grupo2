"""Contrato de eventos del contact center: esquema estable, codificación y validación.

El contrato es el mismo para los cuatro tipos de evento. Cada evento es un sobre
(event_id, event_type, event_time, schema_version, key, payload) y el contenido
propio del tipo vive dentro de payload.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = 1

# Namespace fijo: el mismo hecho produce siempre el mismo event_id, así que un
# reproceso (replay) genera identificadores idénticos y la deduplicación es
# posible aunque el pipeline se re-ejecute.
EVENT_NAMESPACE = uuid.UUID("9f2c4b16-7d3a-4e58-b0c1-5a8e6d21f340")

CONTACT_EVENT_TYPES = ("contact_queued", "contact_connected", "contact_ended")
AGENT_EVENT_TYPES = ("agent_state_changed",)
EVENT_TYPES = CONTACT_EVENT_TYPES + AGENT_EVENT_TYPES

# Campos que cada tipo de evento debe traer en payload. Se admiten campos
# adicionales para no bloquear la evolución del esquema.
REQUIRED_PAYLOAD_FIELDS: dict[str, tuple[str, ...]] = {
    "contact_queued": ("contact_id", "queue_id", "channel", "priority"),
    "contact_connected": ("contact_id", "queue_id", "agent_id", "wait_seconds"),
    "contact_ended": (
        "contact_id",
        "queue_id",
        "disposition",
        "wait_seconds",
        "talk_seconds",
    ),
    "agent_state_changed": ("agent_id", "from_state", "to_state"),
}

DISPOSITIONS = ("resolved", "transferred", "abandoned")
AGENT_STATES = ("logged_in", "ready", "busy", "wrapup", "not_ready", "logged_out")
CHANNELS = ("voice", "chat", "email", "whatsapp")
PRIORITIES = ("normal", "alta")

ENVELOPE_FIELDS = (
    "event_id",
    "event_type",
    "event_time",
    "schema_version",
    "key",
    "payload",
)


def parse_iso_utc(value: str) -> datetime:
    """Convierte un timestamp ISO-8601 a datetime con zona UTC."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso_utc(value: datetime) -> str:
    """Serializa un datetime como ISO-8601 UTC terminado en Z.

    Se fija la precisión en milisegundos para que la representación tenga ancho
    constante. Así el orden lexicográfico de los strings coincide con el orden
    cronológico, que es lo que necesitan los consumidores al comparar o indexar
    por `event_time` sin volver a parsear.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def event_id_for(*parts: str) -> str:
    """Identificador único, estable y determinista, usable para deduplicar."""
    return str(uuid.uuid5(EVENT_NAMESPACE, "|".join(parts)))


@dataclass(frozen=True)
class Event:
    """Un evento del dominio tal como viaja por Kafka."""

    event_id: str
    event_type: str
    event_time: str
    key: str
    payload: dict[str, Any] = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_event(
    *,
    event_type: str,
    event_time: datetime,
    key: str,
    payload: dict[str, Any],
    event_id: str | None = None,
) -> Event:
    """Construye un evento validando el tipo y completando el event_id si falta."""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"event_type no soportado: {event_type!r}")
    return Event(
        event_id=event_id or event_id_for(key, event_type, iso_utc(event_time)),
        event_type=event_type,
        event_time=iso_utc(event_time),
        key=key,
        payload=dict(payload),
    )


def encode_event(event: Event | dict[str, Any]) -> bytes:
    payload = event.as_dict() if isinstance(event, Event) else event
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def decode_event(raw: bytes | str) -> dict[str, Any]:
    """Decodifica y valida el contrato. Lanza ValueError si el evento no lo cumple.

    El mismo validador se usa en el consumidor para separar los eventos inválidos
    hacia el tópico de descarte (DLQ).
    """
    if isinstance(raw, bytes):
        try:
            raw = raw.decode()
        except UnicodeDecodeError as exc:
            raise ValueError(f"el payload no es UTF-8 válido: {exc}") from exc

    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"el payload no es JSON válido: {exc}") from exc

    if not isinstance(decoded, dict):
        raise ValueError("el evento debe ser un objeto JSON")

    missing = [name for name in ENVELOPE_FIELDS if name not in decoded]
    if missing:
        raise ValueError(f"faltan campos obligatorios del sobre: {missing}")

    event_type = decoded["event_type"]
    if event_type not in EVENT_TYPES:
        raise ValueError(f"event_type no soportado: {event_type!r}")

    if not isinstance(decoded["payload"], dict):
        raise ValueError("payload debe ser un objeto JSON")

    missing_payload = [
        name
        for name in REQUIRED_PAYLOAD_FIELDS[event_type]
        if name not in decoded["payload"]
    ]
    if missing_payload:
        raise ValueError(f"{event_type} sin campos obligatorios en payload: {missing_payload}")

    if not isinstance(decoded["event_time"], str):
        raise ValueError("event_time debe ser un string ISO-8601")
    try:
        decoded["event_time"] = iso_utc(parse_iso_utc(decoded["event_time"]))
    except ValueError as exc:
        raise ValueError(f"event_time no es ISO-8601 válido: {decoded['event_time']!r}") from exc

    if not isinstance(decoded["key"], str) or not decoded["key"]:
        raise ValueError("key debe ser un string no vacío")

    if decoded["event_type"] in CONTACT_EVENT_TYPES:
        contact_id = decoded["payload"].get("contact_id")
        if contact_id != decoded["key"]:
            raise ValueError(
                f"key ({decoded['key']!r}) debe ser el contact_id ({contact_id!r}) "
                "en los eventos de contacto"
            )

    return decoded


def is_agent_event(event_type: str) -> bool:
    return event_type in AGENT_EVENT_TYPES
