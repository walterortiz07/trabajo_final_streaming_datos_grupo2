"""Agregación por cola en ventana fija, sobre tiempo de evento.

Regla de asignación: cada evento se contabiliza en la ventana que contiene su
`event_time`, no en la ventana en que llegó. Un contacto largo aporta su encolado
a una ventana y su finalización a otra; esaLimitación se documenta en el README y
es exactamente el problema que aborda la Tarea 2 con ventanas y watermarks.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from contact_center.contracts import iso_utc, parse_iso_utc


def window_start_for(event_time: datetime, window_seconds: int) -> datetime:
    """Inicio de la ventana fija que contiene al evento."""
    epoch = int(event_time.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % window_seconds), tz=UTC)


def percentile(values: list[float], fraction: float) -> float:
    """Percentil por rango más cercano, sin interpolación."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[min(index, len(ordered) - 1)]


@dataclass
class _Window:
    """Estado acumulado de una cola dentro de una ventana."""

    queue_id: str
    window_start: datetime
    window_seconds: int
    offered: int = 0
    connected: int = 0
    abandoned: int = 0
    transferred: int = 0
    resolved: int = 0
    answered_within_sl: int = 0
    waits: list[float] = field(default_factory=list)
    talk_seconds: float = 0.0
    seen_event_ids: set[str] = field(default_factory=set)
    dirty: bool = False

    @property
    def finished(self) -> int:
        return self.connected + self.abandoned

    def observe(self, event: dict[str, Any], service_level_seconds: int) -> bool:
        """Incorpora un evento. Devuelve False si ya estaba contabilizado."""
        event_id = event["event_id"]
        if event_id in self.seen_event_ids:
            return False
        self.seen_event_ids.add(event_id)

        event_type = event["event_type"]
        payload = event["payload"]
        self.dirty = True

        if event_type == "contact_queued":
            self.offered += 1
        elif event_type == "contact_connected":
            self.connected += 1
            if float(payload.get("wait_seconds", 0.0)) <= service_level_seconds:
                self.answered_within_sl += 1
        elif event_type == "contact_ended":
            # La espera se toma del evento de finalización: es el único que
            # existe tanto para contactos atendidos como para abandonados.
            self.waits.append(float(payload.get("wait_seconds", 0.0)))
            disposition = payload.get("disposition")
            if disposition == "abandoned":
                self.abandoned += 1
            else:
                self.talk_seconds += float(payload.get("talk_seconds", 0.0))
                if disposition == "transferred":
                    self.transferred += 1
                else:
                    self.resolved += 1
        return True

    def to_metrics(self, service_level_seconds: int) -> QueueWindowMetrics:
        window_end = self.window_start.timestamp() + self.window_seconds
        return QueueWindowMetrics(
            aggregate_id=f"{self.queue_id}:{iso_utc(self.window_start)}",
            queue_id=self.queue_id,
            window_start=iso_utc(self.window_start),
            window_end=iso_utc(datetime.fromtimestamp(window_end, tz=UTC)),
            window_seconds=self.window_seconds,
            offered=self.offered,
            connected=self.connected,
            abandoned=self.abandoned,
            transferred=self.transferred,
            resolved=self.resolved,
            answered_within_sl=self.answered_within_sl,
            service_level_seconds=service_level_seconds,
            # Cociente sobre la misma población: los atendidos en la ventana.
            ns20=round(self.answered_within_sl / self.connected, 4) if self.connected else 0.0,
            # Cociente sobre la misma población: los resueltos en la ventana.
            abandon_rate=round(self.abandoned / self.finished, 4) if self.finished else 0.0,
            avg_wait_seconds=round(sum(self.waits) / len(self.waits), 3) if self.waits else 0.0,
            p90_wait_seconds=round(percentile(self.waits, 0.90), 3),
            max_wait_seconds=round(max(self.waits), 3) if self.waits else 0.0,
            total_talk_seconds=round(self.talk_seconds, 3),
            events_counted=len(self.seen_event_ids),
        )


@dataclass(frozen=True)
class QueueWindowMetrics:
    """Contrato de salida: un agregado por cola y ventana."""

    aggregate_id: str
    queue_id: str
    window_start: str
    window_end: str
    window_seconds: int
    offered: int
    connected: int
    abandoned: int
    transferred: int
    resolved: int
    answered_within_sl: int
    service_level_seconds: int
    ns20: float
    abandon_rate: float
    avg_wait_seconds: float
    p90_wait_seconds: float
    max_wait_seconds: float
    total_talk_seconds: float
    events_counted: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class MetricsAggregator:
    """Estado por (cola, ventana) con deduplicación por event_id y emisión incremental."""

    def __init__(self, *, window_seconds: int = 60, service_level_seconds: int = 20) -> None:
        self.window_seconds = window_seconds
        self.service_level_seconds = service_level_seconds
        self._windows: dict[tuple[str, int], _Window] = {}
        self.accepted = 0
        self.duplicates = 0

    def observe(self, event: dict[str, Any]) -> bool:
        """Incorpora un evento de contacto ya validado contra el contrato."""
        at = parse_iso_utc(event["event_time"])
        start = window_start_for(at, self.window_seconds)
        key = (event["payload"]["queue_id"], int(start.timestamp()))
        window = self._windows.get(key)
        if window is None:
            window = _Window(
                queue_id=event["payload"]["queue_id"],
                window_start=start,
                window_seconds=self.window_seconds,
            )
            self._windows[key] = window
        if window.observe(event, self.service_level_seconds):
            self.accepted += 1
            return True
        self.duplicates += 1
        return False

    def metrics(self) -> list[QueueWindowMetrics]:
        """Todos los agregados, en orden de ventana y cola."""
        return [
            window.to_metrics(self.service_level_seconds)
            for _, window in sorted(self._windows.items(), key=lambda kv: kv[0][1])
        ]

    def drain(self) -> list[QueueWindowMetrics]:
        """Agregados modificados desde el último drenado, para publicación incremental.

        Un evento tardío reabre su ventana y vuelve a marcarla como sucia, así que
        el agregado se republica con el mismo aggregate_id. El consumidor de
        salida hace upsert por esa clave y conserva la revisión más reciente.
        """
        pending: list[QueueWindowMetrics] = []
        for window in self._windows.values():
            if window.dirty:
                window.dirty = False
                pending.append(window.to_metrics(self.service_level_seconds))
        return sorted(pending, key=lambda m: (m.window_start, m.queue_id))

    def summary(self) -> dict[str, int]:
        return {
            "accepted": self.accepted,
            "duplicates": self.duplicates,
            "windows": len(self._windows),
        }
