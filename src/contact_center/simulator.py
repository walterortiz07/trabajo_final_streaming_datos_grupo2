"""Generador determinista de una jornada de contact center.

Simula el ciclo de vida completo de cada contacto (encolado, atendido, finalizado)
y las transiciones de estado de los agentes, usando una simulación de eventos
discretos. Con la misma semilla produce exactamente la misma secuencia, lo que
permite reproducir la demostración y probar el contrato sin grabar datos.
"""

from __future__ import annotations

import heapq
import itertools
import random
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from contact_center.contracts import Event, build_event, event_id_for, iso_utc


@dataclass(frozen=True)
class QueueProfile:
    """Dimensionamiento de una cola de atención."""

    queue_id: str
    skill: str
    agents: int
    arrivals_per_hour: int
    talk_mean_seconds: int = 240
    patience_seconds: int = 90
    wrapup_seconds: int = 20


# El laboratorio usa 22 agentes. Las tasas por agente son las mismas que se usan
# en el dimensionamiento de producción del documento (120 agentes), así que las
# conclusiones sobre claves, particiones y orden no cambian al escalar.
DEFAULT_QUEUES: tuple[QueueProfile, ...] = (
    QueueProfile("soporte-n1", "soporte", agents=10, arrivals_per_hour=90),
    QueueProfile(
        "soporte-n2",
        "soporte",
        agents=7,
        arrivals_per_hour=45,
        talk_mean_seconds=420,
        patience_seconds=120,
    ),
    QueueProfile("ventas", "ventas", agents=3, arrivals_per_hour=30),
    QueueProfile("cobranzas", "cobranzas", agents=2, arrivals_per_hour=24, talk_mean_seconds=210),
)

CHANNEL_WEIGHTS = (("voice", 70), ("chat", 15), ("whatsapp", 8), ("email", 7))
PRIORITY_WEIGHTS = (("normal", 90), ("alta", 10))
TRANSFER_PROBABILITY = 0.08


@dataclass(frozen=True)
class Scenario:
    """Parámetros de la simulación."""

    start: datetime
    duration_minutes: int = 30
    queues: tuple[QueueProfile, ...] = DEFAULT_QUEUES
    seed: int = 7

    @classmethod
    def now(cls, **kwargs: Any) -> Scenario:
        return cls(start=datetime.now(UTC).replace(microsecond=0), **kwargs)


@dataclass(frozen=True)
class SimulationResult:
    """Eventos generados.

    `timeline` conserva el orden causal de emisión, que ya es no decreciente en
    tiempo de evento. Es el orden que se publica: reordenar por `event_id` dentro
    de un mismo instante publicaría `contact_connected` antes que su
    `contact_queued`.
    """

    contacts: list[Event]
    agent_states: list[Event]
    timeline: list[Event]
    queues: dict[str, QueueProfile]

    @property
    def all_events(self) -> list[Event]:
        return self.timeline

    def summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for event in self.contacts + self.agent_states:
            counts[event.event_type] = counts.get(event.event_type, 0) + 1
        counts["contactos"] = len({e.key for e in self.contacts})
        counts["total"] = len(self.contacts) + len(self.agent_states)
        return counts


@dataclass
class _Waiting:
    """Contacto encolado a la espera de un agente."""

    contact_id: str
    queue_id: str
    channel: str
    enqueued_at: datetime


def simulate(scenario: Scenario) -> SimulationResult:
    """Ejecuta la simulación y devuelve los eventos de contacto y de agente."""
    rng = random.Random(scenario.seed)
    end = scenario.start + timedelta(minutes=scenario.duration_minutes)
    profiles = {profile.queue_id: profile for profile in scenario.queues}

    contacts: list[Event] = []
    agent_states: list[Event] = []
    timeline: list[Event] = []

    def record(sink: list[Event], event: Event) -> None:
        """Acumula el evento en su tópico y en la línea de tiempo causal."""
        sink.append(event)
        timeline.append(event)

    free_agents: dict[str, list[str]] = {
        profile.queue_id: [
            f"ag-{profile.queue_id}-{n + 1:02d}" for n in range(profile.agents)
        ]
        for profile in scenario.queues
    }
    waiting: dict[str, deque[_Waiting]] = {q: deque() for q in profiles}
    waiting_by_id: dict[str, _Waiting] = {}

    heap: list[tuple[datetime, int, str, Any]] = []
    ticker = itertools.count()

    def push(at: datetime, kind: str, data: Any) -> None:
        heapq.heappush(heap, (at, next(ticker), kind, data))

    transition_seq = itertools.count(1)

    def emit_agent_state(
        agent_id: str, from_state: str, to_state: str, at: datetime, contact_id: str | None
    ) -> None:
        payload: dict[str, Any] = {
            "agent_id": agent_id,
            "from_state": from_state,
            "to_state": to_state,
        }
        if contact_id is not None:
            payload["contact_id"] = contact_id
        record(
            agent_states,
            build_event(
                event_type="agent_state_changed",
                event_time=at,
                key=agent_id,
                payload=payload,
                # El contador de transición mantiene el event_id único aunque un
                # agente vuelva al mismo estado en el mismo milisegundo.
                event_id=event_id_for(agent_id, f"transicion-{next(transition_seq)}"),
            )
        )

    def assign(queue_id: str, at: datetime) -> None:
        """Conecta tantos contactos en espera como agentes libres haya."""
        profile = profiles[queue_id]
        while waiting[queue_id] and free_agents[queue_id]:
            item = waiting[queue_id].popleft()
            waiting_by_id.pop(item.contact_id, None)
            agent_id = free_agents[queue_id].pop()
            wait_seconds = (at - item.enqueued_at).total_seconds()
            record(
                contacts,
                build_event(
                    event_type="contact_connected",
                    event_time=at,
                    key=item.contact_id,
                    payload={
                        "contact_id": item.contact_id,
                        "queue_id": queue_id,
                        "agent_id": agent_id,
                        "channel": item.channel,
                        "wait_seconds": round(wait_seconds, 3),
                    },
                    event_id=event_id_for(item.contact_id, "contact_connected"),
                )
            )
            emit_agent_state(agent_id, "ready", "busy", at, item.contact_id)
            talk = max(15.0, rng.gauss(profile.talk_mean_seconds, profile.talk_mean_seconds * 0.35))
            push(at + timedelta(seconds=talk), "complete", (queue_id, agent_id, item, at, talk))

    # Jornada: todos los agentes inician sesión y quedan disponibles.
    for agents in free_agents.values():
        for agent_id in agents:
            emit_agent_state(agent_id, "logged_out", "logged_in", scenario.start, None)
            emit_agent_state(agent_id, "logged_in", "ready", scenario.start, None)

    # Llegadas: proceso de Poisson por cola, reproducible con la semilla.
    arrival_seq = itertools.count(1)
    for profile in scenario.queues:
        at = scenario.start
        mean_gap = 3600.0 / profile.arrivals_per_hour
        while True:
            at = at + timedelta(seconds=rng.expovariate(1.0 / mean_gap))
            if at >= end:
                break
            push(at, "arrival", (profile.queue_id, next(arrival_seq)))

    while heap:
        at, _, kind, data = heapq.heappop(heap)

        if kind == "arrival":
            queue_id, index = data
            profile = profiles[queue_id]
            contact_id = f"ct-{event_id_for(queue_id, str(index))[:12]}"
            channel = rng.choices(
                [c for c, _ in CHANNEL_WEIGHTS], weights=[w for _, w in CHANNEL_WEIGHTS]
            )[0]
            priority = rng.choices(
                [p for p, _ in PRIORITY_WEIGHTS], weights=[w for _, w in PRIORITY_WEIGHTS]
            )[0]
            record(
                contacts,
                build_event(
                    event_type="contact_queued",
                    event_time=at,
                    key=contact_id,
                    payload={
                        "contact_id": contact_id,
                        "queue_id": queue_id,
                        "channel": channel,
                        "priority": priority,
                        "skill_required": profile.skill,
                    },
                    event_id=event_id_for(contact_id, "contact_queued"),
                )
            )
            item = _Waiting(contact_id, queue_id, channel, at)
            waiting[queue_id].append(item)
            waiting_by_id[contact_id] = item
            push(at + timedelta(seconds=profile.patience_seconds), "abandon", contact_id)
            assign(queue_id, at)

        elif kind == "abandon":
            item = waiting_by_id.pop(data, None)
            if item is None:
                continue  # ya fue atendido: el temporizador de paciencia caduca sin efecto
            waiting[item.queue_id].remove(item)
            wait_seconds = (at - item.enqueued_at).total_seconds()
            record(
                contacts,
                build_event(
                    event_type="contact_ended",
                    event_time=at,
                    key=item.contact_id,
                    payload={
                        "contact_id": item.contact_id,
                        "queue_id": item.queue_id,
                        "channel": item.channel,
                        "disposition": "abandoned",
                        "wait_seconds": round(wait_seconds, 3),
                        "talk_seconds": 0.0,
                    },
                    event_id=event_id_for(item.contact_id, "contact_ended"),
                )
            )

        elif kind == "complete":
            queue_id, agent_id, item, connected_at, talk = data
            disposition = "transferred" if rng.random() < TRANSFER_PROBABILITY else "resolved"
            wait_seconds = (connected_at - item.enqueued_at).total_seconds()
            record(
                contacts,
                build_event(
                    event_type="contact_ended",
                    event_time=at,
                    key=item.contact_id,
                    payload={
                        "contact_id": item.contact_id,
                        "queue_id": queue_id,
                        "channel": item.channel,
                        "agent_id": agent_id,
                        "disposition": disposition,
                        "wait_seconds": round(wait_seconds, 3),
                        "talk_seconds": round(talk, 3),
                    },
                    event_id=event_id_for(item.contact_id, "contact_ended"),
                )
            )
            emit_agent_state(agent_id, "busy", "wrapup", at, item.contact_id)
            push(
                at + timedelta(seconds=profiles[queue_id].wrapup_seconds),
                "wrapup_done",
                (queue_id, agent_id),
            )

        elif kind == "wrapup_done":
            queue_id, agent_id = data
            emit_agent_state(agent_id, "wrapup", "ready", at, None)
            free_agents[queue_id].append(agent_id)
            assign(queue_id, at)

    return SimulationResult(
        contacts=contacts,
        agent_states=agent_states,
        timeline=timeline,
        queues=profiles,
    )


def describe(scenario: Scenario) -> dict[str, Any]:
    """Resumen legible de los parámetros antes de ejecutar."""
    return {
        "inicio": iso_utc(scenario.start),
        "duracion_minutos": scenario.duration_minutes,
        "semilla": scenario.seed,
        "agentes": sum(p.agents for p in scenario.queues),
        "colas": [
            {
                "queue_id": p.queue_id,
                "agentes": p.agents,
                "llegadas_por_hora": p.arrivals_per_hour,
            }
            for p in scenario.queues
        ],
    }
