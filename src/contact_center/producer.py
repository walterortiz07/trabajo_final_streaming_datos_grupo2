"""Productor: publica una jornada simulada de contact center en Kafka.

Publica cada evento con la clave de negocio como clave del record y el
`event_time` del contrato como timestamp del record, de modo que Kafka conserve
el orden por entidad y el tiempo de evento quede disponible para los consumidores.
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

# `Event` ya nombra al evento del dominio; la señal de parada se importa aparte.
from threading import Event as StopSignal

from confluent_kafka import Producer

from contact_center.config import Settings
from contact_center.contracts import (
    REQUIRED_PAYLOAD_FIELDS,
    Event,
    encode_event,
    iso_utc,
    parse_iso_utc,
)
from contact_center.simulator import Scenario, describe, simulate


class DeliveryTracker:
    """Cuenta entregas confirmadas por el broker y errores reportados."""

    def __init__(self) -> None:
        self.acknowledged = 0
        self.failed = 0
        self.last_error: str | None = None

    def __call__(self, error, _message) -> None:
        if error is not None:
            self.failed += 1
            self.last_error = str(error)
        else:
            self.acknowledged += 1


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "cc-contact-producer",
            # Idempotencia del productor: evita duplicados por reintentos internos
            # dentro de una sesión. No cubre los duplicados que genera el origen.
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "snappy",
            "linger.ms": 20,
        }
    )


@dataclass(frozen=True)
class _Delivery:
    """Un evento junto al instante en que debe entregarse."""

    deliver_at: datetime
    event: Event


class ContactCenterReplay:
    """Reproductor controlable y determinista de una jornada simulada."""

    def __init__(
        self,
        producer: Producer,
        *,
        settings: Settings,
        speedup: float = 120.0,
        duplicate_rate: float = 0.02,
        tardiness_seconds: float = 45.0,
        invalid_rate: float = 0.0,
        seed: int = 7,
        tracker: DeliveryTracker | None = None,
    ) -> None:
        if speedup <= 0:
            raise ValueError("speedup debe ser positivo")
        if tardiness_seconds < 0:
            raise ValueError("tardiness_seconds no puede ser negativo")
        self.producer = producer
        self.settings = settings
        self.speedup = speedup
        self.duplicate_rate = duplicate_rate
        self.tardiness_seconds = tardiness_seconds
        self.invalid_rate = invalid_rate
        self.random = random.Random(seed)
        self.tracker = tracker or DeliveryTracker()
        self.stop_event = StopSignal()
        self.sent = 0
        self.duplicates = 0
        self.invalid = 0

    def stop(self) -> None:
        self.stop_event.set()

    def _produce(self, event: Event) -> None:
        topic = self.settings.topic_for_event_type(event.event_type)
        timestamp_ms = int(parse_iso_utc(event.event_time).timestamp() * 1000)
        self.producer.produce(
            topic,
            key=event.key.encode(),
            value=encode_event(event),
            timestamp=timestamp_ms,
            headers={
                "event_type": event.event_type,
                "schema_version": str(event.schema_version),
            },
            on_delivery=self.tracker,
        )
        self.producer.poll(0)
        self.sent += 1

    @staticmethod
    def _corrupt(event: Event) -> Event:
        """Omite un campo obligatorio del payload para violar el contrato a propósito.

        El campo se elige del contrato según el tipo de evento, así que cualquier
        tipo se puede volver inválido de forma reproducible.
        """
        payload = dict(event.payload)
        for name in REQUIRED_PAYLOAD_FIELDS[event.event_type]:
            if name != "contact_id" and name in payload:
                del payload[name]
                return replace(event, payload=payload)
        raise ValueError(f"no hay campos corrompibles en {event.event_type}")

    @staticmethod
    def _shift(event: Event, *, source_start: datetime, target_start: datetime) -> Event:
        """Desplaza el tiempo de evento para que la jornada arranque en `target_start`."""
        offset = parse_iso_utc(event.event_time) - source_start
        return replace(event, event_time=iso_utc(target_start + offset))

    def _schedule(self, events: list[Event], *, target_start: datetime) -> list[_Delivery]:
        """Ordena por instante de entrega, no por tiempo de evento.

        Con `tardiness_seconds > 0` un evento puede entregarse después de otro con
        un `event_time` posterior. Eso produce el desorden que el consumidor debe
        tolerar.
        """
        source_start = parse_iso_utc(events[0].event_time)
        deliveries: list[_Delivery] = []
        for event in events:
            shifted = self._shift(event, source_start=source_start, target_start=target_start)
            delay = (
                self.random.uniform(0.0, self.tardiness_seconds)
                if self.tardiness_seconds
                else 0.0
            )
            deliveries.append(
                _Delivery(
                    deliver_at=parse_iso_utc(shifted.event_time) + timedelta(seconds=delay),
                    event=shifted,
                )
            )
        deliveries.sort(key=lambda item: (item.deliver_at, item.event.event_id))
        return deliveries

    def run_schedule(
        self, events: list[Event], *, target_start: datetime | None = None
    ) -> list[_Delivery]:
        """Calcula el orden de entrega sin publicar nada. Sirve para inspección y pruebas."""
        if not events:
            return []
        return self._schedule(
            events, target_start=target_start or datetime.now(UTC).replace(microsecond=0)
        )

    def run(
        self,
        events: list[Event],
        *,
        realtime: bool = True,
        target_start: datetime | None = None,
    ) -> dict[str, int]:
        if not events:
            return {"events": 0, "duplicates": 0, "invalid": 0, "disordered": 0}

        target_start = target_start or datetime.now(UTC).replace(microsecond=0)
        deliveries = self._schedule(events, target_start=target_start)
        disordered = sum(
            1
            for previous, current in zip(deliveries, deliveries[1:], strict=False)
            if parse_iso_utc(current.event.event_time) < parse_iso_utc(previous.event.event_time)
        )

        wall_start = time.monotonic()
        for delivery in deliveries:
            if self.stop_event.is_set():
                break
            logical = (delivery.deliver_at - target_start).total_seconds()
            due = wall_start + logical / self.speedup
            if realtime:
                while not self.stop_event.is_set() and (remaining := due - time.monotonic()) > 0:
                    time.sleep(min(remaining, 0.1))
            self._produce(delivery.event)
            if self.random.random() < self.duplicate_rate:
                # Duplicado lógico: mismo event_id, misma clave, misma partición.
                self._produce(delivery.event)
                self.duplicates += 1
            if self.random.random() < self.invalid_rate:
                # Evento fuera de contrato: el consumidor debe apartarlo al DLQ.
                self._produce(self._corrupt(delivery.event))
                self.invalid += 1

        self.producer.flush(15)
        return {
            "events": self.sent,
            "duplicates": self.duplicates,
            "invalid": self.invalid,
            "disordered": disordered,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-minutes", type=int, default=30)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--speedup",
        type=float,
        default=120.0,
        help="factor de aceleración del reloj de pared (120 = 1 min de jornada por 0,5 s)",
    )
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument(
        "--invalid-rate",
        type=float,
        default=0.0,
        help="proporción de eventos fuera de contrato, para ejercitar el DLQ",
    )
    parser.add_argument(
        "--tardiness-seconds",
        type=float,
        default=45.0,
        help="retraso máximo de transporte; genera desorden respecto al tiempo de evento",
    )
    parser.add_argument(
        "--start", type=str, default=None, help="inicio ISO-8601; por defecto, ahora"
    )
    parser.add_argument("--no-realtime", action="store_true", help="publica lo más rápido posible")
    return parser


def main() -> None:
    settings = Settings.from_env()
    args = build_parser().parse_args()

    start = parse_iso_utc(args.start) if args.start else datetime.now(UTC).replace(microsecond=0)
    scenario = Scenario(start=start, duration_minutes=args.duration_minutes, seed=args.seed)
    result = simulate(scenario)

    replay = ContactCenterReplay(
        build_producer(settings.kafka_bootstrap_servers),
        settings=settings,
        speedup=args.speedup,
        duplicate_rate=args.duplicate_rate,
        tardiness_seconds=args.tardiness_seconds,
        invalid_rate=args.invalid_rate,
        seed=args.seed,
    )
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: replay.stop())

    outcome = replay.run(result.all_events, realtime=not args.no_realtime)
    outcome["acknowledged"] = replay.tracker.acknowledged
    outcome["failed"] = replay.tracker.failed
    outcome["scenario"] = describe(scenario)
    outcome["generated"] = result.summary()
    print(json.dumps(outcome, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
