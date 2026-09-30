"""Materializar el changelog de agregados con upsert por clave lógica.

El pipeline publica una **secuencia de revisiones** por cada agregado: panes
tempranos, uno a tiempo y correcciones tardías. El consumidor no las suma:
conserva la revisión más reciente de cada `aggregate_id`. Si el pipeline vuelve
a emitir una ventana —porque llegó un evento tardío o porque se reprocesó el
histórico— el resultado converge al mismo estado.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass, field
from threading import Event, Timer
from typing import Any

from confluent_kafka import Consumer, KafkaError

from contact_center.config import Settings


@dataclass
class AggregateStore:
    """Vista materializada del changelog, indexada por clave lógica."""

    records: dict[str, dict[str, Any]] = field(default_factory=dict)
    messages_seen: int = 0
    revisions_replaced: int = 0
    out_of_order_ignored: int = 0

    def upsert(self, aggregate: dict[str, Any]) -> bool:
        self.messages_seen += 1
        key = aggregate["aggregate_id"]
        current = self.records.get(key)

        if current is not None:
            if int(current.get("pane_index", -1)) > int(aggregate.get("pane_index", -1)):
                # Una revisión más vieja llegó después: se ignora en lugar de
                # retroceder la vista.
                self.out_of_order_ignored += 1
                return False
            self.revisions_replaced += 1

        self.records[key] = aggregate
        return True

    def rows(self) -> list[dict[str, Any]]:
        return sorted(
            self.records.values(),
            key=lambda row: (row["window_start"], row["aggregate_id"]),
        )

    def summary(self) -> dict[str, int]:
        return {
            "mensajes": self.messages_seen,
            "agregados": len(self.records),
            "revisiones_reemplazadas": self.revisions_replaced,
            "revisiones_fuera_de_orden": self.out_of_order_ignored,
        }


def build_consumer(
    settings: Settings,
    *,
    group_id: str = "cc-dashboard-operacion-v1",
    offset_reset: str = "earliest",
) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([settings.metrics_topic])
    return consumer


def poll_into_store(
    consumer: Consumer,
    store: AggregateStore,
    *,
    max_messages: int = 500,
    timeout_seconds: float = 1.0,
) -> int:
    accepted = 0
    for message in consumer.consume(num_messages=max_messages, timeout=timeout_seconds):
        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                continue
            continue
        try:
            aggregate = json.loads((message.value() or b"").decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        accepted += int(store.upsert(aggregate))
    return accepted


HEADERS = (
    "ventana",
    "metrica",
    "dimension",
    "pane",
    "ofrec",
    "atend",
    "aband",
    "NS20",
    "e_prom",
    "e_p90",
    "trans",
)


def render_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "  (sin agregados)"
    body = []
    for row in rows:
        body.append(
            (
                row["window_start"][11:19],
                row["metric_type"].replace("_", " ")[:12],
                str(row["dimension_id"])[:12],
                str(row.get("pane_index", "")),
                str(row.get("offered", "")),
                str(row.get("connected", "")),
                str(row.get("abandoned", "")),
                f"{row['ns20']:.2f}" if "ns20" in row else "",
                f"{row['avg_wait_seconds']:.1f}" if "avg_wait_seconds" in row else "",
                f"{row['p90_wait_seconds']:.1f}" if "p90_wait_seconds" in row else "",
                str(row.get("transitions", "")),
            )
        )
    widths = [max(len(HEADERS[i]), *(len(r[i]) for r in body)) for i in range(len(HEADERS))]
    lines = [
        "  " + "  ".join(h.ljust(widths[i]) for i, h in enumerate(HEADERS)),
        "  " + "  ".join("-" * w for w in widths),
    ]
    lines += ["  " + "  ".join(c.ljust(widths[i]) for i, c in enumerate(r)) for r in body]
    return "\n".join(lines)


def main() -> None:
    settings = Settings.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group-id", default="cc-dashboard-operacion-v1")
    parser.add_argument("--seconds", type=float, default=15.0)
    parser.add_argument("--settle-seconds", type=float, default=3.0)
    args = parser.parse_args()

    consumer = build_consumer(settings, group_id=args.group_id)
    stop = Event()
    Timer(args.seconds, stop.set).start()

    store = AggregateStore()
    last_message_at = time.monotonic()
    while not stop.is_set():
        accepted = poll_into_store(consumer, store)
        if accepted:
            last_message_at = time.monotonic()
        elif time.monotonic() - last_message_at > args.settle_seconds and store.records:
            break
    consumer.close()

    print(render_table(store.rows()))
    print(json.dumps(store.summary(), indent=2))


if __name__ == "__main__":
    main()
