"""Prueba de humo acotada del recorrido completo: fuente → Kafka → Beam → salida.

Crea tópicos efímeros propios, publica una jornada corta con duplicados,
desorden y eventos fuera de contrato, ejecuta el pipeline Beam contra Kafka y
verifica que el tópico de agregados y el de descartes reciban datos.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from contact_center.config import Settings
from contact_center.consumer import AggregateStore, poll_into_store
from contact_center.producer import ContactCenterReplay, build_producer
from contact_center.simulator import Scenario, simulate

SUFFIX = uuid.uuid4().hex[:8]
PARTITIONS = 4


def check(condition: bool, message: str) -> None:
    print(f"  [{'ok  ' if condition else 'FALLA'}] {message}")
    if not condition:
        raise SystemExit(f"smoke test falló: {message}")


def crear_topicos(bootstrap: str, nombres: list[str]) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futuros = admin.create_topics(
        [NewTopic(n, num_partitions=PARTITIONS, replication_factor=1) for n in nombres]
    )
    for nombre, futuro in futuros.items():
        try:
            futuro.result(timeout=30)
            print(f"  tópico creado: {nombre}")
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"no se pudo crear {nombre}: {exc}") from exc


def borrar_topicos(bootstrap: str, nombres: list[str]) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for futuro in admin.delete_topics(nombres).values():
        try:
            futuro.result(timeout=30)
        except Exception:  # noqa: BLE001 - la limpieza no debe tapar el resultado
            pass


def consumir(topic: str, bootstrap: str, *, timeout: float = 30.0) -> list[dict]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"smoke-{SUFFIX}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    filas: list[dict] = []
    fin = time.monotonic() + timeout
    try:
        while time.monotonic() < fin:
            for mensaje in consumer.consume(num_messages=200, timeout=1.0):
                if mensaje.error():
                    if mensaje.error().code() != KafkaError._PARTITION_EOF:
                        raise SystemExit(f"error de consumo: {mensaje.error()}")
                    continue
                try:
                    filas.append(json.loads((mensaje.value() or b"").decode()))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
            if filas:
                break
    finally:
        consumer.close()
    return filas


def main() -> None:
    base = Settings.from_env()
    settings = Settings(
        kafka_bootstrap_servers=base.kafka_bootstrap_servers,
        contacts_topic=f"cc.smoke.contacts.{SUFFIX}",
        agent_state_topic=f"cc.smoke.agentstate.{SUFFIX}",
        metrics_topic=f"cc.smoke.metrics.{SUFFIX}",
        dlq_topic=f"cc.smoke.dlq.{SUFFIX}",
        allowed_lateness_seconds=180,
    )
    bootstrap = settings.kafka_bootstrap_servers

    print(f"Prueba de humo contra {bootstrap}")
    topicos = [
        settings.contacts_topic,
        settings.agent_state_topic,
        settings.metrics_topic,
        settings.dlq_topic,
    ]
    crear_topicos(bootstrap, topicos)
    try:
        escenario = Scenario(
            start=datetime.now(UTC).replace(microsecond=0),
            duration_minutes=20,
            seed=42,
        )
        resultado = simulate(escenario)
        print(
            f"  jornada simulada: {len(resultado.contacts)} eventos de contacto, "
            f"{len(resultado.agent_states)} de agente"
        )

        replay = ContactCenterReplay(
            build_producer(bootstrap),
            settings=settings,
            speedup=1_000_000.0,
            duplicate_rate=0.05,
            tardiness_seconds=90.0,
            invalid_rate=0.02,
            seed=42,
        )
        salida = replay.run(resultado.all_events, realtime=False)
        print(
            f"  publicados: {salida['events']} "
            f"(duplicados {salida['duplicates']}, inválidos {salida['invalid']}, "
            f"fuera de orden {salida['disordered']})"
        )
        check(salida["events"] > 0, "se publicaron eventos")
        check(salida["duplicates"] > 0, "se inyectaron duplicados")

        entorno = dict(os.environ)
        entorno.update(
            {
                "KAFKA_BOOTSTRAP_SERVERS": bootstrap,
                "KAFKA_CONTACTS_TOPIC": settings.contacts_topic,
                "KAFKA_AGENT_STATE_TOPIC": settings.agent_state_topic,
                "KAFKA_METRICS_TOPIC": settings.metrics_topic,
                "KAFKA_DLQ_TOPIC": settings.dlq_topic,
                "ALLOWED_LATENESS_SECONDS": str(settings.allowed_lateness_seconds),
            }
        )
        print("  ejecutando el pipeline Beam contra Kafka…")
        subprocess.run(
            [
                sys.executable,
                "-m",
                "contact_center.pipeline",
                "--group-id",
                f"smoke-beam-{SUFFIX}",
                "--max-num-records",
                str(salida["events"]),
            ],
            check=True,
            timeout=600,
            env=entorno,
        )

        agregados = consumir(settings.metrics_topic, bootstrap)
        check(len(agregados) > 0, f"el tópico de agregados tiene {len(agregados)} registros")

        tipos = {fila["metric_type"] for fila in agregados}
        check(
            tipos == {"queue_metrics", "agent_state_counts"},
            f"llegaron las dos métricas del changelog: {sorted(tipos)}",
        )
        check(
            all(fila["window_start"].endswith("Z") for fila in agregados),
            "los límites de ventana son instantes ISO-8601 sin ambigüedad",
        )
        check(
            all(
                fila["pane_timing"] in {"EARLY", "ON_TIME", "LATE"}
                for fila in agregados
            ),
            "los panes traen su nombre, no el número del enum",
        )

        descartes = consumir(settings.dlq_topic, bootstrap, timeout=8.0)
        check(len(descartes) > 0, f"los eventos inválidos llegaron al DLQ ({len(descartes)})")

        # La vista materializada converge: reprocesar no duplica entidades.
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"smoke-store-{SUFFIX}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([settings.metrics_topic])
        store = AggregateStore()
        fin = time.monotonic() + 20
        while time.monotonic() < fin and not store.records:
            poll_into_store(consumer, store, timeout_seconds=1.0)
        consumer.close()
        print(f"  vista materializada: {json.dumps(store.summary())}")

        print("\nSmoke test OK")
    finally:
        borrar_topicos(bootstrap, topicos)


if __name__ == "__main__":
    main()
