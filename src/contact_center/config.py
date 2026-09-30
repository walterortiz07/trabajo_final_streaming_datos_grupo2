"""Configuración compartida por productor, pipeline, consumidor y pruebas."""

from __future__ import annotations

import os
from dataclasses import dataclass

# Los nombres de tópico son parte del diseño. El sufijo .v1 fija la versión del
# contrato de eventos.
CONTACTS_TOPIC = "cc.contacts.raw.v1"
AGENT_STATE_TOPIC = "cc.agent-state.raw.v1"
METRICS_TOPIC = "cc.queue-metrics.1m.v1"
DLQ_TOPIC = "cc.events.dlq.v1"


@dataclass(frozen=True)
class Settings:
    """Ajustes de ejecución con valores por defecto aptos para Docker Compose."""

    kafka_bootstrap_servers: str = "kafka:9092"
    contacts_topic: str = CONTACTS_TOPIC
    agent_state_topic: str = AGENT_STATE_TOPIC
    metrics_topic: str = METRICS_TOPIC
    dlq_topic: str = DLQ_TOPIC

    # --- Política temporal -------------------------------------------------
    window_seconds: int = 60
    allowed_lateness_seconds: int = 300
    # 0 = sin emisiones tempranas. El `DirectRunner` no tiene reloj de
    # processing time, así que el disparador temprano falla con
    # `AttributeError: 'NoneType' object has no attribute 'time'`. Con un
    # runner portable (Flink) poner 30 y las emisiones tempranas se activan.
    early_firing_seconds: int = 0
    service_level_seconds: int = 20

    # --- Ejecución ---------------------------------------------------------
    runner: str = "DirectRunner"
    parallelism: int = 2
    job_name: str = "cc-queue-metrics"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            contacts_topic=os.getenv("KAFKA_CONTACTS_TOPIC", CONTACTS_TOPIC),
            agent_state_topic=os.getenv("KAFKA_AGENT_STATE_TOPIC", AGENT_STATE_TOPIC),
            metrics_topic=os.getenv("KAFKA_METRICS_TOPIC", METRICS_TOPIC),
            dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", DLQ_TOPIC),
            window_seconds=int(os.getenv("WINDOW_SECONDS", "60")),
            allowed_lateness_seconds=int(os.getenv("ALLOWED_LATENESS_SECONDS", "300")),
            early_firing_seconds=int(os.getenv("EARLY_FIRING_SECONDS", "0")),
            service_level_seconds=int(os.getenv("SERVICE_LEVEL_SECONDS", "20")),
            runner=os.getenv("BEAM_RUNNER", "DirectRunner"),
            parallelism=int(os.getenv("BEAM_PARALLELISM", "2")),
            job_name=os.getenv("BEAM_JOB_NAME", "cc-queue-metrics"),
        )

    def topic_for_event_type(self, event_type: str) -> str:
        """Enrutar cada tipo de evento al tópico de entrada que le corresponde."""
        if event_type == "agent_state_changed":
            return self.agent_state_topic
        return self.contacts_topic
