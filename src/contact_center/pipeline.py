"""Pipeline Kafka → Beam → Kafka para la analítica del contact center.

Lee los eventos crudos de Kafka con `KafkaIO`, valida el contrato, aplica
ventanas de tiempo de evento, deduplica por `event_id` con estado y timer, y
publica dos agregados en un tópico de cambios.

`KafkaIO` para Python es *cross-language*: la fuente y el sumidero son
transformaciones Java que el *expansion service* incorpora al grafo Python. Eso
funciona igual con `DirectRunner` que con un runner portable, así que el
laboratorio no necesita un clúster para demostrar el recorrido completo.
"""

from __future__ import annotations

import argparse
import json
import os

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka, default_io_expansion_service
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints import KV

from contact_center.config import Settings
from contact_center.transforms import (
    ParseEvent,
    aggregate_to_kafka_record,
    build_analytics,
    invalid_to_kafka_record,
)

# El boot loader del harness Java del SDK de Beam. La imagen lo copia desde
# `apache/beam_java21_sdk`; fuera del contenedor no existe.
BEAM_JAVA_HARNESS_BOOT = "/opt/apache/beam/boot"


def kafka_io_expansion_service():
    """Servicio que expande las etapas Java de KafkaIO dentro del grafo Python.

    Dentro del contenedor el harness Java se ejecuta como **proceso**, apuntando
    al boot loader copiado en la imagen. Fuera de él se usa el entorno por
    defecto, que lanza el harness en un contenedor de Beam: eso funciona en una
    máquina con daemon de Docker, que es el caso de `make smoke` en el host.
    """
    if os.path.exists(BEAM_JAVA_HARNESS_BOOT):
        return default_io_expansion_service(
            append_args=[
                "--defaultEnvironmentType=PROCESS",
                '--defaultEnvironmentConfig={"command":"/opt/apache/beam/boot"}',
            ]
        )
    return default_io_expansion_service()


def pipeline_options(settings: Settings, *, job_name: str) -> PipelineOptions:
    return PipelineOptions(
        [
            f"--runner={settings.runner}",
            "--streaming",
            f"--parallelism={settings.parallelism}",
            f"--job_name={job_name}",
        ]
    )


def build_pipeline(
    pipeline: beam.Pipeline,
    settings: Settings,
    *,
    group_id: str,
    max_num_records: int | None = None,
    max_read_time: int | None = None,
):
    """Construir el grafo. Devuelve `(agregados, inválidos)` sin ejecutarlo."""
    expansion_service = kafka_io_expansion_service()

    read_kwargs: dict[str, object] = {}
    if max_num_records is not None:
        read_kwargs["max_num_records"] = max_num_records
    if max_read_time is not None:
        read_kwargs["max_read_time"] = max_read_time

    raw = pipeline | "Leer eventos de Kafka" >> ReadFromKafka(
        consumer_config={
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",
        },
        topics=[settings.contacts_topic, settings.agent_state_topic],
        # La marca de tiempo inicial es la de escritura en Kafka; el tiempo de
        # dominio se asigna después, con `assign_event_timestamp`.
        timestamp_policy=ReadFromKafka.create_time_policy,
        expansion_service=expansion_service,
        **read_kwargs,
    )

    # La validación es una salida lateral: el evento inválido no interrumpe el
    # pipeline, pero deja de ser invisible.
    parsed = raw | "Validar contrato" >> beam.ParDo(ParseEvent()).with_outputs(
        ParseEvent.INVALID, main="valid"
    )

    (
        parsed.invalid
        # El type hint es obligatorio en los dos sumideros: `WriteToKafka` es
        # una etapa Java y el límite cross-language necesita un coder concreto.
        # Sin él, el expansion service falla con
        # `UnknownCoderWrapper cannot be cast to KvCoder`.
        | "Codificar inválidos"
        >> beam.Map(invalid_to_kafka_record).with_output_types(KV[bytes, bytes])
        | "Escribir descartes" >> WriteToKafka(
            producer_config={"bootstrap.servers": settings.kafka_bootstrap_servers},
            topic=settings.dlq_topic,
            expansion_service=expansion_service,
        )
    )

    aggregates = build_analytics(parsed.valid, settings)

    (
        aggregates
        | "Codificar agregados" >> beam.Map(aggregate_to_kafka_record).with_output_types(
            KV[bytes, bytes]
        )
        | "Escribir agregados" >> WriteToKafka(
            producer_config={
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "enable.idempotence": "true",
                "acks": "all",
            },
            topic=settings.metrics_topic,
            expansion_service=expansion_service,
        )
    )
    return aggregates, parsed.invalid


def run(
    *,
    group_id: str = "cc-beam-metrics-v1",
    max_num_records: int | None = None,
    max_read_time: int | None = None,
):
    settings = Settings.from_env()
    options = pipeline_options(settings, job_name=settings.job_name)
    pipeline = beam.Pipeline(options=options)
    build_pipeline(
        pipeline,
        settings,
        group_id=group_id,
        max_num_records=max_num_records,
        max_read_time=max_read_time,
    )
    result = pipeline.run()
    result.wait_until_finish()
    return result


def main() -> None:
    # El harness Java de Beam sondea el servicio de metadatos de EC2 al
    # registrar el sistema de archivos S3. Fuera de AWS eso devuelve HTML y
    # ensucia los logs; fijar la región evita el sondeo.
    os.environ.setdefault("AWS_REGION", "us-east-1")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group-id", default="cc-beam-metrics-v1")
    parser.add_argument(
        "--max-num-records",
        type=int,
        default=None,
        help="corta la lectura tras N registros",
    )
    parser.add_argument(
        "--max-read-time",
        type=int,
        default=None,
        help="corta la lectura tras N segundos; sirve cuando no se sabe cuántos registros hay",
    )
    args = parser.parse_args()
    result = run(
        group_id=args.group_id,
        max_num_records=args.max_num_records,
        max_read_time=args.max_read_time,
    )
    print(json.dumps({"estado": str(result.state)}))


if __name__ == "__main__":
    main()
