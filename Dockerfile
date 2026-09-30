# syntax=docker/dockerfile:1

# KafkaIO de Beam es cross-language: la fuente y el sumidero son etapas Java que
# el *expansion service* incorpora al grafo Python. Esas etapas necesitan un
# harness de Java. Se copia desde la imagen oficial del SDK de Beam para que el
# harness corra como **proceso** dentro de este contenedor, en lugar de lanzar
# otro contenedor: el pipeline no depende de un daemon de Docker.
FROM apache/beam_java21_sdk:2.74.0 AS beam_java

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    HOME=/app \
    JAVA_HOME=/opt/java/openjdk \
    BEAM_JAVA_HARNESS_BOOT=/opt/apache/beam/boot \
    # El harness Java sondea el servicio de metadatos de EC2 al registrar S3.
    # Fuera de AWS eso devuelve HTML y ensucia los logs; fijar la región lo evita.
    AWS_REGION=us-east-1

COPY --from=beam_java /opt/apache/beam /opt/apache/beam
COPY --from=beam_java /opt/java/openjdk /opt/java/openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app
COPY pyproject.toml README.md ./
RUN uv sync --no-dev --no-install-project

COPY src ./src
COPY scripts ./scripts
RUN uv sync --no-dev

# Pre-descarga el JAR del expansion service para que el primer arranque no
# dependa de Maven ni de la red.
RUN /app/.venv/bin/python -c "\
from apache_beam.io.kafka import default_io_expansion_service; \
svc = default_io_expansion_service(); \
addr = svc.__enter__(); \
print('expansion service listo en', addr); \
svc.__exit__(None, None, None)"

RUN useradd --create-home --uid 10001 student \
    && mkdir -p /app/evidence \
    && chown -R student:student /app
USER student

CMD ["/app/.venv/bin/python", "-m", "contact_center.consumer"]
