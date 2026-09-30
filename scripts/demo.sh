#!/usr/bin/env bash
# Recorrido end-to-end del proyecto: fuente -> Kafka -> Beam -> changelog -> vista.
# Deja toda la evidencia en evidence/ para que sea verificable sin volver a correrlo.
set -euo pipefail

cd "$(dirname "$0")/.."
EVIDENCE="evidence"
COMPOSE="docker compose"
mkdir -p "$EVIDENCE"

paso() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# `docker compose run` escribe el estado de los contenedores en stderr y el
# harness Java de Beam emite trazas multilínea. Se filtran para que la evidencia
# quede limpia y sea parseable.
limpiar() {
  grep -vE '^ (Container|Network|Volume) |^%[0-9]+\||EC2|DOCTYPE|top\.location|^\s*$' || true
}

paso "1/8  Partiendo de un estado limpio y levantando Kafka"
# El demo regenera la evidencia desde cero. Sin este borrado, una segunda
# corrida reutilizaría los offsets ya confirmados por los grupos de consumo y
# los eventos viejos que siguen en los tópicos, y los resultados no serían
# comparables con los de la primera.
$COMPOSE down --volumes >/dev/null 2>&1 || true
$COMPOSE up -d kafka kafka-init
$COMPOSE wait kafka-init >/dev/null 2>&1 || true
until $COMPOSE ps kafka --format '{{.Health}}' | grep -q healthy; do sleep 1; done

paso "2/8  Contrato de tópicos: particiones y retención efectivas"
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe | tee "$EVIDENCE/01-topicos.txt"

paso "3/8  Replay de la jornada simulada (duplicados, desorden e inválidos)"
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 uv run python -m contact_center.producer \
  --duration-minutes 20 --speedup 1000000 --no-realtime --duplicate-rate 0.03 \
  --tardiness-seconds 45 --invalid-rate 0.01 2>&1 | limpiar > "$EVIDENCE/02-productor.json"
EVENTOS=$(python3 -c "
import json,re,pathlib
t = pathlib.Path('$EVIDENCE/02-productor.json').read_text()
print(json.loads(re.search(r'\{.*\}', t, re.S).group(0))['events'])
")
echo "  registros publicados: $EVENTOS"
head -12 "$EVIDENCE/02-productor.json"

paso "4/8  Pipeline Beam: KafkaIO -> validación -> ventanas -> dedup -> Kafka"
echo "  (el arranque incluye el expansion service Java de KafkaIO; puede tardar)"
# El pipeline corre en el host, no dentro de un contenedor. KafkaIO expande
# etapas Java y el SDK de Python solo sabe lanzarlas como contenedor: necesita
# un daemon de Docker, que el host tiene y un contenedor no. Kafka sí queda en
# Docker porque es infraestructura.
export KAFKA_BOOTSTRAP_SERVERS=localhost:29092
export AWS_REGION=us-east-1
uv run python -m contact_center.pipeline --max-num-records "$EVENTOS" 2>&1 | limpiar \
  > "$EVIDENCE/03-pipeline.txt"
tail -2 "$EVIDENCE/03-pipeline.txt"

paso "5/8  Materialización del changelog: upsert por clave lógica"
uv run python -m contact_center.consumer --seconds 25 2>&1 | limpiar \
  > "$EVIDENCE/04-metricas-materializadas.txt"
head -22 "$EVIDENCE/04-metricas-materializadas.txt"

paso "6/8  Eventos apartados por violar el contrato"
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic cc.events.dlq.v1 \
  --from-beginning --timeout-ms 6000 --max-messages 3 2>/dev/null \
  | head -3 | tee "$EVIDENCE/05-dlq.txt"

paso "7/8  Offsets y lag de los grupos de consumo"
# Se escribe a archivo y recién después se recorta: si `head` cerrara la
# tubería antes de tiempo, el SIGPIPE abortaría el script.
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --all-groups --describe \
  > "$EVIDENCE/06-offsets.txt"
head -18 "$EVIDENCE/06-offsets.txt"

paso "8/8  Reproducibilidad: reprocesar converge a la misma vista"
# Se rebobina el grupo del tablero sobre los agregados retenidos: la vista
# materializada debe quedar idéntica porque el upsert reemplaza por clave.
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group cc-dashboard-operacion-v1 \
  --topic cc.queue-metrics.1m.v1 --reset-offsets --to-earliest --execute \
  > "$EVIDENCE/07-replay-reset.txt"
head -6 "$EVIDENCE/07-replay-reset.txt"
uv run python -m contact_center.consumer --seconds 20 2>&1 | limpiar \
  > "$EVIDENCE/08-replay-resultado.txt"
tail -8 "$EVIDENCE/08-replay-resultado.txt"

paso "Demostración completada. Evidencia en ./$EVIDENCE/"
