.PHONY: help up down topics producer pipeline metrics dlq offsets demo smoke test lint check clean sync

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

sync: ## Instala las dependencias con uv
	uv sync

up: ## Levanta Kafka y crea los cuatro tópicos del diseño
	@echo "== Levantando Kafka =="
	@docker compose up -d kafka
	@printf "   esperando que arranque"
	@until docker compose ps kafka --format '{{.Health}}' | grep -q healthy; do printf "."; sleep 1; done
	@echo " listo"
	@echo ""
	@echo "== Creando los cuatro tópicos =="
	@docker compose up kafka-init 2>&1 | grep "PartitionCount" | sed "s/^[^|]*| //" || true
	@echo ""
	@echo "== Tópicos disponibles =="
	@docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --list | grep -v consumer_offsets | sed 's/^/   /'

down: ## Detiene el stack conservando el log de Kafka
	docker compose down

clean: ## Detiene el stack y borra el volumen de Kafka
	docker compose down --volumes

topics: ## Muestra particiones y retención efectivas de cada tópico
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --describe

producer: ## Publica una jornada simulada (duplicados, desorden y eventos inválidos)
	docker compose run --rm -T producer

pipeline: ## Ejecuta el pipeline Beam contra Kafka (corre en el host, ver README)
	KAFKA_BOOTSTRAP_SERVERS=localhost:29092 AWS_REGION=us-east-1 \
		uv run python -m contact_center.pipeline

metrics: ## Materializa el changelog y muestra la tabla de métricas
	KAFKA_BOOTSTRAP_SERVERS=localhost:29092 uv run python -m contact_center.consumer

dlq: ## Muestra los eventos apartados por violar el contrato
	docker compose run --rm -T dashboard --seconds 12

offsets: ## Offsets y lag de los grupos de consumo
	docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 --all-groups --describe

demo: ## Recorrido end-to-end completo, con evidencia en evidence/
	bash scripts/demo.sh

smoke: ## Prueba de humo real contra el stack (tópicos efímeros propios)
	uv run python scripts/smoke.py

test: ## Pruebas unitarias y de ventanas
	uv run pytest

lint: ## Linter
	uv run ruff check .

check: lint test ## Linter y pruebas
	docker compose config --quiet
