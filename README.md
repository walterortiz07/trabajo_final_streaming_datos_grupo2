# Proyecto integrador — Analítica de contact center en tiempo real

**Kafka + Apache Beam + tiempo de evento + estado e idempotencia**

Walter Gabriel Ortiz Medina

Proyecto integrador — 55%. *Streaming de datos y sus aplicaciones*,
Maestría en Inteligencia Artificial y Análisis de Datos,
Facultad Politécnica — Universidad Nacional de Asunción.
Docente: Rodrigo Parra, M.Sc.

Repositorio: <https://github.com/walterortiz07/trabajo_final_streaming_datos_grupo2>

---

## Qué hace

Un contact center sabe cómo estuvo la operación recién al día siguiente, cuando sale
el reporte del sistema de telefonía. El problema es que esa información pierde valor
en minutos: una cola que se satura a las 10:15 se corrige a las 10:20 o ya es una
llamada abandonada.

Este proyecto construye el recorrido completo —fuente, Kafka, Beam y salida
consumible— para que el supervisor vea la operación mientras está pasando, con
métricas por cola y por minuto calculadas sobre el **tiempo en que ocurrieron los
hechos**, no sobre el momento en que llegaron.

```text
  Simulador de jornada          Kafka                  Beam                  Kafka              Vista
  ────────────────────    ────────────────    ─────────────────────    ────────────────    ──────────
  replay acelerado    →   cc.contacts.raw →   validar · ventanear  →   cc.queue-metrics →  upsert por
  con duplicados,         cc.agent-state       deduplicar · combinar    1m.v1              clave lógica
  desorden y retrasos                          (KafkaIO, DirectRunner)  cc.events.dlq
```

---

## Prerrequisitos

- **Docker Engine o Docker Desktop** con Compose v2 — se usa para Kafka.
- **[uv](https://docs.astral.sh/uv/)** — para el pipeline y los consumidores.
- **Java 17 o superior** — `KafkaIO` de Beam no tiene implementación nativa en
  Python: la fuente y el sumidero son etapas Java que el *expansion service*
  incorpora al grafo. En Ubuntu: `sudo apt install openjdk-17-jre-headless`.
- **~2 GB de memoria libre** para Docker (solo Kafka; no se necesita clúster).
- Conexión a Internet la primera vez (imágenes de Kafka y Beam, y el JAR del
  *expansion service*).

---

## Ejecutar

```bash
make demo
```

Un solo comando: levanta Kafka, crea los cuatro tópicos, publica una jornada
simulada con duplicados, desorden y eventos fuera de contrato, ejecuta el pipeline
Beam, materializa el changelog y rebobina el consumidor para demostrar que el
reproceso converge. Deja toda la evidencia en `evidence/`.

### Paso a paso

```bash
make up          # levanta Kafka y crea los tópicos del diseño
make topics      # muestra particiones y retención efectivas

make producer    # publica una jornada simulada
make pipeline    # ejecuta el pipeline Beam contra Kafka
make metrics     # materializa el changelog y muestra la tabla
make dlq         # muestra los eventos apartados por violar el contrato
make offsets     # offsets y lag de los grupos de consumo

make down        # detiene el stack conservando el log
make clean       # además borra el volumen de Kafka
```

### Pruebas

```bash
make check       # linter + pruebas + validación del compose
make test        # 47 pruebas: contratos, transformaciones, ventanas y simulador
make smoke       # prueba de humo real: fuente → Kafka → Beam → salida
```

---

## Arquitectura

### Por qué el pipeline corre en el host y Kafka en Docker

`KafkaIO` de Python es *cross-language*: sus etapas son Java y el SDK de Python solo
sabe lanzarlas como **contenedor**. Eso exige un daemon de Docker, que el host tiene
y un contenedor no. Por eso Kafka —que es infraestructura— vive en Compose, y el
pipeline y los consumidores corren en el host con `uv`.

El `docker-compose.yml` solo declara lo que sí puede correr aislado: `kafka`,
`kafka-init` y `producer`. Es una limitación real del conector, no una preferencia, y
está documentada en [`docs/documento-tecnico.md`](docs/documento-tecnico.md).

### Tópicos y política temporal

Cuatro tópicos: dos de entrada (contactos y estado de agentes), uno derivado con las
métricas y uno de descarte. La entrada se particiona por `contact_id` y por
`agent_id`; el derivado, por `queue_id`. El detalle de cada uno, con particiones y
retención, está en [`docs/documento-tecnico.md`](docs/documento-tecnico.md).

Las decisiones temporales —ventana fija de 60 s sobre `event_time`, acumulación,
300 s de lateness y disparador por watermark— están justificadas en el mismo
documento, y las alternativas que se descartaron en
[`docs/decisiones.md`](docs/decisiones.md).

---

## Los dos agregados

El pipeline publica un único changelog con dos métricas, cada una con su clave:

**`queue_metrics`** — por cola y ventana, desde los eventos de contacto:
`offered`, `connected`, `abandoned`, `transferred`, `resolved`, `answered_within_sl`,
`ns20`, `abandon_rate`, `avg_wait_seconds`, `p90_wait_seconds`, `max_wait_seconds`,
`total_talk_seconds`.

**`agent_state_counts`** — transiciones por estado destino y ventana, desde los
eventos de agente: `transitions`.

`ns20` y `abandon_rate` son cocientes sobre una misma población, para no comparar
conjuntos distintos. La distribución de espera se toma de `contact_ended`, que es el
único evento que existe tanto para contactos atendidos como abandonados.

---

## Contrato de salida

```json
{
  "schema_version": 1,
  "aggregate_id": "queue_metrics|soporte-n1|2026-07-24T13:00:00Z",
  "metric_type": "queue_metrics",
  "dimension_id": "soporte-n1",
  "window_start": "2026-07-24T13:00:00Z",
  "window_end": "2026-07-24T13:01:00Z",
  "pane_index": 0,
  "pane_timing": "ON_TIME",
  "is_first": true,
  "is_last": false,
  "offered": 4, "connected": 4, "abandoned": 0,
  "ns20": 1.0, "avg_wait_seconds": 6.2, "p90_wait_seconds": 14.0
}
```

`pane_timing` viaja con **nombre** (`ON_TIME`), no con el número del enum, y
`window_start` termina en `Z`: sin la zona, `2026-07-24T13:00:00` no identifica un
instante. Ambos detalles se corrigen respecto del laboratorio de la clase 7, que
publica `"pane_timing": "1"` y un `window_start` sin sufijo.

---

## Estructura

```text
proyecto-final/
├── README.md                        este documento
├── docs/
│   ├── documento-tecnico.md         entregable: problema, arquitectura, contrato, límites
│   ├── decisiones.md                cada decisión con su alternativa descartada
│   └── integrantes.md               entregable: contribuciones del equipo
├── src/contact_center/
│   ├── config.py                    tópicos y parámetros
│   ├── contracts.py                 contrato de eventos: codificación y validación
│   ├── simulator.py                 generador determinista de la jornada
│   ├── producer.py                  replay a Kafka con duplicados y desorden
│   ├── transforms.py                transformaciones Beam: ventanas, estado, combiners
│   ├── pipeline.py                  KafkaIO → Beam → Kafka
│   ├── metrics.py                   oráculo en Python puro, contrastado contra el pipeline
│   └── consumer.py                  materialización con upsert por clave
├── tests/                           47 pruebas
├── scripts/
│   ├── demo.sh                      recorrido end-to-end con evidencia
│   └── smoke.py                     prueba de humo contra el stack real
├── docker-compose.yml               Kafka, creación de tópicos y productor
├── Dockerfile                       Python + harness Java de Beam
└── evidence/                        salida capturada de la ejecución
```

---

## Semántica de entrega

| Tramo | Garantía |
|---|---|
| Origen → Kafka | at-least-once (`enable.idempotence=true`, `acks=all`) |
| Kafka → Beam | at-least-once; el offset se confirma tras procesar |
| Deduplicación | por `event_id` con estado aislado por clave y ventana, expirado por timer |
| Publicación | upsert por `aggregate_id`: cada revisión reemplaza a la anterior |

**No se afirma exactly-once de punta a punta.** La solución es at-least-once con
deduplicación y un sumidero idempotente. Los límites están en el documento técnico.

---

## Evidencia

En `evidence/`, capturada con `make demo`:

| Archivo | Qué demuestra |
|---|---|
| `01-topicos.txt` | particiones y retención efectivas de los cuatro tópicos |
| `02-productor.json` | 472 registros: 452 normales, 16 duplicados, 4 inválidos, 150 fuera de orden |
| `03-pipeline.txt` | el pipeline Beam termina en estado `DONE` leyendo y escribiendo en Kafka |
| `04-metricas-materializadas.txt` | 132 agregados de las dos métricas, con ventana, dimensión y pane |
| `05-dlq.txt` | eventos apartados con el motivo exacto de la violación |
| `06-offsets.txt` | offsets y lag de los grupos de consumo |
| `07-replay-reset.txt` | rebobinado del grupo del tablero |
| `08-replay-resultado.txt` | resultado idéntico tras el replay: el reproceso converge |

**El resultado no depende de datos ideales.** La ejecución incluye a propósito
duplicados, eventos entregados fuera de orden y eventos que violan el contrato. El
sistema responde a los tres: descarta los duplicados en la agregación, aparta los
inválidos al DLQ sin interrumpir el pipeline, y el reproceso converge a la misma
vista porque el sumidero hace upsert en lugar de acumular.
