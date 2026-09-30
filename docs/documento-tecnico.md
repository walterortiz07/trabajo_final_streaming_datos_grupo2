# Documento técnico — Analítica de contact center en tiempo real

Proyecto integrador de *Streaming de datos y sus aplicaciones* (FPUNA).

---

## 1. Problema, usuarios y métricas

### El problema

Un contact center opera con un reporte del día siguiente. Ese reporte dice cuántas
llamadas entraron, cuánto esperaron los clientes y cuántas se abandonaron, pero llega
cuando ya no se puede hacer nada al respecto.

La información pierde valor en minutos. Si a las 10:15 la cola de soporte empieza a
crecer, el supervisor necesita enterarse a las 10:17 para mover gente de otra cola.
Si se entera a las 10:40, esos clientes ya se fueron. El sistema actual no está
diseñado para esa escala de tiempo: agrega por lote nocturno.

### Usuarios del resultado

| Usuario | Qué decisión habilita |
|---|---|
| Supervisor de piso | mover agentes entre colas cuando la espera sube |
| Jefe de operaciones | detectar una cola crónicamente mal dimensionada |
| *Workforce management* | ajustar turnos con la serie histórica de demanda |

### Métricas que habilita

- contactos ofrecidos, atendidos y abandonados por cola y por minuto;
- **NS20**: proporción de contactos atendidos que esperaron 20 segundos o menos;
- tasa de abandono y distribución de la espera (promedio, p90, máximo);
- segundos de conversación y transiciones de estado de los agentes.

---

## 2. Arquitectura

```text
  FUENTE                      KAFKA                       BEAM                      SALIDA
  ──────                      ─────                       ────                      ──────

  Simulador                   cc.contacts.raw.v1          KafkaIO (ReadFromKafka)
  de jornada          ──▶     6 part · contact_id  ──▶    │
  (replay acelerado)          7 días                      ├─ ParseEvent ──▶ cc.events.dlq.v1
  · duplicados                                            │   (salida lateral)
  · desorden                                              ├─ AssignEventTime
  · retrasos                                              ├─ FixedWindows(60s)
  · inválidos                                             │   ACCUMULATING · lateness 300s
                                                          ├─ DeduplicateByEventId
                             cc.agent-state.raw.v1        │   (SetStateSpec + timer)
                      ──▶    6 part · agent_id     ──▶    ├─ CombinePerKey
                             7 días                      │   ├─ queue_metrics
                                                          │   └─ agent_state_counts
                                                          ▼
                             cc.queue-metrics.1m.v1  ◀──  WriteToKafka
                             3 part · queue_id
                             30 días
                                    │
                                    ▼
                             Consumidor (upsert por aggregate_id) ──▶ tablero
```

### Componentes

**Fuente — simulador determinista.** Genera una jornada completa de contact center
con una simulación de eventos discretos: llegadas por proceso de Poisson por cola,
asignación FIFO a agentes, paciencia limitada (abandono) y *wrap-up*. Con la misma
semilla produce exactamente la misma secuencia, lo que hace la demostración
reproducible. El replay desplaza el primer evento a "ahora" y comprime el reloj de
pared para que el paralelismo sea observable.

Se eligió **productor sintético** en lugar de *replay histórico* porque el dataset no
existe: los registros de un contact center son datos personales. El simulador además
permite inyectar a voluntad los tres casos que ponen a prueba el diseño
—duplicados, desorden y eventos fuera de contrato— con proporciones controladas y
reproducibles. Un dataset real no permitiría provocarlos de forma determinista.

**Kafka — log durable.** Cuatro tópicos con propósitos distintos (ver §3). Kafka
desacopla velocidades y conserva el historial: el pipeline puede reprocesar desde
offsets anteriores, y otros consumidores leen el mismo historial sin interferirse.

**Beam — la semántica.** El pipeline valida el contrato, asigna tiempo de evento,
ventanea, deduplica con estado y agrega con combinadores incrementales. Beam describe
*qué* transformación aplicar; el runner decide *cómo* distribuirla.

**Runner — `DirectRunner`.** Se eligió por una razón concreta y verificada:
`KafkaIO` de Python expande etapas Java, y el SDK de Python solo sabe lanzarlas como
contenedor, lo que requiere un daemon de Docker. El entorno `PROCESS` —el que usa el
laboratorio de la clase 7— no está registrado en el SDK de Python (`KeyError:
'beam:env:process:v1'`); lo resuelve el *job server* de Flink, no `DirectRunner`. Con
`DirectRunner` el pipeline corre leyendo y escribiendo en Kafka real, con un costo de
infraestructura de ~1,2 GB (solo Kafka) en lugar de los 8,4 GB del stack con Flink.

**Salida — changelog con clave lógica.** Un único tópico recibe las dos métricas. Cada
registro lleva `aggregate_id = metric_type | dimension_id | window_start`, que es lo
que permite el upsert aguas abajo.

**Consumidor — materialización.** No suma revisiones: conserva la de mayor
`pane_index` por `aggregate_id`. Reprocesar converge a la misma vista.

---

## 3. Contrato de eventos, tópicos y particiones

### El evento

```json
{
  "event_id": "9f3c1a72-4b8e-5d21-a6f0-1c7e2b9d4a35",
  "event_type": "contact_queued",
  "event_time": "2026-07-24T13:00:05.000Z",
  "schema_version": 1,
  "key": "ct-8f2a1b3c",
  "payload": {
    "contact_id": "ct-8f2a1b3c",
    "queue_id": "soporte-n1",
    "channel": "voice",
    "priority": "normal"
  }
}
```

| Campo | Responsabilidad |
|---|---|
| `event_id` | identidad lógica, **determinista** (UUID v5): el mismo hecho produce siempre el mismo id, así que un reproceso genera identificadores idénticos y la deduplicación sobrevive al replay |
| `event_type` | uno de los cuatro tipos: `contact_queued`, `contact_connected`, `contact_ended`, `agent_state_changed` |
| `event_time` | tiempo de **evento** del dominio, ISO-8601 UTC con ancho constante (milisegundos) |
| `schema_version` | versión del contrato |
| `key` | clave de negocio, igual a la clave del record de Kafka |
| `payload` | campos propios del tipo, validados contra el contrato |

El `event_time` usa ancho constante a propósito: con precisión variable
(`...T13:00:00Z` vs `...T13:00:05.782Z`), el orden lexicográfico **no** coincide con el
cronológico, porque `.` ordena antes que `Z`.

### Tópicos

| Tópico | Propósito | Clave | Part. | Retención |
|---|---|---|---|---|
| `cc.contacts.raw.v1` | ciclo de vida del contacto | `contact_id` | 6 | 7 días |
| `cc.agent-state.raw.v1` | transiciones de estado del agente | `agent_id` | 6 | 7 días |
| `cc.queue-metrics.1m.v1` | changelog de métricas | `queue_id` | 3 | 30 días |
| `cc.events.dlq.v1` | eventos que violan el contrato | la del origen | 1 | 30 días |

**Dos tópicos crudos y no uno.** Dentro de un tópico Kafka solo garantiza orden por
partición, y la partición la decide la clave. Contacto y agente tienen requisitos de
orden distintos: del contacto se necesita `queued → connected → ended`, del agente
`ready → busy → wrapup → ready`. Un solo tópico obliga a una sola clave y rompería uno
de los dos órdenes.

**Clave `contact_id` y no `queue_id`.** Un contact center tiene ~10 colas con tráfico
muy desigual: `queue_id` concentraría casi todo en una partición (*hot partition*) y
la paralelización se derrumbaría justo en la cola que más importa. `contact_id` es
uniforme por construcción. Lo que se pierde es el orden por cola, y no se necesita:
las métricas se calculan por ventana de tiempo, no por orden de llegada.

**6 particiones.** El volumen real es bajo: una operación de 120 agentes en 8 horas
produce ~50.600 eventos por día, es decir **~1,8 eventos/s de promedio y ~5 en la hora
pico**. Una sola partición maneja miles de eventos por segundo, así que 6 particiones
**no** se justifican por throughput del broker sino por: (a) permitir hasta 6 lectores
en paralelo por grupo, (b) absorber el pico y el crecimiento del canal digital, y
(c) **evitar reparticionar**, que cambia `hash(clave) % particiones` y por lo tanto
rompe el orden por entidad durante la transición. Se elige 6 y no un número suelto
porque es divisible por 2, 3 y 6.

### Esquema de salida

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
  "offered": 4, "connected": 4, "abandoned": 0, "transferred": 0, "resolved": 4,
  "answered_within_sl": 4, "ns20": 1.0, "abandon_rate": 0.0,
  "avg_wait_seconds": 6.2, "p90_wait_seconds": 14.0, "max_wait_seconds": 18.0,
  "total_talk_seconds": 480.0
}
```

`pane_timing` lleva el **nombre** (`EARLY`/`ON_TIME`/`LATE`), no el número del enum, y
`window_start` termina en `Z`. Ambos son defectos reales del laboratorio de la clase 7
(`str(pane_info.timing)` publica `"1"`, y `to_utc_datetime()` devuelve un datetime
*naive*, así que el `.replace("+00:00", "Z")` nunca se aplica y el sufijo no aparece).

---

## 4. Tiempo de evento, ventanas y datos tardíos

| Decisión | Valor | Justificación |
|---|---|---|
| Reloj | `event_time` del contrato | la métrica debe atribuirse al minuto en que ocurrió la atención. Con tiempo de llegada, un evento de las 13:00:42 que llega a las 13:01:35 se contaría en el minuto equivocado, y dos ejecuciones darían resultados distintos |
| Ventana | fija de 60 s, semiabierta `[inicio, fin)` | períodos comparables y no solapados, que es la pregunta del supervisor. Una ventana deslizante costaría más estado y no aporta a este tablero |
| Acumulación | `ACCUMULATING` | cada revisión contiene el total conocido, así que el consumidor **reemplaza** en lugar de sumar. Con deltas, un delta perdido o duplicado corrompe el total de forma silenciosa |
| Lateness | 300 s | el p99 normal es de 30 s, pero se observaron lotes de recuperación de hasta 4 minutos tras incidentes de red. El horizonte cubre el incidente, no el caso normal |
| Disparador | `AfterWatermark(late=AfterCount(1))` | cierre por progreso del tiempo de evento y corrección ante cada evento tardío |
| Fuera del horizonte | desvío al DLQ | descartar sin medir deja el problema invisible; el conteo por origen es en sí un indicador operativo |

**La ventana se decide por `event_time`, no por el orden de llegada.** El simulador
emite eventos con retraso variable (hasta 45 s) y el productor los entrega en orden de
llegada, así que el orden real y el de llegada difieren en ~150 de cada 472 registros.

**El horizonte de lateness se mide contra el cierre de la ventana**, no contra el
atraso propio del evento: un evento ocurrido al comienzo de su ventana tiene más
margen que uno del final, y el estado vive hasta `fin de ventana + lateness`
independientemente de dónde cayó el evento dentro de ella.

---

## 5. Duplicados, idempotencia y semántica de entrega

### Deduplicación con estado y expiración

`DeduplicateByEventId` guarda los `event_id` vistos en un `SetStateSpec` y programa un
timer de tiempo de evento al cierre de la ventana:

```python
SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

def process(self, element, seen_ids=..., window_param=..., expiry=...):
    key, event = element
    if event["event_id"] in set(seen_ids.read() or ()):
        return                      # reintento del origen: no se emite de nuevo
    seen_ids.add(event["event_id"])
    expiry.set(window_param.end)    # el estado vive lo que vive la ventana
    yield key, event

@on_timer(EXPIRY)
def expire(self, seen_ids=...):
    seen_ids.clear()
```

El estado está aislado por clave y por ventana, así que dos colas que compartan un
`event_id` no se estorban. **Sin la expiración el estado crece indefinidamente**: el
runner acumularía en memoria y en los checkpoints identificadores de eventos que ya no
pueden reaparecer.

### Sumidero idempotente

La clave lógica `metric_type | dimension_id | window_start` hace que cada revisión
**reemplace** a la anterior. Reprocesar el histórico produce el mismo estado final, y
la evidencia lo demuestra: tras rebobinar el grupo del tablero, la vista materializada
es byte a byte idéntica.

### Semántica declarada

| Tramo | Garantía | Mecanismo |
|---|---|---|
| Origen → Kafka | at-least-once | `enable.idempotence=true` evita duplicados por reintento del productor, pero el origen puede reenviar |
| Kafka → Beam | at-least-once | el offset se confirma después de procesar; un fallo reprocesa |
| Deduplicación | por `event_id`, dentro de clave y ventana | estado con expiración por timer |
| Publicación | efectivamente una vez en el consumidor | upsert por `aggregate_id`, conservando el mayor `pane_index` |

**No se afirma exactly-once de punta a punta.** La solución es at-least-once con
deduplicación y sumidero idempotente, que es una garantía distinta y más débil.
Afirmar exactly-once exigiría demostrar que ningún tramo puede duplicar ni perder, y
este diseño no lo sostiene.

---

## 6. Pruebas

47 pruebas, todas ejecutables con `uv run pytest`.

| Archivo | Qué cubre |
|---|---|
| `test_contracts.py` | construcción, codificación y validación del contrato; rechazo de eventos inválidos |
| `test_simulator.py` | determinismo por semilla, ciclo de vida completo, orden causal, coherencia de claves |
| `test_transforms.py` | `TestPipeline`: validación con salida lateral, ventanas y agregación, deduplicación aislada por clave, conteo de estados, unificación del changelog |
| `test_windows.py` | `TestStream`: la ventana se decide por `event_time`, el duplicado no cambia el total, la acumulación publica el total, contrato de salida completo |
| contraste | el pipeline y el oráculo en Python puro deben coincidir campo por campo sobre los mismos eventos |

Los escenarios adversos que exige la consigna están cubiertos: **duplicado**
(`test_el_duplicado_en_la_misma_ventana_no_cambia_el_total`), **fuera de orden**
(`test_la_ventana_se_decide_por_event_time_no_por_orden_de_llegada`), **claves
aisladas** (`test_la_deduplicacion_esta_aislada_por_clave`), **escritura repetida**
(evidencia de replay, §7) y **límite del contrato** (los eventos inválidos terminan en
el DLQ).

Además, `make smoke` ejecuta el recorrido completo contra Kafka real con tópicos
efímeros: produce, corre el pipeline, y verifica contra el broker que el changelog
traiga las dos métricas, que los límites de ventana sean instantes ISO-8601 y que los
panes traigan su nombre.

---

## 7. Operación y reproducibilidad

```bash
make up        # Kafka + tópicos        make pipeline  # Beam contra Kafka
make producer  # jornada simulada       make metrics   # tabla de métricas
make demo      # todo lo anterior + evidencia
make down      # detener                make clean     # detener y borrar el log
```

`make demo` deja la evidencia en `evidence/` sin intervención manual. Las variables de
entorno (`KAFKA_*`, `WINDOW_SECONDS`, `ALLOWED_LATENESS_SECONDS`, `BEAM_RUNNER`)
permiten cambiar la configuración sin editar código.

---

## 8. Límites conocidos, supuestos y mejoras

### Límites

1. **No hay exactly-once de punta a punta.** Ver §5.
2. **La deduplicación está acotada a la ventana.** Un duplicado que llega en una
   ventana posterior no se detecta, porque el estado ya expiró. El horizonte debe
   superar el máximo reintento esperado del origen.
3. **El disparador temprano no está activo.** `AfterProcessingTime` necesita un reloj
   de *processing time* que `DirectRunner` no provee en esta configuración
   (`AttributeError: 'NoneType' object has no attribute 'time'`). Con un runner
   portable (Flink) el reloj existe y basta poner `EARLY_FIRING_SECONDS=30`. Sin
   disparador temprano, el primer valor de una ventana llega al cerrarse, no antes.
4. **El pipeline corre en el host, no en un contenedor.** `KafkaIO` expande etapas
   Java y el SDK de Python solo sabe lanzarlas como contenedor; el entorno `PROCESS`
   no está registrado en el SDK. Se necesita un daemon de Docker, que el host tiene y
   un contenedor no.
5. **Un solo broker Kafka, sin replicación.** No sobrevive a la caída del broker. En
   producción serían al menos tres, con factor de replicación 3 e ISR 2.
6. **El horizonte de lateness no se ejerce en la demostración acotada.** Con lectura
   acotada, la fuente termina y el watermark salta a infinito: todas las ventanas
   cierran en un único pane `ON_TIME`. Para observar correcciones `LATE` hace falta
   operación continua con el watermark avanzando en tiempo real.
7. **`p90_wait_seconds` exige retener las esperas de la ventana.** El acumulador del
   combinador lleva la lista de esperas para poder calcular el percentil; es un costo
   acotado por el tamaño de la ventana, pero es estado que un contador no tendría.
8. **El simulador no es el ACD real.** Reproduce el ciclo de vida, la saturación y los
   abandonos con una semilla determinista, pero las tasas de llegada son supuestos,
   no mediciones de una operación real.

### Supuestos

- El sistema de telefonía puede emitir eventos con clave de negocio estable
  (`contact_id`, `agent_id`) y con un `event_time` confiable. Si solo tuviera tiempo
  de ingesta, el diseño temporal cambiaría por completo.
- Los eventos de un mismo contacto se publican cerca del orden real; el pipeline tolera
  el desorden, pero no lo espera como caso normal.
- El orden de magnitud del volumen (~50.600 eventos/día) alcanza para dimensionar
  particiones y retención; no es una proyección exacta.

### Mejoras posibles

- **Flink como runner** para habilitar el disparador temprano y comparar el
  comportamiento con `DirectRunner` (el laboratorio de la clase 7 muestra el camino).
- ***Schema Registry*** con compatibilidad verificada, en lugar de `schema_version`
  entero y validación en el consumidor.
- **Alertas sobre el DLQ**: si la tasa de descarte supera un umbral, el origen está
  emitiendo mal y hay que avisar.
- **Inferencia en streaming** (clase 8): un modelo que estime el riesgo de abandono a
  partir de la profundidad de la cola y la espera acumulada, con `RunInference`, y PSI
  para detectar *drift* en la distribución de la demanda.
- **Métricas de *lag* por grupo de consumo** exportadas a un tablero, en lugar de
  consultarlas con `kafka-consumer-groups.sh`.
