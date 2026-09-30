# Decisiones de diseño

Cada decisión con su justificación, la alternativa descartada y el costo de haberse
equivocado. El resumen está en el [README](../README.md) y el desarrollo completo en el
[documento técnico](documento-tecnico.md).

---

## D1 — Fuente sintética en lugar de replay histórico

**Decisión.** Un simulador determinista de jornada, con semilla.

**Por qué.** Los registros de un contact center son datos personales: no existe un
dataset público como el de taxis de la clase 7. Además, el simulador permite inyectar
a voluntad los tres casos que la consigna exige evidenciar —duplicados, desorden y
eventos fuera de contrato— con proporciones controladas y reproducibles. Un dataset
real no permitiría provocar esos escenarios.

**Alternativa descartada.** Anonimizar un extracto real. Se descartó por privacidad y
porque no permitiría controlar los casos adversos.

**Costo de haberse equivocado.** Las tasas de llegada son supuestos, no mediciones.
El documento técnico lo declara como límite.

---

## D2 — Clave `contact_id` en lugar de `queue_id`

**Decisión.** Particionar el tópico de contactos por `contact_id`.

**Por qué.** Un contact center tiene ~10 colas con tráfico muy desigual. Con
`queue_id`, la partición de `soporte-n1` se llevaría casi todo el trabajo mientras las
demás quedan ociosas: *hot partition*, pérdida de paralelismo y latencia al alza justo
en la cola que más importa. `contact_id` es uniforme por construcción.

**Lo que se pierde y por qué no importa.** Se pierde el orden por cola. No se necesita:
las métricas se calculan por ventana de tiempo, y dentro de una ventana el orden es
irrelevante para un agregado.

**Alternativa descartada.** Clave aleatoria: reparte perfecto pero destruye el orden
por entidad y hace imposible la deduplicación local a la partición.

**Costo de haberse equivocado.** Alto: cambiar la clave de particionamiento exige
crear un tópico nuevo y migrar, y `hash(clave) % particiones` cambia, así que el orden
por entidad se rompe durante la transición.

---

## D3 — Dos tópicos crudos en lugar de uno

**Decisión.** `cc.contacts.raw.v1` (clave `contact_id`) y `cc.agent-state.raw.v1`
(clave `agent_id`).

**Por qué.** Kafka solo garantiza orden por partición, y la partición la decide la
clave. Del contacto se necesita `queued → connected → ended`; del agente,
`ready → busy → wrapup → ready`. Un solo tópico admite una sola clave y rompería uno
de los dos órdenes.

**Alternativa descartada.** Un tópico con un campo `entity_type`. El particionamiento
ocurre antes de que nadie lea el payload, así que el campo no ayuda.

---

## D4 — Seis particiones en los tópicos de entrada

**Decisión.** 6 particiones, aunque el volumen sea bajo.

**El dimensionamiento honesto.** Una operación de 120 agentes en 8 horas produce
~50.600 eventos por día: **~1,8 eventos/s de promedio y ~5 en la hora pico**. Una sola
partición maneja miles por segundo. Las 6 particiones **no** se justifican por
throughput del broker, sino por:

1. **Paralelismo de consumidores.** El paralelismo efectivo de un grupo está limitado
   por las particiones. Seis permiten hasta seis instancias.
2. ***Headroom*** para el pico y para el crecimiento del canal digital.
3. **Evitar reparticionar**, que cambia `hash(clave) % particiones` y rompe el orden
   por entidad durante la transición. Sobreproveer al inicio cuesta casi nada;
   reparticionar después cuesta una migración con pérdida temporal de la garantía.

**Por qué 6 y no un número suelto.** Es divisible por 2, 3 y 6: el reparto entre 2, 3
o 6 consumidores es parejo.

---

## D5 — Retención por capas

**Decisión.** 7 días para el crudo, 30 días para el derivado y el DLQ.

**Por qué.** Los 7 días cubren el ciclo semanal completo, incluido el fin de semana,
que es cuando cambia el patrón de demanda. Las métricas se guardan 30 días porque
ocupan mil veces menos y son la serie histórica que usa *workforce management*. El DLQ
también 30: los descartes son pocos y su valor es diagnóstico —si un origen empezó a
emitir mal, hay que poder mirar atrás lo suficiente.

**Alternativa descartada.** Retención infinita: el disco no es gratis y la retención
existe para acotar el costo.

---

## D6 — `DirectRunner` en lugar de Flink

**Decisión.** `DirectRunner`, con el pipeline corriendo en el host.

**Por qué.** Está verificado, no supuesto: `KafkaIO` de Python expande etapas Java y
el SDK de Python solo sabe lanzarlas como **contenedor** (`DOCKER`). El entorno
`PROCESS` que usa el laboratorio de la clase 7 no está registrado en el SDK
(`KeyError: 'beam:env:process:v1'`); lo resuelve el *job server* de Flink, no
`DirectRunner`. Con Kafka en Compose y el pipeline en el host, el costo de
infraestructura baja de **8,4 GB a ~1,2 GB**, que es lo que hace la demo viable en una
laptop con otros proyectos corriendo.

**Lo que se pierde.** El disparador temprano por *processing time*, porque
`DirectRunner` no provee ese reloj (`AttributeError: 'NoneType' object has no
attribute 'time'`). Con Flink basta poner `EARLY_FIRING_SECONDS=30`.

**Alternativa descartada.** Montar el socket de Docker dentro del contenedor del
pipeline. Funcionaría, pero le da al contenedor acceso equivalente a *root* sobre el
host. No vale el riesgo por comodidad.

---

## D7 — Estado con timer en lugar de `GroupByKey` para deduplicar

**Decisión.** `SetStateSpec` + `TimerSpec` de tiempo de evento.

**Por qué.** La clase 6 enseñó estado explícito y es lo que corresponde cuando se
necesita control fino. El timer programa la liberación del estado al cerrar la
ventana, así que la memoria no crece sin límite. Además el estado queda aislado por
clave y por ventana, que es exactamente el dominio de deduplicación que se necesita.

**Alternativa considerada.** `GroupByKey` sobre `event_id` (el patrón del laboratorio
de la clase 7). Es más simple y la ventana acota la memoria, pero deja el estado
implícito en un shuffle y no demuestra el manejo de expiración que pide la consigna.

---

## D8 — Acumulación `ACCUMULATING` y upsert por clave lógica

**Decisión.** Cada revisión contiene el total conocido; el consumidor reemplaza por
`aggregate_id`.

**Por qué.** Con `DISCARDING`, el consumidor tendría que sumar deltas: si uno se
pierde o se procesa dos veces, el total queda mal de forma silenciosa. Con
acumulativa, la última revisión **es** el total correcto y el orden de llegada de las
revisiones no cambia el resultado. La evidencia lo confirma: tras rebobinar el grupo
de consumo, la vista materializada es idéntica.

---

## D9 — `pane_timing` con nombre y `window_start` con zona

**Decisión.** `PaneInfoTiming.to_string(...)` y reponer el `tzinfo` antes de
`isoformat()`.

**Por qué.** Dos defectos reales, ambos verificados ejecutando el laboratorio de la
clase 7:

- `str(pane_info.timing)` publica `"1"` en lugar de `"ON_TIME"`, porque `timing` es un
  entero y no un enum con nombre.
- `Timestamp.to_utc_datetime()` devuelve un datetime ***naive***, así que
  `isoformat()` produce `2026-07-24T13:00:00` —sin sufijo— y el
  `.replace("+00:00", "Z")` del laboratorio nunca llega a aplicarse. Sin zona, esa
  cadena no identifica un instante.

**Costo de haberse equivocado.** Bajo en apariencia y alto en consecuencia: el
`aggregate_id` sigue siendo internamente consistente y todo "funciona", pero el
contrato de salida queda ambiguo para cualquier consumidor externo.

---

## D10 — Semántica declarada: at-least-once con sumidero idempotente

**Decisión.** Declarar at-least-once, no exactly-once.

**Por qué.** `enable.idempotence=true` y `acks=all` eliminan los duplicados por
reintento **del propio productor**, pero no los que genera el origen ni los que
provoca un fallo del consumidor entre procesar y confirmar. Afirmar exactly-once
exigiría demostrar que ningún tramo puede duplicar ni perder.

**Qué se declara.**

| Tramo | Garantía |
|---|---|
| Origen → Kafka | at-least-once |
| Kafka → Beam | at-least-once |
| Deduplicación | por `event_id`, dentro de clave y ventana |
| Publicación | efectivamente una vez, por upsert de `aggregate_id` |

**Límite explícito.** La deduplicación solo funciona si el duplicado conserva el mismo
`event_id`, y su horizonte está acotado a la ventana.
