# Integrantes y contribuciones

**Proyecto integrador** — *Streaming de datos y sus aplicaciones*

| | |
|---|---|
| **Institución** | Facultad Politécnica — Universidad Nacional de Asunción |
| **Programa** | Maestría en Inteligencia Artificial y Análisis de Datos |
| **Asignatura** | Streaming de datos y sus aplicaciones |
| **Docente** | Rodrigo Parra, M.Sc. |
| **Repositorio** | <https://github.com/walterortiz07/trabajo_final_streaming_datos_grupo2> |

---

## Integrante

| # | Nombre | Contribución |
|---|---|---|
| 1 | **Walter Gabriel Ortiz Medina** | Todos los componentes del proyecto |

El trabajo se desarrolló de forma individual. La consigna admite equipos de hasta
cuatro integrantes, así que la totalidad de los componentes estuvo a cargo de la misma
persona.

---

## Contribuciones por componente

| Componente | Qué incluye |
|---|---|
| **Caso de uso y métricas** | Definición del problema, usuarios del resultado y las métricas que habilita |
| **Contrato de eventos** | Sobre de seis campos, validación, versionado e identidad determinista |
| **Diseño de tópicos y Kafka** | Cuatro tópicos, claves de particionamiento, cantidad de particiones y política de retención |
| **Simulador y productor** | Simulación de eventos discretos, reproducibilidad por semilla e inyección de duplicados, desorden e inválidos |
| **Pipeline con Apache Beam** | Lectura con KafkaIO, validación con salida lateral, ventanas, agregación con combinadores y publicación |
| **Tiempo de evento y ventanas** | Asignación de timestamps, ventana fija, disparadores y política de datos tardíos |
| **Estado y confiabilidad** | Deduplicación con estado y timer, clave lógica de salida, idempotencia y semántica de entrega declarada |
| **Pruebas** | 47 pruebas: contratos, simulador, transformaciones, ventanas y contraste contra el oráculo en Python puro |
| **Operación y reproducibilidad** | Docker Compose, Makefile, scripts de demostración y prueba de humo |
| **Documentación** | Documento técnico, registro de decisiones y evidencia de ejecución |
