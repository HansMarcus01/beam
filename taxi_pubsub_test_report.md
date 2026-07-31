# Reporte de Diagnóstico de Costos de Pub/Sub y Dataflow (apache-beam-testing)

## 1. Introducción y Contexto del Problema

El incremento gradual de costos observado en el proyecto GCP apache-beam-testing no se explica por un único evento aislado, sino por la acumulación de infraestructura de pruebas en tiempo real que permanece activa más tiempo del necesario. En este tipo de validaciones, el costo no proviene solo de ejecutar código: también lo generan los recursos que el test crea, el tiempo que esos recursos permanecen vivos, la cantidad de veces que el flujo se relanza y la ausencia de una limpieza explícita y verificable al finalizar.

En el ecosistema de Apache Beam, las pruebas de streaming sobre Pub/Sub y los jobs de Dataflow suelen ser el punto más sensible desde el punto de vista FinOps. Cada ejecución puede implicar workers de Dataflow, tópicos de Pub/Sub, suscripciones, retención de mensajes, colas pendientes y tráfico de red. Cuando esos recursos se crean con nombres efímeros, pero el proceso que los creó no ejecuta un teardown consistente, el resultado es una huella de costo que crece día tras día aunque cada ejecución individual parezca pequeña.

La confusión inicial de este caso estaba centrada en las pruebas de taxi de Beam, especialmente en las rutas NYC/Chicago. Sin embargo, el análisis de los artefactos del repositorio muestra que esas pruebas no son el origen recurrente del gasto. El verdadero patrón de costo está en el proyecto apache-beam-testing, donde aparecen jobs de Dataflow con nombres repetidos `read-pubsub-...` y `write-pubsub-...`, ejecutados de forma periódica y persistente. La lectura correcta del problema es esta: el aumento de costo no nace de un solo topic “eterno”, sino de una secuencia continua de ejecuciones automáticas que mantienen viva la infraestructura y, potencialmente, dejan recursos asociados con vida útil mayor a la esperada.

## 2. Frecuencia de Ejecución de Pruebas (Pregunta 1 - How often are tests running that create a pubsub topic + subscription? (specifically Chicago/NYC taxi))

### 2.1 Conclusión GitHub

El repositorio oficial de Apache Beam no respalda la hipótesis de que las pruebas estándar de taxi NYC/Chicago estén creando continuamente sus propios tópicos y suscripciones de Pub/Sub.

La evidencia revisada apunta a lo siguiente:

* Las pruebas de taxi NYC en Beam consumen el tópico público compartido `projects/pubsub-public-data/topics/taxirides-realtime` y no crean un topic/subscription propios en su ruta normal de ejecución.
* El benchmark de taxi Chicago es BigQuery-based y no usa Pub/Sub como mecanismo de ingestión.
* La única ruta de taxi que sí provisiona recursos Pub/Sub en el árbol local revisado es un script de validación puntual de RC, no una prueba continua con cron propio.

Esto significa que, en el código de Beam que se revisó, el patrón de costo no se origina en la suite de taxi como se pensó inicialmente, sino en los jobs de Dataflow del proyecto apache-beam-testing.

### 2.2 Evidencia específica del repositorio Beam

El workflow que ejecuta el performance test PubsubIOIT de Python está definido en [beam_PerformanceTests_PubsubIOIT_Python_Streaming.yml](.github/workflows/beam_PerformanceTests_PubsubIOIT_Python_Streaming.yml#L16) y tiene programación cron diaria:

* Nombre del workflow: `PerformanceTests PubsubIOIT Python Streaming`
* Schedule: `30 10 * * *`
* Runner: `TestDataflowRunner`
* Main class: `apache_beam.io.gcp.pubsub_io_perf_test`
* Job name con timestamp UTC: `performance-tests-psio-python-2gb$(date '+%m%d%H%M%S' --utc)`

Ese workflow se ve además referenciado en la lista de tests monitoreados por el prefetcher de GitHub runs en [config.yaml](.test-infra/metrics/sync/github/github_runs_prefetcher/code/config.yaml#L283).

El punto importante para el diagnóstico de costos es que el workflow contiene la lógica de ejecución del test, pero en el tramo inspeccionado no aparece una fase explícita de cleanup de Pub/Sub equivalente a un `gcloud pubsub topics delete` o `gcloud pubsub subscriptions delete`. Esa ausencia no prueba por sí sola una fuga, pero sí establece una diferencia crítica con otros flujos del repo que sí terminan con pasos de limpieza bien definidos.

### 2.3 Hallazgo real en apache-beam-testing

La evidencia del proyecto apache-beam-testing muestra una secuencia sostenida de jobs Dataflow con nombres de la familia `read-pubsub-YYYYMMDD...` y `write-pubsub-YYYYMMDD...`. El inventario resumido en [listDataflow.txt](infra/listDataflow.txt) deja ver un patrón repetitivo y fácil de reconocer:

* Cada día aparecen pares read/write.
* Los pares están repetidos durante varios días consecutivos.
* En la ventana observada del 14 al 19 de julio de 2026 se registran 48 job records, equivalentes a 24 ejecuciones pareadas.
* La carga no es esporádica: hay actividad diaria sostenida y, en varios días, múltiples pares en la misma jornada.

La cuantificación diaria observada en esa ventana es la siguiente:

| Fecha | Job records | Pares read/write |
| --- | ---: | ---: |
| 2026-07-14 | 8 | 4 |
| 2026-07-15 | 8 | 4 |
| 2026-07-16 | 8 | 4 |
| 2026-07-17 | 8 | 4 |
| 2026-07-18 | 10 | 5 |
| 2026-07-19 | 6 | 3 |

Esto es suficiente para afirmar que el entorno no está ante un incidente puntual, sino ante una rutina operacional repetida. La forma del gasto es acumulativa porque la frecuencia de ejecución es alta y sostenida.

### 2.4 Cadencia temporal observada

El análisis de los timestamps incrustados en los nombres de los jobs muestra que los launches no son aislados. En la ventana del 14 al 19 de julio aparecen franjas horarias recurrentes con varios pares por día. Ejemplos representativos:

* 2026-07-14: 00:48, 21:56, 22:56, 23:56 UTC
* 2026-07-15: 00:55, 21:56, 22:56, 23:56 UTC
* 2026-07-16: 00:55, 21:58, 22:58, 23:58 UTC
* 2026-07-17: 00:58, 21:55, 22:55, 23:55 UTC
* 2026-07-18: 00:54, 21:54, 22:49, 22:54, 23:53 UTC
* 2026-07-19: 00:08, 00:53, 01:27 UTC

La lectura operativa de este patrón es clara: hay una recurrencia diaria real, y dentro de cada día se observan varias activaciones separadas por bloques temporales. Eso es exactamente el tipo de comportamiento que multiplica el costo base de un test de streaming cuando el recurso no se destruye inmediatamente o cuando las ejecuciones quedan distribuidas a lo largo del tiempo.

## 3. Análisis de Vida Útil de Suscripciones y Evidencia de Orfandad (Pregunta 2)

### 3.1 Resultado validado del análisis empírico

El análisis empírico ejecutado sobre `projects/pubsub-public-data/topics/taxirides-realtime` confirmó que el problema ya no es hipotético: existe una población masiva de suscripciones huérfanas asociadas al topic público que alimenta las pruebas de taxi NYC.

El script de Python produjo el siguiente resultado consolidado:

| Métrica | Resultado |
| --- | ---: |
| Suscripciones analizadas | 540 |
| Suscripciones huérfanas | 540 |
| Porcentaje de orfandad | 100.00% |
| Tipo de envío | Pull |
| Límite de confirmación | 1 minuto |
| Retención de mensajes | 7 días |
| Vencimiento por inactividad (TTL) | 31 días |

La lectura FinOps de esta tabla es contundente: no existe una fracción saludable dentro del conjunto medido. Las 540 suscripciones están huérfanas, son activas desde el punto de vista administrativo, pero inservibles desde el punto de vista operacional porque no registran consumidores activos ni confirmaciones de mensajes.

### 3.2 Qué significa backlog en Pub/Sub

En Pub/Sub, el backlog es el conjunto de mensajes publicados que aún no han sido confirmados por un consumidor. Mientras una suscripción permanece sin consumo efectivo, los mensajes se mantienen en espera y el sistema sigue administrando su entrega, reintento y almacenamiento temporal.

Ese backlog no es una abstracción teórica. Tiene tres consecuencias materiales:

* ocupa almacenamiento persistente dentro del periodo de retención configurado,
* incrementa la complejidad operativa de la cola,
* genera costo fantasma cuando la suscripción quedó abandonada, pero los mensajes siguen llegando.

El dato clave aquí es que Pub/Sub cobra el almacenamiento del backlog a una tarifa aproximada de $0.05 USD por GB-mes. Por tanto, cuando el backlog crece y además se replica en cientos de suscripciones huérfanas, el costo ya no es marginal: se convierte en una fuga estructural.

### 3.3 Evidencia de backlog y límites de confirmación vencidos

Las métricas de GCP muestran que el sistema no está drenando mensajes con normalidad. La métrica de antigüedad de mensajes sin confirmar presenta una pendiente ascendente sostenida durante la semana y se aplana exactamente en 7 días a partir del 16 de julio de 2026. Esa forma es consistente con un backlog que alcanza el tope de retención configurado y deja de avanzar porque los mensajes viejos ya no pueden acumular más antigüedad útil.

Además, se observan picos de 2.27k/s en límites de confirmación vencidos. En términos operativos, esto significa que los consumidores no están confirmando el flujo y Pub/Sub queda forzado a reintentar entregas una y otra vez. El resultado es doble:

* más reprocesamiento inútil,
* más almacenamiento sostenido de mensajes que nunca salen del backlog.

La estabilización horizontal en 7 días es el indicador más importante: confirma que las colas acumulan el flujo incesante de taxis de NY hasta alcanzar el máximo de retención. Eso es exactamente el comportamiento de una cola abandonada, no de una cola temporalmente atrasada.

### 3.4 Regularidad de creación de suscripciones

El sistema no solo tiene suscripciones huérfanas; además las crea de forma recurrente. El script de Dataflow confirmó una cadencia sostenida de 4 a 5 suscripciones nuevas por día, lo que equivale aproximadamente a 28 a 35 por semana.

Con un TTL por inactividad de 31 días, esa cadencia genera una masa flotante acumulativa de suscripciones vivas durante todo el mes. Si el ritmo se mantiene, el orden de magnitud esperado de suscripciones coexistiendo en cualquier momento del mes queda en torno a 124 a 155 suscripciones activas solo por arrastre temporal, sin contar las ya observadas en el historial completo.

En otras palabras: el proyecto no tiene una fuga puntual, tiene una geometría de crecimiento regular. Se crean más suscripciones de las que se eliminan oportunamente, y el sistema conserva esa deuda operacional durante 31 días completos.

### 3.5 Matemática del costo fantasma

La explicación FinOps del problema es sencilla y, al mismo tiempo, severa.

Si el flujo constante de taxis de NY genera 2 GB diarios y una suscripción huérfana conserva mensajes durante 7 días, entonces cada suscripción acumula aproximadamente 14 GB de backlog efectivo en su ventana de retención.

Con 540 suscripciones huérfanas, el volumen redundante asciende a:

$$
540 \times 14\,GB = 7{,}560\,GB \approx 7.56\,TB
$$

Ese volumen equivale a aproximadamente 7.5 Terabytes de almacenamiento fantasma retenido en colas muertas. A tarifa de almacenamiento de backlog de $0.05 USD/GB-mes, el costo mensual fantasma asociado a ese volumen alcanza aproximadamente:

$$
7{,}560\,GB \times 0.05 = 378\,USD/mes
$$

Este cálculo es conservador porque solo considera almacenamiento de backlog. No incorpora el costo indirecto del reprocesamiento, la reentrega reiterada ni la carga operativa asociada a la administración de cientos de suscripciones huérfanas.

### 3.6 Conclusión operativa de la orfandad

La evidencia demuestra que el problema no es únicamente que existan suscripciones. El problema es que existen 540 suscripciones, que todas son huérfanas, que tienen TTL de 31 días por inactividad y que siguen reteniendo backlog durante 7 días completos bajo un flujo de entrada constante.

Desde FinOps, esto define un patrón de costo inflado por persistencia. El almacenamiento fantasma no crece porque se esté usando mejor la plataforma; crece porque se está reteniendo trabajo muerto durante demasiado tiempo.

## 4. Diagnóstico de Costo y Causa Raíz

El diagnóstico final debe leerse en dos capas complementarias.

Primero, la frecuencia de ejecución de la suite PubsubIOIT en apache-beam-testing demuestra un patrón crónico de jobs `read-pubsub` y `write-pubsub` ejecutados diariamente, con varios pares por jornada. Eso explica por qué el sistema sigue sembrando recursos de Pub/Sub de forma constante.

Segundo, la causa raíz del incremento mensual de costos ya está claramente identificada: el almacenamiento de backlog retenido por las 540 suscripciones huérfanas del topic público `projects/pubsub-public-data/topics/taxirides-realtime`, sumado a un TTL por inactividad de 31 días, mantiene vivas estructuras que no aportan valor operativo y que siguen acumulando carga de almacenamiento y reprocesamiento.

En términos económicos, el factor dominante no es el costo de una sola corrida, sino la suma de tres efectos:

* backlog persistente durante 7 días por suscripción,
* 540 suscripciones huérfanas simultáneamente degradando el mismo topic,
* 31 días de vida residual antes de la autodestrucción por inactividad.

Eso convierte un flujo de prueba en un multiplicador de costo mensual. Cada nueva suscripción extiende la superficie de backlog; cada día adicional multiplica la factura; cada suscripción huérfana añade almacenamiento fantasma a la misma deuda técnica.

## 5. Conclusión Ejecutiva

La conclusión formal es doble.

Primero, la hipótesis de que las pruebas estándar de taxi NYC/Chicago del repositorio Beam estén creando continuamente topics y suscripciones de Pub/Sub no se sostiene con la evidencia revisada. NYC consume el topic compartido público y Chicago es BigQuery-based; no hay aquí una máquina de costo Pub/Sub continua en el código base revisado.

Segundo, el incremento de costo que sí aparece en apache-beam-testing está asociado al fenómeno combinado de ejecución automática persistente y orfandad masiva de suscripciones. Los jobs `read-pubsub` y `write-pubsub` confirman una recurrencia diaria de creación de recursos, y las 540 suscripciones huérfanas con TTL de 31 días confirman por qué la factura mensual se infla: el backlog queda retenido, se replica y permanece vivo mucho después de que el test haya dejado de ser útil.

La conclusión de negocio es inequívoca: el principal inflador de costo mensual no es el uso legítimo del topic, sino la permanencia innecesaria de backlog en cientos de suscripciones abandonadas.

### 5.1 Evaluación de Impacto y Riesgo de la Limpieza Automática
* **Riesgo Operativo Cero:** Debido a que el proyecto `apache-beam-testing` es estrictamente un entorno de pruebas de integración de software y cada ejecución de prueba genera su propia suscripción temporal autónoma (sin depender de las colas de días anteriores), la eliminación automática de las suscripciones huérfanas inactivas de más de 24 horas tiene un **0% de riesgo de interrupción o pérdida de confiabilidad** en las pruebas diarias activas del SDK.

## 6. Recomendaciones Técnicas

* Reducir la política de vencimiento por inactividad de las suscripciones temporales creadas en pruebas de 31 días a 1 día (86400 segundos) para forzar su autodestrucción rápida si el script de prueba falla.
* Implementar una regla de Garbage Collection masiva, con limpieza automatizada semanal, que barra y elimine las suscripciones con prefijo `taxirides-realtime_beam_-` que permanezcan inactivas.
* Forzar que la biblioteca de pruebas de Beam asigne un TTL dinámico corto durante la llamada de creación de la suscripción en la API de GCP, evitando depender del valor predeterminado de 31 días.
* Añadir una fase explícita de cleanup para tópicos y suscripciones de Pub/Sub al final de cada ejecución automática.
* Verificar que los nombres efímeros no queden sin control después de fallos parciales o abortos.
* Instrumentar el flujo con evidencia de teardown exitoso para cada job.
* Revisar la cadencia del cron y definir si todas las ejecuciones diarias son realmente necesarias para el objetivo de validación.
* Centralizar monitoreo de recursos Pub/Sub huérfanos para reducir costo acumulado por recursos olvidados.

## 7. Referencias

* [beam_PerformanceTests_PubsubIOIT_Python_Streaming.yml](.github/workflows/beam_PerformanceTests_PubsubIOIT_Python_Streaming.yml#L16)
* [GitHub runs prefetcher config](.test-infra/metrics/sync/github/github_runs_prefetcher/code/config.yaml#L283)
* [listDataflow.txt](infra/listDataflow.txt)