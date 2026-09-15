# README.md — Andina Market: Pipeline de Datos

---

# NIVEL 1 — Ingesta (Capa Bronze)

## 0. Metodología de trabajo

La ingesta se implementó completamente mediante el **Managed Connector de Lakeflow Connect** (query-based connector para PostgreSQL), configurado vía la UI de Databricks (wizard "Add data" y edición directa de la definición del pipeline), sin escribir código de extracción custom.

Para entender cada paso del proceso —desde la elección entre CDC y polling, la configuración de cursor columns y primary keys, hasta el diagnóstico de errores específicos de la plataforma (como el `PERMISSION_DENIED` de Free Edition al publicar a múltiples schemas)— se usó **Claude (Anthropic)** como asistente técnico durante todo el desarrollo: para investigar el comportamiento real de cada mecanismo contra documentación oficial, validar la sintaxis de las definiciones de pipeline antes de correrlas, y depurar errores puntuales de configuración a medida que aparecían (permission errors, validación de campos no soportados en cada tipo de connector, comportamiento de `primary_keys` según el modo de pipeline). Todas las decisiones de diseño documentadas en este README fueron evaluadas explícitamente en términos de sus trade-offs, no adoptadas por defecto.

## 1. Tipo de carga, frecuencia y detección de cambios por tabla

| Tabla | Tipo de carga | Frecuencia | Cómo se detectan cambios |
|---|---|---|---|
| **Customers** | Completa (full snapshot) | Diaria | No aplica — se relee la tabla completa en cada corrida; no hay comparación incremental |
| **Products** | Completa (full snapshot) | Diaria | No aplica — mismo motivo que Customers |
| **Orders** | Incremental | Cada 30 min | Query-based connector con columna cursor `updated_at` — el connector consulta solo las filas cuyo `updated_at` sea mayor al último valor procesado |
| **OrderItems** | Incremental | Cada 30 min (junto con Orders) | Cursor `created_at` — como es append-only (nunca se edita una línea existente), basta con detectar filas nuevas, no cambios de estado |
| **SupportTickets** | Incremental | Cada 1-2 h | Cursor `updated_at` |
| **Payments** | Incremental | Cada 5-15 min | Cursor `event_timestamp` — cada evento de cambio de estado es una fila nueva (event-log), así que el cursor detecta simplemente "eventos nuevos desde la última corrida" |

**Criterio detrás de la frecuencia:** se calibra según impacto de negocio de la staleness, no solo mutabilidad de la tabla. Payments corre más seguido que Orders porque un dato financiero desactualizado (reconciliación, fraude) es más costoso que un dashboard operacional con retraso; SupportTickets corre menos seguido porque alimenta métricas de SLA que se revisan por hora, no en vivo.

## 2. Formato y organización del almacenamiento

- **Formato:** Delta en todas las tablas — es el único formato soportado por el Managed Connector de Lakeflow Connect, no es una decisión de diseño sino una restricción de la herramienta.
- **Organización:**
  ```
  main.tpt_bronze.customers
  main.tpt_bronze.products
  main.tpt_bronze.orders
  main.tpt_bronze.order_items
  main.tpt_bronze.support_tickets
  main.tpt_bronze.payments
  ```
  Catalog `main`, schema único `tpt_bronze` para todas las tablas crudas (sin separación por ambiente dev/prod, dado el scope del ejercicio).

- **Orquestación:** cada pipeline de ingesta se despliega y ejecuta como un **Job independiente en Databricks Workflows**, con su propio schedule alineado a la frecuencia definida en la tabla anterior. Esto significa 4 jobs distintos (uno por pipeline: `bronze_ingest_rds_reference_data`, `bronze_ingest_rds_orders`, `bronze_ingest_rds_support_tickets`, `bronze_ingest_rds_payments`), no un job monolítico ni ejecución manual — cada uno corre de forma autónoma según su propia cadencia.

## 3. Schema evolution y trazabilidad

**Manejo de cambios de esquema en el origen:** se usa el comportamiento default del Managed Connector, sin configuración adicional (`include_columns`/`exclude_columns` no se usó). Columnas nuevas en el origen se detectan y agregan automáticamente en la siguiente corrida del pipeline — las filas ingeridas antes del cambio quedan con `NULL` en esa columna (sin backfill retroactivo). Columnas eliminadas en el origen no se borran de Bronze; se marcan como `inactive` (vía table property), preservando el historial. Si una columna eliminada reaparece con el mismo nombre, el pipeline falla y requiere full refresh o borrado manual de la columna inactiva.

**Trazabilidad — "cuándo" llegó cada dato:**
- Para tablas full-batch (Customers, Products): `DESCRIBE HISTORY main.tpt_bronze.<tabla>` da el timestamp de cada corrida de escritura, sin necesidad de columnas adicionales — nativo del transaction log de Delta.
- Para tablas incrementales (Orders, OrderItems, SupportTickets, Payments): mismo mecanismo de `DESCRIBE HISTORY` a nivel de tabla. **Nota:** como no se activó `scd_type: SCD_TYPE_2` (documentado en la sección de decisiones), esta trazabilidad es a nivel de tabla/corrida, no por fila individual — no es posible reconstruir en qué corrida exacta llegó un registro específico una vez que fue sobrescrito por una versión más reciente del mismo `source_row_id`.

**Trazabilidad — "de dónde" vino cada dato:** se resuelve automáticamente vía el **lineage graph de Unity Catalog** — cada tabla Bronze creada por el Managed Connector queda enlazada visualmente a su connection origen (`rds_postgres_assessment`) en la pestaña "Lineage" del Catalog Explorer, sin ninguna configuración manual.

## 4. Propuesta: Clickstream como streaming de eventos

No implementado en este ejercicio — se generó una muestra (`clickstream_events.jsonl`) para ilustrar el formato, y aquí se documenta el diseño propuesto para producción.

A diferencia de las tablas de RDS (polling vía Managed Connector), el clickstream de la app móvil llegaría de forma nativa como un **stream de eventos**, típicamente publicado por la app a un tópico de Kafka. La ingesta se diseñaría con Spark Structured Streaming, no con un Managed Connector de base de datos:

```python
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "<broker>:9092")
    .option("subscribe", "andina_market.clickstream")
    .option("startingOffsets", "latest")
    .load()
)

# El value de Kafka llega como binario — se parsea el JSON del evento
parsed_df = (df
    .selectExpr("CAST(value AS STRING) AS json_payload", "timestamp AS kafka_timestamp")
    .select(from_json(col("json_payload"), clickstream_schema).alias("data"), "kafka_timestamp")
    .select("data.*", "kafka_timestamp")
)

(parsed_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/main/tpt_bronze/_checkpoints/clickstream")
    .outputMode("append")
    .trigger(availableNow=False)  # streaming continuo, no triggered
    .toTable("main.tpt_bronze.clickstream_events")
)
```

**Puntos clave del diseño:**
- **Modo continuo, no triggered** — a diferencia de las tablas de RDS, aquí sí se justifica streaming real dado que la fuente (Kafka) es nativamente continua, no un snapshot periódico.
- **Watermarking sobre `event_timestamp`** (el timestamp propio del evento, no el de Kafka) para manejar eventos que lleguen fuera de orden.
- **Checkpointing obligatorio** (`checkpointLocation`) para garantizar exactly-once y permitir reanudar el stream sin reprocesar ni perder eventos tras un reinicio del job.
- Alternativa evaluada y descartada para este diseño: Auto Loader sobre archivos JSONL en S3 — válida si la app escribe batches de archivos en vez de publicar a un bus de eventos, pero Kafka es el patrón más representativo de clickstream real en producción.

## 5. Propuesta: Integración con SAP ECC on-premise

No implementado en este ejercicio — diseño propuesto para el maestro de proveedores y órdenes de compra.

- **Conectividad:** VPN Site-to-Site entre el datacenter on-premise de Andina Market y la VPC de AWS donde corre Databricks — requisito de red previo a cualquier mecanismo de extracción.
- **Extracción:** no existe Managed Connector nativo de Lakeflow Connect para SAP ECC. Se propone **Fivetran SAP Connector**, que se conecta vía la capa RFC/BAPI de SAP (no directo a las tablas crudas `EKKO`/`EKPO`/`LFA1`, evitando el problema de esquema críptico y lógica de negocio en ABAP no reflejada en la base de datos).
- **Staging:** Fivetran escribe a una zona intermedia en S3 (`s3://bucket/staging/sap/`), desacoplando la herramienta de extracción del resto del pipeline.
- **Ingesta a Bronze:** Auto Loader levanta los archivos del staging hacia `main.tpt_bronze.sap_vendor_master` y `main.tpt_bronze.sap_purchase_orders`, reutilizando el mismo patrón medallion ya construido para RDS.
- **Cadencia:** Vendor Master en full batch diario (baja mutabilidad, bajo volumen); Purchase Orders incremental cada 2-4h, usando el mecanismo de Change Documents de SAP gestionado internamente por Fivetran (no un cursor expuesto manualmente).

## 6. Propuesta: Ingesta de archivos en S3 vía Auto Loader con File Arrival Trigger

No implementado en este ejercicio — diseño propuesto para escenarios donde la fuente entrega archivos a S3 en vez de exponer una base de datos consultable (aplica directamente al staging de SAP de la Sección 5, y a cualquier fuente futura que entregue exports/batches como archivos).

**Por qué no un schedule tradicional:** un pipeline con cron (`cada 30 min`, por ejemplo) corre igual haya o no archivos nuevos — desperdicia cómputo en corridas vacías, y en el peor caso introduce hasta el intervalo completo de latencia entre que el archivo llega y se procesa. **File Arrival Trigger** invierte esa lógica: el job se dispara automáticamente **cuando efectivamente llega un archivo nuevo** al path de S3, en vez de revisar por polling en intervalos fijos.

**Cómo funciona:**
- Se configura un **trigger de tipo "File arrival"** en el Job de Databricks Workflows, apuntando al path de S3 donde se depositan los archivos (ej. `s3://bucket/staging/sap/purchase_orders/`).
- Por debajo, Databricks se suscribe a las notificaciones de eventos del bucket (vía SQS), así que no hace polling activo contra S3 — reacciona al evento real de "nuevo objeto creado".
- El job dispara un pipeline de **Auto Loader** (`cloudFiles`), que procesa incrementalmente solo los archivos nuevos desde la última corrida, con checkpointing para garantizar exactly-once.

**Ejemplo de la ingesta con Auto Loader:**
```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")  # o "csv", "json" según la fuente
    .option("cloudFiles.schemaLocation", "/Volumes/main/tpt_bronze/_schemas/sap_purchase_orders")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("s3://bucket/staging/sap/purchase_orders/")
)

(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/main/tpt_bronze/_checkpoints/sap_purchase_orders")
    .trigger(availableNow=True)  # procesa lo disponible y se detiene — coherente con file arrival, no continuo
    .toTable("main.tpt_bronze.sap_purchase_orders")
)
```

**Configuración del trigger a nivel de Job** (conceptual, vía UI o API):
```json
{
  "trigger": {
    "file_arrival": {
      "url": "s3://bucket/staging/sap/purchase_orders/",
      "min_time_between_triggers_seconds": 60
    }
  }
}
```

**Por qué `trigger(availableNow=True)` y no `continuous`:** File Arrival dispara el job completo cuando detecta un archivo nuevo; dentro de esa corrida, Auto Loader debe procesar lo disponible y terminar (`availableNow`), no quedarse escuchando indefinidamente — eso sería redundante con el propio trigger del job, que ya se encarga de la parte "reactiva".

**Diferencia clave con el resto de la arquitectura:** a diferencia de RDS (polling con cursor column vía Managed Connector) y de Clickstream (streaming continuo real desde Kafka), este patrón es para fuentes que entregan **archivos discretos de forma irregular/batch** — ni tan continuo como Kafka, ni tan predecible como un cron — por eso File Arrival Trigger es el mecanismo más apropiado, no un cron ni un `trigger(continuous)`.

## 7. Por qué esta arquitectura

- **Managed Connector en vez de código custom:** delega paginación, checkpointing y detección de schema drift a la plataforma, evitando reimplementar lógica de extracción propensa a errores (manejo de estado, reintentos, watermarks) que el connector ya resuelve de forma nativa y probada.
- **4 pipelines agrupados por fuente + cadencia, no 1 por tabla ni 1 monolítico:** un pipeline por tabla multiplica overhead de orquestación/monitoreo sin beneficio real; un pipeline único obligaría a correr todas las tablas al ritmo de la más exigente (Payments) o de la más relajada (Customers), desperdiciando recursos o generando staleness innecesaria.
- **Catalog `main`, sin split dev/prod, sin DABs:** decisión de scope explícita dado que es un ejercicio de assessment sin requisitos de multi-ambiente ni CI/CD — se prioriza velocidad de entrega, documentando que en producción correspondería un catalog dedicado y despliegue versionado.
- **`source_row_id` como primary key en Customers/Orders (no la clave de negocio):** permite que Bronze cumpla su propósito de preservar los datos crudos tal cual llegan del origen —incluyendo sus problemas de calidad (duplicados, nulls)— en vez de forzar una limpieza prematura en la capa de ingesta, que le corresponde a Silver.
- **SCD Type 1 (sin history tracking) por simplicidad, documentado como trade-off:** se prefirió evitar la complejidad de SCD Type 2 dado el volumen del ejercicio, dejando explícito qué se pierde (reconstrucción de estados intermedios) y cómo se activaría en producción.

---

# NIVEL 2 — Transformación (Capa Silver)

## 2.0 Metodología de trabajo

Los 6 archivos `.sql` de Silver fueron generados con **Claude (Anthropic)** siguiendo el patrón de dos etapas (`silver_clean` → `silver_conformed`) discutido y validado antes de escribir cada archivo. La revisión propia se enfocó en la lógica de negocio de cada query, no en la sintaxis: esto llevó a identificar que, para Customers y Products, la Etapa B (`silver_conformed`) generada inicialmente era un simple passthrough (`SELECT * FROM customers_clean`) sin ningún trabajo real de integridad referencial, dado que ambas son entidades raíz sin FK saliente hacia otra tabla. Se consolidaron ambas etapas en una sola vista materializada por tabla (`main.tpt_silver.customers`, `main.tpt_silver.products`), eliminando el archivo intermedio `_clean` que no aportaba valor. Para las 4 tablas restantes (Orders, OrderItems, Payments, SupportTickets), donde sí existe integridad referencial real que validar, se mantuvo la separación en dos etapas.

## 2.1 Etapas por tabla

| Tabla | Etapas | Justificación |
|---|---|---|
| **Customers** | 1 (`customers`) | Entidad raíz sin FK saliente — limpieza y publicación se combinan en una sola vista, ya que una etapa `_conformed` separada sería un passthrough sin lógica adicional |
| **Products** | 1 (`products`) | Mismo motivo que Customers |
| **Orders** | 2 (`orders_clean` → `orders`) | Etapa B valida `user_id` contra Customers — trabajo real de integridad referencial |
| **OrderItems** | 2 (`order_items_clean` → `order_items`) | Etapa B valida `order_id` contra Orders y `sku` contra Products (doble FK) |
| **Payments** | 2 (`payments_clean` → `payments`) | Etapa B valida `order_id` contra Orders |
| **SupportTickets** | 2 (`support_tickets_clean` → `support_tickets`) | Etapa B valida `order_id` contra Orders y `customer_id` contra Customers (doble FK) |

## 2.2 Garantía de calidad de datos

### Deduplicación

| Tabla | Partición de dedupe | Por qué |
|---|---|---|
| Customers | `user_id` (clave de negocio) | Sin nulls en esta columna — resuelve los 5 duplicados exactos inyectados |
| Products | `sku` (clave de negocio) | Safeguard — sin duplicados esperados |
| Orders | `source_row_id` (clave técnica) | `order_id` tiene NULLs inyectados; particionar por ahí colapsaría pedidos distintos, ya que `PARTITION BY` trata `NULL = NULL` como igual |
| OrderItems | `order_item_id` (clave de negocio) | Safeguard — sin duplicados esperados |
| Payments | `event_id` (clave de negocio) | Resuelve los 8 eventos duplicados exactos inyectados (simulando replay de CDC) |
| SupportTickets | `ticket_id` (clave de negocio) | Safeguard — sin duplicados esperados |

### Constraints de nulls

Implementados como expectations nativas (`CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW`):
- Customers: `user_id` no nulo, formato de `email` válido
- Products: `sku` no nulo, `price > 0`
- Orders: `order_id` no nulo — captura los 5 registros con `order_id` NULL inyectados
- OrderItems: `order_item_id` no nulo, `quantity > 0`, `unit_price > 0` — captura los 10 registros con valores negativos inyectados
- Payments: `event_id` no nulo (`previous_status` se permite NULL intencionalmente, es legítimo en el primer evento de cada pago)
- SupportTickets: `ticket_id` no nulo

### Tipos de columnas

Todo cast es explícito en el `SELECT` (`TIMESTAMP`, `DECIMAL(10,2)`, `INT`), nunca se asume el tipo de Bronze como correcto. Se estandarizan además valores categóricos inconsistentes: `Customers.country` (`Col`/`MX`/`mexico` → nombre completo) y `Orders.channel` (`webb`/`WEB`/`storee` → `web`/`app`/`store`).

### Integridad referencial

Vía `INNER JOIN` en la etapa conformed; toda fila sin coincidencia se captura por separado con `LEFT ANTI JOIN` hacia `main.tpt_quarantine.*`, nunca se descarta silenciosamente:

| Tabla | FK validada | Resultado |
|---|---|---|
| Orders | `user_id` → Customers | Captura las 5 FK inválidas inyectadas |
| OrderItems | `order_id` → Orders; `sku` → Products | Captura las líneas huérfanas de los 5 pedidos con `order_id` NULL |
| Payments | `order_id` → Orders | Captura los eventos huérfanos de los mismos 5 pedidos |
| SupportTickets | `order_id` → Orders; `customer_id` → Customers | 1 ticket huérfano detectado en la práctica (no inyectado intencionalmente), confirmando que el mecanismo generaliza más allá de los issues diseñados |

## 2.3 Modelado de entidades y manejo de cambios en el tiempo

**Modelado:** relacional/normalizado, no dimensional — una tabla por entidad de negocio, fiel a la estructura de origen, sin agregar ni resumir. El modelado dimensional (hechos/dimensiones) se deja para Gold, ya que implica decisiones de agregación que no le corresponden a Silver; esta capa debe servir tanto a analítica como a ML con la granularidad completa.

**Cambios en el tiempo, por tabla:**

| Tabla | Manejo de cambios | Detalle |
|---|---|---|
| Customers | SCD Type 1 (sobrescritura) | El dedupe se queda con la versión más reciente por `user_id`; un cambio de segmento anterior no es reconstruible |
| Products | SCD Type 1 | Mismo comportamiento |
| Orders | SCD Type 1 | Transiciones de estado intermedias entre corridas del pipeline no se preservan |
| OrderItems | No aplica | Append-only por naturaleza — nunca se edita una línea existente |
| Payments | **Historial completo preservado, sin SCD** | El patrón event-log del origen ya captura cada transición de estado como fila inmutable con su propio `event_timestamp` — no requiere SCD Type 2 en Silver |
| SupportTickets | SCD Type 1 | Mismo comportamiento que Orders |

**Trade-off documentado:** no se activó SCD Type 2 en ninguna tabla (decisión ya establecida en Bronze, propagada a Silver por simplicidad del ejercicio). En producción se recomienda activarlo para Customers, Orders y SupportTickets usando `updated_at` como columna de secuencia, para evitar pérdida de historial y riesgo de data leakage en modelos de ML que necesiten el estado de una entidad en el momento exacto de un evento pasado.

## 2.4 Formato, particionamiento y organización

- **Formato:** Delta en todas las tablas.
- **Particionamiento y clustering (actual vs. producción):** no se aplicó particionamiento ni clustering físico en este ejercicio, dado el volumen del dataset sintético (decenas/cientos de filas) — a esta escala, cualquiera de las dos técnicas introduciría overhead de archivos pequeños sin beneficio real de skip de lectura. En producción, con volúmenes reales, se recomienda:

  | Tabla | Técnica recomendada | Por qué |
  |---|---|---|
  | Customers, Products | Liquid Clustering por `user_id`/`sku` (si el volumen lo justifica) | Son las tablas nuevas del proyecto — Databricks recomienda Liquid Clustering como default para tablas nuevas desde 2024/2025, no Z-ORDER (que queda reservado para tablas Delta heredadas que ya lo usaban). Z-ORDER no puede combinarse con Liquid Clustering. |
  | Orders, OrderItems | Partición por mes (`order_date`/`created_at`) + Liquid Clustering opcional por `user_id`/`sku` | Partición por año sería demasiado gruesa para el patrón de consulta típico ("pedidos del último mes"); partición por mes da el grano estándar para tablas de hechos transaccionales de este volumen, sin generar tantas particiones como para causar el problema de archivos pequeños. |
  | Payments, SupportTickets | Liquid Clustering por `order_id` y `event_timestamp` o `created_at` | Alta cardinalidad y patrón de join estable contra Orders — Liquid Clustering es superior a partición aquí porque `order_id` no es una columna de baja cardinalidad como para justificar particiones físicas. |

  **Nota:** Z-ORDER se descartó deliberadamente como técnica para este proyecto — es la opción correcta únicamente para tablas Delta ya existentes que no usan Liquid Clustering; para un proyecto greenfield como este, Liquid Clustering es la recomendación vigente de Databricks y evita tener que elegir de antemano las columnas de partición/Z-ORDER, algo costoso de cambiar después.

- **Organización:**
  ```
  main.tpt_bronze.*                      -- crudo (Nivel 1)

  main.tpt_silver.customers              -- 1 etapa (sin _clean)
  main.tpt_silver.products               -- 1 etapa (sin _clean)
  main.tpt_silver.orders_clean           -- Etapa A
  main.tpt_silver.orders                 -- Etapa B (publicada)
  main.tpt_silver.order_items_clean
  main.tpt_silver.order_items
  main.tpt_silver.payments_clean
  main.tpt_silver.payments
  main.tpt_silver.support_tickets_clean
  main.tpt_silver.support_tickets

  main.tpt_quarantine.*                  -- registros rechazados, con rejection_reason
  ```
- **Nota de plataforma:** se identificó y resolvió un error específico de Databricks Free Edition (`PERMISSION_DENIED: Can not move tables across arclight catalogs`) al publicar simultáneamente hacia `tpt_silver` y `tpt_quarantine` desde el mismo pipeline — limitación conocida de multi-schema publishing en este tier, no un error de diseño.

## 2.5 Pipeline y orquestación

**Pipeline:** los 6 archivos `.sql` de las secciones 2.1–2.2 viven consolidados en un único **Lakeflow Declarative Pipeline** (`pipeline_type: WORKSPACE`) llamado `silver_transform_entities`, apuntando vía glob a la carpeta `.../silver_transform_entities/transformations/` — cualquier `.sql` guardado ahí se procesa automáticamente en cada corrida, sin necesidad de registrarlo manualmente en la definición del pipeline. Corre en modo `Triggered` (no `Continuous`), con `serverless: true` y `photon: true`.

**Por qué un solo pipeline y no uno por tabla:** SDP construye un DAG de dependencias a partir de las referencias en el SQL (`orders` depende de `customers`, `order_items` depende de `orders` y `products`, etc.) y refresca todas las tablas relacionadas **en la misma corrida**. Esto garantiza que un `INNER JOIN` como el de `orders_conformed` contra `customers` siempre compare datos del mismo instante lógico — separar en pipelines/jobs independientes por tabla rompería esa garantía y reintroduciría el riesgo de joins contra snapshots desalineados entre tablas relacionadas por FK.

**Orquestación:** se creó un **Job independiente** (`job_silver_transform_entities`) con una sola task de tipo Pipeline apuntando a `silver_transform_entities`, con un **trigger periódico cada 1 hora**. Este Job **no está encadenado** (`depends_on`) a ninguno de los 4 pipelines de Bronze.

**Por qué Silver no se ancla a ningún pipeline de Bronze:** se evaluó encadenar Silver a un pipeline específico de Bronze (Orders u SupportTickets, por ser los más centrales en el grafo de dependencias vía FK) para darle un disparo reactivo en vez de un schedule fijo. Se descartó por dos motivos:

1. **Anclar a un solo pipeline no resuelve el problema para las demás tablas.** Encadenar solo a Orders, por ejemplo, le da frescura garantizada a Orders/OrderItems, pero Customers, Products, Payments y SupportTickets seguirían leyéndose en el estado que tengan al momento de esa corrida, sin ninguna garantía real adicional — la dependencia daría una falsa sensación de sincronización completa que no existe.
2. **Encadenar a los 4 pipelines de Bronze** (la alternativa "completa") forzaría la cadencia de Silver al pipeline más lento (Customers/Products, diario), desperdiciando la incrementalidad de Orders (30 min) y Payments (5-15 min) — el mismo problema que motivó separar Bronze en 4 pipelines por cadencia en primer lugar.

Se optó por un schedule fijo de 1 hora, independiente de Bronze, que consulta el estado más reciente disponible en cada corrida sin esperar explícitamente a ningún pipeline. Esto acepta una inconsistencia eventual acotada (hasta ~1 día de rezago posible entre el estado de Orders y el snapshot de Customers/Products que ve un join dado, dado que Customers/Products cargan diario), a cambio de simplicidad de orquestación y de no imponer una dependencia arbitraria que solo resolvería el problema parcialmente. En producción, con volúmenes reales, este trade-off se resolvería con **Delta Change Data Feed** o notificaciones basadas en eventos entre capas, en vez de schedules independientes.

---

# NIVEL 3 — Agregación y consumo (Capa Gold)

## 3.0 Metodología de trabajo

El diseño inicial de Gold se planteó como modelo dimensional (tablas de hecho + dimensión), pero se corrigió a **tablas ya agregadas al nivel de negocio** tras revisar la definición estándar de esta capa: *"Gold layer is the final layer in the multi-hop architecture, where tables provide business level aggregates often used for reporting and dashboarding, or even for Machine learning."* Un modelo fact/dim sigue requiriendo que la herramienta de BI haga el `GROUP BY` final — no cumple literalmente "agregado a nivel de negocio". Se optó por una tabla Gold por KPI (o grupo de KPIs relacionados), ya resuelta al grano que el dashboard consume directamente, sin joins ni agregación adicional del lado del consumidor.

## 3.1 Tablas Gold construidas

| Tabla | Grano | Fuente (Silver) | KPI que resuelve |
|---|---|---|---|
| `gold_revenue_by_period_channel_country` | 1 fila por semana × canal × país | `orders` + `customers` | Revenue/GMV por período y canal; revenue por país (concentración geográfica) |
| `gold_order_health` | 1 fila por mes | `orders` | Tasa de cancelación; tiempo de ciclo de pedido; volumen total de pedidos |
| `gold_customer_value_by_segment` | 1 fila por segmento de cliente | `orders` + `customers` | AOV y frecuencia de compra por segmento |

## 3.2 Decisiones de diseño

- **Qué cuenta como "revenue":** se excluyen pedidos `cancelled` del cálculo de ingresos en las 3 tablas (representan demanda que nunca se concretó). No se filtra por estado de pago aprobado — eso corresponde al KPI de Payments (pendiente), que mide caja cobrada, no volumen de pedidos.
- **LEFT JOIN, no INNER JOIN, en `gold_customer_value_by_segment`:** clientes sin ningún pedido válido (típicamente el segmento `inactivo`) se conservan en el resultado con `order_count = 0`, en vez de desaparecer de la tabla — la condición de exclusión de cancelados vive dentro del `ON`, no en un `WHERE` externo, para no perder al cliente completo si todos sus pedidos fueron cancelados.
- **Tiempo de ciclo solo sobre pedidos `delivered`:** estados intermedios (`shipped`, `preparing`) no tienen un "fin" válido todavía y se excluyen del promedio para no subestimarlo.
- **Limitación heredada de Silver (documentada en 2.3):** sin SCD Type 2 activo, `updated_at` solo refleja la última transición de estado — `avg_cycle_time_hours` mide el ciclo total (creación → entrega), no el tiempo detenido en cada estado individual.
- **Grano temporal semanal (Revenue) vs. mensual (Order Health):** Revenue usa semana para ver tendencia con suficiente detalle sin generar demasiadas filas; Order Health usa mes porque cancelación/ciclo son métricas más estables que no necesitan el mismo nivel de granularidad temporal.

## 3.3 Pipeline y orquestación

**Pipeline:** `gold_aggregate_kpis` — mismo patrón `pipeline_type: WORKSPACE` que Silver, con los `.sql` de la sección 3.1 en su carpeta de transformaciones. Se nombra con el verbo `aggregate` (no `transform`) para reflejar explícitamente que aquí sí ocurre agregación de negocio, a diferencia de Silver que solo limpia/concilia sin resumir.

**Orquestación:** `job_gold_aggregate_kpis`, mismo criterio que Silver — Job independiente con su propio schedule, sin `depends_on` hacia el pipeline de Silver, por la misma razón documentada en la sección 2.5 (evitar acoplar la cadencia de Gold a cuándo termine Silver, que a su vez no está acoplado a Bronze).

## 3.4 Dashboard: "Revenue & Order Health Dashboard"

Construido en Databricks AI/BI Dashboards, consumiendo directo de 2 de las 3 tablas Gold (sin escribir SQL adicional — el dashboard agrega/agrupa on-the-fly sobre las tablas ya agregadas):

| Widget | Tipo de visual | Dataset | Config |
|---|---|---|---|
| Revenue mensual por canal | Bar chart apilado | `gold_revenue_by_period_channel_country` | X: `period_week` (agrupado por mes en la UI), Y: `total_revenue`, Group by: `channel` |
| Revenue total por país | Bar chart horizontal | `gold_revenue_by_period_channel_country` | X: `SUM(total_revenue)` agregado por país en el propio widget, Y: `country` |
| Tendencia de cancelación | Line chart | `gold_order_health` | X: `period_month`, Y: `cancellation_rate_pct` |

**Decisión de diseño del dashboard — sin ejes duales:** cancelación y tiempo de ciclo se muestran como dos gráficos de línea separados (un eje cada uno), no combinados en un solo combo chart con doble eje Y. Un gráfico con dos escalas distintas en el mismo eje es una práctica de visualización desaconsejada — dificulta la lectura y puede sugerir correlaciones falsas entre dos métricas con unidades no comparables (%, horas).

`gold_customer_value_by_segment` no se usa en este dashboard — queda disponible para un futuro dashboard enfocado en segmentación de clientes, sin necesidad de reconstruir la tabla.
