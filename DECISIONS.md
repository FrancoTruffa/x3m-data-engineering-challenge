# DECISIONS.md

> Borrador en progreso. Las secciones marcadas como **[PENDIENTE]** se completan a medida que avanza la implementación.

Este documento explica las decisiones técnicas del pipeline, sus trade-offs, cómo usé herramientas de IA durante el desarrollo y qué quedó deliberadamente fuera de alcance.

---

## 1. Decisiones técnicas y trade-offs

### 1.1 Interpretación del problema y semántica temporal

La consigna pide revenue por producto **y por fecha**, pero la fuente no ofrece una fecha de venta: los carts de DummyJSON no tienen ningún campo temporal. El eje temporal lo tiene que construir el pipeline, y esa es la decisión que condiciona todo el diseño.

**Supuestos adoptados** (derivados del enunciado, no explícitos en él):

- **La actualización de medianoche cierra el día anterior.** Los datos que la API expone a partir de las 00:00 UTC del día D+1 corresponden al día D. El snapshot capturado el 26/09 representa las ventas del 25/09.
- **Cada snapshot diario de carts representa las ventas de ese día (foto completa).** No se asume que la API sea acumulativa. Si lo fuera, gold debería calcular deltas entre snapshots consecutivos en lugar de agregar cada snapshot de forma independiente.
- **La API no tiene historia.** No acepta parámetros de fecha y solo devuelve el estado actual. Si los datos cambian a diario y no se puede consultar un día anterior, lo que no se capturó ese día se pierde. Esta es una inferencia a partir de la nota del enunciado y de la forma de la API.
- **Zona horaria: UTC.** "Medianoche" es ambiguo; se fija UTC y se documenta. Airflow también opera en UTC por defecto.

**Consecuencia central:** bronze no es solo una buena práctica de arquitectura medallion. Es el **único lugar donde existe la historia**, porque la fuente no la conserva. Todo lo que está aguas abajo de bronze es re-procesable; la extracción no lo es.

### 1.2 Arquitectura de capas

Se usa una arquitectura medallion de tres capas:

| Capa | Responsabilidad | Estrategia de escritura |
|---|---|---|
| **Bronze** | JSON crudo tal como vino de la API, con campos de auditoría | Append entre días; reemplazo dentro del mismo día (idempotente) |
| **Silver** | Aplanado, tipado, deduplicación; una fila por clave del grano | Upsert + dedup, con la clave según el grano de cada tabla |
| **Gold** | Agregado de negocio `product_daily_revenue` | Reemplazo por día (incremental) |

**Separación de responsabilidades:** Airflow (Python) solo extrae y carga en bronze. Las transformaciones bronze → silver → gold viven en dbt (SQL). Así, toda la lógica de transformación queda en un solo lugar, con tests y linaje, y bronze se mantiene crudo y desacoplado del schema.

### 1.3 Bronze

Una tabla por entidad: `bronze.products` y `bronze.carts`. **Un registro de la API = una fila.**

| Columna | Descripción |
|---|---|
| `id` | Clave de origen |
| `data` | JSON crudo completo (`jsonb`), sin modificar |
| `audit_event_timestamp` | Timestamp de última modificación provisto por el sistema origen, cuando está disponible (`meta.updatedAt` en products; `null` en carts, porque la fuente no lo provee) |
| `audit_ingestion_timestamp` | Momento en que el pipeline procesó el dato. Un único valor por carga de cada entidad (se toma justo antes de insertar), para que un snapshot nunca quede partido entre dos timestamps. Products y carts se cargan en tasks separadas, así que cada uno tiene su propio valor |
| `audit_logical_date` | Día al que pertenece el dato: la **fecha de negocio** resuelta por `resolve_business_date` (ver 1.6). En corridas programadas coincide con la fecha lógica de Airflow (`data_interval_start`); en corridas manuales Airflow no tiene fecha lógica, y el valor es el día anterior a `run_after`. En cualquier caso, la guarda de la extracción exige que sea el último día cerrado al momento de ejecutar |
| `audit_process_name` | Proceso que realizó la ingesta (`dag_id.task_id`), para trazabilidad |

**Por qué tres fechas y no dos.** Cada una responde una pregunta distinta:

| Campo | Pregunta | Quién la define |
|---|---|---|
| `audit_event_timestamp` | ¿Cuándo cambió el registro en el origen? | La fuente |
| `audit_ingestion_timestamp` | ¿Cuándo lo procesamos? | El reloj, al ejecutar |
| `audit_logical_date` | ¿A qué día pertenece? | Airflow, al programar la corrida (o quien dispara una corrida manual) |

En pipelines CDC, el timestamp del evento de origen suele alcanzar para ubicar el dato en el tiempo, y la fecha lógica no hace falta. Acá la fuente no provee ninguna fecha para carts, y con la interpretación de "día cerrado" la fecha del dato (D) y la de ejecución (D+1) caen **siempre** en días distintos. Derivar la fecha de negocio de `audit_ingestion_timestamp` requeriría una regla implícita (`- 1 día`) acoplada al horario de ejecución, que se rompería en silencio si cambiara el schedule. `audit_logical_date` hace explícita esa relación y no cambia con reintentos ni reruns. La única excepción es la corrida manual: ahí se usa el día anterior a `run_after`, porque Airflow no provee otra referencia. Es una regla visible, documentada y testeada en un único lugar (`resolve_business_date`), no una convención repartida aguas abajo.

**Decisiones adicionales:**

- **Unicidad:** `(id, audit_logical_date)`. El mismo `id` se repite una vez por día, porque cada día se captura el snapshot completo.
- **Idempotencia:** la carga hace `DELETE WHERE audit_logical_date = D` + `INSERT` en una sola transacción. Un reintento del mismo día reemplaza la carga anterior y nunca duplica.
- **Sin `batch_id`:** como la carga es idempotente por `audit_logical_date`, nunca coexisten dos cargas del mismo día y un `batch_id` no agrega información. Si la carga fuera append puro, sería necesario para distinguir intentos.
- **Sin columna de origen:** el origen queda implícito en el nombre de la tabla. Repetirlo en cada fila no aporta.
- **Paginación explícita:** la API devuelve 30 registros por página por defecto (194 productos y 208 carts al momento de la verificación). La extracción recorre todas las páginas y, al terminar, **valida que la cantidad de registros obtenidos coincida con el `total` que informa la API**. Si no coincide, la tarea falla. El envoltorio de paginación (`total`, `skip`, `limit`) no se persiste como fila.
- **Configuración por entidad:** la ubicación del timestamp de origen (`meta.updatedAt` o ninguno) se define en una configuración por entidad, así el código de ingesta es genérico y agregar una entidad nueva no requiere código nuevo.

### 1.4 Silver

**Aplanado en dbt (SQL) y no en Python.** Si se aplanara durante la ingesta, bronze dejaría de ser crudo y la ingesta quedaría acoplada al schema. En dbt, cualquier cambio de lógica se re-procesa desde bronze sin volver a llamar a la API. El costo: el SQL sobre JSON es menos legible y los errores de tipo aparecen al ejecutar dbt, no al ingerir.

**Modelos y granos:**

| Modelo | Grano | Clave del upsert | Comportamiento ante un día nuevo |
|---|---|---|---|
| `silver.products` | `product_id` | `product_id` | Se reemplaza la fila con el estado más reciente |
| `silver.carts` | `(snapshot_date, cart_id)` | `snapshot_date` | Se agregan filas; los días anteriores no se tocan |
| `silver.cart_items` | `(snapshot_date, cart_id, line_number)` | `snapshot_date` | Igual que carts |

- **Products como estado actual (SCD1):** es una dimensión y para el reporte interesa el valor vigente. Si hiciera falta historia de precios o títulos, se puede construir un SCD2 porque bronze conserva todos los snapshots.
- **Carts con historia diaria:** es el hecho y de él sale la fecha de gold. Un upsert solo por `cart_id` pisaría el día anterior y destruiría la historia.
- **Carts y cart_items separados:** se evita repetir los totales del cart en cada línea y se habilita un control de reconciliación entre la suma de líneas y el total del cart.
- **Upsert de carts como reemplazo del día completo** (estrategia incremental `delete+insert` por `snapshot_date`) en lugar de un `MERGE` fila por fila. Si en un reintento un cart deja de aparecer, el `MERGE` dejaría la fila vieja huérfana; el reemplazo del día deja el día exactamente igual al último snapshot.
- **`line_number`:** posición del producto dentro del array del cart (`jsonb_array_elements ... WITH ORDINALITY`). Es necesario porque `(snapshot_date, cart_id, product_id)` **no es único**: en la verificación de la fuente, el cart 7 contiene el producto 56 en dos líneas separadas, y en el snapshot del 24/09 había 12 pares (cart, producto) repetidos sobre 800 líneas.
- **Líneas duplicadas:** se mantienen separadas en silver por fidelidad al origen (no se puede saber si son un error o líneas legítimas) y se consolidan en gold al agregar.
- **Montos en `numeric`:** la fuente trae errores de punto flotante (por ejemplo, `99.94999999999999`). Todo lo monetario se castea a `numeric(12,2)`.
- **Título y precio de venta tomados del cart**, no del catálogo: representan el momento de la venta; el catálogo puede cambiar después.
- **Materialización: todo incremental por watermark de ingesta** (ver 1.6). Cada fila de silver guarda en `ingested_at` el `audit_ingestion_timestamp` de la carga de bronze de la que viene.
  - **`carts` y `cart_items`:** reemplazan días completos (`delete+insert` por `snapshot_date`).
  - **`products`:** reemplaza productos (`delete+insert` por `product_id`). Al principio se había decidido como tabla completa ("para 194 productos, un merge no justifica su complejidad"). El argumento miraba el tamaño de un snapshot, pero no que bronze acumula 194 filas por día: la reconstrucción completa leía toda la historia de bronze en cada corrida.
- **SCD1 incremental de `products`:** igual que carts, lee de bronze solo las filas cargadas después del máximo `ingested_at` y reemplaza esos productos (`delete+insert` por `product_id`).
  - **El más reciente dentro del lote:** si hay varios días pendientes en un mismo build (recuperación), gana el snapshot más reciente por producto (`row_number` por `snapshot_date desc, ingested_at desc`).
  - **Productos que dejan de aparecer:** conservan su último estado conocido, porque no vienen en el lote.
  - **No hace falta defenderse de re-cargas de días viejos:** la guarda de la extracción garantiza que bronze solo recibe el día recién cerrado (ver 1.6). Una carga más nueva siempre es un snapshot más nuevo.

### 1.5 Gold

**`gold.product_daily_revenue`** con grano `(product_id, date)`.

| Columna | Definición |
|---|---|
| `product_id` | Identificador del producto |
| `product_title` | Título del catálogo (`silver.products`); fallback al título del cart si el producto no existe en el catálogo |
| `date` | `snapshot_date` del cart: día en que se observó la venta |
| `units_sold` | `sum(quantity)` |
| `revenue` | **Neto de descuentos** (`sum(line_discounted_total)`): lo efectivamente cobrado |
| `gross_revenue` | Bruto (`sum(line_total)`): precio de lista × cantidad |
| `carts_count` | Cantidad de carts distintos que incluyeron el producto |
| `ingested_at` | Máximo `ingested_at` de `silver.cart_items` para esa fecha: el watermark de gold |

- **`revenue` neto + `gross_revenue`:** el enunciado exige una columna `revenue`; se define como neto por ser lo cobrado y se expone el bruto para visibilizar el descuento. La definición queda documentada en el `schema.yml` de dbt.
- **Título del catálogo con `left join`:** sin perder revenue si un producto no está en el catálogo (en ese caso se usa el título del cart).
- **Efecto del incremental sobre `product_title`:** el título se toma del catálogo vigente **al momento de procesar** cada día. Los días ya procesados conservan ese título; si un producto cambia de nombre, el cambio se refleja solo en los días nuevos, salvo que se haga un full refresh, que reescribe toda la historia con el título actual. Un cambio en `silver.products` no dispara el watermark de gold, porque gold solo mira `silver.cart_items`. Es consistente con cómo se procesa cada día y está cubierto por un test.
- **Tabla dispersa:** un producto sin ventas en un día no tiene fila. Cruzar un calendario con todos los productos multiplicaría filas sin información.
- **Materialización incremental por `date`**, coherente con silver, con su propio watermark contra `silver.cart_items` (ver 1.6).

### 1.6 Orquestación y scheduling

- **Schedule diario a las 00:30 UTC:** margen de gracia de 30 minutos después de la actualización de la fuente. DummyJSON no expone ninguna señal de disponibilidad (endpoint de metadata, archivo marker), así que el margen es la única herramienta disponible.
- **Alineación con la semántica de Airflow:** la corrida que se ejecuta el día D+1 a las 00:30 cubre el intervalo que empieza el día D. La fecha lógica coincide con la fecha de negocio sin transformaciones adicionales.
- **Timetable explícito (`CronDataIntervalTimetable`), hallazgo de Airflow 3:** en Airflow 3, `[scheduler] create_cron_data_intervals` es `False` por default, así que un schedule cron escrito como string (`"30 0 * * *"`) se interpreta como `CronTriggerTimetable`: **no hay intervalo de datos** y `logical_date`, `data_interval_start`, `data_interval_end` y `run_after` valen lo mismo, el momento del disparo. Se detectó en la primera corrida real: la corrida del 25/09 00:30 cargó bronze con `audit_logical_date = 2026-09-25`, cuando por la semántica de "día cerrado" esos datos son del 24/09. El punto anterior asumía el comportamiento de Airflow 2. Por eso el DAG declara `schedule=CronDataIntervalTimetable("30 0 * * *", timezone="UTC")`: la corrida del D+1 00:30 tiene intervalo [D 00:30, D+1 00:30) y `logical_date = D`, y así vuelve a valer la alineación sin reglas implícitas de "-1 día". Verificado con una corrida real (`logical_date` y `data_interval_start` = 24/09, bronze con `audit_logical_date = 2026-09-24`) y cubierto por el test de integridad del DAG, que falla si el timetable no es de intervalos.
- **`catchup=False`:** la API no tiene historia. Un backfill capturaría los datos de hoy y los etiquetaría con fechas pasadas: datos incorrectos que parecen correctos. La extracción no es re-ejecutable hacia atrás; las transformaciones sí, desde bronze.
- **Activo al crearse (`is_paused_upon_creation=False`):** la plataforma mantiene el default de Airflow (DAGs nuevos pausados), pero este DAG se declara activo. No hay una carga inicial distinta de las siguientes: cada corrida trae la foto completa. Con `catchup=False`, al levantar el stack el scheduler crea una sola corrida para el último intervalo cerrado, que es la que habría corrido a las 00:30, con los mismos datos. Así el evaluador ve el pipeline funcionando sin pasos manuales. **Caso borde:** si el stack se levanta entre las 00:00 y las 00:30 UTC, el último intervalo completo es el de anteayer. La guarda de la extracción hace fallar esa corrida en lugar de etiquetar mal los datos (ver "Guarda de la fecha de negocio"); a las 00:30 corre la del día correcto. En un entorno productivo el DAG quedaría pausado y lo habilitaría una persona después del deploy.
- **Reintentos con backoff** ante fallas de la API o respuestas incompletas: en el cliente HTTP (429 y 5xx, con backoff exponencial y timeout) y a nivel task (2 reintentos con backoff exponencial). Una fecha de negocio que no es el último día cerrado falla sin reintentos (`AirflowFailException`), porque reintentar no la corrige. Las tasks de dbt tienen **un solo reintento**: cubre errores transitorios de conexión con el warehouse, y un test de datos que falla es determinístico, así que más reintentos solo demorarían la falla (con 2 reintentos y backoff, unos 6 minutos).
- **Toda espera tiene tope:**
  - Timeout por request HTTP (5 s de conexión y 30 s de lectura).
  - `connect_timeout` de 10 s hacia Postgres.
  - `execution_timeout` de 10 minutos por task. Una corrida normal tarda unos segundos.
  - El header `Retry-After` se ignora a propósito, porque urllib3 no lo limita y un valor de horas frenaría la task; ante un 429 se aplica nuestro backoff acotado.

  El motivo es que, con `max_active_runs=1`, una task colgada bloquearía la corrida del día siguiente, y como la fuente no tiene historia, ese día se perdería. Una falla rápida es recuperable; una task colgada en silencio, no.
- **Una extracción fallida no toca bronze:** se descarga el snapshot completo y se valida antes de abrir la conexión a la base. Si la API falla a mitad de camino, el `DELETE` del día nunca se ejecuta y la carga anterior queda intacta. Lo cubre un test.
- **Fallas visibles:** un `on_failure_callback` registra un log estructurado con el DAG, la task, la fecha de negocio y el error. No se conectan canales externos (Slack, email) porque requerirían credenciales, que el enunciado excluye.
- **Resolución de la fecha de negocio** (`resolve_business_date`). Verificado con triggers reales (REST API, que es la que usa la UI, y CLI) en Airflow 3.3.2: las corridas manuales llegan con `logical_date = None` y `data_interval = None`; solo traen `run_after`.
  - **Corrida programada:** fecha de `data_interval_start`, en UTC.
  - **Cualquier otra corrida** (manual o disparada desde otro DAG): el día anterior a `run_after`, en UTC.
  - **No hay parámetro para elegir la fecha.** Se eliminó `conf["business_date"]` (ver "Guarda de la fecha de negocio").

#### Guarda de la fecha de negocio

La API no tiene historia: lo que devuelve pertenece siempre al **último día cerrado**, el día anterior al momento real de ejecución en UTC. Cargar con cualquier otra fecha etiquetaría esos datos con un día que no es el suyo. Por eso, antes de extraer, la task exige `fecha de negocio == día anterior a ahora (UTC)`, para **cualquier tipo de corrida**. Si no coincide, falla sin reintentos (`AirflowFailException`); reintentar no cambia la fecha.

| Situación | Resultado |
|---|---|
| Corrida programada en horario (D+1 00:30) | pasa: carga D |
| Corrida programada demorada, pero antes de la medianoche siguiente | pasa: la API sigue exponiendo D |
| Corrida programada que ejecuta después de la medianoche siguiente | falla: la API ya expone D+1 |
| *Clear* de una corrida vieja, o un backfill | falla |
| Corrida manual | pasa: carga el día anterior a su disparo |
| Corrida manual disparada antes de medianoche que ejecuta después | falla |
| Stack levantado entre las 00:00 y las 00:30 UTC (el intervalo completo es el de anteayer) | falla, en lugar de etiquetar mal; a las 00:30 corre la del día correcto |

Verificado con corridas reales: la programada y una manual pasan, y una corrida de un intervalo viejo (creada con `airflow backfill create`) falla en el primer intento sin tocar bronze, con el `task_failed` del callback indicando la fecha rechazada.

**Consecuencia:** la única re-carga legítima de un día es el reintento de ese mismo día, dentro de su ventana. **Supuesto documentado:** bronze solo recibe cargas del día recién cerrado, y la guarda lo garantiza para todo lo que pasa por el DAG. Si se viola por fuera del DAG (una carga manual directa a bronze o un `UPDATE` de fechas), se repara con full refresh (ver "Reprocesamiento").

#### Procesamiento incremental por watermark de ingesta (silver y gold)

dbt **no recibe ninguna fecha**: el incremental lo resuelve con el patrón estándar de dbt, un filtro dentro de `is_incremental()` más la estrategia `delete+insert`. La ventana es "todo lo cargado después de lo último que ya se procesó":

```sql
where audit_ingestion_timestamp > (select coalesce(max(ingested_at), '-infinity') from {{ this }})
```

- **Silver** (`carts`, `cart_items`): lee de bronze solo las filas con `audit_ingestion_timestamp` posterior al máximo `ingested_at` que ya tiene.
- **Silver `products`:** el mismo filtro, con reemplazo por `product_id` y el snapshot más reciente por producto dentro del lote (ver 1.4).
- **Gold:** el mismo filtro contra `silver.cart_items`.
- **Por qué alcanza con filtrar filas, sin calcular antes qué días procesar:** el loader reemplaza el día completo en bronze con un único timestamp por carga. Entonces las filas nuevas son **snapshots completos** de los días que cambiaron. `delete+insert` con `unique_key` = fecha borra de la tabla destino los días que vienen en el lote y los reinserta: dbt deduce qué días reemplazar a partir del contenido del lote. En silver, todas las líneas de un día comparten el mismo `ingested_at`, así que gold recibe días completos igual.
- **Lectura acotada:** `bronze.products` y `bronze.carts` tienen un índice por `audit_ingestion_timestamp`. Silver y gold tienen índices por su columna de watermark (config `indexes` de dbt), y `silver.products` además un índice único por `product_id`. La consulta del máximo lee una sola entrada del índice, y bronze se lee solo en el rango nuevo; está verificado con `EXPLAIN`. Solo el full refresh recorre todo.
- **Cómo se ejecuta:** las tasks del DAG corren `dbt build --selector silver` y `dbt build --selector gold`, sin `--vars`. En el primer build, o en un full refresh, se procesa todo.
- **Supuesto que lo hace correcto:** los timestamps de ingesta crecen en el mismo orden en que se confirman las cargas. Si una carga tomara su timestamp, tardara en hacer commit y en el medio dbt procesara otra más nueva, la primera quedaría por debajo del watermark y no se procesaría. Acá no puede pasar: hay un solo escritor (el DAG), `max_active_runs=1`, y dbt corre después de las extracciones. Cargar bronze desde otro proceso en paralelo rompería el supuesto.

La primera versión comparaba **día por día** (el último timestamp de cada día en bronze contra el de silver). Funcionaba igual, pero agregaba bronze completa en cada corrida para detectar los días pendientes, y necesitaba un paso previo (macro `pending_dates`). El filtro global es la forma idiomática de dbt y lee solo lo nuevo.

Resultado en cada situación:

| Situación | Qué procesa dbt |
|---|---|
| Corrida programada del D+1 | el día D, recién cargado |
| Reintento del mismo día (por ejemplo, después de una falla parcial) | ese día, con la carga nueva |
| Silver falló un día y al siguiente corre bien | los dos días: se recupera solo |
| `dbt build` a mano sin cargas nuevas | nada |

**Alternativa descartada: pasar la fecha a dbt con `--vars '{"logical_date": ...}'`.**
- Obligaba a transportar desde Airflow un valor que ya está persistido en bronze (`audit_logical_date`).
- En las corridas manuales, que no tienen intervalo de datos, había dos malas opciones: duplicar en Jinja la lógica de `resolve_business_date`, o acoplar dbt al resultado (XCom) de una task de extracción.
- Con watermark, dbt depende solo de las tablas, se recupera automáticamente de días fallidos, y una ejecución manual de dbt procesa lo correcto.

**Alternativa descartada: estrategia `microbatch` de dbt (1.9+).** Es la opción más nativa: dbt parte el trabajo en lotes diarios, los itera y reintenta cada lote por separado. Pero decide qué procesar por una **ventana de tiempo relativa al momento de ejecución** (por ejemplo, los últimos N días), no por lo que se cargó:
- un día que silver no procesó y quedó fuera de la ventana no se recupera;
- los días dentro de la ventana se reprocesan en cada corrida aunque no hayan cambiado.

Encaja con fuentes que tienen timestamp de evento; con días que se reemplazan completos, el watermark de ingesta es más preciso.

**Costos del watermark:**
- **El comando del DAG ya no explicita qué día procesa.** Se compensa con los logs de dbt (`INSERT 0 N` por modelo) y con `ingested_at` en silver y gold, que permite rastrear de qué carga viene cada fila.
- **Requiere serializar las corridas:** el estado es compartido (lo que ya está en silver y gold), y dos corridas simultáneas podrían procesar el mismo día en paralelo. El DAG tiene `max_active_runs=1`.
- **No hay override de fechas** (`force_dates` o similar). Las reparaciones se hacen con full refresh (ver abajo).

`resolve_business_date` no cambia: sigue decidiendo con qué fecha se carga bronze.

**Selección de tests por capa:** los selectores `silver` y `gold` (`dbt/selectors.yml`) incluyen los modelos y los tests singulares de cada capa, y el compose define `DBT_INDIRECT_SELECTION=cautious`. Con el modo por defecto (*eager*), el build de silver incluía el test de reconciliación gold contra silver y lo corría antes de reconstruir gold, lo que daba un falso error cada vez que llegaba un día nuevo. Con *cautious* solo, ese test no quedaba seleccionado en ningún build; por eso los selectores listan los tests singulares por ruta.

#### Reprocesamiento: `dbt build --full-refresh`

El watermark detecta **cargas nuevas en bronze**. No detecta cambios que no pasan por una carga nueva, y esos casos se reparan con un full refresh, que reconstruye silver y gold completos desde bronze:

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector silver --full-refresh
```

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector gold --full-refresh
```

- **Bug corregido en la lógica de silver o gold:** bronze no cambió, así que el watermark no marca ningún día como pendiente. El full refresh reprocesa todo con la lógica nueva.
- **Corrección de etiqueta en bronze:** un `UPDATE` de `audit_logical_date` no cambia `audit_ingestion_timestamp`, así que el watermark tampoco lo detecta, y el día corregido quedaría con los datos viejos en silver. El full refresh lo corrige.
- **Cargas a bronze por fuera del DAG** que violen el supuesto de la guarda (por ejemplo, un día viejo cargado después de uno más nuevo): el incremental podría dejar en `silver.products` un estado más viejo que el vigente. El full refresh recalcula todo desde bronze.

Hay un test que verifica que el full refresh reconstruye silver y gold desde bronze y que el resultado coincide con el incremental.

### 1.7 Calidad de datos

- **Ingesta:** cantidad de registros extraídos igual al `total` informado por la API.
- **Silver (tests de dbt):**
  - Unicidad del grano de cada modelo.
  - `not_null` en claves y montos.
  - `quantity > 0` y `unit_price >= 0`.
  - Integridad referencial `cart_items.product_id` → `products`, con severidad `warn`.
  - Reconciliación: la suma de `line_total` por cart coincide con `carts.total`, con tolerancia de centavos.
- **Gold (tests de dbt):**
  - Unicidad de `(product_id, date)`.
  - `not_null` en las columnas requeridas.
  - `revenue >= 0` y `revenue <= gross_revenue`.
  - **Reconciliación entre capas:** la suma diaria de `revenue` en gold coincide con la suma de `discounted_total` de `silver.carts` para el mismo día.

### 1.8 Stack tecnológico

**Criterio general:** el requisito excluyente es que el proyecto levante en un entorno que no controlo. Cada herramienta adicional es un punto más de falla, así que se priorizó portabilidad y simplicidad sobre sofisticación.

**Base de datos: PostgreSQL, en un contenedor propio (`warehouse`).**

En este pipeline hay varios procesos independientes que usan la base, a veces al mismo tiempo: la extracción en Python, dbt y quien valida el resultado con una query. PostgreSQL es un servidor pensado exactamente para eso: múltiples conexiones concurrentes, cada una con sus propias transacciones.

DuckDB se descartó porque es una base embebida: la base es un archivo que abre el proceso que la usa, y **solo un proceso puede escribir a la vez**. Si el DAG está corriendo y alguien abre el archivo para validar, uno de los dos se bloquea. Además, compartir ese archivo entre contenedores mediante un volumen es frágil.

Lo que se resigna al elegir PostgreSQL: DuckDB es más rápido para consultas analíticas y no necesita un contenedor extra. Con este volumen de datos la performance no es diferencial; la robustez ante accesos concurrentes sí. PostgreSQL suma además `jsonb`, que permite guardar el JSON crudo en bronze y aplanarlo en SQL con operadores maduros.

El warehouse se separa del PostgreSQL de metadata de Airflow: si uno tiene un problema, no afecta al otro, y refleja la separación habitual en producción.

**Orquestación: Apache Airflow 3 con `LocalExecutor`.** Airflow 2 está fuera de soporte activo. El `docker-compose.yaml` oficial usa `CeleryExecutor` con Redis y workers, lo cual es innecesario para un pipeline de este tamaño: se usa `LocalExecutor` para reducir contenedores y puntos de falla. Se descartó `airflow standalone` por estar orientado solo a desarrollo.

**Transformaciones: dbt-core + dbt-postgres, en un entorno virtual aislado dentro de la imagen de Airflow, invocado con `BashOperator`.** El entorno virtual evita conflictos de dependencias entre Airflow y dbt. Alternativas descartadas:

- **Astronomer Cosmos:** agrega una dependencia con su propia matriz de compatibilidad sin un beneficio necesario para este alcance.
- **`DockerOperator` con una imagen de dbt:** requiere montar el socket de Docker, lo que es frágil entre sistemas operativos y tiene implicancias de seguridad.

Las dependencias de dbt (`dbt deps`) se instalan al construir la imagen, no al ejecutar:
- `dbt_utils` 1.4.1, fijado por `package-lock.yml`, versionado.
- Se instalan en `/opt/dbt/packages` (`packages-install-path`), fuera del directorio del proyecto, porque el proyecto se monta como bind mount de solo lectura y taparía los paquetes instalados adentro.

dbt se invoca sin fechas: cada capa procesa lo pendiente según el watermark de ingesta (ver 1.6).

**Extracción: módulo Python independiente de Airflow**, invocado desde tasks de TaskFlow. Usa `requests` con reintentos, backoff y timeout explícitos, y `psycopg` para la carga transaccional. Al estar desacoplado de Airflow, se puede testear sin levantar el orquestador.

**Versiones fijas:** imágenes con tag exacto, Airflow instalado con su archivo de *constraints* oficial y dependencias Python pineadas.

- Airflow `3.3.2` (`apache/airflow:3.3.2-python3.12`) y PostgreSQL `16.15-bookworm` en ambos contenedores (la misma major que el compose oficial).
- El venv de dbt se fija con un lock completo (`pip freeze`, incluidas las dependencias transitivas) en `requirements/dbt.txt`.

**Detalles del compose** (partiendo del `docker-compose.yaml` oficial de 3.3.2):

- **Componentes:** metadata DB, warehouse, `airflow-init`, api-server, scheduler y dag-processor. Con `LocalExecutor` las tasks corren dentro del scheduler, así que no hacen falta Redis, workers ni Flower. **Sin triggerer**, porque no hay tasks diferibles; el health de la API lo reporta como `null`.
- **Autenticación:** `FabAuthManager`, igual que el compose oficial. Usuario `airflow`/`airflow`, valor por defecto de desarrollo.
- **Sin `.env` obligatorio:** todos los valores tienen default en el compose. El código (`dags/`, `src/`, `dbt/`) se monta como bind mount de solo lectura; los logs de Airflow van a un volumen nombrado y los artefactos de dbt a `/tmp`. Como nada escribe sobre el host, no hace falta configurar `AIRFLOW_UID` en Linux.
- **Puertos del host poco comunes por default:** UI en `18080` (`AIRFLOW_PORT`) y warehouse en `15432` (`WAREHOUSE_PORT`). La base de metadata no se expone. Los puertos habituales (`8080`, `5432`, `5433`) suelen estar ocupados por otros servicios de desarrollo; con puertos altos y fijos, levantar el proyecto sigue siendo `docker compose up` y la URL del README no cambia. En el caso improbable de que choquen, se cambian con una variable de entorno sin editar archivos. Se descartaron dos alternativas: el puerto aleatorio asignado por Docker, porque obliga a consultar la URL en cada arranque, y un script que busque un puerto libre, porque suma un punto de falla que depende del sistema operativo.

### 1.9 Testing, CI y observabilidad

**Tests unitarios (`pytest`):**

- **Extracción con la API mockeada:** paginación completa, validación contra el `total`, reintentos ante errores y registros incompletos. No dependen de DummyJSON.
- **Resolución de la fecha de negocio:** corridas programadas, corridas manuales con y sin parámetro explícito.
- **Integridad del DAG:** el DAG carga sin errores de import y tiene las tasks y dependencias esperadas.

**Dónde corren:** en Docker, con una etapa `dev` del mismo Dockerfile de Airflow (la imagen de runtime más `ruff`, `pytest` y `responses`), expuesta como el servicio `tests` del compose (profile `dev`): `docker compose run --rm tests`. Los tests corren con las mismas versiones que el runtime y no hace falta instalar Python en el host. Los tests del loader corren contra el Postgres del warehouse, cada uno en un schema temporal que se crea y se borra.

**Tests de datos (dbt):** los descritos en 1.7, ejecutados como parte de cada corrida del pipeline. Un test fallido en silver impide construir gold.

**Tests de integración de dbt (`pytest`, con fixtures):** cada test crea una base de datos temporal en el Postgres del warehouse, aplica el mismo SQL de init, carga fixtures en bronze con el loader real y corre dbt con los mismos selectores que el DAG. Escenarios:
- **Watermark:** después de cargar A y C y construir, se carga B, se recarga A y se vuelve a construir. Se reprocesan exactamente A y B (C no se toca), y un build sin cargas nuevas no procesa nada.
- **Recuperación:** un día que silver nunca construyó se procesa en el siguiente build, junto con el día nuevo.
- **Full refresh:** reconstruye silver y gold completos y el resultado coincide con el incremental.
- **Gold:** consolida líneas duplicadas y usa el título del cart para un producto que no está en el catálogo; ese caso solo genera un warning de integridad referencial.
- **`product_title`:** el efecto del incremental descrito en 1.5, y cómo lo cambia un full refresh.
- **Calidad:** un dato roto (el total de un cart que no coincide con sus líneas) hace fallar el build de silver.
- **SCD1 de `products`:**
  - un snapshot más nuevo actualiza el producto;
  - con dos días pendientes en el mismo build gana el más reciente;
  - un producto que desaparece conserva su último estado;
  - solo se reescriben los productos que vienen en el lote;
  - el full refresh coincide con el incremental.

Los escenarios cargan los días en el orden que garantiza la guarda: cada día después del anterior, y la única re-carga es el reintento del mismo día.

**Guarda de la fecha de negocio (unitarios):** corrida programada en horario y demorada dentro del día, corrida programada después de la medianoche siguiente, *Clear* de una corrida vieja, arranque entre las 00:00 y las 00:30, corrida manual, y corrida manual que cruza la medianoche.

Para saber qué días o productos se reprocesaron, los tests comparan `xmin`, la columna de sistema de Postgres que cambia cuando una fila se vuelve a insertar. No hace falta agregar columnas técnicas a los modelos. Los tests se validaron con mutaciones y fallan en cada caso:
- **Filtro de watermark:** sin el filtro, o comparando con `>=` en lugar de `>`.
- **`products`:** con el orden de recencia invertido, o sin el filtro incremental.
- **Guarda:** desactivada, o rechazando solo fechas futuras.

**Fixtures** (`tests/fixtures/`): 3 carts y 6 productos reales de la API. Cubren un producto repetido en dos líneas del mismo cart (el 110 en el cart 38), un producto vendido que no está en el catálogo (el 161) y un producto del catálogo sin ventas (el 1).

**CI (GitHub Actions), en cada push:**

1. **`ruff`:** lint del código Python.
2. **`pytest`:** tests unitarios.
3. **`dbt build`** contra un PostgreSQL temporal levantado como servicio del job.

CI **no llama a la API real**: bronze se carga con fixtures versionadas en el repo (una muestra pequeña de products y carts que incluye casos borde como líneas de producto duplicadas dentro de un cart). Así las transformaciones y los tests de calidad se validan con datos controlados y el resultado es determinístico. CI reproduce de forma automatizada lo mismo que hará el evaluador: clonar el repo en una máquina limpia y ejecutarlo.

**Observabilidad:** la extracción emite **logs estructurados (JSON)** con entidad, fecha de negocio, páginas recorridas, registros obtenidos frente al total esperado y duración. Las fallas se registran mediante `on_failure_callback` (ver 1.6).

- **JSON anidado en los logs de Airflow (trade-off aceptado):** el paquete `ingestion` escribe cada evento como un mensaje JSON con el `logging` estándar de Python. Airflow 3 escribe cada línea de log de una task como un JSON propio (structlog) y pone el mensaje en su campo `event`. Como nuestro mensaje es un string, queda escapado: `{"event": "{\"records\": 194, ...}", "task_id": ..., ...}`. Se puede leer (un `json.loads` del campo `event`), pero no queda plano. Para que los campos quedaran en el primer nivel habría que loguear con structlog, el logger de Airflow, y eso acoplaría el paquete de ingesta a Airflow, cuando el diseño pide que sea independiente. Fuera de Airflow (scripts, tests) el JSON sale plano.

---

## 2. Flujo de trabajo con IA

**[PENDIENTE: completar con la etapa de implementación]**

**Diseño (Claude, sesión de chat).** Usé Claude para discutir la interpretación del problema y el diseño de capas antes de escribir código. Trabajé en modo de discusión: la IA proponía, yo cuestionaba y las decisiones finales se tomaron sobre esa base. Casos concretos donde el output se validó o se corrigió:

- **Verificación contra la fuente real.** Antes de fijar el diseño se consultaron los endpoints reales. La IA había estimado de memoria que había 50 carts; la API informaba 208. También apareció el caso del cart 7 con un producto duplicado, que invalidaba el grano `(snapshot_date, cart_id, product_id)` propuesto inicialmente y obligó a agregar `line_number`.
- **Campos de auditoría.** Rechacé varias propuestas iniciales:
  - Que `audit_event_timestamp` se extrajera recién en silver. Lo mantuve en bronze con una definición técnica, sin semántica de negocio.
  - Que se guardara el origen en cada fila. Es redundante con el nombre de la tabla.
  - Que se incluyera un `batch_id`. Es redundante con una carga idempotente por fecha lógica.
- **Fecha lógica.** Cuestioné la necesidad de una tercera fecha, porque en mi experiencia con CDC alcanzaban dos. La discusión dejó claro que la diferencia no está en los reintentos sino en la semántica: bajo la interpretación de "día cerrado", la fecha del dato y la de ejecución difieren siempre, y la fecha lógica evita una regla implícita acoplada al schedule.
- **Nomenclatura.** Ajusté los nombres para que `revenue` y `gross_revenue` fueran explícitos y consistentes en inglés.

**Implementación (Claude Code).** [PENDIENTE: completar al cerrar las fases restantes]

Trabajé por fases, con la consigna de verificar cada una ejecutándola antes de avanzar y de consultar la documentación de la versión fijada de Airflow en lugar de asumir el comportamiento de Airflow 2. Casos concretos:

- **Semántica de schedules cron en Airflow 3 (fase 2).** El diseño (sección 1.6) asumía que la corrida del D+1 cubre el intervalo del día D, como en Airflow 2. El DAG se escribió con el schedule como string cron y los tests unitarios pasaban, porque probaban `resolve_business_date` con un `data_interval_start` que armaba el propio test. El error apareció recién en la primera corrida real: bronze quedó con `audit_logical_date = 2026-09-25` en vez de `2026-09-24`. Consultando la metadata de la corrida se vio que `logical_date`, `data_interval_start` y `data_interval_end` eran iguales, por el nuevo default `create_cron_data_intervals = False`. Se corrigió declarando `CronDataIntervalTimetable`, y se agregó un test de integridad que falla si el DAG vuelve a un timetable sin intervalos. Lección: los tests unitarios validan la lógica contra los supuestos del código; solo la ejecución real valida los supuestos contra la plataforma.
- **Corridas manuales.** El comportamiento de las corridas manuales (sin `logical_date` ni intervalo) se confirmó con triggers reales por REST y por CLI antes de darlo por cerrado, y no solo con la documentación.
- **Reintentos inútiles.** La primera verificación con un `business_date` inválido (en ese momento existía el parámetro) mostró que la task gastaba 3 intentos (unos 6 minutos de backoff) en un error de configuración. Se cambió para que falle en el primer intento.
- **Fecha de negocio en dbt: de `--vars` a watermark (fase 3).** La propuesta de pasar la fecha a dbt con `--vars` venía del diseño hecho con IA y se replicó sin cuestionarla. Al implementar las tasks de dbt, esa decisión obligaba a transportar la fecha desde Airflow, y la IA propuso agregar una task solo para resolverla. Fui yo quien detectó que el problema lo generaba el requisito mismo, al preguntar por qué dbt no leía la fecha de las tablas, si bronze ya la tenía persistida. De esa pregunta salió el procesamiento por watermark de ingesta (1.6), que simplifica el DAG y además recupera días fallidos.
- **Recorrido completo de bronze en cada corrida.** La primera implementación del watermark comparaba día por día, lo que obligaba a agregar bronze completa en cada corrida para detectar los días pendientes. Lo detecté al revisar la propuesta: en mi experiencia, un proceso incremental solo lee su ventana, y un recorrido completo corresponde a una carga completa. También cuestioné si hacía falta calcular los días en un paso previo en lugar de delegarle el incremental a dbt. Se reemplazó por el filtro idiomático de `is_incremental()` sobre el timestamp de ingesta, con índices (verificado con `EXPLAIN`), y se eliminó la macro. Se evaluó `microbatch` y se descartó (1.6).
- **`silver.products` había quedado afuera.** Al aplicar el watermark a carts y gold, `products` siguió como tabla completa, y leía toda la historia de bronze en cada corrida. Lo detecté preguntando por qué no se había modificado. Además, la IA había escrito en CLAUDE.md la regla "nunca agregar bronze completa" mientras dejaba un modelo que la violaba.
- **Defender un escenario que había que prohibir.** Para `products` incremental, la IA propuso una condición de "no retroceder" (unir el lote con las filas actuales) y una columna extra (`watermark_ingested_at`), para tolerar la re-carga de un día viejo después de procesar uno más nuevo. Se llegó a implementar y testear. Al revisarlo, vi que ese escenario era en sí un error: sin historia en la API, re-cargar "el 23" el día 25 trae los datos del 24 etiquetados como 23. Lo correcto era prohibirlo en la extracción, no tolerarlo aguas abajo. Se revirtieron la condición y la columna, y se agregó la guarda de la fecha de negocio (1.6). Lección: antes de agregar lógica defensiva, preguntar si el caso que defiende debería existir.
- **Eliminación de `conf["business_date"]`.** El parámetro para elegir la fecha de una corrida manual parecía una flexibilidad útil (recuperar un día), pero reabría el problema que `catchup=False` cierra: etiquetar con una fecha pasada los datos de hoy. Se eliminó; las corridas manuales siempre cargan el último día cerrado.
- **Tests que no probaban lo que decían.** Un test de reintentos ante `Retry-After` pasaba aunque se cambiara el código a respetar el header. La causa: la librería de mocks HTTP simula los reintentos sin ejecutar nunca la espera. Se detectó con una prueba de mutación (cambiar el código y confirmar que el test falle) y el test se reescribió. Desde entonces, los tests de comportamiento crítico (watermark, `Retry-After`) se validan con mutaciones.

---

## 3. Alcance

### Fuera de alcance deliberadamente

- **SCD2 de productos:** el historial de precios y títulos es reconstruible desde bronze, pero no aporta al objetivo.
- **Campos anidados de products** (`reviews`, `images`, `dimensions`, `tags`): siguen disponibles en bronze.
- **Flag `is_active`** para productos que dejan de aparecer en la API: hoy se conserva el último estado conocido.
- **Tabla gold densa** (productos × calendario con ceros).
- **Backfills de extracción:** no son posibles por la naturaleza de la fuente.
- **Alertas a canales externos** (Slack, email): requieren credenciales externas. Las fallas quedan registradas con logs estructurados.

### Qué haría distinto en un entorno productivo

- **Validar el schema contra la documentación o un contrato de datos con el dueño de la fuente**, y no contra una muestra de respuestas.
- **Usar una señal de disponibilidad del dato** (sensor sobre un marker, endpoint de metadata, tabla de control) en lugar de un margen de tiempo fijo.
- **Confirmar con el negocio la semántica de la fuente:** si la actualización de medianoche cierra el día anterior y si los snapshots son completos o acumulativos.
- **Si la fuente tuviera historia**, habilitar `catchup` y backfills: la extracción pasaría a ser re-ejecutable y el resto del diseño no cambiaría.
- **Almacenamiento y cómputo:** bronze en object storage con formato tabular abierto (Iceberg/Delta) particionado por fecha lógica, en lugar de una base relacional local.
- **Override opcional de fechas (`force_dates`)** para reprocesar días puntuales sin reconstruir todo, manteniendo el watermark como comportamiento por defecto. Hoy la única herramienta de reparación es el full refresh, que con volúmenes grandes sería caro.
- **Garantizar el supuesto del watermark con varios escritores:** si bronze se cargara desde más de un proceso en paralelo, los timestamps de ingesta dejarían de ser monótonos respecto del commit (ver 1.6). Haría falta un número de secuencia asignado al confirmar cada carga, por ejemplo en una tabla de control, o un margen de reprocesamiento sobre el watermark.
- **Alertas a un canal real** (Slack, email, PagerDuty) conectadas al `on_failure_callback`, y métricas del pipeline (registros por corrida, duración, frescura del dato) enviadas a un sistema de monitoreo.
