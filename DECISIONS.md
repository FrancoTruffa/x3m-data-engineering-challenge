# DECISIONS.md

Este documento explica las decisiones técnicas del pipeline, sus trade-offs, cómo usé herramientas de IA durante el desarrollo y qué quedó deliberadamente fuera de alcance.

## Resumen ejecutivo

- **Día cerrado, foto completa:** la fuente se actualiza a medianoche UTC, y se interpreta que esa actualización cierra el día anterior: lo que la API muestra el día 26 son las ventas del 25. Por eso la corrida del día D+1 a las 00:30 UTC carga el día D. Cada snapshot de carts se toma como la foto completa de las ventas de ese día, no como un acumulado (1.1).
- **Bronze es la única historia:** la API solo devuelve el estado actual, sin forma de pedir un día pasado. Lo que no se guarda ese día se pierde. Bronze guarda cada snapshot diario tal como vino, en JSON crudo, y es el único lugar donde queda la historia: silver y gold se pueden reconstruir desde bronze en cualquier momento, pero la extracción no se puede repetir para días pasados (1.1, 1.3).
- **Tres fechas en bronze:** cada fila registra cuándo cambió el dato en el origen (`audit_event_timestamp`, solo products lo trae), cuándo lo cargó el pipeline (`audit_ingestion_timestamp`) y a qué día de negocio pertenece (`audit_logical_date`). Las dos últimas nunca coinciden: el día D se carga el D+1. Como la API solo expone el último día cerrado, una guarda en la extracción hace fallar cualquier corrida que intente cargarlo con otra fecha (1.3, 1.6).
- **Schedule declarado con `CronDataIntervalTimetable`:** en Airflow 2, una corrida diaria representaba el intervalo del día anterior. En Airflow 3, un cron escrito como texto (`"30 0 * * *"`) representa solo el momento del disparo, sin intervalo, y los datos del día D quedarían etiquetados como D+1. Declarar el timetable de intervalos recupera la semántica correcta (1.6).
- **Incremental por watermark de ingesta:** silver y gold no se reconstruyen enteros cada día. Cada modelo recuerda hasta qué carga procesó (su "watermark": el `audit_ingestion_timestamp` más reciente que ya incorporó) y en la corrida siguiente lee de la capa anterior solo lo cargado después, usando índices y sin recorrer la historia. dbt no necesita que Airflow le pase ninguna fecha, y si un día falla, la corrida siguiente lo procesa sola (1.4, 1.6).
- **PostgreSQL y no DuckDB:** DuckDB es una base de un solo archivo, que admite un único proceso escribiendo a la vez. Acá varios procesos usan la base al mismo tiempo (la extracción, dbt y quien valide con una query), así que un servidor como PostgreSQL, que sí soporta conexiones concurrentes, evita ese problema. PostgreSQL además ofrece `jsonb`, un tipo JSON consultable con SQL, para guardar bronze crudo (1.8).
- **Reprocesamiento dirigido con `force_date`:** si se corrige la lógica de un modelo, se puede reconstruir en silver y gold un día puntual que ya está en bronze, pasándole a dbt esa fecha, sin volver a llamar a la API y sin reconstruir todo. La fecha se valida antes de modificar cualquier tabla: si no existe en bronze, falla sin tocar nada (1.6).
- **Chequeo de volumen bronze → silver:** un test compara, para el día recién procesado, cuántos carts y cuántas líneas por cart hay en bronze contra silver. Detecta filas que la transformación perdió o duplicó, algo que las reconciliaciones de montos no siempre ven (por ejemplo, un cart perdido entero no rompe ninguna suma). Mira solo la última carga para no releer toda la historia en cada corrida (1.7).
- **Dos DAGs con reglas de seguridad opuestas:**
  - el diario (`dummyjson_pipeline`) extrae de la API, y como la API solo tiene el día de hoy, nunca acepta una fecha elegida: carga siempre el último día cerrado, y reintenta ante fallas de red;
  - el de reprocesamiento (`dummyjson_reprocess`) nunca llama a la API, solo re-transforma lo que ya está en bronze, así que exige que se le indique la fecha. Es manual y no reintenta, porque sus fallas (una fecha inválida, un test que falla) no se arreglan reintentando.

  Los dos comparten un pool de Airflow de un solo slot, para que sus procesos de dbt nunca corran al mismo tiempo sobre las mismas tablas (1.6).

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
| **Bronze** | JSON crudo tal como vino de la API, con campos de auditoría | Se agrega un día nuevo por corrida. Si se recarga el mismo día (un reintento), sus filas se borran y se vuelven a insertar: nunca se duplican |
| **Silver** | JSON aplanado y tipado; una fila por clave del grano | Incremental: solo se procesan los días con cargas nuevas en bronze, y cada uno se reemplaza completo (`carts`, `cart_items`). En `products`, se reemplazan solo los productos que llegaron en la carga nueva |
| **Gold** | Agregado de negocio `product_daily_revenue` | Incremental: solo se recalculan los días con datos nuevos en silver, y cada uno se reemplaza completo con el agregado recalculado. Los días anteriores no se tocan |

**Cómo se escribe en silver y gold: `delete+insert`.** La idea es "de la tabla destino, borrar todo lo que corresponde a los bloques que trae el lote y poner en su lugar lo que trae el lote". No compara fila por fila: reemplaza bloques enteros. En `carts`, `cart_items` y gold el bloque es un día; en `products`, un producto. Ejemplo con `silver.carts`, que tiene el 25 y el 26, cuando se reintenta la carga del 26 y ahora trae solo los carts 1 y 2:

| Antes | Lote nuevo | Después |
|---|---|---|
| 25 · cart 1 · 100 | | 25 · cart 1 · 100 |
| 25 · cart 2 · 200 | | 25 · cart 2 · 200 |
| 26 · cart 1 · 110 | 26 · cart 1 · 115 | 26 · cart 1 · 115 |
| 26 · cart 2 · 210 | 26 · cart 2 · 205 | 26 · cart 2 · 205 |
| 26 · cart 3 · 50 | | *(ya no está)* |

1. **Armar el lote:** dbt ejecuta el `SELECT` del modelo con el filtro del watermark (ver 1.6) y guarda el resultado en una tabla temporal. Acá, las dos filas nuevas del 26.
2. **Borrar:** mira qué días hay en el lote (solo el 26) y borra de la tabla **todas** las filas de ese día, incluido el cart 3, aunque no venga en el lote. El 25 no se toca.
3. **Insertar:** copia el lote completo.

El día 26 queda exactamente igual a su última foto. Tres propiedades lo hacen seguro:
- **Qué bloques se reemplazan lo decide el lote.** El `unique_key` del modelo indica la columna del borrado. Si el lote trae un día, se reemplaza un día; si trae dos (una recuperación), dos; si viene vacío, no se borra nada.
- **Es atómico:** el borrado y la inserción van en una misma transacción. Si la inserción falla, el borrado se deshace y el bloque nunca queda a medio reemplazar.
- **Es idempotente:** procesar dos veces el mismo lote da el mismo resultado, sin duplicados. En eso se apoyan los reintentos y el reprocesamiento dirigido.

**Por qué reemplazo y no upsert + dedup.** Upsert + dedup es el patrón para fuentes que mandan **cambios** (CDC, APIs incrementales): cada lote trae solo algunos registros, así que hay que actualizar los existentes sin tocar el resto (upsert) y quedarse con la última versión cuando una clave viene repetida (dedup). DummyJSON manda otra cosa: **la foto completa de cada día**.
- **Entre días no hay nada que actualizar:** el grano de `carts` es `(snapshot_date, cart_id)`, y el día 26 agrega sus filas sin modificar las del 25.
- **Dentro de un día, un upsert dejaría basura:** la única re-carga posible es un reintento con la foto completa, y un upsert no borra lo que dejó de venir. En el ejemplo, el cart 3 quedaría huérfano, y lo mismo una línea que un cart dejó de tener. El reemplazo del día lo evita.
- **La deduplicación ya está resuelta antes de silver:** bronze tiene clave única `(id, audit_logical_date)`, la extracción valida que no haya ids repetidos y el loader reemplaza el día entero. A silver nunca le llegan dos versiones del mismo cart para el mismo día. Las líneas del mismo producto dentro de un cart no son duplicados: por eso existe `line_number` (ver 1.4).

**`silver.products` sí es upsert + dedup en la práctica,** porque representa **estado actual** (SCD1) y no fotos diarias. Si el lote trae varios días, se queda con el snapshot más reciente de cada producto (dedup). Después reemplaza solo los productos que llegaron, y los demás conservan su último estado (upsert). Para reemplazar una fila entera por clave, `delete+insert` y `merge` dan el mismo resultado; se usa `delete+insert` por consistencia con el resto.

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

| Modelo | Grano | Bloque que se reemplaza (`unique_key`) | Comportamiento ante un día nuevo |
|---|---|---|---|
| `silver.products` | `product_id` | `product_id` | Se reemplaza la fila con el estado más reciente |
| `silver.carts` | `(snapshot_date, cart_id)` | `snapshot_date` | Se agregan filas; los días anteriores no se tocan |
| `silver.cart_items` | `(snapshot_date, cart_id, line_number)` | `snapshot_date` | Igual que carts |

- **Products como estado actual (SCD1):** es una dimensión y para el reporte interesa el valor vigente. Si hiciera falta historia de precios o títulos, se puede construir un SCD2 porque bronze conserva todos los snapshots.
- **Carts con historia diaria:** es el hecho y de él sale la fecha de gold. Un upsert solo por `cart_id` pisaría el día anterior y destruiría la historia.
- **Carts y cart_items separados:** son dos granos distintos. Un cart tiene datos propios (usuario, totales) y adentro un array `products` de **ítems** o líneas: cada elemento es un producto con su cantidad, su precio y su total. La cantidad de líneas (`totalProducts`) no es la cantidad de unidades compradas (`totalQuantity`, la suma de las cantidades), y el mismo producto puede aparecer en más de una línea (ver `line_number`).
  - **Por qué no una sola tabla:** los datos del cart se repetirían en cada línea, y cualquier suma de sus totales quedaría multiplicada por la cantidad de líneas (un cart de 300 con dos líneas sumaría 600). Separados, cada número vive en un solo lugar.
  - **Qué habilita:** controlar que la suma de las líneas de cada cart coincida con el total que informa el cart (ver 1.7).
  - **Quién usa cada una:** gold agrega por producto a partir de `cart_items`.
- **Escritura de carts como reemplazo del día completo** (estrategia incremental `delete+insert` por `snapshot_date`, explicada en 1.2) en lugar de un `MERGE` fila por fila. Si en un reintento un cart deja de aparecer, el `MERGE` dejaría la fila vieja huérfana; el reemplazo del día deja el día exactamente igual al último snapshot.
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
- **Alineación con la semántica de Airflow:** el DAG declara `schedule=CronDataIntervalTimetable("30 0 * * *", timezone="UTC")`. La corrida que se ejecuta el día D+1 a las 00:30 cubre el intervalo [D 00:30, D+1 00:30), así que `data_interval_start` y la fecha lógica corresponden al día D y coinciden con la fecha de negocio sin reglas implícitas de "-1 día". El timetable se declara explícito porque en Airflow 3 un cron escrito como string se interpreta como `CronTriggerTimetable` (`create_cron_data_intervals = False` por default): la corrida es un disparo sin intervalo y la fecha lógica sería D+1. El test de integridad del DAG falla si el timetable no es de intervalos. Cómo se detectó: sección 2.
- **`catchup=False`:** la API no tiene historia. Un backfill capturaría los datos de hoy y los etiquetaría con fechas pasadas: datos incorrectos que parecen correctos. La extracción no es re-ejecutable hacia atrás; las transformaciones sí, desde bronze.
- **Activo al crearse (`is_paused_upon_creation=False`):** la plataforma mantiene el default de Airflow (DAGs nuevos pausados), pero este DAG se declara activo. No hay una carga inicial distinta de las siguientes: cada corrida trae la foto completa. Con `catchup=False`, al levantar el stack el scheduler crea una sola corrida para el último intervalo cerrado, que es la que habría corrido a las 00:30, con los mismos datos. Así el evaluador ve el pipeline funcionando sin pasos manuales. **Caso borde:** si el stack se levanta entre las 00:00 y las 00:30 UTC, el último intervalo completo es el de anteayer. La guarda de la extracción hace fallar esa corrida en lugar de etiquetar mal los datos (ver "Guarda de la fecha de negocio"); a las 00:30 corre la del día correcto. En un entorno productivo el DAG quedaría pausado y lo habilitaría una persona después del deploy.
- **Reintentos con backoff** ante fallas de la API o respuestas incompletas: en el cliente HTTP (429 y 5xx, con backoff exponencial y timeout) y a nivel task (2 reintentos con backoff exponencial). Una fecha de negocio que no es el último día cerrado falla sin reintentos (`AirflowFailException`), porque reintentar no la corrige. Las tasks de dbt tienen **un solo reintento**: cubre errores transitorios de conexión con el warehouse, y un test de datos que falla es determinístico, así que más reintentos solo demorarían la falla (con 2 reintentos y backoff, unos 6 minutos). La task de `dummyjson_reprocess` no tiene reintentos: es un DAG manual, quien lo dispara ve la falla enseguida, y sus fallas (fecha inválida o inexistente, un test que falla) son determinísticas. Con el reintento por defecto, una fecha inexistente tardaba 5 minutos más en fallar.
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

Verificado con corridas reales:
- **Pasan:** la corrida programada del 25/09, que se ejecutó sola el 26/09 a las 00:30, y una corrida manual del 26/09, que recargó el 25/09.
- **Fallan en el primer intento, sin reintentos:**
  - un *Clear* desde la API, que es la que usa la UI, de la corrida programada del 24/09 ejecutado el 26/09;
  - una corrida de un intervalo viejo creada con `airflow backfill create`.

  En los dos casos la task falla antes de llamar a la API: no hay ningún evento `extraction_started`. Bronze quedó idéntico, con el mismo hash antes y después, y el `task_failed` del callback indica la fecha rechazada junto con la que expone la API.

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
- **Lectura acotada:** `bronze.products` y `bronze.carts` tienen un índice por `audit_ingestion_timestamp`. Silver y gold tienen índices por su columna de watermark (config `indexes` de dbt), `silver.products` además un índice único por `product_id`, y `silver.carts` y `silver.cart_items` uno por `snapshot_date` (lo usan el `delete+insert` y el chequeo de volumen). La consulta del máximo lee una sola entrada del índice, y bronze se lee solo en el rango nuevo; está verificado con `EXPLAIN`. Solo el full refresh recorre todo.
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
- **Reparaciones fuera del watermark:** un día puntual con reprocesamiento dirigido (`force_date`), o todo con full refresh (ver abajo).

`resolve_business_date` no cambia: sigue decidiendo con qué fecha se carga bronze.

**Selección de tests por capa:** los selectores `silver` y `gold` (`dbt/selectors.yml`) incluyen los modelos y los tests singulares de cada capa, y el compose define `DBT_INDIRECT_SELECTION=cautious`. Con el modo por defecto (*eager*), el build de silver incluía el test de reconciliación gold contra silver y lo corría antes de reconstruir gold, lo que daba un falso error cada vez que llegaba un día nuevo. Con *cautious* solo, ese test no quedaba seleccionado en ningún build; por eso los selectores listan los tests singulares por ruta.

#### Reprocesamiento dirigido: `force_date` y el DAG `dummyjson_reprocess`

**Re-extraer y re-transformar son operaciones distintas.**
- **Re-extraer** (volver a llamar a la API) solo es válido para el último día cerrado; lo controla la guarda. Para días pasados es imposible, porque la API no tiene historia.
- **Re-transformar** (reconstruir silver y gold desde bronze) sí vale para cualquier día que esté en bronze, porque bronze conserva cada snapshot.

El reprocesamiento dirigido es re-transformar un día puntual. **El caso de uso típico es reprocesar un día anterior a hoy** que ya está en bronze, sin volver a llamar a la API.

**Es un proceso separado del flujo diario:** tiene su propio DAG (`dummyjson_reprocess`, manual) y no cambia nada del DAG diario ni del watermark. Para que los dos no se pisen, todas sus tasks de dbt comparten un pool de Airflow de un solo slot (ver abajo).

**Cómo funciona:**
- **Variable de dbt `force_date` (`YYYY-MM-DD`):** en `silver.carts`, `silver.cart_items` y `gold.product_daily_revenue`, dentro del bloque incremental, filtra por esa fecha en lugar del watermark (`audit_logical_date` en bronze, `snapshot_date` en silver). `delete+insert` reemplaza solo ese día. Sin la variable, el comportamiento no cambia (macro `incremental_filter`).
- **`silver.products` queda afuera:** reprocesar un día pasado lo haría retroceder a un estado más viejo. Se repara con full refresh de ese modelo.
- **Validación sin no-ops silenciosos:** un hook `on-run-start` corre antes de cualquier modelo y hace fallar el build si `force_date` tiene formato inválido, es una fecha imposible, no existe en bronze, o se combina con `--full-refresh`. Nada se modifica.
- **Watermark intacto:** las filas reprocesadas conservan el `ingested_at` original, que viene de bronze. El build diario siguiente no procesa nada extra.
- **DAG `dummyjson_reprocess`:** sin schedule, con el parámetro `force_date` (validado con un patrón de fecha al disparar, porque termina en un comando de shell). Corre `dbt build --selector reprocess --vars '{"force_date": ...}'`: siempre `build`, nunca `run`. El selector `reprocess` (`dbt/selectors.yml`) incluye esos tres modelos, sus tests y los tests singulares de silver y gold. Los singulares se listan por ruta porque el chequeo de volumen lee una source de bronze, y con `cautious` no quedaría seleccionado por los modelos.
- **Pool `dbt` con 1 slot:** todas las tasks de dbt de los dos DAGs usan este pool, que crea `airflow-init`. Nunca corren dos builds a la vez, por ejemplo un reprocesamiento durante el build diario.

**Cuándo sirve:**
- después de corregir la lógica de silver o gold, para rehacer un día sin reconstruir todo;
- si silver o gold se alteraron por fuera del pipeline.

Sin cambios de lógica ni de bronze, da exactamente el mismo resultado.

**Comando manual (alternativa al DAG):**

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector reprocess --vars '{"force_date": "2026-09-24"}'
```

Advertencia: el comando manual no pasa por el pool de Airflow. No hay que correrlo mientras corre el DAG diario, que arranca a las 00:30 UTC y tarda unos minutos, porque podría pisarse con su build.

**Verificado:** tests con fixtures y una corrida real del DAG para el 24/09.
- `xmin` cambió solo en ese día, en silver y gold.
- El contenido quedó idéntico.
- El build diario posterior no procesó nada.
- Un parámetro con formato inválido lo rechaza la API al disparar (HTTP 400).

#### Reprocesamiento completo: `dbt build --full-refresh`

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
  - **Volumen bronze → silver** (`silver_row_counts_match_bronze`, falla, no advierte):
    - la cantidad de carts por día en `bronze.carts` coincide con la de `silver.carts`;
    - para cada cart y día, la cantidad de elementos del array `products` del JSON de bronze coincide con sus filas en `silver.cart_items`;
    - un día o un cart presente en un solo lado también falla.

    Corre en el build de silver y en el reprocesamiento dirigido. **Complementa, no reemplaza, las reconciliaciones de valor** (`silver_cart_lines_reconcile_with_cart_totals` y `gold_revenue_reconciles_with_silver_carts`): este chequeo detecta **filas perdidas o de más**, y aquellas detectan **contenido corrupto con la misma cantidad de filas**. Los tests de integración muestran casos que solo ve uno de los dos: un cart perdido junto con sus líneas y una línea extra con montos en cero pasan la reconciliación de valor y los detecta el de volumen.

    **Alcance: la última carga, no toda la historia.** Recorrer bronze completa en cada build no escala. Por eso el chequeo compara solo los días de bronze con `audit_ingestion_timestamp` mayor o igual al `max(ingested_at)` de `silver.carts`: en una corrida diaria, el día recién procesado. Con `force_date`, compara ese día. Todo el chequeo usa índices (verificado con `EXPLAIN`); para el lado de silver se agregaron índices por `snapshot_date` en `carts` y `cart_items`.

    - **Por qué "mayor o igual" sobre el watermark actual y no "mayor" sobre el previo:** el test corre después de los modelos, cuando silver ya incorporó la carga nueva. El watermark previo al build ya no está en ninguna tabla, y "mayor" daría una ventana vacía: un test de mutación lo confirma. Si el modelo perdiera el día entero, el máximo de silver quedaría atrás y la ventana se ampliaría para incluirlo.
    - **Por qué no se capturó el watermark previo con `store_result`/`load_result` en un hook `on-run-start`:** se probó, y no funciona. dbt crea un contexto de ejecución por nodo, y lo que guarda el hook no llega al test (`load_result` devuelve `None`).
    - **Por qué no se usó el watermark de gold** (que durante el build de silver todavía tiene el valor previo): la comparación bronze → silver no debe depender del estado de otra capa.
    - **Por qué no una columna ni una tabla nueva** (`processed_at`, o una tabla con el watermark de cada corrida): agregan estado persistente para un chequeo que solo necesita la última carga.

    **Costos aceptados:**
    - **Los días viejos no se vuelven a auditar en cada corrida.** Si un día quedó con un conteo desalineado por un problema ya resuelto, no se detecta de nuevo solo; se repara con un full refresh o se revisa con `force_date`. Lo compensaría la **tabla de control de cargas** documentada como mejora de producción: un registro por corrida permitiría auditar la historia sin volver a leer bronze.
    - **Recuperación de varios días:** si un día fallan los modelos de dbt y al día siguiente el watermark procesa los dos, el chequeo verifica solo el más reciente. Si lo que falló fue un test y no un modelo, el chequeo ya había corrido sobre ese día en su propia corrida.
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
- **Puertos del host poco comunes por default:** UI en `18080` (`AIRFLOW_PORT`) y warehouse en `15432` (`WAREHOUSE_PORT`). La base de metadata no se expone. Los puertos habituales (`8080`, `5432`, `5433`) suelen estar ocupados por otros servicios de desarrollo; con puertos altos y fijos, levantar el proyecto sigue siendo `docker compose up` y la URL del README no cambia. En el caso improbable de que choquen, se cambian con una variable de entorno sin editar archivos. Los dos puertos se publican solo en `127.0.0.1`: son servicios de desarrollo con credenciales por defecto y no deben quedar accesibles desde otras máquinas de la red. Se descartaron dos alternativas: el puerto aleatorio asignado por Docker, porque obliga a consultar la URL en cada arranque, y un script que busque un puerto libre, porque suma un punto de falla que depende del sistema operativo.

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

**Reprocesamiento dirigido (`force_date`):**
- reprocesar un día reemplaza solo ese día; los demás no cambian (`xmin`);
- sin cambios de lógica, el resultado es el mismo;
- una fecha inexistente en bronze, mal formada o imposible falla antes de tocar cualquier tabla, igual que combinarla con `--full-refresh`;
- después de un reprocesamiento, el build diario no procesa nada extra.

Validado con mutaciones: sacando cada validación, o si el filtro ignora `force_date`, el test correspondiente falla.

**Chequeo de volumen bronze → silver:** después de un build correcto se altera silver para simular un bug de transformación (un cart perdido con sus líneas, una línea perdida, una línea extra) y el chequeo falla en cada caso. Los tests afirman también qué ve la reconciliación de valor en cada caso. Sobre la ventana:
- se verifica el día recién procesado;
- un día viejo alterado **no** se vuelve a auditar (costo aceptado, fijado por un test);
- con `force_date` se verifica ese día.

Validado con mutaciones: fallan los tests si se quita el chequeo o cualquiera de sus partes, si la ventana pasa a ser toda la historia, si se usa "mayor" en lugar de "mayor o igual" (la ventana queda vacía) y si se ignora `force_date`.

**Guarda de la fecha de negocio (unitarios):** corrida programada en horario y demorada dentro del día, corrida programada después de la medianoche siguiente, *Clear* de una corrida vieja, arranque entre las 00:00 y las 00:30, corrida manual, y corrida manual que cruza la medianoche.

Para saber qué días o productos se reprocesaron, los tests comparan `xmin`, la columna de sistema de Postgres que cambia cuando una fila se vuelve a insertar. No hace falta agregar columnas técnicas a los modelos. Los tests se validaron con mutaciones y fallan en cada caso:
- **Filtro de watermark:** sin el filtro, o comparando con `>=` en lugar de `>`.
- **`products`:** con el orden de recencia invertido, o sin el filtro incremental.
- **Guarda:** desactivada, o rechazando solo fechas futuras.

**Fixtures** (`tests/fixtures/`): 3 carts y 6 productos reales de la API. Cubren un producto repetido en dos líneas del mismo cart (el 110 en el cart 38), un producto vendido que no está en el catálogo de las fixtures (el 161; en la API real sí existe, se excluyó de las fixtures a propósito) y un producto del catálogo sin ventas (el 1).

**CI (GitHub Actions): diseño planeado, no implementado en esta entrega.** Hoy la verificación se hace localmente, con los comandos del README, y con las dos pruebas desde cero descritas en la sección 2. Lo que sigue es la especificación del workflow a implementar, en cada push:

1. **`ruff`:** lint y formato del código Python.
2. **`pytest`:** la suite completa, la misma que corre `docker compose run --rm tests`: unitarios, integridad de los DAGs y tests de integración de dbt con fixtures, contra un PostgreSQL temporal levantado como servicio del job.
3. **`dbt build`** de silver y gold sobre fixtures cargadas en bronze, contra ese mismo PostgreSQL.
4. **Verificación en amd64:** construir la imagen y correr la suite en los runners de GitHub, que son amd64. Hasta ahora todo se ejecutó en arm64 (Apple Silicon). Las imágenes base son multi-arquitectura y las dependencias están fijadas, pero amd64 no está verificado.

El workflow **no llamaría a la API real**: bronze se cargaría con las fixtures versionadas en el repo (`tests/fixtures/`, una muestra pequeña de products y carts con casos borde, como líneas de producto duplicadas dentro de un cart). Así las transformaciones y los tests de calidad se validan con datos controlados y el resultado es determinístico. Las fixtures y los tests de integración que las cargan con el loader real ya existen; falta el workflow que los ejecute automáticamente. Automatizaría lo mismo que hará el evaluador: clonar el repo en una máquina limpia y ejecutarlo.

**Observabilidad:** la extracción emite **logs estructurados (JSON)** con entidad, fecha de negocio, páginas recorridas, registros obtenidos frente al total esperado y duración. Las fallas se registran mediante `on_failure_callback` (ver 1.6).

- **JSON anidado en los logs de Airflow (trade-off aceptado):** el paquete `ingestion` escribe cada evento como un mensaje JSON con el `logging` estándar de Python. Airflow 3 escribe cada línea de log de una task como un JSON propio (structlog) y pone el mensaje en su campo `event`. Como nuestro mensaje es un string, queda escapado: `{"event": "{\"records\": 194, ...}", "task_id": ..., ...}`. Se puede leer (un `json.loads` del campo `event`), pero no queda plano. Para que los campos quedaran en el primer nivel habría que loguear con structlog, el logger de Airflow, y eso acoplaría el paquete de ingesta a Airflow, cuando el diseño pide que sea independiente. Fuera de Airflow (scripts, tests) el JSON sale plano.

---

## 2. Flujo de trabajo con IA

**Diseño (Claude, sesión de chat).** Usé Claude para discutir la interpretación del problema y el diseño de capas antes de escribir código. Trabajé en modo de discusión: la IA proponía, yo cuestionaba y las decisiones finales se tomaron sobre esa base. Casos concretos donde el output se validó o se corrigió:

- **Verificación contra la fuente real.** Antes de fijar el diseño se consultaron los endpoints reales. La IA había estimado de memoria que había 50 carts; la API informaba 208. También apareció el caso del cart 7 con un producto duplicado, que invalidaba el grano `(snapshot_date, cart_id, product_id)` propuesto inicialmente y obligó a agregar `line_number`.
- **Campos de auditoría.** Rechacé varias propuestas iniciales:
  - Que `audit_event_timestamp` se extrajera recién en silver. Lo mantuve en bronze con una definición técnica, sin semántica de negocio.
  - Que se guardara el origen en cada fila. Es redundante con el nombre de la tabla.
  - Que se incluyera un `batch_id`. Es redundante con una carga idempotente por fecha lógica.
- **Fecha lógica.** Cuestioné la necesidad de una tercera fecha, porque en mi experiencia con CDC alcanzaban dos. La discusión dejó claro que la diferencia no está en los reintentos sino en la semántica: bajo la interpretación de "día cerrado", la fecha del dato y la de ejecución difieren siempre, y la fecha lógica evita una regla implícita acoplada al schedule.
- **Nomenclatura.** Ajusté los nombres para que `revenue` y `gross_revenue` fueran explícitos y consistentes en inglés.

**Implementación (Claude Code).**

Trabajé por fases, con la consigna de verificar cada una ejecutándola antes de avanzar y de consultar la documentación de la versión fijada de Airflow en lugar de asumir el comportamiento de Airflow 2. Casos concretos:

- **Semántica de schedules cron en Airflow 3 (fase 2).** El diseño (sección 1.6) asumía que la corrida del D+1 cubre el intervalo del día D, como en Airflow 2. El DAG se escribió con el schedule como string cron y los tests unitarios pasaban, porque probaban `resolve_business_date` con un `data_interval_start` que armaba el propio test. El error apareció recién en la primera corrida real: bronze quedó con `audit_logical_date = 2026-09-25` en vez de `2026-09-24`. Consultando la metadata de la corrida se vio que `logical_date`, `data_interval_start` y `data_interval_end` eran iguales, por el nuevo default `create_cron_data_intervals = False`. Se corrigió declarando `CronDataIntervalTimetable`, y se agregó un test de integridad que falla si el DAG vuelve a un timetable sin intervalos. Lección: los tests unitarios validan la lógica contra los supuestos del código; solo la ejecución real valida los supuestos contra la plataforma.
- **Corridas manuales.** El comportamiento de las corridas manuales (sin `logical_date` ni intervalo) se confirmó con triggers reales por REST y por CLI antes de darlo por cerrado, y no solo con la documentación.
- **Reintentos inútiles.** La primera verificación con un `business_date` inválido (en ese momento existía el parámetro) mostró que la task gastaba 3 intentos (unos 6 minutos de backoff) en un error de configuración. Se cambió para que falle en el primer intento.
- **Fecha de negocio en dbt: de `--vars` a watermark (fase 3).** La propuesta de pasar la fecha a dbt con `--vars` venía del diseño hecho con IA y se replicó sin cuestionarla. Al implementar las tasks de dbt, esa decisión obligaba a transportar la fecha desde Airflow, y la IA propuso agregar una task solo para resolverla. Fui yo quien detectó que el problema lo generaba el requisito mismo, al preguntar por qué dbt no leía la fecha de las tablas, si bronze ya la tenía persistida. De esa pregunta salió el procesamiento por watermark de ingesta (1.6), que simplifica el DAG y además recupera días fallidos.
- **Recorrido completo de bronze en cada corrida.** La primera implementación del watermark comparaba día por día, lo que obligaba a agregar bronze completa en cada corrida para detectar los días pendientes. Lo detecté al revisar la propuesta: en mi experiencia, un proceso incremental solo lee su ventana, y un recorrido completo corresponde a una carga completa. También cuestioné si hacía falta calcular los días en un paso previo en lugar de delegarle el incremental a dbt. Se reemplazó por el filtro idiomático de `is_incremental()` sobre el timestamp de ingesta, con índices (verificado con `EXPLAIN`), y se eliminó la macro. Se evaluó `microbatch` y se descartó (1.6).
- **`silver.products` había quedado afuera.** Al aplicar el watermark a carts y gold, `products` siguió como tabla completa, y leía toda la historia de bronze en cada corrida. Lo detecté preguntando por qué no se había modificado. Además, la IA había escrito en CLAUDE.md la regla "nunca agregar bronze completa" mientras dejaba un modelo que la violaba.
- **Defender un escenario que había que prohibir.** Para `products` incremental, la IA propuso una condición de "no retroceder" (unir el lote con las filas actuales) y una columna extra (`watermark_ingested_at`), para tolerar la re-carga de un día viejo después de procesar uno más nuevo. Se llegó a implementar y testear. Al revisarlo, vi que ese escenario era en sí un error: sin historia en la API, re-cargar "el 23" el día 25 trae los datos del 24 etiquetados como 23. Lo correcto era prohibirlo en la extracción, no tolerarlo aguas abajo. Se revirtieron la condición y la columna, y se agregó la guarda de la fecha de negocio (1.6). Lección: antes de agregar lógica defensiva, preguntar si el caso que defiende debería existir.
- **Eliminación de `conf["business_date"]`.** El parámetro para elegir la fecha de una corrida manual parecía una flexibilidad útil (recuperar un día), pero reabría el problema que `catchup=False` cierra: etiquetar con una fecha pasada los datos de hoy. Se eliminó; las corridas manuales siempre cargan el último día cerrado.
- **Verificación real del reprocesamiento dirigido.** No lo di por bueno solo con los tests con fixtures. Disparé `dummyjson_reprocess` con `force_date = 2026-09-24` sobre el stack real, que tenía cargados el 24/09 y el 25/09, y comparé el `xmin` de cada fila por tabla y por día, antes y después:
  - solo cambiaron las filas del 24/09 en `silver.carts`, `silver.cart_items` y `gold.product_daily_revenue`;
  - el 25/09 y `silver.products` quedaron intactos;
  - el contenido de las tres tablas quedó idéntico, incluido `ingested_at`, como se espera sin cambios de lógica;
  - después corrí el build diario: `INSERT 0 0` en los cuatro modelos y el `xmin` sin cambios, así que el reprocesamiento no alteró el watermark.

  En la misma verificación, un disparo con un parámetro malicioso (`"24/09/2026; rm -rf /"`) lo rechazó la API con HTTP 400, antes de llegar a la shell.
- **Tests que no probaban lo que decían.** Un test de reintentos ante `Retry-After` pasaba aunque se cambiara el código a respetar el header. La causa: la librería de mocks HTTP simula los reintentos sin ejecutar nunca la espera. Se detectó con una prueba de mutación (cambiar el código y confirmar que el test falle) y el test se reescribió. Desde entonces, los tests de comportamiento crítico (watermark, `Retry-After`) se validan con mutaciones.
- **Prueba real desde cero (fase 6).** Antes de cerrar la documentación, hice la prueba que va a hacer el evaluador. Bajé el stack con sus volúmenes, borré las imágenes y el caché de build de Docker, cloné el repo en un directorio nuevo y seguí **solo** el README, anotando cada paso que había que inferir.
  - **Resultado:** el pipeline, los tests y el reprocesamiento funcionaron sin tocar código, pero aparecieron **12 huecos de documentación**. Ninguno era de funcionalidad. Por ejemplo: cómo clonar, una query de validación, cómo consultar el warehouse sin un cliente SQL instalado, cómo saber que el pipeline terminó, ejemplos de reprocesamiento con una fecha que no existe en una instalación nueva, el Triggerer en rojo en la UI y la ventana de 00:00 a 00:30 UTC.
  - **Un mensaje engañoso:** Docker mostraba un "pull access denied" al construir la imagen de tests por primera vez. Se corrigió en el compose (`pull_policy: build`), no solo en el README.
  - **Repetición:** con el README reescrito, la prueba se repitió desde cero y no quedó ningún paso sin documentar. Las queries de validación devolvieron exactamente la salida esperada publicada.
- **Correcciones sobre la propia prueba.**
  - **El botón de la UI:** en la primera prueba, la IA reportó que, con la UI en español, el botón para disparar un DAG se llamaba "Activar Dag" y no "Trigger" como decía el README. Al verificarlo de punta a punta, el botón visible dice "Trigger": "Activar Dag" era solo su nombre de accesibilidad, que es lo que lee la herramienta. El README estaba bien y no se cambió.
  - **Los tiempos:** los que dice el README salen de la medición de la prueba, no de estimaciones. La estimación inicial para el build de la imagen de tests (~1 minuto) resultó ser de unos segundos, porque reutiliza la imagen de Airflow, y se corrigió.

**CLAUDE.md como contrato de trabajo.** Antes de escribir código dejé en `CLAUDE.md` las restricciones no negociables, las decisiones ya cerradas (con referencia a este documento) y una regla explícita: si algo de la implementación contradice una decisión tomada, avisar antes de desviarse. Esa regla funcionó así:
- **Hallazgo de Airflow 3 (fase 2):** la IA frenó y consultó. Al ver que la primera corrida cargaba bronze con la fecha equivocada, detuvo el trabajo, explicó la causa y planteó las opciones antes de cambiar el schedule.
- **Cambios a decisiones documentadas** (pasar de `--vars` a watermark, `silver.products` incremental): la IA señaló que contradecían lo escrito y pidió confirmación antes de implementar.
- **`watermark_ingested_at`, donde la regla se cumplió a medias:** la IA avisó que agregaba una columna que no estaba en el diseño, pero la implementó en el mismo paso sin esperar confirmación. El desvío se detectó en mi revisión y se revirtió (ver "Defender un escenario que había que prohibir"). Avisar no alcanza si no se espera la respuesta; desde entonces, cualquier agregado al diseño se propone y se implementa recién después del OK.

`CLAUDE.md` se fue actualizando con cada decisión nueva (timetable explícito, watermark, guarda, pool), para que el contrato siguiera reflejando el diseño vigente.

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
- **Garantizar el supuesto del watermark con varios escritores:** si bronze se cargara desde más de un proceso en paralelo, los timestamps de ingesta dejarían de ser monótonos respecto del commit (ver 1.6). Haría falta un número de secuencia asignado al confirmar cada carga, por ejemplo en una tabla de control, o un margen de reprocesamiento sobre el watermark.
- **Frescura de los datos:** comparar la fecha más reciente cargada contra la fecha actual y alertar si el pipeline dejó de correr sin un error explícito, por ejemplo con el scheduler caído o el DAG pausado. Una falla se nota; una ausencia silenciosa, no. No se implementa porque suma alcance sin un beneficio claro en una evaluación puntual; en un pipeline que corre durante meses sería de lo primero a agregar.
- **Volumen día contra día:** comparar la cantidad de registros del día contra el promedio de días anteriores, para detectar una caída anómala en la fuente. Hoy, si la API devolviera 20 carts en lugar de 208, el `total` coincidiría y el pipeline terminaría bien con datos incompletos. No se implementa por la misma razón: necesita historia de varios días para tener un umbral con sentido.
- **Tabla de control de cargas** (`_control.pipeline_runs` o similar): una fila por corrida con fecha procesada, registros extraídos y esperados, duración y resultado. Permite monitorear la operación con SQL, sin revisar logs de Airflow uno por uno, y sirve de base para los dos chequeos anteriores. No se implementa ahora: hoy esa información está en los logs estructurados y en el resumen de cada task.
- **Alertas a un canal real** (Slack, email, PagerDuty) conectadas al `on_failure_callback`, y métricas del pipeline (registros por corrida, duración, frescura del dato) enviadas a un sistema de monitoreo.
