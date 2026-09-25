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
| `audit_ingestion_timestamp` | Momento en que el pipeline procesó el dato. Un único valor por corrida, para que un snapshot nunca quede partido entre dos timestamps |
| `audit_logical_date` | Día al que pertenece el dato: fecha lógica de la corrida de Airflow |
| `audit_process_name` | Proceso que realizó la ingesta (`dag_id.task_id`), para trazabilidad |

**Por qué tres fechas y no dos.** Cada una responde una pregunta distinta:

| Campo | Pregunta | Quién la define |
|---|---|---|
| `audit_event_timestamp` | ¿Cuándo cambió el registro en el origen? | La fuente |
| `audit_ingestion_timestamp` | ¿Cuándo lo procesamos? | El reloj, al ejecutar |
| `audit_logical_date` | ¿A qué día pertenece? | Airflow, al programar la corrida |

En pipelines CDC, el timestamp del evento de origen suele alcanzar para ubicar el dato en el tiempo, y la fecha lógica no hace falta. Acá la fuente no provee ninguna fecha para carts, y con la interpretación de "día cerrado" la fecha del dato (D) y la de ejecución (D+1) caen **siempre** en días distintos. Derivar la fecha de negocio de `audit_ingestion_timestamp` requeriría una regla implícita (`- 1 día`) acoplada al horario de ejecución, que se rompería en silencio si cambiara el schedule. `audit_logical_date` hace explícita esa relación y no cambia con reintentos ni reruns.

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
- **`line_number`:** posición del producto dentro del array del cart (`jsonb_array_elements ... WITH ORDINALITY`). Es necesario porque `(snapshot_date, cart_id, product_id)` **no es único**: en la verificación de la fuente, el cart 7 contiene el producto 56 en dos líneas separadas.
- **Líneas duplicadas:** se mantienen separadas en silver por fidelidad al origen (no se puede saber si son un error o líneas legítimas) y se consolidan en gold al agregar.
- **Montos en `numeric`:** la fuente trae errores de punto flotante (por ejemplo, `99.94999999999999`). Todo lo monetario se castea a `numeric(12,2)`.
- **Título y precio de venta tomados del cart**, no del catálogo: representan el momento de la venta; el catálogo puede cambiar después.
- **Materialización:** `carts` y `cart_items` incrementales por día; `products` como tabla completa. Con estos volúmenes todo podría ser full refresh, pero el incremental por día es coherente con el particionado lógico y muestra cómo escalaría. Para 194 productos, un merge no justifica su complejidad.

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

- **`revenue` neto + `gross_revenue`:** el enunciado exige una columna `revenue`; se define como neto por ser lo cobrado y se expone el bruto para visibilizar el descuento. La definición queda documentada en el `schema.yml` de dbt.
- **Título del catálogo con `left join`:** un único título por producto en toda la historia, sin perder revenue si un producto no está en el catálogo.
- **Tabla dispersa:** un producto sin ventas en un día no tiene fila. Cruzar un calendario con todos los productos multiplicaría filas sin información.
- **Materialización incremental por `date`**, coherente con silver.

### 1.6 Orquestación y scheduling

- **Schedule diario a las 00:30 UTC:** margen de gracia de 30 minutos después de la actualización de la fuente. DummyJSON no expone ninguna señal de disponibilidad (endpoint de metadata, archivo marker), así que el margen es la única herramienta disponible.
- **Alineación con la semántica de Airflow:** la corrida que se ejecuta el día D+1 a las 00:30 cubre el intervalo que empieza el día D. La fecha lógica coincide con la fecha de negocio sin transformaciones adicionales.
- **`catchup=False`:** la API no tiene historia. Un backfill capturaría los datos de hoy y los etiquetaría con fechas pasadas: datos incorrectos que parecen correctos. La extracción no es re-ejecutable hacia atrás; las transformaciones sí, desde bronze.
- **Reintentos con backoff** ante fallas de la API o respuestas incompletas.
- **Fallas visibles:** un `on_failure_callback` registra un log estructurado con el DAG, la task, la fecha de negocio y el error. No se conectan canales externos (Slack, email) porque requerirían credenciales, que el enunciado excluye.
- **[PENDIENTE] Corridas manuales:** en Airflow 3, las corridas disparadas manualmente pueden no tener fecha lógica ni intervalo de datos asociado. Hay que resolver explícitamente la fecha de negocio para ese caso y cubrirlo con un test.

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

Las dependencias de dbt (`dbt deps`) se instalan al construir la imagen, no al ejecutar.

**Extracción: módulo Python independiente de Airflow**, invocado desde tasks de TaskFlow. Usa `requests` con reintentos, backoff y timeout explícitos, y `psycopg` para la carga transaccional. Al estar desacoplado de Airflow, se puede testear sin levantar el orquestador.

**Versiones fijas:** imágenes con tag exacto, Airflow instalado con su archivo de *constraints* oficial y dependencias Python pineadas.

### 1.9 Testing, CI y observabilidad

**Tests unitarios (`pytest`):**

- **Extracción con la API mockeada:** paginación completa, validación contra el `total`, reintentos ante errores y registros incompletos. No dependen de DummyJSON.
- **Resolución de la fecha de negocio:** corridas programadas, corridas manuales con y sin parámetro explícito.
- **Integridad del DAG:** el DAG carga sin errores de import y tiene las tasks y dependencias esperadas.

**Tests de datos (dbt):** los descritos en 1.7, ejecutados como parte de cada corrida del pipeline. Un test fallido en silver impide construir gold.

**CI (GitHub Actions), en cada push:**

1. **`ruff`:** lint del código Python.
2. **`pytest`:** tests unitarios.
3. **`dbt build`** contra un PostgreSQL temporal levantado como servicio del job.

CI **no llama a la API real**: bronze se carga con fixtures versionadas en el repo (una muestra pequeña de products y carts que incluye casos borde como líneas de producto duplicadas dentro de un cart). Así las transformaciones y los tests de calidad se validan con datos controlados y el resultado es determinístico. CI reproduce de forma automatizada lo mismo que hará el evaluador: clonar el repo en una máquina limpia y ejecutarlo.

**Observabilidad:** la extracción emite **logs estructurados (JSON)** con entidad, fecha de negocio, páginas recorridas, registros obtenidos frente al total esperado y duración. Las fallas se registran mediante `on_failure_callback` (ver 1.6).

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

**Implementación (Claude Code).** [PENDIENTE]

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
- **Alertas a un canal real** (Slack, email, PagerDuty) conectadas al `on_failure_callback`, y métricas del pipeline (registros por corrida, duración, frescura del dato) enviadas a un sistema de monitoreo.
