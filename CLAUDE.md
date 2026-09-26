# CLAUDE.md

Contexto del proyecto para Claude Code. El diseño completo y su justificación están en `DECISIONS.md`: **leelo antes de proponer cambios**. Las decisiones de ese documento ya están tomadas; si algo de la implementación las contradice, avisá antes de desviarte.

## Qué es

Challenge técnico de Data Engineering (X3M). Pipeline batch que ingiere Products y Carts de DummyJSON, los persiste en PostgreSQL y produce `gold.product_daily_revenue`, orquestado con Airflow y ejecutable 100% local con `docker compose`.

## Restricciones no negociables

- **Tiene que levantar en una máquina limpia** siguiendo el README, con 2 o 3 comandos. Es condición excluyente de la evaluación.
- Sin servicios cloud ni credenciales externas.
- **Todas las versiones fijas:** imágenes Docker con tag exacto (nunca `latest` ni tags flotantes), dependencias Python pineadas, Airflow instalado con su archivo de constraints oficial, paquetes de dbt con versión exacta.
- No exponer puertos del host que suelen estar ocupados (por ejemplo, 5432).

## Stack

- **Apache Airflow 3.3.2** (fijado; no actualizar sin acordarlo) con `LocalExecutor`.
- **PostgreSQL** en dos contenedores: metadata de Airflow y `warehouse` (datos del pipeline).
- **dbt-core + dbt-postgres** en un **virtualenv aislado dentro de la imagen de Airflow**, invocado con `BashOperator`. No usar Cosmos ni `DockerOperator`.
- **Extracción en Python puro** (`requests` + reintentos/backoff/timeout, `psycopg`), en un paquete independiente de Airflow, llamado desde tasks de TaskFlow.

## Modelo de datos (resumen; detalle en DECISIONS.md)

- **Bronze** (`bronze.products`, `bronze.carts`): un registro de la API = una fila.
  - Columnas: `id`, `data` (`jsonb` crudo), `audit_event_timestamp`, `audit_ingestion_timestamp` (un valor único por carga de cada entidad), `audit_logical_date` (la fecha de negocio resuelta; no siempre es la `logical_date` de Airflow), `audit_process_name`.
  - Carga idempotente: `DELETE WHERE audit_logical_date = D` + `INSERT` en una transacción.
- **Silver** (dbt):
  - `products`: SCD1 por `product_id`, incremental `delete+insert` por `product_id`: el snapshot más reciente por producto dentro del lote (recuperación con varios días pendientes); los productos ausentes conservan su último estado. Sin lógica de "no retroceder": la guarda de la extracción impide cargar días viejos.
  - `carts`: grano `(snapshot_date, cart_id)`, incremental `delete+insert` por `snapshot_date`.
  - `cart_items`: grano `(snapshot_date, cart_id, line_number)` usando `jsonb_array_elements ... WITH ORDINALITY`, incremental por `snapshot_date`.
  - Montos en `numeric(12,2)`. `ingested_at` = `audit_ingestion_timestamp` de bronze (watermark).
- **Gold** (dbt): `product_daily_revenue` con grano `(product_id, date)`.
  - Columnas: `product_id`, `product_title`, `date`, `units_sold`, `revenue` (neto), `gross_revenue`, `carts_count`, `ingested_at` (máximo `ingested_at` de silver por fecha: watermark de gold).
  - Incremental por `date`.

## Procesamiento incremental en dbt: watermark de ingesta (crítico)

- **dbt no recibe fechas.** No usar `--vars` para pasarle la fecha de negocio. El incremental es el patrón estándar de dbt: dentro de `is_incremental()`, filtrar `audit_ingestion_timestamp > (select coalesce(max(ingested_at), '-infinity') from {{ this }})` (en gold, `ingested_at` de `silver.cart_items`), con `delete+insert` y `unique_key` = fecha (o `product_id` en products).
  - No calcular días pendientes en un paso previo: cada carga de bronze reemplaza un día completo con un único timestamp, así que las filas nuevas son días completos, y `delete+insert` reemplaza esos días.
  - **Todos los modelos de silver y gold son incrementales.** Nunca agregar bronze completa en una corrida incremental, tampoco en tests (el chequeo de volumen `silver_row_counts_match_bronze` mira solo la última carga o `force_date`); solo el full refresh la recorre. Índices: `audit_ingestion_timestamp` en `bronze.products` y `bronze.carts`, y la columna de watermark en silver y gold (config `indexes`).
  - **Supuesto:** los timestamps de ingesta crecen en el mismo orden que los commits de las cargas. Se cumple porque hay un solo escritor (el DAG) y `max_active_runs=1`. No cargar bronze desde otros procesos en paralelo.
- **Tasks del DAG:** ejecutan `dbt build --selector silver` y `dbt build --selector gold` (`dbt/selectors.yml`). Cada selector incluye los modelos y los tests singulares de su capa. El compose define `DBT_INDIRECT_SELECTION=cautious`, para que el build de silver no corra el test de reconciliación de gold antes de reconstruir gold.
- **`max_active_runs=1` en el DAG es obligatorio:** el watermark es estado compartido, y dos corridas simultáneas podrían procesar el mismo día en paralelo.
- **Reprocesamiento dirigido:** la variable de dbt `force_date` (`YYYY-MM-DD`) hace que `carts`, `cart_items` y `product_daily_revenue` reconstruyan solo ese día en lugar de usar el watermark (macro `incremental_filter`). `silver.products` queda afuera; se repara con full refresh.
  - El hook `on-run-start` (`validate_force_date`) falla antes de cualquier modelo si la fecha es inválida, no existe en bronze o se combina con `--full-refresh`. Nada de no-ops silenciosos.
  - Se ejecuta con el DAG `dummyjson_reprocess` (sin schedule, parámetro `force_date` con patrón de fecha, `dbt build --selector reprocess`, nunca `run`; `retries=0`). El selector incluye los tests singulares por ruta: un test que lee una source de bronze no se selecciona por los modelos con `cautious`. Las filas conservan el `ingested_at` de bronze: el watermark diario no se altera.
- **Pool `dbt` (1 slot, lo crea `airflow-init`):** obligatorio en toda task de dbt de cualquier DAG; nunca corren dos builds a la vez.
- **Reparación completa:** `dbt build --full-refresh`. Casos que el watermark no detecta y requieren full refresh: un bug corregido en la lógica de silver o gold (bronze no cambió), y una corrección de `audit_logical_date` en bronze (un `UPDATE` no cambia `audit_ingestion_timestamp`).
- **Efecto sobre `product_title` en gold:** los días ya procesados conservan el título del catálogo vigente cuando se procesaron. Un renombre solo se ve en los días nuevos, o en todos después de un full refresh.

## Semántica temporal (crítico)

- La actualización de medianoche UTC cierra el día anterior: la corrida del día D+1 a las 00:30 UTC carga datos del día D.
- Schedule diario a las 00:30 UTC, `catchup=False`, declarado **siempre** como `schedule=CronDataIntervalTimetable("30 0 * * *", timezone="UTC")`. **Nunca como string cron.**
  - La expresión cron es la misma; lo que cambia es cómo interpreta Airflow 3 la corrida. Un string cron se interpreta como `CronTriggerTimetable` (porque `create_cron_data_intervals = False` por default): la corrida es un disparo sin intervalo, y `logical_date = data_interval_start = run_after`. Con eso, la corrida del 25/09 00:30 etiquetaría como 25/09 los datos del 24/09.
  - `CronDataIntervalTimetable` hace que la corrida del D+1 00:30 cubra el intervalo [D 00:30, D+1 00:30), así que `data_interval_start` es el día D.
  - Verificado con una corrida real (DECISIONS 1.6). El test de integridad del DAG falla si el timetable no es `CronDataIntervalTimetable`.
- La fecha de negocio se resuelve con una función pura `resolve_business_date(context)`, **sin parámetros para elegirla** (no existe `conf["business_date"]`; no reintroducirlo):
  - **Corrida programada:** fecha de `data_interval_start`.
  - **Cualquier otra corrida (manual, disparada):** el día anterior a `run_after` en UTC.
- **Guarda (crítico):** antes de extraer, `ensure_business_date_is_last_closed_day` exige que la fecha de negocio sea el día anterior al momento real de ejecución (UTC), para cualquier tipo de corrida; si no, `AirflowFailException` (sin reintentos). La API no tiene historia: cualquier otra fecha etiquetaría mal los datos. Bloquea *Clear* de corridas viejas, backfills, corridas programadas que ejecutan después de la medianoche siguiente y el arranque entre 00:00 y 00:30 UTC.
  - Consecuencia: bronze solo recibe cargas del día recién cerrado; la única re-carga legítima es el reintento del mismo día. No agregar lógica defensiva aguas abajo para re-cargas de días viejos: el escenario está prohibido. Si se viola por fuera del DAG, se repara con full refresh.
- En Airflow 3.3.2, las corridas manuales llegan con `logical_date = None` y `data_interval = None`; solo traen `run_after`. Esto está verificado con triggers reales por REST y por CLI.
- La fecha de negocio solo decide con qué `audit_logical_date` se carga bronze. dbt no la recibe: la lee de las tablas (ver la sección de watermark).

## Estructura del repo

```
.
├── README.md
├── DECISIONS.md
├── CLAUDE.md
├── docker-compose.yml          # stack + servicio `tests` (profile dev)
├── pyproject.toml              # config de ruff y pytest
├── requirements/
│   ├── airflow.txt             # deps de runtime de Airflow (con constraints)
│   ├── dbt.txt                 # deps del venv de dbt
│   └── dev.txt                 # ruff, pytest, responses, etc.
├── docker/
│   ├── airflow/Dockerfile      # etapas runtime (Airflow + venv de dbt + dbt deps) y dev (+ ruff/pytest)
│   └── warehouse/init/         # SQL de init: schemas bronze/silver/gold + tablas bronze
├── dags/
│   ├── dummyjson_pipeline.py   # DAG diario: solo orquestación, sin lógica de negocio
│   └── dummyjson_reprocess.py  # DAG manual: dbt build de un día con force_date (pool dbt)
├── src/
│   └── ingestion/
│       ├── config.py           # config por entidad (endpoint, clave de datos, campo de event ts)
│       ├── client.py           # cliente HTTP paginado con reintentos y validación de total
│       ├── loader.py           # carga idempotente a bronze
│       ├── business_date.py    # resolve_business_date + guarda del último día cerrado
│       ├── logging.py          # logging estructurado (JSON) + on_failure_callback
│       └── run.py              # extract_and_load: punto de entrada de cada task (config → client → loader → log)
├── dbt/
│   ├── dbt_project.yml         # packages-install-path fuera del bind mount (/opt/dbt/packages)
│   ├── packages.yml            # dbt_utils con versión exacta
│   ├── package-lock.yml        # lock de paquetes; `dbt deps` corre en el build de la imagen
│   ├── profiles.yml            # lee WAREHOUSE_*
│   ├── selectors.yml           # selectores `silver`, `gold` y `reprocess` (modelos + tests singulares)
│   ├── macros/                 # generate_schema_name, force_date (incremental_filter + validate_force_date)
│   ├── models/
│   │   ├── sources.yml
│   │   ├── silver/             # products.sql, carts.sql, cart_items.sql + schema.yml
│   │   └── gold/               # product_daily_revenue.sql + schema.yml
│   └── tests/                  # tests singulares: reconciliación de valor y volumen bronze→silver (silver/, gold/)
├── tests/
│   ├── unit/                   # client, loader, business_date, logging, run
│   ├── dags/                   # integridad del DAG
│   ├── dbt/                    # integración dbt: watermark, products, reprocesamiento, volumen, full refresh (DB temporal)
│   └── fixtures/               # products.json, carts.json (incluye casos borde)
├── scripts/
│   └── load_fixtures.py        # carga fixtures en bronze (usado por CI)
└── .github/workflows/ci.yml    # ruff + pytest + dbt build contra Postgres de servicio
```

## DAG

```
extract_products ─┐
                  ├─→ dbt_build_silver ─→ dbt_build_gold
extract_carts ────┘
```

## Convenciones

- Código, identificadores y comentarios en inglés. Documentación (README, DECISIONS) en español.
- Toda la lógica en `src/`; el DAG solo orquesta.
- Sin secretos en el repo. Las credenciales locales del compose son valores por defecto de desarrollo y se documentan como tales.
- Lint y tests corren en Docker: `docker compose run --rm tests` (pytest) y `docker compose run --rm tests ruff check .`.

## Forma de trabajo

- Trabajá **por fases** y **verificá cada fase ejecutándola** antes de avanzar. No des una fase por terminada solo porque el código "se ve bien".
  1. **Scaffold:** compose y Dockerfile. Verificar que todos los servicios queden `healthy` y la UI responda.
  2. **Ingesta a bronze** con tests unitarios. Verificar con un trigger manual real y con conteos en bronze contra el `total` de la API.
  3. **dbt silver** con tests. Verificar con `dbt build`.
  4. **dbt gold** con tests y la query de validación.
  5. **CI** con fixtures.
  6. **README** y prueba desde cero: `docker compose down -v`, clonar en un directorio limpio y levantar solo con los comandos del README.
- Para Airflow 3 (componentes del compose, CLI, comportamiento de corridas manuales, pausa de DAGs al crearse), **consultá la documentación oficial de la versión fijada** en lugar de asumir comportamiento de Airflow 2.
- Si una decisión de implementación no está cubierta en DECISIONS.md, proponela y esperá confirmación.
