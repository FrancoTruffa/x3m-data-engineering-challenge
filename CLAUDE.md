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

- **Apache Airflow 3** con `LocalExecutor`. Verificá la última versión estable 3.x al momento de implementar y fijala.
- **PostgreSQL** en dos contenedores: metadata de Airflow y `warehouse` (datos del pipeline).
- **dbt-core + dbt-postgres** en un **virtualenv aislado dentro de la imagen de Airflow**, invocado con `BashOperator`. No usar Cosmos ni `DockerOperator`.
- **Extracción en Python puro** (`requests` + reintentos/backoff/timeout, `psycopg`), en un paquete independiente de Airflow, llamado desde tasks de TaskFlow.

## Modelo de datos (resumen; detalle en DECISIONS.md)

- **Bronze** (`bronze.products`, `bronze.carts`): un registro de la API = una fila.
  - Columnas: `id`, `data` (`jsonb` crudo), `audit_event_timestamp`, `audit_ingestion_timestamp` (un valor único por corrida), `audit_logical_date`, `audit_process_name`.
  - Carga idempotente: `DELETE WHERE audit_logical_date = D` + `INSERT` en una transacción.
- **Silver** (dbt):
  - `products`: SCD1 por `product_id`, tabla completa.
  - `carts`: grano `(snapshot_date, cart_id)`, incremental `delete+insert` por `snapshot_date`.
  - `cart_items`: grano `(snapshot_date, cart_id, line_number)` usando `jsonb_array_elements ... WITH ORDINALITY`, incremental por `snapshot_date`.
  - Montos en `numeric(12,2)`.
- **Gold** (dbt): `product_daily_revenue` con grano `(product_id, date)`.
  - Columnas: `product_id`, `product_title`, `date`, `units_sold`, `revenue` (neto), `gross_revenue`, `carts_count`.
  - Incremental por `date`.

## Semántica temporal (crítico)

- La actualización de medianoche UTC cierra el día anterior: la corrida del día D+1 a las 00:30 UTC carga datos del día D.
- Schedule `30 0 * * *`, `catchup=False`.
- La fecha de negocio se resuelve con una función pura `resolve_business_date(context)`:
  - **Corrida programada:** fecha de `data_interval_start`.
  - **Corrida manual:** `conf["business_date"]` si viene; si no, el día anterior a `run_after` en UTC.
- En Airflow 3, las corridas manuales pueden no tener `logical_date` ni `data_interval`. **Verificá el comportamiento real con un trigger manual.**
- La fecha de negocio se pasa a dbt con `--vars '{"logical_date": "YYYY-MM-DD"}'`.

## Estructura del repo

```
.
├── README.md
├── DECISIONS.md
├── CLAUDE.md
├── docker-compose.yml
├── pyproject.toml              # config de ruff y pytest
├── requirements/
│   ├── airflow.txt             # deps de runtime de Airflow (con constraints)
│   ├── dbt.txt                 # deps del venv de dbt
│   └── dev.txt                 # ruff, pytest, responses, etc.
├── docker/
│   ├── airflow/Dockerfile      # Airflow + venv de dbt + dbt deps en build
│   └── warehouse/init/         # SQL de init: schemas bronze/silver/gold + tablas bronze
├── dags/
│   └── dummyjson_pipeline.py   # solo orquestación, sin lógica de negocio
├── src/
│   └── ingestion/
│       ├── config.py           # config por entidad (endpoint, clave de datos, campo de event ts)
│       ├── client.py           # cliente HTTP paginado con reintentos y validación de total
│       ├── loader.py           # carga idempotente a bronze
│       ├── business_date.py    # resolve_business_date
│       └── logging.py          # logging estructurado (JSON)
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml            # dbt_utils con versión exacta
│   ├── profiles.yml
│   ├── models/
│   │   ├── sources.yml
│   │   ├── silver/             # products.sql, carts.sql, cart_items.sql + schema.yml
│   │   └── gold/               # product_daily_revenue.sql + schema.yml
│   └── tests/                  # tests singulares de reconciliación
├── tests/
│   ├── unit/                   # client, loader, business_date
│   ├── dags/                   # integridad del DAG
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
