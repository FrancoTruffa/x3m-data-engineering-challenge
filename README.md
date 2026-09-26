# X3M Data Engineering Challenge

Pipeline batch que ingiere Products y Carts de [DummyJSON](https://dummyjson.com), los guarda en PostgreSQL (bronze → silver → gold) y produce `gold.product_daily_revenue`. Está orquestado con Airflow 3 y corre 100% local con Docker Compose. El diseño y sus decisiones están en [DECISIONS.md](DECISIONS.md).

> README en construcción: se completa al cerrar la implementación.

## Levantar el proyecto

Requisitos: Docker con Compose v2 y al menos 4 GB de RAM asignados al engine.

```bash
docker compose up -d --build --wait
```

- **UI de Airflow:** http://localhost:18080, con usuario `airflow` y contraseña `airflow` (valores por defecto de desarrollo).
- **Warehouse (PostgreSQL):** `localhost:15432`, base `warehouse`, usuario y contraseña `warehouse`.

El DAG `dummyjson_pipeline` se crea activo y corre solo al levantar el stack.

Si un puerto está ocupado, se cambia sin editar archivos:

```bash
AIRFLOW_PORT=18081 WAREHOUSE_PORT=15433 docker compose up -d --build --wait
```

## Tests

```bash
docker compose run --rm tests
```

```bash
docker compose run --rm tests ruff check .
```

## Reprocesamiento

dbt procesa solo los días con cargas nuevas en bronze (watermark de ingesta). Reprocesar vuelve a transformar lo que ya está en bronze; nunca vuelve a llamar a la API.

### Un día puntual

Desde la UI: disparar el DAG `dummyjson_reprocess` con el parámetro `force_date` (`YYYY-MM-DD`). Reconstruye ese día en `silver.carts`, `silver.cart_items` y `gold.product_daily_revenue`, y corre sus tests. Si la fecha no existe en bronze, falla sin tocar nada.

O por consola, fuera de la ventana del DAG diario (00:30 UTC):

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --select carts cart_items product_daily_revenue --vars '{"force_date": "2026-09-24"}'
```

### Todo, desde bronze

Por ejemplo, después de corregir un bug en un modelo o una etiqueta de fecha en bronze:

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector silver --full-refresh
```

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector gold --full-refresh
```

Primero silver y después gold, en ese orden. Cuándo hace falta y por qué: [DECISIONS.md, sección 1.6](DECISIONS.md#reprocesamiento-completo-dbt-build---full-refresh).
