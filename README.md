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

### Reproceso de un día en particular para Silver y Gold

Sirve para reprocesar **cualquier día anterior a hoy que ya esté en bronze**, sin tocar la API: vuelve a construir ese día en `silver.carts`, `silver.cart_items` y `gold.product_daily_revenue` a partir de bronze, y corre sus tests. Es un proceso separado del flujo diario. Casos típicos: después de corregir la lógica de un modelo, o si silver o gold se alteraron por fuera del pipeline. Sin cambios de lógica, el resultado es idéntico.

**Desde la UI de Airflow:** en el DAG `dummyjson_reprocess`, *Trigger*, y completar `force_date` con la fecha (`YYYY-MM-DD`).

**Desde la CLI de Airflow:**

```bash
docker compose exec airflow-scheduler airflow dags trigger dummyjson_reprocess --conf '{"force_date": "2026-09-24"}'
```

**Desde la API REST de Airflow:**

```bash
TOKEN=$(curl -s -X POST http://localhost:18080/auth/token -H 'Content-Type: application/json' -d '{"username": "airflow", "password": "airflow"}' | python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')
```

```bash
curl -X POST http://localhost:18080/api/v2/dags/dummyjson_reprocess/dagRuns -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {"force_date": "2026-09-24"}}'
```

**Comando manual de dbt equivalente** (sin Airflow):

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector reprocess --vars '{"force_date": "2026-09-24"}'
```

> No correr el comando manual mientras corre el DAG diario, que arranca a las 00:30 UTC y tarda unos minutos. El DAG `dummyjson_reprocess` no tiene ese problema: comparte con el DAG diario un pool de Airflow de un solo slot, así que sus builds de dbt nunca se superponen.

**Si la fecha no existe en bronze** (o tiene formato inválido), el build falla antes de tocar cualquier tabla, con un mensaje que lo indica. No hay reprocesos silenciosos que no hagan nada.

**`silver.products` no se reprocesa con esto:** reprocesar un día pasado lo haría volver a un estado más viejo del catálogo. Si hace falta repararlo, se usa el full refresh completo (sección siguiente).

### Todo, desde bronze

Por ejemplo, después de corregir un bug en un modelo o una etiqueta de fecha en bronze:

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector silver --full-refresh
```

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector gold --full-refresh
```

Primero silver y después gold, en ese orden. Cuándo hace falta y por qué: [DECISIONS.md, sección 1.6](DECISIONS.md#reprocesamiento-completo-dbt-build---full-refresh).
