# X3M Data Engineering Challenge

Pipeline batch que ingiere Products y Carts de [DummyJSON](https://dummyjson.com), los guarda en PostgreSQL en tres capas (bronze → silver → gold) y produce `gold.product_daily_revenue`: revenue por producto y por día. Está orquestado con Airflow 3 y corre 100% local con Docker Compose. El diseño y sus decisiones están en [DECISIONS.md](DECISIONS.md).

## Requisitos

- **Docker** con **Docker Compose v2** (Docker Desktop, OrbStack o Docker Engine en Linux), con al menos **4 GB de RAM** asignados al engine.
- **git**, para clonar el repositorio.
- **Conexión a internet:** el primer arranque descarga imágenes de Docker y dependencias de Python, y el pipeline consulta la API de DummyJSON.

No hace falta instalar Python, Airflow, dbt ni un cliente de PostgreSQL: todo corre dentro de los contenedores.

## 1. Obtener el proyecto

```bash
git clone https://github.com/FrancoTruffa/x3m-data-engineering-challenge.git
```

```bash
cd x3m-data-engineering-challenge
```

Todos los comandos siguientes se corren desde este directorio.

## 2. Levantar todo

```bash
docker compose up -d --build --wait
```

- **Cuánto tarda:** la primera vez, unos **2 a 3 minutos**, según la conexión. Construye la imagen de Airflow y descarga PostgreSQL. Las siguientes veces, unos segundos.
- **Qué hace:** el comando termina cuando todos los servicios están sanos. No hace falta nada más: el DAG diario `dummyjson_pipeline` se crea activo y **corre solo al levantar el stack**. Carga el último día cerrado (ayer, en UTC) en bronze, y después silver y gold. Termina en **menos de un minuto**.

**Accesos** (credenciales por defecto de desarrollo; los puertos solo se publican en `localhost`):

- **UI de Airflow:** http://localhost:18080, usuario `airflow`, contraseña `airflow`.
- **Warehouse (PostgreSQL):** `localhost:15432`, base `warehouse`, usuario `warehouse`, contraseña `warehouse`.

Si alguno de esos puertos está ocupado, se cambian sin editar archivos:

```bash
AIRFLOW_PORT=18081 WAREHOUSE_PORT=15433 docker compose up -d --build --wait
```

## 3. Ver que el pipeline terminó

**En la UI:** http://localhost:18080/dags/dummyjson_pipeline/runs. Tiene que haber una corrida `scheduled__<hoy>T00:30:00+00:00` en estado exitoso, con las 4 tasks en verde: `extract_products`, `extract_carts`, `dbt_build_silver` y `dbt_build_gold`. Haciendo clic en la corrida se ve el detalle de cada task y sus logs.

**Por consola:**

```bash
docker compose exec airflow-scheduler airflow dags list-runs dummyjson_pipeline -o plain
```

La columna `state` tiene que decir `success`. `logical_date` es el día cargado (ayer); `run_after` es cuándo le tocaba correr (hoy a las 00:30 UTC).

## 4. Consultar el resultado

El warehouse se consulta con `psql` desde dentro del contenedor, sin instalar nada:

```bash
docker compose exec warehouse psql -U warehouse -d warehouse
```

O con cualquier cliente SQL (DBeaver, DataGrip, etc.) en `localhost:15432`, con las credenciales de arriba.

### Query de validación

Revenue por producto y por día, los productos que más facturaron:

```bash
docker compose exec warehouse psql -U warehouse -d warehouse -c "
select date, product_id, product_title, units_sold, revenue, gross_revenue, carts_count
from gold.product_daily_revenue
order by date desc, revenue desc
limit 5;"
```

Salida esperada (datos de DummyJSON; la fecha es la de ayer en UTC):

```
    date    | product_id |     product_title      | units_sold |  revenue  | gross_revenue | carts_count
------------+------------+------------------------+------------+-----------+---------------+-------------
 2026-09-26 |        170 | Durango SXT RWD        |         19 | 587426.64 |     702999.81 |           6
 2026-09-26 |        169 | Dodge Hornet GT Plus   |         17 | 413822.33 |     424999.83 |           4
 2026-09-26 |        115 | MotoGP CI.H1           |         22 | 307163.78 |     329999.78 |           5
 2026-09-26 |        167 | 300 Touring            |          9 | 250612.11 |     260999.91 |           5
 2026-09-26 |         98 | Rolex Submariner Watch |         17 | 225980.83 |     237999.83 |           7
```

- `revenue` es **neto de descuentos** (lo cobrado).
- `gross_revenue` es el bruto (precio de lista × cantidad).
- `carts_count` es la cantidad de carts distintos que incluyeron el producto.

Totales por día, conciliados contra los carts de silver:

```bash
docker compose exec warehouse psql -U warehouse -d warehouse -c "
select
    g.date,
    count(*)             as products,
    sum(g.units_sold)    as units_sold,
    sum(g.revenue)       as revenue,
    sum(g.gross_revenue) as gross_revenue,
    (select sum(c.discounted_total) from silver.carts c where c.snapshot_date = g.date) as carts_revenue
from gold.product_daily_revenue g
group by g.date
order by g.date;"
```

Salida esperada después de la primera corrida:

```
    date    | products | units_sold |  revenue   | gross_revenue | carts_revenue
------------+----------+------------+------------+---------------+---------------
 2026-09-26 |      189 |       2417 | 3456709.58 |    3834278.63 |    3456709.58
```

`revenue` tiene que ser igual a `carts_revenue`: todo lo cobrado en los carts de ese día queda repartido entre los productos. Los números salen de los datos de DummyJSON; si la API cambiara su contenido, cambiarían, pero la igualdad se mantiene. El pipeline lo verifica en cada corrida con un test de dbt.

## 5. Tests

```bash
docker compose run --rm tests
```

```bash
docker compose run --rm tests ruff check .
```

- **Qué son:** el primero corre la suite completa de `pytest`: unitarios, integridad de los DAGs y tests de integración de dbt con fixtures. El segundo, el lint.
- **Cuánto tardan:** la suite tarda unos **3 minutos**. La primera vez se construye antes la imagen de tests, en pocos segundos, porque reutiliza la imagen de Airflow del paso 2.
- **Qué necesitan:** usan el PostgreSQL del warehouse del stack; si no está corriendo, `docker compose run` lo levanta. Cada test de integración crea su propia base de datos temporal y la borra al terminar: **no tocan los datos del pipeline**. No llaman a la API real.

## 6. Reprocesamiento

dbt procesa solo los días con cargas nuevas en bronze (watermark de ingesta). Reprocesar vuelve a transformar lo que ya está en bronze; **nunca vuelve a llamar a la API**.

### Reproceso de un día en particular para Silver y Gold

Sirve para reprocesar **cualquier día anterior a hoy que ya esté en bronze**, sin tocar la API: vuelve a construir ese día en `silver.carts`, `silver.cart_items` y `gold.product_daily_revenue` a partir de bronze, y corre sus tests. Es un proceso separado del flujo diario. Casos típicos: después de corregir la lógica de un modelo, o si silver o gold se alteraron por fuera del pipeline. Sin cambios de lógica, el resultado es idéntico.

**Primero, ver qué días hay en bronze.** En una instalación recién levantada hay uno solo: ayer, en UTC.

```bash
docker compose exec warehouse psql -U warehouse -d warehouse -c "select audit_logical_date, count(*) as carts from bronze.carts group by 1 order by 1;"
```

En los comandos siguientes, reemplazar `AAAA-MM-DD` por uno de esos días.

**Desde la UI de Airflow:** en http://localhost:18080/dags/dummyjson_reprocess, botón **Trigger** (arriba a la derecha), completar `force_date` con el día y confirmar con **Trigger**. El campo "Fecha lógica" ("Logical date" con la UI en inglés) se puede dejar como está: el día a reprocesar lo define solo `force_date`.

**Desde la CLI de Airflow:**

```bash
docker compose exec airflow-scheduler airflow dags trigger dummyjson_reprocess --conf '{"force_date": "AAAA-MM-DD"}'
```

**Desde la API REST de Airflow** (en la misma terminal, porque el segundo comando usa `$TOKEN`):

```bash
TOKEN=$(curl -s -X POST http://localhost:18080/auth/token -H 'Content-Type: application/json' -d '{"username": "airflow", "password": "airflow"}' | sed -E 's/.*"access_token":"([^"]+)".*/\1/')
```

```bash
curl -X POST http://localhost:18080/api/v2/dags/dummyjson_reprocess/dagRuns -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {"force_date": "AAAA-MM-DD"}}'
```

El resultado se ve en http://localhost:18080/dags/dummyjson_reprocess/runs: la corrida tiene que terminar en `success`, en pocos segundos.

**Comando manual de dbt equivalente** (sin Airflow):

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector reprocess --vars '{"force_date": "AAAA-MM-DD"}'
```

> No correr el comando manual mientras corre el DAG diario, que arranca a las 00:30 UTC y tarda unos minutos. El DAG `dummyjson_reprocess` no tiene ese problema: comparte con el DAG diario un pool de Airflow de un solo slot, así que sus builds de dbt nunca se superponen.

**Si la fecha no existe en bronze** (o tiene formato inválido), el build falla antes de tocar cualquier tabla, con un mensaje que lo indica (por ejemplo, `force_date 2026-09-01 has no data in bronze.carts`). No hay reprocesos silenciosos que no hagan nada.

**`silver.products` no se reprocesa con esto:** reprocesar un día pasado lo haría volver a un estado más viejo del catálogo. Si hace falta repararlo, se usa el reproceso completo (sección siguiente).

### Reproceso completo, desde bronze

Reconstruye silver y gold enteros a partir de bronze. Por ejemplo, después de corregir un bug en un modelo o una etiqueta de fecha en bronze:

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector silver --full-refresh
```

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --selector gold --full-refresh
```

Primero silver y después gold, en ese orden. Cuándo hace falta y por qué: [DECISIONS.md, sección 1.6](DECISIONS.md#reprocesamiento-completo-dbt-build---full-refresh).

## 7. Bajar y limpiar

Detener los contenedores y **conservar** los datos (bronze, silver, gold y el historial de Airflow):

```bash
docker compose down
```

Detener y **borrar todos los datos**, para volver a empezar de cero:

```bash
docker compose down -v
```

Después de `down -v`, el próximo `docker compose up -d --build --wait` arranca como la primera vez y vuelve a cargar solo el día de ayer. La API no tiene historia, así que los días cargados antes no se pueden recuperar.

## 8. Notas para no confundirse

- **"Triggerer" en rojo en la UI.** En la pantalla de inicio, la sección de salud muestra el Triggerer con una ✗. Es esperado: este proyecto no lo usa (no tiene tasks diferibles), así que no se despliega. El resto (base de metadata, programador, procesador de DAGs) tiene que estar en verde.
- **Si se levanta el stack entre las 00:00 y las 00:30 UTC, la primera corrida automática falla, a propósito.** En esa ventana, la corrida que Airflow lanza es la del intervalo de anteayer, pero la API ya expone los datos de ayer. Una guarda en la extracción lo detecta y falla sin cargar nada, en lugar de guardar los datos con la fecha equivocada. A las 00:30 UTC corre la del día correcto y carga normalmente. Fuera de esa ventana, esto no pasa.
- **Idioma y zona horaria de la UI.** La UI de Airflow toma el idioma y la zona horaria del navegador (se cambia con el reloj de la barra lateral). Por eso la corrida de las 00:30 UTC puede aparecer, por ejemplo, como 21:30 del día anterior en `-03:00`. Las fechas de negocio del pipeline (`audit_logical_date`, `snapshot_date`, `date`) son **siempre UTC**.
- **Tiempos de referencia:**
  - primer arranque: 2 a 3 minutos;
  - primera corrida del DAG: menos de 1 minuto después de que termina el arranque;
  - tests: unos 3 minutos; la primera vez se suman unos segundos de build.
