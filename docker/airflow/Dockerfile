FROM apache/airflow:3.3.2-python3.12 AS runtime

ARG PYTHON_MAJOR_MINOR=3.12
ARG DBT_VENV=/opt/dbt-venv
ENV AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR}.txt"

# Isolated virtualenv for dbt (no --system-site-packages), so dbt's dependencies
# never conflict with Airflow's.
USER root
RUN python -m venv "${DBT_VENV}" \
    && chown -R airflow:0 "${DBT_VENV}"

USER airflow

# Airflow runtime dependencies, resolved against the official constraints file.
COPY requirements/airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        -r /tmp/requirements-airflow.txt \
        --constraint "${AIRFLOW_CONSTRAINTS_URL}"

# dbt, fully pinned (including transitive dependencies).
COPY requirements/dbt.txt /tmp/requirements-dbt.txt
RUN "${DBT_VENV}/bin/pip" install --no-cache-dir -r /tmp/requirements-dbt.txt


# Runtime image + lint/test tooling. Used only by the `tests` compose service.
FROM runtime AS dev

COPY requirements/dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        -r /tmp/requirements-dev.txt \
        --constraint "${AIRFLOW_CONSTRAINTS_URL}"
