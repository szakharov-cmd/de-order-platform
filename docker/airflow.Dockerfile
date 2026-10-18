FROM apache/airflow:3.1.0-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/dbt-venv /opt/airflow/dbt-artifacts \
    && chown -R airflow:0 /opt/dbt-venv /opt/airflow/dbt-artifacts \
    && chmod -R g+rwX /opt/dbt-venv /opt/airflow/dbt-artifacts

COPY --chown=airflow:0 requirements-dbt.txt /opt/airflow/requirements-dbt.txt

USER airflow

# FAB provides the authentication manager configured in Compose.
RUN pip install --no-cache-dir \
        "apache-airflow==3.1.0" \
        "apache-airflow-providers-fab==2.4.3" \
        --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt \
    && pip check

# Keep dbt dependencies separate from Airflow's Python environment.
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/python -m pip install --no-cache-dir \
        -r /opt/airflow/requirements-dbt.txt \
    && /opt/dbt-venv/bin/python -m pip check

ENV DBT_PROFILES_DIR=/opt/dbt \
    DBT_TARGET_PATH=/opt/airflow/dbt-artifacts/target \
    DBT_LOG_PATH=/opt/airflow/dbt-artifacts/logs