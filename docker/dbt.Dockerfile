FROM python:3.12-slim-bookworm
WORKDIR /app/dbt
COPY requirements-dbt.txt /app/requirements-dbt.txt
ENV DBT_PROFILES_DIR=/app/dbt
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
RUN python -m pip install --no-cache-dir -r /app/requirements-dbt.txt
ENTRYPOINT ["dbt"]