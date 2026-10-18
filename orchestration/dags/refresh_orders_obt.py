from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="refresh_orders_obt",
    description="Проверка Silver и обновление витрины заказов",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 10, 18, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "sergey",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["dbt", "clickhouse", "orders"],
) as dag:
    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=(
            "/opt/dbt-venv/bin/dbt test "
            "--select path:models/silver "
            "--exclude orders_obt"
        ),
        cwd="/opt/dbt",
        do_xcom_push=False,
    )

    refresh_gold = BashOperator(
        task_id="refresh_gold",
        bash_command="/opt/dbt-venv/bin/dbt run --select orders_obt",
        cwd="/opt/dbt",
        do_xcom_push=False,
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command="/opt/dbt-venv/bin/dbt test --select orders_obt",
        cwd="/opt/dbt",
        do_xcom_push=False,
    )

    validate_silver >> refresh_gold >> validate_gold