"""
TidaLine Lakehouse Pipeline — Master Orchestration DAG

Automates the full Bronze → Silver → Gold transformation chain:
  1. Register Debezium CDC connector (idempotent)
  2. Spark batch: Bronze CSVs → Silver Iceberg tables
  3. dbt: Silver Iceberg → Gold analytical tables via Trino

The streaming pipeline (seismic_cdc_to_iceberg.py) runs independently
as a long-lived Spark Structured Streaming job.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import logging
import requests

logger = logging.getLogger(__name__)

DEBEZIUM_URL = "http://debezium:8083"
CONNECTOR_CONFIG = "/opt/airflow/debezium/debezium_postgres_source.json"


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def register_debezium_connector(**kwargs):
    """Idempotently register the Debezium Postgres CDC connector."""
    with open(CONNECTOR_CONFIG) as f:
        config = json.load(f)

    connector_name = config["name"]

    # Check if the connector already exists
    try:
        resp = requests.get(
            f"{DEBEZIUM_URL}/connectors/{connector_name}/status",
            timeout=10,
        )
        if resp.status_code == 200:
            state = resp.json().get("connector", {}).get("state", "UNKNOWN")
            logger.info(
                "Connector '%s' already exists (state=%s). Skipping registration.",
                connector_name,
                state,
            )
            return
    except requests.ConnectionError:
        raise Exception(f"Cannot reach Debezium Connect at {DEBEZIUM_URL}")

    # Register
    resp = requests.post(
        f"{DEBEZIUM_URL}/connectors",
        headers={"Content-Type": "application/json"},
        json=config,
        timeout=30,
    )

    if resp.status_code in (200, 201):
        logger.info("Connector '%s' registered successfully.", connector_name)
    elif resp.status_code == 409:
        logger.info("Connector '%s' already exists (409).", connector_name)
    else:
        raise Exception(
            f"Failed to register connector: HTTP {resp.status_code} — {resp.text}"
        )


def run_spark_bronze_to_silver(**kwargs):
    """Execute the Bronze → Silver Spark batch job inside the spark-master container."""
    import docker

    client = docker.from_env()

    try:
        container = client.containers.get("spark-master")
    except docker.errors.NotFound:
        raise Exception(
            "spark-master container not found. Is it running? "
            "Run: docker compose start spark-master"
        )

    cmd = (
        "/opt/spark/bin/spark-submit "
        "--master local[2] "
        "--driver-memory 512m "
        "/opt/spark/jobs/batch/bronze_to_silver.py"
    )

    logger.info("Submitting Spark batch job to spark-master container...")

    exec_id = client.api.exec_create(container.id, cmd)
    output = client.api.exec_start(exec_id["Id"], stream=True)

    for chunk in output:
        for line in chunk.decode("utf-8", errors="replace").splitlines():
            logger.info("[spark] %s", line)

    result = client.api.exec_inspect(exec_id["Id"])
    exit_code = result["ExitCode"]

    if exit_code != 0:
        raise Exception(f"Spark batch job failed with exit code {exit_code}")

    logger.info("Spark Bronze → Silver completed successfully.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "tidaline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="lakehouse_pipeline",
    default_args=default_args,
    description="Master pipeline: Bronze → Silver (Spark) → Gold (dbt/Trino)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pipeline", "master", "lakehouse"],
) as dag:

    t1_debezium = PythonOperator(
        task_id="register_debezium_connector",
        python_callable=register_debezium_connector,
    )

    t2_spark_batch = PythonOperator(
        task_id="spark_bronze_to_silver",
        python_callable=run_spark_bronze_to_silver,
        execution_timeout=timedelta(minutes=30),
    )

    t3_dbt_test_silver = BashOperator(
        task_id="dbt_test_silver_layer",
        bash_command=(
            "cd /opt/airflow/transform && "
            "dbt test --select source:silver --target docker --profiles-dir ."
        ),
        execution_timeout=timedelta(minutes=10),
    )

    t4_dbt_gold = BashOperator(
        task_id="dbt_gold_transformations",
        bash_command=(
            "cd /opt/airflow/transform && "
            "dbt run --target docker --profiles-dir ."
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # Pipeline chain: Register CDC -> Batch to Silver -> Test Silver -> Build Gold
    t1_debezium >> t2_spark_batch >> t3_dbt_test_silver >> t4_dbt_gold
