import airflow.utils.dates
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


from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    tags=["pipeline", "master", "lakehouse"],
) as dag:

    t1_debezium = PythonOperator(
        task_id="register_debezium_connector",
        python_callable=register_debezium_connector,
    )

    t2_spark_batch = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        application="/opt/spark/jobs/batch/bronze_to_silver.py",
        conn_id="spark_default",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.driver.memory": "512m",
        },
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18,org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        execution_timeout=timedelta(minutes=30),
    )

    t3_dbt_test_silver = BashOperator(
        task_id="dbt_test_silver_layer",
        bash_command=(
            "cd /opt/airflow/transform && "
            "/home/airflow/dbt_venv/bin/dbt test --select source:silver --target docker --profiles-dir ."
        ),
        execution_timeout=timedelta(minutes=10),
    )

    t4_dbt_gold = BashOperator(
        task_id="dbt_gold_transformations",
        bash_command=(
            "cd /opt/airflow/transform && "
            "/home/airflow/dbt_venv/bin/dbt run --target docker --profiles-dir ."
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # Pipeline chain: Register CDC -> Batch to Silver -> Test Silver -> Build Gold
    t1_debezium >> t2_spark_batch >> t3_dbt_test_silver >> t4_dbt_gold
