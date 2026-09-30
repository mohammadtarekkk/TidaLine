FROM apache/airflow:2.9.1
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.9.1" boto3 beautifulsoup4 requests apache-airflow-providers-apache-spark pyspark==3.5.1 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt"

RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-trino
