import requests
import json
import time

connector_config = {
    "name": "earthquake-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "maritime_logistics",
        "database.server.name": "pg-server",
        "schema.include.list": "public",
        "table.include.list": "public.earthquakes",
        "plugin.name": "pgoutput",
        "topic.prefix": "pg-server"
    }
}

print("Registering Debezium connector...")
try:
    response = requests.post(
        "http://debezium:8083/connectors",
        headers={"Content-Type": "application/json"},
        json=connector_config
    )
    if response.status_code in [200, 201]:
        print("Success! Connector created.")
    elif response.status_code == 409:
        print("Connector already exists!")
    else:
        print(f"Failed: {response.status_code} - {response.text}")
except Exception as e:
    print(f"Error connecting to Debezium: {e}")
