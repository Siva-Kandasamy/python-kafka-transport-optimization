"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": "stations_connect",
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "topic.prefix": "connect-",
                    "mode": "incrementing",
                    "incrementing.column.name": "stop_id",
                    "table.whitelist": "stations",
                    "tasks.max": 1,
                    "connection.url": "jdbc:postgresql://localhost:5432/cta",
                    "connection.user": "cta_admin",
                    "connection.password": "chicago",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false"
                },
            }
        ),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        print(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    print("connector created successfully.")
    print("Use kafka-console-consumer and kafka-topics to see data!")
        #return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    logger.info("connector code not completed skipping connector creation")
    #resp = requests.post(
    #    KAFKA_CONNECT_URL,
    #    headers={"Content-Type": "application/json"},
    #    data=json.dumps({
    #        "name": CONNECTOR_NAME,
    #        "config": {
    #            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    #            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    #            "key.converter.schemas.enable": "false",
    #            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    #            "value.converter.schemas.enable": "false",
    #            "batch.max.rows": "500",
    #            # TODO
    #            "connection.url": "",
    #            # TODO
    #            "connection.user": "",
    #            # TODO
    #            "connection.password": "",
    #            # TODO
    #            "table.whitelist": "",
    #            # TODO
    #            "mode": "",
    #            # TODO
    #            "incrementing.column.name": "",
    #            # TODO
    #            "topic.prefix": "",
    #            # TODO
    #            "poll.interval.ms": "",
    #        }
    #    }),
    #)

    ## Ensure a healthy response was given
    #resp.raise_for_status()
    #logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
