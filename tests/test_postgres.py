from beam_sink.sinks.postgres import ReadPostgres, WriteToPostgres, PostgresConfig
import apache_beam as beam
import unittest
import pytest
import json
from pydantic import ValidationError
import psycopg2 as postgres
import os

HOST = "localhost" if os.getenv("DEVELOP", False) else "postgres"


def setup_database(config: PostgresConfig):
    cnx = postgres.connect(
        host=config.host, port=config.port, user=config.user, password=config.password, database=config.database
    )
    cur = cnx.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS thrillhouse (id text, description text, amount text)")
    cnx.commit()
    cnx.close()


class TestPostgres(unittest.TestCase):
    def setUp(self) -> None:
        self.config = PostgresConfig(host=HOST, user="postgres", password="root", database="thrillhouse")
        setup_database(self.config)

    def test_mysql_query_returns(self):
        with beam.Pipeline() as p:
            p | 'ReadTable' >> ReadPostgres(self.config, "select * from thrillhouse")

    def test_success_of_assignment(self):
        config = {"host": HOST, "user": "postgres", "password": "root", "database": "thrillhouse"}
        dbconfig = PostgresConfig(**config)
        assert dbconfig.host == config["host"]
        assert dbconfig.user == config["user"]
        assert dbconfig.password == config["password"]
        assert dbconfig.database == config["database"]

    def test_validation_error(self):
        with pytest.raises(ValidationError):
            config = {"host": HOST, "user": "postgres", "database": "thrillhouse"}
            PostgresConfig(**config)

    def test_mysql_insert(self):
        with beam.Pipeline() as p:
            (
                p
                | 'ReadJson' >> beam.io.ReadFromText("tests/.data/test.jsonl")
                | 'Parse' >> beam.Map(lambda x: [json.loads(x)])
                | 'WriteData' >> WriteToPostgres(self.config, "thrillhouse", ["id", "description", "amount"])
            )

    def test_mysql_insert_with_partitions(self):
        with beam.Pipeline() as p:
            output = (
                p
                | 'ReadJson' >> beam.io.ReadFromText("tests/.data/test.jsonl")
                | 'Parse' >> beam.Map(lambda x: json.loads(x))
                | 'Batch' >> beam.BatchElements(2)
                | 'WriteData' >> WriteToPostgres(self.config, "thrillhouse", ["id", "description", "amount"])
            )
