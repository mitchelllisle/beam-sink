from beam_sink.sinks.mysql import MySQLQuery, MySQLInsert, MySQLConfig
import apache_beam as beam
import unittest
import pytest
import json
from pydantic import ValidationError
import mysql.connector as mysql


def setup_database(config: MySQLConfig):
    cnx = mysql.connect(host=config.host, port=config.port, username=config.username, password=config.password)
    cur = cnx.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS thrillhouse")
    cnx.database = "thrillhouse"
    cur.execute("CREATE TABLE IF NOT EXISTS thrillhouse (id text, description text, amount text)")
    cnx.commit()
    cnx.close()


class TestMySQL(unittest.TestCase):
    def setUp(self) -> None:
        self.config = MySQLConfig(host="localhost", username="root", password="root", database="thrillhouse")
        setup_database(self.config)

    def test_mysql_query_returns(self):
        with beam.Pipeline() as p:
            p | 'ReadTable' >> MySQLQuery(self.config, "select * from thrillhouse")

    def test_success_of_assignment(self):
        config = {"host": "localhost", "username": "root", "password": "root", "database": "thrillhouse"}
        dbconfig = MySQLConfig(**config)
        assert dbconfig.host == config["host"]
        assert dbconfig.username == config["username"]
        assert dbconfig.password == config["password"]
        assert dbconfig.database == config["database"]

    def test_validation_error(self):
        with pytest.raises(ValidationError):
            config = {"host": "localhost", "username": "root", "database": "thrillhouse"}
            MySQLConfig(**config)

    def test_mysql_insert(self):
        with beam.Pipeline() as p:
            (
                p
                | 'ReadJson' >> beam.io.ReadFromText("tests/.data/test.json")
                | 'Parse' >> beam.Map(lambda x: json.loads(x))
                | 'WriteData' >> MySQLInsert(self.config, "thrillhouse", ["id", "description", "amount"])
            )

