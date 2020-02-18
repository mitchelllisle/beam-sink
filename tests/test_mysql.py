from beam_sink.sinks.mysql import MySQLQuery, MySQLConfig
import apache_beam as beam
import unittest
import pytest
from pydantic import ValidationError


class TestMySQL(unittest.TestCase):
    def test_mysql_query_returns(self):
        config = MySQLConfig(host="localhost", username="root", password="password", database="thrillhouse")

        with beam.Pipeline() as p:
            p | 'ReadTable' >> MySQLQuery(config, "select * from thrillhouse")

    def test_success_of_assignment(self):
        config = {"host": "localhost", "username": "root", "password": "password", "database": "thrillhouse"}
        dbconfig = MySQLConfig(**config)
        assert dbconfig.host == config["host"]
        assert dbconfig.username == config["username"]
        assert dbconfig.password == config["password"]
        assert dbconfig.database == config["database"]

    def test_validation_error(self):
        with pytest.raises(ValidationError):
            config = {"host": "localhost", "username": "root", "database": "thrillhouse"}
            MySQLConfig(**config)

