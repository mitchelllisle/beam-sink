import apache_beam as beam
from beam_sink import WriteToMySQL, ReadMySQL, MySQLConfig
import os
import json

HOST = "localhost" if os.getenv("DEVELOP", False) else "mysql"
config = MySQLConfig(host=HOST, username="root", password="root", database="thrillhouse")


def test_bench_mysql_query_returns(benchmark):
    p = beam.Pipeline()
    p | (ReadMySQL(config, "select * from thrillhouse"))
    benchmark(p.run)


def test_bench_mysql_insert(benchmark):
    p = beam.Pipeline()
    (
        p
        | 'ReadJson' >> beam.io.ReadFromText("tests/.data/test.jsonl")
        | 'Parse' >> beam.Map(lambda x: [json.loads(x)])
        | 'WriteData' >> WriteToMySQL(config, "thrillhouse", ["id", "description", "amount"])
    )
    benchmark(p.run)
