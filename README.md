# BeamSink
![Tests](https://github.com/mitchelllisle/beam-sink/workflows/Tests/badge.svg?branch=master)
[![codecov](https://codecov.io/gh/mitchelllisle/beam-sink/branch/master/graph/badge.svg)](https://codecov.io/gh/mitchelllisle/beam-sink)
[![PyPI version](https://badge.fury.io/py/beam-sink.svg)](https://badge.fury.io/py/beam-sink)

🤖 An Apache Beam Sink Library for Databases and other Sinks

🐘 Supports MySQL (Postgres, Elasticsearch coming soon...)

## Installation

```shell script
pip install beam_sink
```

## Usage
### MySQL

```python
import apache_beam as beam
from beam_sink import MySQLQuery, MySQLInsert, MySQLConfig
import json

# First, we initialise a DB config object that can validate we're providing the right information
config = MySQLConfig(host="localhost", username="lenny", password="karl", database="springfield")

# Then we write a query 
query = "select * from thrillhouse"

# Initialise your Beam Pipeline the way you normally would
with beam.Pipeline() as p:
    (
        p 
        | 'ReadTable' >> MySQLQuery(config, query)
        | 'PrintResult' >> beam.ParDo(lambda x: print(x))
    )

# We can also insert data into a table
table = "thrillhouse"
columns = ["id", "description", "amount"]

with beam.Pipeline() as p:
    (
        p
        | 'ReadJson' >> beam.io.ReadFromText("tests/.data/test.json")
        | 'Parse' >> beam.Map(lambda x: json.loads(x))
        | 'WriteData' >> MySQLInsert(config, table, columns)
    )
```

| Other operations such as Upsert or DB commands coming soon...
