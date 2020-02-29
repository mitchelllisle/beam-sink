# BeamSink
![Tests](https://github.com/mitchelllisle/beam-sink/workflows/Tests/badge.svg?branch=master)
[![PyPI version](https://badge.fury.io/py/beam-sink.svg)](https://badge.fury.io/py/beam-sink)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/aa0103c0a79c4f7c9188cf4e0fd4ad83)](https://www.codacy.com/manual/lislemitchell/beam-sink?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=mitchelllisle/beam-sink&amp;utm_campaign=Badge_Grade)
[![Known Vulnerabilities](https://snyk.io/test/github/mitchelllisle/beam-sink/badge.svg?targetFile=requirements.txt)](https://snyk.io/test/github/mitchelllisle/beam-sink?targetFile=requirements.txt)


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
from beam_sink import ReadMySQL, WriteToMySQL, MySQLConfig
import json

# First, we initialise a DB config object that can validate we're providing the right information
config = MySQLConfig(host="localhost", username="lenny", password="karl", database="springfield")

# Then we write a query 
query = "select * from thrillhouse"

# Initialise your Beam Pipeline the way you normally would
with beam.Pipeline() as p:
    (
        p 
        | 'ReadTable' >> ReadMySQL(config, query)
        | 'PrintResult' >> beam.ParDo(lambda x: print(x))
    )

# We can also insert data into a table
table = "thrillhouse"
columns = ["id", "description", "amount"]

with beam.Pipeline() as p:
    (
        p
        | 'ReadJson' >> beam.io.ReadFromText("tests/.data/test.jsonl")
        | 'Parse' >> beam.Map(lambda x: json.loads(x))
        | 'WriteData' >> WriteToMySQL(config, table, columns)
    )
```

| Other operations such as Upsert or DB commands coming soon...
