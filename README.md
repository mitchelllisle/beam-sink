# BeamSink
🤖 An Apache Beam Sink Library for Databases and other Sinks
🐘 Supports MySQL (Postgres, Elasticsearch)

## Installation

```shell script
pip install beam_sink
```

## Usage
### MySQL

```python
import apache_beam as beam
from beam_sink.sinks.mysql import MySQLQuery, DBConfig

config = DBConfig(host="localhost", username="lenny", password="karl", database="springfield")

with beam.Pipeline() as p:
    (
        p 
        | 'ReadTable' >> MySQLQuery(config, "select * from thrillhouse")
        | 'PrintResult' >> beam.ParDo(lambda x: print(x))
    )

```
