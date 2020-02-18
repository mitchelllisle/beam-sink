# BeamSink

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
from beam_sink import MySQLQuery, MySQLConfig

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

```
