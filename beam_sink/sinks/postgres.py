import apache_beam as beam
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel
from typing import Iterator, Dict, List


class PostgresConfig(BaseModel):
    host: str
    port: int = 5432
    user: str
    password: str
    database: str


class InvalidElementType(TypeError):
    """Error Class for when an Element is not the correct type"""


class ReadPostgres(beam.PTransform):
    """
    Query Postgres databases
    Args:
        dbconfig: Supports passing any config elements to the psycopg2 library. Most
        commonly this will be:
            host - host of database
            port - 5432 is the Postgres default
            username - User to connect as
            password -  Password for username
            database - The database to connect to
        query: The query to run against the database
    """
    def __init__(self, dbconfig: PostgresConfig, query: str):
        super().__init__()
        self.dbconfig = dbconfig
        self.query = query

    def expand(self, pcoll):
        return (
            pcoll
            | beam.Create([self.query])
            | beam.ParDo(_Query(self.dbconfig))
        )


class WriteToPostgres(beam.PTransform):
    """
    Insert Data into Postgres databases
    Args:
        dbconfig: Supports passing any config elements to the psycopg2 library. Most
        commonly this will be:
            host - host of database
            port - 5432 is the Postgres default
            username - User to connect as
            password -  Password for username
            database - The database to connect to
        table: The table name to insert into
        columns: The columns contained in the rows to be included
    """
    def __init__(self, dbconfig: PostgresConfig, table: str, columns: List, batch_size: int = 10000, **kwargs):
        super().__init__()
        self.dbconfig = dbconfig
        self.table = table
        self.columns = columns
        self.batch_size = batch_size
        self.kwargs = kwargs

    def expand(self, pcoll):
        return (
            pcoll
            | beam.BatchElements(min_batch_size=self.batch_size, **self.kwargs)
            | beam.ParDo(_Insert(self.table, self.columns, self.dbconfig))
        )


class _Query(beam.DoFn):
    """An internal DoFn to be used in a PTransform. Not for external use.
    """
    def __init__(self, config: PostgresConfig):
        super().__init__(config)
        self.config = config
        self.conn = None
        self.cursor = None

    def start_bundle(self) -> None:
        self.conn = psycopg2.connect(**self.config.dict())
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)

    def process(self, query) -> Iterator[Dict]:
        self.cursor.execute(query)
        for result in self.cursor:
            yield dict(result)

    def finish_bundle(self) -> None:
        if self.conn is not None:
            self.conn.close()


class _PutFn(beam.DoFn):
    """An internal DoFn to be used in a PTransform. Not for external use.
    """
    def __init__(self, table: str, columns: List, config: PostgresConfig):
        super().__init__()
        self.config = config
        self.table = table
        self.cols = columns
        self.conn = None
        self.cursor = None

    def start_bundle(self) -> None:
        self.conn = psycopg2.connect(**self.config.dict())
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)

    def process(self, element) -> None:
        raise NotImplementedError

    def finish_bundle(self):
        self.conn.close()


class _Insert(_PutFn):
    """An internal DoFn to be used in a PTransform. Not for external use.
    """
    def process(self, element: List[Dict]) -> None:
        stmt = f"INSERT INTO {self.table} ({', '.join(self.cols)}) VALUES ({', '.join([f'%({col})s' for col in self.cols])});"
        if isinstance(element, list):
            self.cursor.executemany(stmt, element)
            self.conn.commit()
        else:
            raise InvalidElementType(f"Wrong element type. Expected List[Dict], received {type(element)}")
