import apache_beam as beam
import mysql.connector as mysql
from pydantic import BaseModel
from typing import Iterator, Dict, Optional, List


class MySQLConfig(BaseModel):
    host: str
    port: int = 3306
    username: str
    password: str
    database: str


class MySQLQuery(beam.PTransform):
    def __init__(self, dbconfig: MySQLConfig, query: str):
        super().__init__()
        self.dbconfig = dbconfig
        self.query = query

    def expand(self, pcoll):
        return (
            pcoll
            | beam.Create([self.query])
            | beam.ParDo(_Query(self.dbconfig))
        )


class MySQLInsert(beam.PTransform):
    def __init__(self, dbconfig: MySQLConfig, table: str, columns: List):
        super().__init__()
        self.dbconfig = dbconfig
        self.table = table
        self.columns = columns

    def expand(self, pcoll):
        return (
            pcoll
            | beam.ParDo(_Insert(self.table, self.columns, self.dbconfig))
        )


class _Query(beam.DoFn):
    """MySQLQuery
    Query MySQL databases
    Args:
        config: Supports passing any config elements to the mysql.connector library. Most
        commonly this will be:
        host - host of database
        port - 3306 is the MySQL default
        username - User to connect as
        password -  Password for username
        database - The database to connect to
    """
    def __init__(self, config: MySQLConfig):
        super().__init__(config)
        self.config = config
        self.conn = None
        self.cursor = None

    def start_bundle(self) -> None:
        self.conn = mysql.connect(**self.config.dict())
        self.cursor = self.conn.cursor(dictionary=True)

    def process(self, query) -> Iterator[Dict]:
        self.cursor.execute(query)
        for result in self.cursor:
            yield result

    def finish_bundle(self) -> None:
        if self.conn is not None:
            self.conn.close()


class _PutFn(beam.DoFn):
    def __init__(self, table: str, columns: List, config: MySQLConfig):
        super().__init__()
        self.config = config
        self.table = table
        self.cols = columns
        self.conn = None
        self.cursor = None

    def start_bundle(self) -> None:
        self.conn = mysql.connect(**self.config.dict())
        self.cursor = self.conn.cursor(dictionary=True)

    def process(self, element) -> None:
        raise NotImplementedError

    def finish_bundle(self):
        try:
            self.conn.commit()
        finally:
            self.conn.close()


class _Insert(_PutFn):
    def process(self, element) -> None:
        stmt = f"INSERT INTO `{self.table}` ({', '.join(self.cols)}) VALUES ({', '.join([f'%({col})s' for col in self.cols])});"
        self.cursor.execute(stmt, element)