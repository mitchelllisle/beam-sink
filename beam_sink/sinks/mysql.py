import apache_beam as beam
import mysql.connector as mysql
from pydantic import BaseModel
from typing import Iterator, Dict, List


class MySQLConfig(BaseModel):
    host: str
    port: int = 3306
    username: str
    password: str
    database: str


class InvalidElementType(TypeError):
    """Error Class for when an Element is not the correct type"""


class ReadMySQL(beam.PTransform):
    """
    Query MySQL databases
    Args:
        dbconfig: Supports passing any config elements to the mysql.connector library. Most
        commonly this will be:
            host - host of database
            port - 3306 is the MySQL default
            username - User to connect as
            password -  Password for username
            database - The database to connect to
        query: The query to run against the database
    """
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


class WriteToMySQL(beam.PTransform):
    """
    Insert Data into MySQL databases
    Args:
        dbconfig: Supports passing any config elements to the mysql.connector library. Most
        commonly this will be:
            host - host of database
            port - 3306 is the MySQL default
            username - User to connect as
            password -  Password for username
            database - The database to connect to
        table: The table name to insert into
        columns: The columns contained in the rows to be included
    """
    def __init__(
            self,
            dbconfig: MySQLConfig,
            table: str,
            columns: List,
            batch_size: int = 10000,
            upsert: bool = False,
            **kwargs
    ):
        super().__init__()
        self.dbconfig = dbconfig
        self.table = table
        self.columns = columns
        self.upsert = upsert
        self.batch_size = batch_size
        self.kwargs = kwargs

    def expand(self, pcoll):
        return (
            pcoll
            | beam.BatchElements(min_batch_size=self.batch_size, **self.kwargs)
            | beam.ParDo(_Insert(self.table, self.columns, self.dbconfig, self.upsert))
        )


class _Query(beam.DoFn):
    """An internal DoFn to be used in a PTransform. Not for external use.
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
    """An internal DoFn to be used in a PTransform. Not for external use.
    """
    def __init__(self, table: str, columns: List, config: MySQLConfig, upsert: bool = False):
        super().__init__()
        self.config = config
        self.table = table
        self.cols = columns
        self.conn = None
        self.cursor = None
        self.upsert = upsert

    def start_bundle(self) -> None:
        self.conn = mysql.connect(**self.config.dict())
        self.cursor = self.conn.cursor(dictionary=True)

    def process(self, element) -> None:
        raise NotImplementedError

    def finish_bundle(self):
        self.conn.close()


class _Insert(_PutFn):
    """An internal DoFn to be used in a PTransform. Not for external use.
    """
    def process(self, element) -> None:
        stmt = (
            f"INSERT INTO `{self.table}`"
            f"({', '.join(self.cols)}) VALUES ({', '.join([f'%({col})s' for col in self.cols])})"
        )
        if self.upsert is True:
            stmt = (
                f"{stmt} ON DUPLICATE KEY UPDATE "
                f"{', '.join([f'{col} = VALUES({col})' for col in self.cols])}"
            )
        if isinstance(element, list):
            self.cursor.executemany(stmt, element)
            self.conn.commit()
        else:
            raise InvalidElementType(f"Wrong element type. Expected List[Dict], received {type(element)}")
