import apache_beam as beam
import mysql.connector as mysql
from typing import List, Dict


class MySQLQuery(beam.DoFn):
    """MySQLQuery
    Query MySQL databases
    Args:
        config: Supports passing any config elements to the mysql.connector library. Most
        commonly this will be:
        host: host of database
        port: 3306 is the MySQL default
        username: User to connect as
        password: Password for username
        database: The database to connect to
    """
    def __init__(self, **config):
        super().__init__(**config)
        self.config = config
        self.conn = None
        self.cursor = None

    def start_bundle(self) -> None:
        self.conn = mysql.connect(**self.config)
        self.cursor = self.conn.cursor(dictionary=True)

    def process(self, query: str) -> List[Dict]:
        self.cursor.execute(query)
        for result in self.cursor:
            yield result

    def finish_bundle(self) -> None:
        if self.conn is not None:
            self.conn.close()


class MySQLInsert(beam.DoFn):
    def __init__(self, table, columns, **config):
        super().__init__(**config)
        self.config = config
        self.table = table
        self.cols = columns
        self.conn = None
        self.cursor = None

    def start_bundle(self) -> None:
        self.conn = mysql.connect(**self.config)
        self.cursor = self.conn.cursor(dictionary=True)

    def process(self, element) -> None:
        self.cursor.execute(
            f"REPLACE {self.table} ({', '.join(self.cols)}) VALUES ({', '.join([f'%s' for _ in self.cols])});",
            element
        )

    def finish_bundle(self):
        try:
            self.conn.commit()
        finally:
            self.conn.close()
