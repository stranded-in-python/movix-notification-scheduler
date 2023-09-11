import logging
from abc import ABC, abstractmethod
from contextlib import closing
from functools import wraps
from time import sleep
from typing import Any, Callable, Iterable, Mapping, cast

from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import BulkIndexError, bulk
from psycopg2 import OperationalError
from psycopg2 import connect as pg_connect
from psycopg2.extensions import connection as pg_connection
from psycopg2.extras import DictCursor, DictRow
from redis import Redis
from redis.exceptions import ConnectionError, RedisError

from .config.settings import settings
from .exceptions import ConnectionFailedError, DataInconsistentError


def get_cursor(connection: pg_connection) -> DictCursor:
    return cast(DictCursor, connection.cursor())


class Connector(ABC):
    def __init__(self):
        self.connection: Elasticsearch | pg_connection | Redis | None = None

    @abstractmethod
    def _connect(self) -> Elasticsearch | pg_connection | Redis:
        ...

    @abstractmethod
    def _ping(self):
        ...

    def connect(self) -> Elasticsearch | pg_connection | Redis:
        if not self.connection:
            self.connection = self._connect()
        return self.connection

    def reconnect(self):
        self.connection = self._connect()
        self._ping()


class PostgresConnector(Connector):
    def __init__(self, dsl: dict | None = None):
        super().__init__()
        if not dsl:
            self.dsl = {
                "dbname": settings.postgres_db,
                "user": settings.postgres_user,
                "password": settings.postgres_password,
                "host": settings.postgres_host,
                "port": settings.postgres_port,
            }
        else:
            self.dsl = dsl

    def _ping(self):
        self.connection: pg_connection
        cursor = self.connection.cursor()
        cursor.execute("SELECT 1;")
        cursor.close()

    def _connect(self) -> pg_connection:
        return pg_connect(**self.dsl, cursor_factory=DictCursor)


class ElasticConnector(Connector):
    def __init__(self, endpoint: str | None = None):
        super().__init__()
        if not endpoint:
            self.endpoint = settings.elastic_endpoint
        else:
            self.endpoint = endpoint

    def _ping(self):
        self.connection: Elasticsearch
        if not self.connection.ping():
            raise TransportError("No ping from ElasticSearch")

    def _connect(self) -> Elasticsearch:
        return Elasticsearch(self.endpoint)


class RedisConnector(Connector):
    def __init__(self, host: str | None = None, port: int | None = None):
        super().__init__()
        if not host:
            self.host = settings.redis_host
        else:
            self.host = host
        if not port:
            self.port = settings.redis_port
        else:
            self.port = port

    def _ping(self):
        self.connection.ping()  # type: ignore

    def _connect(self) -> Redis:
        return Redis(self.host, self.port, decode_responses=True)


def backing_connect(connector: Connector) -> Callable:
    def decorator(func: Callable):
        @wraps(func)
        def inner(*args, **kwargs):
            wait = settings.first_nap
            time_passed = 0
            is_connected = True
            while time_passed < settings.wait_up_to:
                try:
                    # trying to reconnect
                    if not is_connected:
                        connector.reconnect()  # type: ignore

                    return func(*args, **kwargs)
                except (
                    OperationalError or TransportError or ConnectionError or RedisError
                ) as e:
                    logging.error(e)
                    is_connected = False
                    time_passed += wait
                    wait = (
                        wait * settings.waiting_factor
                        if wait < settings.waiting_interval
                        else settings.waiting_interval
                    )
                    logging.debug(f"Sleeping for {wait} seconds")
                    sleep(wait)
            raise ConnectionFailedError("Waiting for connection exceeded limit")

        return inner

    return decorator


class ConnectionManager:
    def __init__(self, connector: Connector):
        self.connector = connector
        self._connection = None

    def back_connection(self) -> Callable:
        return backing_connect(self.connector)

    def get_connection(self) -> pg_connection | Redis | Elasticsearch:
        if self._connection:
            return self._connection

        self._connection = self.back_connection()(self.connector.connect)()
        return self._connection

    @property
    def connection(self) -> pg_connection | Redis | Elasticsearch | None:
        return self.get_connection()

    @connection.setter
    def connection(self, connection: pg_connection | Redis | Elasticsearch | None):
        self._connection = connection

    def __exit__(self, *args):
        if not self._connection:
            return

        self._connection.close()
        self._connection = None  # type: ignore
        logging.debug("Closed connection")

    def __enter__(self):
        return self


class RedisConnectionManager(ConnectionManager):
    def __init__(self, connector: RedisConnector):
        super().__init__(connector)

    def get_connection(self) -> Redis:
        return cast(Redis, super().get_connection())


class PostgresConnectionManager(ConnectionManager):
    def __init__(self, connector: PostgresConnector):
        self.cursor_name_prefix = 1
        super().__init__(connector)

    def get_connection(self) -> pg_connection:
        return cast(pg_connection, super().get_connection())

    def cursor(self, itersize: int = 10000) -> DictCursor:
        # prevent cursor name from bloating
        if self.cursor_name_prefix > 10000000:
            self.cursor_name_prefix = 1

        if itersize < 1:
            return self.back_connection()(self.get_connection().cursor)()

        # create server side cursor to limit memory use
        cursor = self.back_connection()(self.get_connection().cursor)(
            name=f"cursor_{self.cursor_name_prefix}"
        )

        self.cursor_name_prefix += 1

        logging.debug(f"Created cursor {self.cursor_name_prefix}")
        cursor.itersize = itersize
        return cursor

    def _execute(
        self, cursor: DictCursor, sql: str, sql_vars: Any = None
    ) -> DictCursor:
        if not sql_vars:
            cursor.execute(sql)
        else:
            cursor.execute(sql, sql_vars)
        return cursor

    def _fetchall(self, sql: str, sql_vars: Any = None) -> Iterable[DictRow]:
        """
        cursor.fetchall() with reconnect
        """

        with closing(self.cursor()) as cursor:
            return self._execute(cursor, sql, sql_vars).fetchall()

    def _fetchone(self, sql, sql_vars: Any = None, itersize: int = 1) -> DictRow | None:
        with closing(self.cursor(itersize)) as cursor:
            return self._execute(cursor, sql, sql_vars).fetchone()

    def _fetchmany(
        self, sql: str, size: int, sql_vars: Any = None, itersize: int = 0
    ) -> Iterable[Iterable[DictRow]]:
        with closing(self.cursor(itersize)) as cursor:
            self._execute(cursor, sql, sql_vars)

            while True:
                rows = cursor.fetchmany(size)
                if len(rows) > 0:
                    yield rows
                else:
                    return

    def _execute_sql(self, sql: str, sql_vars: Any = None):
        with closing(self.cursor()) as cursor:
            self._execute(cursor, sql, sql_vars)

    def fetchall(
        self, sql: str, sql_vars: Any = None, itersize: int = 0
    ) -> Iterable[DictRow]:
        """
        cursor.fetchall() with reconnect
        """

        return self.back_connection()(self._fetchall)(sql, sql_vars, itersize)

    def fetchone(
        self, sql: str, sql_vars: Any = None, itersize: int = 0
    ) -> Iterable[DictRow]:
        """
        cursor.fetchone() with reconnect
        """

        return self.back_connection()(self._fetchone)(sql, sql_vars)

    def fetchmany(self, sql: str, size: int, sql_vars: Any = None, itersize: int = 0):
        """
        cursor.fetchmany with reconnect and batch control

        sql: str - SQL expression
        size: int - number of rows to read
        close: bool - close after first batch
        intersize: int - if > 0, creates server-side cursor with
            len(batch) == intersize

        """

        return self.back_connection()(self._fetchmany)(sql, size, sql_vars, itersize)

    def execute(self, sql: str, sql_vars: Any = None):
        return self.back_connection()(self._execute_sql)(sql)
