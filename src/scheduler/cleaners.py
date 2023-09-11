from abc import ABC, abstractmethod
from typing import Iterable

from .connections import ConnectionManager, PostgresConnectionManager
from .models import CronEntry


def scan(tables, scanning_method) -> Iterable[tuple[str, Iterable, Iterable]]:
    for table_name, items in tables:
        for to_be_scheduled, to_be_deleted in scanning_method(table_name, items):
            yield table_name, to_be_scheduled, to_be_deleted
    return


class Cleaner(ABC):
    def __init__(self, manager: ConnectionManager):
        self.manager: ConnectionManager = manager

    @abstractmethod
    def clean(self, table: str, entries: Iterable[CronEntry]):
        ...


class PostgresCleaner(Cleaner):
    def __init__(self, manager: PostgresConnectionManager):
        super().__init__(manager)
        self.manager: PostgresConnectionManager = manager

    def clean(self, table: str, entries: Iterable[CronEntry]):
        ids = tuple(entry.id for entry in entries)

        self.manager.execute(f"delete from {table} where id in {ids}")
