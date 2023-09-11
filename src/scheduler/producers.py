from abc import ABC, abstractmethod
from typing import Iterable

from scheduler.config import settings

from .connections import ConnectionManager, PostgresConnectionManager
from .models import CronEntry
from .state import State


def scan(tables, scanning_method) -> Iterable[tuple[str, Iterable, Iterable]]:
    for table_name, items in tables:
        for to_be_scheduled, to_be_deleted in scanning_method(table_name, items):
            yield table_name, to_be_scheduled, to_be_deleted
    return


class Producer(ABC):
    def __init__(self, state: State, manager: ConnectionManager):
        self.state: State = state
        self.not_processed_entities = {}
        self.manager: ConnectionManager = manager

    def set_state(self, table: str):
        entity = self.not_processed_entities[table]
        self.state.set_state(f"scheduler:{table}", entity)
        # this batch processed sucessfully
        self.not_processed_entities[table] = None

    @abstractmethod
    def scan_table(self, table: str, items: int = 50) -> Iterable:
        ...

    def scan(
        self, tables: Iterable[tuple[str, int]] | None = None
    ) -> Iterable[tuple[str, Iterable, Iterable]]:
        return scan(tables, self.scan_table)


class PostgresProducer(Producer):
    def __init__(self, state: State, manager: PostgresConnectionManager):
        super().__init__(state, manager)
        self.manager: PostgresConnectionManager = manager

    def scan_table(
        self, table: str, pack_size: int
    ) -> Iterable[tuple[Iterable, Iterable]]:
        state = self.state.get_state(f"scheduler:{table}")
        date_field = "updated_at"

        sql = (
            f"select {date_field}, id, status, cron_str from {table} where {date_field} >= '{state.modified}' "
            f"and id >= '{state.id}' order by {date_field} asc, id asc;"
        )

        for rows in self.manager.fetchmany(sql, pack_size, itersize=5000):
            to_be_scheduled = []
            to_be_deleted = []

            for row in rows:
                entry = CronEntry(
                    modified=row[0], id=row[1], status=row[2], cron_str=row[3]
                )
                if entry.status == settings.CronStatuses.PENDING:
                    to_be_scheduled.append(entry)
                else:
                    to_be_deleted.append(entry)

            # Processing didn't go on happy path

            if self.not_processed_entities.get(table):
                return

            # Remembering current batch

            self.not_processed_entities[table] = rows[-1]

            yield to_be_scheduled, to_be_deleted
