import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from uuid import UUID

from .connections import ConnectionManager, RedisConnectionManager
from .models import Entry


class BaseStorage(ABC):
    def __init__(self, conn_mann: ConnectionManager):
        self.conn_mann = conn_mann

    @abstractmethod
    def save_state(self, state: dict[str, Any]):
        ...

    @abstractmethod
    def retrieve_state(self) -> dict[str, Any]:
        ...


class RedisStorage(BaseStorage):
    def __init__(self, conn_mann: RedisConnectionManager):
        super().__init__(conn_mann)
        self.redis = conn_mann.get_connection()

    def save_state(self, state: dict[str, Any]):
        serialized = json.dumps(state)
        self.conn_mann.back_connection()(self.redis.set)("etl_state", serialized)

    def retrieve_state(self) -> dict[str, Any]:
        serialized = self.conn_mann.back_connection()(self.redis.get)("etl_state")
        if not serialized:
            return {}
        return json.loads(serialized)


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Entry):
        state = self.storage.retrieve_state()
        state[key] = value.json()
        self.storage.save_state(state)

    def get_state(self, key: str) -> Entry:
        state = self.storage.retrieve_state()
        value = state.get(key)
        if not value:
            return Entry(modified=datetime(1, 1, 1, 1, 1, 1, 1), id=UUID(int=0))
        return Entry.parse_raw(value)
