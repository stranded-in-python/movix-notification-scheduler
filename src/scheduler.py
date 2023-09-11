"""Loading data from Postgresql to ElasticSearch"""
import logging
import os
from time import sleep

from scheduler.cleaners import Cleaner, PostgresCleaner
from scheduler.config.settings import settings
from scheduler.connections import (
    PostgresConnectionManager,
    PostgresConnector,
    RedisConnectionManager,
    RedisConnector,
)
from scheduler.exceptions import Error
from scheduler.producers import PostgresProducer, Producer
from scheduler.schedulers import NotificationScheduler, Scheduler
from scheduler.state import RedisStorage, State

logger = logging.getLogger()
logger.setLevel(os.environ.get("ETL_LOG_LEVEL", logging.INFO))


class ScheduleManager:
    producer: Producer
    scheduler: Scheduler
    cleaner: Cleaner

    def schedule(self):
        for table, to_be_scheduled, to_be_deleted in self.producer.scan(
            settings.tables_for_scan
        ):
            logging.debug(f"Watching table {table}")
            try:
                self.scheduler.unschedule(to_be_deleted)
                self.scheduler.schedule(to_be_scheduled)
                self.cleaner.clean(to_be_deleted)
            except Error as e:
                logging.error(e)
                continue
            self.producer.set_state(table)
        logging.info(f"Pending...")


class PostgresScheduleManager(ScheduleManager):
    def __init__(
        self, postgres: PostgresConnectionManager, redis: RedisConnectionManager
    ):
        self.postgres = postgres
        self.redis = redis
        self.storage = RedisStorage(self.redis)
        self.state = State(self.storage)
        self.producer = PostgresProducer(self.state, self.postgres)
        self.scheduler = NotificationScheduler()
        self.cleaner = PostgresCleaner(self.postgres)


def schedule():
    """Основной метод, планирующий уведомления"""

    logging.debug("Beginning the extraction process...")
    logging.debug("Trying to establsh connection with db...")

    while True:
        try:
            postgres_manager = PostgresConnectionManager(PostgresConnector())
            redis_manager = RedisConnectionManager(RedisConnector())
            with (postgres_manager as postgres, redis_manager as redis):
                manager = PostgresScheduleManager(postgres, redis)
                manager.schedule()
                del manager

            logging.debug("Sleeping...")
            sleep(settings.wait_up_to)

        except Exception as e:
            logging.error(type(e))
            logging.error(e)
            logging.error("Exited scan...")
            return


if __name__ == "__main__":
    schedule()
