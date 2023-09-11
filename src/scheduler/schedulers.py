import abc
import typing

import crontab

from config.settings import settings
from scheduler import models


class Scheduler(abc.ABC):
    @abc.abstractmethod
    def schedule(self, entries: typing.Iterable[models.CronEntry]):
        ...

    @abc.abstractmethod
    def unschedule(self, entries: typing.Iterable[models.CronEntry]):
        ...

    @abc.abstractmethod
    def generate_command(self, entry: models.CronEntry) -> str:
        ...


class NotificationScheduler(Scheduler):
    def schedule(self, entries: typing.Iterable[models.CronEntry]):
        tab = crontab.CronTab(user=True)
        for entry in entries:
            tab.new(self.generate_command(entry), str(entry.id)).setall(entry.cron_str)
        tab.write()

    def unschedule(self, entries: typing.Iterable[models.CronEntry]):
        tab = crontab.CronTab(user=True)
        for entry in entries:
            tab.remove_all(comment=str(entry.id))

        tab.write()

    def generate_command(self, entry: models.CronEntry) -> str:
        return f"exec {settings.cron_command} {entry.id}"
