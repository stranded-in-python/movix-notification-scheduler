import enum

import pydantic


class CronStatuses(enum.Enum):
    PENDING = "pending"
    STALE = "stale"


class Settings(pydantic.BaseSettings):
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: str

    redis_host: str
    redis_port: int

    tables_for_scan: list[tuple[str, int]] = [("notifications.notification_cron", 1000)]
    cron_command: str = "/send"

    wait_up_to: int = 60 * 60 * 12
    waiting_interval: int = 60 * 30
    waiting_factor: int = 2
    first_nap: float = 0.1

    sentry_dsn_api: str = ""


settings = Settings()  # type: ignore

if settings.sentry_dsn_api:
    import sentry_sdk  # type: ignore

    sentry_sdk.init(dsn=settings.sentry_dsn_api, traces_sample_rate=1.0)
