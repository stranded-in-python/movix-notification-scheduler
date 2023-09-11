from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class Entry(BaseModel):
    id: UUID
    modified: datetime


class CronEntry(BaseModel):
    id: UUID
    modified: datetime
    status: str
    cron_str: str


class CrewMember(BaseModel):
    id: UUID
    full_name: str


class Genre(BaseModel):
    id: UUID
    name: str


class FilmWorkDocument(BaseModel):
    id: UUID
    title: str
    description: str | None
    rating: float
    genre: tuple[str, ...]
    actors: tuple[CrewMember, ...]
    directors: tuple[CrewMember, ...]
    writers: tuple[CrewMember, ...]
    genres: tuple[Genre, ...]


class PersonDocument(BaseModel):
    id: UUID
    full_name: str


class GenreDocument(BaseModel):
    id: UUID
    name: str
    description: str | None
