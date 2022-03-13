import datetime
import uuid
from dataclasses import dataclass


@dataclass(frozen=True)
class Movie:
    model = 'film_work'

    __slots__ = ('id', 'title', 'description', 'creation_date', 'rating', 'type', 'created', 'modified')

    id: uuid.UUID
    title: str
    description: str
    creation_date: datetime.date
    rating: float
    type: str
    created: datetime.datetime
    modified: datetime.datetime


@dataclass(frozen=True)
class Genre:
    model = 'genre'
    __slots__ = ('id', 'name', 'description', 'created', 'modified')

    id: uuid.UUID
    name: str
    description: str
    created: datetime.datetime
    modified: datetime.datetime


@dataclass(frozen=True)
class Person:
    model = 'person'
    __slots__ = ('id', 'full_name', 'created', 'modified')

    id: uuid.UUID
    full_name: str
    created: datetime.datetime
    modified: datetime.datetime


@dataclass(frozen=True)
class PersonFilmWork:
    model = 'person_film_work'
    __slots__ = ('id', 'film_work_id', 'person_id', 'role', 'created')

    id: uuid.UUID
    film_work_id: str
    person_id: str
    role: str
    created: datetime.datetime


@dataclass(frozen=True)
class GenreFilmWork:
    model = 'genre_film_work'
    __slots__ = ('id', 'film_work_id', 'genre_id', 'created')

    id: uuid.UUID
    film_work_id: str
    genre_id: str
    created: datetime.datetime