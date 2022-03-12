"""Модуль загрузки данных из sqlite в PostgreSQL."""

import datetime
import sqlite3
import uuid
from copy import deepcopy
from dataclasses import dataclass, astuple

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


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


def get_fields(fields, fields_mapping: dict = None):
    ret = list(deepcopy(fields))
    if fields_mapping:
        for pos, item in enumerate(ret):
            if item in fields_mapping.keys():
                ret[pos] = '{old} as "{new}"'.format(old=fields_mapping[item], new=item)
    return ', '.join(ret)


@dataclass
class SQLiteLoader:
    connection: sqlite3.Connection

    def load_objs(self, instance: dataclass, fields_mapping: dict = None, batch_size: int = 500):
        self.connection.row_factory = sqlite3.Row
        cur = self.connection.cursor()
        params = {
            'fields': get_fields(instance.__slots__, fields_mapping),
            'table_name': instance.model,
        }
        cur.execute("SELECT {fields} FROM {table_name}".format(**params))

        while records := cur.fetchmany(batch_size):
            yield [instance(**record) for record in records]

    def load_movies(self, size: int = 500):
        """Wrapper for Movie.
        Args:
            size: batch size
        Returns:
             objs generator sizeof size
        """
        return self.load_objs(Movie, batch_size=size)

    def load_genres(self, size: int = 500):
        """Wrapper for Genre.
        Args:
            size: batch size
        Returns:
             objs generator sizeof size
        """
        return self.load_objs(Genre, batch_size=size)

    def load_persons(self, size: int = 500):
        """Wrapper for Person.
        Args:
            size: batch size
        Returns:
             objs generator sizeof size
        """
        return self.load_objs(Person, batch_size=size)

    def load_person_film_works(self, size: int = 500):
        """Wrapper for PersonFilmWork.
        Args:
            size: batch size
        Returns:
             objs generator sizeof size
        """
        return self.load_objs(PersonFilmWork, batch_size=size)

    def load_genre_filmworks(self, size: int = 500):
        """Wrapper for GenreFilmWork.
        Args:
            size: batch size
        Returns:
             objs generator sizeof size
        """
        return self.load_objs(GenreFilmWork, batch_size=size)


@dataclass
class PostgresSaver:
    pg_conn: _connection

    def save_all_data(self, cls: dataclass, data):
        table = "content.{}".format(cls.model)
        cursor = self.pg_conn.cursor()

        fields = ', '.join(cls.__slots__)
        args = ', '.join(cursor.mogrify('(%s)' % ', '.join('%s' for _ in cls.__slots__),
                                        astuple(item)).decode() for item in data)
        query = f"""
        INSERT INTO {table} ({fields})
        VALUES {args}
        ON CONFLICT (id) DO NOTHING
        """
        cursor.execute(query)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres."""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)
    batch_size = 500
    classes = (Movie, Genre, Person, GenreFilmWork, PersonFilmWork)
    for cls in classes:
        with pg_conn.cursor() as cursor:
            cursor.execute("TRUNCATE content.{}".format(cls.model))
        objs = sqlite_loader.load_objs(cls, {'created': 'created_at', 'modified': 'updated_at'}, batch_size)
        for data in objs:
            postgres_saver.save_all_data(cls, data)


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        try:
            load_from_sqlite(sqlite_conn, pg_conn)
        except Exception as err:
            print(err)
            pg_conn.rollback()
            sqlite_conn.rollback()
