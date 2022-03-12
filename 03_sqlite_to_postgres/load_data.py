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
    MODEL = 'film_work'

    __slots__ = ('id', 'title', 'description', 'creation_date', 'rating', 'type', 'created', 'modified',)

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
    MODEL = 'genre'
    __slots__ = ('id', 'name', 'description', 'created', 'modified',)

    id: uuid.UUID
    name: str
    description: str
    created: datetime.datetime
    modified: datetime.datetime


@dataclass(frozen=True)
class Person:
    MODEL = 'person'
    __slots__ = ('id', 'full_name', 'created', 'modified',)

    id: uuid.UUID
    full_name: str
    created: datetime.datetime
    modified: datetime.datetime


@dataclass(frozen=True)
class PersonFilmWork:
    MODEL = 'person_film_work'
    __slots__ = ('id', 'film_work_id', 'person_id', 'role', 'created',)

    id: uuid.UUID
    film_work_id: str
    person_id: str
    role: str
    created: datetime.datetime


@dataclass(frozen=True)
class GenreFilmWork:
    MODEL = 'genre_film_work'
    __slots__ = ('id', 'film_work_id', 'genre_id', 'created',)

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

    def load_objs(self, cls: dataclass, fields_mapping: dict = None, batch_size: int = 500):
        self.connection.row_factory = sqlite3.Row
        cur = self.connection.cursor()
        params = {
            'fields': get_fields(cls.__slots__, fields_mapping),
            'table_name': cls.MODEL,
        }
        cur.execute("SELECT {fields} FROM {table_name}".format(**params))

        while record := cur.fetchmany(batch_size):
            yield [cls(**r) for r in record]

    def load_movies(self, size: int = 500):
        return self.load_objs(Movie, batch_size=size)

    def load_genre(self, size: int = 500):
        return self.load_objs(Genre, batch_size=size)

    def load_person(self, size: int = 500):
        return self.load_objs(Person, batch_size=size)

    def load_person_film_work(self, size: int = 500):
        return self.load_objs(PersonFilmWork, batch_size=size)

    def load_genre_filmwork(self, size: int = 500):
        return self.load_objs(GenreFilmWork, batch_size=size)


@dataclass
class PostgresSaver:
    pg_conn: _connection

    def save_all_data(self, cls: dataclass, data):
        table = "content.{}".format(cls.MODEL)
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
            cursor.execute("TRUNCATE content.{}".format(cls.MODEL))
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
