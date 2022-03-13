"""Модуль загрузки данных из sqlite в PostgreSQL."""
import os
import sqlite3

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from contexts import sqlite_conn_context, pg_conn_context
from helpers import SQLiteLoader, PostgresSaver
from models import Movie, Genre, Person, PersonFilmWork, GenreFilmWork
import logging

load_dotenv(dotenv_path='../02_movies_admin/.env')
logging.basicConfig(level=logging.DEBUG)


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
    params = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST', '127.0.0.1'),
        'port': os.environ.get('DB_PORT', 5432),
        'cursor_factory': DictCursor
    }
    try:
        with sqlite_conn_context('db.sqlite') as sqlite_conn, pg_conn_context(**params) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn)
    except psycopg2.OperationalError as er:
        logging.error('psycopg2.OperationalError: %s' % (' '.join(er.args)))
    except sqlite3.OperationalError as er:
        logging.error('sqlite3.OperationalError: %s' % (' '.join(er.args)))
