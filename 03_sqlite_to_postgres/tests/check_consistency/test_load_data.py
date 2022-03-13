from helpers import get_fields
from load_data import *
from models import Movie, Genre, Person, PersonFilmWork, GenreFilmWork

load_dotenv(dotenv_path='../../../02_movies_admin/.env')
params = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432),
}
sqlite_path = '../../db.sqlite'


def test_tables_count_equals():
    """Количество записей в таблицах должны совпадать."""

    with sqlite_conn_context(sqlite_path) as sqlite_conn, pg_conn_context(**params) as pg_conn:
        classes = (Movie, Genre, Person, GenreFilmWork, PersonFilmWork)
        sqlite_cur = sqlite_conn.cursor()
        cur = pg_conn.cursor()
        for cls in classes:
            query = 'SELECT count(*) FROM {table}'.format(table=cls.model)
            sqlite_cur.execute(query)
            sqlite_record = sqlite_cur.fetchone()

            query = 'SELECT count(*) FROM {table}'.format(table=cls.model)
            cur.execute(query)
            pg_record = cur.fetchone()
            assert sqlite_record == tuple(pg_record)


def test_tables_rows_equals():
    """Проверка содержимого записей внутри каждой таблицы. Время добавления/изменения можно опустить."""

    with sqlite_conn_context(sqlite_path) as sqlite_conn, pg_conn_context(**params) as pg_conn:
        classes = (Movie, Genre, Person, GenreFilmWork, PersonFilmWork)
        sqlite_cur = sqlite_conn.cursor()
        cur = pg_conn.cursor()
        for cls in classes:
            fields = list(cls.__slots__)
            if 'created' in fields:
                fields.remove('created')
            if 'modified' in fields:
                fields.remove('modified')
            query = 'SELECT {fields} FROM {table} ORDER BY id'.format(fields=get_fields(fields), table=cls.model)

            sqlite_cur.execute(query)
            cur.execute(query)
            while (sqlite_record := sqlite_cur.fetchmany(500)) and (pg_record := cur.fetchmany(500)):
                assert all([a == b for a, b in zip(sqlite_record, pg_record)])
