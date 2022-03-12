from load_data import *

dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}
sqlite = '../../db.sqlite'


def test_tables_count_equals():
    """Количество записей в таблицах должны совпадать."""

    with sqlite3.connect(sqlite) as sqlite_conn, psycopg2.connect(**dsl) as pg_conn:
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
            assert sqlite_record == pg_record


def test_tables_rows_equals():
    """Проверка содержимого записей внутри каждой таблицы. Время добавления/изменения можно опустить."""

    with sqlite3.connect(sqlite) as sqlite_conn, psycopg2.connect(**dsl) as pg_conn:
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
                assert sqlite_record == pg_record
