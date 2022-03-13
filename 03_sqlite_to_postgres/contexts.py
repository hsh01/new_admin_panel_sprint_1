from contextlib import contextmanager
import sqlite3
import psycopg2


@contextmanager
def sqlite_conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def pg_conn_context(**kwargs):
    conn = psycopg2.connect(**kwargs)
    try:
        yield conn
    finally:
        conn.close()
