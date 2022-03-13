import sqlite3
import sys
import traceback
from copy import deepcopy
from dataclasses import dataclass, astuple
import logging
import psycopg2
from psycopg2.extensions import connection as _connection


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
        try:
            cur.execute("SELECT {fields} FROM {table_name}".format(**params))

            while records := cur.fetchmany(batch_size):
                yield [instance(**record) for record in records]
        except sqlite3.Error as er:
            logging.error('SQLite error: %s' % (' '.join(er.args)))
            logging.error("Exception class is: ", er.__class__)
            logging.error('SQLite traceback: ')
            exc_type, exc_value, exc_tb = sys.exc_info()
            logging.error(traceback.format_exception(exc_type, exc_value, exc_tb))
        finally:
            cur.close()


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

        try:
            cursor.execute(query)
        except psycopg2.Error as er:
            logging.error('PostgreSQL error: %s' % (' '.join(er.args)))
            logging.error("Exception class is: ", er.__class__)
            logging.error('PostgreSQL traceback: ')
            exc_type, exc_value, exc_tb = sys.exc_info()
            logging.error(traceback.format_exception(exc_type, exc_value, exc_tb))
        finally:
            cursor.close()

