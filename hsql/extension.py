"""Activate required extensions"""
import psycopg2

from .config import ConfigParser
from .connection import Connection

class Extension:
    """Extension API wrapper"""

    def __init__(self):
        self.cp = ConfigParser()
        self.config = self.cp.parse_config()
        self.conn = Connection()

    @property
    def fdw(self):
        """List of FDWs"""
        return self.config['fdw_list']


    def init_extensions(self):
        """Get list of enabled extensions"""
        try:
            cur = self.conn.cursor

            for ext, props in self.fdw.items():
                schema_name = props['schema_name'] if props['schema_name'] is not None else ext
                if props['enabled']:
                    sql = f'CREATE SCHEMA IF NOT EXISTS {schema_name};'
                    cur.execute(sql)
                    sql = f'CREATE EXTENSION IF NOT EXISTS {ext} WITH SCHEMA {schema_name};'
                    cur.execute(sql)
                    self.conn.commit()
                    print(f'Extension "{ext}" successfully created')
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {sql}')
        finally:
            cur.close()

