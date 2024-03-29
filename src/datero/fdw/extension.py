"""Activate required extensions"""
from typing import Dict
import psycopg2

from .. import CONNECTION, DATERO_FDW_SCHEMA
from ..connection import Connection

class Extension:
    """Extension API wrapper"""

    def __init__(self, config: Dict):
        self.config = config
        self.conn = Connection(self.config[CONNECTION])

    @property
    def fdws(self) -> Dict:
        """List of FDWs"""
        return self.config['fdw_list']


    def fdw_list(self):
        """Get list of available FDWs"""
        try:
            cur = self.conn.cursor
            query = """
                SELECT e.name                       AS name
                     , e.comment                    AS comment
                  FROM pg_available_extensions      e
                 WHERE e.name                       LIKE '%fdw%'
                 ORDER BY e.name
            """
            cur.execute(query)
            rows = cur.fetchall()
            res = [{ 'name': val[0], 'description': val[1] } for val in rows]

            self.conn.commit()

            return res

        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query}')
        finally:
            cur.close()


    def init_extensions(self):
        """Get list of enabled extensions"""
        try:
            cur = self.conn.cursor

            for ext, props in self.fdws.items():
                if props['enabled']:
                    sql = f'CREATE EXTENSION IF NOT EXISTS {ext} WITH SCHEMA {DATERO_FDW_SCHEMA};'
                    cur.execute(sql)
                    self.conn.commit()
                    print(f'Extension "{ext}" successfully created')
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {sql}')
        finally:
            cur.close()
