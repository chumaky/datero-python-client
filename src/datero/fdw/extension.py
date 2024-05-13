"""Activate required extensions"""
from typing import Dict
import psycopg2

from .. import CONNECTION, DATERO_FDW_SCHEMA
from ..connection import ConnectionPool

class Extension:
    """Extension API wrapper"""

    def __init__(self, config: Dict):
        self.config = config
        self.pool = ConnectionPool(self.config[CONNECTION])

    @property
    def fdws(self) -> Dict:
        """List of FDWs"""
        return self.config['fdw_list']


    def fdw_list(self):
        """Get list of available FDWs"""
        query = """
            SELECT e.name                       AS name
                 , e.comment                    AS comment
              FROM pg_available_extensions      e
             WHERE e.name                       LIKE '%fdw%'
             ORDER BY e.name
        """
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()

            res = [{ 'name': val[0], 'description': val[1] } for val in rows]
            return res

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query}')
            raise e


    def init_extensions(self):
        """Create FDW extensions from the config and if they are available in the system"""
        sql = None
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    for fdw_name in self.fdws:
                        if any(fdw_name in fdw['name'] for fdw in self.fdw_list()):
                            sql = f'CREATE EXTENSION IF NOT EXISTS {fdw_name} WITH SCHEMA {DATERO_FDW_SCHEMA};'
                            cur.execute(sql)
                            conn.commit()   # explicitly commit every extension creation
                            print(f'Extension "{fdw_name}" successfully created')
        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {sql}')
            raise e
