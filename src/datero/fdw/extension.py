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
        try:
            conn = self.pool.get_conn()
            cur = conn.cursor()
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

            conn.commit()

            return res

        except psycopg2.Error as e:
            conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query}')
        finally:
            cur.close()
            self.pool.put_conn(conn)


    def init_extensions(self):
        """Create FDW extensions from the config and if they are available in the system"""
        try:
            conn = self.pool.get_conn()
            cur = conn.cursor()

            for fdw_name in self.fdws:
                if any(fdw_name in fdw['name'] for fdw in self.fdw_list()):
                    sql = f'CREATE EXTENSION IF NOT EXISTS {fdw_name} WITH SCHEMA {DATERO_FDW_SCHEMA};'
                    cur.execute(sql)
                    conn.commit()
                    print(f'Extension "{fdw_name}" successfully created')
        except psycopg2.Error as e:
            conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {sql}')
        finally:
            cur.close()
            self.pool.put_conn(conn)
