"""Administrative functions"""
from typing import Dict
import psycopg2

from . import CONNECTION
from .connection import Connection

class Admin:
    """Administrative functions"""

    def __init__(self, config: Dict):
        self.config = config


    def healthcheck(self):
        """Check database availability"""
        cur = None
        try:
            query = "SELECT 'Connected' AS status, '1.0.0' AS version, now() AS heartbeat"
            conn = Connection(self.config[CONNECTION])
            cur = conn.cursor
            cur.execute(query)
            row = cur.fetchone()
            res = { 'status': row[0], 'version': row[1], 'heartbeat': row[2] }
        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query}')
            raise e
        finally:
            if cur is not None:
                cur.close()

        return res
