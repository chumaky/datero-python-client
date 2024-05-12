"""Administrative functions"""
from typing import Dict
import psycopg2
from psycopg2 import sql
import datetime

from . import CONNECTION
from .connection import ConnectionPool

class Admin:
    """Administrative functions"""

    def __init__(self, config: Dict):
        self.config = config
        self.pool = ConnectionPool(self.config[CONNECTION])


    def healthcheck(self):
        """Check database availability"""
        try:
            conn = self.pool.get_conn()
            cur = conn.cursor()

            query = "SELECT 'Connected' AS status, '1.1.0' AS version, now() AS heartbeat"
            cur.execute(query)

            row = cur.fetchone()
            res = { 'status': row[0], 'version': row[1], 'heartbeat': row[2] }

            return res
        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}, SQL: {query}')
            res = { 'status': 'Not connected', 'version': '1.1.0', 'heartbeat': datetime.datetime.now() }
            return res
            #raise e
        finally:
            if cur is not None:
                cur.close()
            self.pool.put_conn(conn)


    def create_system_schema(self, schema_name: str):
        """Create system schema"""
        conn = self.pool.get_conn()
        cur = conn.cursor()
        try:
            query = sql.SQL('CREATE SCHEMA IF NOT EXISTS {datero_schema}') \
                .format(datero_schema=sql.Identifier(schema_name))

            cur.execute(query)

            conn.commit()
            print(f'System schema "{schema_name}" successfully created')
        except psycopg2.Error as e:
            conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
        finally:
            cur.close()
            self.pool.put_conn(conn)
