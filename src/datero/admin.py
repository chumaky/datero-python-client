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
            version = '1.1.0'
            query = f"SELECT 'Connected' AS status, '{version}' AS version, now() AS heartbeat"

            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()

            res = { 'status': row[0], 'version': row[1], 'heartbeat': row[2] }
            return res
        
        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query}')
            res = { 'status': 'Not connected', 'version': version, 'heartbeat': datetime.datetime.now() }
            return res
            #raise e


    def create_system_schema(self, schema_name: str):
        """Create system schema"""
        try:
            stmt = None
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    query = sql.SQL('CREATE SCHEMA IF NOT EXISTS {datero_schema}') \
                        .format(datero_schema=sql.Identifier(schema_name))
                    stmt = query.as_string(cur)
                    cur.execute(query)

            print(f'System schema "{schema_name}" successfully created')
        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {stmt}')
