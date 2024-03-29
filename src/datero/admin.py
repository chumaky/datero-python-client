"""Administrative functions"""
from typing import Dict
import psycopg2
from psycopg2 import sql

from . import CONNECTION
from .connection import Connection

class Admin:
    """Administrative functions"""

    def __init__(self, config: Dict):
        self.config = config
        self.conn = Connection(self.config[CONNECTION])


    def healthcheck(self):
        """Check database availability"""
        try:
            cur = self.conn.cursor

            query = "SELECT 'Connected' AS status, '1.1.0' AS version, now() AS heartbeat"
            cur.execute(query)

            row = cur.fetchone()
            res = { 'status': row[0], 'version': row[1], 'heartbeat': row[2] }

            return res
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query}')
            raise e
        finally:
            if cur is not None:
                cur.close()


    def create_system_schema(self, schema_name: str):
        """Create system schema"""
        cur = self.conn.cursor
        try:
            query = sql.SQL('CREATE SCHEMA IF NOT EXISTS {datero_schema}') \
                .format(datero_schema=sql.Identifier(schema_name))

            cur.execute(query)

            self.conn.commit()
            print(f'System schema "{schema_name}" successfully created')
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
        finally:
            cur.close()
