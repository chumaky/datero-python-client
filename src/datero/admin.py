"""Administrative functions"""
from typing import Dict
import psycopg2
from psycopg2 import sql
import datetime
import os

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
            query = f"SELECT 'Connected' AS status, now() AS heartbeat"

            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()

            res = { 'status': row[0], 'heartbeat': row[1] }
            return res
        
        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query}')
            res = { 'status': 'Not connected', 'heartbeat': datetime.datetime.now() }
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


    def deploy_datero_schema(self):
        """Apply SQL scripts to deploy Datero schema"""
        try:
            print('Start deploying "datero" schema')
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    script_dir = os.path.dirname(__file__)  # Directory of the script
                    file_path = os.path.join(script_dir, 'sql', 'datero.sql')

                    # parse file content and execute each statement
                    # each statement is started with a comment line '-- stmt'
                    # and finished either with a ';' or ');' at the end of the line
                    with open(file_path, 'r') as f:
                        content = f.read()
                        stmts = content.split('-- stmt')
                        for stmt in stmts:
                            if len(stmt) > 0:
                                stmt = sql.SQL(stmt.strip())
                                #print(stmt.as_string(cur))
                                cur.execute(stmt)


            print('Schema "datero" successfully deployed')
        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {stmt}')