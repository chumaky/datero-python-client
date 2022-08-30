"""Foreign server management"""
import psycopg2

from .config import ConfigParser
from .connection import Connection

class FDW:
    """Foreign server management"""

    def __init__(self, config_file: str = None):
        self.config = ConfigParser(config_file).params
        self.conn = Connection()

    @property
    def servers(self):
        """List of foreign servers"""
        print(self.config)
        return self.config['servers'] if 'servers' in self.config else {}


    def init_servers(self):
        """Get list of enabled extensions"""
        try:
            cur = self.conn.cursor

            for ext, props in self.servers.items():
                print(f'{ext} - {props}')
                #schema_name = props['schema_name'] if props['schema_name'] is not None else ext
                #if props['enabled']:
                #    sql = f'CREATE SCHEMA IF NOT EXISTS {schema_name};'
                #    cur.execute(sql)
                #    sql = f'CREATE EXTENSION IF NOT EXISTS {ext} WITH SCHEMA {schema_name};'
                #    cur.execute(sql)
                #    self.conn.commit()
                #    print(f'Extension "{ext}" successfully created')
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {sql}')
        finally:
            cur.close()
