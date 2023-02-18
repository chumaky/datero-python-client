"""Foreign server user mapping management"""

from typing import Dict
import psycopg2
from psycopg2 import sql

from .. import CONNECTION
from ..connection import Connection
from .util import options_and_values

class UserMapping:
    """Foreign server user mapping management"""

    def __init__(self, config: Dict):
        self.config = config
        self.conn = Connection(self.config[CONNECTION])

    @property
    def servers(self):
        """List of foreign servers"""
        return self.config['servers'] if 'servers' in self.config else {}


    def init_user_mappings(self):
        """Create user mapping for a foreign servers"""
        try:
            cur = self.conn.cursor

            for server, props in self.servers.items():
                stmt = \
                    'CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER ' \
                    'SERVER {server} ' \
                    'OPTIONS ({options})'

                options, values = options_and_values(props['user_mapping'])

                query = sql.SQL(stmt).format(
                    server=sql.Identifier(server),
                    options=options
                )

                cur.execute(query, values)
                self.conn.commit()
                print(f'User mapping for "{server}" foreign server successfully created')
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
        finally:
            cur.close()


    def create_user_mapping(self, server: str, props: Dict):
        """Create user mapping for a foreign server"""
        try:
            cur = self.conn.cursor

            stmt = \
                'CREATE USER MAPPING FOR CURRENT_USER ' \
                'SERVER {server} ' \
                'OPTIONS ({options})'

            options, values = options_and_values(props)

            query = sql.SQL(stmt).format(
                server=sql.Identifier(server),
                options=options
            )

            cur.execute(query, values)

            print(f'User mapping for foreign server "{server}" successfully created')
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
            raise e
        finally:
            cur.close()


    def alter_user_mapping(self, server: str, props: Dict):
        """Alter user mapping for a foreign server"""
        try:
            cur = self.conn.cursor

            stmt = \
                'ALTER USER MAPPING FOR CURRENT_USER ' \
                'SERVER {server} ' \
                'OPTIONS ({options})'

            options, values = options_and_values(props, is_update=True)

            query = sql.SQL(stmt).format(
                server=sql.Identifier(server),
                options=options
            )

            cur.execute(query, values)

            print(f'User mapping for foreign server "{server}" successfully updated')
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
            raise e
        finally:
            cur.close()
