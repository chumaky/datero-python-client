"""Foreign server user mapping management"""

from typing import Dict
import psycopg2
from psycopg2 import sql

from .. import CONNECTION
from ..connection import ConnectionPool
from .util import options_and_values

class UserMapping:
    """Foreign server user mapping management"""

    def __init__(self, config: Dict):
        self.config = config
        self.pool = ConnectionPool(self.config[CONNECTION])

    @property
    def servers(self):
        """List of foreign servers"""
        return self.config['servers'] if 'servers' in self.config else {}


    def init_user_mappings(self):
        """Create user mapping for a foreign servers"""
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:

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
                        stmt = query.as_string(cur)
                        cur.execute(query, values)
                        print(f'User mapping for "{server}" foreign server successfully created')

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {stmt}\nValues: {values}')
            raise e


    def create_user_mapping(self, server: str, props: Dict):
        """Create user mapping for a foreign server"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
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


    def alter_user_mapping(self, server: str, props: Dict):
        """Alter user mapping for a foreign server"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
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
