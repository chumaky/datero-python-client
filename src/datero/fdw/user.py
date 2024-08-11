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
            values = None
            stmt = \
                'CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER ' \
                'SERVER {server} ' \
                'OPTIONS ({options})'

            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    for server, props in self.servers.items():
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
        try:
            values = None
            stmt = \
                'CREATE USER MAPPING FOR CURRENT_USER ' \
                'SERVER {server} ' \
                'OPTIONS ({options})'

            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    options, values = options_and_values(props)

                    query = sql.SQL(stmt).format(
                        server=sql.Identifier(server),
                        options=options
                    )
                    stmt = query.as_string(cur)
                    cur.execute(query, values)
                    print(f'User mapping for foreign server "{server}" successfully created')

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {stmt}\nValues: {values}')
            raise e


    def alter_user_mapping(self, server: str, props: Dict):
        """Alter user mapping for a foreign server"""
        try:
            values = None
            stmt = \
                'ALTER USER MAPPING FOR CURRENT_USER ' \
                'SERVER {server} ' \
                'OPTIONS ({options})'

            cur_user_mapping_options = self.get_user_mapping_options(server)
            #print(f'Current user mapping options: {cur_user_mapping_options}')

            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    options, values = options_and_values(input_options=props, current_options=cur_user_mapping_options)
                    #print(f'New user mapping options: {options.as_string(cur)}, values: {values}')

                    query = sql.SQL(stmt).format(
                        server=sql.Identifier(server),
                        options=options
                    )
                    stmt = query.as_string(cur)
                    #print(f'Query: {stmt}')
                    cur.execute(query, values)
                    print(f'User mapping for foreign server "{server}" successfully updated')

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {stmt}\nValues: {values}')
            raise e


    def get_user_mapping_options(self, server_name: str):
        """Get user mapping options"""
        query = r"""
            SELECT umo.option_name                          AS option_name
                 , umo.option_value                         AS option_value
              FROM pg_user_mappings                         um
             CROSS JOIN pg_options_to_table(um.umoptions)   umo(option_name, option_value)
             WHERE um.srvname = %(server_name)s
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, {'server_name': server_name})
                ds = cur.fetchall()

        # transform list of tuples to dictionary
        res = {row[0]: row[1] for row in ds}
        
        return res
