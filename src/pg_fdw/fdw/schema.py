"""Importing schema from foreign server"""

from typing import Dict
import psycopg2
from psycopg2 import sql

from .. import CONNECTION
from ..connection import Connection
from .util import options_and_values

class Schema:
    """Importing schema from foreign server"""

    def __init__(self, config: Dict):
        self.config = config
        self.conn = Connection(self.config[CONNECTION])

    @property
    def servers(self):
        """List of foreign servers"""
        return self.config['servers'] if 'servers' in self.config else {}


    def import_foreign_schema(self):
        """Import foreign schema"""

        def recreate_schema():
            query = sql.SQL('DROP SCHEMA IF EXISTS {local_schema} CASCADE') \
                .format(local_schema=sql.Identifier(local_schema))

            cur.execute(query)

            query = sql.SQL('CREATE SCHEMA IF NOT EXISTS {local_schema}') \
                .format(local_schema=sql.Identifier(local_schema))

            cur.execute(query)

        try:
            cur = self.conn.cursor

            for server, props in self.servers.items():
                ##print(f'{server} - {props}')
                conf = props['import_foreign_schema']
                remote_schema = conf['remote_schema']
                local_schema = conf['local_schema']

                recreate_schema()

                stmt = \
                    'IMPORT FOREIGN SCHEMA {remote_schema} ' \
                    'FROM SERVER {server} ' \
                    'INTO {local_schema} '

                # IMPORT FOREIGN SCHEMA doesn't accept an empty OPTIONS () clause
                # hence we need to process separately cases with and without any options specified
                if 'options' not in conf:
                    query = sql.SQL(stmt).format(
                        remote_schema=sql.Identifier(remote_schema),
                        server=sql.Identifier(server),
                        local_schema=sql.Identifier(local_schema),
                    )
                    cur.execute(query)
                else:
                    stmt += 'OPTIONS({options})'

                    options, values = options_and_values(conf['options'])

                    query = sql.SQL(stmt).format(
                        remote_schema=sql.Identifier(remote_schema),
                        server=sql.Identifier(server),
                        local_schema=sql.Identifier(local_schema),
                        options=options
                    )
                    cur.execute(query, values)

                self.conn.commit()
                print(f'Foreign schema "{remote_schema}" from server "{server}" successfully imported into "{local_schema}"')
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
        finally:
            cur.close()
