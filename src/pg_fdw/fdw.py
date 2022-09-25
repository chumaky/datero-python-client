"""Foreign server management"""
from typing import Dict, Tuple
import psycopg2
from psycopg2 import sql

from .connection import Connection

class FDW:
    """Foreign server management"""

    def __init__(self, config: Dict):
        self.config = config
        self.conn = Connection(self.config['postgres'])

    @property
    def servers(self):
        """List of foreign servers"""
        return self.config['servers'] if 'servers' in self.config else {}


    def fdw_list(self):
        """Get list of available FDWs"""
        try:
            cur = self.conn.cursor
            query = "SELECT name, comment FROM pg_available_extensions WHERE name LIKE '%fdw%' ORDER BY name"
            cur.execute(query)
            rows = cur.fetchall()
            res = [{ 'name': val[0], 'description': val[1] } for val in rows]
            return res
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
        finally:
            cur.close()

        #return [key for key in self.config['fdw_list'].keys()]

    def server_list(self):
        """Get list of foreign servers"""
        try:
            cur = self.conn.cursor
            query = """
                SELECT s.srvname                AS server_name
                     , f.fdwname                AS fdw_name
                  FROM pg_foreign_server        s
                 INNER JOIN
                       pg_foreign_data_wrapper  f
                    ON f.oid = s.srvfdw
                 ORDER BY s.srvname
            """
            cur.execute(query)
            rows = cur.fetchall()
            res = [{ 'server_name': val[0], 'fdw_name': val[1] } for val in rows]
            return res
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
        finally:
            cur.close()


    def init_servers(self):
        """Get list of enabled extensions"""
        try:
            cur = self.conn.cursor

            for server, props in self.servers.items():
                stmt = \
                    'CREATE SERVER IF NOT EXISTS {server} ' \
                    'FOREIGN DATA WRAPPER {fdw_name} ' \
                    'OPTIONS ({options})'

                options, values = self._options_and_values(props['options'])

                query = sql.SQL(stmt).format(
                    server=sql.Identifier(server),
                    fdw_name=sql.Identifier(props['fdw_name']),
                    options=options
                )

                cur.execute(query, values)
                self.conn.commit()
                print(f'Foreign server "{server}" successfully created')
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
        finally:
            cur.close()


    def create_user_mappings(self):
        """Create user mapping for a foreign servers"""
        try:
            cur = self.conn.cursor

            for server, props in self.servers.items():
                stmt = \
                    'CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER ' \
                    'SERVER {server} ' \
                    'OPTIONS ({options})'

                options, values = self._options_and_values(props['user_mapping'])

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

                    options, values = self._options_and_values(conf['options'])

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


    def _options_and_values(self, options: Dict) -> Tuple[sql.SQL, Dict]:
        """Internal method to prepare list of key-value options in a safe bind variables manner"""
        keys = sql.SQL(', ').join([
            sql.SQL(' ').join([sql.SQL(option), sql.Placeholder(option)])
                for option in options.keys()
        ])
        values = {}
        for option, value in options.items():
            values[option] = str(value)

        return (keys, values)
