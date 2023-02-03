"""Foreign server management"""

from typing import Dict
import psycopg2
from psycopg2 import sql

from .. import CONNECTION
from ..connection import Connection
from .user import UserMapping
from .util import options_and_values


class Server:
    """Foreign server management"""

    def __init__(self, config: Dict):
        self.config = config
        self.conn = Connection(self.config[CONNECTION])
        self.user_mapping = UserMapping(self.config)

    @property
    def servers(self) -> Dict:
        """List of foreign servers"""
        return self.config['servers'] if 'servers' in self.config else {}


    def server_list(self):
        """Get list of foreign servers"""
        try:
            cur = self.conn.cursor
            query = """
                SELECT fs.srvname                      AS server_name
                     , fdw.fdwname                     AS fdw_name
                     , (
                         SELECT json_object_agg(fso.option_name, fso.option_value)
                           FROM pg_options_to_table(fs.srvoptions) AS fso(option_name, option_value)
                       )                               AS options
                     , (
                         SELECT json_object_agg(umo.option_name, umo.option_value)
                           FROM pg_options_to_table(um.umoptions) AS umo(option_name, option_value)
                       )                               AS user_mapping
                  FROM pg_foreign_server               fs
                 INNER JOIN
                       pg_foreign_data_wrapper         fdw
                    ON fdw.oid                         = fs.srvfdw
                  LEFT JOIN
                       pg_user_mappings                um
                    ON um.srvname                      = fs.srvname
                 ORDER BY fs.srvname
            """
            cur.execute(query)
            rows = cur.fetchall()

            res = [{
                'server_name': val[0],
                'fdw_name': val[1],
                'options': val[2],
                'user_mapping': val[3]
            } for val in rows]

        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query}')
        finally:
            cur.close()

        return res


    def get_server(self, server_name: str) -> Dict:
        """Get server details"""
        return filter(lambda x: x['server_name'] == server_name, self.server_list())


    def init_servers(self):
        """Create foreign servers defined in config if any"""
        try:
            cur = self.conn.cursor

            for server, props in self.servers.items():
                stmt = \
                    'CREATE SERVER IF NOT EXISTS {server} ' \
                    'FOREIGN DATA WRAPPER {fdw_name} ' \
                    'OPTIONS ({options})'

                options, values = options_and_values(props['foreign_server'])

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


    def create_server_by_name(self, server_name: str):
        """Create foreign server by entry name in config file"""
        return self.create_server(self.servers[server_name])


    def create_server(self, data: Dict):
        """Create foreign server"""
        try:
            cur = self.conn.cursor

            stmt = 'CREATE SERVER {server} FOREIGN DATA WRAPPER {fdw_name}'

            key = 'options'
            if key in data and len(data[key]) > 0:
                stmt += ' OPTIONS ({options})'
                options, values = options_and_values(data[key])

                query = sql.SQL(stmt).format(
                    server=sql.Identifier(data['server_name']),
                    fdw_name=sql.Identifier(data['fdw_name']),
                    options=options
                )
                cur.execute(query, values)
            else:
                query = sql.SQL(stmt).format(
                    server=sql.Identifier(data['server_name']),
                    fdw_name=sql.Identifier(data['fdw_name'])
                )
                cur.execute(query)

            self.conn.commit()

            key = 'user_mapping'
            if key in data and len(data[key]) > 0:
                self.user_mapping.create_user_mapping_by_data(
                    data['server_name'],
                    data['user_mapping']
                )

            # TODO: create default schema for the foreign server
            # within this schema create foreign table to fetch the list of schemas that could be imported
            # this functionality should be triggered only for FDWs which support foreign schema import
            #
            # SQL (MYSQL)
            # create foreign table schema_list (schema_name text) server mysql options (dbname 'information_schema', table_name 'schemata');
            # select * from schema_list where schema_name not in ('information_schema', 'performance_schema');
            #
            # create foreign table table_list (table_schema text, table_name text, table_type text) server mysql options (dbname 'information_schema', table_name 'tables');
            # select * from table_list where table_schema not in ('information_schema', 'performance_schema') and table_type in ('BASE TABLE', 'VIEW');
            #
            # postgres=# \d public.*
            #                 Foreign table "public.schema_list"
            #    Column    | Type | Collation | Nullable | Default | FDW options
            # -------------+------+-----------+----------+---------+-------------
            #  schema_name | text |           |          |         |
            # Server: mysql
            # FDW options: (dbname 'information_schema', table_name 'schemata')
            #
            #                  Foreign table "public.table_list"
            #     Column    | Type | Collation | Nullable | Default | FDW options
            # --------------+------+-----------+----------+---------+-------------
            #  table_schema | text |           |          |         |
            #  table_name   | text |           |          |         |
            #  table_type   | text |           |          |         |
            # Server: mysql
            # FDW options: (dbname 'information_schema', table_name 'tables')


            msg = f'Foreign server "{data["server_name"]}" successfully created'
            print(msg)
            return {
                'status_code': 200,
                'message': msg
            }
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
            raise e
        finally:
            cur.close()


    def update_server(self, data: Dict):
        """Update foreign server"""

        return self.rename_server(data)

#       try:
#           cur = self.conn.cursor
#
#           stmt = 'CREATE SERVER {server} FOREIGN DATA WRAPPER {fdw_name}'
#
#           key = 'options'
#           if key in data and len(data[key]) > 0:
#               stmt += ' OPTIONS ({options})'
#               options, values = options_and_values(data[key])
#
#               query = sql.SQL(stmt).format(
#                   server=sql.Identifier(data['server_name']),
#                   fdw_name=sql.Identifier(data['fdw_name']),
#                   options=options
#               )
#               cur.execute(query, values)
#           else:
#               query = sql.SQL(stmt).format(
#                   server=sql.Identifier(data['server_name']),
#                   fdw_name=sql.Identifier(data['fdw_name'])
#               )
#               cur.execute(query)
#
#           self.conn.commit()
#
#           key = 'user_mapping'
#           if key in data and len(data[key]) > 0:
#               self.user_mapping.create_user_mapping_by_data(
#                   data['server_name'],
#                   data['user_mapping']
#               )
#
#
#            msg = f'Foreign server "{data["server_name"]}" successfully updated'
#            print(msg)
#            return {
#                'status_code': 200,
#                'message': msg
#            }
#        except psycopg2.Error as e:
#            self.conn.rollback()
#            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
#            raise e
#        finally:
#           cur.close()


    def rename_server(self, data: Dict):
        """Rename foreign server"""
        try:
            if data['server_name'] != data['old_name']:
                cur = self.conn.cursor

                stmt = 'ALTER SERVER {old_name} RENAME TO {new_name}'

                query = sql.SQL(stmt).format(
                    old_name=sql.Identifier(data['old_name']),
                    new_name=sql.Identifier(data['server_name'])
                )
                cur.execute(query)

                self.conn.commit()

                msg = f'Foreign server "{data["server_name"]}" successfully updated'
            else:
                msg = 'Server name is the same. Nothing to do'

            print(msg)
            return {
                'status_code': 200,
                'message': msg
            }
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f'Error code: {e.pgcode}, Message: {e.pgerror}' f'SQL: {query.as_string(cur)}')
            raise e
        finally:
            cur.close()
