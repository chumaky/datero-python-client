"""Foreign server management"""

from typing import Dict
import psycopg2
from psycopg2 import sql
from copy import deepcopy

from .. import CONNECTION
from ..connection import ConnectionPool
from ..adapter import Adapter
from .user import UserMapping
from .util import options_and_values
from .. import DATERO_SCHEMA


class Server:
    """Foreign server management"""

    def __init__(self, config: Dict):
        self.config = config
        self.pool = ConnectionPool(self.config[CONNECTION])
        self.user_mapping = UserMapping(self.config)

    @property
    def servers(self) -> Dict:
        """List of foreign servers"""
        return self.config['servers'] if 'servers' in self.config else {}


    def server_list(self):
        """Get list of foreign servers"""
        query = """
            SELECT fs.srvname                      AS server_name
                 , fdw.fdwname                     AS fdw_name
                 , d.description                   AS description
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
              LEFT JOIN
                   pg_description                  d
                ON d.classoid                      = fs.tableoid
               AND d.objoid                        = fs.oid
               AND d.objsubid                      = 0
             ORDER BY d.description
        """

        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()

            res = [{
                'server_name': val[0],
                'fdw_name': val[1],
                'description': val[2],
                'options': val[3],
                'user_mapping': val[4]
            } for val in rows]

            return res

        except psycopg2.Error as e:
            print(f'server_list: Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query}')
            raise e


    def get_server(self, server_name: str) -> Dict:
        """Get server details"""
        result = list(filter(lambda x: x['server_name'] == server_name, self.server_list()))
        return result[0] if len(result) > 0 else None


    def init_servers(self):
        """Create foreign servers defined in config if any"""
        if self.servers:
            print('Creating foreign servers from config.yaml:', len(self.servers))
            for server_name, props in self.servers.items():
                server = self.get_server(server_name)

                if server is not None:
                    print(f'Server "{server_name}" already exists. Skipping...')
                else:
                    server = deepcopy(props)
                    server['server_name'] = server_name
                    #print(f'Input: {server}')

                    self.create_server(props)
        else:
            print('No foreign servers defined in config.yaml. Nothing to create.')


    def create_server_by_name(self, server_name: str):
        """Create foreign server by entry name in config file"""
        return self.create_server(self.servers[server_name])


    def create_server(self, data: Dict):
        """Create foreign server"""
        try:
            server_name = self.gen_server_name(data)
            stmt = 'CREATE SERVER {server} FOREIGN DATA WRAPPER {fdw_name}'
            values = None

            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    key = 'options'
                    if key in data and len(data[key]) > 0:
                        stmt += ' OPTIONS ({options})'
                        options, values = options_and_values(data[key])

                        query = sql.SQL(stmt).format(
                            server=sql.Identifier(server_name),
                            fdw_name=sql.Identifier(data['fdw_name']),
                            options=options
                        )
                    else:
                        query = sql.SQL(stmt).format(
                            server=sql.Identifier(server_name),
                            fdw_name=sql.Identifier(data['fdw_name'])
                        )

                    stmt = query.as_string(cur)
                    cur.execute(query, values)

            key = 'user_mapping'
            if key in data and len(data[key]) > 0:
                self.user_mapping.create_user_mapping(
                    server_name,
                    data['user_mapping']
                )

            self.set_description(server_name, data['description'])
            self.create_sys_views(server_name, data['fdw_name'])

            print(f'Foreign server "{server_name}" successfully created')

            return self.get_server(server_name)

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {stmt}\nValues: {values}')
            raise e


    def update_server(self, data: Dict):
        """Update foreign server"""
        try:
            self.set_description(data['server_name'], data['description'])
            stmt = None
            values = None

            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    key = 'options'
                    if key in data and len(data[key]) > 0:
                        stmt = 'ALTER SERVER {server} OPTIONS ({options})'
                        options, values = options_and_values(data[key], is_update=True)

                        query = sql.SQL(stmt).format(
                            server=sql.Identifier(data['server_name']),
                            fdw_name=sql.Identifier(data['fdw_name']),
                            options=options
                        )
                        stmt = query.as_string(cur)
                        cur.execute(query, values)

            key = 'user_mapping'
            if key in data and len(data[key]) > 0:
                self.user_mapping.alter_user_mapping(
                    data['server_name'],
                    data['user_mapping']
                )

            print(f'Foreign server "{data["server_name"]}" successfully updated')

            return self.get_server(data["server_name"])

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {stmt}\nValues: {values}')
            raise e


    def delete_server(self, data: Dict):
        """Delete foreign server"""
        try:
            stmt = None

            with self.pool.connection() as conn:
                with conn.cursor() as cur:

                    stmt = 'DROP SERVER {server} CASCADE'
                    query = sql.SQL(stmt).format(
                        server=sql.Identifier(data["server_name"]),
                    )
                    cur.execute(query)

                    stmt = 'DROP SCHEMA {schema} CASCADE'
                    for schema in self.get_imported_schemas(data["server_name"]):
                        query = sql.SQL(stmt).format(
                            schema=sql.Identifier(schema)
                        )
                        cur.execute(query)
                        conn.commit()   # explicitly commit every schema deletion
                        print(f'Schema "{schema}" successfully deleted')

            msg = f'Server "{data["description"]}" successfully deleted'
            print(msg)

            return { 'message': msg }

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {stmt}')
            raise e


    def gen_server_name(self, data: Dict):
        """Generate server name"""
        query = r"""
            SELECT COALESCE
                (MAX(CASE
                        WHEN fs.srvname LIKE fdw.fdwname || '\_%%'
                        THEN REPLACE(fs.srvname, fdw.fdwname || '_', '')::INT
                        ELSE 0
                    END)
                , 0) + 1                        AS next_server_id
            FROM pg_foreign_server               fs
            INNER JOIN
                pg_foreign_data_wrapper         fdw
                ON fdw.oid                         = fs.srvfdw
            WHERE fdw.fdwname = %(fdw_name)s
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, {'fdw_name': data['fdw_name']})
                row = cur.fetchone()

        server_name = data['fdw_name'] + '_' + str(row[0])
        print(f'New server name: {server_name}')

        return server_name


    def set_description(self, server_name: str, description: str):
        """Update user-defined name"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                stmt = 'COMMENT ON SERVER {server} IS %s'
                query = sql.SQL(stmt).format(
                    server=sql.Identifier(server_name)
                )
                cur.execute(query, (description,))
                print(f'Description for "{server_name}" server successfully updated')


    def create_sys_views(self, server_name: str, fdw_name: str):
        """Supplemental views to support schema/table import operations."""
        adapter = Adapter(fdw_name)
        self.create_foreign_table(adapter.schema_list_table(), server_name, f'{server_name}_schema_list')
        self.create_foreign_table(adapter.table_list_table(), server_name, f'{server_name}_table_list')


    def create_foreign_table(self, stmt: str, server_name: str, table_name: str):
        """Create helper dictionary foreign table in a public schema"""
        if stmt is not None:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    query = sql.SQL(stmt).format(
                        full_table_name=sql.Identifier(DATERO_SCHEMA, table_name),
                        server=sql.Identifier(server_name)
                    )
                    cur.execute(query)
                    print(f'"{table_name}" system table for "{server_name}" server successfully created')


    # get list of imported schemas
    def get_imported_schemas(self, server_name: str):
        """Get list of imported schemas by specified server"""
        try:
            query = r"""
                SELECT nsp.nspname      AS schema_name
                  FROM pg_namespace     nsp
                 INNER JOIN
                       pg_description   dsc
                    ON dsc.objoid       = nsp.oid
                 WHERE dsc.description  LIKE %(comment)s
            """
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, {'comment': f'{server_name}#{DATERO_SCHEMA}#%'})
                    res = [row[0] for row in cur.fetchall()]

            return res

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query} : {server_name}#{DATERO_SCHEMA}#%')
            raise e
