"""Importing schema from foreign server"""

from typing import Dict
import psycopg2
from psycopg2 import sql

from .. import CONNECTION
from ..adapter import Adapter
from ..connection import ConnectionPool
from .util import options_and_values, FdwType
from .. import DATERO_SCHEMA

class Schema:
    """Importing schema from foreign server"""

    def __init__(self, config: Dict):
        self.config = config
        self.pool = ConnectionPool(self.config[CONNECTION])

    @property
    def servers(self):
        """List of foreign servers"""
        return self.config['servers'] if 'servers' in self.config else {}


    def init_foreign_schemas(self):
        """Init foreign schemas"""

        def recreate_schema():
            query = sql.SQL('DROP SCHEMA IF EXISTS {local_schema} CASCADE') \
                .format(local_schema=sql.Identifier(local_schema))

            cur.execute(query)

            query = sql.SQL('CREATE SCHEMA IF NOT EXISTS {local_schema}') \
                .format(local_schema=sql.Identifier(local_schema))

            cur.execute(query)

        try:
            stmt = None
            values = None            

            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    for server, props in self.servers.items():
                        ##print(f'{server} - {props}')
                        conf = props['import_foreign_schema']
                        remote_schema = conf['remote_schema']
                        local_schema = conf['local_schema']

                        recreate_schema()

                        stmt = \
                            'IMPORT FOREIGN SCHEMA {remote_schema} ' \
                            'FROM SERVER {server} ' \
                            'INTO {local_schema}'

                        values = None
                        key = 'options'
                        if key in conf and len(conf[key]) > 0:
                            stmt += ' OPTIONS({options})'
                            options, values = options_and_values(conf[key])

                            query = sql.SQL(stmt).format(
                                remote_schema=sql.Identifier(remote_schema),
                                server=sql.Identifier(server),
                                local_schema=sql.Identifier(local_schema),
                                options=options
                            )
                        else:
                            query = sql.SQL(stmt).format(
                                remote_schema=sql.Identifier(remote_schema),
                                server=sql.Identifier(server),
                                local_schema=sql.Identifier(local_schema),
                            )

                        stmt = query.as_string(cur)
                        cur.execute(query, values)
                        conn.commit()   # explicitly commit every schema import

                        print(f'Foreign schema "{remote_schema}" from server "{server}" successfully imported into "{local_schema}"')
        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {stmt}\nValues: {values}')
            raise e


    def get_foreign_schema_list(self, server_name: str, fdw_name: str):
        """Get list of available schemas to import."""
        table_name = f'{server_name}_schema_list'

        adapter = Adapter(fdw_name)
        stmt = adapter.schema_list()

        res = []
        try:
            if stmt is not None:
                with self.pool.connection() as conn:
                    with conn.cursor() as cur:
                        query = sql.SQL(stmt).format(
                            full_table_name=sql.Identifier(DATERO_SCHEMA, table_name),
                        )
                        stmt = query.as_string(cur)
                        cur.execute(query)
                        rows = cur.fetchall()

                res = [val[0] for val in rows]

            elif fdw_name == FdwType.SQLITE.value or fdw_name == FdwType.DUCKDB.value:
                res = ['public']

            if len(res) > 0:
                print(f'Foreign server "{server_name}" schemas count: {len(res)}')
            else:
                print(f'Foreign server "{server_name}" doesn''t support schemas import')

            return res

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {stmt}')
            raise e


    def import_foreign_schema(self, data: Dict):
        """Import foreign schema"""

        def recreate_schema():
            """Recreate schema"""
            query = sql.SQL('DROP SCHEMA IF EXISTS {local_schema} CASCADE') \
                .format(local_schema=sql.Identifier(local_schema))

            cur.execute(query)

            query = sql.SQL('CREATE SCHEMA IF NOT EXISTS {local_schema}') \
                .format(local_schema=sql.Identifier(local_schema))

            cur.execute(query)

        def set_description():
            """Set description for schema"""
            query = sql.SQL('COMMENT ON SCHEMA {schema} IS %s') \
                .format(schema=sql.Identifier(local_schema)
                                    )
            cur.execute(query, (f'Imported from (foreign_server.schema): {server_name}.{remote_schema}',))

        try:
            server_name = data['server_name']
            remote_schema = data['remote_schema']
            local_schema = data['local_schema']

            import_options = data['options'] if 'options' in data else None

            values = None
            stmt = \
                'IMPORT FOREIGN SCHEMA {remote_schema} ' \
                'FROM SERVER {server} ' \
                'INTO {local_schema}'

            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    recreate_schema()
                    set_description()

                    if import_options is not None and len(import_options) > 0:
                        stmt += ' OPTIONS({options})'
                        options, values = options_and_values(import_options)

                        query = sql.SQL(stmt).format(
                            remote_schema=sql.Identifier(remote_schema),
                            server=sql.Identifier(server_name),
                            local_schema=sql.Identifier(local_schema),
                            options=options
                        )
                    else:
                        query = sql.SQL(stmt).format(
                            remote_schema=sql.Identifier(remote_schema),
                            server=sql.Identifier(server_name),
                            local_schema=sql.Identifier(local_schema),
                        )

                    stmt = query.as_string(cur)
                    cur.execute(query, values)
                    print(f'Foreign schema "{remote_schema}" from server "{server_name}" successfully imported into "{local_schema}"')

            return data

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {stmt}\nValues: {values}')
            raise e


    def get_local_schema_list(self):
        """Get list of local schemas with set of categorization flags"""
        try:
            query = r"""
                SELECT n.nspname            AS schema_name
                  FROM pg_namespace         n
                 WHERE n.nspname            NOT IN ( 'pg_catalog'
                                                   , 'pg_toast'
                                                   , 'information_schema'
                                                   , %(datero)s
                                                   )
                   AND n.nspname            NOT LIKE %(datero)s || '\_%%'
                   AND NOT EXISTS
                     (
                       SELECT 1
                         FROM pg_extension      e
                        WHERE e.extname         LIKE '%%\_fdw'
                          AND e.extnamespace    = n.oid
                     )
                 ORDER BY n.nspname
            """
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, {'datero': DATERO_SCHEMA})
                    rows = cur.fetchall()

            res = [val[0] for val in rows]
            return res

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query}')
            raise e


    def get_local_schema_objects(self, schema_name: str):
        """Get list of local schema objects"""
        try:
            query = r"""
                SELECT c.relname            AS object_name
                     , c.relkind            AS object_type
                  FROM pg_class             c
                 INNER JOIN
                       pg_namespace         n
                    ON n.oid                = c.relnamespace
                 WHERE n.nspname            = %(schema_name)s
                   AND c.relkind            IN ('f', 'r', 'p', 'v', 'm')
                   AND n.nspname            NOT IN ( 'pg_catalog'
                                                   , 'pg_toast'
                                                   , 'information_schema'
                                                   , %(datero)s
                                                   )
                   AND NOT EXISTS
                     (
                       SELECT 1
                         FROM pg_extension      e
                        WHERE e.extname         LIKE '%%\_fdw'
                          AND e.extnamespace    = n.oid
                     )
                 ORDER BY
                       object_type
                     , object_name
            """
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, {'schema_name': schema_name, 'datero': DATERO_SCHEMA})
                    rows = cur.fetchall()

            res = [{
                'object_name': val[0],
                'object_type': val[1]
            } for val in rows]

            return res

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query}\nSchema: {schema_name}')
            raise e


    def get_object_details(self, schema_name: str, object_name: str, object_type: str):
        """Get list of columns for a given table/view"""
        try:
            query = r"""
                SELECT c.relname                                AS object_name
                     , c.relkind                                AS object_type
                     , JSON_AGG
                       ( JSON_BUILD_OBJECT('name', a.attname, 'data_type', t.typname)
                         ORDER BY a.attnum
                       )                                        AS columns
                  FROM pg_class             c
                 INNER JOIN
                       pg_attribute         a
                    ON a.attrelid           = c.oid
                 INNER JOIN
                       pg_type              t
                    ON t.oid                = a.atttypid
                 INNER JOIN
                       pg_namespace         n
                    ON n.oid                = c.relnamespace
                 WHERE n.nspname            = %(schema_name)s
                   AND c.relname            = %(object_name)s
                   AND c.relkind            = %(object_type)s
                   AND a.attnum             > 0
                   AND c.relkind            IN ('f', 'r', 'p', 'v', 'm')
                   AND n.nspname            NOT IN ( 'pg_catalog'
                                                   , 'pg_toast'
                                                   , 'information_schema'
                                                   , %(datero)s
                                                   )
                   AND NOT EXISTS
                     (
                       SELECT 1
                         FROM pg_extension      e
                        WHERE e.extname         LIKE '%%\_fdw'
                          AND e.extnamespace    = n.oid
                     )
                 GROUP BY
                       c.relname
                     , c.relkind
                 ORDER BY
                       object_type
                     , object_name
            """
            params = {
                'schema_name': schema_name,
                'object_name': object_name,
                'object_type': object_type,
                'datero': DATERO_SCHEMA
            }
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    row = cur.fetchone()

            res = {
                'object_name': row[0],
                'object_type': row[1],
                'columns': row[2]
            } if row is not None else None

            return res

        except psycopg2.Error as e:
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query}\nParams: {params}')
            raise e
