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
            conn = self.pool.get_conn()
            cur = conn.cursor()

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
                    cur.execute(query, values)
                else:
                    query = sql.SQL(stmt).format(
                        remote_schema=sql.Identifier(remote_schema),
                        server=sql.Identifier(server),
                        local_schema=sql.Identifier(local_schema),
                    )
                    cur.execute(query)

                conn.commit()
                print(f'Foreign schema "{remote_schema}" from server "{server}" successfully imported into "{local_schema}"')
        except psycopg2.Error as e:
            conn.rollback()
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query.as_string(cur)}')
        finally:
            cur.close()
            self.pool.put_conn(conn)


    def get_foreign_schema_list(self, server_name: str, fdw_name: str):
        """Get list of available schemas to import."""
        table_name = f'{server_name}_schema_list'

        adapter = Adapter(fdw_name)
        stmt = adapter.schema_list()

        res = []
        try:
            conn = self.pool.get_conn()
            cur = conn.cursor()

            if stmt is not None:
                query = sql.SQL(stmt).format(
                    full_table_name=sql.Identifier(DATERO_SCHEMA, table_name),
                )
                cur.execute(query)
                rows = cur.fetchall()

                res = [val[0] for val in rows]

                conn.commit()

            elif fdw_name == FdwType.SQLITE.value or fdw_name == FdwType.DUCKDB.value:
                res = ['public']

            if len(res) > 0:
                print(f'Foreign server "{server_name}" schemas count: {len(res)}')
            else:
                print(f'Foreign server "{server_name}" doesn''t support schemas import')

            return res

        except psycopg2.Error as e:
            conn.rollback()
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query.as_string(cur)}')
            raise e
        finally:
            cur.close()
            self.pool.put_conn(conn)


    def import_foreign_schema(self, data: Dict):
        """Import foreign schema"""
        def recreate_schema():
            query = sql.SQL('DROP SCHEMA IF EXISTS {local_schema} CASCADE') \
                .format(local_schema=sql.Identifier(local_schema))

            cur.execute(query)

            query = sql.SQL('CREATE SCHEMA IF NOT EXISTS {local_schema}') \
                .format(local_schema=sql.Identifier(local_schema))

            cur.execute(query)

        try:
            conn = self.pool.get_conn()
            cur = conn.cursor()

            server_name = data['server_name']
            remote_schema = data['remote_schema']
            local_schema = data['local_schema']

            import_options = data['options'] if 'options' in data else None

            recreate_schema()
            self.set_description(server_name, remote_schema, local_schema)

            stmt = \
                'IMPORT FOREIGN SCHEMA {remote_schema} ' \
                'FROM SERVER {server} ' \
                'INTO {local_schema}'

            if import_options is not None and len(import_options) > 0:
                stmt += ' OPTIONS({options})'
                options, values = options_and_values(import_options)

                query = sql.SQL(stmt).format(
                    remote_schema=sql.Identifier(remote_schema),
                    server=sql.Identifier(server_name),
                    local_schema=sql.Identifier(local_schema),
                    options=options
                )
                cur.execute(query, values)
            else:
                query = sql.SQL(stmt).format(
                    remote_schema=sql.Identifier(remote_schema),
                    server=sql.Identifier(server_name),
                    local_schema=sql.Identifier(local_schema),
                )
                cur.execute(query)

            conn.commit()
            print(f'Foreign schema "{remote_schema}" from server "{server_name}" successfully imported into "{local_schema}"')

            return data

        except psycopg2.Error as e:
            conn.rollback()
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query.as_string(cur)}')
            raise e
        finally:
            cur.close()
            self.pool.put_conn(conn)


    def set_description(self, server_name: str, remote_schema: str, local_schema: str):
        """Update user-defined name"""
        conn = self.pool.get_conn()
        with conn.cursor() as cur:
            stmt = 'COMMENT ON SCHEMA {schema} IS %s'
            query = sql.SQL(stmt).format(
                schema=sql.Identifier(local_schema)
            )
            cur.execute(query, (f'{server_name}#{DATERO_SCHEMA}#{remote_schema}',))

        self.pool.put_conn(conn)


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

                    conn.commit()

            return res

        except psycopg2.Error as e:
            conn.rollback()
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query}')
            raise e


    def get_local_schema_objects(self, schema_name: str):
        """Get list of local schema objects"""
        try:
            conn = self.pool.get_conn()
            cur = conn.cursor()
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
            cur.execute(query, {'schema_name': schema_name, 'datero': DATERO_SCHEMA})
            rows = cur.fetchall()

            res = [{
                'object_name': val[0],
                'object_type': val[1]
            } for val in rows]

            conn.commit()
            return res

        except psycopg2.Error as e:
            conn.rollback()
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query}')
            raise e
        finally:
            cur.close()
            self.pool.put_conn(conn)


    def get_object_details(self, schema_name: str, object_name: str, object_type: str):
        """Get list of columns for a given table/view"""
        try:
            conn = self.pool.get_conn()
            cur = conn.cursor()
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
            cur.execute(query, {
                'schema_name': schema_name,
                'object_name': object_name,
                'object_type': object_type,
                'datero': DATERO_SCHEMA
            })
            row = cur.fetchone()

            res = {
                'object_name': row[0],
                'object_type': row[1],
                'columns': row[2]
            } if row is not None else None

            conn.commit()
            return res

        except psycopg2.Error as e:
            conn.rollback()
            print(f'Error code: {e.pgcode}\nMessage: {e.pgerror}\nSQL: {query}')
            raise e
        finally:
            cur.close()
            self.pool.put_conn(conn)
