"""Wrapper for the underlying specific adapters"""

from ..fdw import FdwType
from .mysql import MySQL
from .postgres  import Postgres
from .oracle import Oracle


class Adapter:
    """Wrapper for the underlying specific adapters"""

    def __init__(self, fdw_name: str):
        self.fdw_name = fdw_name


    def schema_list_table(self):
        """Command to create foreign table which will return schemas list"""
        stmt = None

        match self.fdw_name:
            case FdwType.MYSQL.value:
                stmt = MySQL.schema_list_table()
            case FdwType.POSTGRES.value:
                stmt = Postgres.schema_list_table()
            case FdwType.ORACLE.value:
                stmt = Oracle.schema_list_table()
            case _:
                print('Schema import is not supported')

        return stmt


    def table_list_table(self):
        """Command to create foreign table which will return tables list"""
        stmt = None

        match self.fdw_name:
            case FdwType.MYSQL.value:
                stmt = MySQL.table_list_table()
            case FdwType.POSTGRES.value:
                stmt = Postgres.table_list_table()
            case FdwType.ORACLE.value:
                stmt = Oracle.table_list_table()
            case _:
                print('Schema import is not supported')

        return stmt


    def schema_list(self):
        """Query to return schemas list"""
        stmt = None

        match self.fdw_name:
            case FdwType.MYSQL.value:
                stmt = MySQL.schema_list()
            case FdwType.POSTGRES.value:
                stmt = Postgres.schema_list()
            case FdwType.ORACLE.value:
                stmt = Oracle.schema_list()
            case _:
                print('Schema import is not supported')

        return stmt


    def table_list(self):
        """Query to return tables list"""
        stmt = None

        match self.fdw_name:
            case FdwType.MYSQL.value:
                stmt = MySQL.table_list()
            case FdwType.POSTGRES.value:
                stmt = Postgres.table_list()
            case FdwType.ORACLE.value:
                stmt = Oracle.table_list()
            case _:
                print('Getting table list is not supported')

        return stmt
