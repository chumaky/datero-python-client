"""Oracle database queries"""

class Oracle:
    """Oracle database queries"""

    @staticmethod
    def schema_list_table():
        """Command to create foreign table which will return schemas list"""
        return """
            CREATE FOREIGN TABLE IF NOT EXISTS {full_table_name} (owner TEXT)
            SERVER {server}
            OPTIONS (schema 'PUBLIC', table 'ALL_OBJECTS')
        """

    @staticmethod
    def table_list_table():
        """Command to create foreign table which will return tables list"""
        return  """
            CREATE FOREIGN TABLE IF NOT EXISTS {full_table_name} (owner TEXT, object_name TEXT, object_type TEXT)
            SERVER {server}
            OPTIONS (schema 'PUBLIC', table 'ALL_OBJECTS')
        """

    @staticmethod
    def schema_list():
        """Query to return schemas list"""
        return  """
            SELECT DISTINCT
                   tab.owner            AS schema_name
              FROM {full_table_name}    tab
             WHERE tab.owner            NOT IN ('PUBLIC', 'SYS', 'SYSTEM', 'XDB', 'MDSYS', 'CTXSYS', 'WMSYS', 'LBACSYS', 'OLAPSYS', 'ORDSYS')
        """

    @staticmethod
    def table_list():
        """Query to return tables list"""
        return  """
            SELECT tab.owner            AS table_schema
                 , tab.object_name      AS table_name
                 , tab.object_type      AS table_type
              FROM {full_table_name}    tab
             WHERE tab.owner            NOT IN ('PUBLIC', 'SYS', 'SYSTEM', 'XDB', 'MDSYS', 'CTXSYS', 'WMSYS', 'LBACSYS', 'OLAPSYS', 'ORDSYS')
               AND tab.object_type      IN ('TABLE', 'VIEW')
        """
