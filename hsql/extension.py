"""Activate required extensions"""

from hsql.config import load_config

class Extension:
    """Extension API wrapper"""

    def __init__(self):
        self.config = load_config()

    @property
    def fdw(self):
        """List of FDWs"""
        return self.config['fdw']


    def extension_list(self):
        """Get list of enabled extensions"""
        for ext, props in self.fdw.items():
            print(ext, props['enabled'], props['schema_name'] if props['schema_name'] is not None else ext)


    #CREATE SCHEMA mysql_fdw;
    #CREATE EXTENSION mysql_fdw SCHEMA mysql_fdw;

