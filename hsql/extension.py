"""Activate required extensions"""

from .config import ConfigParser

class Extension:
    """Extension API wrapper"""

    def __init__(self):
        self.cp = ConfigParser()
        self.config = self.cp.parse_config()

    @property
    def fdw(self):
        """List of FDWs"""
        return self.config['fdw_list']


    def extension_list(self):
        """Get list of enabled extensions"""
        for ext, props in self.fdw.items():
            schema_name = props['schema_name'] if props['schema_name'] is not None else ext
            if props['enabled']:
                print(f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
                print(f'CREATE EXTENSION IF NOT EXISTS {ext} WITH SCHEMA {schema_name};')

