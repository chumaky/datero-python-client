"""Singleton class for postgres database connection"""
from typing import Dict
import psycopg2

class Connection:
    """Parsing config files"""

    def __new__(cls, *args):
        """Connection object is singleton"""
        if not hasattr(cls, 'instance'):
            cls.instance = super(Connection, cls).__new__(cls)
            cls._initialized = False
        return cls.instance


    def __init__(self, config: Dict):
        if self._initialized:
            return

        self.config = config
        self._conn = self.init_connection()

        self._initialized = True


    def __del__(self):
        if hasattr(self, '_conn') and self._conn is not None:
            self._conn.close()


    def init_connection(self):
        """Instantiating connection from config credentials"""
        #print(self.config['postgres'])
        return psycopg2.connect(
            dbname=self.config['database'],
            user=self.config['username'],
            password=self.config['password'],
            host=self.config['hostname'],
            port=self.config['port']
        )


    @property
    def cursor(self):
        """Create new cursor over connection"""
        return self._conn.cursor()

    def commit(self):
        """Wrapper method for commit"""
        self._conn.commit()

    def rollback(self):
        """Wrapper method for rollback"""
        self._conn.rollback()
