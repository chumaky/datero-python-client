"""Singleton class for postgres database connection"""
import psycopg2

from .config import ConfigParser

class Connection:
    """Parsing config files"""

    def __new__(cls, config_file: str = None):
        """Connection object is singleton"""
        if not hasattr(cls, 'instance'):
            cls.instance = super(Connection, cls).__new__(cls)
        return cls.instance


    def __init__(self, config_file: str = None):
        self.config = ConfigParser(config_file).params
        self._conn = self.init_connection()


    def __del__(self):
        if hasattr(self, '_conn') and self._conn is not None:
            self._conn.close()


    def init_connection(self):
        """Instantiating connection from config credentials"""
        #print(self.config['postgres'])
        conf = self.config['postgres']
        return psycopg2.connect(
            dbname=conf['database'],
            user=conf['username'],
            password=conf['password'],
            host=conf['hostname'],
            port=conf['port']
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
