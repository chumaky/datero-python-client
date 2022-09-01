"""Singleton class for postgres database connection"""
import psycopg2

from .config import ConfigParser

class Connection:
    """Parsing config files"""

    def __new__(cls, *args):
        """Connection object is singleton"""
        if not hasattr(cls, 'instance'):
            cls.instance = super(Connection, cls).__new__(cls)
            cls._initialized = False
        return cls.instance


    def __init__(self, config_file: str = None):
        if self._initialized:
            return

        self.config = ConfigParser(config_file).params
        self._conn = self.init_connection()

        self._initialized = True


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
