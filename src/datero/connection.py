"""Singleton class for postgres database connection"""
from typing import Dict

from .pool import RestartableConnectionPool


class ConnectionPool:
    """Connection Pool Singleton class"""
    MIN_CONNECTIONS = 2
    MAX_CONNECTIONS = 4

    def __new__(cls, *_):
        """Connection object is singleton"""
        if not hasattr(cls, 'instance'):
            cls.instance = super(ConnectionPool, cls).__new__(cls)
            cls._initialized = False
        return cls.instance


    def __init__(self, config: Dict):
        if self._initialized:
            return

        self.config = config
        self.pool = self.init_pool()

        self._initialized = True


    def __del__(self):
        if hasattr(self, 'conn') and self.conn is not None:
            self.conn.close()


    def init_pool(self):
        """Instantiating connection from config credentials"""
        return RestartableConnectionPool(
            ConnectionPool.MIN_CONNECTIONS,
            ConnectionPool.MAX_CONNECTIONS,
            dbname=self.config['database'],
            user=self.config['username'],
            password=self.config['password'],
            host=self.config['hostname'],
            port=self.config['port'],
            application_name='datero'
        )


    def get_conn(self):
        return self.pool.getconn()

    def put_conn(self, conn):
        self.pool.putconn(conn)

