"""Singleton class for postgres database connection"""
from typing import Dict
from contextlib import contextmanager

from .pool import RestartableConnectionPool


class ConnectionPool:
    """Connection Pool Singleton class"""
    MIN_CONNECTIONS = 1
    MAX_CONNECTIONS = 2

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
        if hasattr(self, 'pool') and self.pool is not None:
            self.pool.closeall()


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


    @contextmanager
    def connection(self):
        conn = self.get_conn()
        try:
            yield conn
        except Exception:
            conn.rollback()  # rollback changes in case of error
            raise
        finally:
            conn.commit()  # commit changes before returning the connection
            self.put_conn(conn)
