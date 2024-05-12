from psycopg2 import pool, OperationalError

class RestartableConnectionPool(pool.SimpleConnectionPool):
    """
    Some FDWs cause PostgreSQL server crash and following auto-restart.
    This invalidates all connections in the pool.
    This class is a workaround to handle this situation.
    It tries to get a valid connection up to 10 times.
    """
    
    def getconn(self, key=None):
        """
        Get a connection from the pool.
        If there is an error, try to get a valid connection up to 10 times.
        """

        max_attempts = 10
        for i in range(max_attempts):
            try:
                if i > 0:
                    print(f"Getting connection from the pool. Attempt {i + 1}")

                conn = super().getconn(key)
                # Try to perform a simple operation to check if the connection is valid
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")

                if i > 0:
                    print(f"Connection obtained from the pool after {i + 1} attempts")

                return conn
            except OperationalError:
                # If the connection is not valid, close it and discard it from the pool
                print(f"Failed to get a valid connection. Attempt {i + 1}")
                self.putconn(conn, key=key, close=True)

        raise OperationalError(f"Failed to get a valid connection after {max_attempts} attempts")