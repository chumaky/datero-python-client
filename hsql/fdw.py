"""Foreign server management"""
import psycopg2

from .config import ConfigParser
from .connection import Connection

class FDW:
    """Foreign server management"""
    def __init__(self):
        self.config = ConfigParser().params
        self.conn = Connection()
