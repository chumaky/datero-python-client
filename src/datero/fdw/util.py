"""Misc utilities for package modules"""

from typing import Dict, Tuple
from enum import Enum
from psycopg2 import sql

def options_and_values(input_options: Dict, current_options: Dict = {}) -> Tuple[sql.SQL, Dict]:
    """Prepare list of key-value options in a safe bind variables manner"""

    # for Create operation modifier must be '' empty string
    # for Update it could be either 'set' or 'add'
    add_or_empty = '' if current_options == {} else 'add'

    # direct path to specify SET/ADD modifiers
    new_existing_options = [
        sql.SQL(' ').join([
            sql.SQL('set' if option in current_options else add_or_empty),
            sql.SQL(option),
            sql.Placeholder(option)
        ])
        for option in input_options.keys()
    ]
    drop_options = [
        sql.SQL(' ').join([
            sql.SQL('drop'),
            sql.SQL(option)
        ])
        for option in current_options.keys() if option not in input_options.keys()
    ]

    keys = sql.SQL(', ').join(new_existing_options + drop_options)

    values = {}
    for option, value in input_options.items():
        values[option] = str(value)

    return (keys, values)


class FdwType(Enum):
    """FDW types"""
    MYSQL = 'mysql_fdw'
    POSTGRES = 'postgres_fdw'
    MONGO = 'mongo_fdw'
    ORACLE = 'oracle_fdw'
    TDS = 'tds_fdw'
    SQLITE = 'sqlite_fdw'
    FILE = 'file_fdw'
    REDIS = 'redis_fdw'
    DUCKDB = 'duckdb_fdw'

class ImportType(Enum):
    """Schema/Table import levels"""
    SCHEMA = 'schema'
    TABLE = 'table'
