"""Misc utilities for package modules"""

from typing import Dict, Tuple
from psycopg2 import sql


def options_and_values(options: Dict, is_update: bool = False) -> Tuple[sql.SQL, Dict]:
    """Prepare list of key-value options in a safe bind variables manner"""
    keys = sql.SQL(', ').join([
        sql.SQL(' ').join([
            sql.SQL('set' if is_update else ''),
            sql.SQL(option),
            sql.Placeholder(option)
        ])
        for option in options.keys()
    ])
    values = {}
    for option, value in options.items():
        values[option] = str(value)

    return (keys, values)
