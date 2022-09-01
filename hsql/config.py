"""Parsing config file"""
import os
#import json

from copy import deepcopy
from ruamel.yaml import YAML

from . import CONFIG_DIR, DEFAULT_CONFIG, USER_CONFIG


class ConfigParser:
    """Parsing config files"""

    def __new__(cls, *args):
        """Config object is singleton"""
        if not hasattr(cls, 'instance'):
            self = super(ConfigParser, cls).__new__(cls)
            cls.instance = self
            cls._initialized = False

        return cls.instance


    def __init__(self, config_file: str = USER_CONFIG) -> None:
        if self._initialized:
            return

        self.default_config_file = \
            os.path.join(os.path.dirname(__file__), '..', CONFIG_DIR, DEFAULT_CONFIG)
        self.user_config_file = \
            config_file if config_file is not None else \
            os.path.join(os.path.dirname(__file__), '..', CONFIG_DIR, USER_CONFIG)
        self.default_params = {}
        self.user_params = {}
        self.params = {}

        self.yaml = YAML(typ='safe')
        self.yaml.allow_duplicate_keys = True

        self.parse_config()

        self._initialized = True


    def parse_default_config(self):
        "Parse default config file"
        with open(self.default_config_file, encoding='utf-8') as f:
            self.default_params = self.yaml.load(f)


    def parse_config(self):
        "Parse user config file and merge it with default config"
        self.parse_default_config()

        if self.user_config_file is not None:
            with open(self.user_config_file, encoding='utf-8') as f:
                self.user_params = self.yaml.load(f)

        self.params = self.deep_merge(self.default_params, self.user_params)
        #print('result', json.dumps(self.params, indent=2))


    def deep_merge(self, a: dict, b: dict) -> dict:
        """Merging two dictionaries of arbitrary depth"""
        res = deepcopy(a)
        for k, bv in b.items():
            av = res.get(k)
            if isinstance(av, dict) and isinstance(bv, dict):
                res[k] = self.deep_merge(av, bv)
            elif bv is not None:
                res[k] = deepcopy(bv)
        return res
