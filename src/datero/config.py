"""Parsing config file"""
import os
import json

from copy import deepcopy
from ruamel.yaml import YAML

from . import CONFIG_DIR, DEFAULT_CONFIG, USER_CONFIG

FDW_SPEC_SECTIONS = [
    'foreign_server',
    'user_mapping',
    'import_foreign_schema',
    'create_foreign_table',
    'foreign_table_column'
]

class ConfigParser:
    """Parsing config files"""

    def __new__(cls, *args):
        """Config object is singleton"""
        if not hasattr(cls, 'instance'):
            self = super(ConfigParser, cls).__new__(cls)
            cls.instance = self
            cls._initialized = False

        return cls.instance


    def __init__(self, config_file: str) -> None:
        if self._initialized:
            return

        self.default_config_file = \
            os.path.join(os.path.dirname(__file__), CONFIG_DIR, DEFAULT_CONFIG)
        self.user_config_file = \
            config_file if config_file is not None else \
            os.path.join(os.path.dirname(__file__), CONFIG_DIR, USER_CONFIG)
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

        self.transform_default_config()


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


    def transform_default_config(self):
        """
        For "fdw_options" key in the default config file check for any drivers references denoted by "version" key.
        If present, get corresponding driver options from the "fdw_spec" folder and merge them with the default config.
        """
        for fdw_name in self.default_params['fdw_options']:
            if 'version' in self.default_params['fdw_options'][fdw_name]:
                print(f'Expanding {fdw_name} options...')
                version = self.default_params['fdw_options'][fdw_name]['version']
                fdw_spec_path = os.path.join(
                    os.path.dirname(__file__),
                    CONFIG_DIR,
                    'fdw_spec',
                    fdw_name,
                    f'{version}.yaml'
                )
                with open(fdw_spec_path, encoding='utf-8') as f:
                    fdw_spec = self.yaml.load(f)

                self.prepare_fdw_options(fdw_name, fdw_spec)


    def prepare_fdw_options(self, fdw_name: str, fdw_spec: dict):
        """
        Basing on the given FDW specification, prepare FDW options with Datero added attributes.
        """
        datero_fdw_options = self.default_params['fdw_options'][fdw_name]
        result = {
            'name': fdw_spec['name'],
            'version': fdw_spec['version'],
            'driver_official_source': fdw_spec['source']
        }

        # passing through standard FDW sections
        for section in FDW_SPEC_SECTIONS:
            # if section is present in the datero config
            # pick up its options from the specification in order specified in the datero config
            if section in datero_fdw_options:
                result[section] = {}
                for idx, item in enumerate(datero_fdw_options[section]):
                    if item in fdw_spec[section]:
                        result[section][item] = fdw_spec[section][item]
                        result[section][item]['position'] = idx
                        # options listed in the datero config and having default value in the specification are mandatory
                        # this is because they are exposed to the user and user has a capability to erase them.
                        # we must ensure that they are present. either with the default value or with the user provided one
                        if 'default' in result[section][item]:
                            result[section][item]['required'] = True

            # if section is not present in the datero config
            # pick up its options from the specification in order specified in the specification
            elif section in fdw_spec:
                result[section] = {}
                for idx, item in enumerate(fdw_spec[section]):
                    result[section][item] = fdw_spec[section][item]
                    result[section][item]['position'] = idx

        #print(json.dumps(result, indent=2))

        # replace the original fdw_options with the expanded one
        self.default_params['fdw_options'][fdw_name] = result

