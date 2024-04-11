"""Parsing config file"""
import os
import json

from copy import deepcopy
from ruamel.yaml import YAML

from . import CONFIG_DIR, DEFAULT_CONFIG, USER_CONFIG, CONNECTION

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
        """
        Parse different config files and merge them into one configuration.
        Levels of precedence:
        - default config file
        - user config file
        - environment variables
        - command line arguments
        """
        self.parse_default_config()

        self.params = deepcopy(self.default_params)
        
        self.apply_user_config()
        self.apply_env_config()


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
            'driver_official_source': fdw_spec['source'],
            'advanced': {}
        }

        def set_default_flag(section: str, item: str) -> None:
            """
            Options listed in the datero config and having default value in the specification are mandatory.
            This is because they are exposed to the user and user has a capability to erase them.
            We must ensure that they are present. Either with the default value or with the user provided one
            """
            if 'default' in result[section][item]:
                result[section][item]['required'] = True

        def set_advanced_attribute(section: str, item: str, position: int) -> None:
            """
            Set non-referenced by Datero attribute from the specification as advanced.
            """
            result['advanced'][section][item] = fdw_spec[section][item]
            result['advanced'][section][item]['position'] = position


        # passing through standard FDW sections
        for section in FDW_SPEC_SECTIONS:
            # if section is present in the datero config
            # pick up its options from the specification in order specified in the datero config
            if section in datero_fdw_options:
                result[section] = {}

                # add minimum set of options expected by Datero if section contains any options
                if datero_fdw_options[section]:
                    for idx, item in enumerate(datero_fdw_options[section]):
                        # if option in datero config is scalar
                        # it means that it has no override attributes and we can just copy its definition from fdw_spec
                        if isinstance(datero_fdw_options[section][idx], str) and item in fdw_spec[section]:
                            result[section][item] = fdw_spec[section][item]
                            result[section][item]['position'] = idx

                            set_default_flag(section, item)

                        # if option in datero config is dict and not scalar
                        # it means that it has some additional and/or override attributes over the fdw_spec
                        # other non-matched attributes from fdw_spec will be preserved
                        elif isinstance(datero_fdw_options[section][idx], dict):
                            key = list(item.keys())[0]  # get the dict key name
                            if key in fdw_spec[section]:
                                result[section][key] = self.deep_merge(fdw_spec[section][key], item[key])
                                result[section][key]['position'] = idx

                            set_default_flag(section, key)

                # add the rest of the options for the given section from the specification as advanced options
                for item in fdw_spec[section]:
                    if item not in result[section]:
                        # initialize the advanced options section if it is not present
                        if section not in result['advanced']:
                            result['advanced'][section] = {}
                            position = 0

                        set_advanced_attribute(section, item, position)
                        position += 1

            # if section is not present in the datero config
            # pick up its options from the specification in order specified in the specification
            # and set them as advanced options
            elif section in fdw_spec:
                result['advanced'][section] = {}
                for idx, item in enumerate(fdw_spec[section]):
                    set_advanced_attribute(section, item, idx)

        #print(json.dumps(result, indent=2))

        # replace the original fdw_options with the expanded one
        self.default_params['fdw_options'][fdw_name] = result


    def apply_user_config(self):
        """
        If user config file is present, apply it on top of the the default config.
        """
        if self.user_config_file is not None:
            with open(self.user_config_file, encoding='utf-8') as f:
                self.user_params = self.yaml.load(f)

        #print('config', json.dumps(self.params, indent=2))

        self.params[CONNECTION].update({
            'hostname': self.user_params[CONNECTION].get('hostname', self.params[CONNECTION]['hostname']),
            'port'    : self.user_params[CONNECTION].get('port'    , self.params[CONNECTION]['port'    ]),
            'database': self.user_params[CONNECTION].get('database', self.params[CONNECTION]['database']),
            'username': self.user_params[CONNECTION].get('username', self.params[CONNECTION]['username']),
            'password': self.user_params[CONNECTION].get('password', self.params[CONNECTION]['password'])
        })

        if 'servers' in self.user_params:
            self.params['servers'] = self.user_params['servers']


    def apply_env_config(self):
        """
        Apply environment variables to the configuration.
        """
        self.params[CONNECTION].update({
            # datero specific variables
            'hostname': os.environ.get('POSTGRES_HOST'      , self.params[CONNECTION]['hostname']),
            'port'    : os.environ.get('POSTGRES_PORT'      , self.params[CONNECTION]['port'    ]),
            # official postgres images variables
            'database': os.environ.get('POSTGRES_DB'        , self.params[CONNECTION]['database']),
            'username': os.environ.get('POSTGRES_USER'      , self.params[CONNECTION]['username']),
            'password': os.environ.get('POSTGRES_PASSWORD'  , self.params[CONNECTION]['password'])
        })

        #print('connection', json.dumps(self.params[CONNECTION], indent=2))
