"""Parsing config file"""

from ruamel.yaml import YAML

from . import CONFIG_FILE

def load_config(config_file=CONFIG_FILE):
    "Parse config file"
    yaml = YAML(typ='safe')
    yaml.allow_duplicate_keys = True

    with open(config_file, encoding='utf-8') as f:
        data = yaml.load(f)

    print(data)
    return data
