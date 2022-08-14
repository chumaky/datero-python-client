"""Application entry point"""

from ruamel.yaml import YAML

def load_config():
    "Parse config file"
    yaml = YAML(typ='safe')
    yaml.allow_duplicate_keys = True

    with open('config/template.yml', encoding='utf-8') as f:
        data = yaml.load(f)

    print(data)

if __name__ == "__main__":
    load_config()
