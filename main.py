"""Application entry point"""
import argparse

from hsql.extension import Extension
from hsql.fdw import FDW


def parse_params() -> str:
    """Parse input parameters"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file', help='Config file with foreign servers definition')
    args = parser.parse_args()

    if args.config_file is None:
        print('WARNING: Config file is not specified. Used default config which only install FDW extensions')
        print('WARNING: No foreign servers will be available')

    return args.config_file


def main():
    """Application entry point"""
    config = parse_params()

    ext = Extension(config)
    ext.init_extensions()
    fdw = FDW(config)
    fdw.init_servers()
    fdw.create_user_mappings()
    fdw.import_foreign_schema()


if __name__ == "__main__":
    main()
