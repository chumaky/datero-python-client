"""Application entry point"""
import sys
import argparse

from .extension import Extension
from .fdw import FDW


def parse_params() -> str:
    """Parse input parameters"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file', help='config file with foreign servers definition')
    parser.add_argument('-r', '--run', action='store_true', help='process config file and create foreign servers')
    parser.add_argument('-s', '--servers', action='store_true', help='print list of created foreign servers')
    parser.add_argument('-f', '--fdw-list', action='store_true', help='print list of available FDWs')
    parser.add_argument('-v', '--version', action='version', version='0.0.1')

    if len(sys.argv) < 2:
        parser.print_help()

    args = parser.parse_args()

    return args


def main():
    """Application entry point"""
    args = parse_params()

    fdw = FDW(args.config_file)

    if args.run:
        if args.config_file is None:
            print('WARNING: Config file is not specified. Used default config which could only install FDW extensions')
            print('WARNING: No foreign servers will be available')
        ext = Extension(args.config_file)
        ext.init_extensions()
        fdw.init_servers()
        fdw.create_user_mappings()
        fdw.import_foreign_schema()
    elif args.fdw_list:
        fdw.fdw_list()
    elif args.servers:
        fdw.server_list()


if __name__ == "__main__":
    main()
