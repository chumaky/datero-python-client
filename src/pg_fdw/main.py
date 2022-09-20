"""Application entry point"""
import sys
import argparse

from .app import App


def parse_params() -> argparse.Namespace:
    """Parse input parameters"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file', help='config file with foreign servers definition')
    parser.add_argument('-r', '--run', action='store_true', help='process config file and create foreign servers')
    parser.add_argument('-s', '--servers', action='store_true', help='print list of created foreign servers')
    parser.add_argument('-f', '--fdw-list', action='store_true', help='print list of available FDWs')
    parser.add_argument('-v', '--version', action='version', version='0.0.2')

    if len(sys.argv) < 2:
        parser.print_help()

    args = parser.parse_args()

    return args


def main():
    """Application entry point"""
    args = parse_params()

    app = App(args.config_file)

    if args.run:
        app.run()
    elif args.fdw_list:
        res = app.fdw_list
        for row in res:
            print(f"Name: {row['name']}, Description: {row['description']}")
    elif args.servers:
        res = app.server_list
        for row in res:
            print(f"Name: {row['server_name']}, Description: {row['fdw_name']}")


if __name__ == "__main__":
    main()
