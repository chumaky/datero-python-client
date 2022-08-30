"""Application entry point"""
import sys
import getopt
from typing import List

from hsql.extension import Extension
from hsql.fdw import FDW


def parse_params(argv: List[str]) -> str:
    """Parse input parameters"""
    config_file = None
    usage_msg = 'main.py [-c|--config_file  <config_file>]'
    try:
        opts, _ = getopt.getopt(argv,"hc:",["config_file="])
    except getopt.GetoptError:
        print(usage_msg)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(usage_msg)
            sys.exit()
        elif opt in ("-c", "--config_file"):
            config_file = arg

    print('Config file:', config_file)
    if config_file is None:
        print('WARNING: Config file is not specified. Used default config which only install FDW extensions')
        print('WARNING: No foreign servers will be available')
    return config_file


def main(argv):
    """Application entry point"""
    config = parse_params(argv)
    ext = Extension(config)
    ext.init_extensions()
    fdw = FDW(config)
    fdw.init_servers()


if __name__ == "__main__":
    main(sys.argv[1:])
