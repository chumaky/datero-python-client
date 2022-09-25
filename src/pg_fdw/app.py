"""main API interface"""

from .config import ConfigParser
from .extension import Extension
from .fdw import FDW

class App:
    """main API interface"""

    def __init__(self, config_file: str = None):
        self.config_file = config_file
        self.cp = ConfigParser(config_file)
        self.ext = Extension(self.config)
        self.fdw = FDW(self.config)

    @property
    def config(self):
        """Current configuration. Merge of default and user config files"""
        return self.cp.params

    @property
    def default_config(self):
        """Default configuration"""
        return self.cp.default_params

    @property
    def user_config(self):
        """User provided configuration"""
        return self.cp.user_params

    @property
    def fdw_list(self):
        """Return list of available FDWs"""
        return self.fdw.fdw_list()

    @property
    def server_list(self):
        """Return list of available foreign servers"""
        return self.fdw.server_list()


    def run(self):
        """Process config file and create specified extensions and foreign servers"""

        if self.config_file is None:
            print('WARNING: Config file is not specified. Used default config which could only install FDW extensions')
            print('WARNING: No foreign servers will be available')

        self.ext.init_extensions()

        self.fdw.init_servers()
        self.fdw.create_user_mappings()
        self.fdw.import_foreign_schema()
