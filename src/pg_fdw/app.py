"""main API interface"""

from .config import ConfigParser
from .fdw import Extension, Server, UserMapping, Schema
from .admin import Admin

class App:
    """main API interface"""

    def __init__(self, config_file: str = None):
        self.config_file = config_file
        self.cp = ConfigParser(config_file)

        self.admin = Admin(self.config)

        self.extension = Extension(self.config)
        self.server = Server(self.config)
        self.user = UserMapping(self.config)
        self.schema = Schema(self.config)


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
        return self.extension.fdw_list()

    @property
    def server_list(self):
        """Return list of available foreign servers"""
        return self.server.server_list()

    @property
    def health_check(self):
        """Return health check status"""
        return self.admin.healthcheck()


    def run(self):
        """Process config file and create specified extensions and foreign servers"""

        if self.config_file is None:
            print('WARNING: Config file is not specified. Used default config which could only install FDW extensions')
            print('WARNING: No foreign servers will be available')

        self.extension.init_extensions()
        self.server.init_servers()
        self.user.create_user_mappings()
        self.schema.import_foreign_schema()
