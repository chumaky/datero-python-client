"""Application entry point"""

from hsql.extension import Extension


if __name__ == "__main__":
    ext = Extension()
    ext.extension_list()