import json
import os
import sys


class Configuration:
    def __init__(self):
        # self.system_config = self.get_system_config()
        pass

    def get_system_config(self):
        """"读取配置"""
        if sys.platform == 'win32':
            # config_file_path = os.path.dirname(os.path.realpath(sys.argv[0])) + r'\config.json'
            config_file_path = r'.\config.json'
        elif sys.platform == 'linux':
            config_file_path = 'config.json'
        with open(config_file_path) as json_file:
            config = json.load(json_file)
        return config

    def set_config(self):
        pass

    def add_device(self):
        pass

    def delete_device(self):
        pass

    def updata_device(self):
        pass
