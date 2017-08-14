# -*- coding : utf-8 -*-
import configparser


class ConfigurationParser(object):
    def __init__(self, path, config_file, section):
        self.path = path
        self.config_file = config_file
        self.section = section
        self.config = configparser.ConfigParser()
        self.config.read(self.path + self.config_file)

    def get_config(self, item):
        return self.config.get(self.section, item)
