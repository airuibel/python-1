# -*- coding : utf-8 -*-
from FileProcess import FileProcess
from ConfigurationParser import ConfigurationParser

if __name__ == "__main__":
    config = ConfigurationParser("D:\\Git\\config\\", "conf.ini", "FileInfo")
    src_dir = config.get_config("src_dir")
    src_file = config.get_config("src_file")
    src_filename = src_dir + src_file
    clean_filename = "D://Git//result//test.txt"
    des_dir = config.get_config("des_dir")
    print(src_filename, des_dir)
    fo = FileProcess(src_filename, clean_filename, des_dir)
    fo.get_clean_file(" ", 1)
    fo.split_file(10000)
