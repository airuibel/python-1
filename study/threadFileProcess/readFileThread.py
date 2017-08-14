# _*_coding:utf-8_*_
import logging
from retrying import retry


class Reader(object):
    def __init__(self, file_name, dest_name, log_name):
        self.rfile_name = file_name
        self.dest_name = dest_name
        self.log_name = log_name
        handler = logging.handlers.RotatingFileHandler(self.log_name, maxBytes = 1024 * 1024, backupCount = 5)  # 实例化handler
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
        formatter = logging.Formatter(fmt)  # 实例化formatter
        handler.setFormatter(formatter)  # 为handler添加formatter
        logger = logging.getLogger('tst')  # 获取名为tst的logger
        logger.addHandler(handler)  # 为logger添加handler
        logger.setLevel(logging.DEBUG)

    @retry(stop_max_attempt_number = 3)
    def run(self):
        with open(self.rfile_name, mode = 'r') as f:
            for line in f:
                # print(line)
                pass
