# -*- coding : utf-8 -*-
from FileProcess import FileProcess
from ConfigurationParser import ConfigurationParser
import threadpool
from NECIPS import NECIPS
import time

if __name__ == "__main__":
    config = ConfigurationParser("D:\\Git\\config\\", "conf.ini", "FileInfo")
    src_dir = config.get_config("src_dir")
    src_file = config.get_config("src_file")
    tmp_dir = config.get_config("tmp_dir")
    src_filename = src_dir + src_file
    clean_filename = tmp_dir + "clean.txt"
    des_dir = config.get_config("des_dir")
    log_dir = config.get_config("log_dir")
    print(src_filename, des_dir, clean_filename)
    fo = FileProcess(src_filename, clean_filename, des_dir)
    fo.get_clean_file("|", 0, False)
    fo_list = fo.split_file(1)
    # print(fo_list)
    # n = NECIPS(fo_list[0], fo_list[0] + '_done', tmp_dir, des_dir + fo_list[0].split('\\')[-1],
    #            log_dir + fo_list[0].split('\\')[-1])
    # n.main()
    t = []
    for fname in fo_list:
        print(fname)
        t.append(
            NECIPS(fname, fname + '_done', tmp_dir, des_dir + fname.split('\\')[-1],
                   log_dir + fname.split('\\')[-1]))
    start_time = time.time()
    pool = threadpool.ThreadPool(1)
    requests = threadpool.makeRequests(NECIPS.main, t)
    [pool.putRequest(req) for req in requests]
    pool.wait()
    print('%d second' % (time.time() - start_time))
