# _*_coding:utf-8_*_
import time, threading, configparser
from urllib import parse
import urllib.request
import json
from retrying import retry


class Reader(threading.Thread):
    def __init__(self, file_name, dest_name):
        super(Reader, self).__init__()
        self.rfile_name = file_name
        self.dest_name = dest_name

    def isZh(self, s):
        if len(s) < 5:
            return False
        if not ('\u4e00' <= s[0] <= '\u9fa5'):
            return False
        return True

    @retry(stop_max_attempt_number=3)
    def run(self):
        destfile = open(self.dest_name, 'w')
        with open(self.rfile_name, mode='rb') as f:
            for line in f:
                data = line.decode('gbk', 'ignore').strip().split(" ")
                if len(data) > 1:
                    addr = data[1]
                    if self.isZh(addr):
                        query = {
                            'key': '****',
                            'address': addr,
                            'output': 'json',
                        }
                        base = 'http://api.map.baidu.com/geocoder?'
                        url = base + parse.urlencode(query)
                        doc = urllib.request.urlopen(url)
                        s = doc.read().decode('utf-8')
                        jsonData = json.loads(s)
                        if len(jsonData['result']) != 0:
                            lat = jsonData['result']['location']['lat']
                            lng = jsonData['result']['location']['lng']
                            result = data[0] + '|' + data[1] + '|' + str(lat) + '|' + str(lng) + '\n'
                            # print(result)
                            destfile.write(result)
        destfile.close()


if __name__ == '__main__':
    ''''' 
    读取配置文件 
    '''
    config = configparser.ConfigParser()
    config.readfp(open('D:\Git\config\conf.ini'))
    # 文件名
    file_name = config.get('info', 'fileName')
    file_name = file_name.split(',')
    print(file_name[0])
    # 线程数量
    thread_num = int(config.get('info', 'threadNum'))
    # 起始时间
    start_time = time.clock()
    t = []
    # 生成线程
    for i in range(thread_num):
        print(i)
        t.append(Reader(file_name[i], "D:\Git\\result\\" + file_name[i].split('\\')[-1]))
        # 开启线程
    for i in range(thread_num):
        t[i].start()
    for i in range(thread_num):
        t[i].join()
        # 结束时间
    end_time = time.clock()
    print("Cost time is %f" % (end_time - start_time))
