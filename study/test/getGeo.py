# -*- coding : utf-8 -*-
'''''
利用百度地图api实现经纬度与地址的批量转换
'''
from urllib import parse
import urllib.request
import json


def isZh(s):
    if not ('\u4e00' <= s[0] <= '\u9fa5'):
        return False
    return True


destfile = open('result.txt', 'w')
with open('OCRM_F_CI_PER_CUST_INFO.csv', mode='r') as f:
    for line in f:
        data = line.strip().split(" ")
        addr = data[1]
        if isZh(addr):
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
                print(result)
                destfile.write(result)
destfile.close()
