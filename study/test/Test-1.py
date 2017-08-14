# -*- coding: utf-8 -*-
import urllib3
import re
import pandas as pd

a = [[]]
url = 'http://focus.tianya.cn/'
request = urllib3.Request(url)
response = urllib3.urlopen(request)
content = response.read().decode('utf-8')
pattern = re.compile('<h3>.*?href="(.*?)"*?title="(.*?)".*?title" >', re.S)
items = re.findall(pattern, content)
for item in items:
    print (item[0], item[1].encode('utf-8'))
