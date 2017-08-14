# -*- coding: utf-8 -*-
import urllib2
import re

a = [[]]
url = 'http://66.143.97.152/mydomain/index.jsp?isLogin=true'
request = urllib2.Request(url)
response = urllib2.urlopen(request)
data = response.read()
print data
