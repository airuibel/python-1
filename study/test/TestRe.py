# -*- coding: utf-8 -*-
import re

test = '用户输入的字符串'
if re.match(r'用户', test):
    print('ok')
else:
    print('failed')
print('a b   c'.split(' '))
print(re.split(r'\s*', 'a b   c'))
print(re.split(r'[\s\,\;]+', 'a,b;; c  d'))
m = re.match(r'^(\d{3})-(\d{3,8})$', '010-12345')
print(m.group(1))
m = re.match(r'^(\S+)@(\S+.com)$', 'cysuncn@126.com')
print(m.group(2))
print(m.groups())
# <Tom Paris> tom@voyager .org

re_mail = re.compile(r'<(\S+)\s+(\S+)>\s+(\S+)@(\S+.org)')
print(re_mail.match('<Tom Paris> tom@voyager.org').groups())

str = 'abcbacba'
# non-greed match
re = re.compile(r'a.*?a', re.S)
print(re.match(str).group())
