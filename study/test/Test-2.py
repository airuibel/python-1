# -*- coding: utf-8 -*-
"this is a test module"
import os,sys,string
debug = True
class FooClass(object):
    "Foo Class"
    pass
def test():
    "test function"
    foo = FooClass()
    if debug:
        print ('ran test()')
if __name__ ==  '__main__':
    test()
