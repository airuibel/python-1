# -*- coding:gbk -*-
'''
Created on 2014年8月21日

@author: lenovo
'''
import binascii
import struct


def example(express, result=None):
    if result == None:
        result = eval(express)
    print(express, ' ==> ', result)


if __name__ == '__main__':


    print('字符串转整数:')



    example("int('0x1d', 16)")

    print('\n-------------------\n')