# -*- coding:gbk -*-
'''
Created on 2014��8��21��

@author: lenovo
'''
import binascii
import struct


def example(express, result=None):
    if result == None:
        result = eval(express)
    print(express, ' ==> ', result)


if __name__ == '__main__':


    print('�ַ���ת����:')



    example("int('0x1d', 16)")

    print('\n-------------------\n')