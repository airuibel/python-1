# -*- coding: utf-8 -*-
import csv
import matplotlib.pyplot as plt
from matplotlib import style
import numpy as np

style.use('ggplot')


class calDistri(object):
    def __init__(self, dic, filename, sentif, sentib):
        self.dic = dic
        self.filename = filename
        self.sentif = sentif
        self.sentib = sentib

    # read csv file to map
    def readfile2map(self):
        with open(self.filename, "r", encoding="utf-8") as csvfile:
            # 读取csv文件，返回的是迭代类型
            read = csv.reader(csvfile)
            for data in read:
                s1, s2 = data[0], data[1]
                self.dic[s1] = s2
        return self.dic

    # sum key mins key's value in the map  between sentif and sentib
    def caldistr(self):
        sum = 0
        for k in self.dic:
            k1 = int(self.dic[k])
            k = int(k)
            print("k:%d, k1:%d" % (k, k1))
            if (k >= self.sentif and k <= self.sentib):
                j = k1 - k
                sum += j
            else:
                print("sum not change")
        return sum

    def np_read_csv(self, filename):
        data = np.loadtxt(filename, dtype=np.str, delimiter=",")
        data = data[1:, 0:].astype(np.float)
        return data


if __name__ == "__main__":
    data = calDistri({}, "data2.csv", 0, 128)
    # data.readfile2map()
    # print((data.caldistr()) / 128)
    p = data.np_read_csv("D:\Git\python\study\StatData\data2.csv")
