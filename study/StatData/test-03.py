# -*- coding: utf-8 -*-

from random import random
from collections import Counter


class Interval(object):
    def __init__(self, val):
        self.val = val

    def __hash__(self):
        return int(self.val * 10) % 10

    def __str__(self):
        h = self.__hash__()
        return '[{0}, {1})'.format(h * 0.1, h * 0.1 + 0.1)

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __ne__(self, other):
        return self.__hash__() != other.__hash__()
cx

def main():
    c = Counter(Interval(random()) for i in range(100))
    for k, v in c.most_common():
        print(k, '-->', v)


if __name__ == '__main__':
    main()
