# -*- coding: utf-8 -*-
def nextj(t, j):
    k = 0
    if (j == 0):
        return 0
    elif (True):
        while (k < j):
            if (t[0:k] == t[j - k:j]):
                max = k
            k += 1
        return max
    else:
        return 1


def indexStrKmp(source, tag):
    i = 0
    j = 0
    slen = int(len(source))
    tlen = int(len(tag))
    print(isinstance(j, int))
    while ((i < slen) and (j < tlen)):
        if (source[i] == tag[j] or j == 0):
            i += 1
            j += 1
        else:
            j = nextj(tag, j)
    if (j > tlen - 1):
        return i - tlen
    else:
        return -1


if __name__ == '__main__':
    s0 = 'acdababdacabcdac'
    s1 = 'abc'
    print(indexStrKmp(s0, s1))
