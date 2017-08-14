import math

def jNumberInteger(n, e):
    tmp = math.pow(n, 1.0 / e)
    tmp = str(tmp).split('.')
    if tmp[1] != '0':
        return False
    else:
        return True

def sumN1toN2Cube(n1, n2, e):
    sum = 0
    while (n1 <= n2):
        sum += math.pow(n1, e)
        n1 += 1
    return sum

def cubeEq(n,e):
    start = 2
    n3 = math.pow(n, e)
    while start < n:
        sum = 0
        tmp = start
        while sum < n3:
            sum += math.pow(start, e)
            if jNumberInteger(sum, e) and start != tmp:
                print tmp, start, math.pow(sum, 1.0 / e)
                print "sumN1toN2Cube(n1, n2) is " +  str(sumN1toN2Cube(tmp, start, e)) + " equals " + str(sum)
            start += 1
        start = tmp
        start += 1

def main():
    cubeEq(100000, 3)
main()
