import unittest


def addNum(a, b):
    return a + b


def delNum(a, b):
    return a - b


def ploy(a):
    if a < 0:
        raise ValueError
    if 0 == a:
        return 1
    if 1 == a:
        return 1
    return a * ploy(delNum(a, 1))


class TestFun(unittest.TestCase):
    def testAdd(self):
        self.assertEqual(2, addNum(1, 1))

    def testDel(self):
        self.assertEqual(0, delNum(1, 1))

    def testPloy(self):
        self.assertEqual(24, ploy(5))


if __name__ == "__main__":
    unittest.main()
