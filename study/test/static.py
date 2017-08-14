#static.py
import unittest

from study.test.widget import Widget


class WidgetTestCase(unittest.TestCase):
    def runTest(self):
        widget = Widget()
        self.assertEqual(widget.getSize(), (40, 40))

if __name__ == "__main__":
    testCase = WidgetTestCase()
    testCase.runTest()