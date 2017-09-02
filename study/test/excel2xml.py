# coding=utf-8
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtCore import *
import sys
from xml.etree import ElementTree
import xlrd


class SelectDialog(QDialog):
    def __init__(self, parent=None):
        super(SelectDialog, self).__init__(parent)
        self.excelfile = None
        self.inxmlfile = None
        self.outxmlpath = None
        self.initUI()
        self.setWindowTitle("银监客户风险报文生成工具v0.1")
        self.resize(500, 300)

    def initUI(self):
        grid = QGridLayout()
        grid.addWidget(QLabel("excel文件："), 0, 0)
        self.pathLineEdit = QLineEdit()
        self.pathLineEdit.setFixedWidth(350)
        self.pathLineEdit.setText(None)
        grid.addWidget(self.pathLineEdit, 0, 1)
        button = QPushButton("打开")
        button.clicked.connect(self.changePath)
        grid.addWidget(button, 0, 2)

        grid.addWidget(QLabel("输入xml文件："), 1, 0)
        self.pathLineEdit1 = QLineEdit()
        self.pathLineEdit1.setFixedWidth(350)
        self.pathLineEdit1.setText(None)
        grid.addWidget(self.pathLineEdit1, 1, 1)
        button1 = QPushButton("打开")
        button1.clicked.connect(self.changePath1)
        grid.addWidget(button1, 1, 2)

        grid.addWidget(QLabel("输出xml文件："), 2, 0)
        self.pathLineEdit2 = QLineEdit()
        self.pathLineEdit2.setFixedWidth(350)
        self.pathLineEdit2.setText(None)
        grid.addWidget(self.pathLineEdit2, 2, 1)
        button2 = QPushButton("打开")
        button2.clicked.connect(self.changePath2)
        grid.addWidget(button2, 2, 2)

        buttonBox = QDialogButtonBox()
        buttonBox.setOrientation(Qt.Horizontal)  # 设置为水平方向
        buttonBox.setStandardButtons(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttonBox.accepted.connect(self.accept)  # 确定
        buttonBox.rejected.connect(self.reject)  # 取消
        grid.addWidget(buttonBox, 3, 1)
        self.setLayout(grid)

    def changePath(self):
        open = QFileDialog()
        self.excelfile = open.getOpenFileName()[0]
        self.pathLineEdit.setText(self.excelfile)

    def changePath1(self):
        open = QFileDialog()
        self.inxmlfile = open.getOpenFileName()[0]
        self.pathLineEdit1.setText(self.inxmlfile)

    def changePath2(self):
        open = QFileDialog()
        self.outxmlpath = open.getSaveFileName()[0]
        self.pathLineEdit2.setText(self.outxmlpath)


class Item(object):
    def __init__(self):
        self.com = None
        self.value = None

    def set_item(self, c, v):
        self.value = v
        self.com = c

    def get_com(self):
        return self.com

    def get_value(self):
        return self.value


def get_comp_list(excelfile):
    comp_list = []
    wb = xlrd.open_workbook(
        excelfile)  # 'D:\Test\银监客户风险：需要导入表3的明细预警信号表，并附相关导入数据上报文件\\1.需要导入表3的明细，的201708预警信号表.xls'
    sh = wb.sheet_by_name(u'Sheet1')
    for i in range(0, sh.nrows):
        item = Item()
        item.set_item(sh.cell_value(i, 0), sh.cell_value(i, 1))
        comp_list.append(item)
    return comp_list


def find_alarm(c, clist):
    for cl in clist:
        # print(cl.get_com(), cl.get_value())
        if cl.get_com() == c:
            return cl.get_value()
    return '\n    '


def main(excelfile, inxmlfile, outxmlpath):
    try:
        comp_list = get_comp_list(excelfile)
        flag = 0
        xmlDoc = ElementTree.parse(inxmlfile)  # 'D:\\Test\\fxyj03.xml'
        # step 2: 获取 根节点
        root = xmlDoc.getroot()
        # root.getchildren() 获取节点 返回的是列表
        gateServerNodeList = root.getchildren()
        # 下面是在每个gateServer 节点下 增加一个子节点
        for node in gateServerNodeList:
            for meta in node:
                if meta.tag == 'customerName':
                    flag = 1
                    customerName = meta.text
                if meta.tag == 'warningSignal' and flag == 1:
                    # print(customerName)
                    alarm_info = find_alarm(customerName, comp_list)
                    # print(alarm_info)
                    meta.text = alarm_info
                    flag = 0

        xmlDoc.write(outxmlpath, 'utf-8', True)  # outdir + '/out.xml'
    except Exception as e:
        print(e)

    return None


if __name__ == '__main__':
    app = QApplication(sys.argv)
    dialog = SelectDialog()
    if dialog.exec_():
        excelfile = dialog.excelfile.replace('/', '\\')
        inxmlfile = dialog.inxmlfile.replace('/', '\\')
        outxmlpath = dialog.outxmlpath.replace('/', '\\')
        main(excelfile, inxmlfile, outxmlpath)
