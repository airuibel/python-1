# -*- coding:gbk -*-

import os
import sys
import datetime
import re
allFileNum = 0
today = datetime.date.today()


def printPath(level, path, path1):
    global allFileNum
    '''''
    打印一个目录下的所有文件夹和文件
    '''
    # 所有文件夹，第一个字段是次目录的级别
    dirList = []
    # 所有文件
    fileList = []
    # 返回一个列表，其中包含在目录条目的名称(google翻译)
    files = os.listdir(path)
    # 先添加目录级别
    dirList.append(str(level))
    for f in files:
        if (os.path.isdir(path + '/' + f)):
            # 排除隐藏文件夹。因为隐藏文件夹过多
            if (f[0] == '.'):
                pass
            else:
                # 添加非隐藏文件夹
                dirList.append(f)
        if (os.path.isfile(path + '/' + f)):
            # 添加文件
            fileList.append(f)
            # 当一个标志使用，文件夹列表第一个级别不打印
    i = 0
    file1 = open(path1 + str(today) + ".txt","a")
    for fl in fileList:
        # 打印文件

        file = open(path + fl)
        j = 0
        if (i == 0):
            line = file.read()
            line = line.replace(" ","").replace("\\t","").replace(chr(9),'')
            file1.write(line)
            print file1
        else:
            line = file.readline()
            while line:
                if (j == 0):
                    print line
                    pass
                else:
                    line = line.replace(" ","").replace("\\t","").replace(chr(9),'')
                    line = line.split(',')
                    print len(line[2])
                    if(len(line[2]) == 11):
                        tmp = line[2]
                        str11 = tmp[0:8]
                        str21 = tmp[9:]
                        line[2] = str11 + '-' + str21
#                        print line[2]
                    line = ','.join(line)
                    file1.writelines(line)
                j+=1
                line = file.readline()
        file.close()
        print fl
        i += 1
        print i
        # 随便计算一下有多少个文件
        allFileNum = allFileNum + 1
    file1.close()


if __name__ == '__main__':
#    directory = sys.argv[1]
    src_dir = 'C:\Users\Administrator\Desktop\\20170531\\'
    des_dir = 'C:\Users\Administrator\Desktop\\1\\'
#    src_dir = sys.argv[1]
#    des_dir = sys.argv[2]
    printPath(1,src_dir,des_dir )
    print 'total number of files is ', allFileNum