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
    ��ӡһ��Ŀ¼�µ������ļ��к��ļ�
    '''
    # �����ļ��У���һ���ֶ��Ǵ�Ŀ¼�ļ���
    dirList = []
    # �����ļ�
    fileList = []
    # ����һ���б����а�����Ŀ¼��Ŀ������(google����)
    files = os.listdir(path)
    # �����Ŀ¼����
    dirList.append(str(level))
    for f in files:
        if (os.path.isdir(path + '/' + f)):
            # �ų������ļ��С���Ϊ�����ļ��й���
            if (f[0] == '.'):
                pass
            else:
                # ��ӷ������ļ���
                dirList.append(f)
        if (os.path.isfile(path + '/' + f)):
            # ����ļ�
            fileList.append(f)
            # ��һ����־ʹ�ã��ļ����б��һ�����𲻴�ӡ
    i = 0
    file1 = open(path1 + str(today) + ".txt","a")
    for fl in fileList:
        # ��ӡ�ļ�

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
        # ������һ���ж��ٸ��ļ�
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