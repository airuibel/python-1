# -*- coding: utf-8 -*-
import socket
import ibm_db

conn = ibm_db.connect("DATABASE=sample;HOSTNAME=66.3.44.37;PORT=60004;PROTOCOL=TCPIP;UID=cyrus;PWD=cyrus;", "", "")
sql = "SELECT PHONENO FROM EMPLOYEE"


def queryData(s):
    result = []
    if conn:
        try:
            stmt = ibm_db.exec_immediate(conn, s)
            dictionary = ibm_db.fetch_both(stmt)  # 提取结果
            while dictionary != False:
                for key in dictionary:
                    if key == 0:
                        result.append(dictionary[key])
                dictionary = ibm_db.fetch_both(stmt)
            return result
        except Exception as ex:
            print(ex)
        finally:
            ibm_db.close(conn)


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 9000))
print((s.recv(1024).decode()))

tupleOfquery = ('41588', '41939')
s.send((str(tupleOfquery)).encode())
print((s.recv(1024)).decode())

# res = queryData(sql)
# for data in res:
#     s.send((str(data)).encode())
#     print((s.recv(1024)).decode())

#s.send('exit'.encode())
s.close()
