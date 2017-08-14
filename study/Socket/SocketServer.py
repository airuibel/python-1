# -*- coding: utf-8 -*-
import socket
import threading
import time
import ibm_db


def tcplink(sock, addr):
    print('Accept new connection from %s:%s ' % addr)
    sock.send(('Welcome').encode())
    while True:
        data = sock.recv(1024)
        time.sleep(0.1)
        if data == 'exit' or not data:
            break
        restr = querydata(data)
        sock.send(('Hello, %s, %s!' % restr).encode())
    sock.close()
    print('Connection from %s:%s closed' % addr)

def tcplinbatch(sock, addr):
    print('Accept new connection from %s:%s ' % addr)
    sock.send(('Welcome').encode())
    while True:
        data = sock.recv(1024)
        time.sleep(0.1)
        if data == 'exit' or not data:
            break
        restr = querydatabatch(data)
        sock.send(str(restr).encode())
    sock.close()
    print('Connection from %s:%s closed' % addr)


def querydata(str):
    conn = ibm_db.connect("DATABASE=sample;HOSTNAME=66.3.44.37;PORT=60004;PROTOCOL=TCPIP;UID=cyrus;PWD=cyrus;", "", "")
    if conn:
        try:
            sql = "SELECT * FROM EMPLOYEE WHERE PHONENO= ?"
            stmt = ibm_db.prepare(conn, sql)  # 预编译
            s = (str,)
            ibm_db.execute(stmt, s)  # 执行
            result = ibm_db.fetch_both(stmt)
            return str, result[0]
        except Exception as ex:
            print(ex)
        finally:
            ibm_db.close(conn)


def querydatabatch(tupleOfquery):
    conn = ibm_db.connect("DATABASE=sample;HOSTNAME=66.3.44.37;PORT=60004;PROTOCOL=TCPIP;UID=cyrus;PWD=cyrus;", "", "")
    if conn:
        try:
            result = []
            str1 = str(tupleOfquery)
            sql = "SELECT PHONENO FROM EMPLOYEE WHERE EMPNO IN " + (str1[2:str1.__len__()-1])
            print(sql)
            stmt = ibm_db.exec_immediate(conn, sql)
            dictionary = ibm_db.fetch_assoc(stmt)
            while dictionary != False:
                for key in dictionary:
                    if key == 'PHONENO':
                        result.append(dictionary[key])
                dictionary = ibm_db.fetch_assoc(stmt)
            print(result)
            return result
        except Exception as ex:
            print(ex)
        finally:
            ibm_db.close(conn)


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('127.0.0.1', 9000))
s.listen(5)

print('Waiting for connection')

while True:
    sock, addr = s.accept()
    t = threading.Thread(target=tcplinbatch, args=(sock, addr))
    t.start()
