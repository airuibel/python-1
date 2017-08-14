# -*- coding: utf-8 -*-
import ibm_db


def queryDatabatch(tupleOfquery):
    conn = ibm_db.connect(
        "DATABASE=sample;HOSTNAME=66.3.44.37;PORT=60004;PROTOCOL=TCPIP;UID=cyrus;PWD=cyrus;", "", "")
    if conn:
        try:
            result = []
            sql = "SELECT PHONENO FROM EMPLOYEE WHERE EMPNO IN " + (str(tupleOfquery))
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


tuplel = ('41588', '41939')
str1 = str(tuplel)
str2 = 'b"' + str1 + '"'

print(str1[2:str1.__len__() - 1])
queryDatabatch(('41588', '41939'))
