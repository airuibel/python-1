# -*- coding: utf-8 -*-
import ibm_db
import pandas as pd
conn = ibm_db.connect("dsn=sample", "administrator", "ctl7220fe")
sql = "SELECT EMPNO, LASTNAME FROM EMPLOYEE WHERE EMPNO > ? AND EMPNO < ?"
stmt = ibm_db.prepare(conn, sql)
max = 50
min = 0
# Explicitly bind parameters
ibm_db.bind_param(stmt, 1, min)
ibm_db.bind_param(stmt, 2, max)
ibm_db.execute(stmt)
# Process results
sql = "SELECT * FROM EMPLOYEE"
stmt = ibm_db.exec_immediate(conn, sql)
dictionary = ibm_db.fetch_both(stmt)
df = pd.DataFrame(dictionary.items())
print (df , '-----------------------------------')
while dictionary != False:
    for key in dictionary:
        print(key, dictionary[key])
    dictionary = ibm_db.fetch_both(stmt)
sql1 = "update A1 set N = ?"
stmt1 = ibm_db.prepare(conn, sql1)
i = 'scy'
ibm_db.bind_param(stmt1, 1, i)
ibm_db.execute(stmt1)





#
