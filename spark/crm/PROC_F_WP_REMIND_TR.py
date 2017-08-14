#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_WP_REMIND_TR').setMaster(sys.argv[2])
sc = SparkContext(conf = conf)
sc.setLogLevel('WARN')
if len(sys.argv) > 5:
    if sys.argv[5] == "hive":
        sqlContext = HiveContext(sc)
else:
    sqlContext = SQLContext(sc)
hdfs = sys.argv[3]
dbname = sys.argv[4]

#处理需要使用的日期
etl_date = sys.argv[1]
#etl日期
V_DT = etl_date  
#上一日日期
V_DT_LD = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8])) + timedelta(-1)).strftime("%Y%m%d")
#月初日期
V_DT_FMD = date(int(etl_date[0:4]), int(etl_date[4:6]), 1).strftime("%Y%m%d") 
#上月末日期
V_DT_LMD = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1) + timedelta(-1)).strftime("%Y%m%d")
#10位日期
V_DT10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8]))).strftime("%Y-%m-%d")
V_STEP = 0

OCRM_F_WP_REMIND_BIRTHDAY = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_BIRTHDAY/*')
OCRM_F_WP_REMIND_BIRTHDAY.registerTempTable("OCRM_F_WP_REMIND_BIRTHDAY")
OCRM_F_WP_REMIND_BIGCHANGE = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_BIGCHANGE/*')
OCRM_F_WP_REMIND_BIGCHANGE.registerTempTable("OCRM_F_WP_REMIND_BIGCHANGE")
OCRM_F_WP_REMIND_FINANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_FINANCE/*')
OCRM_F_WP_REMIND_FINANCE.registerTempTable("OCRM_F_WP_REMIND_FINANCE")
OCRM_F_WP_REMIND_LOANINT = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_LOANINT/*')
OCRM_F_WP_REMIND_LOANINT.registerTempTable("OCRM_F_WP_REMIND_LOANINT")
OCRM_F_WP_REMIND_APCL_GUADEP = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_APCL_GUADEP/*')
OCRM_F_WP_REMIND_APCL_GUADEP.registerTempTable("OCRM_F_WP_REMIND_APCL_GUADEP")
OCRM_F_WP_REMIND_OVER_GUADEP = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_OVER_GUADEP/*')
OCRM_F_WP_REMIND_OVER_GUADEP.registerTempTable("OCRM_F_WP_REMIND_OVER_GUADEP")
OCRM_F_WP_REMIND_OVERDUE_DEP = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_OVERDUE_DEP/*')
OCRM_F_WP_REMIND_OVERDUE_DEP.registerTempTable("OCRM_F_WP_REMIND_OVERDUE_DEP")
OCRM_F_WP_REMIND_OVERDUELOAN = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_OVERDUELOAN/*')
OCRM_F_WP_REMIND_OVERDUELOAN.registerTempTable("OCRM_F_WP_REMIND_OVERDUELOAN")
OCRM_F_WP_REMIND_LEVELCHANGE = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_LEVELCHANGE/*')
OCRM_F_WP_REMIND_LEVELCHANGE.registerTempTable("OCRM_F_WP_REMIND_LEVELCHANGE")
OCRM_F_WP_REMIND_LOAN_EXPIRE = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_LOAN_EXPIRE/*')
OCRM_F_WP_REMIND_LOAN_EXPIRE.registerTempTable("OCRM_F_WP_REMIND_LOAN_EXPIRE")
OCRM_F_WP_REMIND_TERM = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_TERM/*')
OCRM_F_WP_REMIND_TERM.registerTempTable("OCRM_F_WP_REMIND_TERM")
OCRM_F_WP_REMIND_SAACCT = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_SAACCT/*')
OCRM_F_WP_REMIND_SAACCT.registerTempTable("OCRM_F_WP_REMIND_SAACCT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000101'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_BIGCHANGE A                           --客户账户大额提醒信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000102'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_LEVELCHANGE A                         --客户升降级变化提醒信息表
  WHERE REMIND_FLAG             = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000103'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_BIRTHDAY A                            --客户生日提醒信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000104'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_TERM A                                --定期存款到期提醒信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000105'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_OVERDUELOAN A                         --贷款逾期提醒信息表
  WHERE REMIND_FLAG             = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-06::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000106'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_LOANINT A                             --贷款欠息提醒信息表
  WHERE REMIND_FLAG             = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-07::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000108'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_LOAN_EXPIRE A                         --贷款产品到期提醒信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-08::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000109'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_FINANCE A                             --理财产品到期提醒信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-09::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000110'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_SAACCT A                              --贷款已核销客户存款账户变动提醒信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-10::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000111'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_OVERDUE_DEP A                         --逾期贷款客户存款账户变动提醒信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-11::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000112'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_OVER_GUADEP A                         --逾期贷款客户担保人存款账户变动提醒信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-12::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,'A000113'               AS MSG_TYP 
       ,''                    AS MSG_STS 
       ,FR_ID                   AS ORG_ID 
       ,RULE_ID                 AS RULE_ID 
       ,CUST_ID                 AS CUST_ID 
       ,MSG_CRT_DATE            AS MSG_CRT_DATE 
       ,''                    AS MSG_END_DATE 
       ,''                    AS READ_DATE 
       ,''                    AS CUST_NAME 
   FROM OCRM_F_WP_REMIND_APCL_GUADEP A                         --核销贷款客户担保人存款账户变动提醒信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND = sqlContext.sql(sql)
OCRM_F_WP_REMIND.registerTempTable("OCRM_F_WP_REMIND")
dfn="OCRM_F_WP_REMIND/"+V_DT+".parquet"
OCRM_F_WP_REMIND.cache()
nrows = OCRM_F_WP_REMIND.count()
OCRM_F_WP_REMIND.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
