#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_SLNA_SUN_CUSTOMER_FSRECORD').setMaster(sys.argv[2])
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

O_CI_SUN_CUSTOMER_FSRECORD = sqlContext.read.parquet(hdfs+'/O_CI_SUN_CUSTOMER_FSRECORD/*')
O_CI_SUN_CUSTOMER_FSRECORD.registerTempTable("O_CI_SUN_CUSTOMER_FSRECORD")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUSTOMERID            AS CUSTOMERID 
       ,A.RECORDNO              AS RECORDNO 
       ,A.REPORTDATE            AS REPORTDATE 
       ,A.REPORTSCOPE           AS REPORTSCOPE 
       ,A.REPORTPERIOD          AS REPORTPERIOD 
       ,A.REPORTCURRENCY        AS REPORTCURRENCY 
       ,A.REPORTUNIT            AS REPORTUNIT 
       ,A.REPORTSTATUS          AS REPORTSTATUS 
       ,A.REPORTFLAG            AS REPORTFLAG 
       ,A.REPORTOPINION         AS REPORTOPINION 
       ,A.AUDITFLAG             AS AUDITFLAG 
       ,A.AUDITOFFICE           AS AUDITOFFICE 
       ,A.AUDITOPINION          AS AUDITOPINION 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.ORGID                 AS ORGID 
       ,A.USERID                AS USERID 
       ,A.UPDATEDATE            AS UPDATEDATE 
       ,A.REMARK                AS REMARK 
       ,A.REPORTLOCKED          AS REPORTLOCKED 
       ,A.MODELCLASS            AS MODELCLASS 
       ,A.FARMILYID             AS FARMILYID 
       ,A.CORPORATEORGID        AS CORPORATEORGID 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'SLNA'                  AS ODS_SYS_ID 
   FROM O_CI_SUN_CUSTOMER_FSRECORD A                           --阳光信贷客户财务报表（CF）
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_SUN_CUSTOMER_FSRECORD = sqlContext.sql(sql)
F_CI_SUN_CUSTOMER_FSRECORD.registerTempTable("F_CI_SUN_CUSTOMER_FSRECORD")
dfn="F_CI_SUN_CUSTOMER_FSRECORD/"+V_DT+".parquet"
F_CI_SUN_CUSTOMER_FSRECORD.cache()
nrows = F_CI_SUN_CUSTOMER_FSRECORD.count()
F_CI_SUN_CUSTOMER_FSRECORD.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_SUN_CUSTOMER_FSRECORD.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_SUN_CUSTOMER_FSRECORD/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_SUN_CUSTOMER_FSRECORD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
