#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUSTOMER_FSRECORD').setMaster(sys.argv[2])
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

#----------来源表---------------
F_CI_XDXT_CUSTOMER_FSRECORD = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_FSRECORD/*')
F_CI_XDXT_CUSTOMER_FSRECORD.registerTempTable("F_CI_XDXT_CUSTOMER_FSRECORD")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT B.ODS_CUST_ID           AS CUST_ID 
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
       ,''                    AS UPDATEDATE 
       ,A.REMARK                AS REMARK 
       ,A.REPORTLOCKED          AS REPORTLOCKED 
       ,A.MODELCLASS            AS MODELCLASS 
       ,''                    AS FARMILYID 
       ,A.FR_ID                 AS CORPORATEORGID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,monotonically_increasing_id() AS ID 
   FROM F_CI_XDXT_CUSTOMER_FSRECORD A                          --客户财务报表（CF）
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.CUSTOMERID            = B.SOURCE_CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ODS_SYS_ID            = 'LNA' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUSTOMER_FSRECORD = sqlContext.sql(sql)
OCRM_F_CI_CUSTOMER_FSRECORD.registerTempTable("OCRM_F_CI_CUSTOMER_FSRECORD")
dfn="OCRM_F_CI_CUSTOMER_FSRECORD/"+V_DT+".parquet"
OCRM_F_CI_CUSTOMER_FSRECORD.cache()
nrows = OCRM_F_CI_CUSTOMER_FSRECORD.count()
OCRM_F_CI_CUSTOMER_FSRECORD.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUSTOMER_FSRECORD.unpersist()
F_CI_XDXT_CUSTOMER_FSRECORD.unpersist()
OCRM_F_CI_SYS_RESOURCE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUSTOMER_FSRECORD/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUSTOMER_FSRECORD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
