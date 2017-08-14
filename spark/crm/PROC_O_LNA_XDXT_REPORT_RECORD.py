#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_REPORT_RECORD').setMaster(sys.argv[2])
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

OCRM_O_ENT_REPORT_RECORD = sqlContext.read.parquet(hdfs+'/OCRM_O_ENT_REPORT_RECORD/*')
OCRM_O_ENT_REPORT_RECORD.registerTempTable("OCRM_O_ENT_REPORT_RECORD")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS BIGINT)                    AS ID 
       ,A.REPORTNO              AS REPORTNO 
       ,A.OBJECTTYPE            AS OBJECTTYPE 
       ,A.OBJECTNO              AS OBJECTNO 
       ,A.MODELNO               AS MODELNO 
       ,A.REPORTNAME            AS REPORTNAME 
       ,A.INPUTTIME             AS INPUTTIME 
       ,A.ORGID                 AS ORGID 
       ,A.USERID                AS USERID 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.VALIDFLAG             AS VALIDFLAG 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_O_ENT_REPORT_RECORD A                             --对公财务报表记录表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_ENT_REPORT_RECORD = sqlContext.sql(sql)
OCRM_F_ENT_REPORT_RECORD.registerTempTable("OCRM_F_ENT_REPORT_RECORD")
dfn="OCRM_F_ENT_REPORT_RECORD/"+V_DT+".parquet"
OCRM_F_ENT_REPORT_RECORD.cache()
nrows = OCRM_F_ENT_REPORT_RECORD.count()
OCRM_F_ENT_REPORT_RECORD.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_ENT_REPORT_RECORD.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_ENT_REPORT_RECORD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
