#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_REPORT_MODEL').setMaster(sys.argv[2])
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

OCRM_O_CI_REPORT_MODEL = sqlContext.read.parquet(hdfs+'/OCRM_O_CI_REPORT_MODEL/*')
OCRM_O_CI_REPORT_MODEL.registerTempTable("OCRM_O_CI_REPORT_MODEL")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS BIGINT)                    AS ID 
       ,A.MODELNO               AS MODELNO 
       ,A.ROWNO                 AS ROWNO 
       ,A.ROWNAME               AS ROWNAME 
       ,A.ROWSUBJECT            AS ROWSUBJECT 
       ,A.DISPLAYORDER          AS DISPLAYORDER 
       ,A.ROWDIMTYPE            AS ROWDIMTYPE 
       ,A.ROWATTRIBUTE          AS ROWATTRIBUTE 
       ,A.COL1DEF               AS COL1DEF 
       ,A.COL2DEF               AS COL2DEF 
       ,A.COL3DEF               AS COL3DEF 
       ,A.COL4DEF               AS COL4DEF 
       ,A.STANDARDVALUE         AS STANDARDVALUE 
       ,A.DELETEFLAG            AS DELETEFLAG 
       ,A.FORMULAEXP1           AS FORMULAEXP1 
       ,A.FORMULAEXP2           AS FORMULAEXP2 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_O_CI_REPORT_MODEL A                               --财务报表模型表(公共)
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_REPORT_MODEL = sqlContext.sql(sql)
OCRM_F_CI_REPORT_MODEL.registerTempTable("OCRM_F_CI_REPORT_MODEL")
dfn="OCRM_F_CI_REPORT_MODEL/"+V_DT+".parquet"
OCRM_F_CI_REPORT_MODEL.cache()
nrows = OCRM_F_CI_REPORT_MODEL.count()
OCRM_F_CI_REPORT_MODEL.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_REPORT_MODEL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_REPORT_MODEL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
