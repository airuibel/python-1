#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_IND_REPORT_DATA').setMaster(sys.argv[2])
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

O_CI_XDXT_IND_REPORT_DATA = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_IND_REPORT_DATA/*')
O_CI_XDXT_IND_REPORT_DATA.registerTempTable("O_CI_XDXT_IND_REPORT_DATA")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.REPORTNO              AS REPORTNO 
       ,A.ROWNO                 AS ROWNO 
       ,A.ROWNAME               AS ROWNAME 
       ,A.ROWSUBJECT            AS ROWSUBJECT 
       ,A.DISPLAYORDER          AS DISPLAYORDER 
       ,A.ROWDIMTYPE            AS ROWDIMTYPE 
       ,A.ROWATTRIBUTE          AS ROWATTRIBUTE 
       ,A.COL1VALUE             AS COL1VALUE 
       ,A.COL2VALUE             AS COL2VALUE 
       ,A.COL3VALUE             AS COL3VALUE 
       ,A.COL4VALUE             AS COL4VALUE 
       ,A.STANDARDVALUE         AS STANDARDVALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_IND_REPORT_DATA A                            --个人财务报表详细数据表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_IND_REPORT_DATA = sqlContext.sql(sql)
F_CI_XDXT_IND_REPORT_DATA.registerTempTable("F_CI_XDXT_IND_REPORT_DATA")
dfn="F_CI_XDXT_IND_REPORT_DATA/"+V_DT+".parquet"
F_CI_XDXT_IND_REPORT_DATA.cache()
nrows = F_CI_XDXT_IND_REPORT_DATA.count()
F_CI_XDXT_IND_REPORT_DATA.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_IND_REPORT_DATA.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_IND_REPORT_DATA lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
