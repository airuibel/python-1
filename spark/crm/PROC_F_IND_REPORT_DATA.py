#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_IND_REPORT_DATA').setMaster(sys.argv[2])
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

F_CI_XDXT_IND_REPORT_DATA = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_REPORT_DATA/*')
F_CI_XDXT_IND_REPORT_DATA.registerTempTable("F_CI_XDXT_IND_REPORT_DATA")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT REPORTNO                AS REPORTNO 
       ,ROWNO                   AS ROWNO 
       ,ROWNAME                 AS ROWNAME 
       ,ROWSUBJECT              AS ROWSUBJECT 
       ,DISPLAYORDER            AS DISPLAYORDER 
       ,ROWDIMTYPE              AS ROWDIMTYPE 
       ,ROWATTRIBUTE            AS ROWATTRIBUTE 
       ,CAST(COL1VALUE    AS DECIMAL(24))           AS COL1VALUE 
       ,CAST(COL2VALUE    AS DECIMAL(24))           AS COL2VALUE 
       ,CAST(COL3VALUE   AS DECIMAL(24))            AS COL3VALUE 
       ,CAST(COL4VALUE   AS DECIMAL(24))            AS COL4VALUE 
       ,CAST(STANDARDVALUE  AS DECIMAL(24))         AS STANDARDVALUE 
       ,V_DT                  AS ODS_ST_DATE 
   FROM F_CI_XDXT_IND_REPORT_DATA A                            --个人财务报表详细数据表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_IND_REPORT_DATA = sqlContext.sql(sql)
OCRM_F_IND_REPORT_DATA.registerTempTable("OCRM_F_IND_REPORT_DATA")
dfn="OCRM_F_IND_REPORT_DATA/"+V_DT+".parquet"
OCRM_F_IND_REPORT_DATA.cache()
nrows = OCRM_F_IND_REPORT_DATA.count()
OCRM_F_IND_REPORT_DATA.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_IND_REPORT_DATA.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_IND_REPORT_DATA/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_IND_REPORT_DATA lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
