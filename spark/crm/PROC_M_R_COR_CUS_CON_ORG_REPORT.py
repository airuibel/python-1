#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_COR_CUS_CON_ORG_REPORT').setMaster(sys.argv[2])
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

MCRM_COR_CUS_CON_ORG_REPORT_BE = sqlContext.read.parquet(hdfs+'/MCRM_COR_CUS_CON_ORG_REPORT_BE/*')
MCRM_COR_CUS_CON_ORG_REPORT_BE.registerTempTable("MCRM_COR_CUS_CON_ORG_REPORT_BE")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT 
	    ORG_NAME
		,ORG_ID
		,CUST_ID
		,CUST_ZH_NAME
		,CUS_CONTRIBUTION_RMB
		,CRE_CONTRIBUTION_RMB
		,DEP_CONTRIBUTION_RMB
		,DEP_AMOUNT_RMB
		,CRE_AMOUNT_RMB
		,MAIN_TYPE
		,REPORT_DATE
		,CUST_TYP
		,MGR_ID
		,MGR_NAME
		,M_MAIN_TYPE
   FROM MCRM_COR_CUS_CON_ORG_REPORT_BE A                       --
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_COR_CUS_CON_ORG_REPORT = sqlContext.sql(sql)
MCRM_COR_CUS_CON_ORG_REPORT.registerTempTable("MCRM_COR_CUS_CON_ORG_REPORT")
dfn="MCRM_COR_CUS_CON_ORG_REPORT/"+V_DT+".parquet"
MCRM_COR_CUS_CON_ORG_REPORT.cache()
nrows = MCRM_COR_CUS_CON_ORG_REPORT.count()
MCRM_COR_CUS_CON_ORG_REPORT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_COR_CUS_CON_ORG_REPORT.unpersist()
MCRM_COR_CUS_CON_ORG_REPORT_BE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_COR_CUS_CON_ORG_REPORT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
