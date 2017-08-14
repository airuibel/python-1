#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_CUST_CONT_TMP').setMaster(sys.argv[2])
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

ACRM_F_CI_CUST_CONTRIBUTION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_CONTRIBUTION/*')
ACRM_F_CI_CUST_CONTRIBUTION.registerTempTable("ACRM_F_CI_CUST_CONTRIBUTION")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT  
	 CUST_ID                 AS CUST_ID
	,CUST_NAME               AS CUST_NAME 
	,CAST(SUM(COALESCE(CRE_BAL_RMB, 0)) as DECIMAL(24,6))           AS CRE_BAL_RMB 
	,CAST(SUM(COALESCE(DEP_BAL_RMB, 0)) as DECIMAL(24,6))           AS DEP_BAL_RMB
	,CAST(SUM(COALESCE(CUST_CONTRIBUTION, 0)) as DECIMAL(24,6))     AS CUST_CONTRIBUTION
	,CAST(SUM(COALESCE(CRE_CONTRIBUTION, 0))  as DECIMAL(24,6))     AS CRE_CONTRIBUTION 
	,CAST(SUM(COALESCE(DEP_CONTRIBUTION, 0))  as DECIMAL(24,6))     AS DEP_CONTRIBUTION
	,FR_ID                   AS FR_ID 	   
   FROM ACRM_F_CI_CUST_CONTRIBUTION A                          --
  WHERE ODS_DATE                = V_DT 
    AND CUST_TYP                = '2' 
  GROUP BY CUST_ID 
       ,CUST_NAME 
       ,FR_ID 
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_CONT_TMP = sqlContext.sql(sql)
ACRM_F_CI_CUST_CONT_TMP.registerTempTable("ACRM_F_CI_CUST_CONT_TMP")
dfn="ACRM_F_CI_CUST_CONT_TMP/"+V_DT+".parquet"
ACRM_F_CI_CUST_CONT_TMP.cache()
nrows = ACRM_F_CI_CUST_CONT_TMP.count()
ACRM_F_CI_CUST_CONT_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_CONT_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_CONT_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_CONT_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
