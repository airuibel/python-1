#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_INOUTCOME').setMaster(sys.argv[2])
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

#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
OCRM_F_CUST_ORG_MGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CUST_ORG_MGR/*')
OCRM_F_CUST_ORG_MGR.registerTempTable("OCRM_F_CUST_ORG_MGR")
ACRM_A_INOUTCOME = sqlContext.read.parquet(hdfs+'/ACRM_A_INOUTCOME/*')
ACRM_A_INOUTCOME.registerTempTable("ACRM_A_INOUTCOME")
#目标表
#MCRM_A_R_INOUTCOME 增量表


#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT 
     A.CUST_ID               AS CUST_ID 
	,B.CUST_NAME             AS CUST_NAME
	,B.CUST_TYP              AS CUST_TYPE
	,A.BAL                   AS BAL	 
	,A.ORG_ID                AS ORG_ID  
	,B.ORG_NAME              AS ORG_NAME  
	,B.MGR_ID                AS MGR_ID 
	,B.MGR_NAME              AS MGR_NAME
	,V_DT                    AS REPORT_DATE 
	,A.FR_ID                 AS FR_ID 
   FROM ACRM_A_INOUTCOME A                                     --
  INNER JOIN OCRM_F_CUST_ORG_MGR B                             --
     ON A.FR_ID                 = B.FR_ID 
    AND A.ORG_ID                = B.ORG_ID 
    AND A.CUST_ID               = B.CUST_ID 
  WHERE A.ODS_DATE              = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_A_R_INOUTCOME = sqlContext.sql(sql)
MCRM_A_R_INOUTCOME.registerTempTable("MCRM_A_R_INOUTCOME")
dfn="MCRM_A_R_INOUTCOME/"+V_DT+".parquet"
MCRM_A_R_INOUTCOME.cache()
nrows = MCRM_A_R_INOUTCOME.count()
MCRM_A_R_INOUTCOME.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_A_R_INOUTCOME.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_A_R_INOUTCOME lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
