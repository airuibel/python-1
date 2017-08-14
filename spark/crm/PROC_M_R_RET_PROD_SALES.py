#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_RET_PROD_SALES').setMaster(sys.argv[2])
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
MCRM_RET_PROD_SALES_LC = sqlContext.read.parquet(hdfs+'/MCRM_RET_PROD_SALES_LC/*')
MCRM_RET_PROD_SALES_LC.registerTempTable("MCRM_RET_PROD_SALES_LC")
MCRM_RET_PROD_SALES_DK = sqlContext.read.parquet(hdfs+'/MCRM_RET_PROD_SALES_DK/*')
MCRM_RET_PROD_SALES_DK.registerTempTable("MCRM_RET_PROD_SALES_DK")
#目标表
#MCRM_RET_PROD_SALES 增量表


#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT  
	     CUST_ID
		,CUST_ZH_NAME
		,CUST_MANAGER
		,CUST_MANAGER_NAME
		,ORG_ID
		,ORG_NAME
		,CUST_LEVEL
		,PROD_ID
		,PROD_NAME
		,cast(CATL_CODE as string) as CATL_CODE
		,CURR
		,LAST_MONTH_BAL
		,LAST_MONTH_CNY_BAL
		,BAL
		,CNY_BAL
		,V_DT as ST_DATE
		,O_MAIN_TYPE
		,M_MAIN_TYPE
		,FR_ID
		,'DL' as SIGN   
   FROM MCRM_RET_PROD_SALES_DK A                               --
  WHERE ST_DATE = V_DT 
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_PROD_SALES1 = sqlContext.sql(sql)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT  
	     CUST_ID
		,CUST_ZH_NAME
		,CUST_MANAGER
		,CUST_MANAGER_NAME
		,ORG_ID
		,ORG_NAME
		,CUST_LEVEL
		,PROD_ID
		,PROD_NAME
		,CATL_CODE
		,CURR
		,LAST_MONTH_BAL
		,LAST_MONTH_CNY_BAL
		,BAL
		,CNY_BAL
		,V_DT as ST_DATE
		,O_MAIN_TYPE
		,M_MAIN_TYPE
		,FR_ID
		,'DL' as SIGN   
   FROM MCRM_RET_PROD_SALES_LC A                               --
  WHERE ST_DATE = V_DT 
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_PROD_SALES2 = sqlContext.sql(sql)

MCRM_RET_PROD_SALES = MCRM_RET_PROD_SALES1.unionAll(MCRM_RET_PROD_SALES2)
dfn="MCRM_RET_PROD_SALES/"+V_DT+".parquet"
MCRM_RET_PROD_SALES.write.save(path=hdfs + '/' + dfn, mode='overwrite')
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
