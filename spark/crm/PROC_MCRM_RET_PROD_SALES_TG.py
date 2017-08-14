#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MCRM_RET_PROD_SALES_TG').setMaster(sys.argv[2])
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
MCRM_RET_PROD_SALES = sqlContext.read.parquet(hdfs+'/MCRM_RET_PROD_SALES/*')
MCRM_RET_PROD_SALES.registerTempTable("MCRM_RET_PROD_SALES")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
#目标表
#MCRM_RET_PROD_SALES_TG 增量表

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT  
		 CAST(SUM(S.CNY_BAL) AS DECIMAL(24,6))   	AS BAL 
		,CAST(SUM(S.LAST_MONTH_CNY_BAL) AS DECIMAL(24,6))                        AS LAST_BAL
		,CAST(CASE WHEN SUM(S.CNY_BAL - S.LAST_MONTH_CNY_BAL) < 0 THEN 0 ELSE SUM(S.CNY_BAL - S.LAST_MONTH_CNY_BAL) END AS DECIMAL(24,6))  AS ADD_CUST_BAL
		,CAST(CASE WHEN SUM(S.LAST_MONTH_CNY_BAL - S.CNY_BAL) < 0 THEN 0 ELSE SUM(S.LAST_MONTH_CNY_BAL - S.CNY_BAL) END AS DECIMAL(24,6))  AS LOSE_CUST_BAL 
		,S.PROD_NAME             AS PROD_NAME 
		,S.ORG_ID                AS ORG_ID
		,S.CUST_LEVEL            AS CUST_LEVEL 
		,S.ST_DATE               AS ST_DATE
   FROM MCRM_RET_PROD_SALES S                                  --
   INNER JOIN ADMIN_AUTH_ORG A                                  --
     ON S.ORG_ID                = A.ORG_ID 
  WHERE S.O_MAIN_TYPE           = '1' 
  GROUP BY S.ORG_ID 
       ,S.PROD_NAME 
       ,S.CUST_LEVEL 
       ,S.ST_DATE 
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_PROD_SALES_TG = sqlContext.sql(sql)
MCRM_RET_PROD_SALES_TG.registerTempTable("MCRM_RET_PROD_SALES_TG")
dfn="MCRM_RET_PROD_SALES_TG/"+V_DT+".parquet"
MCRM_RET_PROD_SALES_TG.cache()
nrows = MCRM_RET_PROD_SALES_TG.count()
MCRM_RET_PROD_SALES_TG.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_RET_PROD_SALES_TG.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_RET_PROD_SALES_TG lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
