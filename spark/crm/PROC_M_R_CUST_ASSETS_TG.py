#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_CUST_ASSETS_TG').setMaster(sys.argv[2])
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
MCRM_RET_CUST_ASSETS = sqlContext.read.parquet(hdfs+'/MCRM_RET_CUST_ASSETS/*')
MCRM_RET_CUST_ASSETS.registerTempTable("MCRM_RET_CUST_ASSETS")
#目标表
#MCRM_RET_CUST_ASSETS_TG 增量表

#任务[11] 001-01::
V_STEP = V_STEP + 1
sql = """
 SELECT  
	     ORG_ID                  AS ORG_ID
		,ORG_NAME                AS ORG_NAME
		,CUST_LEVEL              AS CUST_LEVEL
		,CUST_MANAGER            AS CUST_MANAGER
		,CUST_MANAGER_NAME       AS CUST_MANAGER_NAME
		,cast(COUNT(CUST_ID) as string)          AS C_CUST_ID
		,cast(SUM(MONTH_BAL) as DECIMAL(24,6))                  	AS MONTH_BAL
		,cast(SUM(MONTH_AVG_BAL) as DECIMAL(24,6))                  AS MONTH_AVG_BAL 
		,cast(SUM(THREE_MONTH_AVG_BAL) as DECIMAL(24,6))                      	 AS THREE_MONTH_AVG_BAL
		,cast((SUM(MONTH_BAL) - SUM(LAST_MONTH_BAL)) as DECIMAL(24,6))           AS LAST_MONTH_BAL
		,cast((SUM(MONTH_AVG_BAL) - SUM(LAST_MONTH_AVG_BAL)) as DECIMAL(24,6))   AS LAST_MONTH_AVG_BAL
		,cast((SUM(MONTH_AVG_BAL) - SUM(LTHREE_MONTH_AVG_BAL)) as DECIMAL(24,6)) AS LTHREE_MONTH_AVG_BAL
		,cast((SUM(MONTH_BAL) - SUM(YEAR_BAL)) as DECIMAL(24,6))                 AS YEAR_BAL
		,cast((SUM(MONTH_AVG_BAL) - SUM(YEAR_AVG_BAL)) as DECIMAL(24,6))         AS YEAR_AVG_BAL
		,cast((SUM(MONTH_AVG_BAL) - SUM(YEAR_THREE_AVG_BAL)) as DECIMAL(24,6))   AS YEAR_THREE_AVG_BAL
		,V_DT                    AS ST_DATE 
   FROM MCRM_RET_CUST_ASSETS A                                 --
  WHERE O_MAIN_TYPE             = '1' 
  GROUP BY ORG_ID 
       ,ORG_NAME 
       ,CUST_LEVEL 
       ,CUST_MANAGER 
       ,CUST_MANAGER_NAME """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_CUST_ASSETS_TG = sqlContext.sql(sql)
dfn="MCRM_RET_CUST_ASSETS_TG/"+V_DT+".parquet"
MCRM_RET_CUST_ASSETS_TG.cache()
nrows = MCRM_RET_CUST_ASSETS_TG.count()
MCRM_RET_CUST_ASSETS_TG.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_RET_CUST_ASSETS_TG.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_RET_CUST_ASSETS_TG lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
