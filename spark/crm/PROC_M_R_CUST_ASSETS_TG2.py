#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_CUST_ASSETS_TG2').setMaster(sys.argv[2])
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
#MCRM_RET_CUST_ASSETS_TG2 增量表


#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT 
    ORG_ID, ORG_NAME,
	CAST(COUNT(CUST_ID) as string) as C_CUST_ID,
	CASE WHEN s.MONTH_BAL < 10000 THEN '1'
         WHEN s.MONTH_BAL < 30000 AND s.MONTH_BAL >= 10000 THEN '2'
         WHEN s.MONTH_BAL < 50000 AND s.MONTH_BAL >= 30000 THEN '3'
         WHEN s.MONTH_BAL < 100000 AND s.MONTH_BAL >= 50000 THEN '4'
         WHEN s.MONTH_BAL < 300000 AND s.MONTH_BAL >= 100000 THEN '5'
         WHEN s.MONTH_BAL < 500000 AND s.MONTH_BAL >= 300000 THEN '6'
         WHEN s.MONTH_BAL < 2000000 AND s.MONTH_BAL >= 500000 THEN '7'
         WHEN s.MONTH_BAL < 5000000 AND s.MONTH_BAL >= 2000000 THEN '8'
         WHEN s.MONTH_BAL < 10000000 AND s.MONTH_BAL >= 5000000 THEN '9'
         ELSE '99' END as BAL_PART,
	CASE WHEN s.MONTH_AVG_BAL < 10000 THEN '1'
         WHEN s.MONTH_AVG_BAL < 30000  AND s.MONTH_AVG_BAL >= 10000 THEN '2'
         WHEN s.MONTH_AVG_BAL < 50000 AND s.MONTH_AVG_BAL >= 30000 THEN '3'
         WHEN s.MONTH_AVG_BAL < 100000 AND s.MONTH_AVG_BAL >= 50000 THEN '4'
         WHEN s.MONTH_AVG_BAL < 300000 AND s.MONTH_AVG_BAL >= 100000 THEN '5'
         WHEN s.MONTH_AVG_BAL < 500000 AND s.MONTH_AVG_BAL >= 300000 THEN '6'
         WHEN s.MONTH_AVG_BAL < 2000000  AND s.MONTH_AVG_BAL >= 500000 THEN '7'
         WHEN s.MONTH_AVG_BAL < 5000000 AND s.MONTH_AVG_BAL >= 2000000 THEN '8'
         WHEN s.MONTH_AVG_BAL < 10000000 AND s.MONTH_AVG_BAL >= 5000000 THEN '9'
         ELSE '99' END as AVG_BAL_PART,
	CASE WHEN s.THREE_MONTH_AVG_BAL < 10000 THEN '1'
         WHEN s.THREE_MONTH_AVG_BAL < 30000  AND s.THREE_MONTH_AVG_BAL >= 10000 THEN '2'
         WHEN s.THREE_MONTH_AVG_BAL < 50000 AND s.THREE_MONTH_AVG_BAL >= 30000 THEN '3'
         WHEN s.THREE_MONTH_AVG_BAL < 100000 AND s.THREE_MONTH_AVG_BAL >= 50000 THEN '4'
         WHEN s.THREE_MONTH_AVG_BAL < 300000 AND s.THREE_MONTH_AVG_BAL >= 100000 THEN '5'
         WHEN s.THREE_MONTH_AVG_BAL < 500000 AND s.THREE_MONTH_AVG_BAL >= 300000 THEN '6'
         WHEN s.THREE_MONTH_AVG_BAL < 2000000  AND s.THREE_MONTH_AVG_BAL >= 500000 THEN '7'
         WHEN s.THREE_MONTH_AVG_BAL < 5000000 AND s.THREE_MONTH_AVG_BAL >= 2000000 THEN '8'
         WHEN s.THREE_MONTH_AVG_BAL < 10000000 AND s.THREE_MONTH_AVG_BAL >= 5000000 THEN '9'
         ELSE '99' END as THREE_AVG_PART,
	V_DT
   FROM MCRM_RET_CUST_ASSETS s
   WHERE s.O_MAIN_TYPE = '1'
    GROUP BY ORG_ID,
            ORG_NAME,
			CASE WHEN s.MONTH_BAL < 10000 THEN '1'
				 WHEN s.MONTH_BAL < 30000 AND s.MONTH_BAL >= 10000 THEN '2'
				 WHEN s.MONTH_BAL < 50000 AND s.MONTH_BAL >= 30000 THEN '3'
				 WHEN s.MONTH_BAL < 100000 AND s.MONTH_BAL >= 50000 THEN '4'
				 WHEN s.MONTH_BAL < 300000 AND s.MONTH_BAL >= 100000 THEN '5'
				 WHEN s.MONTH_BAL < 500000 AND s.MONTH_BAL >= 300000 THEN '6'
				 WHEN s.MONTH_BAL < 2000000 AND s.MONTH_BAL >= 500000 THEN '7'
				 WHEN s.MONTH_BAL < 5000000 AND s.MONTH_BAL >= 2000000 THEN '8'
				 WHEN s.MONTH_BAL < 10000000 AND s.MONTH_BAL >= 5000000 THEN '9'
				 ELSE '99' END,
			CASE WHEN s.MONTH_AVG_BAL < 10000 THEN '1'
				 WHEN s.MONTH_AVG_BAL < 30000  AND s.MONTH_AVG_BAL >= 10000 THEN '2'
				 WHEN s.MONTH_AVG_BAL < 50000 AND s.MONTH_AVG_BAL >= 30000 THEN '3'
				 WHEN s.MONTH_AVG_BAL < 100000 AND s.MONTH_AVG_BAL >= 50000 THEN '4'
				 WHEN s.MONTH_AVG_BAL < 300000 AND s.MONTH_AVG_BAL >= 100000 THEN '5'
				 WHEN s.MONTH_AVG_BAL < 500000 AND s.MONTH_AVG_BAL >= 300000 THEN '6'
				 WHEN s.MONTH_AVG_BAL < 2000000  AND s.MONTH_AVG_BAL >= 500000 THEN '7'
				 WHEN s.MONTH_AVG_BAL < 5000000 AND s.MONTH_AVG_BAL >= 2000000 THEN '8'
				 WHEN s.MONTH_AVG_BAL < 10000000 AND s.MONTH_AVG_BAL >= 5000000 THEN '9'
				 ELSE '99' END,
			CASE WHEN s.THREE_MONTH_AVG_BAL < 10000 THEN '1'
				 WHEN s.THREE_MONTH_AVG_BAL < 30000  AND s.THREE_MONTH_AVG_BAL >= 10000 THEN '2'
				 WHEN s.THREE_MONTH_AVG_BAL < 50000 AND s.THREE_MONTH_AVG_BAL >= 30000 THEN '3'
				 WHEN s.THREE_MONTH_AVG_BAL < 100000 AND s.THREE_MONTH_AVG_BAL >= 50000 THEN '4'
				 WHEN s.THREE_MONTH_AVG_BAL < 300000 AND s.THREE_MONTH_AVG_BAL >= 100000 THEN '5'
				 WHEN s.THREE_MONTH_AVG_BAL < 500000 AND s.THREE_MONTH_AVG_BAL >= 300000 THEN '6'
				 WHEN s.THREE_MONTH_AVG_BAL < 2000000  AND s.THREE_MONTH_AVG_BAL >= 500000 THEN '7'
				 WHEN s.THREE_MONTH_AVG_BAL < 5000000 AND s.THREE_MONTH_AVG_BAL >= 2000000 THEN '8'
				 WHEN s.THREE_MONTH_AVG_BAL < 10000000 AND s.THREE_MONTH_AVG_BAL >= 5000000 THEN '9'
				 ELSE '99' END
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_CUST_ASSETS_TG2 = sqlContext.sql(sql)
MCRM_RET_CUST_ASSETS_TG2.registerTempTable("MCRM_RET_CUST_ASSETS_TG2")
dfn="MCRM_RET_CUST_ASSETS_TG2/"+V_DT+".parquet"
MCRM_RET_CUST_ASSETS_TG2.cache()
nrows = MCRM_RET_CUST_ASSETS_TG2.count()
MCRM_RET_CUST_ASSETS_TG2.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_RET_CUST_ASSETS_TG2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_RET_CUST_ASSETS_TG2 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
