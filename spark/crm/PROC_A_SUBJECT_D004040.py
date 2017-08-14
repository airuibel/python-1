#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_SUBJECT_D004040').setMaster(sys.argv[2])
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
#---------------------------------------------------------------------------------------#
V_YEAR_MONTH =  etl_date[0:4]+"-" + etl_date[4:6]
v_sub_id = 'D004040'; 

ACRM_A_TARGET_D004025 = sqlContext.read.parquet(hdfs+'/ACRM_A_TARGET_D004025/*')
ACRM_A_TARGET_D004025.registerTempTable("ACRM_A_TARGET_D004025")

ACRM_A_TARGET_D004024 = sqlContext.read.parquet(hdfs+'/ACRM_A_TARGET_D004024/*')
ACRM_A_TARGET_D004024.registerTempTable("ACRM_A_TARGET_D004024")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT 
			A.CUST_ID			as CUST_ID
			,'' 				as ORG_ID  
			,'D004040'			as INDEX_CODE
			,CASE WHEN B.INDEX_VALUE = 0 THEN 0.0
				 WHEN A.INDEX_VALUE/B.INDEX_VALUE > 1.5 THEN 0.0
				 WHEN A.INDEX_VALUE/B.INDEX_VALUE < 2/3 THEN 2.0
				ELSE 1.0 END			as INDEX_VALUE
			,V_YEAR_MONTH		as YEAR_MONTH
			,V_DT				as ETL_DATE
			,A.CUST_TYPE 		as CUST_TYPE
			,A.FR_ID			as FR_ID
	FROM ACRM_A_TARGET_D004024 A,
	     ACRM_A_TARGET_D004025 B 
	WHERE A.CUST_ID = B.CUST_ID 
		  AND A.FR_ID = B.FR_ID																	  
		  AND A.CUST_TYPE = '2'		
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_YEAR_MONTH\b", "'"+V_YEAR_MONTH+"'", sql)

ACRM_A_TARGET_D004040 = sqlContext.sql(sql)
ACRM_A_TARGET_D004040.registerTempTable("ACRM_A_TARGET_D004040")
dfn="ACRM_A_TARGET_D004040/"+V_DT+".parquet"
ACRM_A_TARGET_D004040.cache()
nrows = ACRM_A_TARGET_D004040.count()
ACRM_A_TARGET_D004040.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_TARGET_D004040.unpersist()
ACRM_A_TARGET_D004024.unpersist()
ACRM_A_TARGET_D004025.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TARGET_D004040/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_TARGET_D004040 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
