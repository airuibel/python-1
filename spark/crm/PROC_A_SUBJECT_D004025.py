#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_SUBJECT_D004025').setMaster(sys.argv[2])
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
v_sub_id = 'D004025';

ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT 
	    CUST_ID                 		AS CUST_ID --
		,''                   			AS ORG_ID
		,'D004025'                      AS INDEX_CODE 
		,cast(SUM(OLD_YEAR_BAL_SUM /31) as decimal(22,2))     AS INDEX_VALUE 
		,V_YEAR_MONTH                 AS YEAR_MONTH 
		,V_DT                         AS ETL_DATE 
        ,CUST_TYP                		AS CUST_TYPE 
        ,FR_ID                   		AS FR_ID      
   FROM ACRM_F_RE_LENDSUMAVGINFO A                             --
  WHERE YEAR = substr(V_DT,1,4) 
  GROUP BY CUST_TYP 
       ,CUST_ID 
       ,FR_ID 
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_YEAR_MONTH\b", "'"+V_YEAR_MONTH+"'", sql)

ACRM_A_TARGET_D004025 = sqlContext.sql(sql)
ACRM_A_TARGET_D004025.registerTempTable("ACRM_A_TARGET_D004025")
dfn="ACRM_A_TARGET_D004025/"+V_DT+".parquet"
ACRM_A_TARGET_D004025.cache()
nrows = ACRM_A_TARGET_D004025.count()
ACRM_A_TARGET_D004025.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_TARGET_D004025.unpersist()
ACRM_F_RE_LENDSUMAVGINFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TARGET_D004025/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_TARGET_D004025 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
