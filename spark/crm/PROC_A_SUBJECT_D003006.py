#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_SUBJECT_D003006').setMaster(sys.argv[2])
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
#月初10位日期
V_DT_FMD10 = date(int(etl_date[0:4]), int(etl_date[4:6]), 1).strftime("%Y-%m-%d") 
#上月末日期
V_DT_LMD = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1) + timedelta(-1)).strftime("%Y%m%d")
#10位日期
V_DT10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8]))).strftime("%Y-%m-%d")
V_STEP = 0

ACRM_F_NI_COLL_PAY_INSURANCE = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_COLL_PAY_INSURANCE/*')
ACRM_F_NI_COLL_PAY_INSURANCE.registerTempTable("ACRM_F_NI_COLL_PAY_INSURANCE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,''                    AS ORG_ID 
       ,'D003006'               AS INDEX_CODE 
       ,CAST(COUNT(1) AS DECIMAL(22,2))                     AS INDEX_VALUE 
       ,SUBSTR(V_DT, 1, 7)                       AS YEAR_MONTH 
       ,V_DT               AS ETL_DATE 
       ,A.CUST_TYPE             AS CUST_TYPE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_NI_COLL_PAY_INSURANCE A                         --代收代付/保险协议表
  WHERE A.BUY_DATE >= V_DT_FMD10 
    AND A.BUY_DATE <= V_DT 
    AND A.MONEY > 0 
  GROUP BY A.CUST_ID 
       ,A.FR_ID 
       ,A.CUST_TYPE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\V_DT_FMD10\b", "'"+V_DT_FMD10+"'", sql)
ACRM_A_TARGET_D003006 = sqlContext.sql(sql)
ACRM_A_TARGET_D003006.registerTempTable("ACRM_A_TARGET_D003006")
dfn="ACRM_A_TARGET_D003006/"+V_DT+".parquet"
ACRM_A_TARGET_D003006.cache()
nrows = ACRM_A_TARGET_D003006.count()
ACRM_A_TARGET_D003006.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_TARGET_D003006.unpersist()
ACRM_F_NI_COLL_PAY_INSURANCE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TARGET_D003006/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_TARGET_D003006 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
