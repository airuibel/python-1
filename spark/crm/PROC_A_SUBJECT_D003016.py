#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_SUBJECT_D003016').setMaster(sys.argv[2])
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

ACRM_F_CI_LOYALTY_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_LOYALTY_INFO/*')
ACRM_F_CI_LOYALTY_INFO.registerTempTable("ACRM_F_CI_LOYALTY_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,''                    AS ORG_ID 
       ,'D003016'               AS INDEX_CODE 
       ,CAST(A.LOYA_LEVEL AS DECIMAL(22,2))       AS INDEX_VALUE 
       ,SUBSTR(V_DT, 1, 7)                       AS YEAR_MONTH 
       ,V_DT               AS ETL_DATE 
       ,A.CUST_TYP              AS CUST_TYPE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_LOYALTY_INFO A                               --客户忠诚度信息
  WHERE  A.ETL_DATE              = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_TARGET_D003016 = sqlContext.sql(sql)
ACRM_A_TARGET_D003016.registerTempTable("ACRM_A_TARGET_D003016")
dfn="ACRM_A_TARGET_D003016/"+V_DT+".parquet"
ACRM_A_TARGET_D003016.cache()
nrows = ACRM_A_TARGET_D003016.count()
ACRM_A_TARGET_D003016.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_TARGET_D003016.unpersist()
ACRM_F_CI_LOYALTY_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TARGET_D003016/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_TARGET_D003016 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
