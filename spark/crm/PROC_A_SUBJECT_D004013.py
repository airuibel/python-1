#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_SUBJECT_D004013').setMaster(sys.argv[2])
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
#年初10位日期
V_YEAR_DT10 = (date(int(etl_date[0:4]),1,1)).strftime("%Y-%m-%d")
V_STEP = 0

OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUST_ID           AS CUST_ID 
       ,''                    AS ORG_ID 
       ,'D004013'               AS INDEX_CODE 
       ,CAST(COUNT(1) AS DECIMAL(22,2))           AS INDEX_VALUE 
       ,SUBSTR(V_DT, 1, 7)                       AS YEAR_MONTH 
       ,V_DT               AS ETL_DATE 
       ,A.ODS_CUST_TYPE         AS CUST_TYPE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
  WHERE COALESCE(A.END_DATE, '2099-12-31') > V_YEAR_DT 
    AND A.BEGIN_DATE <= V_YEAR_DT 
    AND(A.ODS_SYS_ID            = 'IBK' 
             OR A.ODS_SYS_ID            = 'MBK' 
             OR A.ODS_SYS_ID            = 'MSG' 
             OR A.ODS_SYS_ID            = 'APY') 
  GROUP BY A.ODS_CUST_ID 
       ,A.ODS_CUST_TYPE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\V_YEAR_DT\b", "'"+V_YEAR_DT10+"'", sql)
ACRM_A_TARGET_D004013 = sqlContext.sql(sql)
ACRM_A_TARGET_D004013.registerTempTable("ACRM_A_TARGET_D004013")
dfn="ACRM_A_TARGET_D004013/"+V_DT+".parquet"
ACRM_A_TARGET_D004013.cache()
nrows = ACRM_A_TARGET_D004013.count()
ACRM_A_TARGET_D004013.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_TARGET_D004013.unpersist()
OCRM_F_CI_SYS_RESOURCE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TARGET_D004013/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_TARGET_D004013 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
