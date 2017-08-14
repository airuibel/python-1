#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_SUBJECT_D004024').setMaster(sys.argv[2])
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

ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,''                    AS ORG_ID 
       ,'D004024'               AS INDEX_CODE 
       ,CASE MONTH(V_DT) WHEN 1 THEN SUM(MONTH_BAL_SUM_1 / MONTH_DAYS_1) WHEN 2 THEN SUM(MONTH_BAL_SUM_2 / MONTH_DAYS_2) WHEN 3 THEN SUM(MONTH_BAL_SUM_3 / MONTH_DAYS_3) WHEN 4 THEN SUM(MONTH_BAL_SUM_4 / MONTH_DAYS_4) WHEN 5 THEN SUM(MONTH_BAL_SUM_5 / MONTH_DAYS_5) WHEN 6 THEN SUM(MONTH_BAL_SUM_6 / MONTH_DAYS_6) WHEN 7 THEN SUM(MONTH_BAL_SUM_7 / MONTH_DAYS_7) WHEN 8 THEN SUM(MONTH_BAL_SUM_8 / MONTH_DAYS_8) WHEN 9 THEN SUM(MONTH_BAL_SUM_9 / MONTH_DAYS_9) WHEN 10 THEN SUM(MONTH_BAL_SUM_10 / MONTH_DAYS_10) WHEN 11 THEN SUM(MONTH_BAL_SUM_11 / MONTH_DAYS_11) WHEN 12 THEN SUM(MONTH_BAL_SUM_12 / MONTH_DAYS_12) END                     AS INDEX_VALUE 
       ,SUBSTR(V_DT, 1, 7)                       AS YEAR_MONTH 
       ,V_DT                    AS ETL_DATE 
       ,CUST_TYP                AS CUST_TYPE 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_LENDSUMAVGINFO A                             --贷款积数表
  WHERE A.YEAR                  = YEAR(V_DT) - 1 
  GROUP BY A.CUST_TYP 
       ,A.CUST_ID 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_TARGET_D004024 = sqlContext.sql(sql)
ACRM_A_TARGET_D004024.registerTempTable("ACRM_A_TARGET_D004024")
dfn="ACRM_A_TARGET_D004024/"+V_DT+".parquet"
ACRM_A_TARGET_D004024.cache()
nrows = ACRM_A_TARGET_D004024.count()
ACRM_A_TARGET_D004024.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_TARGET_D004024.unpersist()
ACRM_F_RE_LENDSUMAVGINFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TARGET_D004024/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_TARGET_D004024 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
