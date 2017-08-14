#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_SAVE_SUM_REPORT').setMaster(sys.argv[2])
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

OCRM_F_FID = sqlContext.read.parquet(hdfs+'/OCRM_F_FID/*')
OCRM_F_FID.registerTempTable("OCRM_F_FID")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT FR_ID                   AS FR_ID 
       ,CUST_ID                 AS CUST_ID 
       ,SUM(BAL_RMB)                       AS BAL_RMB 
       ,CUST_TYP                AS CUST_TYP 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议
  WHERE ACCT_STATUS             = '01' 
  GROUP BY FR_ID 
       ,CUST_ID 
       ,CUST_TYP """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_MCRM_SAVE_SUM_01 = sqlContext.sql(sql)
TMP_MCRM_SAVE_SUM_01.registerTempTable("TMP_MCRM_SAVE_SUM_01")
dfn="TMP_MCRM_SAVE_SUM_01/"+V_DT+".parquet"
TMP_MCRM_SAVE_SUM_01.cache()
nrows = TMP_MCRM_SAVE_SUM_01.count()
TMP_MCRM_SAVE_SUM_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_MCRM_SAVE_SUM_01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_MCRM_SAVE_SUM_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_MCRM_SAVE_SUM_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.FR_ID                 AS FR_ID 
       ,B.FR_NAME               AS FR_NAME 
       ,SUM(CASE WHEN A.BAL_RMB > 30000000 THEN A.BAL_RMB ELSE 0 END)                       AS SUM1 
       ,SUM(CASE WHEN A.BAL_RMB > 30000000 THEN 1 ELSE 0 END)                       AS COUNT1 
       ,SUM(CASE WHEN A.BAL_RMB > 10000000 
            AND A.BAL_RMB <= 30000000 THEN A.BAL_RMB ELSE 0 END)                       AS SUM2 
       ,SUM(CASE WHEN A.BAL_RMB > 10000000 
            AND A.BAL_RMB <= 30000000 THEN 1 ELSE 0 END)                       AS COUNT2 
       ,SUM(CASE WHEN A.BAL_RMB > 1000000 
            AND A.BAL_RMB <= 10000000 THEN A.BAL_RMB ELSE 0 END)                       AS SUM3 
       ,SUM(CASE WHEN A.BAL_RMB > 1000000 
            AND A.BAL_RMB <= 10000000 THEN 1 ELSE 0 END)                       AS COUNT3 
       ,SUM(CASE WHEN A.BAL_RMB > 500000 
            AND A.BAL_RMB <= 1000000 THEN A.BAL_RMB ELSE 0 END)                       AS SUM4 
       ,SUM(CASE WHEN A.BAL_RMB > 500000 
            AND A.BAL_RMB <= 1000000 THEN 1 ELSE 0 END)                       AS COUNT4 
       ,SUM(CASE WHEN A.BAL_RMB > 100000 
            AND A.BAL_RMB <= 500000 THEN A.BAL_RMB ELSE 0 END)                       AS SUM5 
       ,SUM(CASE WHEN A.BAL_RMB > 100000 
            AND A.BAL_RMB <= 500000 THEN 1 ELSE 0 END)                       AS COUNT5 
       ,SUM(CASE WHEN A.BAL_RMB > 50000 
            AND A.BAL_RMB <= 100000 THEN A.BAL_RMB ELSE 0 END)                       AS SUM6 
       ,SUM(CASE WHEN A.BAL_RMB > 50000 
            AND A.BAL_RMB <= 100000 THEN 1 ELSE 0 END)                       AS COUNT6 
       ,SUM(CASE WHEN A.BAL_RMB > 10000 
            AND A.BAL_RMB <= 50000 THEN A.BAL_RMB ELSE 0 END)                       AS SUM7 
       ,SUM(CASE WHEN A.BAL_RMB > 10000 
            AND A.BAL_RMB <= 50000 THEN 1 ELSE 0 END)                       AS COUNT7 
       ,SUM(CASE WHEN A.BAL_RMB <= 10000 THEN A.BAL_RMB ELSE 0 END)                       AS SUM8 
       ,SUM(CASE WHEN A.BAL_RMB <= 10000 THEN 1 ELSE 0 END)                       AS COUNT8 
       ,A.CUST_TYP              AS CUST_TYP 
       ,V_DT                    AS REPORT_DATE 
   FROM TMP_MCRM_SAVE_SUM_01 A                                 --客户存款汇总临时表
  INNER JOIN OCRM_F_FID B                                      --法人表
     ON A.FR_ID                 = B.FR_ID 
    AND B.IF_USE                = '1' 
  GROUP BY A.FR_ID,B.FR_NAME,A.CUST_TYP"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SAVE_SUM = sqlContext.sql(sql)
MCRM_SAVE_SUM.registerTempTable("MCRM_SAVE_SUM")
dfn="MCRM_SAVE_SUM/"+V_DT+".parquet"
MCRM_SAVE_SUM.cache()
nrows = MCRM_SAVE_SUM.count()
MCRM_SAVE_SUM.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_SAVE_SUM.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_SAVE_SUM/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_SAVE_SUM lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
