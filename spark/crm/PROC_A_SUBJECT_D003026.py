#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_SUBJECT_D003026').setMaster(sys.argv[2])
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
#季末字段
V_DT_QUA = etl_date[4:8]

V_STEP = 0

ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1
if V_DT_QUA != '0331' and V_DT_QUA != '0630' and V_DT_QUA != '0930' and V_DT_QUA != '1231':
    sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,''                    AS ORG_ID 
       ,'D003026'               AS INDEX_CODE 
       ,CAST(SUM(COALESCE(A.MONTH_AVG,0)) AS DECIMAL(22,2))              AS INDEX_VALUE 
       ,SUBSTR(V_DT, 1, 7)                       AS YEAR_MONTH 
       ,V_DT               AS ETL_DATE 
       ,A.CUST_TYP              AS CUST_TYPE 
       ,A.FR_ID                   AS FR_ID 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议表
  WHERE A.ACCT_STATUS           = '01' 
    AND A.MONTH_AVG > 0 
  GROUP BY A.CUST_ID 
       ,A.CUST_TYP,A.FR_ID """

else:
    sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,''                    AS ORG_ID 
       ,'D003026'               AS INDEX_CODE 
       ,CAST(SUM(COALESCE(QUARTER_DAILY, 0)) AS DECIMAL(22,2))      AS INDEX_VALUE 
       ,SUBSTR(V_DT, 1, 7)                       AS YEAR_MONTH 
       ,V_DT               AS ETL_DATE 
       ,A.CUST_TYP              AS CUST_TYPE 
       ,A.FR_ID                   AS FR_ID 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议表
  WHERE A.ACCT_STATUS           = '01' 
    AND A.QUARTER_DAILY > 0 
  GROUP BY A.CUST_ID 
       ,A.CUST_TYP,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_TARGET_D003026 = sqlContext.sql(sql)
ACRM_A_TARGET_D003026.registerTempTable("ACRM_A_TARGET_D003026")
dfn="ACRM_A_TARGET_D003026/"+V_DT+".parquet"
ACRM_A_TARGET_D003026.cache()
nrows = ACRM_A_TARGET_D003026.count()
ACRM_A_TARGET_D003026.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_TARGET_D003026.unpersist()
ACRM_F_DP_SAVE_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TARGET_D003026/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_TARGET_D003026 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
