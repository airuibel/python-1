#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_ANALYSIS_INFO').setMaster(sys.argv[2])
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

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
ACRM_F_CI_CUST_CONTRIBUTION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_CONTRIBUTION/*')
ACRM_F_CI_CUST_CONTRIBUTION.registerTempTable("ACRM_F_CI_CUST_CONTRIBUTION")
ACRM_F_CI_LOYALTY_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_LOYALTY_INFO/*')
ACRM_F_CI_LOYALTY_INFO.registerTempTable("ACRM_F_CI_LOYALTY_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,CAST(SUM(B.CUST_CONTRIBUTION) AS DECIMAL(24,6)  )                    AS CUST_CONTRIBUTION 
       ,CAST(MAX(C.LOYA_SCORE) AS DECIMAL(22,2))                        AS LOYA_SCORE 
       ,CAST(''   AS DECIMAL(22,2))                 AS MONTH_TOTAL_INT 
       ,CAST('' AS DECIMAL(22,2))                    AS CUST_TOTAL_INT 
       ,CAST('' AS DECIMAL(22,2))                    AS MONTH_COST_INT 
       ,CAST('' AS DECIMAL(22,2))                    AS CUST_TOTAL_COST 
       ,CAST('' AS DECIMAL(22,2))                    AS CUST_USABLE_INT 
       ,A.OBJ_RATING            AS OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,CAST('' AS DECIMAL(22,2))                    AS ALERT_SCORE 
       ,CAST('' AS DECIMAL(22,2))                    AS LOST_SCORE 
       ,CAST('' AS DECIMAL(22,2))                    AS LIFE_CYCLE 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息
   LEFT JOIN ACRM_F_CI_CUST_CONTRIBUTION B                     --
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ODS_DATE              = V_DT 
   LEFT JOIN ACRM_F_CI_LOYALTY_INFO C                          --
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.ETL_DATE              = V_DT 
  GROUP BY A.CUST_ID 
       ,A.OBJ_RATING 
       ,A.SUB_RATING 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_ANALYSIS_INFO = sqlContext.sql(sql)
ACRM_A_ANALYSIS_INFO.registerTempTable("ACRM_A_ANALYSIS_INFO")
dfn="ACRM_A_ANALYSIS_INFO/"+V_DT+".parquet"
ACRM_A_ANALYSIS_INFO.cache()
nrows = ACRM_A_ANALYSIS_INFO.count()
ACRM_A_ANALYSIS_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_ANALYSIS_INFO.unpersist()
OCRM_F_CI_CUST_DESC.unpersist()
ACRM_F_CI_CUST_CONTRIBUTION.unpersist()
ACRM_F_CI_LOYALTY_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_ANALYSIS_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_ANALYSIS_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
