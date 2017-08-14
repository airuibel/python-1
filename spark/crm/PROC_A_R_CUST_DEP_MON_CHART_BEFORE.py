#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_CUST_DEP_MON_CHART_BEFORE').setMaster(sys.argv[2])
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
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
#目标表
#ACRM_A_CUST_DEP_MON_CHART_BEFORE 全量表 

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()   AS ID 
       ,B.CUST_ID               AS CUST_ID 
       ,B.CUST_NAME             AS CUST_NAME 
       ,C.ORG_ID                AS ORG_ID 
       ,C.ORG_NAME              AS ORG_NAME 
       ,substr(V_DT, 1,4)                       AS COUNT_YEAR 
       ,SUBSTR(V_DT, 6, 2)                       AS COUNT_MONTH 
       ,CAST(COALESCE(SUM(B.BAL_RMB), 0) AS DECIMAL(24,6))                       AS DEP_BAL 
       ,CAST(COALESCE(SUM(B.MVAL_RMB), 0) AS DECIMAL(24,6))                       AS DEP_YEAR_AVG 
       ,V_DT                    AS REPORT_DATE 
       ,B.CUST_TYP              AS CUST_TYP 
       ,C.FR_NAME               AS FR_NAME 
       ,C.FR_ID                 AS FR_ID 
       ,CAST(0 AS DECIMAL(24,6))                    AS DEP_MONTH_AVG 
   FROM ACRM_F_DP_SAVE_INFO B                                  --负债协议
   LEFT JOIN ADMIN_AUTH_ORG C                                  --机构表
     ON C.FR_ID                 = B.FR_ID 
    AND B.ORG_ID                = C.ORG_ID 
  WHERE B.ACCT_STATUS           = '01' 
  GROUP BY C.FR_ID 
       ,C.FR_NAME 
       ,B.CUST_ID 
       ,B.CUST_NAME 
       ,B.CUST_TYP 
       ,C.ORG_ID 
       ,C.ORG_NAME """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CUST_DEP_MON_CHART_BEFORE = sqlContext.sql(sql)
ACRM_A_CUST_DEP_MON_CHART_BEFORE.registerTempTable("ACRM_A_CUST_DEP_MON_CHART_BEFORE")
dfn="ACRM_A_CUST_DEP_MON_CHART_BEFORE/"+V_DT+".parquet"
ACRM_A_CUST_DEP_MON_CHART_BEFORE.cache()
nrows = ACRM_A_CUST_DEP_MON_CHART_BEFORE.count()
ACRM_A_CUST_DEP_MON_CHART_BEFORE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_CUST_DEP_MON_CHART_BEFORE.unpersist()
#全量表，删除前一天记录
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CUST_DEP_MON_CHART_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CUST_DEP_MON_CHART_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
