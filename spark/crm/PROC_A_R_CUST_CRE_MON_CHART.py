#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_CUST_CRE_MON_CHART').setMaster(sys.argv[2])
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

#保留当天及月末数据
if V_DT_LD != V_DT_LMD :
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CUST_CRE_MON_CHART/"+V_DT_LD+".parquet")
	
#删除当天的
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CUST_CRE_MON_CHART/"+V_DT+".parquet")

#----------来源表---------------

ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")
OCRM_F_CM_EXCHANGE_RATE = sqlContext.read.parquet(hdfs+'/OCRM_F_CM_EXCHANGE_RATE/*')
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('000000' AS BIGINT)        AS ID 
       ,CAST(C.CUST_ID  AS VARCHAR(32))             AS CUST_ID 
       ,CAST(C.CUST_NAME   AS VARCHAR(200))          AS CUST_NAME 
       ,CAST(C.CUST_TYP AS VARCHAR(13))             AS CUST_TYP 
       ,CAST(C.OPEN_BRC    AS   VARCHAR(20))       AS ORG_ID 
       ,CAST(M.ORG_NAME  AS VARCHAR(200))            AS ORG_NAME 
       ,YEAR(V_DT)                       AS COUNT_YEAR 
       ,MONTH(V_DT)                       AS COUNT_MONTH 
       ,CAST(SUM(NVL(AMOUNT, 0) * NVL(H.EXCHANGE_RATE, 1)) AS DECIMAL(24,6))                       AS CRE_BAL 
       ,CAST(SUM((NVL(MONTH_BAL_SUM_1, 0) + NVL(MONTH_BAL_SUM_2, 0) + NVL(MONTH_BAL_SUM_3, 0) + NVL(MONTH_BAL_SUM_4, 0) + NVL(MONTH_BAL_SUM_5, 0) + NVL(MONTH_BAL_SUM_6, 0) + NVL(MONTH_BAL_SUM_7, 0) + NVL(MONTH_BAL_SUM_8, 0) + NVL(MONTH_BAL_SUM_9, 0) + NVL(MONTH_BAL_SUM_10, 0) + NVL(MONTH_BAL_SUM_11, 0) + NVL(MONTH_BAL_SUM_12, 0)) /(NVL(MONTH_DAYS_1, 0) + NVL(MONTH_DAYS_2, 0) + NVL(MONTH_DAYS_3, 0) + NVL(MONTH_DAYS_4, 0) + NVL(MONTH_DAYS_5, 0) + NVL(MONTH_DAYS_6, 0) + NVL(MONTH_DAYS_7, 0) + NVL(MONTH_DAYS_8, 0) + NVL(MONTH_DAYS_9, 0) + NVL(MONTH_DAYS_10, 0) + NVL(MONTH_DAYS_11, 0) + NVL(MONTH_DAYS_12, 0)) * NVL(H.EXCHANGE_RATE, 1)) AS DECIMAL(24,6))                       AS CRE_YEAR_AVG 
       ,V_DT                       AS REPORT_DATE 
       ,CAST(C.FR_ID AS VARCHAR(100))                AS FR_ID 
       ,CAST(M.FR_NAME AS VARCHAR(100))              AS FR_NAME 
   FROM ACRM_F_RE_LENDSUMAVGINFO C                             --贷款账户积数表
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE H                         --
     ON C.MONEY_TYPE            = H.CURRENCY_CD 
    AND H.OBJECT_CURRENCY_CD    = 'CNY' 
    AND H.ETL_DT                = V_DT
   LEFT JOIN ADMIN_AUTH_ORG M                                  --
     ON C.OPEN_BRC              = M.ORG_ID 
  WHERE C.YEAR                  = TRIM(YEAR(V_DT)) 
  GROUP BY C.FR_ID 
       ,C.OPEN_BRC 
       ,C.CUST_ID 
       ,C.CUST_TYP 
       ,C.CUST_NAME 
       ,M.FR_NAME 
       ,M.ORG_NAME """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CUST_CRE_MON_CHART = sqlContext.sql(sql)
ACRM_A_CUST_CRE_MON_CHART.registerTempTable("ACRM_A_CUST_CRE_MON_CHART")
dfn="ACRM_A_CUST_CRE_MON_CHART/"+V_DT+".parquet"
ACRM_A_CUST_CRE_MON_CHART.cache()
nrows = ACRM_A_CUST_CRE_MON_CHART.count()
ACRM_A_CUST_CRE_MON_CHART.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_CUST_CRE_MON_CHART.unpersist()
ADMIN_AUTH_ORG.unpersist()
ACRM_F_RE_LENDSUMAVGINFO.unpersist()
OCRM_F_CM_EXCHANGE_RATE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CUST_CRE_MON_CHART lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
