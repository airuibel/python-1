#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_ENT_CRE_COUNT').setMaster(sys.argv[2])
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
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_ENT_CRE_COUNT/"+V_DT_LD+".parquet")

#删除当天的
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_ENT_CRE_COUNT/"+V_DT+".parquet")

ACRM_F_CUS_DEV_CONFIG = sqlContext.read.parquet(hdfs+'/ACRM_F_CUS_DEV_CONFIG/*')
ACRM_F_CUS_DEV_CONFIG.registerTempTable("ACRM_F_CUS_DEV_CONFIG")
ACRM_A_CUST_CRE_MON_CHART = sqlContext.read.parquet(hdfs+'/ACRM_A_CUST_CRE_MON_CHART/*')
ACRM_A_CUST_CRE_MON_CHART.registerTempTable("ACRM_A_CUST_CRE_MON_CHART")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('00000' AS INTEGER)        AS ID 
       ,D1.ORG_ID               AS ORG_ID 
       ,D1.ORG_NAME             AS ORG_NAME 
       ,'CRM_CRE_001'           AS SUB_ID 
       ,G.COLUMN_NAME           AS SUB_NAME 
       ,COUNT(D1.CUST_ID)                       AS BAL 
       ,D1.REPORT_DATE          AS ODS_DATE 
       ,G.INDEX_MIN_VALUE       AS SORT_ID 
       ,CAST(SUM(D1.CRE_BAL) AS DECIMAL(24,6))        AS AMOUNT 
       ,D1.FR_NAME              AS FR_NAME 
       ,D1.FR_ID                AS FR_ID 
   FROM ACRM_A_CUST_CRE_MON_CHART D1                           --
  INNER JOIN ACRM_F_CUS_DEV_CONFIG G                           --客户发展情况一览表配制表
     ON D1.ORG_ID               = G.ORG_ID 
    AND G.INDEX_CODE            = 'CRM_CRE_001' 
  WHERE D1.CUST_TYP             = '2' 
    AND D1.REPORT_DATE          = V_DT 
    AND CAST(D1.CRE_BAL AS DECIMAL(24,6))   >= NVL(G.INDEX_MIN_VALUE, 0.001) 
    AND CAST(D1.CRE_BAL AS DECIMAL(24,6))   < NVL(G.INDEX_MAX_VALUE, 999999999999999999.99) 
  GROUP BY D1.FR_ID 
       ,D1.FR_NAME 
       ,D1.ORG_ID 
       ,D1.ORG_NAME 
       ,G.COLUMN_NAME 
       ,G.INDEX_MIN_VALUE 
       ,D1.REPORT_DATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_ENT_CRE_COUNT = sqlContext.sql(sql)
ACRM_A_ENT_CRE_COUNT.registerTempTable("ACRM_A_ENT_CRE_COUNT")
dfn="ACRM_A_ENT_CRE_COUNT/"+V_DT+".parquet"
ACRM_A_ENT_CRE_COUNT.cache()
nrows = ACRM_A_ENT_CRE_COUNT.count()
ACRM_A_ENT_CRE_COUNT.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_ENT_CRE_COUNT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_ENT_CRE_COUNT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
