#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_CUST_DEP_MON_CHART').setMaster(sys.argv[2])
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

#----------来源表---------------
ACRM_A_CUST_DEP_MON_CHART_BEFORE = sqlContext.read.parquet(hdfs+'/ACRM_A_CUST_DEP_MON_CHART_BEFORE/*')
ACRM_A_CUST_DEP_MON_CHART_BEFORE.registerTempTable("ACRM_A_CUST_DEP_MON_CHART_BEFORE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,COUNT_YEAR              AS COUNT_YEAR 
       ,COUNT_MONTH             AS COUNT_MONTH 
       ,DEP_BAL                 AS DEP_BAL 
       ,DEP_YEAR_AVG            AS DEP_YEAR_AVG 
       ,REPORT_DATE             AS REPORT_DATE 
       ,CUST_TYP                AS CUST_TYP 
       ,FR_NAME                 AS FR_NAME 
       ,FR_ID                   AS FR_ID 
       ,DEP_MONTH_AVG           AS DEP_MONTH_AVG 
   FROM ACRM_A_CUST_DEP_MON_CHART_BEFORE A                     --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CUST_DEP_MON_CHART = sqlContext.sql(sql)
ACRM_A_CUST_DEP_MON_CHART.registerTempTable("ACRM_A_CUST_DEP_MON_CHART")
dfn="ACRM_A_CUST_DEP_MON_CHART/"+V_DT+".parquet"
ACRM_A_CUST_DEP_MON_CHART.cache()
nrows = ACRM_A_CUST_DEP_MON_CHART.count()
ACRM_A_CUST_DEP_MON_CHART.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_CUST_DEP_MON_CHART.unpersist()
ACRM_A_CUST_DEP_MON_CHART_BEFORE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CUST_DEP_MON_CHART/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CUST_DEP_MON_CHART lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
