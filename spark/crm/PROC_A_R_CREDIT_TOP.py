#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_CREDIT_TOP').setMaster(sys.argv[2])
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

ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
ACRM_A_CUST_CRE_MON_CHART = sqlContext.read.parquet(hdfs+'/ACRM_A_CUST_CRE_MON_CHART/*')
ACRM_A_CUST_CRE_MON_CHART.registerTempTable("ACRM_A_CUST_CRE_MON_CHART")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('000000' AS BIGINT)                AS ID 
       ,T.ORG_ID                AS ORG_ID 
       ,T.ORG_NAME              AS ORG_NAME 
       ,T.CUST_ID               AS CUST_ID 
       ,T.CUST_NAME             AS CUST_NAME 
       ,T.CRE_BAL               AS CRE_BAL 
       ,T.REPORT_DATE           AS REPORT_DATE 
   FROM ACRM_A_CUST_CRE_MON_CHART T                            --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CREDIT_TOP = sqlContext.sql(sql)
ACRM_A_CREDIT_TOP.registerTempTable("ACRM_A_CREDIT_TOP")
dfn="ACRM_A_CREDIT_TOP/"+V_DT+".parquet"
ACRM_A_CREDIT_TOP.cache()
nrows = ACRM_A_CREDIT_TOP.count()
ACRM_A_CREDIT_TOP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_CREDIT_TOP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CREDIT_TOP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CREDIT_TOP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('000000' AS BIGINT)  AS ID 
       ,BB.ORG_ID               AS ORG_ID 
       ,BB.ORG_NAME             AS ORG_NAME 
       ,AA.CUST_ID              AS CUST_ID 
       ,AA.CUST_NAME            AS CUST_NAME 
       ,AA.CRE_BAL              AS CRE_BAL 
       ,V_DT                       AS REPORT_DATE 
   FROM ACRM_A_CUST_CRE_MON_CHART AA                           --
   LEFT JOIN ADMIN_AUTH_ORG BB                                 --
     ON AA.FR_ID                = BB.FR_ID 
    AND BB.UP_ORG_ID            = '320000000' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT+"'", sql)
ACRM_A_CREDIT_TOP = sqlContext.sql(sql)
ACRM_A_CREDIT_TOP.registerTempTable("ACRM_A_CREDIT_TOP")
dfn="ACRM_A_CREDIT_TOP/"+V_DT+".parquet"
ACRM_A_CREDIT_TOP.cache()
nrows = ACRM_A_CREDIT_TOP.count()
ACRM_A_CREDIT_TOP.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_CREDIT_TOP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CREDIT_TOP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
