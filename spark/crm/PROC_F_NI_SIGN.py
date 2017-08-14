#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_NI_SIGN').setMaster(sys.argv[2])
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

ACRM_F_NI_COLL_PAY_INSURANCE = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_COLL_PAY_INSURANCE/*')
ACRM_F_NI_COLL_PAY_INSURANCE.registerTempTable("ACRM_F_NI_COLL_PAY_INSURANCE")
ACRM_F_NI_FINANCING = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_FINANCING/*')
ACRM_F_NI_FINANCING.registerTempTable("ACRM_F_NI_FINANCING")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS BIGINT) AS AGREEMENT_ID
          ,CUST_ID AS CUST_ID
          ,CUST_NAME AS CUST_NAME
          ,'' AS S_CUST_NO
          ,PRODUCT_ID AS S_PRODUCT_ID
          ,'' AS ORG_ID
          ,ACCOUNT AS ACCT_NO
          ,'' AS ARG_DT
          ,'' AS END_DT
          ,'' AS ARG_STATUS
          ,'' AS S_SERIALNO
          ,'' AS AMT_CYNO
          ,CAST(CAST(0 AS DECIMAL(24,6)) AS DECIMAL(24,6)) AS AMT
          ,CAST(MONEY AS DECIMAL(24,6)) AS BAL
          ,CAST(CAST(0 AS DECIMAL(17,2)) AS DECIMAL(17,2)) AS FEE_AMT
          ,CAST(CAST(0 AS DECIMAL(12,9)) AS DECIMAL(12,9)) AS DOLLAR_RATE
          ,CAST(CAST(0 AS DECIMAL(12,9)) AS DECIMAL(12,9)) AS CNY_RATE
          ,'' AS ACCT_STATE
          ,'' AS V_FLAG
          ,'' AS OTHER_BANK_FLAG
          ,'' AS AGREEMENT_NAME
          ,'' AS ITEM
          ,'' AS SBIT
          ,'' AS SSIT
          ,CAST(CAST(0 AS DECIMAL(24,6)) AS DECIMAL(24,6)) AS LL_RATE
          ,'' AS START_DATE
          ,'' AS ENT_DATE
          ,'' AS TYPE
          ,'' AS STATUS
          ,'' AS HANDLE_USER
          ,CRM_DT AS CRM_DT
          ,'' AS SYS_NO
          ,'' AS DP_TYPE
          ,CAST(CAST(0 AS DECIMAL(17,2)) AS DECIMAL(17,2)) AS TPD_BAL
          ,CAST(CAST(0 AS DECIMAL(17,2)) AS DECIMAL(17,2)) AS BTC_AMT
          ,CAST(CAST(0 AS DECIMAL(17,2)) AS DECIMAL(17,2)) AS CTB_AMT
          ,CAST(CAST(0 AS DECIMAL(17,2)) AS DECIMAL(17,2)) AS EXPECT_INC
          ,'' AS INT_BGN_DATE
          ,'' AS END_DATE
          ,'' AS BUY_DATE
          ,FR_ID AS FR_ID
          ,CUST_TYPE AS CUST_TYPE
   FROM ACRM_F_NI_COLL_PAY_INSURANCE A                         --代收代付/保险协议表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_NI_SIGN = sqlContext.sql(sql)
ACRM_F_NI_SIGN.registerTempTable("ACRM_F_NI_SIGN")
dfn="ACRM_F_NI_SIGN/"+V_DT+".parquet"
ACRM_F_NI_SIGN.cache()
nrows = ACRM_F_NI_SIGN.count()
ACRM_F_NI_SIGN.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_NI_SIGN.unpersist()
ACRM_F_NI_COLL_PAY_INSURANCE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_NI_SIGN/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_NI_SIGN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS BIGINT) AS AGREEMENT_ID
           ,CUST_ID AS CUST_ID
           ,CUST_NAME AS CUST_NAME
           ,'' AS S_CUST_NO
           ,PRODUCT_ID AS S_PRODUCT_ID
           ,'' AS ORG_ID
           ,ACCOUNT AS ACCT_NO
           ,'' AS ARG_DT
           ,'' AS END_DT
           ,'' AS ARG_STATUS
           ,'' AS S_SERIALNO
           ,CNCY AS AMT_CYNO
           ,CAST(CAST(0 AS DECIMAL(24,6)) AS DECIMAL(24,6)) AS AMT
           ,CAST(CURRE_AMOUNT AS DECIMAL(24,6)) AS BAL
           ,CAST(CAST(0 AS DECIMAL(17,2)) AS DECIMAL(17,2)) AS FEE_AMT
           ,CAST(CAST(0 AS DECIMAL(12,9)) AS DECIMAL(12,9)) AS DOLLAR_RATE
           ,CAST(CAST(0 AS DECIMAL(12,9)) AS DECIMAL(12,9)) AS CNY_RATE
           ,'' AS ACCT_STATE
           ,'' AS V_FLAG
           ,'' AS OTHER_BANK_FLAG
           ,'' AS AGREEMENT_NAME
           ,'' AS ITEM
           ,'' AS SBIT
           ,'' AS SSIT
           ,CAST(CAST(0 AS DECIMAL(24,6)) AS DECIMAL(24,6)) AS LL_RATE
           ,START_DATE AS START_DATE
           ,END_DATE AS ENT_DATE
           ,'' AS TYPE
           ,'' AS STATUS
           ,'' AS HANDLE_USER
           ,CRM_DT AS CRM_DT
           ,'' AS SYS_NO
           ,'' AS DP_TYPE
           ,CAST(CAST(0 AS DECIMAL(17,2)) AS DECIMAL(17,2)) AS TPD_BAL
           ,CAST(CAST(0 AS DECIMAL(17,2)) AS DECIMAL(17,2)) AS BTC_AMT
           ,CAST(CAST(0 AS DECIMAL(17,2)) AS DECIMAL(17,2)) AS CTB_AMT
           ,CAST(CAST(0 AS DECIMAL(17,2)) AS DECIMAL(17,2)) AS EXPECT_INC
           ,START_DATE AS INT_BGN_DATE
           ,END_DATE AS END_DATE
           ,BUY_DATE AS BUY_DATE
           ,FR_ID AS FR_ID
           ,CUST_TYPE AS CUST_TYPE
   FROM ACRM_F_NI_FINANCING A                                  --理财协议表
""" 

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_NI_SIGN = sqlContext.sql(sql)
dfn="ACRM_F_NI_SIGN/"+V_DT+".parquet"
ACRM_F_NI_SIGN.cache()
nrows = ACRM_F_NI_SIGN.count()
ACRM_F_NI_SIGN.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_NI_SIGN.unpersist()
ACRM_F_NI_FINANCING.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_NI_SIGN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
