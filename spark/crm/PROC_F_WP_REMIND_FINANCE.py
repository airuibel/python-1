#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_WP_REMIND_FINANCE').setMaster(sys.argv[2])
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

ACRM_F_NI_FINANCING = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_FINANCING/*')
ACRM_F_NI_FINANCING.registerTempTable("ACRM_F_NI_FINANCING")
OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
OCRM_F_WP_REMIND_RULE = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_RULE/*')
OCRM_F_WP_REMIND_RULE.registerTempTable("OCRM_F_WP_REMIND_RULE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id() AS ID 
       ,E.RULE_ID               AS RULE_ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.ACCOUNT               AS ACCT_NO 
       ,C.PRODUCT_ID            AS PROD_ID 
       ,C.PROD_NAME             AS PROD_NAME 
       ,A.CURRE_AMOUNT          AS ACCT_BAL 
       ,A.END_DATE              AS END_DATE 
       ,'A000109'               AS REMIND_TYPE 
       ,V_DT                    AS MSG_CRT_DATE 
       ,E.ORG_ID                AS FR_ID 
   FROM OCRM_F_WP_REMIND_RULE E                                --提醒规则表
  INNER JOIN ACRM_F_NI_FINANCING A                             --理财协议表
     ON E.CUST_TYPE             = A.CUST_TYPE 
    AND A.FR_ID                 = E.ORG_ID 
  INNER JOIN OCRM_F_PD_PROD_INFO C                             --产品表
     ON A.PRODUCT_ID            = C.PRODUCT_ID 
    AND C.FR_ID                 = E.ORG_ID 
  WHERE E.REMIND_TYPE           = 'A000109' 
    AND A.END_DATE              = date_add(V_DT ,coalesce(E.BEFOREHEAD_DAY,0)) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND_FINANCE = sqlContext.sql(sql)
OCRM_F_WP_REMIND_FINANCE.registerTempTable("OCRM_F_WP_REMIND_FINANCE")
dfn="OCRM_F_WP_REMIND_FINANCE/"+V_DT+".parquet"
OCRM_F_WP_REMIND_FINANCE.cache()
nrows = OCRM_F_WP_REMIND_FINANCE.count()

#删除当天数据，支持重跑
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND_FINANCE/"+V_DT+".parquet ")
OCRM_F_WP_REMIND_FINANCE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND_FINANCE.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND_FINANCE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND_FINANCE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
