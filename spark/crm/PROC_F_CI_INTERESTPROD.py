#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_INTERESTPROD').setMaster(sys.argv[2])
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

INTEREST_PRODUCT_CFG = sqlContext.read.parquet(hdfs+'/INTEREST_PRODUCT_CFG/*')
INTEREST_PRODUCT_CFG.registerTempTable("INTEREST_PRODUCT_CFG")
OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")

TARGET_FEATURE_CFG = sqlContext.read.parquet(hdfs+'/TARGET_FEATURE_CFG/*')
TARGET_FEATURE_CFG.registerTempTable("TARGET_FEATURE_CFG")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,B.PROD_TYPE             AS PROD_TYPE 
       ,B.PROD_NAME             AS PROD_NAME 
       ,B.PRODUCT_ID            AS PROD_NUMBER 
       ,A.CUST_ID               AS CUST_ID 
       ,V_DT               AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
 FROM OCRM_F_CI_PER_CUST_INFO A                          
 INNER JOIN INTEREST_PRODUCT_CFG B                      
     ON A.FR_ID  = B.FR_ID 
   where  (A.CUST_SEX = B.SEX OR B.SEX IS NULL) 
     AND(A.CUST_OCCUP_COD = B.OCCUP  OR B.OCCUP IS NULL) 
      AND(A.CUST_EDU_LVL_COD = B.EDUCATION   OR B.EDUCATION IS NULL) 
      AND(A.CUST_MRG = B.MARRIGE  OR B.MARRIGE IS NULL) 
      AND((EXISTS( SELECT 1  FROM TARGET_FEATURE_CFG T 
                             ,OCRM_F_CI_CUST_DESC C 
                          WHERE T.TARGET_ID             = '100002' 
                            AND T.TARGET_VALUE          = B.AGE 
                            AND A.CUST_ID               = C.CUST_ID 
                            AND C.CUST_TYP              = '1' 
                            AND C.FR_ID                 = A.FR_ID 
                            AND C.CERT_TYPE             = '0' 
                            AND YEAR(V_DT) - INT(SUBSTR(C.CERT_NUM, 7, 4)) > INT(T.TARGET_LOW) 
                            AND YEAR(V_DT) - INT(SUBSTR(C.CERT_NUM, 7, 4)) <= INT(T.TARGET_HIGH)) 
           AND B.AGE IS NOT NULL AND B.AGE <> '') or TRIM(B.AGE) IS NULL) 
      AND((EXISTS  ( SELECT 1 FROM TARGET_FEATURE_CFG D 
                          WHERE D.TARGET_ID             = '100003' 
                            AND D.TARGET_VALUE          = B.INCOME 
                            AND A.CUST_ANNUAL_INCOME > INT(D.TARGET_LOW) 
                            AND A.CUST_ANNUAL_INCOME <= INT(D.TARGET_HIGH)) 
          AND B.INCOME IS NOT NULL AND B.INCOME <> '')
         or trim(B.INCOME) is null) 
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INTERESTPROD = sqlContext.sql(sql)
OCRM_F_CI_INTERESTPROD.registerTempTable("OCRM_F_CI_INTERESTPROD")
dfn="OCRM_F_CI_INTERESTPROD/"+V_DT+".parquet"
OCRM_F_CI_INTERESTPROD.cache()
nrows = OCRM_F_CI_INTERESTPROD.count()
OCRM_F_CI_INTERESTPROD.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INTERESTPROD.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_INTERESTPROD/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INTERESTPROD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
