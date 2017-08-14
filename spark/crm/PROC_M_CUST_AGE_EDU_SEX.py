#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_CUST_AGE_EDU_SEX').setMaster(sys.argv[2])
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

MCRM_RET_CUST_ASSETS = sqlContext.read.parquet(hdfs+'/MCRM_RET_CUST_ASSETS/*')
MCRM_RET_CUST_ASSETS.registerTempTable("MCRM_RET_CUST_ASSETS")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_ZH_NAME            AS CUST_ZH_NAME 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,SEX                     AS SEX 
       ,CAST(AGE AS VARCHAR(20))                    AS AGE 
       ,EDUCATION               AS EDUCATION 
       ,CUST_LEVEL              AS CUST_LEVEL 
       ,V_DT                    AS ST_DATE 
       ,GRADE_DATE              AS GRADE_DATE 
       ,FR_ID                   AS FR_ID 
   FROM MCRM_RET_CUST_ASSETS A                                 --客户资产情况表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_CUST_AGE_EDU_SEX = sqlContext.sql(sql)
MCRM_CUST_AGE_EDU_SEX.registerTempTable("MCRM_CUST_AGE_EDU_SEX")
dfn="MCRM_CUST_AGE_EDU_SEX/"+V_DT+".parquet"
MCRM_CUST_AGE_EDU_SEX.cache()
nrows = MCRM_CUST_AGE_EDU_SEX.count()
MCRM_CUST_AGE_EDU_SEX.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_CUST_AGE_EDU_SEX.unpersist()
MCRM_RET_CUST_ASSETS.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_CUST_AGE_EDU_SEX/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_CUST_AGE_EDU_SEX lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
