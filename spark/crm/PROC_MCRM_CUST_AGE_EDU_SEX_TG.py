#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MCRM_CUST_AGE_EDU_SEX_TG').setMaster(sys.argv[2])
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

MCRM_CUST_AGE_EDU_SEX = sqlContext.read.parquet(hdfs+'/MCRM_CUST_AGE_EDU_SEX/*')
MCRM_CUST_AGE_EDU_SEX.registerTempTable("MCRM_CUST_AGE_EDU_SEX")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(COUNT(1) AS VARCHAR(10))   AS COUNT_CUST_ID 
       ,ORG_ID                  AS ORG_ID 
       ,SEX                     AS SEX 
       ,EDUCATION               AS EDUCATION 
       ,CUST_LEVEL              AS CUST_LEVEL 
       ,CASE WHEN AGE <= 20 THEN '0-20岁' WHEN AGE <= 30 
    AND AGE > 20 THEN '20-30岁' WHEN AGE <= 40 
    AND AGE > 30 THEN '31-40岁' WHEN AGE <= 50 
    AND AGE > 40 THEN '41-50岁' WHEN AGE <= 60 
    AND AGE > 50 THEN '51-60岁' WHEN AGE > 60 THEN '60岁以上' ELSE '不详' END                     AS AGE 
       ,SUBSTR(V_DT, 1, 7)                       AS ST_DATE 
   FROM MCRM_CUST_AGE_EDU_SEX A                                --客户年龄学历性别报表
  GROUP BY ORG_ID 
       ,SEX 
       ,EDUCATION 
       ,CUST_LEVEL 
       ,AGE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_CUST_AGE_EDU_SEX_TG = sqlContext.sql(sql)
MCRM_CUST_AGE_EDU_SEX_TG.registerTempTable("MCRM_CUST_AGE_EDU_SEX_TG")
dfn="MCRM_CUST_AGE_EDU_SEX_TG/"+V_DT+".parquet"
MCRM_CUST_AGE_EDU_SEX_TG.cache()
nrows = MCRM_CUST_AGE_EDU_SEX_TG.count()
MCRM_CUST_AGE_EDU_SEX_TG.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_CUST_AGE_EDU_SEX_TG.unpersist()
MCRM_CUST_AGE_EDU_SEX.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_CUST_AGE_EDU_SEX_TG/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_CUST_AGE_EDU_SEX_TG lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
