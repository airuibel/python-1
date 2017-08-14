#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_ACCOUNT_SORT').setMaster(sys.argv[2])
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
ADMIN_AUTH_ACCOUNT_COMPARE = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ACCOUNT_COMPARE/*')
ADMIN_AUTH_ACCOUNT_COMPARE.registerTempTable("ADMIN_AUTH_ACCOUNT_COMPARE")
ADMIN_AUTH_ACCOUNT = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ACCOUNT/*')
ADMIN_AUTH_ACCOUNT.registerTempTable("ADMIN_AUTH_ACCOUNT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(A.ID   AS DECIMAL(27))                 AS ID 
       ,A.ACCOUNT_NAME          AS ACCOUNT_NAME 
       ,A.USER_NAME             AS USER_NAME 
       ,A.ORG_ID                AS ORG_ID 
       ,B.ORG_NAME              AS ORG_NAME 
       ,C.SOURCE_SYS_USER_ID    AS SOURCE_SYS_USER_ID 
       ,CAST(CURRENT_TIMESTAMP   AS VARCHAR(100))   AS UPDATE_DT 
       ,NVL(B.FR_ID, 'UNK')                       AS FR_ID 
   FROM ADMIN_AUTH_ACCOUNT A                                   --系统用户表
  INNER JOIN ADMIN_AUTH_ORG B                                  --ADMIN_AUTH_ORG
     ON A.ORG_ID                = B.ORG_ID 
  INNER JOIN ADMIN_AUTH_ACCOUNT_COMPARE C                      --ADMIN_AUTH_ACCOUNT_COMPARE
     ON A.ACCOUNT_NAME          = C.USER_ID 
  WHERE B.FR_ID IS  NOT NULL 
    AND B.FR_ID <> '' 
  GROUP BY A.ID 
       ,A.ACCOUNT_NAME 
       ,A.USER_NAME 
       ,A.ORG_ID 
       ,B.ORG_NAME 
       ,C.SOURCE_SYS_USER_ID 
       ,NVL(B.FR_ID, 'UNK') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ADMIN_AUTH_ACCOUNT_SORT = sqlContext.sql(sql)
ADMIN_AUTH_ACCOUNT_SORT.registerTempTable("ADMIN_AUTH_ACCOUNT_SORT")
dfn="ADMIN_AUTH_ACCOUNT_SORT/"+V_DT+".parquet"
ADMIN_AUTH_ACCOUNT_SORT.cache()
nrows = ADMIN_AUTH_ACCOUNT_SORT.count()
ADMIN_AUTH_ACCOUNT_SORT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ADMIN_AUTH_ACCOUNT_SORT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ADMIN_AUTH_ACCOUNT_SORT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ADMIN_AUTH_ACCOUNT_SORT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
