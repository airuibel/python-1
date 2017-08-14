#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_MSG_DXPT_CUSTOMER').setMaster(sys.argv[2])
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

O_CI_DXPT_CUSTOMER = sqlContext.read.parquet(hdfs+'/O_CI_DXPT_CUSTOMER/*')
O_CI_DXPT_CUSTOMER.registerTempTable("O_CI_DXPT_CUSTOMER")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_NO               AS CUST_NO 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.ID_TYPE               AS ID_TYPE 
       ,A.ID_NO                 AS ID_NO 
       ,A.GENDER                AS GENDER 
       ,A.BIRTHDAY              AS BIRTHDAY 
       ,A.ADDRESS               AS ADDRESS 
       ,A.REMARK                AS REMARK 
       ,A.ORG_ID                AS ORG_ID 
       ,A.LOCKED                AS LOCKED 
       ,A.PER_NO                AS PER_NO 
       ,A.CUST_TYPE             AS CUST_TYPE 
       ,A.CUST_DATE             AS CUST_DATE 
       ,A.PAUSE_DATE            AS PAUSE_DATE 
       ,A.END_DATE              AS END_DATE 
       ,A.IS_ALL                AS IS_ALL 
       ,A.CHL_ID                AS CHL_ID 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'MSG'                   AS ODS_SYS_ID 
   FROM O_CI_DXPT_CUSTOMER A                                   --客户信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_DXPT_CUSTOMER = sqlContext.sql(sql)
F_CI_DXPT_CUSTOMER.registerTempTable("F_CI_DXPT_CUSTOMER")
dfn="F_CI_DXPT_CUSTOMER/"+V_DT+".parquet"
F_CI_DXPT_CUSTOMER.cache()
nrows = F_CI_DXPT_CUSTOMER.count()
F_CI_DXPT_CUSTOMER.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_DXPT_CUSTOMER.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_DXPT_CUSTOMER/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_DXPT_CUSTOMER lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
