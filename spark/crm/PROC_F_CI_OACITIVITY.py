#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_OACITIVITY').setMaster(sys.argv[2])
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

F_CI_XDXT_CUSTOMER_OACTIVITY = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_OACTIVITY/*')
F_CI_XDXT_CUSTOMER_OACTIVITY.registerTempTable("F_CI_XDXT_CUSTOMER_OACTIVITY")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()        AS ID 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,A.SERIALNO              AS SERIALNO 
       ,A.INFO                  AS INFO 
       ,A.OTHERBADRECORD        AS OTHERBADRECORD 
       ,A.OCCURORG              AS OCCURORG 
       ,A.BUSINESSTYPE          AS BUSINESSTYPE 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,''                    AS UPDATEDATE 
       ,A.REMARK                AS REMARK 
       ,A.CURRENCY              AS CURRENCY 
       ,CAST(A.BUSINESSSUM  AS DECIMAL(24,6))        AS BUSINESSSUM 
       ,A.UPTODATE              AS UPTODATE 
       ,CAST(A.BALANCE    AS DECIMAL(24,6))            AS BALANCE 
       ,A.CLASSIFYRESULT        AS CLASSIFYRESULT 
       ,A.BEGINDATE             AS BEGINDATE 
       ,A.MATURITY              AS MATURITY 
       ,A.SECURITYWAY           AS SECURITYWAY 
       ,A.PAYDATE               AS PAYDATE 
       ,CAST(A.FINARATE   AS DECIMAL(24,6))            AS FINARATE 
       ,A.BORROWDATE            AS BORROWDATE 
       ,CAST(A.MARGINPERCENT  AS DECIMAL(24,6))        AS MARGINPERCENT 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM F_CI_XDXT_CUSTOMER_OACTIVITY A                         --客户外行业务活动情况
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.CUSTOMERID            = B.SOURCE_CUST_ID 
    AND B.ODS_SYS_ID            = 'LNA' 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_OACITIVITY = sqlContext.sql(sql)
OCRM_F_CI_OACITIVITY.registerTempTable("OCRM_F_CI_OACITIVITY")
dfn="OCRM_F_CI_OACITIVITY/"+V_DT+".parquet"
OCRM_F_CI_OACITIVITY.cache()
nrows = OCRM_F_CI_OACITIVITY.count()
OCRM_F_CI_OACITIVITY.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_OACITIVITY.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_OACITIVITY/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_OACITIVITY lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
