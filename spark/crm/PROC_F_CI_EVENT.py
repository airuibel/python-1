#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_EVENT').setMaster(sys.argv[2])
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

F_CI_XDXT_CUSTOMER_MEMO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_MEMO/*')
F_CI_XDXT_CUSTOMER_MEMO.registerTempTable("F_CI_XDXT_CUSTOMER_MEMO")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT 
            A.SERIALNO                              AS EVENT_ID  
           ,B.ODS_CUST_ID                           AS CUST_ID   
           ,A.EVENTNAME                             AS EVENT_NAME
           ,B.ODS_CUST_NAME                         AS CUST_NAME 
           ,A.EVENTTYPE                            AS EVENT_TYP 
           ,A.EVENTDESCRIBE                        AS EVENT_DESC
           ,NVL(A.OCCURDATE,A.INPUTDATE)       AS FS_DT     
           ,A.INPUTUSERID                           AS WHRY      
           ,A.INPUTDATE                             AS WHDT      
           ,''                                    AS WARN_FLG  
           ,A.ODS_ST_DATE                           AS ODS_DT    
           ,B.ODS_SYS_ID                            AS ODS_SYS_ID
           ,C.FR_ID                                 AS FR_ID     
           ,A.INPUTORGID                             AS ORG_ID    
    FROM F_CI_XDXT_CUSTOMER_MEMO A, OCRM_F_CI_SYS_RESOURCE B,ADMIN_AUTH_ORG C 
    WHERE A.CUSTOMERID = B.SOURCE_CUST_ID 
    AND B.ODS_SYS_ID = 'LNA' AND A.INPUTORGID=C.ORG_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_EVENT = sqlContext.sql(sql)
ACRM_F_CI_EVENT.registerTempTable("ACRM_F_CI_EVENT")
dfn="ACRM_F_CI_EVENT/"+V_DT+".parquet"
ACRM_F_CI_EVENT.cache()
nrows = ACRM_F_CI_EVENT.count()
ACRM_F_CI_EVENT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_EVENT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_EVENT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_EVENT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
