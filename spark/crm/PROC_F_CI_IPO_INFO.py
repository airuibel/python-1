#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_IPO_INFO').setMaster(sys.argv[2])
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

OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_CI_XDXT_ENT_IPO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_ENT_IPO/*')
F_CI_XDXT_ENT_IPO.registerTempTable("F_CI_XDXT_ENT_IPO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()  AS MXTID 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,A.SERIALNO              AS SERIALNO 
       ,A.IPODATE               AS IPODATE 
       ,A.BOURSENAME            AS BOURSENAME 
       ,A.STOCKCODE             AS STOCKCODE 
       ,A.STOCKTYPE             AS STOCKTYPE 
       ,A.STOCKNAME             AS STOCKNAME 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,''                    AS UPDATEDATE 
       ,A.REMARK                AS REMARK 
       ,CAST(A.STOCKAMOUNT   AS DECIMAL(24))        AS STOCKAMOUNT 
       ,A.STOCKCURRENCY         AS STOCKCURRENCY 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.FR_ID                 AS FR_ID 
   FROM F_CI_XDXT_ENT_IPO A                                    --股票信息表
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.CUSTOMERID            = B.SOURCE_CUST_ID 
    AND B.ODS_SYS_ID            = 'LNA' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_IPO_INFO = sqlContext.sql(sql)
OCRM_F_CI_IPO_INFO.registerTempTable("OCRM_F_CI_IPO_INFO")
dfn="OCRM_F_CI_IPO_INFO/"+V_DT+".parquet"
OCRM_F_CI_IPO_INFO.cache()
nrows = OCRM_F_CI_IPO_INFO.count()
OCRM_F_CI_IPO_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_IPO_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_IPO_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_IPO_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
