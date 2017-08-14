#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_IND_ENT_ADDITIONAL_INFO').setMaster(sys.argv[2])
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

O_CI_XDXT_IND_ENT_ADDITIONAL_INFO = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_IND_ENT_ADDITIONAL_INFO/*')
O_CI_XDXT_IND_ENT_ADDITIONAL_INFO.registerTempTable("O_CI_XDXT_IND_ENT_ADDITIONAL_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUSTOMERID            AS CUSTOMERID 
       ,A.SERIALNO              AS SERIALNO 
       ,A.CUSTOMERNAME          AS CUSTOMERNAME 
       ,A.MOSTBUSINESS          AS MOSTBUSINESS 
       ,A.BUSINESSADD           AS BUSINESSADD 
       ,A.LICENSENO             AS LICENSENO 
       ,A.LICENSEDATE           AS LICENSEDATE 
       ,A.TAXNO                 AS TAXNO 
       ,A.TAXNO1                AS TAXNO1 
       ,A.REMARK                AS REMARK 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_IND_ENT_ADDITIONAL_INFO A                    --个人工商户附加信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_IND_ENT_ADDITIONAL_INFO = sqlContext.sql(sql)
F_CI_XDXT_IND_ENT_ADDITIONAL_INFO.registerTempTable("F_CI_XDXT_IND_ENT_ADDITIONAL_INFO")
dfn="F_CI_XDXT_IND_ENT_ADDITIONAL_INFO/"+V_DT+".parquet"
F_CI_XDXT_IND_ENT_ADDITIONAL_INFO.cache()
nrows = F_CI_XDXT_IND_ENT_ADDITIONAL_INFO.count()
F_CI_XDXT_IND_ENT_ADDITIONAL_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_IND_ENT_ADDITIONAL_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_IND_ENT_ADDITIONAL_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_IND_ENT_ADDITIONAL_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
