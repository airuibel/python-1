#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_WBK_WXYH_MARKETING').setMaster(sys.argv[2])
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

O_CM_WXYH_MARKETING = sqlContext.read.parquet(hdfs+'/O_CM_WXYH_MARKETING/*')
O_CM_WXYH_MARKETING.registerTempTable("O_CM_WXYH_MARKETING")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.MARKETINGSEQ          AS MARKETINGSEQ 
       ,A.DEPTSEQ               AS DEPTSEQ 
       ,A.POSITION              AS POSITION 
       ,A.LEVEL                 AS LEVEL 
       ,A.TYPE                  AS TYPE 
       ,A.MARTITLE              AS MARTITLE 
       ,A.BEGINDATE             AS BEGINDATE 
       ,A.ENDDATE               AS ENDDATE 
       ,A.STATE                 AS STATE 
       ,A.REMARK                AS REMARK 
       ,A.PUBDATE               AS PUBDATE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'WBK'                   AS ODS_SYS_ID 
   FROM O_CM_WXYH_MARKETING A                                  --F_CM_WXYH_MARKETING
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_WXYH_MARKETING = sqlContext.sql(sql)
F_CM_WXYH_MARKETING.registerTempTable("F_CM_WXYH_MARKETING")
dfn="F_CM_WXYH_MARKETING/"+V_DT+".parquet"
F_CM_WXYH_MARKETING.cache()
nrows = F_CM_WXYH_MARKETING.count()
F_CM_WXYH_MARKETING.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_WXYH_MARKETING.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_WXYH_MARKETING lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
