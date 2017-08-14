#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_INT_GJJS_PTC').setMaster(sys.argv[2])
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

O_CM_GJJS_PTC = sqlContext.read.parquet(hdfs+'/O_CM_GJJS_PTC/*')
O_CM_GJJS_PTC.registerTempTable("O_CM_GJJS_PTC")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.PTYINR                AS PTYINR 
       ,A.PTAINR                AS PTAINR 
       ,A.NAM                   AS NAM 
       ,A.ENO                   AS ENO 
       ,A.DEP                   AS DEP 
       ,A.GEN                   AS GEN 
       ,A.BIDDAT                AS BIDDAT 
       ,A.TELFAC                AS TELFAC 
       ,A.TELFAX                AS TELFAX 
       ,A.TELMOB                AS TELMOB 
       ,A.TELOFF                AS TELOFF 
       ,A.VER                   AS VER 
       ,A.EML                   AS EML 
       ,A.ETGEXTKEY             AS ETGEXTKEY 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'INT'                   AS ODS_SYS_ID 
   FROM O_CM_GJJS_PTC A                                        --利息中间表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_GJJS_PTC = sqlContext.sql(sql)
F_CM_GJJS_PTC.registerTempTable("F_CM_GJJS_PTC")
dfn="F_CM_GJJS_PTC/"+V_DT+".parquet"
F_CM_GJJS_PTC.cache()
nrows = F_CM_GJJS_PTC.count()
F_CM_GJJS_PTC.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_GJJS_PTC.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_GJJS_PTC/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_GJJS_PTC lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
