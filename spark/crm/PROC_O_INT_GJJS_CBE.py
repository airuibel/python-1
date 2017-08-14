#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_INT_GJJS_CBE').setMaster(sys.argv[2])
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

O_TX_GJJS_CBE = sqlContext.read.parquet(hdfs+'/O_TX_GJJS_CBE/*')
O_TX_GJJS_CBE.registerTempTable("O_TX_GJJS_CBE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.INR                   AS INR 
       ,A.OBJTYP                AS OBJTYP 
       ,A.OBJINR                AS OBJINR 
       ,A.EXTID                 AS EXTID 
       ,A.CBT                   AS CBT 
       ,A.TRNTYP                AS TRNTYP 
       ,A.TRNINR                AS TRNINR 
       ,A.DAT                   AS DAT 
       ,A.CUR                   AS CUR 
       ,A.AMT                   AS AMT 
       ,A.RELFLG                AS RELFLG 
       ,A.CREDAT                AS CREDAT 
       ,A.XRFCUR                AS XRFCUR 
       ,A.XRFAMT                AS XRFAMT 
       ,A.NAM                   AS NAM 
       ,A.ACC                   AS ACC 
       ,A.ACC2                  AS ACC2 
       ,A.OPTDAT                AS OPTDAT 
       ,A.GLEDAT                AS GLEDAT 
       ,A.CHKFLG                AS CHKFLG 
       ,A.BCHKEY                AS BCHKEY 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'INT'                   AS ODS_SYS_ID 
   FROM O_TX_GJJS_CBE A                                        --发生额
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_GJJS_CBE = sqlContext.sql(sql)
F_TX_GJJS_CBE.registerTempTable("F_TX_GJJS_CBE")
dfn="F_TX_GJJS_CBE/"+V_DT+".parquet"
F_TX_GJJS_CBE.cache()
nrows = F_TX_GJJS_CBE.count()
F_TX_GJJS_CBE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_TX_GJJS_CBE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_GJJS_CBE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_GJJS_CBE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
