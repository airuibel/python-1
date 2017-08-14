#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_INT_GJJS_FCD').setMaster(sys.argv[2])
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

O_TX_GJJS_FCD = sqlContext.read.parquet(hdfs+'/O_TX_GJJS_FCD/*')
O_TX_GJJS_FCD.registerTempTable("O_TX_GJJS_FCD")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.INR                   AS INR 
       ,A.OWNREF                AS OWNREF 
       ,A.NAM                   AS NAM 
       ,A.OWNUSR                AS OWNUSR 
       ,A.CREDAT                AS CREDAT 
       ,A.OPNDAT                AS OPNDAT 
       ,A.CLSDAT                AS CLSDAT 
       ,A.APLPTYINR             AS APLPTYINR 
       ,A.APLPTAINR             AS APLPTAINR 
       ,A.APLNAM                AS APLNAM 
       ,A.APLREF                AS APLREF 
       ,A.CNVDAT                AS CNVDAT 
       ,A.VER                   AS VER 
       ,A.ACCCUR                AS ACCCUR 
       ,A.DCBEXTRAT             AS DCBEXTRAT 
       ,A.SCBEXTRAT             AS SCBEXTRAT 
       ,A.RAT                   AS RAT 
       ,A.ENGACT                AS ENGACT 
       ,A.BCHKEYINR             AS BCHKEYINR 
       ,A.CUR                   AS CUR 
       ,A.AMT                   AS AMT 
       ,A.EXTKEY                AS EXTKEY 
       ,A.RELFLG                AS RELFLG 
       ,A.USEFLG                AS USEFLG 
       ,A.REASON                AS REASON 
       ,A.GLEFLG                AS GLEFLG 
       ,A.REGREF                AS REGREF 
       ,A.BRANCHINR             AS BRANCHINR 
       ,A.EXTACT                AS EXTACT 
       ,A.TRMTYP                AS TRMTYP 
       ,A.ACCAMT                AS ACCAMT 
       ,A.CRDRFLG               AS CRDRFLG 
       ,A.FLGZY                 AS FLGZY 
       ,A.FCDTYP                AS FCDTYP 
       ,A.OBJTYP                AS OBJTYP 
       ,A.OBJINR                AS OBJINR 
       ,A.TRQ                   AS TRQ 
       ,A.ICBEXTRAT             AS ICBEXTRAT 
       ,A.ETYEXTKEY             AS ETYEXTKEY 
       ,A.BCHKEY                AS BCHKEY 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'INT'                   AS ODS_SYS_ID 
   FROM O_TX_GJJS_FCD A                                        --结售汇
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_GJJS_FCD = sqlContext.sql(sql)
F_TX_GJJS_FCD.registerTempTable("F_TX_GJJS_FCD")
dfn="F_TX_GJJS_FCD/"+V_DT+".parquet"
F_TX_GJJS_FCD.cache()
nrows = F_TX_GJJS_FCD.count()
F_TX_GJJS_FCD.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_TX_GJJS_FCD.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_GJJS_FCD/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_GJJS_FCD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
