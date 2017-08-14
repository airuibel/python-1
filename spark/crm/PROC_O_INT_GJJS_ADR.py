#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_INT_GJJS_ADR').setMaster(sys.argv[2])
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

O_CI_GJJS_ADR = sqlContext.read.parquet(hdfs+'/O_CI_GJJS_ADR/*')
O_CI_GJJS_ADR.registerTempTable("O_CI_GJJS_ADR")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.INR                   AS INR 
       ,A.EXTKEY                AS EXTKEY 
       ,A.NAM                   AS NAM 
       ,A.BIC                   AS BIC 
       ,A.BICAUT                AS BICAUT 
       ,A.BID                   AS BID 
       ,A.BLZ                   AS BLZ 
       ,A.CLC                   AS CLC 
       ,A.DPT                   AS DPT 
       ,A.EML                   AS EML 
       ,A.FAX1                  AS FAX1 
       ,A.FAX2                  AS FAX2 
       ,A.NAM1                  AS NAM1 
       ,A.NAM2                  AS NAM2 
       ,A.NAM3                  AS NAM3 
       ,A.STR1                  AS STR1 
       ,A.STR2                  AS STR2 
       ,A.LOCZIP                AS LOCZIP 
       ,A.LOCTXT                AS LOCTXT 
       ,A.LOC2                  AS LOC2 
       ,A.LOCCTY                AS LOCCTY 
       ,A.CORTYP                AS CORTYP 
       ,A.POB                   AS POB 
       ,A.POBZIP                AS POBZIP 
       ,A.POBTXT                AS POBTXT 
       ,A.TEL1                  AS TEL1 
       ,A.TEL2                  AS TEL2 
       ,A.TID                   AS TID 
       ,A.TLX                   AS TLX 
       ,A.TLXAUT                AS TLXAUT 
       ,A.UIL                   AS UIL 
       ,A.VER                   AS VER 
       ,A.MANMOD                AS MANMOD 
       ,A.RTGFLG                AS RTGFLG 
       ,A.TARFLG                AS TARFLG 
       ,A.DTACID                AS DTACID 
       ,A.DTECID                AS DTECID 
       ,A.ETGEXTKEY             AS ETGEXTKEY 
       ,A.ISBCHFLG              AS ISBCHFLG 
       ,A.BCHIDN                AS BCHIDN 
       ,A.ADR1                  AS ADR1 
       ,A.ADR2                  AS ADR2 
       ,A.ADR3                  AS ADR3 
       ,A.ADR4                  AS ADR4 
       ,A.EDICID                AS EDICID 
       ,A.CLITYP                AS CLITYP 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'INT'                   AS ODS_SYS_ID 
   FROM O_CI_GJJS_ADR A                                        --ADDRESS名址
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_GJJS_ADR = sqlContext.sql(sql)
F_CI_GJJS_ADR.registerTempTable("F_CI_GJJS_ADR")
dfn="F_CI_GJJS_ADR/"+V_DT+".parquet"
F_CI_GJJS_ADR.cache()
nrows = F_CI_GJJS_ADR.count()
F_CI_GJJS_ADR.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_GJJS_ADR.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_GJJS_ADR/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_GJJS_ADR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
