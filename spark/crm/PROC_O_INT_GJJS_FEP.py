#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_INT_GJJS_FEP').setMaster(sys.argv[2])
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

O_NI_GJJS_FEP = sqlContext.read.parquet(hdfs+'/O_NI_GJJS_FEP/*')
O_NI_GJJS_FEP.registerTempTable("O_NI_GJJS_FEP")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.INR                   AS INR 
       ,A.FEECOD                AS FEECOD 
       ,A.OBJTYP                AS OBJTYP 
       ,A.OBJINR                AS OBJINR 
       ,A.RELOBJTYP             AS RELOBJTYP 
       ,A.RELOBJINR             AS RELOBJINR 
       ,A.EXTKEY                AS EXTKEY 
       ,A.NAM                   AS NAM 
       ,A.RELCUR                AS RELCUR 
       ,A.RELAMT                AS RELAMT 
       ,A.DAT1                  AS DAT1 
       ,A.DAT2                  AS DAT2 
       ,A.MODFLG                AS MODFLG 
       ,A.UNT                   AS UNT 
       ,A.UNTAMT                AS UNTAMT 
       ,A.RATCAL                AS RATCAL 
       ,A.RAT                   AS RAT 
       ,A.MINMAXFLG             AS MINMAXFLG 
       ,A.CUR                   AS CUR 
       ,A.AMT                   AS AMT 
       ,A.XRFCUR                AS XRFCUR 
       ,A.XRFAMT                AS XRFAMT 
       ,A.FEEACC                AS FEEACC 
       ,A.INFDETSTM             AS INFDETSTM 
       ,A.PTYINR                AS PTYINR 
       ,A.SRCTRNINR             AS SRCTRNINR 
       ,A.SRCDAT                AS SRCDAT 
       ,A.RPLTRNINR             AS RPLTRNINR 
       ,A.RPLDAT                AS RPLDAT 
       ,A.DONTRNINR             AS DONTRNINR 
       ,A.DONDAT                AS DONDAT 
       ,A.ADVTRNINR             AS ADVTRNINR 
       ,A.ADVDAT                AS ADVDAT 
       ,A.ACRINR                AS ACRINR 
       ,A.SEPINR                AS SEPINR 
       ,A.ROL                   AS ROL 
       ,A.FLG                   AS FLG 
       ,A.FLG1                  AS FLG1 
       ,A.BASEAMT               AS BASEAMT 
       ,A.STAFLG                AS STAFLG 
       ,A.PRVFEPINR             AS PRVFEPINR 
       ,A.BCHKEY                AS BCHKEY 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'INT'                   AS ODS_SYS_ID 
   FROM O_NI_GJJS_FEP A                                        --费用明细
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_NI_GJJS_FEP = sqlContext.sql(sql)
F_NI_GJJS_FEP.registerTempTable("F_NI_GJJS_FEP")
dfn="F_NI_GJJS_FEP/"+V_DT+".parquet"
F_NI_GJJS_FEP.cache()
nrows = F_NI_GJJS_FEP.count()
F_NI_GJJS_FEP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_NI_GJJS_FEP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_NI_GJJS_FEP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_NI_GJJS_FEP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
