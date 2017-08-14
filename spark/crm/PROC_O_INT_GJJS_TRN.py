#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_INT_GJJS_TRN').setMaster(sys.argv[2])
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

O_TX_GJJS_TRN = sqlContext.read.parquet(hdfs+'/O_TX_GJJS_TRN/*')
O_TX_GJJS_TRN.registerTempTable("O_TX_GJJS_TRN")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.INR                   AS INR 
       ,A.INIDATTIM             AS INIDATTIM 
       ,A.INIFRM                AS INIFRM 
       ,A.INIUSR                AS INIUSR 
       ,A.ININAM                AS ININAM 
       ,A.OWNREF                AS OWNREF 
       ,A.OBJTYP                AS OBJTYP 
       ,A.OBJINR                AS OBJINR 
       ,A.OBJNAM                AS OBJNAM 
       ,A.SSNINR                AS SSNINR 
       ,A.SMHNXT                AS SMHNXT 
       ,A.USG                   AS USG 
       ,A.USR                   AS USR 
       ,A.CPLDATTIM             AS CPLDATTIM 
       ,A.INFDSP                AS INFDSP 
       ,A.INFTXT                AS INFTXT 
       ,A.RELFLG                AS RELFLG 
       ,A.COMFLG                AS COMFLG 
       ,A.COMDAT                AS COMDAT 
       ,A.CORTRNINR             AS CORTRNINR 
       ,A.XREFLG                AS XREFLG 
       ,A.XRECURBLK             AS XRECURBLK 
       ,A.RELCUR                AS RELCUR 
       ,A.RELAMT                AS RELAMT 
       ,A.RELORICUR             AS RELORICUR 
       ,A.RELORIAMT             AS RELORIAMT 
       ,A.RELREQ                AS RELREQ 
       ,A.RELRES                AS RELRES 
       ,A.CNFFLG                AS CNFFLG 
       ,A.EVTTXT                AS EVTTXT 
       ,A.RPRUSR                AS RPRUSR 
       ,A.ORDINR                AS ORDINR 
       ,A.EXEDAT                AS EXEDAT 
       ,A.CLRFLG                AS CLRFLG 
       ,A.ETYEXTKEY             AS ETYEXTKEY 
       ,A.BCHKEYINR             AS BCHKEYINR 
       ,A.ACCBCHINR             AS ACCBCHINR 
       ,A.TRXSQNB               AS TRXSQNB 
       ,A.TRXSQNBADD            AS TRXSQNBADD 
       ,A.ACCSNDFLG             AS ACCSNDFLG 
       ,A.RELREQ0               AS RELREQ0 
       ,A.RELREQ1               AS RELREQ1 
       ,A.RELREQ2               AS RELREQ2 
       ,A.RELRES0               AS RELRES0 
       ,A.RELRES1               AS RELRES1 
       ,A.RELRES2               AS RELRES2 
       ,A.IMGINR                AS IMGINR 
       ,A.CMTFLG                AS CMTFLG 
       ,A.ESBINR                AS ESBINR 
       ,A.BCHKEY                AS BCHKEY 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'INT'                   AS ODS_SYS_ID 
   FROM O_TX_GJJS_TRN A                                        --国际业务流水信息
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_GJJS_TRN = sqlContext.sql(sql)
F_TX_GJJS_TRN.registerTempTable("F_TX_GJJS_TRN")
dfn="F_TX_GJJS_TRN/"+V_DT+".parquet"
F_TX_GJJS_TRN.cache()
nrows = F_TX_GJJS_TRN.count()
F_TX_GJJS_TRN.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_TX_GJJS_TRN.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_GJJS_TRN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
