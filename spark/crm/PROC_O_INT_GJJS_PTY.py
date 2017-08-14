#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_INT_GJJS_PTY').setMaster(sys.argv[2])
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

O_CI_GJJS_PTY = sqlContext.read.parquet(hdfs+'/O_CI_GJJS_PTY/*')
O_CI_GJJS_PTY.registerTempTable("O_CI_GJJS_PTY")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.INR                   AS INR 
       ,A.EXTKEY                AS EXTKEY 
       ,A.NAM                   AS NAM 
       ,A.PTYTYP                AS PTYTYP 
       ,A.ACCUSR                AS ACCUSR 
       ,A.HBKACCFLG             AS HBKACCFLG 
       ,A.HBKCONFLG             AS HBKCONFLG 
       ,A.HBKINR                AS HBKINR 
       ,A.HEQACCFLG             AS HEQACCFLG 
       ,A.HEQCONFLG             AS HEQCONFLG 
       ,A.HEQINR                AS HEQINR 
       ,A.PRFCTR                AS PRFCTR 
       ,A.RESUSR                AS RESUSR 
       ,A.RSKCLS                AS RSKCLS 
       ,A.RSKCTY                AS RSKCTY 
       ,A.RSKTXT                AS RSKTXT 
       ,A.UIL                   AS UIL 
       ,A.VER                   AS VER 
       ,A.AKKBRA                AS AKKBRA 
       ,A.AKKCOM                AS AKKCOM 
       ,A.AKKREG                AS AKKREG 
       ,A.LIDCNDFLG             AS LIDCNDFLG 
       ,A.LIDMAXDUR             AS LIDMAXDUR 
       ,A.TRDCNDFLG             AS TRDCNDFLG 
       ,A.TRDTENTOT             AS TRDTENTOT 
       ,A.TRDTENINI             AS TRDTENINI 
       ,A.TRDTENEXT             AS TRDTENEXT 
       ,A.TRDEXTNMB             AS TRDEXTNMB 
       ,A.BADCNDFLG             AS BADCNDFLG 
       ,A.BADTENEXT             AS BADTENEXT 
       ,A.ADRSTA                AS ADRSTA 
       ,A.SELTYP                AS SELTYP 
       ,A.BUYTYP                AS BUYTYP 
       ,A.SLA                   AS SLA 
       ,A.ETGEXTKEY             AS ETGEXTKEY 
       ,A.NAM1                  AS NAM1 
       ,A.BRANCHINR             AS BRANCHINR 
       ,A.JUSCOD                AS JUSCOD 
       ,A.BILVVV                AS BILVVV 
       ,A.CUNQII                AS CUNQII 
       ,A.IDCODE                AS IDCODE 
       ,A.IDTYPE                AS IDTYPE 
       ,A.BCHKEYINR             AS BCHKEYINR 
       ,A.CLSCTY                AS CLSCTY 
       ,A.PROCOD                AS PROCOD 
       ,A.RGCP                  AS RGCP 
       ,A.CRNM                  AS CRNM 
       ,A.CRIT                  AS CRIT 
       ,A.CRID                  AS CRID 
       ,A.RGCPCUR               AS RGCPCUR 
       ,A.CTVC                  AS CTVC 
       ,A.CRDCNT                AS CRDCNT 
       ,A.CLIENTID              AS CLIENTID 
       ,A.CBOSTA                AS CBOSTA 
       ,A.BIDDAT                AS BIDDAT 
       ,A.BCHKEY                AS BCHKEY 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'INT'                   AS ODS_SYS_ID 
   FROM O_CI_GJJS_PTY A                                        --客户资料表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_GJJS_PTY = sqlContext.sql(sql)
F_CI_GJJS_PTY.registerTempTable("F_CI_GJJS_PTY")
dfn="F_CI_GJJS_PTY/"+V_DT+".parquet"
F_CI_GJJS_PTY.cache()
nrows = F_CI_GJJS_PTY.count()
F_CI_GJJS_PTY.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_GJJS_PTY.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_GJJS_PTY/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_GJJS_PTY lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
