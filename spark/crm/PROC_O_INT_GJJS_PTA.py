#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_INT_GJJS_PTA').setMaster(sys.argv[2])
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

O_CI_GJJS_PTA = sqlContext.read.parquet(hdfs+'/O_CI_GJJS_PTA/*')
O_CI_GJJS_PTA.registerTempTable("O_CI_GJJS_PTA")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.INR                   AS INR 
       ,A.PTYINR                AS PTYINR 
       ,A.NAM                   AS NAM 
       ,A.PRI                   AS PRI 
       ,A.ENO                   AS ENO 
       ,A.OBJTYP                AS OBJTYP 
       ,A.OBJINR                AS OBJINR 
       ,A.OBJKEY                AS OBJKEY 
       ,A.USG                   AS USG 
       ,A.VER                   AS VER 
       ,A.BIC                   AS BIC 
       ,A.ADRSTA                AS ADRSTA 
       ,A.PTYTYP                AS PTYTYP 
       ,A.PTYEXTKEY             AS PTYEXTKEY 
       ,A.TID                   AS TID 
       ,A.ETGEXTKEY             AS ETGEXTKEY 
       ,A.BRANCHINR             AS BRANCHINR 
       ,A.BCHKEYINR             AS BCHKEYINR 
       ,A.NAM1                  AS NAM1 
       ,A.ISSBCHINF             AS ISSBCHINF 
       ,A.BCHKEY                AS BCHKEY 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'INT'                   AS ODS_SYS_ID 
   FROM O_CI_GJJS_PTA A                                        --名址中间表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_GJJS_PTA = sqlContext.sql(sql)
F_CI_GJJS_PTA.registerTempTable("F_CI_GJJS_PTA")
dfn="F_CI_GJJS_PTA/"+V_DT+".parquet"
F_CI_GJJS_PTA.cache()
nrows = F_CI_GJJS_PTA.count()
F_CI_GJJS_PTA.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_GJJS_PTA.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_GJJS_PTA/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_GJJS_PTA lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
