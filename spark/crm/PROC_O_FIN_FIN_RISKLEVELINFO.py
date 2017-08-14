#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_FIN_FIN_RISKLEVELINFO').setMaster(sys.argv[2])
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
#----------来源表---------------
O_CI_FIN_RISKLEVELINFO = sqlContext.read.parquet(hdfs+'/O_CI_FIN_RISKLEVELINFO/*')
O_CI_FIN_RISKLEVELINFO.registerTempTable("O_CI_FIN_RISKLEVELINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.RISKCODE              AS RISKCODE 
       ,A.RISKLEV               AS RISKLEV 
       ,A.RISKNAME              AS RISKNAME 
       ,A.RISKSMARK             AS RISKSMARK 
       ,A.RISKEMARK             AS RISKEMARK 
       ,A.RISKFLAG              AS RISKFLAG 
       ,A.REGZONENO             AS REGZONENO 
       ,A.REGBRNO               AS REGBRNO 
       ,A.REGTELLERNO           AS REGTELLERNO 
       ,A.REGDATE               AS REGDATE 
       ,A.REGTIME               AS REGTIME 
       ,A.NOTE1                 AS NOTE1 
       ,A.NOTE2                 AS NOTE2 
       ,A.NOTE3                 AS NOTE3 
       ,A.NOTE4                 AS NOTE4 
       ,A.NOTE5                 AS NOTE5 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'FIN'                   AS ODS_SYS_ID 
   FROM O_CI_FIN_RISKLEVELINFO A                               --客户风险能力评估级别参数表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_FIN_RISKLEVELINFO = sqlContext.sql(sql)
F_CI_FIN_RISKLEVELINFO.registerTempTable("F_CI_FIN_RISKLEVELINFO")
dfn="F_CI_FIN_RISKLEVELINFO/"+V_DT+".parquet"
F_CI_FIN_RISKLEVELINFO.cache()
nrows = F_CI_FIN_RISKLEVELINFO.count()
F_CI_FIN_RISKLEVELINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_FIN_RISKLEVELINFO.unpersist()
O_CI_FIN_RISKLEVELINFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_FIN_RISKLEVELINFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_FIN_RISKLEVELINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
