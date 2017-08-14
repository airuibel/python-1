#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_FIN_FIN_CUSTRISKINFO').setMaster(sys.argv[2])
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
O_CI_FIN_CUSTRISKINFO = sqlContext.read.parquet(hdfs+'/O_CI_FIN_CUSTRISKINFO/*')
O_CI_FIN_CUSTRISKINFO.registerTempTable("O_CI_FIN_CUSTRISKINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ZONENO                  AS ZONENO 
       ,IDTYPE                  AS IDTYPE 
       ,IDNO                    AS IDNO 
       ,CUSTNAME                AS CUSTNAME 
       ,RISKCODE                AS RISKCODE 
       ,RISKRESULT              AS RISKRESULT 
       ,RISKMARK                AS RISKMARK 
       ,CUSTRISKLEV             AS CUSTRISKLEV 
       ,RISKNAME                AS RISKNAME 
       ,RESUEFFDATE             AS RESUEFFDATE 
       ,DATAORI                 AS DATAORI 
       ,REGZONENO               AS REGZONENO 
       ,REGBRNO                 AS REGBRNO 
       ,REGTELLERNO             AS REGTELLERNO 
       ,REGDATE                 AS REGDATE 
       ,REGTIME                 AS REGTIME 
       ,NOTE1                   AS NOTE1 
       ,NOTE2                   AS NOTE2 
       ,NOTE3                   AS NOTE3 
       ,NOTE4                   AS NOTE4 
       ,NOTE5                   AS NOTE5 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'FIN'                   AS ODS_SYS_ID 
   FROM O_CI_FIN_CUSTRISKINFO A                                --客户风险能力评估表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_FIN_CUSTRISKINFO = sqlContext.sql(sql)
F_CI_FIN_CUSTRISKINFO.registerTempTable("F_CI_FIN_CUSTRISKINFO")
dfn="F_CI_FIN_CUSTRISKINFO/"+V_DT+".parquet"
F_CI_FIN_CUSTRISKINFO.cache()
nrows = F_CI_FIN_CUSTRISKINFO.count()
F_CI_FIN_CUSTRISKINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_FIN_CUSTRISKINFO.unpersist()
O_CI_FIN_CUSTRISKINFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_FIN_CUSTRISKINFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_FIN_CUSTRISKINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
