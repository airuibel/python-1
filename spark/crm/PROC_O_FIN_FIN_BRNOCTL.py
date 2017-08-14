#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_FIN_FIN_BRNOCTL').setMaster(sys.argv[2])
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
#删除当天的
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_FIN_BRNOCTL/"+V_DT+".parquet")
#----------来源表---------------
O_CM_FIN_BRNOCTL = sqlContext.read.parquet(hdfs+'/O_CM_FIN_BRNOCTL/*')
O_CM_FIN_BRNOCTL.registerTempTable("O_CM_FIN_BRNOCTL")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT PRODCODE                AS PRODCODE 
       ,PRODZONENO              AS PRODZONENO 
       ,SUBBKNO                 AS SUBBKNO 
       ,SUBBKNAME               AS SUBBKNAME 
       ,ZONENO                  AS ZONENO 
       ,PURCFLAG                AS PURCFLAG 
       ,SUBBKMINAMT             AS SUBBKMINAMT 
       ,SUBBKMAXAMT             AS SUBBKMAXAMT 
       ,SUBBKAMT                AS SUBBKAMT 
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
   FROM O_CM_FIN_BRNOCTL A                                     --法人内分行额度控制
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_FIN_BRNOCTL = sqlContext.sql(sql)
F_CM_FIN_BRNOCTL.registerTempTable("F_CM_FIN_BRNOCTL")
dfn="F_CM_FIN_BRNOCTL/"+V_DT+".parquet"
F_CM_FIN_BRNOCTL.cache()
nrows = F_CM_FIN_BRNOCTL.count()
F_CM_FIN_BRNOCTL.write.save(path=hdfs + '/' + dfn, mode='append')
F_CM_FIN_BRNOCTL.unpersist()
O_CM_FIN_BRNOCTL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_FIN_BRNOCTL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
