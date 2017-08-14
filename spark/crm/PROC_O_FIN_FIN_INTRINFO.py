#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_FIN_FIN_INTRINFO').setMaster(sys.argv[2])
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
O_CM_FIN_INTRINFO = sqlContext.read.parquet(hdfs+'/O_CM_FIN_INTRINFO/*')
O_CM_FIN_INTRINFO.registerTempTable("O_CM_FIN_INTRINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT PRODCODE                AS PRODCODE 
       ,INTRNO                  AS INTRNO 
       ,INTRFLAG                AS INTRFLAG 
       ,CHANNELCODE             AS CHANNELCODE 
       ,EFFMINAMT               AS EFFMINAMT 
       ,EFFMAXAMT               AS EFFMAXAMT 
       ,EFFDATE                 AS EFFDATE 
       ,MESSFLAG                AS MESSFLAG 
       ,MESSSTIME               AS MESSSTIME 
       ,MESSETIME               AS MESSETIME 
       ,MESSDESC                AS MESSDESC 
       ,PAYTYPE                 AS PAYTYPE 
       ,INTRTYPE                AS INTRTYPE 
       ,PERINTRRATE             AS PERINTRRATE 
       ,REALINTEDATE            AS REALINTEDATE 
       ,YEARDATENUM             AS YEARDATENUM 
       ,PERINTRAMT              AS PERINTRAMT 
       ,VALIDFLAG               AS VALIDFLAG 
       ,TRYFLAG                 AS TRYFLAG 
       ,TRYAMT                  AS TRYAMT 
       ,TRYBRNO                 AS TRYBRNO 
       ,TRYTELLERNO             AS TRYTELLERNO 
       ,TRYDATE                 AS TRYDATE 
       ,TRYTIME                 AS TRYTIME 
       ,REGZONENO               AS REGZONENO 
       ,REGBRNO                 AS REGBRNO 
       ,REGTELLERNO             AS REGTELLERNO 
       ,REGDATE                 AS REGDATE 
       ,REGTIME                 AS REGTIME 
       ,DEALFLAG                AS DEALFLAG 
       ,NOTE1                   AS NOTE1 
       ,NOTE2                   AS NOTE2 
       ,NOTE3                   AS NOTE3 
       ,NOTE4                   AS NOTE4 
       ,NOTE5                   AS NOTE5 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'FIN'                   AS ODS_SYS_ID 
   FROM O_CM_FIN_INTRINFO A                                    --付本付息参数表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_FIN_INTRINFO = sqlContext.sql(sql)
F_CM_FIN_INTRINFO.registerTempTable("F_CM_FIN_INTRINFO")
dfn="F_CM_FIN_INTRINFO/"+V_DT+".parquet"
F_CM_FIN_INTRINFO.cache()
nrows = F_CM_FIN_INTRINFO.count()
F_CM_FIN_INTRINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_FIN_INTRINFO.unpersist()
O_CM_FIN_INTRINFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_FIN_INTRINFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_FIN_INTRINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
