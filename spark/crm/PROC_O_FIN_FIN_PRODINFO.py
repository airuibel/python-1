#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_FIN_FIN_PRODINFO').setMaster(sys.argv[2])
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
O_CM_FIN_PRODINFO = sqlContext.read.parquet(hdfs+'/O_CM_FIN_PRODINFO/*')
O_CM_FIN_PRODINFO.registerTempTable("O_CM_FIN_PRODINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.PRODCODE              AS PRODCODE 
       ,A.PRODZONENO            AS PRODZONENO 
       ,A.PRODKIND              AS PRODKIND 
       ,A.PRODSETTP             AS PRODSETTP 
       ,A.PRODNO                AS PRODNO 
       ,A.PRODNAME              AS PRODNAME 
       ,A.BRAND                 AS BRAND 
       ,A.PERIOD                AS PERIOD 
       ,A.PURCCUR               AS PURCCUR 
       ,A.CAPITCUR              AS CAPITCUR 
       ,A.BENEFCUR              AS BENEFCUR 
       ,A.CUSTTYPE              AS CUSTTYPE 
       ,A.PURCTYPE              AS PURCTYPE 
       ,A.MANAGETYPE            AS MANAGETYPE 
       ,A.RUNTYPE               AS RUNTYPE 
       ,A.PRODTYPE              AS PRODTYPE 
       ,A.BUSITYPE              AS BUSITYPE 
       ,A.CAPITFLAG             AS CAPITFLAG 
       ,A.BENEFFLAG             AS BENEFFLAG 
       ,A.ESTCUSTHYR            AS ESTCUSTHYR 
       ,A.ESTCUSTLYR            AS ESTCUSTLYR 
       ,A.PURCSDATE             AS PURCSDATE 
       ,A.PURCEDATE             AS PURCEDATE 
       ,A.PREENDFL              AS PREENDFL 
       ,A.CUSTBACKFL            AS CUSTBACKFL 
       ,A.INCRFLAG              AS INCRFLAG 
       ,A.INCRBRTYPE            AS INCRBRTYPE 
       ,A.INCRTYPE              AS INCRTYPE 
       ,A.ICHDEPBRNAME          AS ICHDEPBRNAME 
       ,A.ICHDEPBRNO            AS ICHDEPBRNO 
       ,A.OCHDEPBRNAT           AS OCHDEPBRNAT 
       ,A.OCHDEPBRNAME          AS OCHDEPBRNAME 
       ,A.PRODSDATE             AS PRODSDATE 
       ,A.PRODEDATE             AS PRODEDATE 
       ,A.PURCCHANNEL           AS PURCCHANNEL 
       ,A.PURCLAMT              AS PURCLAMT 
       ,A.PURCHAMT              AS PURCHAMT 
       ,A.SGPURCHAMT            AS SGPURCHAMT 
       ,A.BRPURCHAMT            AS BRPURCHAMT 
       ,A.PERAMOUT              AS PERAMOUT 
       ,A.SGPURCLPER            AS SGPURCLPER 
       ,A.SGPURCHPER            AS SGPURCHPER 
       ,A.BRPURCLPER            AS BRPURCLPER 
       ,A.BRPURCHPER            AS BRPURCHPER 
       ,A.RISKCODE              AS RISKCODE 
       ,A.PRODRISKLEV           AS PRODRISKLEV 
       ,A.SALERANGE             AS SALERANGE 
       ,A.FEERATE               AS FEERATE 
       ,A.PURCACCT              AS PURCACCT 
       ,A.PURCACCTNAME          AS PURCACCTNAME 
       ,A.PURCENDFL             AS PURCENDFL 
       ,A.PRODSTATE             AS PRODSTATE 
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
   FROM O_CM_FIN_PRODINFO A                                    --理财产品参数表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_FIN_PRODINFO = sqlContext.sql(sql)
F_CM_FIN_PRODINFO.registerTempTable("F_CM_FIN_PRODINFO")
dfn="F_CM_FIN_PRODINFO/"+V_DT+".parquet"
F_CM_FIN_PRODINFO.cache()
nrows = F_CM_FIN_PRODINFO.count()
F_CM_FIN_PRODINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_FIN_PRODINFO.unpersist()
O_CM_FIN_PRODINFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_FIN_PRODINFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_FIN_PRODINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
