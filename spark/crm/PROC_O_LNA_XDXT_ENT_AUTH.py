#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_ENT_AUTH').setMaster(sys.argv[2])
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

O_CI_XDXT_ENT_AUTH = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_ENT_AUTH/*')
O_CI_XDXT_ENT_AUTH.registerTempTable("O_CI_XDXT_ENT_AUTH")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUSTOMERID            AS CUSTOMERID 
       ,A.SERIALNO              AS SERIALNO 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.AUTHDATE              AS AUTHDATE 
       ,A.CERTNAME              AS CERTNAME 
       ,A.CERTID                AS CERTID 
       ,A.CERTRESULT            AS CERTRESULT 
       ,A.CERTORG               AS CERTORG 
       ,A.VALIDDATE             AS VALIDDATE 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.REMARK                AS REMARK 
       ,A.TRADENAME             AS TRADENAME 
       ,A.NAME_RENDING          AS NAME_RENDING 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_ENT_AUTH A                                   --企业资质与认证信息
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_ENT_AUTH = sqlContext.sql(sql)
F_CI_XDXT_ENT_AUTH.registerTempTable("F_CI_XDXT_ENT_AUTH")
dfn="F_CI_XDXT_ENT_AUTH/"+V_DT+".parquet"
F_CI_XDXT_ENT_AUTH.cache()
nrows = F_CI_XDXT_ENT_AUTH.count()
F_CI_XDXT_ENT_AUTH.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_ENT_AUTH.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_ENT_AUTH/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_ENT_AUTH lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
