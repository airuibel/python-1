#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_WBK_WXYH_BINDRELATIONALINFO').setMaster(sys.argv[2])
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

O_CI_WXYH_BINDRELATIONALINFO = sqlContext.read.parquet(hdfs+'/O_CI_WXYH_BINDRELATIONALINFO/*')
O_CI_WXYH_BINDRELATIONALINFO.registerTempTable("O_CI_WXYH_BINDRELATIONALINFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.JNLNO                 AS JNLNO 
       ,A.WEIXINID              AS WEIXINID 
       ,A.ACNO                  AS ACNO 
       ,A.ACTYPE                AS ACTYPE 
       ,A.IDNO                  AS IDNO 
       ,A.IDTYPE                AS IDTYPE 
       ,A.STATE                 AS STATE 
       ,A.BINDDATE              AS BINDDATE 
       ,A.CIFNO                 AS CIFNO 
       ,A.CIFNAME               AS CIFNAME 
       ,A.DEPTID                AS DEPTID 
       ,A.DEPTNAME              AS DEPTNAME 
       ,A.PARENTDEPTID          AS PARENTDEPTID 
       ,A.PARENTDEPTNAME        AS PARENTDEPTNAME 
       ,A.MOBILEPHONE           AS MOBILEPHONE 
       ,A.PASSWORD              AS PASSWORD 
       ,A.EMAIL                 AS EMAIL 
       ,A.ADDR                  AS ADDR 
       ,A.ISDEFAULT             AS ISDEFAULT 
       ,A.CHANNEL               AS CHANNEL 
       ,A.NOTIFYFLAG            AS NOTIFYFLAG 
       ,A.RECOMMENDPEOPLE       AS RECOMMENDPEOPLE 
       ,A.REMARK                AS REMARK 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'WBK'                   AS ODS_SYS_ID 
   FROM O_CI_WXYH_BINDRELATIONALINFO A                         --用户绑定信息表
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WXYH_BINDRELATIONALINFO = sqlContext.sql(sql)
F_CI_WXYH_BINDRELATIONALINFO.registerTempTable("F_CI_WXYH_BINDRELATIONALINFO")
dfn="F_CI_WXYH_BINDRELATIONALINFO/"+V_DT+".parquet"
F_CI_WXYH_BINDRELATIONALINFO.cache()
nrows = F_CI_WXYH_BINDRELATIONALINFO.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WXYH_BINDRELATIONALINFO/"+V_DT+".parquet")
F_CI_WXYH_BINDRELATIONALINFO.write.save(path=hdfs + '/' + dfn, mode='append')
F_CI_WXYH_BINDRELATIONALINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_WXYH_BINDRELATIONALINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
