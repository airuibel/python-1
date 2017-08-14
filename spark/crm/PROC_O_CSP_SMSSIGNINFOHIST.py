#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CSP_SMSSIGNINFOHIST').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CSP_SMSSIGNINFOHIST/"+V_DT+".parquet")
#----------来源表---------------
O_CSP_SMSSIGNINFOHIST = sqlContext.read.parquet(hdfs+'/O_CSP_SMSSIGNINFOHIST/*')
O_CSP_SMSSIGNINFOHIST.registerTempTable("O_CSP_SMSSIGNINFOHIST")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT MAINTJNLNO              AS MAINTJNLNO 
       ,MAINTCODE               AS MAINTCODE 
       ,MAINTMCHANNELID         AS MAINTMCHANNELID 
       ,MAINTDATE               AS MAINTDATE 
       ,MAINTTIMESTAMP          AS MAINTTIMESTAMP 
       ,SMSUSER                 AS SMSUSER 
       ,CIFNO                   AS CIFNO 
       ,STATE                   AS STATE 
       ,CIFTYPE                 AS CIFTYPE 
       ,GENDER                  AS GENDER 
       ,SIGNDATE                AS SIGNDATE 
       ,ENDDATE                 AS ENDDATE 
       ,SIGNTYPE                AS SIGNTYPE 
       ,SIGNDEPT                AS SIGNDEPT 
       ,ADDTELLERNO             AS ADDTELLERNO 
       ,CREATETIME              AS CREATETIME 
       ,UPDTELLERNO             AS UPDTELLERNO 
       ,UPDATETIME              AS UPDATETIME 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CSP'                   AS ODS_SYS_ID 
   FROM O_CSP_SMSSIGNINFOHIST A                                --短信平台签约信息历史表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CSP_SMSSIGNINFOHIST = sqlContext.sql(sql)
F_CSP_SMSSIGNINFOHIST.registerTempTable("F_CSP_SMSSIGNINFOHIST")
dfn="F_CSP_SMSSIGNINFOHIST/"+V_DT+".parquet"
F_CSP_SMSSIGNINFOHIST.cache()
nrows = F_CSP_SMSSIGNINFOHIST.count()
F_CSP_SMSSIGNINFOHIST.write.save(path=hdfs + '/' + dfn, mode='append')
F_CSP_SMSSIGNINFOHIST.unpersist()
O_CSP_SMSSIGNINFOHIST.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CSP_SMSSIGNINFOHIST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
