#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_ECUSRMCH').setMaster(sys.argv[2])
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

O_TX_WSYH_ECUSRMCH = sqlContext.read.parquet(hdfs+'/O_TX_WSYH_ECUSRMCH/*')
O_TX_WSYH_ECUSRMCH.registerTempTable("O_TX_WSYH_ECUSRMCH")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.USERSEQ               AS USERSEQ 
       ,A.MCHANNELID            AS MCHANNELID 
       ,A.SECRETNOTICE          AS SECRETNOTICE 
       ,A.USERID                AS USERID 
       ,A.PASSWORD              AS PASSWORD 
       ,A.USERMCHSTATE          AS USERMCHSTATE 
       ,A.ACTIVATIONTIME        AS ACTIVATIONTIME 
       ,A.UPDATEPASSWORDDATE    AS UPDATEPASSWORDDATE 
       ,A.WRONGPASSCOUNT        AS WRONGPASSCOUNT 
       ,A.LOCKTIME              AS LOCKTIME 
       ,A.UNLOCKDATE            AS UNLOCKDATE 
       ,A.FIRSTLOGINTIME        AS FIRSTLOGINTIME 
       ,A.LASTLOGINTIME         AS LASTLOGINTIME 
       ,A.LOGINTIMES            AS LOGINTIMES 
       ,A.LASTLOGINADDR         AS LASTLOGINADDR 
       ,A.MOVESIGN              AS MOVESIGN 
       ,A.CREATEUSERSEQ         AS CREATEUSERSEQ 
       ,A.CREATEDEPTSEQ         AS CREATEDEPTSEQ 
       ,A.CREATETIME            AS CREATETIME 
       ,A.UPDATEUSERSEQ         AS UPDATEUSERSEQ 
       ,A.UPDATEDEPTSEQ         AS UPDATEDEPTSEQ 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.MOBILEPHONE           AS MOBILEPHONE 
       ,A.OPERTYPE              AS OPERTYPE 
       ,A.DEVBIND               AS DEVBIND 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'IBK'                   AS ODS_SYS_ID 
   FROM O_TX_WSYH_ECUSRMCH A                                   --用户渠道表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_ECUSRMCH = sqlContext.sql(sql)
F_TX_WSYH_ECUSRMCH.registerTempTable("F_TX_WSYH_ECUSRMCH")
dfn="F_TX_WSYH_ECUSRMCH/"+V_DT+".parquet"
F_TX_WSYH_ECUSRMCH.cache()
nrows = F_TX_WSYH_ECUSRMCH.count()
F_TX_WSYH_ECUSRMCH.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_TX_WSYH_ECUSRMCH.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_ECUSRMCH/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_WSYH_ECUSRMCH lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
