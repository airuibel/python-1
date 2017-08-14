#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_MCJNL').setMaster(sys.argv[2])
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

O_TX_WSYH_MCJNL = sqlContext.read.parquet(hdfs+'/O_TX_WSYH_MCJNL/*')
O_TX_WSYH_MCJNL.registerTempTable("O_TX_WSYH_MCJNL")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_MCJNL/"+V_DT+".parquet")
#从昨天备表复制一份全量过来
#ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_TX_WSYH_MCJNL_BK/"+V_DT_LD+".parquet /"+dbname+"/F_TX_WSYH_MCJNL/"+V_DT+".parquet")

sql = """
 SELECT A.JNLNO                 AS JNLNO 
       ,A.MCHANNELID            AS MCHANNELID 
       ,A.LOGINTYPE             AS LOGINTYPE 
       ,A.TERMINALTYPE          AS TERMINALTYPE 
       ,A.TERMINALID            AS TERMINALID 
       ,A.DEPTSEQ               AS DEPTSEQ 
       ,A.CIFSEQ                AS CIFSEQ 
       ,A.USERSEQ               AS USERSEQ 
       ,A.TRANSDATE             AS TRANSDATE 
       ,A.TRANSTIME             AS TRANSTIME 
       ,A.TRANSRESPTIME         AS TRANSRESPTIME 
       ,A.TRANSCODE             AS TRANSCODE 
       ,A.SERVICEID             AS SERVICEID 
       ,A.PRDID                 AS PRDID 
       ,A.JNLSTATE              AS JNLSTATE 
       ,A.RETURNCODE            AS RETURNCODE 
       ,A.RETURNMSG             AS RETURNMSG 
       ,A.CHANNELTRANSTIME      AS CHANNELTRANSTIME 
       ,A.CHANNELJNLNO          AS CHANNELJNLNO 
       ,A.PARENTJNLNO           AS PARENTJNLNO 
       ,A.LOCALADDR             AS LOCALADDR 
       ,A.SYSTEMTYPE            AS SYSTEMTYPE 
       ,A.RESPONSEJNLNO         AS RESPONSEJNLNO 
       ,A.PAYERACNO             AS PAYERACNO 
       ,A.PAYERBANK             AS PAYERBANK 
       ,A.PAYERACNAME           AS PAYERACNAME 
       ,A.PAYEEACNO             AS PAYEEACNO 
       ,A.PAYEEBANK             AS PAYEEBANK 
       ,A.PAYEEACNAME           AS PAYEEACNAME 
       ,A.AMOUNT                AS AMOUNT 
       ,A.CURRENCY              AS CURRENCY 
       ,A.REMARK                AS REMARK 
       ,A.CUSTOMERCIFSEQ        AS CUSTOMERCIFSEQ 
       ,A.CUSTOMERNAME          AS CUSTOMERNAME 
       ,A.UUID                  AS UUID 
       ,A.PROCESSTIME           AS PROCESSTIME 
       ,A.AUTHMODE              AS AUTHMODE 
       ,A.FR_ID                 AS FR_ID 
       ,'IBK'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
   FROM O_TX_WSYH_MCJNL A                                      --电子银行日志表
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_MCJNL = sqlContext.sql(sql)
F_TX_WSYH_MCJNL.registerTempTable("F_TX_WSYH_MCJNL")
dfn="F_TX_WSYH_MCJNL/"+V_DT+".parquet"
F_TX_WSYH_MCJNL.cache()
nrows = F_TX_WSYH_MCJNL.count()
F_TX_WSYH_MCJNL.write.save(path=hdfs + '/' + dfn, mode='append')
F_TX_WSYH_MCJNL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_WSYH_MCJNL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

