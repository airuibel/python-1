#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_AFA_EBPS_MAINTRANSDTL').setMaster(sys.argv[2])
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

#清除数据，支持重跑
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_EBPS_MAINTRANSDTL/"+V_DT+".parquet")

O_TX_EBPS_MAINTRANSDTL = sqlContext.read.parquet(hdfs+'/O_TX_EBPS_MAINTRANSDTL/*')
O_TX_EBPS_MAINTRANSDTL.registerTempTable("O_TX_EBPS_MAINTRANSDTL")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT SYSID                   AS SYSID 
       ,UNITNO                  AS UNITNO 
       ,WORKDATE                AS WORKDATE 
       ,WORKTIME                AS WORKTIME 
       ,AGENTSERIALNO           AS AGENTSERIALNO 
       ,TRANSCODE               AS TRANSCODE 
       ,AGENTFLAG               AS AGENTFLAG 
       ,CORPTRADETYPE           AS CORPTRADETYPE 
       ,CHANNELCODE             AS CHANNELCODE 
       ,CHANNELDATE             AS CHANNELDATE 
       ,CHANNELSEQ              AS CHANNELSEQ 
       ,MBFLAG                  AS MBFLAG 
       ,DCFLAG                  AS DCFLAG 
       ,PAYFLAG                 AS PAYFLAG 
       ,BRNO                    AS BRNO 
       ,BRANCHNAMES             AS BRANCHNAMES 
       ,AUTHTELLERNO            AS AUTHTELLERNO 
       ,ENTRUSTDATE             AS ENTRUSTDATE 
       ,SENDBANK                AS SENDBANK 
       ,SENDBANKNAME            AS SENDBANKNAME 
       ,RECVBANK                AS RECVBANK 
       ,RECVBANKNAME            AS RECVBANKNAME 
       ,CURRENCY                AS CURRENCY 
       ,AMOUNT                  AS AMOUNT 
       ,FEEAMOUNT               AS FEEAMOUNT 
       ,MSGID                   AS MSGID 
       ,PAYERACC                AS PAYERACC 
       ,PAYERACCTYPE            AS PAYERACCTYPE 
       ,PAYEROPNBANK            AS PAYEROPNBANK 
       ,PAYERNAME               AS PAYERNAME 
       ,PAYERACCBANKNAME        AS PAYERACCBANKNAME 
       ,PAYEEACC                AS PAYEEACC 
       ,PAYEEACCTYPE            AS PAYEEACCTYPE 
       ,PAYEEOPNBANK            AS PAYEEOPNBANK 
       ,PAYEENAME               AS PAYEENAME 
       ,PAYEEACCBANKNAME        AS PAYEEACCBANKNAME 
       ,PROTOCOLNO              AS PROTOCOLNO 
       ,TRADEBUSISTEP           AS TRADEBUSISTEP 
       ,TRADESTEP               AS TRADESTEP 
       ,TRADESTATUS             AS TRADESTATUS 
       ,CENTERDATE              AS CENTERDATE 
       ,CENTERROUND             AS CENTERROUND 
       ,CENTERSTATUS            AS CENTERSTATUS 
       ,CENTERERRCODE           AS CENTERERRCODE 
       ,CENTERERRMSG            AS CENTERERRMSG 
       ,ERRORCODE               AS ERRORCODE 
       ,ERRORMSG                AS ERRORMSG 
       ,CORPCHKFLAG             AS CORPCHKFLAG 
       ,CORPBUSITYPE            AS CORPBUSITYPE 
       ,CORPBUSIKIND            AS CORPBUSIKIND 
       ,HOSTSERIALNO            AS HOSTSERIALNO 
       ,BANKSYSTYPE             AS BANKSYSTYPE 
       ,BANKSYSDATE             AS BANKSYSDATE 
       ,BANKSYSTIME             AS BANKSYSTIME 
       ,BANKSYSSEQ              AS BANKSYSSEQ 
       ,BANKSYSSTATUS           AS BANKSYSSTATUS 
       ,BANKSYSERRCODE          AS BANKSYSERRCODE 
       ,BANKSYSERRMSG           AS BANKSYSERRMSG 
       ,BANKCHKFLAG             AS BANKCHKFLAG 
       ,PRINTNUM                AS PRINTNUM 
       ,REMARK1                 AS REMARK1 
       ,REMARK2                 AS REMARK2 
       ,REMARK3                 AS REMARK3 
       ,REMARK4                 AS REMARK4 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'AFA'                   AS ODS_SYS_ID 
   FROM O_TX_EBPS_MAINTRANSDTL A                               --超级网银业务流水表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_EBPS_MAINTRANSDTL = sqlContext.sql(sql)
F_TX_EBPS_MAINTRANSDTL.registerTempTable("F_TX_EBPS_MAINTRANSDTL")
dfn="F_TX_EBPS_MAINTRANSDTL/"+V_DT+".parquet"
F_TX_EBPS_MAINTRANSDTL.cache()
nrows = F_TX_EBPS_MAINTRANSDTL.count()
F_TX_EBPS_MAINTRANSDTL.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_TX_EBPS_MAINTRANSDTL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_EBPS_MAINTRANSDTL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
