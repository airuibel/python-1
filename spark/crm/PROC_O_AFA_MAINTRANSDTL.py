#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_AFA_MAINTRANSDTL').setMaster(sys.argv[2])
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

##原表
O_TX_AFA_MAINTRANSDTL = sqlContext.read.parquet(hdfs+'/O_TX_AFA_MAINTRANSDTL/*')
O_TX_AFA_MAINTRANSDTL.registerTempTable("O_TX_AFA_MAINTRANSDTL")
#目标表 ：F_TX_AFA_MAINTRANSDTL 增量
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_AFA_MAINTRANSDTL/"+V_DT+".parquet")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT WORKDATE                AS WORKDATE 
       ,AGENTSERIALNO           AS AGENTSERIALNO 
       ,SYSID                   AS SYSID 
       ,UNITNO                  AS UNITNO 
       ,SUBUNITNO               AS SUBUNITNO 
       ,AGENTFLAG               AS AGENTFLAG 
       ,TRANSCODE               AS TRANSCODE 
       ,CHANNELCODE             AS CHANNELCODE 
       ,CHANNELDATE             AS CHANNELDATE 
       ,CHANNELSEQ              AS CHANNELSEQ 
       ,BRNO                    AS BRNO 
       ,TELLERNO                AS TELLERNO 
       ,CHKTELLERNO             AS CHKTELLERNO 
       ,AUTHTELLERNO            AS AUTHTELLERNO 
       ,SENDTELLERNO            AS SENDTELLERNO 
       ,TERMINALNO              AS TERMINALNO 
       ,MBFLAG                  AS MBFLAG 
       ,DCFLAG                  AS DCFLAG 
       ,TRANSFLAG               AS TRANSFLAG 
       ,CASHPRONO               AS CASHPRONO 
       ,ACCCLASS                AS ACCCLASS 
       ,CLEARACCSEQ             AS CLEARACCSEQ 
       ,CURRENCY                AS CURRENCY 
       ,AMOUNT                  AS AMOUNT 
       ,REALAMOUNT              AS REALAMOUNT 
       ,FEEFLAG                 AS FEEFLAG 
       ,TRANSFEECODE            AS TRANSFEECODE 
       ,FEECODE                 AS FEECODE 
       ,COSTFEECODE             AS COSTFEECODE 
       ,FEEAMOUNT               AS FEEAMOUNT 
       ,TRANSAMOUNT             AS TRANSAMOUNT 
       ,COSTAMOUNT              AS COSTAMOUNT 
       ,ENTRUSTDATE             AS ENTRUSTDATE 
       ,BUSSEQNO                AS BUSSEQNO 
       ,SENDBANK                AS SENDBANK 
       ,SENDSETTLEBANK          AS SENDSETTLEBANK 
       ,PAYEROPNBANK            AS PAYEROPNBANK 
       ,PAYERACC                AS PAYERACC 
       ,PAYERNAME               AS PAYERNAME 
       ,PAYERADDR               AS PAYERADDR 
       ,RECVBANK                AS RECVBANK 
       ,RECVSETTLEBANK          AS RECVSETTLEBANK 
       ,PAYEEOPNBANK            AS PAYEEOPNBANK 
       ,PAYEEACC                AS PAYEEACC 
       ,PAYEENAME               AS PAYEENAME 
       ,PAYEEADDR               AS PAYEEADDR 
       ,VOUCHTYPE               AS VOUCHTYPE 
       ,VOUCHDATE               AS VOUCHDATE 
       ,VOUCHNO                 AS VOUCHNO 
       ,IDTYPE                  AS IDTYPE 
       ,IDNO                    AS IDNO 
       ,THIRDSYSERRCODE         AS THIRDSYSERRCODE 
       ,THIRDSYSERRMSG          AS THIRDSYSERRMSG 
       ,THIRDCHKFLAG            AS THIRDCHKFLAG 
       ,BANKCHKFLAG             AS BANKCHKFLAG 
       ,TRADEBUSISTEP           AS TRADEBUSISTEP 
       ,TRADESTEP               AS TRADESTEP 
       ,TRADESTATUS             AS TRADESTATUS 
       ,STATUS                  AS STATUS 
       ,PRIORITY                AS PRIORITY 
       ,POSTSCRIPT              AS POSTSCRIPT 
       ,BOOKNAME                AS BOOKNAME 
       ,TRUSTPAYNO              AS TRUSTPAYNO 
       ,BUSTYPE                 AS BUSTYPE 
       ,BUSSUBTYPE              AS BUSSUBTYPE 
       ,PREWORKDATE             AS PREWORKDATE 
       ,PREAGENTSERIALNO        AS PREAGENTSERIALNO 
       ,PRINTCNT                AS PRINTCNT 
       ,PKGAGTDATE              AS PKGAGTDATE 
       ,PKGNO                   AS PKGNO 
       ,PKGTYPE                 AS PKGTYPE 
       ,PKGCOSEQ                AS PKGCOSEQ 
       ,CLRDTATE                AS CLRDTATE 
       ,BATSEQNO                AS BATSEQNO 
       ,AGENTPROTOCOLNO         AS AGENTPROTOCOLNO 
       ,OTXSTAT                 AS OTXSTAT 
       ,RESPONSELIMIT           AS RESPONSELIMIT 
       ,RESPONSEDATE            AS RESPONSEDATE 
       ,NOTE1                   AS NOTE1 
       ,NOTE2                   AS NOTE2 
       ,NOTE3                   AS NOTE3 
       ,NOTE4                   AS NOTE4 
       ,NOTE5                   AS NOTE5 
       ,ACCTBRNO                AS ACCTBRNO 
       ,CNAPSTYPE               AS CNAPSTYPE 
       ,CNAPSMSGTYPE            AS CNAPSMSGTYPE 
       ,OTXREJCTCODE            AS OTXREJCTCODE 
       ,OTXREJCTMSG             AS OTXREJCTMSG 
       ,CHKBUSTYPE              AS CHKBUSTYPE 
       ,CHKBUSSUBTYPE           AS CHKBUSSUBTYPE 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'AFA'                   AS ODS_SYS_ID 
   FROM O_TX_AFA_MAINTRANSDTL A                                --统一业务流水表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_AFA_MAINTRANSDTL = sqlContext.sql(sql)
F_TX_AFA_MAINTRANSDTL.registerTempTable("F_TX_AFA_MAINTRANSDTL")
dfn="F_TX_AFA_MAINTRANSDTL/"+V_DT+".parquet"
F_TX_AFA_MAINTRANSDTL.cache()
nrows = F_TX_AFA_MAINTRANSDTL.count()
F_TX_AFA_MAINTRANSDTL.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_TX_AFA_MAINTRANSDTL.unpersist()

et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_AFA_MAINTRANSDTL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
