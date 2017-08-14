#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_JTFCROSSBANK').setMaster(sys.argv[2])
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

O_TX_WSYH_JTFCROSSBANK = sqlContext.read.parquet(hdfs+'/O_TX_WSYH_JTFCROSSBANK/*')
O_TX_WSYH_JTFCROSSBANK.registerTempTable("O_TX_WSYH_JTFCROSSBANK")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_JTFCROSSBANK/"+V_DT+".parquet")


sql = """
 SELECT A.JNLNO                 AS JNLNO 
       ,A.SCHEDULEFLAG          AS SCHEDULEFLAG 
       ,A.SCHEDULERULE          AS SCHEDULERULE 
       ,A.PROCESSSTATE          AS PROCESSSTATE 
       ,A.PAYERBANKACTYPE       AS PAYERBANKACTYPE 
       ,A.PAYERACNO             AS PAYERACNO 
       ,A.CORECIFNO             AS CORECIFNO 
       ,A.PAYERSUBACNO          AS PAYERSUBACNO 
       ,A.PAYERSUBACTYPE        AS PAYERSUBACTYPE 
       ,A.PAYERSUBACSEQ         AS PAYERSUBACSEQ 
       ,A.PAYERCURRENCYCODE     AS PAYERCURRENCYCODE 
       ,A.PAYERCRFLAG           AS PAYERCRFLAG 
       ,A.PAYEEBANKPROV         AS PAYEEBANKPROV 
       ,A.PAYEEBANKCITY         AS PAYEEBANKCITY 
       ,A.PAYEEBANKID           AS PAYEEBANKID 
       ,A.PAYEEBANKDEPTID       AS PAYEEBANKDEPTID 
       ,A.PAYEECIFTYPE          AS PAYEECIFTYPE 
       ,A.PAYEEACNAME           AS PAYEEACNAME 
       ,A.PAYEEBANKACTYPE       AS PAYEEBANKACTYPE 
       ,A.PAYEEACNO             AS PAYEEACNO 
       ,A.CITYCODE              AS CITYCODE 
       ,A.PAYEECURRENCYCODE     AS PAYEECURRENCYCODE 
       ,A.PAYEECRFLAG           AS PAYEECRFLAG 
       ,A.TRSCURRENCYCODE       AS TRSCURRENCYCODE 
       ,A.TRSCRFLAG             AS TRSCRFLAG 
       ,A.TRSAMOUNT             AS TRSAMOUNT 
       ,A.NOTICEPAYEEFLAG       AS NOTICEPAYEEFLAG 
       ,A.PAYEEMOBILE           AS PAYEEMOBILE 
       ,A.FUNDUSAGE             AS FUNDUSAGE 
       ,A.ATTACHINFO            AS ATTACHINFO 
       ,A.TRSPASSWORD           AS TRSPASSWORD 
       ,A.PRIORITY              AS PRIORITY 
       ,A.PAYERCHANNEL          AS PAYERCHANNEL 
       ,A.MPFLAG                AS MPFLAG 
       ,A.SAMECITYIFLAG         AS SAMECITYIFLAG 
       ,A.PAYEEBANKCLEARID      AS PAYEEBANKCLEARID 
       ,A.SVCID                 AS SVCID 
       ,A.ISADDTOPAYEEBOOK      AS ISADDTOPAYEEBOOK 
       ,A.PAYEEACID             AS PAYEEACID 
       ,A.NOTICE                AS NOTICE 
       ,A.PROCESSTIME           AS PROCESSTIME 
       ,A.BANKCODE              AS BANKCODE 
       ,A.PAYEEBANKPROVNAME     AS PAYEEBANKPROVNAME 
       ,A.PAYEEBANKCITYNAME     AS PAYEEBANKCITYNAME 
       ,A.PAYEEBANKIDNAME       AS PAYEEBANKIDNAME 
       ,A.PAYEEBANKDEPTIDNAME   AS PAYEEBANKDEPTIDNAME 
       ,A.PARENTJNLNO           AS PARENTJNLNO 
       ,A.PAYERACNAME           AS PAYERACNAME 
       ,A.BATCHJNLNO            AS BATCHJNLNO 
       ,A.TRSTYPEFLAG           AS TRSTYPEFLAG 
       ,A.MOBILE                AS MOBILE 
       ,A.ORDERID               AS ORDERID 
       ,A.ENTINNERNO            AS ENTINNERNO 
       ,A.TRANSDATE             AS TRANSDATE 
       ,A.FEECODE               AS FEECODE 
       ,A.FEEAMOUNT             AS FEEAMOUNT 
       ,A.PAYEEEMAIL            AS PAYEEEMAIL 
       ,A.BATCHNOTICESEQ        AS BATCHNOTICESEQ 
       ,A.BATCHNOTICEFLAG       AS BATCHNOTICEFLAG 
       ,A.ERRORDAY              AS ERRORDAY 
       ,A.ERRORTIME             AS ERRORTIME 
       ,A.PAYERBANKORGID        AS PAYERBANKORGID 
       ,A.PAYERBANKORGNAME      AS PAYERBANKORGNAME 
       ,A.FEENAME               AS FEENAME 
       ,A.CTRANSFERJNLNO        AS CTRANSFERJNLNO 
       ,A.FR_ID                 AS FR_ID 
       ,'IBK'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
   FROM O_TX_WSYH_JTFCROSSBANK A                               --跨行转账交易流水
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_JTFCROSSBANK = sqlContext.sql(sql)
F_TX_WSYH_JTFCROSSBANK.registerTempTable("F_TX_WSYH_JTFCROSSBANK")
dfn="F_TX_WSYH_JTFCROSSBANK/"+V_DT+".parquet"
F_TX_WSYH_JTFCROSSBANK.cache()
nrows = F_TX_WSYH_JTFCROSSBANK.count()
F_TX_WSYH_JTFCROSSBANK.write.save(path=hdfs + '/' + dfn, mode='append')
F_TX_WSYH_JTFCROSSBANK.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_WSYH_JTFCROSSBANK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)





