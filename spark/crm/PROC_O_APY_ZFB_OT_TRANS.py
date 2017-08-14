#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_APY_ZFB_OT_TRANS').setMaster(sys.argv[2])
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

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_ZFB_OT_TRANS/"+V_DT+".parquet")
O_TX_ZFB_OT_TRANS = sqlContext.read.parquet(hdfs+'/O_TX_ZFB_OT_TRANS/*')
O_TX_ZFB_OT_TRANS.registerTempTable("O_TX_ZFB_OT_TRANS")

sql = """
 SELECT CAST('' AS VARCHAR(12))                     AS JGM 
       ,TRANS_SEQNO             AS TRANS_SEQNO 
       ,TRANS_HOSTSEQNO         AS TRANS_HOSTSEQNO 
       ,TRANS_MERSEQNO          AS TRANS_MERSEQNO 
       ,TRANS_CLEARDATE         AS TRANS_CLEARDATE 
       ,TRANS_HOSTDATE          AS TRANS_HOSTDATE 
       ,TRANS_DATE              AS TRANS_DATE 
       ,TRANS_MERDATE           AS TRANS_MERDATE 
       ,TRANS_MERDATETIME       AS TRANS_MERDATETIME 
       ,TRANS_ORGMERSEQNO       AS TRANS_ORGMERSEQNO 
       ,TRANS_ORGMERDATE        AS TRANS_ORGMERDATE 
       ,TRANS_ORGSEQNO          AS TRANS_ORGSEQNO 
       ,TRANS_ORGCLEARDATE      AS TRANS_ORGCLEARDATE 
       ,TRANS_ID                AS TRANS_ID 
       ,MER_ID                  AS MER_ID 
       ,TRANS_TERMCODE          AS TRANS_TERMCODE 
       ,MER_OPENDEPTID          AS MER_OPENDEPTID 
       ,MER_DEVELOPDEPTID       AS MER_DEVELOPDEPTID 
       ,CHANNEL_ID              AS CHANNEL_ID 
       ,USER_CIFNO              AS USER_CIFNO 
       ,USER_ACCTNO             AS USER_ACCTNO 
       ,USER_ACCTTYPE           AS USER_ACCTTYPE 
       ,USER_ACCTDEPTID         AS USER_ACCTDEPTID 
       ,TRANS_RCVACCTNO         AS TRANS_RCVACCTNO 
       ,TRANS_RCVACCTDEPTID     AS TRANS_RCVACCTDEPTID 
       ,TRANS_AMT               AS TRANS_AMT 
       ,TRANS_CURRENCY          AS TRANS_CURRENCY 
       ,TRANS_AMT1              AS TRANS_AMT1 
       ,TRANS_AMT2              AS TRANS_AMT2 
       ,TRANS_AMT3              AS TRANS_AMT3 
       ,TRANS_AMT4              AS TRANS_AMT4 
       ,TRANS_FEEAMT            AS TRANS_FEEAMT 
       ,TRANS_HOSTRESPCODE      AS TRANS_HOSTRESPCODE 
       ,TRANS_RESPCODE          AS TRANS_RESPCODE 
       ,TRANS_STATUS            AS TRANS_STATUS 
       ,TRANS_TELLERID          AS TRANS_TELLERID 
       ,TRANS_TERMID            AS TRANS_TERMID 
       ,TRANS_STEP              AS TRANS_STEP 
       ,TRANS_PROCSTATUS        AS TRANS_PROCSTATUS 
       ,TRANS_REMARK1           AS TRANS_REMARK1 
       ,TRANS_DATETIME          AS TRANS_DATETIME 
       ,TRANS_REMARK2           AS TRANS_REMARK2 
       ,TRANS_RCVACCTTYPE       AS TRANS_RCVACCTTYPE 
       ,PAY_TYPE                AS PAY_TYPE 
       ,SIGN_ID                 AS SIGN_ID 
       ,BATCH_NO                AS BATCH_NO 
       ,TRANS_RCVACNAME         AS TRANS_RCVACNAME 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'APY'                   AS ODS_SYS_ID 
   FROM O_TX_ZFB_OT_TRANS A                                    --交易明细表
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_ZFB_OT_TRANS = sqlContext.sql(sql)
F_TX_ZFB_OT_TRANS.registerTempTable("F_TX_ZFB_OT_TRANS")
dfn="F_TX_ZFB_OT_TRANS/"+V_DT+".parquet"
F_TX_ZFB_OT_TRANS.cache()
nrows = F_TX_ZFB_OT_TRANS.count()
F_TX_ZFB_OT_TRANS.write.save(path=hdfs + '/' + dfn, mode='append')
F_TX_ZFB_OT_TRANS.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_ZFB_OT_TRANS lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


