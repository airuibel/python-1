#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_ZDH_ZZDH_HISTORY_LS').setMaster(sys.argv[2])
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

O_TX_ZDH_ZZDH_HISTORY_LS = sqlContext.read.parquet(hdfs+'/O_TX_ZDH_ZZDH_HISTORY_LS/*')
O_TX_ZDH_ZZDH_HISTORY_LS.registerTempTable("O_TX_ZDH_ZZDH_HISTORY_LS")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.HOST_DATE             AS HOST_DATE 
       ,A.HOST_TIME             AS HOST_TIME 
       ,A.PAN                   AS PAN 
       ,A.AMOUNT                AS AMOUNT 
       ,A.CARD_TYPE             AS CARD_TYPE 
       ,A.TRANS_TYPE            AS TRANS_TYPE 
       ,A.TRACE                 AS TRACE 
       ,A.RETRIREFNUM           AS RETRIREFNUM 
       ,A.AUTH_CODE             AS AUTH_CODE 
       ,A.TERMINAL_ID           AS TERMINAL_ID 
       ,A.MERCHANT_ID           AS MERCHANT_ID 
       ,A.ACCOUNT2              AS ACCOUNT2 
       ,A.ADDI_AMOUNT           AS ADDI_AMOUNT 
       ,A.BATCH_NO              AS BATCH_NO 
       ,A.PSAM_NO               AS PSAM_NO 
       ,A.INVOICE               AS INVOICE 
       ,A.RETURN_CODE           AS RETURN_CODE 
       ,A.HOST_RET_CODE         AS HOST_RET_CODE 
       ,A.CANCEL_FLAG           AS CANCEL_FLAG 
       ,A.RECOVER_FLAG          AS RECOVER_FLAG 
       ,A.POS_SETTLE            AS POS_SETTLE 
       ,A.POS_BATCH             AS POS_BATCH 
       ,A.HOST_SETTLE           AS HOST_SETTLE 
       ,A.POSP_TRACE            AS POSP_TRACE 
       ,A.OLDRETRIREFNUM        AS OLDRETRIREFNUM 
       ,A.POS_DATE              AS POS_DATE 
       ,A.POS_TIME              AS POS_TIME 
       ,A.USER_CODE1            AS USER_CODE1 
       ,A.USER_CODE2            AS USER_CODE2 
       ,A.BANK_ID               AS BANK_ID 
       ,A.SETTLE_DATE           AS SETTLE_DATE 
       ,A.OPER_NO               AS OPER_NO 
       ,A.DEPTDETAIL            AS DEPTDETAIL 
       ,A.ICC_SEQNUM            AS ICC_SEQNUM 
       ,A.ICC_DATA              AS ICC_DATA 
       ,A.MAC                   AS MAC 
       ,A.SHOP_NAME             AS SHOP_NAME 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'ZDH'                   AS ODS_SYS_ID 
   FROM O_TX_ZDH_ZZDH_HISTORY_LS A                             --金融进村2期历史流水表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_ZDH_ZZDH_HISTORY_LS = sqlContext.sql(sql)
F_TX_ZDH_ZZDH_HISTORY_LS.registerTempTable("F_TX_ZDH_ZZDH_HISTORY_LS")
dfn="F_TX_ZDH_ZZDH_HISTORY_LS/"+V_DT+".parquet"
F_TX_ZDH_ZZDH_HISTORY_LS.cache()
nrows = F_TX_ZDH_ZZDH_HISTORY_LS.count()
F_TX_ZDH_ZZDH_HISTORY_LS.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_TX_ZDH_ZZDH_HISTORY_LS.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_ZDH_ZZDH_HISTORY_LS lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
