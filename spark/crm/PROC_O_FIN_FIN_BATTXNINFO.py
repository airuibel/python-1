#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_FIN_FIN_BATTXNINFO').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_FIN_BATTXNINFO/"+V_DT+".parquet")
#----------来源表---------------
O_TX_FIN_BATTXNINFO = sqlContext.read.parquet(hdfs+'/O_TX_FIN_BATTXNINFO/*')
O_TX_FIN_BATTXNINFO.registerTempTable("O_TX_FIN_BATTXNINFO")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT WORKDATE                AS WORKDATE 
       ,BATNO                   AS BATNO 
       ,SEQNO                   AS SEQNO 
       ,OAGENTSERIALNO          AS OAGENTSERIALNO 
       ,BATTYP                  AS BATTYP 
       ,CURRENCY                AS CURRENCY 
       ,PRODCODE                AS PRODCODE 
       ,SALECHL                 AS SALECHL 
       ,PAYERACC                AS PAYERACC 
       ,PAYERNAME               AS PAYERNAME 
       ,PAYEEACC                AS PAYEEACC 
       ,PAYEENAME               AS PAYEENAME 
       ,IDTYPE                  AS IDTYPE 
       ,IDNO                    AS IDNO 
       ,AMOUNT                  AS AMOUNT 
       ,REALAMOUNT              AS REALAMOUNT 
       ,MESSGEFL                AS MESSGEFL 
       ,MESSGEDESC              AS MESSGEDESC 
       ,LOADSEQNO               AS LOADSEQNO 
       ,LOADAMOUNT              AS LOADAMOUNT 
       ,LOADDATE                AS LOADDATE 
       ,SPECTYPE                AS SPECTYPE 
       ,NEWPRODCODE             AS NEWPRODCODE 
       ,NEWAGENTSERIALNO        AS NEWAGENTSERIALNO 
       ,FUNDEALFLG              AS FUNDEALFLG 
       ,LOADFUNC                AS LOADFUNC 
       ,INTRSERIALNO            AS INTRSERIALNO 
       ,INTRSTATE               AS INTRSTATE 
       ,INTRSCODE               AS INTRSCODE 
       ,INTRSDESC               AS INTRSDESC 
       ,APPLBRNO                AS APPLBRNO 
       ,NOTE1                   AS NOTE1 
       ,NOTE2                   AS NOTE2 
       ,NOTE3                   AS NOTE3 
       ,NOTE4                   AS NOTE4 
       ,NOTE5                   AS NOTE5 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'FIN'                   AS ODS_SYS_ID 
   FROM O_TX_FIN_BATTXNINFO A                                  --批量明细表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_FIN_BATTXNINFO = sqlContext.sql(sql)
F_TX_FIN_BATTXNINFO.registerTempTable("F_TX_FIN_BATTXNINFO")
dfn="F_TX_FIN_BATTXNINFO/"+V_DT+".parquet"
F_TX_FIN_BATTXNINFO.cache()
nrows = F_TX_FIN_BATTXNINFO.count()
F_TX_FIN_BATTXNINFO.write.save(path=hdfs + '/' + dfn, mode='append')
F_TX_FIN_BATTXNINFO.unpersist()
O_TX_FIN_BATTXNINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_FIN_BATTXNINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
