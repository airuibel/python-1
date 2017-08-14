#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_FIN_MID_BATCHINFO').setMaster(sys.argv[2])
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

#----------来源表---------------
O_TX_MID_BATCHINFO = sqlContext.read.parquet(hdfs+'/O_TX_MID_BATCHINFO/*')
O_TX_MID_BATCHINFO.registerTempTable("O_TX_MID_BATCHINFO")

#目标表：F_TX_MID_BATCHINFO 增改表
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_MID_BATCHINFO/*.parquet")
F_TX_MID_BATCHINFO = sqlContext.read.parquet(hdfs+'/F_TX_MID_BATCHINFO_BK/'+V_DT_LD+'.parquet')
F_TX_MID_BATCHINFO.registerTempTable("F_TX_MID_BATCHINFO")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.SYSID                 AS SYSID 
       ,A.SUBSYSID              AS SUBSYSID 
       ,A.WORKDATE              AS WORKDATE 
       ,A.BATNO                 AS BATNO 
       ,A.WORKTIME              AS WORKTIME 
       ,A.CRPCOD                AS CRPCOD 
       ,A.ITMCOD                AS ITMCOD 
       ,A.BATTYP                AS BATTYP 
       ,A.SUBBATTYP             AS SUBBATTYP 
       ,A.CTLFLG                AS CTLFLG 
       ,A.ACCTYP                AS ACCTYP 
       ,A.SAVTIM                AS SAVTIM 
       ,A.RESAVTIM              AS RESAVTIM 
       ,A.TOTALCOUNT            AS TOTALCOUNT 
       ,A.TOTALAMOUNT           AS TOTALAMOUNT 
       ,A.SUCCESSCOUNT          AS SUCCESSCOUNT 
       ,A.SUCCESSAMOUNT         AS SUCCESSAMOUNT 
       ,A.BATSTAT               AS BATSTAT 
       ,A.FILENAME              AS FILENAME 
       ,A.FILELEN               AS FILELEN 
       ,A.FILETYPE              AS FILETYPE 
       ,A.PRODUCTCODE           AS PRODUCTCODE 
       ,A.FILESEQ               AS FILESEQ 
       ,A.RELEASE               AS RELEASE 
       ,A.BANKFILESTATE         AS BANKFILESTATE 
       ,A.BANKSQNO              AS BANKSQNO 
       ,A.BANKQUEUENO           AS BANKQUEUENO 
       ,A.DATASOURCE            AS DATASOURCE 
       ,A.ZONENO                AS ZONENO 
       ,A.BRNO                  AS BRNO 
       ,A.TELLERNO              AS TELLERNO 
       ,A.CMTDAT                AS CMTDAT 
       ,A.CMTBRNO               AS CMTBRNO 
       ,A.CMTTELLERNO           AS CMTTELLERNO 
       ,A.CHANNELCODE           AS CHANNELCODE 
       ,A.CHANNELSEQ            AS CHANNELSEQ 
       ,A.CHANNELDATE           AS CHANNELDATE 
       ,A.THIRDFILE             AS THIRDFILE 
       ,A.DOWNFILENAME          AS DOWNFILENAME 
       ,A.ERRORCODE             AS ERRORCODE 
       ,A.ERRORMS               AS ERRORMS 
       ,A.NOTE1                 AS NOTE1 
       ,A.NOTE2                 AS NOTE2 
       ,A.NOTE3                 AS NOTE3 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'FIN'                   AS ODS_SYS_ID 
   FROM O_TX_MID_BATCHINFO A                                   --批次信息汇总登记薄
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
O_TX_MID_BATCHINFO_INNER1 = sqlContext.sql(sql)
O_TX_MID_BATCHINFO_INNER1.registerTempTable("O_TX_MID_BATCHINFO_INNER1")

sql = """
 SELECT A.SYSID                 AS SYSID 
       ,A.SUBSYSID              AS SUBSYSID 
       ,A.WORKDATE              AS WORKDATE 
       ,A.BATNO                 AS BATNO 
       ,A.WORKTIME              AS WORKTIME 
       ,A.CRPCOD                AS CRPCOD 
       ,A.ITMCOD                AS ITMCOD 
       ,A.BATTYP                AS BATTYP 
       ,A.SUBBATTYP             AS SUBBATTYP 
       ,A.CTLFLG                AS CTLFLG 
       ,A.ACCTYP                AS ACCTYP 
       ,A.SAVTIM                AS SAVTIM 
       ,A.RESAVTIM              AS RESAVTIM 
       ,A.TOTALCOUNT            AS TOTALCOUNT 
       ,A.TOTALAMOUNT           AS TOTALAMOUNT 
       ,A.SUCCESSCOUNT          AS SUCCESSCOUNT 
       ,A.SUCCESSAMOUNT         AS SUCCESSAMOUNT 
       ,A.BATSTAT               AS BATSTAT 
       ,A.FILENAME              AS FILENAME 
       ,A.FILELEN               AS FILELEN 
       ,A.FILETYPE              AS FILETYPE 
       ,A.PRODUCTCODE           AS PRODUCTCODE 
       ,A.FILESEQ               AS FILESEQ 
       ,A.RELEASE               AS RELEASE 
       ,A.BANKFILESTATE         AS BANKFILESTATE 
       ,A.BANKSQNO              AS BANKSQNO 
       ,A.BANKQUEUENO           AS BANKQUEUENO 
       ,A.DATASOURCE            AS DATASOURCE 
       ,A.ZONENO                AS ZONENO 
       ,A.BRNO                  AS BRNO 
       ,A.TELLERNO              AS TELLERNO 
       ,A.CMTDAT                AS CMTDAT 
       ,A.CMTBRNO               AS CMTBRNO 
       ,A.CMTTELLERNO           AS CMTTELLERNO 
       ,A.CHANNELCODE           AS CHANNELCODE 
       ,A.CHANNELSEQ            AS CHANNELSEQ 
       ,A.CHANNELDATE           AS CHANNELDATE 
       ,A.THIRDFILE             AS THIRDFILE 
       ,A.DOWNFILENAME          AS DOWNFILENAME 
       ,A.ERRORCODE             AS ERRORCODE 
       ,A.ERRORMS               AS ERRORMS 
       ,A.NOTE1                 AS NOTE1 
       ,A.NOTE2                 AS NOTE2 
       ,A.NOTE3                 AS NOTE3 
       ,A.FR_ID                 AS FR_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
   FROM F_TX_MID_BATCHINFO A                                   --批次信息汇总登记薄
        LEFT JOIN O_TX_MID_BATCHINFO_INNER1 B ON A.BATNO=B.BATNO AND A.WORKDATE=B.WORKDATE
		WHERE B.BATNO IS NULL	
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
O_TX_MID_BATCHINFO_INNER2 = sqlContext.sql(sql)
UNIONALL=O_TX_MID_BATCHINFO_INNER2.unionAll(O_TX_MID_BATCHINFO_INNER1)
dfn="F_TX_MID_BATCHINFO/"+V_DT+".parquet"
UNIONALL.cache()
nrows = UNIONALL.count()
UNIONALL.write.save(path=hdfs + '/' + dfn, mode='overwrite')
UNIONALL.unpersist()
O_TX_MID_BATCHINFO_INNER2.unpersist()

#增改表复制当天数据备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_MID_BATCHINFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_TX_MID_BATCHINFO/"+V_DT+".parquet /"+dbname+"/F_TX_MID_BATCHINFO_BK/"+V_DT+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_MID_BATCHINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
