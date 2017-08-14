#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_SLNA_SUNCUST_DELAYCREDIT').setMaster(sys.argv[2])
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

O_LN_SUNCUST_DELAYCREDIT = sqlContext.read.parquet(hdfs+'/O_LN_SUNCUST_DELAYCREDIT/*')
O_LN_SUNCUST_DELAYCREDIT.registerTempTable("O_LN_SUNCUST_DELAYCREDIT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.SERIALNO              AS SERIALNO 
       ,A.CUSTOMERID            AS CUSTOMERID 
       ,A.CUSTOMERNAME          AS CUSTOMERNAME 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CERTID                AS CERTID 
       ,A.FAMILYID              AS FAMILYID 
       ,A.AFFIRMTYPE            AS AFFIRMTYPE 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.UPDATEDATE            AS UPDATEDATE 
       ,A.FLAG                  AS FLAG 
       ,A.TEMPSAVEFLAG          AS TEMPSAVEFLAG 
       ,A.DELAYCREDITREASON     AS DELAYCREDITREASON 
       ,A.FINISHDATE            AS FINISHDATE 
       ,A.REMARK                AS REMARK 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'SLNA'                  AS ODS_SYS_ID 
   FROM O_LN_SUNCUST_DELAYCREDIT A                             --阳光信贷暂缓授信信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_SUNCUST_DELAYCREDIT = sqlContext.sql(sql)
F_LN_SUNCUST_DELAYCREDIT.registerTempTable("F_LN_SUNCUST_DELAYCREDIT")
dfn="F_LN_SUNCUST_DELAYCREDIT/"+V_DT+".parquet"
F_LN_SUNCUST_DELAYCREDIT.cache()
nrows = F_LN_SUNCUST_DELAYCREDIT.count()
F_LN_SUNCUST_DELAYCREDIT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_LN_SUNCUST_DELAYCREDIT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_SUNCUST_DELAYCREDIT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_SUNCUST_DELAYCREDIT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
