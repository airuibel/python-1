#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MID_DP_CBOD_LNLNSLNS').setMaster(sys.argv[2])
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

#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
F_DP_CBOD_LNLNSLNS = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_LNLNSLNS/*')
F_DP_CBOD_LNLNSLNS.registerTempTable("F_DP_CBOD_LNLNSLNS")
#目标表：
#MID_DP_CBOD_LNLNSLNS 全量

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.LN_LN_ACCT_NO         AS LN_LN_ACCT_NO 
       ,A.LN_APCL_FLG           AS LN_APCL_FLG 
       ,A.FR_ID                 AS FR_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
   FROM F_DP_CBOD_LNLNSLNS A                                   --
  INNER JOIN(
         SELECT MAX(ETLDT)                       AS ETLDT 
               ,LN_LN_ACCT_NO 
               ,FR_ID 
           FROM F_DP_CBOD_LNLNSLNS 
          GROUP BY LN_LN_ACCT_NO 
               ,FR_ID) B                                      --
     ON A.LN_LN_ACCT_NO         = B.LN_LN_ACCT_NO 
    AND A.FR_ID                 = B.FR_ID 
    AND A.ETLDT                 = B.ETLDT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_DP_CBOD_LNLNSLNS = sqlContext.sql(sql)
MID_DP_CBOD_LNLNSLNS.registerTempTable("MID_DP_CBOD_LNLNSLNS")
dfn="MID_DP_CBOD_LNLNSLNS/"+V_DT+".parquet"
MID_DP_CBOD_LNLNSLNS.cache()
nrows = MID_DP_CBOD_LNLNSLNS.count()
MID_DP_CBOD_LNLNSLNS.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_DP_CBOD_LNLNSLNS.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_DP_CBOD_LNLNSLNS/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_DP_CBOD_LNLNSLNS lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
