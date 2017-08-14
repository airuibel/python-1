#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_TRANSLOG_TARGET').setMaster(sys.argv[2])
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
#V_DT10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8]))).strftime("%Y-%m-%d")
V_DT10 = etl_date[0:4]+"-"+etl_date[4:6]+"-"+etl_date[6:8]
V_STEP = 0
#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
ACRM_F_CI_NIN_TRANSLOG = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_NIN_TRANSLOG/*')
ACRM_F_CI_NIN_TRANSLOG.registerTempTable("ACRM_F_CI_NIN_TRANSLOG")
#目标表：
#ACRM_A_TRANSLOG_TARGET 全量表

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
	   SELECT 
		 CUST_ID as CUST_ID,
		 CUST_TYP as CUST_TYP,
		 FR_ID,
		 CHANNEL_FLAG as CHANNEL_FLAG,
		 CAST(COUNT(1) AS DECIMAL(22,2)) as INDEX_VALUE_C,   --DECIMAL(22,2)
		 CAST(COALESCE(SUM(SA_DR_AMT),0) - COALESCE(SUM(SA_CR_AMT),0)  AS DECIMAL(22,2)) as SA_AMT,
		 CAST(SUM(SA_TVARCHAR_AMT) AS DECIMAL(22,2)) as SA_TX_AMT,
		 '' 		as SA_TX_DT,
		 V_DT10		as ETL_DATE				
	  FROM ACRM_F_CI_NIN_TRANSLOG A
	 WHERE  --A.FR_ID = V_FR_ID
	    A.SA_TVARCHAR_DT = V_DT
	 GROUP BY FR_ID,CUST_ID,CUST_TYP,CHANNEL_FLAG
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT+"'", sql)
sql = re.sub(r"\bV_DT10\b", "'"+V_DT10+"'", sql)
ACRM_A_TRANSLOG_TARGET = sqlContext.sql(sql)
ACRM_A_TRANSLOG_TARGET.registerTempTable("ACRM_A_TRANSLOG_TARGET")
dfn="ACRM_A_TRANSLOG_TARGET/"+V_DT+".parquet"
ACRM_A_TRANSLOG_TARGET.cache()
nrows = ACRM_A_TRANSLOG_TARGET.count()
ACRM_A_TRANSLOG_TARGET.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_TRANSLOG_TARGET.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TRANSLOG_TARGET/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_TRANSLOG_TARGET lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
