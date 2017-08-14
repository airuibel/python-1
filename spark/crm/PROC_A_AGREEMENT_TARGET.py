#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_AGREEMENT_TARGET').setMaster(sys.argv[2])
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

ACRM_F_AG_AGREEMENT = sqlContext.read.parquet(hdfs+'/ACRM_F_AG_AGREEMENT/*')
ACRM_F_AG_AGREEMENT.registerTempTable("ACRM_F_AG_AGREEMENT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
	SELECT 
				 CUST_ID 	as CUST_ID,
				 ''			as CUST_NAME,
				 CUST_TYP	as CUST_TYP,
				 FR_ID		as FR_ID,
				 MIN(START_DATE) as START_DATE_MIN,
				 MAX(END_DATE)   as END_DATE,
				 MAX(START_DATE) as START_DATE_MAX,
				 int(COUNT(1)) 		 as COUNT, --INTEGER
				 V_DT		 as ETL_DATE			
	  FROM ACRM_F_AG_AGREEMENT        
	 WHERE END_DATE >= V_DT
	 GROUP BY CUST_ID,CUST_TYP,FR_ID
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_AGREEMENT_TARGET = sqlContext.sql(sql)
dfn="ACRM_A_AGREEMENT_TARGET/"+V_DT+".parquet"
ACRM_A_AGREEMENT_TARGET.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_AGREEMENT_TARGET/"+V_DT_LD+".parquet")
ACRM_F_AG_AGREEMENT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
