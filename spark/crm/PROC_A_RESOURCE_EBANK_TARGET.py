#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_RESOURCE_EBANK_TARGET').setMaster(sys.argv[2])
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
#---------------------------------------------------------------------------------------#
V_YEAR_MONTH =  etl_date[0:4]+"-" + etl_date[4:6]

OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
	select 
			A.ODS_CUST_TYPE 	as CUST_TYPE,
			A.ODS_CUST_ID 		as CUST_ID,
			A.FR_ID 			as FR_ID,
			CAST(COUNT(1) AS DECIMAL(22,2)) 	as INDEX_VALUE_COUNT,
			CAST(YEAR(V_DT)-YEAR(MIN(A.BEGIN_DATE)) AS DECIMAL(22,2)) as INDEX_VALUE_YEAR,
			V_YEAR_MONTH 						as YEAR_MONTH,
			V_DT 								as ETL_DATE,
			MIN(BEGIN_DATE) 					as BEGIN_DATE
		FROM OCRM_F_CI_SYS_RESOURCE A 
		  WHERE (A.ODS_SYS_ID = 'IBK'
						OR A.ODS_SYS_ID = 'MBK' 
						OR A.ODS_SYS_ID = 'MSG' 
						OR A.ODS_SYS_ID = 'APY' 
						) AND COALESCE(A.END_DATE,'2099-12-31') > V_DT
				GROUP BY A.ODS_CUST_ID,A.ODS_CUST_TYPE,A.FR_ID
	   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_YEAR_MONTH\b", "'"+V_YEAR_MONTH+"'", sql)
OCRM_A_RESOURCE_EBANK_TARGET = sqlContext.sql(sql)
dfn="OCRM_A_RESOURCE_EBANK_TARGET/"+V_DT+".parquet"
OCRM_A_RESOURCE_EBANK_TARGET.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_A_RESOURCE_EBANK_TARGET/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
