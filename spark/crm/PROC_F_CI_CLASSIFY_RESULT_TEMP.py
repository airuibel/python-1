#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CLASSIFY_RESULT_TEMP').setMaster(sys.argv[2])
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

OCRM_F_CI_CLASSIFY_RESULT = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CLASSIFY_RESULT/*')
OCRM_F_CI_CLASSIFY_RESULT.registerTempTable("OCRM_F_CI_CLASSIFY_RESULT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT 
	 A.OBJECTNO
	,A.ACCOUNTMONTH
	,A.CUST_ID
	,A.CUSTOMERNAME
	,A.CERTTYPE
	,A.CUSTOMERTYPE
	,A.BUSINESSTYPE
	,A.CURRENTBALANCE
	,A.BUSINESSBALANCE
	,A.FIRSTRESULT
	,A.FIRSTRESULTRESOURCE
	,A.SECONDRESULT
	,A.MODELRESULT
	,A.LASTFIVERESULT
	,A.LASTTENRESULT
	,A.CURRENTFIVERESULT
	,A.CURRENTTENRESULT
	,A.FINALLYRESULT
	,A.CONFIRMTYPE
	,A.CLASSIFYTYPE
	,A.USERID
	,A.ORGID
	,A.FINISHDATE
	,A.LASTRESULT
	,A.UPDATEDATE
	,A.ODS_ST_DATE
	,A.ID
	,A.FR_ID
   FROM OCRM_F_CI_CLASSIFY_RESULT A
      INNER JOIN ( 
		SELECT FR_ID,CUST_ID ,MAX(OBJECTNO) AS OBJECTNO 
		FROM OCRM_F_CI_CLASSIFY_RESULT GROUP BY FR_ID,CUST_ID
	  )B
      ON A.FR_ID=B.FR_ID AND A.CUST_ID=B.CUST_ID AND A.OBJECTNO=B.OBJECTNO
	"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CLASSIFY_RESULT_TEMP = sqlContext.sql(sql)
dfn="OCRM_F_CI_CLASSIFY_RESULT_TEMP/"+V_DT+".parquet"
OCRM_F_CI_CLASSIFY_RESULT_TEMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CLASSIFY_RESULT_TEMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
