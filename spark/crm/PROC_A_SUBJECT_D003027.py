#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_SUBJECT_D003027').setMaster(sys.argv[2])
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
#年份
V_DT_YE = repr(int(etl_date[0:4]))
#月份
V_DT_MO = str(int(etl_date[4:6]))
#年月
V_YM = V_DT10[0:7]

V_STEP = 0

ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")

#任务[21] 001-01::
V_STEP = V_STEP + 1
sql = """
             SELECT
               A.CUST_ID AS CUST_ID ,
               '' AS ORG_ID,
							 B.CUST_TYP AS CUST_TYPE,
							 A.FR_ID AS FR_ID,
							 'D003027'  AS INDEX_CODE,
               CAST(SUM(MONTH_BAL_V_DT_MO/MONTH_DAYS_V_DT_MO) AS DECIMAL(22,2)) AS INDEX_VALUE,
							 V_YM AS YEAR_MONTH,
							 V_DT AS ETL_DATE
						 FROM ACRM_F_RE_ACCSUMAVGINFO A
						 JOIN OCRM_F_CI_CUST_DESC B ON A.CUST_ID = B.CUST_ID AND A.FR_ID = B.FR_ID 
						 WHERE A.YEAR = V_DT_YE
             GROUP BY A.CUST_ID,B.CUST_TYP,A.FR_ID  """
   


sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"V_DT_MO", V_DT_MO, sql)
sql = re.sub(r"\bV_YM\b", "'"+V_YM+"'", sql)
sql = re.sub(r"\bV_DT_YE\b", "'"+V_DT_YE+"'", sql)
ACRM_A_TARGET_D003027 = sqlContext.sql(sql)
ACRM_A_TARGET_D003027.registerTempTable("ACRM_A_TARGET_D003027")
dfn="ACRM_A_TARGET_D003027/"+V_DT+".parquet"
ACRM_A_TARGET_D003027.cache()
nrows = ACRM_A_TARGET_D003027.count()
ACRM_A_TARGET_D003027.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_TARGET_D003027.unpersist()
ACRM_F_RE_ACCSUMAVGINFO.unpersist()
OCRM_F_CI_CUST_DESC.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TARGET_D003027/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_TARGET_D003027 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
