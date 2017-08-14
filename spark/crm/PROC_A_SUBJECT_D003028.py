#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_SUBJECT_D003026').setMaster(sys.argv[2])
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
#季末字段
V_DT_QUA = etl_date[4:8]

V_STEP = 0

ACRM_F_RE_INSUSUMINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_INSUSUMINFO/*')
ACRM_F_RE_INSUSUMINFO.registerTempTable("ACRM_F_RE_INSUSUMINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1
if V_DT_QUA != '0331' and V_DT_QUA != '0630' and V_DT_QUA != '0930' and V_DT_QUA != '1231':
    sql = """
 					 SELECT       
 					   A.CUST_ID AS CUST_ID,
					   '1' AS CUST_TYPE,
					   A.FR_ID AS FR_ID,
					   'D003028' AS INDEX_CODE,
             CAST(SUM(MONTH_BAL_SUM_V_DT_MO) AS DECIMAL(22,2)) AS INDEX_VALUE,
					   V_YM AS YEAR_MONTH,
					   V_DT AS ETL_DATE
					 FROM ACRM_F_RE_INSUSUMINFO A 
					 WHERE  YEAR = V_DT_YE
           GROUP BY A.CUST_ID,A.FR_ID """
elif V_DT_QUA == '0331':
    sql = """
          SELECT
            A.CUST_ID AS CUST_ID,
					  '1' AS CUST_TYPE,
					  A.FR_ID AS FR_ID,
					  'D003028' AS INDEX_CODE,
            CAST(SUM((MONTH_BAL_SUM_1+MONTH_BAL_SUM_2+MONTH_BAL_SUM_3)/3) AS DECIMAL(22,2)) AS INDEX_VALUE,
						V_YM AS YEAR_MONTH,
					   V_DT AS ETL_DATE
					FROM ACRM_F_RE_INSUSUMINFO 
					WHERE  YEAR = V_DT_YE
          GROUP BY A.CUST_ID,A.FR_ID """
elif V_DT_QUA == '0630':
    sql = """
          SELECT
            A.CUST_ID AS CUST_ID,
					  '1' AS CUST_TYPE,
					  A.FR_ID AS FR_ID,
					  'D003028' AS INDEX_CODE,
            CAST(SUM((MONTH_BAL_SUM_4+MONTH_BAL_SUM_5+MONTH_BAL_SUM_6)/3) AS DECIMAL(22,2)) AS INDEX_VALUE,
						V_YM AS YEAR_MONTH,
					   V_DT AS ETL_DATE
					FROM ACRM_F_RE_INSUSUMINFO 
					WHERE  YEAR = V_DT_YE
          GROUP BY A.CUST_ID,A.FR_ID """
elif V_DT_QUA == '0930':
    sql = """
          SELECT
            A.CUST_ID AS CUST_ID,
					  '1' AS CUST_TYPE,
					  A.FR_ID AS FR_ID,
					  'D003028' AS INDEX_CODE,
            CAST(SUM((MONTH_BAL_SUM_7+MONTH_BAL_SUM_8+MONTH_BAL_SUM_9)/3) AS DECIMAL(22,2)) AS INDEX_VALUE,
						V_YM AS YEAR_MONTH,
					   V_DT AS ETL_DATE
					FROM ACRM_F_RE_INSUSUMINFO 
					WHERE  YEAR = V_DT_YE
          GROUP BY A.CUST_ID,A.FR_ID """
elif V_DT_QUA == '1231':
    sql = """
          SELECT
            A.CUST_ID AS CUST_ID,
					  '1' AS CUST_TYPE,
					  A.FR_ID AS FR_ID,
					  'D003028' AS INDEX_CODE,
            CAST(SUM((MONTH_BAL_SUM_10+MONTH_BAL_SUM_11+MONTH_BAL_SUM_12)/3) AS DECIMAL(22,2)) AS INDEX_VALUE,
						V_YM AS YEAR_MONTH,
					   V_DT AS ETL_DATE
					FROM ACRM_F_RE_INSUSUMINFO 
					WHERE  YEAR = V_DT_YE
          GROUP BY A.CUST_ID,A.FR_ID """
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"V_DT_MO", V_DT_MO, sql)
sql = re.sub(r"\bV_YM\b", "'"+V_YM+"'", sql)
sql = re.sub(r"\bV_DT_YE\b", "'"+V_DT_YE+"'", sql)
ACRM_A_TARGET_D003028 = sqlContext.sql(sql)
ACRM_A_TARGET_D003028.registerTempTable("ACRM_A_TARGET_D003028")
dfn="ACRM_A_TARGET_D003028/"+V_DT+".parquet"
ACRM_A_TARGET_D003028.cache()
nrows = ACRM_A_TARGET_D003028.count()
ACRM_A_TARGET_D003028.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_TARGET_D003028.unpersist()
ACRM_F_RE_INSUSUMINFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TARGET_D003028/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_TARGET_D003028 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
