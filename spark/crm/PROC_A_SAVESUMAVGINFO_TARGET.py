#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_SAVESUMAVGINFO_TARGET').setMaster(sys.argv[2])
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

ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
       ,CURR                    AS CURR 
       ,cast(SUM(AMOUNT)as decimal(22,2)) AS AMOUNT 
       ,cast(SUM(CASE MONTH(V_DT) WHEN 1 THEN A.MONTH_BAL_SUM_1 / A.MONTH_DAYS_1 WHEN 2 THEN A.MONTH_BAL_SUM_2 / A.MONTH_DAYS_2 WHEN 3 THEN A.MONTH_BAL_SUM_3 / A.MONTH_DAYS_3 WHEN 4 THEN A.MONTH_BAL_SUM_4 / A.MONTH_DAYS_4 WHEN 5 THEN A.MONTH_BAL_SUM_5 / A.MONTH_DAYS_5 WHEN 6 THEN A.MONTH_BAL_SUM_6 / A.MONTH_DAYS_6 WHEN 7 THEN A.MONTH_BAL_SUM_7 / A.MONTH_DAYS_7 WHEN 8 THEN A.MONTH_BAL_SUM_8 / A.MONTH_DAYS_8 WHEN 9 THEN A.MONTH_BAL_SUM_9 / A.MONTH_DAYS_9 WHEN 10 THEN A.MONTH_BAL_SUM_10 / A.MONTH_DAYS_10 WHEN 11 THEN A.MONTH_BAL_SUM_11 / A.MONTH_DAYS_11 WHEN 12 THEN A.MONTH_BAL_SUM_12 / A.MONTH_DAYS_12 END)as decimal(22,2)) AS MOUTH_BAL 
       ,cast(SUM(YEAR_BAL_SUM /(COALESCE(MONTH_DAYS_1, 0) + COALESCE(MONTH_DAYS_2, 0) + COALESCE(MONTH_DAYS_3, 0) + COALESCE(MONTH_DAYS_4, 0) + COALESCE(MONTH_DAYS_5, 0) + COALESCE(MONTH_DAYS_6, 0) + COALESCE(MONTH_DAYS_7, 0) + COALESCE(MONTH_DAYS_8, 0) + COALESCE(MONTH_DAYS_9, 0) + COALESCE(MONTH_DAYS_10, 0) + COALESCE(MONTH_DAYS_11, 0) + COALESCE(MONTH_DAYS_12, 0))) as decimal(22,2)) AS YEAR_BAL 
       ,cast(SUM(OLD_YEAR_BAL) as decimal(22,2))  AS OLD_YEAR_BAL 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --存款积数表
  WHERE CASE MONTH(V_DT) WHEN 1 THEN A.MONTH_BAL_SUM_1 WHEN 2 THEN A.MONTH_BAL_SUM_2 WHEN 3 THEN A.MONTH_BAL_SUM_3 WHEN 4 THEN A.MONTH_BAL_SUM_4 WHEN 5 THEN A.MONTH_BAL_SUM_5 WHEN 6 THEN A.MONTH_BAL_SUM_6 WHEN 7 THEN A.MONTH_BAL_SUM_7 WHEN 8 THEN A.MONTH_BAL_SUM_8 WHEN 9 THEN A.MONTH_BAL_SUM_9 WHEN 10 THEN A.MONTH_BAL_SUM_10 WHEN 11 THEN A.MONTH_BAL_SUM_11 WHEN 12 THEN A.MONTH_BAL_SUM_12 END > 0 
    AND YEAR                    = YEAR(V_DT)
group by CUST_ID,
         CUST_TYP,
         FR_ID,
         CURR        
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_SAVESUMAVGINFO_TARGET = sqlContext.sql(sql)
ACRM_A_SAVESUMAVGINFO_TARGET.registerTempTable("ACRM_A_SAVESUMAVGINFO_TARGET")
dfn="ACRM_A_SAVESUMAVGINFO_TARGET/"+V_DT+".parquet"
ACRM_A_SAVESUMAVGINFO_TARGET.cache()
nrows = ACRM_A_SAVESUMAVGINFO_TARGET.count()
ACRM_A_SAVESUMAVGINFO_TARGET.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_SAVESUMAVGINFO_TARGET.unpersist()
ACRM_F_RE_SAVESUMAVGINFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_SAVESUMAVGINFO_TARGET/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_SAVESUMAVGINFO_TARGET lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
