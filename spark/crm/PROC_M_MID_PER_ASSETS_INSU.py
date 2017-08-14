#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_MID_PER_ASSETS_INSU').setMaster(sys.argv[2])
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
ACRM_F_RE_INSUSUMINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_INSUSUMINFO/*')
ACRM_F_RE_INSUSUMINFO.registerTempTable("ACRM_F_RE_INSUSUMINFO")

#目标表：
#TMP_PER_ASSETS_INSU 全量



#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,'2'                     AS PRD_TYP 
       ,CAST(SUM(A.ALL_MONEY)   AS DECIMAL(24,6))          AS MONTH_BAL 
       ,CAST(SUM(CASE MONTH(V_DT) WHEN '1' THEN A.MONTH_BAL_SUM_1 
				WHEN '2' THEN A.MONTH_BAL_SUM_2 
				WHEN '3' THEN A.MONTH_BAL_SUM_3 
				WHEN '4' THEN A.MONTH_BAL_SUM_4 
				WHEN '5' THEN A.MONTH_BAL_SUM_5 
				WHEN '6' THEN A.MONTH_BAL_SUM_6 
				WHEN '7' THEN A.MONTH_BAL_SUM_7 
				WHEN '8' THEN A.MONTH_BAL_SUM_8 
				WHEN '9' THEN A.MONTH_BAL_SUM_9 
				WHEN '10' THEN A.MONTH_BAL_SUM_10 
				WHEN '11' THEN A.MONTH_BAL_SUM_11 
				WHEN '12' THEN A.MONTH_BAL_SUM_12 END)     AS DECIMAL(24,6))                   AS MONTH_AVG_BAL 
       ,CAST(SUM(CASE MONTH(V_DT) WHEN '1' THEN (A.MONTH_BAL_SUM_1 + B.MONTH_BAL_SUM_12 + B.MONTH_BAL_SUM_11) / 3 
	   WHEN '2' THEN (A.MONTH_BAL_SUM_2 + A.MONTH_BAL_SUM_1 + B.MONTH_BAL_SUM_12) / 3 
	   WHEN '3' THEN (A.MONTH_BAL_SUM_3 + A.MONTH_BAL_SUM_2 + A.MONTH_BAL_SUM_1) / 3 
	   WHEN '4' THEN (A.MONTH_BAL_SUM_4 + A.MONTH_BAL_SUM_3 + A.MONTH_BAL_SUM_2) / 3 
	   WHEN '5' THEN (A.MONTH_BAL_SUM_5 + A.MONTH_BAL_SUM_4 + A.MONTH_BAL_SUM_3) / 3 
	   WHEN '6' THEN (A.MONTH_BAL_SUM_6 + A.MONTH_BAL_SUM_5 + A.MONTH_BAL_SUM_4) / 3 
	   WHEN '7' THEN (A.MONTH_BAL_SUM_7 + A.MONTH_BAL_SUM_6 + A.MONTH_BAL_SUM_5) / 3 
	   WHEN '8' THEN (A.MONTH_BAL_SUM_8 + A.MONTH_BAL_SUM_7 + A.MONTH_BAL_SUM_6) / 3 
	   WHEN '9' THEN (A.MONTH_BAL_SUM_9 + A.MONTH_BAL_SUM_8 + A.MONTH_BAL_SUM_7) / 3 
	   WHEN '10' THEN (A.MONTH_BAL_SUM_10 + A.MONTH_BAL_SUM_9 + A.MONTH_BAL_SUM_8) / 3 
	   WHEN '11' THEN (A.MONTH_BAL_SUM_11 + A.MONTH_BAL_SUM_10 + A.MONTH_BAL_SUM_9) / 3 
	   WHEN '12' THEN (A.MONTH_BAL_SUM_12 + A.MONTH_BAL_SUM_11 + A.MONTH_BAL_SUM_10) / 3 END)     AS DECIMAL(24,6))                   AS THREE_MONTH_AVG_BAL 
       ,CAST(SUM(CASE MONTH(V_DT) WHEN '1' THEN NVL(B.MONTH_BAL_SUM_12, 0) 
	   WHEN '2' THEN A.MONTH_BAL_SUM_1 
	   WHEN '3' THEN A.MONTH_BAL_SUM_2 
	   WHEN '4' THEN A.MONTH_BAL_SUM_3 
	   WHEN '5' THEN A.MONTH_BAL_SUM_4 
	   WHEN '6' THEN A.MONTH_BAL_SUM_5 
	   WHEN '7' THEN A.MONTH_BAL_SUM_6 
	   WHEN '8' THEN A.MONTH_BAL_SUM_7 
	   WHEN '9' THEN A.MONTH_BAL_SUM_8 
	   WHEN '10' THEN A.MONTH_BAL_SUM_9 
	   WHEN '11' THEN A.MONTH_BAL_SUM_10 
	   WHEN '12' THEN A.MONTH_BAL_SUM_11 END)     AS DECIMAL(24,6))                   AS LAST_MONTH_BAL 
       ,CAST(SUM(CASE MONTH(V_DT) WHEN '1' THEN NVL(B.MONTH_BAL_SUM_12, 0) 
	   WHEN '2' THEN A.MONTH_BAL_SUM_1 
	   WHEN '3' THEN A.MONTH_BAL_SUM_2 
	   WHEN '4' THEN A.MONTH_BAL_SUM_3 
	   WHEN '5' THEN A.MONTH_BAL_SUM_4 
	   WHEN '6' THEN A.MONTH_BAL_SUM_5 
	   WHEN '7' THEN A.MONTH_BAL_SUM_6 
	   WHEN '8' THEN A.MONTH_BAL_SUM_7 
	   WHEN '9' THEN A.MONTH_BAL_SUM_8 
	   WHEN '10' THEN A.MONTH_BAL_SUM_9 
	   WHEN '11' THEN A.MONTH_BAL_SUM_10 
	   WHEN '12' THEN A.MONTH_BAL_SUM_11 END)     AS DECIMAL(24,6))                   AS LAST_MONTH_AVG_BAL 
       ,CAST(SUM(CASE MONTH(V_DT) WHEN '1' THEN (NVL(B.MONTH_BAL_SUM_12, 0) + NVL(B.MONTH_BAL_SUM_11, 0) + NVL(B.MONTH_BAL_SUM_10, 0)) / 3 
	   WHEN '2' THEN (A.MONTH_BAL_SUM_1 + NVL(B.MONTH_BAL_SUM_12, 0) + NVL(B.MONTH_BAL_SUM_11, 0)) / 3 
	   WHEN '3' THEN (A.MONTH_BAL_SUM_2 + A.MONTH_BAL_SUM_1 + NVL(B.MONTH_BAL_SUM_12, 0)) / 3 
	   WHEN '4' THEN (A.MONTH_BAL_SUM_3 + A.MONTH_BAL_SUM_2 + A.MONTH_BAL_SUM_1) / 3 
	   WHEN '5' THEN (A.MONTH_BAL_SUM_4 + A.MONTH_BAL_SUM_3 + A.MONTH_BAL_SUM_2) / 3 
	   WHEN '6' THEN (A.MONTH_BAL_SUM_5 + A.MONTH_BAL_SUM_4 + A.MONTH_BAL_SUM_3) / 3 
	   WHEN '7' THEN (A.MONTH_BAL_SUM_6 + A.MONTH_BAL_SUM_5 + A.MONTH_BAL_SUM_4) / 3 
	   WHEN '8' THEN (A.MONTH_BAL_SUM_7 + A.MONTH_BAL_SUM_6 + A.MONTH_BAL_SUM_5) / 3 
	   WHEN '9' THEN (A.MONTH_BAL_SUM_8 + A.MONTH_BAL_SUM_7 + A.MONTH_BAL_SUM_6) / 3 
	   WHEN '10' THEN (A.MONTH_BAL_SUM_9 + A.MONTH_BAL_SUM_8 + A.MONTH_BAL_SUM_7) / 3 
	   WHEN '11' THEN (A.MONTH_BAL_SUM_10 + A.MONTH_BAL_SUM_9 + A.MONTH_BAL_SUM_8) / 3 
	   WHEN '12' THEN (A.MONTH_BAL_SUM_11 + A.MONTH_BAL_SUM_10 + A.MONTH_BAL_SUM_9) / 3 END) AS DECIMAL(24,6))   AS LTHREE_MONTH_AVG_BAL 
       ,CAST(SUM(A.OLD_YEAR_BAL ) AS DECIMAL(24,6))         AS YEAR_BAL 
       ,CAST(SUM(NVL(B.MONTH_BAL_SUM_12, 0)) AS DECIMAL(24,6))                       AS YEAR_AVG_BAL 
       ,CAST(SUM((NVL(B.MONTH_BAL_SUM_12, 0) + NVL(B.MONTH_BAL_SUM_11, 0) + NVL(B.MONTH_BAL_SUM_10, 0)) / 3 ) AS DECIMAL(24,6))        AS YEAR_THREE_AVG_BAL 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_RE_INSUSUMINFO A                                --保险账户累计表
   LEFT JOIN ACRM_F_RE_INSUSUMINFO B                           --保险账户累计表
     ON A.ACCT_NO               = B.ACCT_NO 
    AND A.FR_ID                 = B.FR_ID 
    AND B.YEAR                  = TRIM(YEAR(V_DT) - 1) 
  WHERE A.YEAR                  = TRIM(YEAR(V_DT)) 
  GROUP BY A.CUST_ID 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_PER_ASSETS_INSU = sqlContext.sql(sql)
TMP_PER_ASSETS_INSU.registerTempTable("TMP_PER_ASSETS_INSU")
dfn="TMP_PER_ASSETS_INSU/"+V_DT+".parquet"
TMP_PER_ASSETS_INSU.cache()
nrows = TMP_PER_ASSETS_INSU.count()
TMP_PER_ASSETS_INSU.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_PER_ASSETS_INSU.unpersist()

#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_PER_ASSETS_INSU/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_PER_ASSETS_INSU lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
