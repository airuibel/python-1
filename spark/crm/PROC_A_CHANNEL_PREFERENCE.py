#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CHANNEL_PREFERENCE').setMaster(sys.argv[2])
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

ACRM_F_CI_NIN_TRANSLOG = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_NIN_TRANSLOG/*')
ACRM_F_CI_NIN_TRANSLOG.registerTempTable("ACRM_F_CI_NIN_TRANSLOG")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CHANNEL_FLAG          AS TRANS_CHANNEL 
       ,CAST(A.TRANS_TIME AS DECIMAL(10))	           AS TRANS_TIME 
       ,CAST(A.SA_TVARCHAR_AMT AS DECIMAL(20,2))        AS TRANS_AMT 
       ,A.RANK                  AS RANK 
       ,CASE WHEN B.RANK >= 4  OR B.RANK IS NULL THEN 4 ELSE B.RANK END                     AS LAST_RANK 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM (SELECT CUST_ID,FR_ID,
                   CHANNEL_FLAG,
                   COUNT(1) AS TRANS_TIME,
                   SUM(NVL(SA_TVARCHAR_AMT,0)) AS SA_TVARCHAR_AMT,
                   ROW_NUMBER () OVER (PARTITION BY CUST_ID ORDER BY SUM(NVL(SA_TVARCHAR_AMT,0)) DESC) RANK
            FROM   ACRM_F_CI_NIN_TRANSLOG 
            WHERE  CUST_TYP = '1' AND (SA_TVARCHAR_DT >= concat(substr(V_DT8,1,6),'01') AND SA_TVARCHAR_DT <= regexp_replace(V_DT,'-',''))
            GROUP BY CUST_ID ,CHANNEL_FLAG,FR_ID) A                                                 --本月流水数据
   LEFT JOIN 
 (SELECT CUST_ID,FR_ID,
                                  CHANNEL_FLAG,
                                  COUNT(1) AS TRANS_TIME,
                                  SUM(NVL(SA_TVARCHAR_AMT,0)) AS SA_TVARCHAR_AMT,
                                  ROW_NUMBER () OVER (PARTITION BY CUST_ID ORDER BY SUM(NVL(SA_TVARCHAR_AMT,0)) DESC) RANK
                           FROM ACRM_F_CI_NIN_TRANSLOG
                           WHERE  CUST_TYP = '1' AND (SA_TVARCHAR_DT >= regexp_replace(add_months(CONCAT(substr(V_DT,1,8),'01') ,- 1),'-','') AND SA_TVARCHAR_DT <= regexp_replace(date_add(CONCAT(substr(V_DT,1,8),'01'),- 1),'-',''))
                           GROUP BY CUST_ID ,CHANNEL_FLAG,FR_ID) B                                                 --上月流水数据
     ON A.CUST_ID               = B.CUST_ID 
    AND A.CHANNEL_FLAG          = B.CHANNEL_FLAG 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.RANK <= 3 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT8\b", "'"+V_DT+"'", sql)
ACRM_F_CI_CHANNEL_PREFERENCE = sqlContext.sql(sql)
ACRM_F_CI_CHANNEL_PREFERENCE.registerTempTable("ACRM_F_CI_CHANNEL_PREFERENCE")
dfn="ACRM_F_CI_CHANNEL_PREFERENCE/"+V_DT+".parquet"
ACRM_F_CI_CHANNEL_PREFERENCE.cache()
nrows = ACRM_F_CI_CHANNEL_PREFERENCE.count()
ACRM_F_CI_CHANNEL_PREFERENCE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CHANNEL_PREFERENCE.unpersist()
ACRM_F_CI_NIN_TRANSLOG.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CHANNEL_PREFERENCE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CHANNEL_PREFERENCE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
