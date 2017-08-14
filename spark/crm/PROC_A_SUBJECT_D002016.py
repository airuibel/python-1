#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_SUBJECT_D002016').setMaster(sys.argv[2])
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

OCRM_F_CI_CUSTLNAINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUSTLNAINFO/*')
OCRM_F_CI_CUSTLNAINFO.registerTempTable("OCRM_F_CI_CUSTLNAINFO")
OCRM_F_CUST_ORG_MGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CUST_ORG_MGR/*')
OCRM_F_CUST_ORG_MGR.registerTempTable("OCRM_F_CUST_ORG_MGR")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(A.CUST_ID  AS VARCHAR(32))             AS CUST_ID 
		,CAST('' AS VARCHAR(20))				AS ORG_ID      --插入的空值，包顺龙2017/05/13
		,CAST('D002016' AS VARCHAR(20))                 AS INDEX_CODE
		,CAST( (YEAR(V_DT) - YEAR(MIN(PUB_DATE)))  AS DECIMAL(22,2) )                      AS INDEX_VALUE
       ,CAST(SUBSTR(V_DT, 1, 7)  AS VARCHAR(7))                     AS YEAR_MONTH
		,CAST(V_DT  AS DATE)                  AS ETL_DATE	
		,CAST(B.CUST_TYP  AS VARCHAR(5))             AS CUST_TYPE		
       ,CAST(A.FR_ID AS VARCHAR(5))              AS FR_ID    
   FROM OCRM_F_CI_CUSTLNAINFO A                                --
  INNER JOIN OCRM_F_CUST_ORG_MGR B                             --
     ON A.ORG_ID              = B.ORG_ID 
    AND A.CUST_ID               = B.CUST_ID
   LEFT JOIN 
 (SELECT C.ORG_ID 
       ,C.CUST_ID 
       ,MAX(C.EXP_DATE) AS EXP_DATE
   FROM OCRM_F_CI_CUSTLNAINFO C 
  GROUP BY C.ORG_ID 
       ,C.CUST_ID) D                                            --
     ON A.ORG_ID                = D.ORG_ID 
    AND A.CUST_ID               = D.CUST_ID 
    AND D.EXP_DATE > V_DT 
  GROUP BY A.CUST_ID 
       ,B.CUST_TYP 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_TARGET_D002016 = sqlContext.sql(sql)
ACRM_A_TARGET_D002016.registerTempTable("ACRM_A_TARGET_D002016")
dfn="ACRM_A_TARGET_D002016/"+V_DT+".parquet"
ACRM_A_TARGET_D002016.cache()
nrows = ACRM_A_TARGET_D002016.count()
ACRM_A_TARGET_D002016.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_TARGET_D002016.unpersist()
OCRM_F_CI_CUSTLNAINFO.unpersist()
OCRM_F_CUST_ORG_MGR.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TARGET_D002016/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_TARGET_D002016 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
