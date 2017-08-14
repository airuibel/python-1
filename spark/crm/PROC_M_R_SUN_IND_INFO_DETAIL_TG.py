#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_SUN_IND_INFO_DETAIL_TG').setMaster(sys.argv[2])
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

MCRM_SUN_IND_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/MCRM_SUN_IND_INFO_DETAIL/*')
MCRM_SUN_IND_INFO_DETAIL.registerTempTable("MCRM_SUN_IND_INFO_DETAIL")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ORG_ID                AS ORG_ID 
       ,A.ORG_NAME              AS ORG_NAME 
       ,COUNT(NVL(A.CUST_ID, 0))                       AS VALIED_NUM 
       ,COUNT(NVL(B.CUST_ID, 0))                       AS CURR_DEP_NUM 
       ,SUM(NVL(B.DEP_BAL, 0))                       AS CURR_DEP_BAL 
       ,COUNT(NVL(C.CUST_ID, 0))                       AS FIXED_DEP_NUM 
       ,SUM(NVL(C.DEP_BAL, 0))                       AS FIXED_DEP_BAL 
       ,COUNT(NVL(D.CUST_ID, 0))                       AS CRE_NUM 
       ,SUM(NVL(D.CRE_BAL, 0))                       AS CRE_BAL 
       ,COUNT(NVL(E.CUST_ID, 0))                       AS FIN_NUM 
       ,SUM(NVL(E.FIN_BAL, 0))                       AS FIN_BAL 
       ,SUM(CASE WHEN A.MOBILE_BAL IS 
            NOT NULL THEN 1 ELSE 0 END)                       AS MOBILE_NUM 
       ,SUM(A.MOBILE_BAL)                       AS MOBILE_BAL 
       ,SUM(CASE WHEN A.EBANK_BAL IS 
            NOT NULL THEN 1 ELSE 0 END)                       AS EBANK_NUM 
       ,SUM(A.EBANK_BAL)                       AS EBANK_BAL 
       ,SUM(CASE WHEN A.POS_BAL IS 
            NOT NULL THEN 1 ELSE 0 END)                       AS POS_NUM 
       ,SUM(A.POS_BAL)                       AS POS_BAL 
       ,SUM(CASE WHEN A.BMT_BAL IS 
            NOT NULL THEN 1 ELSE 0 END)                       AS BMT_NUM 
       ,SUM(A.BMT_BAL)                       AS BMT_BAL 
       ,V_DT10               AS REPORT_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM (SELECT DISTINCT CUST_ID ,FR_ID,ORG_ID,ORG_NAME,MOBILE_BAL,EBANK_BAL,POS_BAL,BMT_BAL
              FROM
              MCRM_SUN_IND_INFO_DETAIL
              WHERE  REPORT_DATE = V_DT10
                AND  TEMPSAVEFLAG = 'N'
) A
   LEFT JOIN MCRM_SUN_IND_INFO_DETAIL B
     ON A.ORG_ID                = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.DEP_TYPE              = 'H' 
    AND B.REPORT_DATE           = V_DT10 
   LEFT JOIN MCRM_SUN_IND_INFO_DETAIL C
     ON A.ORG_ID                = C.ORG_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.DEP_TYPE              = 'D' 
    AND C.REPORT_DATE           = V_DT10 
   LEFT JOIN MCRM_SUN_IND_INFO_DETAIL D
     ON A.ORG_ID                = D.ORG_ID 
    AND A.FR_ID                 = D.FR_ID 
    AND D.REPORT_DATE           = V_DT10 
   LEFT JOIN MCRM_SUN_IND_INFO_DETAIL E
     ON A.ORG_ID= E.ORG_ID 
    AND A.FR_ID= E.FR_ID 
    AND E.REPORT_DATE= V_DT10 
  GROUP BY A.ORG_ID 
       ,A.ORG_NAME 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT10\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_TG = sqlContext.sql(sql)
MCRM_SUN_IND_INFO_DETAIL_TG.registerTempTable("MCRM_SUN_IND_INFO_DETAIL_TG")
dfn="MCRM_SUN_IND_INFO_DETAIL_TG/"+V_DT+".parquet"
MCRM_SUN_IND_INFO_DETAIL_TG.cache()
nrows = MCRM_SUN_IND_INFO_DETAIL_TG.count()
MCRM_SUN_IND_INFO_DETAIL_TG.write.save(path=hdfs + '/' + dfn, mode='append')
MCRM_SUN_IND_INFO_DETAIL_TG.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_SUN_IND_INFO_DETAIL_TG lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
