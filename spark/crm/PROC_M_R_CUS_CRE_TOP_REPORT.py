#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_CUS_CRE_TOP_REPORT').setMaster(sys.argv[2])
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
ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")

ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
#目标表
#MCRM_CUS_CRE_TOP_REPORT 增量表

#任务[21] 001-01::
V_STEP = V_STEP + 1
# -----更新支行层级TOP100数据
sql = """
 SELECT 
	 CUST_ID
	,CUST_ZH_NAME
	,CERT_NUM
	,OBJ_RATING
	,AMOUNT
	,ORG_ID
	,ORG_NAME
	,REPORT_DATE
	,FR_ID
	,RANK
	,CUST_TYP
	,CERT_TYPE
  FROM (SELECT 
               ROW_NUMBER () OVER (PARTITION BY C.ORG_ID,g.CUST_TYP ORDER BY COALESCE (e.DEP_AMOUNT_RMB, 0) DESC)
                  AS rank,
               e.cust_Id,
               g.CUST_ZH_NAME,
               g.CERT_TYPE,
               g.CERT_NUM,
               g.OBJ_RATING,
               e.DEP_AMOUNT_RMB as AMOUNT,
               c.org_id,
               c.ORG_NAME,
               V_DT as REPORT_DATE,
               e.FR_ID,
               g.CUST_TYP               
               
         FROM (SELECT a.CUST_ID, a.FR_ID, a.OPEN_BRC, a.YEAR, sum(AMOUNT) AS DEP_AMOUNT_RMB
                  FROM ACRM_F_RE_LENDSUMAVGINFO a
				  WHERE a.YEAR = substr(V_DT,1,4)
                GROUP BY a.CUST_ID, a.OPEN_BRC,a.FR_ID,a.YEAR 
			   ) e
               INNER JOIN OCRM_F_CI_BELONG_ORG b
                  ON     e.CUST_ID = b.CUST_ID
                     AND b.FR_ID = e.FR_ID
                     AND e.OPEN_BRC=b.INSTITUTION_CODE
                  --   AND b.MAIN_TYPE = '1'
               INNER JOIN OCRM_F_CI_CUST_DESC g
                  ON g.FR_ID = e.FR_ID AND e.CUST_ID = g.CUST_ID
               INNER JOIN ADMIN_AUTH_ORG c
                  ON C.FR_ID = e.FR_ID AND B.INSTITUTION_CODE = c.ORG_ID) f
 WHERE rank BETWEEN 1 AND 100
 
 """
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_CUS_CRE_TOP_REPORT1 = sqlContext.sql(sql)
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

#任务[21] 001-02::
V_STEP = V_STEP + 1
 #-----更新法人层级TOP100数据
sql = """
 SELECT 
	CUST_ID
	,CUST_ZH_NAME
	,CERT_NUM
	,OBJ_RATING
	,AMOUNT
	,ORG_ID
	,ORG_NAME
	,REPORT_DATE
	,FR_ID
	,RANK
	,CUST_TYP
	,CERT_TYPE
  FROM (SELECT 
               ROW_NUMBER () OVER (PARTITION BY g.CUST_TYP ORDER BY COALESCE (e.DEP_AMOUNT_RMB, 0) DESC)
                  AS rank,
               e.cust_Id,
               g.CUST_ZH_NAME,
               g.CERT_TYPE,
               g.CERT_NUM,
               g.OBJ_RATING,
               e.DEP_AMOUNT_RMB as AMOUNT,
               c.org_id,
               c.ORG_NAME,
               V_DT as REPORT_DATE,
               e.FR_ID,
               g.CUST_TYP               
               
         FROM (SELECT a.CUST_ID, a.FR_ID,sum (AMOUNT) AS DEP_AMOUNT_RMB
                  FROM ACRM_F_RE_LENDSUMAVGINFO a
				  where a.YEAR = substr(V_DT,1,4)
                GROUP BY a.CUST_ID ,a.FR_ID
			  ) e
               INNER JOIN OCRM_F_CI_BELONG_ORG b
                  ON     e.CUST_ID = b.CUST_ID
                     AND b.FR_ID = e.FR_ID
                     AND b.MAIN_TYPE = '1'
               INNER JOIN OCRM_F_CI_CUST_DESC g
                  ON g.FR_ID = e.FR_ID AND e.CUST_ID = g.CUST_ID
               LEFT JOIN ADMIN_AUTH_ORG c
                  ON C.FR_ID = e.FR_ID AND C.UP_ORG_ID = '320000000') f 
         WHERE rank BETWEEN 1 AND 100 
 
 """
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_CUS_CRE_TOP_REPORT2 = sqlContext.sql(sql)
MCRM_CUS_CRE_TOP_REPORT = MCRM_CUS_CRE_TOP_REPORT1.unionAll(MCRM_CUS_CRE_TOP_REPORT2)
dfn="MCRM_CUS_CRE_TOP_REPORT/"+V_DT+".parquet"
MCRM_CUS_CRE_TOP_REPORT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
