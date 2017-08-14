#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_COR_CUS_CON_ORG_REPORT_BE').setMaster(sys.argv[2])
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

ACRM_F_CI_CUST_CONT_TMP = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_CONT_TMP/*')
ACRM_F_CI_CUST_CONT_TMP.registerTempTable("ACRM_F_CI_CUST_CONT_TMP")
OCRM_F_CUST_ORG_MGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CUST_ORG_MGR/*')
OCRM_F_CUST_ORG_MGR.registerTempTable("OCRM_F_CUST_ORG_MGR")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT TRIM(C.ORG_NAME)                       AS ORG_NAME 
       ,TRIM(C.ORG_ID)                       AS ORG_ID 
       ,TRIM(A.CUST_ID)                       AS CUST_ID 
       ,TRIM(A.CUST_NAME)                       AS CUST_ZH_NAME 
       ,COALESCE(A.CUST_CONTRIBUTION,0)                       AS CUS_CONTRIBUTION_RMB 
       ,COALESCE(A.CRE_CONTRIBUTION,0)                       AS CRE_CONTRIBUTION_RMB 
       ,COALESCE(A.DEP_CONTRIBUTION,0)                       AS DEP_CONTRIBUTION_RMB 
       ,COALESCE(A.DEP_BAL_RMB,0)                       AS DEP_AMOUNT_RMB 
       ,COALESCE(A.CRE_BAL_RMB,0)                       AS CRE_AMOUNT_RMB 
       ,C.O_MAIN_TYPE           AS MAIN_TYPE 
       ,V_DT                    AS REPORT_DATE 
       ,C.CUST_TYP              AS CUST_TYP 
       ,TRIM(C.MGR_ID)                       AS MGR_ID 
       ,TRIM(C.MGR_NAME)                       AS MGR_NAME 
       ,C.M_MAIN_TYPE           AS M_MAIN_TYPE 
   FROM ACRM_F_CI_CUST_CONT_TMP A                              --客户贡献度临时表
   LEFT JOIN OCRM_F_CUST_ORG_MGR C                             --客户归属信息表
     ON A.CUST_ID               = C.CUST_ID 
    AND C.CUST_TYP              = '2' 
    AND A.FR_ID                 = C.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_COR_CUS_CON_ORG_REPORT_BE = sqlContext.sql(sql)
MCRM_COR_CUS_CON_ORG_REPORT_BE.registerTempTable("MCRM_COR_CUS_CON_ORG_REPORT_BE")
dfn="MCRM_COR_CUS_CON_ORG_REPORT_BE/"+V_DT+".parquet"
MCRM_COR_CUS_CON_ORG_REPORT_BE.cache()
nrows = MCRM_COR_CUS_CON_ORG_REPORT_BE.count()
MCRM_COR_CUS_CON_ORG_REPORT_BE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_COR_CUS_CON_ORG_REPORT_BE.unpersist()
ACRM_F_CI_CUST_CONT_TMP.unpersist()
OCRM_F_CUST_ORG_MGR.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_COR_CUS_CON_ORG_REPORT_BE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_COR_CUS_CON_ORG_REPORT_BE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
