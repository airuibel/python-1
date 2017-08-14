#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CUST_LNA_INFO').setMaster(sys.argv[2])
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

ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CUST_LNA_INFO_DETAIL/"+V_DT+".parquet")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CUST_LNA_INFO/"+V_DT+".parquet")

ACRM_F_CI_CRE_CONTRIBUTION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CRE_CONTRIBUTION/*')
ACRM_F_CI_CRE_CONTRIBUTION.registerTempTable("ACRM_F_CI_CRE_CONTRIBUTION")
ADMIN_AUTH_ACCOUNT = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ACCOUNT/*')
ADMIN_AUTH_ACCOUNT.registerTempTable("ADMIN_AUTH_ACCOUNT")
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT B.MGR_ID                AS MGR_ID 
       ,CAST(SUM(BAL_RMB) AS DECIMAL(24,6))                       AS BAL 
       ,CAST(SUM(MONTH_AVG) AS DECIMAL(24,6))                      AS MONTH_BAL 
       ,CAST(SUM(CONTRIBUTION_RMB)  AS DECIMAL(24,6))                     AS MNLR 
       ,A.FR_ID                 AS FR_ID 
       ,SUBSTR(V_DT, 1, 7)                       AS YEAR_MONTH 
       ,V_DT                    AS CRM_DT 
   FROM ACRM_F_CI_CRE_CONTRIBUTION A                           --ACRM_F_CI_CRE_CONTRIBUTION
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --OCRM_F_CI_BELONG_CUSTMGR
     ON B.FR_ID                 = A.FR_ID 
    AND B.MAIN_TYPE             = '1' 
    AND A.CUST_ID               = B.CUST_ID 
  WHERE A.ODS_DATE              = V_DT 
  GROUP BY B.MGR_ID
          ,A.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CUST_LNA_INFO_DETAIL = sqlContext.sql(sql)
ACRM_A_CUST_LNA_INFO_DETAIL.registerTempTable("ACRM_A_CUST_LNA_INFO_DETAIL")
dfn="ACRM_A_CUST_LNA_INFO_DETAIL/"+V_DT+".parquet"
ACRM_A_CUST_LNA_INFO_DETAIL.cache()
nrows = ACRM_A_CUST_LNA_INFO_DETAIL.count()
ACRM_A_CUST_LNA_INFO_DETAIL.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_CUST_LNA_INFO_DETAIL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CUST_LNA_INFO_DETAIL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1
ACRM_A_CUST_LNA_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/ACRM_A_CUST_LNA_INFO_DETAIL/*')
ACRM_A_CUST_LNA_INFO_DETAIL.registerTempTable("ACRM_A_CUST_LNA_INFO_DETAIL")

sql = """
 SELECT A.MGR_ID                AS MGR_ID 
       ,CASE WHEN B.ACCOUNT_NAME IS NOT NULL THEN B.USER_NAME   END           AS MGR_NAME 
       ,CAST(CASE WHEN C.MGR_ID IS NOT NULL THEN C.BAL END AS DECIMAL(24,6))                  AS END_YEAR_BAL 
       ,CAST(CASE WHEN C.MGR_ID IS NOT NULL THEN C.MONTH_BAL END AS DECIMAL(24,6))            AS END_YEAR_MONTH_BAL 
       ,CAST(CASE WHEN C.MGR_ID IS NOT NULL THEN C.MNLR  END AS DECIMAL(24,6))                AS END_YEAR_MNLR 
       ,CAST(CASE WHEN D.MGR_ID IS NOT NULL THEN D.BAL  END AS DECIMAL(24,6))                AS LAST_BAL 
       ,CAST(CASE WHEN D.MGR_ID IS NOT NULL THEN D.MONTH_BAL END AS DECIMAL(24,6))           AS LAST_MONTH_BAL 
       ,CAST(CASE WHEN D.MGR_ID IS NOT NULL THEN D.MNLR END AS DECIMAL(24,6))                AS LAST_MNLR 
       ,CAST(A.BAL  AS DECIMAL(24,6))                AS BAL 
       ,CAST(A.MONTH_BAL AS DECIMAL(24,6))            AS MONTH_BAL 
       ,CAST(A.MNLR  AS DECIMAL(24,6))               AS MNLR 
       ,A.FR_ID                 AS FR_ID 
       ,A.YEAR_MONTH            AS YEAR_MONTH 
       ,A.CRM_DT                AS CRM_DT 
   FROM ACRM_A_CUST_LNA_INFO_DETAIL A                          --贷款模拟利润表-明细表
   LEFT JOIN ADMIN_AUTH_ACCOUNT B                              --系统用户表
     ON A.FR_ID                 = B.FR_ID 
    AND A.MGR_ID                = B.ACCOUNT_NAME 
   LEFT JOIN ACRM_A_CUST_LNA_INFO_DETAIL C                     --贷款模拟利润表-明细表
     ON A.FR_ID                 = C.FR_ID 
    AND A.MGR_ID                = C.MGR_ID 
    AND C.CRM_DT                = CONCAT(SUBSTR(add_months(V_DT, -12), 1, 5) , '12-31' )
   LEFT JOIN ACRM_A_CUST_LNA_INFO_DETAIL D                     --贷款模拟利润表-明细表
     ON A.FR_ID                 = D.FR_ID 
    AND A.MGR_ID                = D.MGR_ID 
    AND D.CRM_DT                = LAST_DAY(add_months(V_DT,-1) )
  WHERE A.CRM_DT                = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CUST_LNA_INFO = sqlContext.sql(sql)
ACRM_A_CUST_LNA_INFO.registerTempTable("ACRM_A_CUST_LNA_INFO")
dfn="ACRM_A_CUST_LNA_INFO/"+V_DT+".parquet"
ACRM_A_CUST_LNA_INFO.cache()
nrows = ACRM_A_CUST_LNA_INFO.count()
ACRM_A_CUST_LNA_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_CUST_LNA_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CUST_LNA_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
