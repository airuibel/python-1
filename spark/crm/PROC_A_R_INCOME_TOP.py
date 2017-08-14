#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_INCOME_TOP').setMaster(sys.argv[2])
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
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
ACRM_A_INOUTCOME = sqlContext.read.parquet(hdfs+'/ACRM_A_INOUTCOME/*')
ACRM_A_INOUTCOME.registerTempTable("ACRM_A_INOUTCOME")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
#目标表
#ACRM_A_INCOME_TOP 全量表


#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ROW_NUMBER() OVER(
      PARTITION BY A.ORG_ID 
               ,C.CUST_TYP 
          ORDER BY ABS(COALESCE(COALESCE(SUM(A.BAL), 0), 0)) DESC)                       AS RANK 
       ,A.ORG_ID                AS ORG_ID 
       ,B.ORG_NAME              AS ORG_NAME 
       ,A.CUST_ID               AS CUST_ID 
       ,C.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,C.CUST_TYP              AS CUST_TYP 
       ,C.CERT_TYPE             AS CERT_TYPE 
       ,C.CERT_NUM              AS CERT_NUM 
       ,ABS(COALESCE(SUM(A.BAL), 0))                       AS BAL 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_A_INOUTCOME A                                     --
  INNER JOIN ADMIN_AUTH_ORG B                                  --
     ON A.ORG_ID                = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_CI_CUST_DESC C                             --
     ON A.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
  WHERE A.BAL < 0 
    AND A.ODS_DATE              = V_DT 
  GROUP BY A.ORG_ID 
       ,B.ORG_NAME 
       ,A.CUST_ID 
       ,C.CUST_ZH_NAME 
       ,C.CUST_TYP 
       ,C.CERT_TYPE 
       ,C.CERT_NUM 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_INCOME_TOP_01 = sqlContext.sql(sql)
ACRM_A_INCOME_TOP_01.registerTempTable("ACRM_A_INCOME_TOP_01")
#dfn="ACRM_A_INCOME_TOP_01/"+V_DT+".parquet"
#ACRM_A_INCOME_TOP_01.cache()
nrows = ACRM_A_INCOME_TOP_01.count()
#ACRM_A_INCOME_TOP_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
#ACRM_A_INCOME_TOP_01.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_INCOME_TOP_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_INCOME_TOP_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT ROW_NUMBER() OVER(
      PARTITION BY A.ORG_ID 
               ,C.CUST_TYP 
          ORDER BY ABS(COALESCE(COALESCE(SUM(A.BAL), 0), 0)) DESC)                       AS RANK 
       ,A.ORG_ID                AS ORG_ID 
       ,B.ORG_NAME              AS ORG_NAME 
       ,A.CUST_ID               AS CUST_ID 
       ,C.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,C.CUST_TYP              AS CUST_TYP 
       ,C.CERT_TYPE             AS CERT_TYPE 
       ,C.CERT_NUM              AS CERT_NUM 
       ,ABS(COALESCE(SUM(A.BAL), 0))                       AS BAL 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_A_INOUTCOME A                                     --
  INNER JOIN ADMIN_AUTH_ORG B                                  --
     ON B.UP_ORG_ID             = '320000000' 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_CI_CUST_DESC C                             --
     ON A.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
  WHERE A.BAL < 0 
    AND A.ODS_DATE              = V_DT 
  GROUP BY A.ORG_ID 
       ,B.ORG_NAME 
       ,A.CUST_ID 
       ,C.CUST_ZH_NAME 
       ,C.CUST_TYP 
       ,C.CERT_TYPE 
       ,C.CERT_NUM 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_INCOME_TOP_02 = sqlContext.sql(sql)
ACRM_A_INCOME_TOP_02.registerTempTable("ACRM_A_INCOME_TOP_02")
#dfn="ACRM_A_INCOME_TOP_02/"+V_DT+".parquet"
#ACRM_A_INCOME_TOP_02.cache()
nrows = ACRM_A_INCOME_TOP_02.count()
#ACRM_A_INCOME_TOP_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
#ACRM_A_INCOME_TOP_02.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_INCOME_TOP_02/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_INCOME_TOP_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()    AS ID 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_ZH_NAME            AS CUST_NAME 
       ,ABS(A.BAL)                       AS CRE_BAL 
       ,V_DT                    AS ODS_DATE 
       ,CERT_TYPE               AS CERT_TYPE 
       ,CERT_NUM                AS CERT_NUM 
       ,CUST_TYP                AS CUST_TYPE 
   FROM ACRM_A_INCOME_TOP_01 A                                 --机构资金净流入TOP10临时表01
  WHERE RANK <= 10 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_INCOME_TOP = sqlContext.sql(sql)
ACRM_A_INCOME_TOP.registerTempTable("ACRM_A_INCOME_TOP")
dfn="ACRM_A_INCOME_TOP/"+V_DT+".parquet"
ACRM_A_INCOME_TOP.cache()
nrows = ACRM_A_INCOME_TOP.count()
ACRM_A_INCOME_TOP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_INCOME_TOP.unpersist()
#全量表，删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_INCOME_TOP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_INCOME_TOP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()    AS ID 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_ZH_NAME            AS CUST_NAME 
       ,ABS(A.BAL)                       AS CRE_BAL 
       ,V_DT                    AS ODS_DATE 
       ,CERT_TYPE               AS CERT_TYPE 
       ,CERT_NUM                AS CERT_NUM 
       ,CUST_TYP                AS CUST_TYPE 
   FROM ACRM_A_INCOME_TOP_02 A                                 --机构资金净流入TOP10临时表02
  WHERE RANK <= 10 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_INCOME_TOP = sqlContext.sql(sql)
ACRM_A_INCOME_TOP.registerTempTable("ACRM_A_INCOME_TOP")
dfn="ACRM_A_INCOME_TOP/"+V_DT+".parquet"
ACRM_A_INCOME_TOP.cache()
nrows = ACRM_A_INCOME_TOP.count()
ACRM_A_INCOME_TOP.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_INCOME_TOP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_INCOME_TOP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
