#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_CUS_DEP_TOP_REPORT').setMaster(sys.argv[2])
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

ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,OPEN_BRC                AS OPEN_BRC 
       ,SUM(AMOUNT)                       AS DEP_AMOUNT_RMB 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --存款积数表
  WHERE A.YEAR                  = YEAR(V_DT) 
  GROUP BY CUST_ID 
       ,OPEN_BRC 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_MCRM_CUS_DEP_TOP_REPORT_01 = sqlContext.sql(sql)
TMP_MCRM_CUS_DEP_TOP_REPORT_01.registerTempTable("TMP_MCRM_CUS_DEP_TOP_REPORT_01")
dfn="TMP_MCRM_CUS_DEP_TOP_REPORT_01/"+V_DT+".parquet"
TMP_MCRM_CUS_DEP_TOP_REPORT_01.cache()
nrows = TMP_MCRM_CUS_DEP_TOP_REPORT_01.count()
TMP_MCRM_CUS_DEP_TOP_REPORT_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_MCRM_CUS_DEP_TOP_REPORT_01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_MCRM_CUS_DEP_TOP_REPORT_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_MCRM_CUS_DEP_TOP_REPORT_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
TMP_MCRM_CUS_DEP_TOP_REPORT_01 = sqlContext.read.parquet(hdfs+'/TMP_MCRM_CUS_DEP_TOP_REPORT_01/*')
TMP_MCRM_CUS_DEP_TOP_REPORT_01.registerTempTable("TMP_MCRM_CUS_DEP_TOP_REPORT_01")
sql = """
 SELECT ROW_NUMBER() OVER(
      PARTITION BY C.ORG_ID 
               ,G.CUST_TYP 
          ORDER BY COALESCE(E.DEP_AMOUNT_RMB, 0) DESC)                       AS RANK 
       ,E.CUST_ID               AS CUST_ID 
       ,G.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,G.CERT_TYPE             AS CERT_TYPE 
       ,G.CERT_NUM              AS CERT_NUM 
       ,G.IS_CRE                AS IS_CRE 
       ,G.OBJ_RATING            AS OBJ_RATING 
       ,E.DEP_AMOUNT_RMB        AS DEP_AMOUNT_RMB 
       ,C.ORG_ID                AS ORG_ID 
       ,C.ORG_NAME              AS ORG_NAME 
       ,V_DT                    AS CRM_DT 
       ,E.FR_ID                 AS FR_ID 
       ,G.CUST_TYP              AS CUST_TYP 
   FROM TMP_MCRM_CUS_DEP_TOP_REPORT_01 E                       --
  INNER JOIN OCRM_F_CI_CUST_DESC G                             --统一客户信息表
     ON G.FR_ID                 = E.FR_ID 
    AND E.CUST_ID               = G.CUST_ID 
  INNER JOIN ADMIN_AUTH_ORG C                                  --机构表
     ON C.FR_ID                 = E.FR_ID 
    AND E.OPEN_BRC              = C.ORG_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_MCRM_CUS_DEP_TOP_REPORT_02 = sqlContext.sql(sql)
TMP_MCRM_CUS_DEP_TOP_REPORT_02.registerTempTable("TMP_MCRM_CUS_DEP_TOP_REPORT_02")
dfn="TMP_MCRM_CUS_DEP_TOP_REPORT_02/"+V_DT+".parquet"
TMP_MCRM_CUS_DEP_TOP_REPORT_02.cache()
nrows = TMP_MCRM_CUS_DEP_TOP_REPORT_02.count()
TMP_MCRM_CUS_DEP_TOP_REPORT_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_MCRM_CUS_DEP_TOP_REPORT_02.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_MCRM_CUS_DEP_TOP_REPORT_02/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_MCRM_CUS_DEP_TOP_REPORT_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,SUM(AMOUNT)                       AS DEP_AMOUNT_RMB 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --存款积数表
  WHERE A.YEAR                  = YEAR(V_DT) 
  GROUP BY CUST_ID 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_MCRM_CUS_DEP_TOP_REPORT_03 = sqlContext.sql(sql)
TMP_MCRM_CUS_DEP_TOP_REPORT_03.registerTempTable("TMP_MCRM_CUS_DEP_TOP_REPORT_03")
dfn="TMP_MCRM_CUS_DEP_TOP_REPORT_03/"+V_DT+".parquet"
TMP_MCRM_CUS_DEP_TOP_REPORT_03.cache()
nrows = TMP_MCRM_CUS_DEP_TOP_REPORT_03.count()
TMP_MCRM_CUS_DEP_TOP_REPORT_03.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_MCRM_CUS_DEP_TOP_REPORT_03.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_MCRM_CUS_DEP_TOP_REPORT_03/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_MCRM_CUS_DEP_TOP_REPORT_03 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::
V_STEP = V_STEP + 1
TMP_MCRM_CUS_DEP_TOP_REPORT_03 = sqlContext.read.parquet(hdfs+'/TMP_MCRM_CUS_DEP_TOP_REPORT_03/*')
TMP_MCRM_CUS_DEP_TOP_REPORT_03.registerTempTable("TMP_MCRM_CUS_DEP_TOP_REPORT_03")
sql = """
 SELECT ROW_NUMBER() OVER(
      PARTITION BY C.ORG_ID 
               ,G.CUST_TYP 
          ORDER BY COALESCE(E.DEP_AMOUNT_RMB, 0) DESC)                       AS RANK 
       ,E.CUST_ID               AS CUST_ID 
       ,G.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,G.CERT_TYPE             AS CERT_TYPE 
       ,G.CERT_NUM              AS CERT_NUM 
       ,G.IS_CRE                AS IS_CRE 
       ,G.OBJ_RATING            AS OBJ_RATING 
       ,E.DEP_AMOUNT_RMB        AS DEP_AMOUNT_RMB 
       ,C.ORG_ID                AS ORG_ID 
       ,C.ORG_NAME              AS ORG_NAME 
       ,V_DT                    AS CRM_DT 
       ,E.FR_ID                 AS FR_ID 
       ,G.CUST_TYP              AS CUST_TYP 
   FROM TMP_MCRM_CUS_DEP_TOP_REPORT_03 E                       --
  INNER JOIN OCRM_F_CI_CUST_DESC G                             --统一客户信息表
     ON G.FR_ID                 = E.FR_ID 
    AND E.CUST_ID               = G.CUST_ID 
  INNER JOIN ADMIN_AUTH_ORG C                                  --机构表
     ON C.FR_ID                 = E.FR_ID 
    AND C.UP_ORG_ID             = '320000000' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_MCRM_CUS_DEP_TOP_REPORT_04 = sqlContext.sql(sql)
TMP_MCRM_CUS_DEP_TOP_REPORT_04.registerTempTable("TMP_MCRM_CUS_DEP_TOP_REPORT_04")
dfn="TMP_MCRM_CUS_DEP_TOP_REPORT_04/"+V_DT+".parquet"
TMP_MCRM_CUS_DEP_TOP_REPORT_04.cache()
nrows = TMP_MCRM_CUS_DEP_TOP_REPORT_04.count()
TMP_MCRM_CUS_DEP_TOP_REPORT_04.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_MCRM_CUS_DEP_TOP_REPORT_04.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_MCRM_CUS_DEP_TOP_REPORT_04/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_MCRM_CUS_DEP_TOP_REPORT_04 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-05::
V_STEP = V_STEP + 1
TMP_MCRM_CUS_DEP_TOP_REPORT_02 = sqlContext.read.parquet(hdfs+'/TMP_MCRM_CUS_DEP_TOP_REPORT_02/*')
TMP_MCRM_CUS_DEP_TOP_REPORT_02.registerTempTable("TMP_MCRM_CUS_DEP_TOP_REPORT_02")
sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_ZH_NAME            AS CUST_ZH_NAME 
       ,CERT_NUM                AS CERT_NUM 
       ,IS_CRE                  AS IS_CRE 
       ,OBJ_RATING              AS OBJ_RATING 
       ,DEP_AMOUNT_RMB          AS AMOUNT 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CRM_DT                  AS REPORT_DATE 
       ,FR_ID                   AS FR_ID 
       ,RANK                    AS RANK 
       ,CUST_TYP                AS CUST_TYP 
       ,CERT_TYPE               AS CERT_TYPE 
   FROM TMP_MCRM_CUS_DEP_TOP_REPORT_02 A                       --机构客户存款TOP100临时表02
  WHERE RANK BETWEEN 1 
    AND 100 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_CUS_DEP_TOP_REPORT = sqlContext.sql(sql)
MCRM_CUS_DEP_TOP_REPORT.registerTempTable("MCRM_CUS_DEP_TOP_REPORT")
dfn="MCRM_CUS_DEP_TOP_REPORT/"+V_DT+".parquet"
MCRM_CUS_DEP_TOP_REPORT.cache()
nrows = MCRM_CUS_DEP_TOP_REPORT.count()
MCRM_CUS_DEP_TOP_REPORT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_CUS_DEP_TOP_REPORT.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_CUS_DEP_TOP_REPORT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_CUS_DEP_TOP_REPORT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-06::
V_STEP = V_STEP + 1
TMP_MCRM_CUS_DEP_TOP_REPORT_04 = sqlContext.read.parquet(hdfs+'/TMP_MCRM_CUS_DEP_TOP_REPORT_04/*')
TMP_MCRM_CUS_DEP_TOP_REPORT_04.registerTempTable("TMP_MCRM_CUS_DEP_TOP_REPORT_04")
sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_ZH_NAME            AS CUST_ZH_NAME 
       ,CERT_NUM                AS CERT_NUM 
       ,IS_CRE                  AS IS_CRE 
       ,OBJ_RATING              AS OBJ_RATING 
       ,DEP_AMOUNT_RMB          AS AMOUNT 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CRM_DT                  AS REPORT_DATE 
       ,FR_ID                   AS FR_ID 
       ,RANK                    AS RANK 
       ,CUST_TYP                AS CUST_TYP 
       ,CERT_TYPE               AS CERT_TYPE 
   FROM TMP_MCRM_CUS_DEP_TOP_REPORT_04 A                       --机构客户存款TOP100临时表02
  WHERE RANK BETWEEN 1 
    AND 100 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_CUS_DEP_TOP_REPORT = sqlContext.sql(sql)
MCRM_CUS_DEP_TOP_REPORT.registerTempTable("MCRM_CUS_DEP_TOP_REPORT")
dfn="MCRM_CUS_DEP_TOP_REPORT/"+V_DT+".parquet"
MCRM_CUS_DEP_TOP_REPORT.cache()
nrows = MCRM_CUS_DEP_TOP_REPORT.count()
MCRM_CUS_DEP_TOP_REPORT.write.save(path=hdfs + '/' + dfn, mode='append')
MCRM_CUS_DEP_TOP_REPORT.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_CUS_DEP_TOP_REPORT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_CUS_DEP_TOP_REPORT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
