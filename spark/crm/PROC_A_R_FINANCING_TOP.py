#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_FINANCING_TOP').setMaster(sys.argv[2])
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
ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
#目标表
#ACRM_F_CI_CUST_FIN_TOP 增量表


#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ROW_NUMBER() OVER(
      PARTITION BY F.FR_ID 
               ,D.CUST_TYP 
          ORDER BY COALESCE(E.FIN_AMT, 0) DESC)                       AS RANK 
       ,E.CUST_ID               AS CUST_ID 
       ,D.CUST_ZH_NAME          AS CUST_NAME 
       ,D.CERT_TYPE             AS CERT_TYPE 
       ,D.CERT_NUM              AS CERT_NUM 
       ,E.FIN_AMT               AS FIN_AMT 
       ,V_DT                    AS ETL_DATE 
       ,F.ORG_ID                AS ORG_ID 
       ,F.ORG_NAME              AS ORG_NAME 
       ,D.CUST_TYP              AS CUST_TYP 
       ,E.FR_ID                 AS FR_ID 
   FROM (SELECT A.CUST_ID,A.FR_ID,A.ORG_ID,A.YEAR,SUM(A.AMT) AS FIN_AMT 
           FROM ACRM_F_RE_ACCSUMAVGINFO A 
          WHERE A.YEAR = YEAR(V_DT) 
          GROUP BY A.CUST_ID,A.FR_ID,A.ORG_ID,A.YEAR) E                                                      --机构客户理财汇总表
  INNER JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON E.CUST_ID               = B.CUST_ID 
    AND E.ORG_ID                = B.INSTITUTION_CODE 
    AND B.MAIN_TYPE             = '1' 
    AND B.FR_ID                 = E.FR_ID 
  INNER JOIN OCRM_F_CI_CUST_DESC D                             --统一客户信息表
     ON E.CUST_ID               = D.CUST_ID 
    AND D.FR_ID                 = E.FR_ID 
  INNER JOIN ADMIN_AUTH_ORG F                                  --机构表
     ON F.ORG_ID                = E.ORG_ID 
    AND F.FR_ID                 = E.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_ACRM_F_CI_CUST_FIN_TOP_01 = sqlContext.sql(sql)
TMP_ACRM_F_CI_CUST_FIN_TOP_01.registerTempTable("TMP_ACRM_F_CI_CUST_FIN_TOP_01")
#dfn="TMP_ACRM_F_CI_CUST_FIN_TOP_01/"+V_DT+".parquet"
#TMP_ACRM_F_CI_CUST_FIN_TOP_01.cache()
nrows = TMP_ACRM_F_CI_CUST_FIN_TOP_01.count()
#TMP_ACRM_F_CI_CUST_FIN_TOP_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
#TMP_ACRM_F_CI_CUST_FIN_TOP_01.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_ACRM_F_CI_CUST_FIN_TOP_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_ACRM_F_CI_CUST_FIN_TOP_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT ROW_NUMBER() OVER(
      PARTITION BY F.FR_ID 
               ,D.CUST_TYP 
          ORDER BY COALESCE(E.FIN_AMT, 0) DESC)                       AS RANK 
       ,E.CUST_ID               AS CUST_ID 
       ,D.CUST_ZH_NAME          AS CUST_NAME 
       ,D.CERT_TYPE             AS CERT_TYPE 
       ,D.CERT_NUM              AS CERT_NUM 
       ,E.FIN_AMT               AS FIN_AMT 
       ,V_DT                    AS ETL_DATE 
       ,F.ORG_ID                AS ORG_ID 
       ,F.ORG_NAME              AS ORG_NAME 
       ,D.CUST_TYP              AS CUST_TYP 
       ,E.FR_ID                 AS FR_ID 
   FROM (SELECT A.CUST_ID,A.FR_ID,A.ORG_ID,A.YEAR,SUM(A.AMT) AS FIN_AMT 
           FROM ACRM_F_RE_ACCSUMAVGINFO A 
          WHERE A.YEAR = YEAR(V_DT) 
          GROUP BY A.CUST_ID,A.FR_ID,A.ORG_ID,A.YEAR) E                                                      --机构客户理财汇总表
  INNER JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON E.CUST_ID               = B.CUST_ID 
    AND E.ORG_ID                = B.INSTITUTION_CODE 
    AND B.MAIN_TYPE             = '1' 
    AND B.FR_ID                 = E.FR_ID 
  INNER JOIN OCRM_F_CI_CUST_DESC D                             --统一客户信息表
     ON E.CUST_ID               = D.CUST_ID 
    AND D.FR_ID                 = E.FR_ID 
  INNER JOIN ADMIN_AUTH_ORG F                                  --机构表
     ON F.FR_ID                 = E.FR_ID 
    AND F.UP_ORG_ID             = '320000000' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_ACRM_F_CI_CUST_FIN_TOP_02 = sqlContext.sql(sql)
TMP_ACRM_F_CI_CUST_FIN_TOP_02.registerTempTable("TMP_ACRM_F_CI_CUST_FIN_TOP_02")
#dfn="TMP_ACRM_F_CI_CUST_FIN_TOP_02/"+V_DT+".parquet"
#TMP_ACRM_F_CI_CUST_FIN_TOP_02.cache()
#nrows = TMP_ACRM_F_CI_CUST_FIN_TOP_02.count()
#TMP_ACRM_F_CI_CUST_FIN_TOP_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
#TMP_ACRM_F_CI_CUST_FIN_TOP_02.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_ACRM_F_CI_CUST_FIN_TOP_02/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_ACRM_F_CI_CUST_FIN_TOP_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_NAME               AS CUST_NAME 
       ,CUST_ID                 AS CUST_ID 
       ,FIN_AMT                 AS FIN_AMT 
       ,ETL_DATE                AS REPORT_DATE 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
       ,CERT_TYPE               AS CERT_TYPE 
       ,CERT_NUM                AS CERT_NUM 
   FROM TMP_ACRM_F_CI_CUST_FIN_TOP_01 A                        --客户理财TOP10临时表01
  WHERE RANK <= 10 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_FIN_TOP = sqlContext.sql(sql)
ACRM_F_CI_CUST_FIN_TOP.registerTempTable("ACRM_F_CI_CUST_FIN_TOP")
dfn="ACRM_F_CI_CUST_FIN_TOP/"+V_DT+".parquet"
ACRM_F_CI_CUST_FIN_TOP.cache()
nrows = ACRM_F_CI_CUST_FIN_TOP.count()
ACRM_F_CI_CUST_FIN_TOP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_FIN_TOP.unpersist()
#增量表
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_FIN_TOP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_FIN_TOP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_NAME               AS CUST_NAME 
       ,CUST_ID                 AS CUST_ID 
       ,FIN_AMT                 AS FIN_AMT 
       ,ETL_DATE                AS REPORT_DATE 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
       ,CERT_TYPE               AS CERT_TYPE 
       ,CERT_NUM                AS CERT_NUM 
   FROM TMP_ACRM_F_CI_CUST_FIN_TOP_02 A                        --客户理财TOP10临时表02
  WHERE RANK <= 10 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_FIN_TOP = sqlContext.sql(sql)
ACRM_F_CI_CUST_FIN_TOP.registerTempTable("ACRM_F_CI_CUST_FIN_TOP")
dfn="ACRM_F_CI_CUST_FIN_TOP/"+V_DT+".parquet"
ACRM_F_CI_CUST_FIN_TOP.cache()
nrows = ACRM_F_CI_CUST_FIN_TOP.count()
ACRM_F_CI_CUST_FIN_TOP.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_FIN_TOP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_FIN_TOP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
