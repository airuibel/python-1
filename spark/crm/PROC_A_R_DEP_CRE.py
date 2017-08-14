#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_DEP_CRE').setMaster(sys.argv[2])
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

ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEP_CRE/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_A_DEP_CRE_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_DEP_CRE/")

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
ACRM_A_DEPOSIT_MONTH_CHART_TEMP = sqlContext.read.parquet(hdfs+'/ACRM_A_DEPOSIT_MONTH_CHART_TEMP/*')
ACRM_A_DEPOSIT_MONTH_CHART_TEMP.registerTempTable("ACRM_A_DEPOSIT_MONTH_CHART_TEMP")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ORG_ID                AS ORG_ID 
       ,C.ORG_NAME              AS ORG_NAME 
       ,SUM(A.DEP_MONTH_AVG)                       AS DEP_MONTH 
   FROM ACRM_A_DEPOSIT_MONTH_CHART_TEMP A                      --
  INNER JOIN ADMIN_AUTH_ORG C                                  --
     ON A.ORG_ID                = C.ORG_ID 
  GROUP BY A.ORG_ID 
       ,C.ORG_NAME """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_DEP_TMP = sqlContext.sql(sql)
ACRM_A_DEP_TMP.registerTempTable("ACRM_A_DEP_TMP")
dfn="ACRM_A_DEP_TMP/"+V_DT+".parquet"
ACRM_A_DEP_TMP.cache()
nrows = ACRM_A_DEP_TMP.count()
ACRM_A_DEP_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_DEP_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEP_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_DEP_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.LENDER                AS ORG_ID 
       ,C.ORG_NAME              AS ORG_NAME 
       ,SUM(A.MONTH_RMB)                       AS CRE_MONTH 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN ADMIN_AUTH_ORG C                                  --
     ON A.LENDER                = C.ORG_ID 
  WHERE(A.SUBJECTNO LIKE '1301%' 
             OR A.SUBJECTNO LIKE '1302%' 
             OR A.SUBJECTNO LIKE '1303%' 
             OR A.SUBJECTNO LIKE '1304%' 
             OR A.SUBJECTNO LIKE '1305%' 
             OR A.SUBJECTNO LIKE '1306%' 
             OR A.SUBJECTNO LIKE '1307%' 
             OR A.SUBJECTNO LIKE '1308%') 
    AND A.BAL > 0 
    AND A.LN_APCL_FLG           = 'N' 
  GROUP BY A.LENDER 
       ,C.ORG_NAME """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CRE_TMP = sqlContext.sql(sql)
ACRM_A_CRE_TMP.registerTempTable("ACRM_A_CRE_TMP")
dfn="ACRM_A_CRE_TMP/"+V_DT+".parquet"
ACRM_A_CRE_TMP.cache()
nrows = ACRM_A_CRE_TMP.count()
ACRM_A_CRE_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_CRE_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CRE_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CRE_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('000000' AS INTEGER)      AS ID 
       ,T.ORG_ID                AS ORG_ID 
       ,T.ORG_NAME              AS ORG_NAME 
       ,CAST(NVL(A.DEP_MONTH, 0) AS DECIMAL(24,6))                       AS DEP_MONTH 
       ,CAST(NVL(B.CRE_MONTH, 0)  AS DECIMAL(24,6))                     AS CRE_MONTH 
       ,V_DT                  AS ODS_DATE 
   FROM ADMIN_AUTH_ORG T                                       --
  INNER JOIN ACRM_A_DEP_TMP A                                  --
     ON T.ORG_ID                = A.ORG_ID 
  INNER JOIN ACRM_A_CRE_TMP B                                  --
     ON T.ORG_ID                = B.ORG_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_DEP_CRE = sqlContext.sql(sql)
ACRM_A_DEP_CRE.registerTempTable("ACRM_A_DEP_CRE")
dfn="ACRM_A_DEP_CRE/"+V_DT+".parquet"
ACRM_A_DEP_CRE.cache()
nrows = ACRM_A_DEP_CRE.count()
ACRM_A_DEP_CRE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_DEP_CRE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_DEP_CRE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,DEP_MONTH               AS DEP_MONTH 
       ,CRE_MONTH               AS CRE_MONTH 
       ,ODS_DATE                AS ODS_DATE 
   FROM ACRM_A_DEP_CRE A                                       --
  WHERE ODS_DATE < TRUNC(DATE(V_DT), 'MM') 
     OR ODS_DATE               >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_DEP_CRE_TMP = sqlContext.sql(sql)
ACRM_A_DEP_CRE_TMP.registerTempTable("ACRM_A_DEP_CRE_TMP")
dfn="ACRM_A_DEP_CRE_TMP/"+V_DT+".parquet"
ACRM_A_DEP_CRE_TMP.cache()
nrows = ACRM_A_DEP_CRE_TMP.count()
ACRM_A_DEP_CRE_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_DEP_CRE_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEP_CRE_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_DEP_CRE_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,DEP_MONTH               AS DEP_MONTH 
       ,CRE_MONTH               AS CRE_MONTH 
       ,ODS_DATE                AS ODS_DATE 
   FROM ACRM_A_DEP_CRE_TMP A                                   --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_DEP_CRE = sqlContext.sql(sql)
ACRM_A_DEP_CRE.registerTempTable("ACRM_A_DEP_CRE")
dfn="ACRM_A_DEP_CRE/"+V_DT+".parquet"
ACRM_A_DEP_CRE.cache()
nrows = ACRM_A_DEP_CRE.count()
ACRM_A_DEP_CRE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_DEP_CRE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEP_CRE/"+V_DT_LD+".parquet")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEP_CRE_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp  /"+dbname+"/ACRM_A_DEP_CRE/"+V_DT+".parquet /"+dbname+"/ACRM_A_DEP_CRE_BK/")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_DEP_CRE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
