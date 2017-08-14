#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_CRE_TYPE').setMaster(sys.argv[2])
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

ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CRE_TYPE/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_A_CRE_TYPE_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_CRE_TYPE/"+V_DT+".parquet")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.LENDER                AS ORG_ID 
       ,C.ORG_NAME              AS ORG_NAME 
       ,CASE WHEN(A.PRODUCT_ID LIKE '1010%' 
             OR A.PRODUCT_ID LIKE '1020%' 
             OR A.PRODUCT_ID LIKE '1030%' 
             OR A.PRODUCT_ID LIKE '1040%' 
             OR A.PRODUCT_ID LIKE '1050%' 
             OR A.PRODUCT_ID LIKE '1060%' 
             OR A.PRODUCT_ID LIKE '1070%') THEN NVL(BAL, 0) ELSE 0 END                     AS ENT_LOAN_BAL 
       ,CASE WHEN A.PRODUCT_ID LIKE '3010%' THEN NVL(CONT_AMT, 0) ELSE 0 END                     AS ENT_CRE_ALL 
       ,CASE WHEN(A.PRODUCT_ID LIKE '2010%' 
             OR A.PRODUCT_ID LIKE '2020%' 
             OR A.PRODUCT_ID LIKE '2030%' 
             OR A.PRODUCT_ID LIKE '2040%' 
             OR A.PRODUCT_ID LIKE '2050%') THEN NVL(BAL, 0) ELSE 0 END                     AS IND_LOAN_BAL 
       ,CASE WHEN A.PRODUCT_ID LIKE '3020%' THEN NVL(CONT_AMT, 0) ELSE 0 END                     AS IND_CRE_ALL 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN ADMIN_AUTH_ORG C                                  --
     ON A.LENDER                = C.ORG_ID 
  WHERE A.BAL > 0 
  AND A.END_DATE >V_DT
  --  AND A.END_DATE > SUBSTR(CHAR((V_DT), ISO), 1, 4) || SUBSTR(CHAR((V_DT), ISO), 6, 2) || SUBSTR(CHAR((V_DT), ISO), 9, 2)
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_ACRM_A_CRE_TYPE = sqlContext.sql(sql)
TMP_ACRM_A_CRE_TYPE.registerTempTable("TMP_ACRM_A_CRE_TYPE")
dfn="TMP_ACRM_A_CRE_TYPE/"+V_DT+".parquet"
TMP_ACRM_A_CRE_TYPE.cache()
nrows = TMP_ACRM_A_CRE_TYPE.count()
TMP_ACRM_A_CRE_TYPE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_ACRM_A_CRE_TYPE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_ACRM_A_CRE_TYPE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_ACRM_A_CRE_TYPE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('000000' AS INTEGER)      AS ID 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CAST(SUM(ENT_LOAN_BAL) AS DECIMAL(24,6))                      AS ENT_LOAN_BAL 
       ,CAST(SUM(ENT_CRE_ALL - ENT_LOAN_BAL)  AS DECIMAL(24,6))                     AS ENT_CRE_BAL 
       ,CAST(SUM(ENT_CRE_ALL)  AS DECIMAL(24,6))                    AS ENT_CRE_ALL 
       ,CAST(SUM(IND_LOAN_BAL)  AS   DECIMAL(24,6))                   AS IND_LOAN_BAL 
       ,CAST(SUM(IND_CRE_ALL - IND_LOAN_BAL)  AS DECIMAL(24,6))                     AS IND_CRE_BAL 
       ,CAST(SUM(IND_CRE_ALL)     AS DECIMAL(24,6))                  AS IND_CRE_ALL 
       ,V_DT                  AS ODS_DATE 
   FROM TMP_ACRM_A_CRE_TYPE A                                  --
  GROUP BY ORG_ID 
       ,ORG_NAME """


sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CRE_TYPE = sqlContext.sql(sql)
ACRM_A_CRE_TYPE.registerTempTable("ACRM_A_CRE_TYPE")
dfn="ACRM_A_CRE_TYPE/"+V_DT+".parquet"
ACRM_A_CRE_TYPE.cache()
nrows = ACRM_A_CRE_TYPE.count()
ACRM_A_CRE_TYPE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_CRE_TYPE.unpersist()
ACRM_F_CI_ASSET_BUSI_PROTO.unpersist()
ADMIN_AUTH_ORG.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CRE_TYPE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,ENT_LOAN_BAL            AS ENT_LOAN_BAL 
       ,ENT_CRE_BAL             AS ENT_CRE_BAL 
       ,ENT_CRE_ALL             AS ENT_CRE_ALL 
       ,IND_LOAN_BAL            AS IND_LOAN_BAL 
       ,IND_CRE_BAL             AS IND_CRE_BAL 
       ,IND_CRE_ALL             AS IND_CRE_ALL 
       ,ODS_DATE                AS ODS_DATE 
   FROM ACRM_A_CRE_TYPE A                                      --
  WHERE ODS_DATE < TRUNC(DATE(V_DT), 'MM') 
     OR ODS_DATE                >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CRE_TYPE_TMP = sqlContext.sql(sql)
ACRM_A_CRE_TYPE_TMP.registerTempTable("ACRM_A_CRE_TYPE_TMP")
dfn="ACRM_A_CRE_TYPE_TMP/"+V_DT+".parquet"
ACRM_A_CRE_TYPE_TMP.cache()
nrows = ACRM_A_CRE_TYPE_TMP.count()
ACRM_A_CRE_TYPE_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_CRE_TYPE_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CRE_TYPE_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CRE_TYPE_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,ENT_LOAN_BAL            AS ENT_LOAN_BAL 
       ,ENT_CRE_BAL             AS ENT_CRE_BAL 
       ,ENT_CRE_ALL             AS ENT_CRE_ALL 
       ,IND_LOAN_BAL            AS IND_LOAN_BAL 
       ,IND_CRE_BAL             AS IND_CRE_BAL 
       ,IND_CRE_ALL             AS IND_CRE_ALL 
       ,ODS_DATE                AS ODS_DATE 
   FROM ACRM_A_CRE_TYPE_TMP A                                  --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CRE_TYPE = sqlContext.sql(sql)
ACRM_A_CRE_TYPE.registerTempTable("ACRM_A_CRE_TYPE")
dfn="ACRM_A_CRE_TYPE/"+V_DT+".parquet"
ACRM_A_CRE_TYPE.cache()
nrows = ACRM_A_CRE_TYPE.count()
ACRM_A_CRE_TYPE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_CRE_TYPE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CRE_TYPE/"+V_DT_LD+".parquet")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CRE_TYPE_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp  /"+dbname+"/ACRM_A_CRE_TYPE/"+V_DT+".parquet /"+dbname+"/ACRM_A_CRE_TYPE_BK/")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CRE_TYPE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
