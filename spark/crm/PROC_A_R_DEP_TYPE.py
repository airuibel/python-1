#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_DEP_TYPE').setMaster(sys.argv[2])
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

ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEP_TYPE/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_A_DEP_TYPE_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_DEP_CRE/")

ACRM_A_DEPOSIT_MONTH_CHART_TEMP1 = sqlContext.read.parquet(hdfs+'/ACRM_A_DEPOSIT_MONTH_CHART_TEMP1/*')
ACRM_A_DEPOSIT_MONTH_CHART_TEMP1.registerTempTable("ACRM_A_DEPOSIT_MONTH_CHART_TEMP1")
OCRM_F_PD_PROD_CATL = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_CATL/*')
OCRM_F_PD_PROD_CATL.registerTempTable("OCRM_F_PD_PROD_CATL")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('000000' AS INTEGER)      AS ID 
       ,A.ORG_ID                AS ORG_ID 
       ,C.ORG_NAME              AS ORG_NAME 
       ,CAST(SUM(CASE WHEN B.CATL_CODE             = 'SA1' THEN A.BAL_RMB END)    AS DECIMAL(24,6))                   AS ENT_H_DEP_BAL 
       ,CAST(SUM(CASE WHEN B.CATL_CODE             = 'TD1' THEN A.BAL_RMB END)    AS DECIMAL(24,6))                 AS ENT_D_DEP_BAL 
       ,CAST(SUM(CASE WHEN B.CATL_CODE             = 'SA0' THEN A.BAL_RMB END)    AS DECIMAL(24,6))                   AS IND_H_DEP_BAL 
       ,CAST(SUM(CASE WHEN B.CATL_CODE             = 'TD0' THEN A.BAL_RMB END)     AS DECIMAL(24,6))                  AS IND_D_DEP_BAL 
       ,V_DT                  AS ODS_DATE 
   FROM ACRM_A_DEPOSIT_MONTH_CHART_TEMP1 A                     --
   LEFT JOIN ADMIN_AUTH_ORG C                                  --
     ON A.ORG_ID                = C.ORG_ID 
   LEFT JOIN (select DISTINCT m.CATL_CODE as CATL_CODE,m.CATL_PARENT as CATL_PARENT,m.fr_id,
   n.PRODUCT_ID as PRODUCT_ID from OCRM_F_PD_PROD_CATL m inner join OCRM_F_PD_PROD_INFO n on m.CATL_CODE = n.CATL_CODE 
   where m.CATL_PARENT in ('TD','SA') ) B                             --
     ON A.PRODUCT_ID            = B.PRODUCT_ID 
  GROUP BY A.ORG_ID 
       ,C.ORG_NAME """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_DEP_TYPE = sqlContext.sql(sql)
ACRM_A_DEP_TYPE.registerTempTable("ACRM_A_DEP_TYPE")
dfn="ACRM_A_DEP_TYPE/"+V_DT+".parquet"
ACRM_A_DEP_TYPE.cache()
nrows = ACRM_A_DEP_TYPE.count()
ACRM_A_DEP_TYPE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_DEP_TYPE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_DEP_TYPE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,ENT_H_DEP_BAL           AS ENT_H_DEP_BAL 
       ,ENT_D_DEP_BAL           AS ENT_D_DEP_BAL 
       ,IND_H_DEP_BAL           AS IND_H_DEP_BAL 
       ,IND_D_DEP_BAL           AS IND_D_DEP_BAL 
       ,ODS_DATE                AS ODS_DATE 
   FROM ACRM_A_DEP_TYPE A                                      --
  WHERE ODS_DATE < TRUNC(DATE(V_DT), 'MM') 
     OR ODS_DATE                = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_DEP_TYPE_TMP = sqlContext.sql(sql)
ACRM_A_DEP_TYPE_TMP.registerTempTable("ACRM_A_DEP_TYPE_TMP")
dfn="ACRM_A_DEP_TYPE_TMP/"+V_DT+".parquet"
ACRM_A_DEP_TYPE_TMP.cache()
nrows = ACRM_A_DEP_TYPE_TMP.count()
ACRM_A_DEP_TYPE_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_DEP_TYPE_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEP_TYPE_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_DEP_TYPE_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,ENT_H_DEP_BAL           AS ENT_H_DEP_BAL 
       ,ENT_D_DEP_BAL           AS ENT_D_DEP_BAL 
       ,IND_H_DEP_BAL           AS IND_H_DEP_BAL 
       ,IND_D_DEP_BAL           AS IND_D_DEP_BAL 
       ,ODS_DATE                AS ODS_DATE 
   FROM ACRM_A_DEP_TYPE_TMP A                                  --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_DEP_TYPE = sqlContext.sql(sql)
ACRM_A_DEP_TYPE.registerTempTable("ACRM_A_DEP_TYPE")
dfn="ACRM_A_DEP_TYPE/"+V_DT+".parquet"
ACRM_A_DEP_TYPE.cache()
nrows = ACRM_A_DEP_TYPE.count()
ACRM_A_DEP_TYPE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_DEP_TYPE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEP_TYPE/"+V_DT_LD+".parquet")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEP_TYPE_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp  /"+dbname+"/ACRM_A_DEP_TYPE/"+V_DT+".parquet /"+dbname+"/ACRM_A_DEP_TYPE/")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_DEP_TYPE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
