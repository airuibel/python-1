#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_AUM_TOP').setMaster(sys.argv[2])
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
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
ACRM_F_RE_AUMSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_AUMSUMAVGINFO/*')
ACRM_F_RE_AUMSUMAVGINFO.registerTempTable("ACRM_F_RE_AUMSUMAVGINFO")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
#目标表
#ACRM_F_CI_CUST_AUM_TOP 全量表


#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ROW_NUMBER() OVER(
      PARTITION BY A.FR_ID 
               ,C.ORG_ID 
               ,B.CUST_TYP 
          ORDER BY A.AUM_AMT DESC)                       AS RANK 
       ,A.CUST_ID               AS CUST_ID 
       ,B.CUST_ZH_NAME          AS CUST_NAME 
       ,B.CUST_TYP              AS CUST_TYP 
       ,B.CERT_TYPE             AS CERT_TYPE 
       ,B.CERT_NUM              AS CERT_NUM 
       ,A.AUM_AMT               AS AUM_AMT 
       ,A.ORG_ID                AS ORG_ID 
       ,C.ORG_NAME              AS ORG_NAME 
       ,''                    AS FR_ID 
   FROM (SELECT FR_ID,CUST_ID,ORG_ID,SUM(AMOUNT) AS AUM_AMT FROM ACRM_F_RE_AUMSUMAVGINFO GROUP BY CUST_ID,ORG_ID,FR_ID) A                                                      --客户机构存款余额汇总
  INNER JOIN OCRM_F_CI_CUST_DESC B                             --统一客户信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN ADMIN_AUTH_ORG C                                  --机构表
     ON A.ORG_ID                = C.ORG_ID 
    AND A.FR_ID                 = C.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_ACRM_F_CI_CUST_AUM_TOP_01 = sqlContext.sql(sql)
TMP_ACRM_F_CI_CUST_AUM_TOP_01.registerTempTable("TMP_ACRM_F_CI_CUST_AUM_TOP_01")
dfn="TMP_ACRM_F_CI_CUST_AUM_TOP_01/"+V_DT+".parquet"
TMP_ACRM_F_CI_CUST_AUM_TOP_01.cache()
nrows = TMP_ACRM_F_CI_CUST_AUM_TOP_01.count()
TMP_ACRM_F_CI_CUST_AUM_TOP_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_ACRM_F_CI_CUST_AUM_TOP_01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_ACRM_F_CI_CUST_AUM_TOP_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_ACRM_F_CI_CUST_AUM_TOP_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT ROW_NUMBER() OVER(
      PARTITION BY A.FR_ID 
               ,B.CUST_TYP 
          ORDER BY A.AUM_AMT DESC)                       AS RANK 
       ,A.CUST_ID               AS CUST_ID 
       ,B.CUST_ZH_NAME          AS CUST_NAME 
       ,B.CUST_TYP              AS CUST_TYP 
       ,B.CERT_TYPE             AS CERT_TYPE 
       ,B.CERT_NUM              AS CERT_NUM 
       ,A.AUM_AMT               AS AUM_AMT 
       ,C.ORG_ID                AS ORG_ID 
       ,C.ORG_NAME              AS ORG_NAME 
       ,A.FR_ID                 AS FR_ID 
   FROM(
         SELECT CUST_ID 
               ,FR_ID 
               ,SUM(AMOUNT)                       AS AUM_AMT 
           FROM ACRM_F_RE_AUMSUMAVGINFO 
          GROUP BY CUST_ID 
               ,FR_ID) A                                       --客户机构存款余额汇总
  INNER JOIN OCRM_F_CI_CUST_DESC B                             --统一客户信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN ADMIN_AUTH_ORG C                                  --机构表
     ON C.FR_ID                 = A.FR_ID 
    AND C.UP_ORG_ID             = '320000000' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_ACRM_F_CI_CUST_AUM_TOP_02 = sqlContext.sql(sql)
TMP_ACRM_F_CI_CUST_AUM_TOP_02.registerTempTable("TMP_ACRM_F_CI_CUST_AUM_TOP_02")
dfn="TMP_ACRM_F_CI_CUST_AUM_TOP_02/"+V_DT+".parquet"
TMP_ACRM_F_CI_CUST_AUM_TOP_02.cache()
nrows = TMP_ACRM_F_CI_CUST_AUM_TOP_02.count()
TMP_ACRM_F_CI_CUST_AUM_TOP_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_ACRM_F_CI_CUST_AUM_TOP_02.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_ACRM_F_CI_CUST_AUM_TOP_02/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_ACRM_F_CI_CUST_AUM_TOP_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_NAME               AS CUST_NAME 
       ,CUST_ID                 AS CUST_ID 
       ,AUM_AMT                 AS AUM_AMT 
       ,V_DT                    AS REPORT_DATE 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
       ,CERT_TYPE               AS CERT_TYPE 
       ,CERT_NUM                AS CERT_NUM 
   FROM TMP_ACRM_F_CI_CUST_AUM_TOP_01 A                        --客户AUM值TOP10临时表01
  WHERE RANK <= 10 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_AUM_TOP = sqlContext.sql(sql)
ACRM_F_CI_CUST_AUM_TOP.registerTempTable("ACRM_F_CI_CUST_AUM_TOP")
dfn="ACRM_F_CI_CUST_AUM_TOP/"+V_DT+".parquet"
ACRM_F_CI_CUST_AUM_TOP.cache()
nrows = ACRM_F_CI_CUST_AUM_TOP.count()
ACRM_F_CI_CUST_AUM_TOP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_AUM_TOP.unpersist()
#全量表，保存后删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_AUM_TOP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_AUM_TOP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_NAME               AS CUST_NAME 
       ,CUST_ID                 AS CUST_ID 
       ,AUM_AMT                 AS AUM_AMT 
       ,V_DT                    AS REPORT_DATE 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
       ,CERT_TYPE               AS CERT_TYPE 
       ,CERT_NUM                AS CERT_NUM 
   FROM TMP_ACRM_F_CI_CUST_AUM_TOP_02 A                        --客户AUM值TOP10临时表02
  WHERE RANK <= 10 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_AUM_TOP = sqlContext.sql(sql)
ACRM_F_CI_CUST_AUM_TOP.registerTempTable("ACRM_F_CI_CUST_AUM_TOP")
dfn="ACRM_F_CI_CUST_AUM_TOP/"+V_DT+".parquet"
ACRM_F_CI_CUST_AUM_TOP.cache()
nrows = ACRM_F_CI_CUST_AUM_TOP.count()
ACRM_F_CI_CUST_AUM_TOP.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_AUM_TOP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_AUM_TOP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
