#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CERT_INFO').setMaster(sys.argv[2])
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

OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_CI_CBOD_ECCIFIDI = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCIFIDI/*')
F_CI_CBOD_ECCIFIDI.registerTempTable("F_CI_CBOD_ECCIFIDI")
F_TX_WSYH_ECCIF = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIF/*')
F_TX_WSYH_ECCIF.registerTempTable("F_TX_WSYH_ECCIF")
F_CI_WSYH_ECCIFID = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFID/*')
F_CI_WSYH_ECCIFID.registerTempTable("F_CI_WSYH_ECCIFID")
F_TX_WSYH_DEPT = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_DEPT/*')
F_TX_WSYH_DEPT.registerTempTable("F_TX_WSYH_DEPT")
F_TX_WSYH_ECCIFMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIFMCH/*')
F_TX_WSYH_ECCIFMCH.registerTempTable("F_TX_WSYH_ECCIFMCH")
F_CI_XDXT_ENT_AUTH = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_ENT_AUTH/*')
F_CI_XDXT_ENT_AUTH.registerTempTable("F_CI_XDXT_ENT_AUTH")
ACRM_F_CI_CERT_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CERT_INFO_BK/'+V_DT_LD+'.parquet/*')
ACRM_F_CI_CERT_INFO.registerTempTable("ACRM_F_CI_CERT_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(A.ID  AS BIGINT)                  AS ID 
       ,A.CUST_ID                           AS CUST_ID 
       ,A.TACK_INSTN                        AS TACK_INSTN 
       ,A.CERT_TYPE                         AS CERT_TYPE 
       ,A.CERT_NO                           AS CERT_NO 
       ,A.ISSUE_DATE                        AS ISSUE_DATE 
       ,A.CARD_ON_NAME                      AS CARD_ON_NAME 
       ,A.LOST_DATE                         AS LOST_DATE 
       ,A.AS_ANN_ID                         AS AS_ANN_ID 
       ,A.ODS_ST_DATE                       AS ODS_ST_DATE 
       ,A.CREATE_ORG_NAME                   AS CREATE_ORG_NAME 
       ,A.CREATE_USER                       AS CREATE_USER 
       ,A.CREATE_USER_NAME                  AS CREATE_USER_NAME 
       ,A.UPDATE_TIME                       AS UPDATE_TIME 
       ,A.CREATE_ORG                        AS CREATE_ORG 
       ,A.FR_ID                             AS FR_ID 
       ,A.ORG_ID                            AS ORG_ID 
       ,A.SYS_ID                            AS SYS_ID 
   FROM ACRM_F_CI_CERT_INFO A                                  --
  WHERE SYS_ID  NOT IN('LNA', 'IBK') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CERT_INFO_TMP = sqlContext.sql(sql)
ACRM_F_CI_CERT_INFO_TMP.registerTempTable("ACRM_F_CI_CERT_INFO_TMP")
dfn="ACRM_F_CI_CERT_INFO_TMP/"+V_DT+".parquet"
ACRM_F_CI_CERT_INFO_TMP.cache()
nrows = ACRM_F_CI_CERT_INFO_TMP.count()
ACRM_F_CI_CERT_INFO_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CERT_INFO_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CERT_INFO_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CERT_INFO_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
ACRM_F_CI_CERT_INFO_TMP = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CERT_INFO_TMP/*')
ACRM_F_CI_CERT_INFO_TMP.registerTempTable("ACRM_F_CI_CERT_INFO_TMP")
sql = """
 SELECT CAST(A.ID  AS BIGINT)                             AS ID 
       ,A.CUST_ID                        AS CUST_ID 
       ,A.TACK_INSTN                     AS TACK_INSTN 
       ,A.CERT_TYPE                      AS CERT_TYPE 
       ,A.CERT_NO                        AS CERT_NO 
       ,A.ISSUE_DATE                     AS ISSUE_DATE 
       ,A.CARD_ON_NAME                   AS CARD_ON_NAME 
       ,A.LOST_DATE                      AS LOST_DATE 
       ,A.AS_ANN_ID                      AS AS_ANN_ID 
       ,A.ODS_ST_DATE                    AS ODS_ST_DATE 
       ,A.CREATE_ORG_NAME                AS CREATE_ORG_NAME 
       ,A.CREATE_USER                    AS CREATE_USER 
       ,A.CREATE_USER_NAME               AS CREATE_USER_NAME 
       ,A.UPDATE_TIME                    AS UPDATE_TIME 
       ,A.CREATE_ORG                     AS CREATE_ORG 
       ,A.FR_ID                          AS FR_ID 
       ,A.ORG_ID                         AS ORG_ID 
       ,A.SYS_ID                         AS SYS_ID 
   FROM ACRM_F_CI_CERT_INFO_TMP A                              --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CERT_INFO = sqlContext.sql(sql)
dfn="ACRM_F_CI_CERT_INFO/"+V_DT+".parquet"
ACRM_F_CI_CERT_INFO.cache()
nrows = ACRM_F_CI_CERT_INFO.count()
ACRM_F_CI_CERT_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CERT_INFO.unpersist()
ACRM_F_CI_CERT_INFO_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CERT_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CERT_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1
ACRM_F_CI_CERT_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CERT_INFO/*')
ACRM_F_CI_CERT_INFO.registerTempTable("ACRM_F_CI_CERT_INFO")
sql = """
 SELECT monotonically_increasing_id() AS ID 
       ,A.EC_CUST_NO            AS CUST_ID 
       ,''                    AS TACK_INSTN 
       ,A.EC_CER_TYP            AS CERT_TYPE 
       ,A.EC_CER_NO             AS CERT_NO 
       ,''                    AS ISSUE_DATE 
       ,A.EC_FULL_NAM           AS CARD_ON_NAME 
       ,''                    AS LOST_DATE 
       ,''                    AS AS_ANN_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                    AS CREATE_ORG_NAME 
       ,''                    AS CREATE_USER 
       ,''                    AS CREATE_USER_NAME 
       ,''                    AS UPDATE_TIME 
       ,''                    AS CREATE_ORG 
       ,NVL(A.FR_ID, 'UNK')                       AS FR_ID 
       ,''                    AS ORG_ID 
       ,'CEN'                   AS SYS_ID 
   FROM F_CI_CBOD_ECCIFIDI A                                   --
   LEFT JOIN ACRM_F_CI_CERT_INFO B                             --
     ON A.EC_CER_TYP            = B.CERT_TYPE 
    AND A.EC_CER_NO             = B.CERT_NO 
    AND A.EC_FULL_NAM           = B.CARD_ON_NAME 
  WHERE A.ODS_ST_DATE           = V_DT 
    AND B.CERT_TYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CERT_INFO = sqlContext.sql(sql)
dfn="ACRM_F_CI_CERT_INFO/"+V_DT+".parquet"
ACRM_F_CI_CERT_INFO.cache()
nrows = ACRM_F_CI_CERT_INFO.count()
ACRM_F_CI_CERT_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CERT_INFO.unpersist()
F_CI_CBOD_ECCIFIDI.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CERT_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id() AS ID 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,A.CERTORG               AS TACK_INSTN 
       ,A.CERTTYPE              AS CERT_TYPE 
       ,A.CERTID                AS CERT_NO 
       ,''                    AS ISSUE_DATE 
       ,A.NAME_RENDING          AS CARD_ON_NAME 
       ,''                    AS LOST_DATE 
       ,''                    AS AS_ANN_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,''                    AS CREATE_ORG_NAME 
       ,''                    AS CREATE_USER 
       ,''                    AS CREATE_USER_NAME 
       ,''                    AS UPDATE_TIME 
       ,''                    AS CREATE_ORG 
       ,B.FR_ID                 AS FR_ID 
       ,A.INPUTORGID            AS ORG_ID 
       ,'LNA'                   AS SYS_ID 
   FROM F_CI_XDXT_ENT_AUTH A                                   --
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --
     ON B.SOURCE_CUST_ID        = A.CUSTOMERID 
    AND B.ODS_SYS_ID            = 'LNA' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CERT_INFO = sqlContext.sql(sql)
dfn="ACRM_F_CI_CERT_INFO/"+V_DT+".parquet"
ACRM_F_CI_CERT_INFO.cache()
nrows = ACRM_F_CI_CERT_INFO.count()
ACRM_F_CI_CERT_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CERT_INFO.unpersist()
F_CI_XDXT_ENT_AUTH.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CERT_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id() AS ID 
       ,E.ODS_CUST_ID           AS CUST_ID 
       ,''                    AS TACK_INSTN 
       ,B.IDTYPE                AS CERT_TYPE 
       ,B.IDNO                  AS CERT_NO 
       ,''                    AS ISSUE_DATE 
       ,A.CIFNAME               AS CARD_ON_NAME 
       ,''                    AS LOST_DATE 
       ,''                    AS AS_ANN_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                    AS CREATE_ORG_NAME 
       ,''                    AS CREATE_USER 
       ,''                    AS CREATE_USER_NAME 
       ,''                    AS UPDATE_TIME 
       ,''                    AS CREATE_ORG 
       ,E.FR_ID                 AS FR_ID 
       ,D.DEPTID                AS ORG_ID 
       ,'IBK'                   AS SYS_ID 
   FROM F_TX_WSYH_ECCIF A                                      --
  INNER JOIN F_CI_WSYH_ECCIFID B                               --
     ON A.CIFSEQ                = B.CIFSEQ 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN F_TX_WSYH_ECCIFMCH C                              --
     ON A.CIFSEQ                = C.CIFSEQ 
    AND C.MCHANNELID            = 'EIBS' 
  INNER JOIN F_TX_WSYH_DEPT D                                  --
     ON A.CIFDEPTSEQ            = D.DEPTSEQ 
  INNER JOIN OCRM_F_CI_SYS_RESOURCE E                          --
     ON A.CIFNAME               = E.SOURCE_CUST_NAME 
    AND B.IDTYPE                = E.CERT_TYPE 
    AND B.IDNO                  = E.CERT_NO 
    AND E.ODS_SYS_ID            = 'IBK' 
    AND A.FR_ID                 = E.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CERT_INFO = sqlContext.sql(sql)
dfn="ACRM_F_CI_CERT_INFO/"+V_DT+".parquet"
ACRM_F_CI_CERT_INFO.cache()
nrows = ACRM_F_CI_CERT_INFO.count()
ACRM_F_CI_CERT_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CERT_INFO.unpersist()
F_TX_WSYH_ECCIF.unpersist()
F_CI_WSYH_ECCIFID.unpersist()
F_TX_WSYH_ECCIFMCH.unpersist()
F_TX_WSYH_DEPT.unpersist()
OCRM_F_CI_SYS_RESOURCE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CERT_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CERT_INFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_CI_CERT_INFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_CI_CERT_INFO_BK/")
