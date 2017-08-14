#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_LINKMAN').setMaster(sys.argv[2])
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

F_CI_XDXT_CUSTOMER_RELATIVE = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_RELATIVE/*')
F_CI_XDXT_CUSTOMER_RELATIVE.registerTempTable("F_CI_XDXT_CUSTOMER_RELATIVE")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_TX_WSYH_ECCIF = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIF/*')
F_TX_WSYH_ECCIF.registerTempTable("F_TX_WSYH_ECCIF")
F_CI_DXPT_CUSTOMER = sqlContext.read.parquet(hdfs+'/F_CI_DXPT_CUSTOMER/*')
F_CI_DXPT_CUSTOMER.registerTempTable("F_CI_DXPT_CUSTOMER")
F_CI_DXPT_ADDRESS = sqlContext.read.parquet(hdfs+'/F_CI_DXPT_ADDRESS/*')
F_CI_DXPT_ADDRESS.registerTempTable("F_CI_DXPT_ADDRESS")
F_CI_WSYH_ECCIFID = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFID/*')
F_CI_WSYH_ECCIFID.registerTempTable("F_CI_WSYH_ECCIFID")
F_CI_CBOD_ECCIFCCR = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCIFCCR/*')
F_CI_CBOD_ECCIFCCR.registerTempTable("F_CI_CBOD_ECCIFCCR")
F_TX_WSYH_ECCIFMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIFMCH/*')
F_TX_WSYH_ECCIFMCH.registerTempTable("F_TX_WSYH_ECCIFMCH")
F_CI_WSYH_ECUSR = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECUSR/*')
F_CI_WSYH_ECUSR.registerTempTable("F_CI_WSYH_ECUSR")
OCRM_F_CI_LINKMAN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_LINKMAN_BK/'+V_DT_LD+'.parquet/*')
OCRM_F_CI_LINKMAN.registerTempTable("OCRM_F_CI_LINKMAN")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(ID    AS  BIGINT)                  AS ID 
       ,REL_TYPE                AS REL_TYPE 
       ,CUST_ID                 AS CUST_ID 
       ,REL_CUST_ID             AS REL_CUST_ID 
       ,REL_CUST_NAME           AS REL_CUST_NAME 
       ,SEX                     AS SEX 
       ,CERT_ID                 AS CERT_ID 
       ,CERT_TYPE               AS CERT_TYPE 
       ,BIRTHDAY                AS BIRTHDAY 
       ,EDU                     AS EDU 
       ,IND_AGE                 AS IND_AGE 
       ,STATION                 AS STATION 
       ,ADM_DEGREE              AS ADM_DEGREE 
       ,POST                    AS POST 
       ,POST_AGE                AS POST_AGE 
       ,HOLD_STOCK              AS HOLD_STOCK 
       ,UNIT_TEL                AS UNIT_TEL 
       ,UNIT_ZIP                AS UNIT_ZIP 
       ,UNIT_ADD                AS UNIT_ADD 
       ,QQ_CODE                 AS QQ_CODE 
       ,MSN_CODE                AS MSN_CODE 
       ,ZIP_CODE                AS ZIP_CODE 
       ,SS_CODE                 AS SS_CODE 
       ,ADD                     AS ADD 
       ,ADD_ZIP                 AS ADD_ZIP 
       ,REMARK                  AS REMARK 
       ,ODS_DATE                AS ODS_DATE 
       ,FR_ID                   AS FR_ID 
       ,ORG_ID                  AS ORG_ID 
       ,SYS_ID                  AS SYS_ID 
   FROM OCRM_F_CI_LINKMAN A                                    --干系人信息
  WHERE ODS_DATE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_LINKMAN_TMP = sqlContext.sql(sql)
F_CI_LINKMAN_TMP.registerTempTable("F_CI_LINKMAN_TMP")
dfn="F_CI_LINKMAN_TMP/"+V_DT+".parquet"
F_CI_LINKMAN_TMP.cache()
nrows = F_CI_LINKMAN_TMP.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_LINKMAN_TMP/*.parquet")
F_CI_LINKMAN_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_LINKMAN_TMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_LINKMAN_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()   AS ID 
       ,''                    AS REL_TYPE 
       ,C.EC_CUST_ID_A          AS CUST_ID 
       ,C.EC_CUST_ID_B          AS REL_CUST_ID 
       ,A.ODS_CUST_NAME         AS REL_CUST_NAME 
       ,''                    AS SEX 
       ,''                    AS CERT_ID 
       ,''                    AS CERT_TYPE 
       ,''                    AS BIRTHDAY 
       ,''                    AS EDU 
       ,''                    AS IND_AGE 
       ,''                    AS STATION 
       ,''                    AS ADM_DEGREE 
       ,''                    AS POST 
       ,''                    AS POST_AGE 
       ,''                    AS HOLD_STOCK 
       ,''                    AS UNIT_TEL 
       ,''                    AS UNIT_ZIP 
       ,''                    AS UNIT_ADD 
       ,''                    AS QQ_CODE 
       ,''                    AS MSN_CODE 
       ,''                    AS ZIP_CODE 
       ,''                    AS SS_CODE 
       ,''                    AS ADD 
       ,''                    AS ADD_ZIP 
       ,''                    AS REMARK 
       ,A.ODS_ST_DATE           AS ODS_DATE 
       ,C.EC_BNK_NO             AS FR_ID 
       ,C.EC_CRT_ORG            AS ORG_ID 
       ,'CEN'                   AS SYS_ID 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
  INNER JOIN F_CI_CBOD_ECCIFCCR C                              --客户关系信息档
     ON A.SOURCE_CUST_ID        = C.EC_CUST_ID_B 
    AND A.FR_ID                 = C.FR_ID 
    AND C.EC_REL_TYP            = '02' 
    AND A.ODS_SYS_ID            = 'CEN'
  WHERE A.ODS_CUST_TYPE         = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_LINKMAN = sqlContext.sql(sql)
dfn="OCRM_F_CI_LINKMAN/"+V_DT+".parquet"
OCRM_F_CI_LINKMAN.cache()
nrows = OCRM_F_CI_LINKMAN.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_LINKMAN/*.parquet")
OCRM_F_CI_LINKMAN.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_LINKMAN.unpersist()
F_CI_CBOD_ECCIFCCR.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_LINKMAN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()   AS ID 
       ,''                    AS REL_TYPE 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,A.RELATIVEID            AS REL_CUST_ID 
       ,A.CUSTOMERNAME          AS REL_CUST_NAME 
       ,''                    AS SEX 
       ,''                    AS CERT_ID 
       ,''                    AS CERT_TYPE 
       ,''                    AS BIRTHDAY 
       ,''                    AS EDU 
       ,''                    AS IND_AGE 
       ,A.RELATIONSHIP          AS STATION 
       ,''                    AS ADM_DEGREE 
       ,''                    AS POST 
       ,''                    AS POST_AGE 
       ,''                    AS HOLD_STOCK 
       ,A.TELEPHONE             AS UNIT_TEL 
       ,A.FAMILYZIP             AS UNIT_ZIP 
       ,A.FAMILYADD             AS UNIT_ADD 
       ,''                    AS QQ_CODE 
       ,''                    AS MSN_CODE 
       ,''                    AS ZIP_CODE 
       ,''                    AS SS_CODE 
       ,''                    AS ADD 
       ,''                    AS ADD_ZIP 
       ,''                    AS REMARK 
       ,A.ODS_ST_DATE           AS ODS_DATE 
       ,A.FR_ID                 AS FR_ID 
       ,A.INPUTORGID            AS ORG_ID 
       ,'LNA'                   AS SYS_ID 
   FROM F_CI_XDXT_CUSTOMER_RELATIVE A                          --客户关联信息
  INNER JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.CUSTOMERID            = B.SOURCE_CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ODS_SYS_ID            = 'LNA' 
    AND B.ODS_CUST_TYPE         = '2' 
  WHERE LENGTH(A.CERTID) < 15 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_LINKMAN = sqlContext.sql(sql)
dfn="OCRM_F_CI_LINKMAN/"+V_DT+".parquet"
OCRM_F_CI_LINKMAN.cache()
nrows = OCRM_F_CI_LINKMAN.count()
OCRM_F_CI_LINKMAN.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_LINKMAN.unpersist()
F_CI_XDXT_CUSTOMER_RELATIVE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_LINKMAN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()   AS ID 
       ,''                    AS REL_TYPE 
       ,E.ODS_CUST_ID           AS CUST_ID 
       ,''                    AS REL_CUST_ID 
       ,D.USERNAME              AS REL_CUST_NAME 
       ,''                    AS SEX 
       ,''                    AS CERT_ID 
       ,''                    AS CERT_TYPE 
       ,''                    AS BIRTHDAY 
       ,''                    AS EDU 
       ,''                    AS IND_AGE 
       ,''                    AS STATION 
       ,''                    AS ADM_DEGREE 
       ,''                    AS POST 
       ,''                    AS POST_AGE 
       ,''                    AS HOLD_STOCK 
       ,D.PHONE                 AS UNIT_TEL 
       ,''                    AS UNIT_ZIP 
       ,''                    AS UNIT_ADD 
       ,''                    AS QQ_CODE 
       ,''                    AS MSN_CODE 
       ,''                    AS ZIP_CODE 
       ,''                    AS SS_CODE 
       ,''                    AS ADD 
       ,''                    AS ADD_ZIP 
       ,''                    AS REMARK 
       ,D.ODS_ST_DATE           AS ODS_DATE 
       ,D.FR_ID                 AS FR_ID 
       ,''                    AS ORG_ID 
       ,'IBK'                   AS SYS_ID 
   FROM F_TX_WSYH_ECCIF A                                      --电子银行参与方信息表
  INNER JOIN F_CI_WSYH_ECCIFID B                               --客户证件表
     ON A.CIFSEQ                = B.CIFSEQ 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN F_TX_WSYH_ECCIFMCH C                              --客户渠道表
     ON A.CIFSEQ                = C.CIFSEQ 
    AND C.MCHANNELID            = 'EIBS' 
  INNER JOIN F_CI_WSYH_ECUSR D                                 --电子银行用户表
     ON A.CIFSEQ                = D.CIFSEQ 
  INNER JOIN OCRM_F_CI_SYS_RESOURCE E                          --系统来源中间表
     ON A.CIFNAME               = E.SOURCE_CUST_NAME 
    AND B.IDTYPE                = E.CERT_TYPE 
    AND B.IDNO                  = E.CERT_NO 
    AND A.FR_ID                 = E.FR_ID 
    AND E.ODS_SYS_ID            = 'IBK' 
    AND E.ODS_CUST_TYPE         = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_LINKMAN = sqlContext.sql(sql)
dfn="OCRM_F_CI_LINKMAN/"+V_DT+".parquet"
OCRM_F_CI_LINKMAN.cache()
nrows = OCRM_F_CI_LINKMAN.count()
OCRM_F_CI_LINKMAN.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_LINKMAN.unpersist()
F_TX_WSYH_ECCIF.unpersist()
F_CI_WSYH_ECCIFID.unpersist()
F_TX_WSYH_ECCIFMCH.unpersist()
F_CI_WSYH_ECUSR.unpersist()
OCRM_F_CI_SYS_RESOURCE.unpersist()

et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_LINKMAN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()   AS ID 
       ,''                    AS REL_TYPE 
       ,C.CUST_NO               AS CUST_ID 
       ,''                    AS REL_CUST_ID 
       ,C.CUST_NAME             AS REL_CUST_NAME 
       ,''                    AS SEX 
       ,''                    AS CERT_ID 
       ,''                    AS CERT_TYPE 
       ,''                    AS BIRTHDAY 
       ,''                    AS EDU 
       ,''                    AS IND_AGE 
       ,''                    AS STATION 
       ,''                    AS ADM_DEGREE 
       ,''                    AS POST 
       ,''                    AS POST_AGE 
       ,''                    AS HOLD_STOCK 
       ,A.ADDRESS               AS UNIT_TEL 
       ,''                    AS UNIT_ZIP 
       ,''                    AS UNIT_ADD 
       ,''                    AS QQ_CODE 
       ,''                    AS MSN_CODE 
       ,''                    AS ZIP_CODE 
       ,''                    AS SS_CODE 
       ,''                    AS ADD 
       ,''                    AS ADD_ZIP 
       ,''                    AS REMARK 
       ,A.ODS_ST_DATE           AS ODS_DATE 
       ,C.FR_ID                 AS FR_ID 
       ,C.ORG_ID                AS ORG_ID 
       ,'MSG'                   AS SYS_ID 
   FROM F_CI_DXPT_ADDRESS A                                    --手机信息表
  INNER JOIN F_CI_DXPT_CUSTOMER C                              --客户信息表
     ON A.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
    AND C.CUST_TYPE             = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_LINKMAN = sqlContext.sql(sql)
dfn="OCRM_F_CI_LINKMAN/"+V_DT+".parquet"
OCRM_F_CI_LINKMAN.cache()
nrows = OCRM_F_CI_LINKMAN.count()
OCRM_F_CI_LINKMAN.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_LINKMAN.unpersist()
F_CI_DXPT_ADDRESS.unpersist()
F_CI_DXPT_CUSTOMER.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_LINKMAN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-06::
V_STEP = V_STEP + 1
F_CI_LINKMAN_TMP = sqlContext.read.parquet(hdfs+'/F_CI_LINKMAN_TMP/*')
F_CI_LINKMAN_TMP.registerTempTable("F_CI_LINKMAN_TMP")
sql = """
 SELECT CAST(ID    AS  BIGINT)                      AS ID 
       ,REL_TYPE                AS REL_TYPE 
       ,CUST_ID                 AS CUST_ID 
       ,REL_CUST_ID             AS REL_CUST_ID 
       ,REL_CUST_NAME           AS REL_CUST_NAME 
       ,SEX                     AS SEX 
       ,CERT_ID                 AS CERT_ID 
       ,CERT_TYPE               AS CERT_TYPE 
       ,BIRTHDAY                AS BIRTHDAY 
       ,EDU                     AS EDU 
       ,IND_AGE                 AS IND_AGE 
       ,STATION                 AS STATION 
       ,ADM_DEGREE              AS ADM_DEGREE 
       ,POST                    AS POST 
       ,POST_AGE                AS POST_AGE 
       ,HOLD_STOCK              AS HOLD_STOCK 
       ,UNIT_TEL                AS UNIT_TEL 
       ,UNIT_ZIP                AS UNIT_ZIP 
       ,UNIT_ADD                AS UNIT_ADD 
       ,QQ_CODE                 AS QQ_CODE 
       ,MSN_CODE                AS MSN_CODE 
       ,ZIP_CODE                AS ZIP_CODE 
       ,SS_CODE                 AS SS_CODE 
       ,ADD                     AS ADD 
       ,ADD_ZIP                 AS ADD_ZIP 
       ,REMARK                  AS REMARK 
       ,ODS_DATE                AS ODS_DATE 
       ,FR_ID                   AS FR_ID 
       ,ORG_ID                  AS ORG_ID 
       ,SYS_ID                  AS SYS_ID 
   FROM F_CI_LINKMAN_TMP A                                     --干系人信息临时表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_LINKMAN = sqlContext.sql(sql)
dfn="OCRM_F_CI_LINKMAN/"+V_DT+".parquet"
OCRM_F_CI_LINKMAN.cache()
nrows = OCRM_F_CI_LINKMAN.count()
OCRM_F_CI_LINKMAN.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_LINKMAN.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_LINKMAN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-07::
V_STEP = V_STEP + 1
OCRM_F_CI_LINKMAN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_LINKMAN/*')
OCRM_F_CI_LINKMAN.registerTempTable("OCRM_F_CI_LINKMAN")
sql = """
 SELECT CAST(ID    AS  BIGINT)                      AS ID 
       ,'1'                     AS REL_TYPE 
       ,CUST_ID                 AS CUST_ID 
       ,REL_CUST_ID             AS REL_CUST_ID 
       ,REL_CUST_NAME           AS REL_CUST_NAME 
       ,SEX                     AS SEX 
       ,CERT_ID                 AS CERT_ID 
       ,CERT_TYPE               AS CERT_TYPE 
       ,BIRTHDAY                AS BIRTHDAY 
       ,EDU                     AS EDU 
       ,IND_AGE                 AS IND_AGE 
       ,STATION                 AS STATION 
       ,ADM_DEGREE              AS ADM_DEGREE 
       ,POST                    AS POST 
       ,POST_AGE                AS POST_AGE 
       ,HOLD_STOCK              AS HOLD_STOCK 
       ,UNIT_TEL                AS UNIT_TEL 
       ,UNIT_ZIP                AS UNIT_ZIP 
       ,UNIT_ADD                AS UNIT_ADD 
       ,QQ_CODE                 AS QQ_CODE 
       ,MSN_CODE                AS MSN_CODE 
       ,ZIP_CODE                AS ZIP_CODE 
       ,SS_CODE                 AS SS_CODE 
       ,ADD                     AS ADD 
       ,ADD_ZIP                 AS ADD_ZIP 
       ,REMARK                  AS REMARK 
       ,ODS_DATE                AS ODS_DATE 
       ,FR_ID                   AS FR_ID 
       ,ORG_ID                  AS ORG_ID 
       ,SYS_ID                  AS SYS_ID 
   FROM OCRM_F_CI_LINKMAN A                                    --干系人信息临时表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_LINKMAN = sqlContext.sql(sql)
OCRM_F_CI_LINKMAN.registerTempTable("OCRM_F_CI_LINKMAN")
dfn="OCRM_F_CI_LINKMAN/"+V_DT+".parquet"
OCRM_F_CI_LINKMAN.cache()
nrows = OCRM_F_CI_LINKMAN.count()
OCRM_F_CI_LINKMAN.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_LINKMAN.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_LINKMAN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_LINKMAN_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_LINKMAN/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_LINKMAN_BK/")


