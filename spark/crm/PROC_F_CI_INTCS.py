#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_INTCS').setMaster(sys.argv[2])
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
F_CM_GJJS_PTC = sqlContext.read.parquet(hdfs+'/F_CM_GJJS_PTC/*')
F_CM_GJJS_PTC.registerTempTable("F_CM_GJJS_PTC")
F_CI_GJJS_PTY = sqlContext.read.parquet(hdfs+'/F_CI_GJJS_PTY/*')
F_CI_GJJS_PTY.registerTempTable("F_CI_GJJS_PTY")
F_CI_GJJS_ADR = sqlContext.read.parquet(hdfs+'/F_CI_GJJS_ADR/*')
F_CI_GJJS_ADR.registerTempTable("F_CI_GJJS_ADR")

#目标表：
#OCRM_F_CI_LINKMAN 全量表
#OCRM_F_CI_COM_ADDR 全量表
#ACRM_F_CI_CERT_INFO 增改表 多个PY 非第一个 ,拿BK目录今天日期数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CERT_INFO/*")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_CI_CERT_INFO_BK/"+V_DT+".parquet /"+dbname+"/ACRM_F_CI_CERT_INFO/"+V_DT+".parquet")
ACRM_F_CI_CERT_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CERT_INFO/*')
ACRM_F_CI_CERT_INFO.registerTempTable("ACRM_F_CI_CERT_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(MONOTONICALLY_INCREASING_ID()                       AS BIGINT)                       AS ID 
       ,''                      AS REL_TYPE 
       ,B.CLIENTID              AS CUST_ID 
       ,''                      AS REL_CUST_ID 
       ,''                      AS REL_CUST_NAME 
       ,''                      AS SEX 
       ,''                      AS CERT_ID 
       ,''                      AS CERT_TYPE 
       ,''                      AS BIRTHDAY 
       ,''                      AS EDU 
       ,''                      AS IND_AGE 
       ,''                      AS STATION 
       ,''                      AS ADM_DEGREE 
       ,''                      AS POST 
       ,''                      AS POST_AGE 
       ,''                      AS HOLD_STOCK 
       ,A.TELOFF                AS UNIT_TEL 
       ,''                      AS UNIT_ZIP 
       ,''                      AS UNIT_ADD 
       ,''                      AS QQ_CODE 
       ,''                      AS MSN_CODE 
       ,''                      AS ZIP_CODE 
       ,''                      AS SS_CODE 
       ,''                      AS ADD 
       ,''                      AS ADD_ZIP 
       ,''                      AS REMARK 
       ,A.ODS_ST_DATE           AS ODS_DATE 
       ,B.FR_ID                 AS FR_ID 
       ,B.BCHKEY                AS ORG_ID 
       ,''                      AS SYS_ID 
   FROM F_CM_GJJS_PTC A                                        --利息中间表
  INNER JOIN F_CI_GJJS_PTY B                                   --客户资料表
     ON A.PTYINR                = B.INR 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_LINKMAN = sqlContext.sql(sql)
OCRM_F_CI_LINKMAN.registerTempTable("OCRM_F_CI_LINKMAN")
dfn="OCRM_F_CI_LINKMAN/"+V_DT+".parquet"
OCRM_F_CI_LINKMAN.cache()
nrows = OCRM_F_CI_LINKMAN.count()
OCRM_F_CI_LINKMAN.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_LINKMAN.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_LINKMAN/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_LINKMAN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(MONOTONICALLY_INCREASING_ID()                       AS BIGINT)                       AS ID 
       ,B.CLIENTID              AS CUST_ID 
       ,A.NAM                   AS REGISTER_ADDRESS 
       ,A.POBZIP                AS REGISTER_ZIP 
       ,A.LOCCTY                AS COUNTRY 
       ,A.LOCTXT                AS PROVINCE 
       ,A.ADR1                  AS WORK_ADDRESS 
       ,A.EML                   AS E_MAIL 
       ,''                      AS WEB_ADDRESS 
       ,''                      AS CREATE_USER 
       ,''                      AS CREATE_USER_NAME 
       ,''                      AS CREATE_ORG 
       ,''                      AS CREATE_ORG_NAME 
       ,''                      AS UPDATE_TIME 
       ,A.FR_ID                 AS FR_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                      AS SYS_ID 
   FROM F_CI_GJJS_ADR A                                        --地址信息表
  INNER JOIN F_CI_GJJS_PTY B                                   --客户资料表
     ON A.EXTKEY                = B.EXTKEY 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_ADDR = sqlContext.sql(sql)
OCRM_F_CI_COM_ADDR.registerTempTable("OCRM_F_CI_COM_ADDR")
dfn="OCRM_F_CI_COM_ADDR/"+V_DT+".parquet"
OCRM_F_CI_COM_ADDR.cache()
nrows = OCRM_F_CI_COM_ADDR.count()
OCRM_F_CI_COM_ADDR.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_ADDR.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_ADDR/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_ADDR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(MONOTONICALLY_INCREASING_ID()                       AS BIGINT)                       AS ID 
       ,A.CLIENTID              AS CUST_ID 
       ,''                      AS TACK_INSTN 
       ,A.CRIT                  AS CERT_TYPE 
       ,A.CRID                  AS CERT_NO 
       ,''                      AS ISSUE_DATE 
       ,A.NAM1                  AS CARD_ON_NAME 
       ,''                      AS LOST_DATE 
       ,''                      AS AS_ANN_ID 
       ,A.ODS_ST_DATE             AS ODS_ST_DATE 
       ,''                      AS CREATE_ORG_NAME 
       ,''                      AS CREATE_USER 
       ,''                      AS CREATE_USER_NAME 
       ,''                      AS UPDATE_TIME 
       ,''                      AS CREATE_ORG 
       ,A.FR_ID                   AS FR_ID 
       ,A.BRANCHINR               AS ORG_ID 
       ,'INT'                   AS SYS_ID 
   FROM F_CI_GJJS_PTY A                                        --客户资料表
   LEFT JOIN ACRM_F_CI_CERT_INFO C                             --证件表
     ON A.CRIT                  = C.CERT_TYPE 
    AND A.CRID                  = C.CERT_NO 
    AND A.NAM1                  = C.CARD_ON_NAME 
    AND C.FR_ID                 = A.FR_ID 
  WHERE C.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CERT_INFO = sqlContext.sql(sql)
ACRM_F_CI_CERT_INFO.registerTempTable("ACRM_F_CI_CERT_INFO")
dfn="ACRM_F_CI_CERT_INFO/"+V_DT+".parquet"
ACRM_F_CI_CERT_INFO.cache()
nrows = ACRM_F_CI_CERT_INFO.count()
ACRM_F_CI_CERT_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CERT_INFO.unpersist()
#增改表保存后，复制当天数据进BK：先删除BK当天日期，然后复制
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CERT_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
