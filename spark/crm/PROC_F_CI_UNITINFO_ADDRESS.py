#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_UNITINFO_ADDRESS').setMaster(sys.argv[2])
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

F_CI_CBOD_CICIEADR = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIEADR/*')
F_CI_CBOD_CICIEADR.registerTempTable("F_CI_CBOD_CICIEADR")
F_CI_XDXT_ENT_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_ENT_INFO/*')
F_CI_XDXT_ENT_INFO.registerTempTable("F_CI_XDXT_ENT_INFO")
F_CI_WSYH_ECCIFADDR = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFADDR/*')
F_CI_WSYH_ECCIFADDR.registerTempTable("F_CI_WSYH_ECCIFADDR")
F_CI_WSYH_ECCIFID = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFID/*')
F_CI_WSYH_ECCIFID.registerTempTable("F_CI_WSYH_ECCIFID")
F_TX_WSYH_DEPT = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_DEPT/*')
F_TX_WSYH_DEPT.registerTempTable("F_TX_WSYH_DEPT")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_CI_WSYH_ECUSR = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECUSR/*')
F_CI_WSYH_ECUSR.registerTempTable("F_CI_WSYH_ECUSR")
F_TX_WSYH_ECCIFMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIFMCH/*')
F_TX_WSYH_ECCIFMCH.registerTempTable("F_TX_WSYH_ECCIFMCH")
F_TX_WSYH_ECCIF = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIF/*')
F_TX_WSYH_ECCIF.registerTempTable("F_TX_WSYH_ECCIF")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()  AS ID 
       ,A.FK_CICIF_KEY          AS CUST_ID 
       ,A.CI_ADDR               AS REGISTER_ADDRESS 
       ,A.CI_POSTCOD            AS REGISTER_ZIP 
       ,A.CI_CNTY_COD           AS COUNTRY 
       ,''                    AS PROVINCE 
       ,''                    AS WORK_ADDRESS 
       ,A.CI_EMAIL              AS E_MAIL 
       ,A.CI_WEBSITE            AS WEB_ADDRESS 
       ,''                    AS CREATE_USER 
       ,''                    AS CREATE_USER_NAME 
       ,''                    AS CREATE_ORG 
       ,''                    AS CREATE_ORG_NAME 
       ,''                    AS UPDATE_TIME 
       ,NVL(A.FR_ID, 'UNK')                       AS FR_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,'CEN'                   AS SYS_ID 
   FROM F_CI_CBOD_CICIEADR A                                   --对公客户地址信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_ADDR = sqlContext.sql(sql)
OCRM_F_CI_COM_ADDR.registerTempTable("OCRM_F_CI_COM_ADDR")
dfn="OCRM_F_CI_COM_ADDR/"+V_DT+".parquet"
OCRM_F_CI_COM_ADDR.cache()
nrows = OCRM_F_CI_COM_ADDR.count()
OCRM_F_CI_COM_ADDR.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_ADDR.unpersist()
F_CI_CBOD_CICIEADR.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_ADDR/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_ADDR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()  AS ID 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,A.REGISTERADD           AS REGISTER_ADDRESS 
       ,A.OFFICEZIP             AS REGISTER_ZIP 
       ,A.COUNTRYCODE           AS COUNTRY 
       ,A.REGIONCODE            AS PROVINCE 
       ,A.OFFICEADD             AS WORK_ADDRESS 
       ,A.EMAILADD              AS E_MAIL 
       ,A.WEBADD                AS WEB_ADDRESS 
       ,''                    AS CREATE_USER 
       ,''                    AS CREATE_USER_NAME 
       ,''                    AS CREATE_ORG 
       ,''                    AS CREATE_ORG_NAME 
       ,''                    AS UPDATE_TIME 
       ,NVL(A.FR_ID, 'UNK')                       AS FR_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,'LNA'                   AS SYS_ID 
   FROM F_CI_XDXT_ENT_INFO A                                   --企业基本信息
  INNER JOIN (SELECT DISTINCT ODS_CUST_ID,SOURCE_CUST_ID ,FR_ID FROM OCRM_F_CI_SYS_RESOURCE WHERE ODS_SYS_ID='LNA'
) B                          --系统来源中间表
     ON A.CUSTOMERID            = B.SOURCE_CUST_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_ADDR = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_ADDR/"+V_DT+".parquet"
OCRM_F_CI_COM_ADDR.cache()
nrows = OCRM_F_CI_COM_ADDR.count()
OCRM_F_CI_COM_ADDR.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_COM_ADDR.unpersist()
F_CI_XDXT_ENT_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_ADDR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()  AS ID 
       ,F.ODS_CUST_ID           AS CUST_ID 
       ,E.ADDR                  AS REGISTER_ADDRESS 
       ,G.ZIPCODE               AS REGISTER_ZIP 
       ,''                    AS COUNTRY 
       ,''                    AS PROVINCE 
       ,''                    AS WORK_ADDRESS 
       ,D.EMAIL                 AS E_MAIL 
       ,''                    AS WEB_ADDRESS 
       ,''                    AS CREATE_USER 
       ,''                    AS CREATE_USER_NAME 
       ,''                    AS CREATE_ORG 
       ,''                    AS CREATE_ORG_NAME 
       ,''                    AS UPDATE_TIME 
       ,D.FR_ID                 AS FR_ID 
       ,F.ODS_ST_DATE           AS ODS_ST_DATE 
       ,'IBK'                   AS SYS_ID 
   FROM F_TX_WSYH_ECCIF A                                      --电子银行参与方信息表
  INNER JOIN F_CI_WSYH_ECCIFID B                               --客户证件表
     ON A.CIFSEQ                = B.CIFSEQ 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN F_TX_WSYH_ECCIFMCH C                              --客户渠道表
     ON A.CIFSEQ                = C.CIFSEQ 
    AND A.FR_ID                 = C.FR_ID 
    AND C.MCHANNELID            = 'EIBS' 
  INNER JOIN F_CI_WSYH_ECUSR D                                 --电子银行用户表
     ON A.CIFSEQ                = D.CIFSEQ 
    AND A.FR_ID                 = D.FR_ID 
  INNER JOIN F_CI_WSYH_ECCIFADDR E                             --客户地址表
     ON A.CIFSEQ                = E.CIFSEQ 
    AND A.FR_ID                 = E.FR_ID 
  INNER JOIN OCRM_F_CI_SYS_RESOURCE F                                  --网点表
     ON A.CIFNAME               = F.SOURCE_CUST_NAME 
    AND A.FR_ID                 = F.FR_ID 
    AND B.IDTYPE                = F.CERT_TYPE 
    AND B.IDNO                  = F.CERT_NO 
    AND F.ODS_CUST_TYPE         = '2' 
    AND F.ODS_SYS_ID            = 'IBK' 
  INNER JOIN F_TX_WSYH_DEPT G                          --系统来源中间表
     ON A.CIFDEPTSEQ            = G.DEPTSEQ 
    AND A.FR_ID                 = G.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_ADDR = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_ADDR/"+V_DT+".parquet"
OCRM_F_CI_COM_ADDR.cache()
nrows = OCRM_F_CI_COM_ADDR.count()
OCRM_F_CI_COM_ADDR.write.save(path=hdfs + '/' + dfn, mode='append')
F_TX_WSYH_ECCIF.unpersist()
F_CI_WSYH_ECCIFID.unpersist()
F_CI_WSYH_ECUSR.unpersist()
F_CI_WSYH_ECCIFADDR.unpersist()
OCRM_F_CI_SYS_RESOURCE.unpersist()
F_TX_WSYH_DEPT.unpersist()
OCRM_F_CI_COM_ADDR.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_ADDR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
