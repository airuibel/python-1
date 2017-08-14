#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_BLACKLIST').setMaster(sys.argv[2])
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
#ACRM_F_CI_BLACKLIST  增量 删除当天文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_BLACKLIST/"+V_DT+".parquet")

F_CI_CBOD_ECCSIATT = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCSIATT/*')
F_CI_CBOD_ECCSIATT.registerTempTable("F_CI_CBOD_ECCSIATT")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()  AS ID 
       ,A.EC_CUST_NO            AS CUST_ID 
       ,''                    AS CERT_TYP 
       ,''                    AS CERT_NO 
       ,B.CUST_ZH_NAME          AS CUST_NAME 
       ,A.EC_CUST_TYP           AS TYPE 
       ,''                    AS REGISTRANT 
       ,''                    AS TELEPHONE 
       ,''                    AS ORIGIN 
       ,''                    AS RISK_TYPE 
       ,''                    AS RISK_LVL 
       ,''                    AS TEN_LVL 
       ,CAST('' AS  DECIMAL(24,6))                  AS BAL 
       ,A.EC_ATT_DESC           AS CAUSE 
       ,CONCAT(SUBSTR(A.EC_ATT_FMDAT_N, 1, 4),'-',SUBSTR(A.EC_ATT_FMDAT_N, 5, 2),'-',SUBSTR(A.EC_ATT_FMDAT_N, 7, 2)) AS RECORD_DATE 
       ,''                    AS ADDRESS 
       ,''                    AS MANAGE_BRAN 
       ,A.EC_CRT_OPR            AS DUTY_OFFICER 
       ,''                    AS STATE 
       ,A.EC_CRT_ORG            AS AGENCY_BRAN 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                    AS ODS_LOAD_DT 
       ,''                    AS OTHER_BANK_CAUSE 
	   ,'' AS ATT_TYPE
	   ,A.FR_ID AS FR_ID
   FROM F_CI_CBOD_ECCSIATT A                                   --关注客户清单档
  INNER JOIN OCRM_F_CI_CUST_DESC B                             --统一客户信息
     ON A.EC_CUST_NO            = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_BLACKLIST = sqlContext.sql(sql)
ACRM_F_CI_BLACKLIST.registerTempTable("ACRM_F_CI_BLACKLIST")
dfn="ACRM_F_CI_BLACKLIST/"+V_DT+".parquet"
ACRM_F_CI_BLACKLIST.cache()
nrows = ACRM_F_CI_BLACKLIST.count()
ACRM_F_CI_BLACKLIST.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_BLACKLIST.unpersist()
F_CI_CBOD_ECCSIATT.unpersist()
OCRM_F_CI_CUST_DESC.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_BLACKLIST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
