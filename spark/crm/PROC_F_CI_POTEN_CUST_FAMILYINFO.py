#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_POTEN_CUST_FAMILYINFO').setMaster(sys.argv[2])
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
#上月末日期
V_DT_LMD = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1) + timedelta(-1)).strftime("%Y%m%d")
#10位日期
V_DT10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8]))).strftime("%Y-%m-%d")
V_STEP = 0
#OCRM_F_CI_POTEN_CUST_FAMILYINFO  增量文件 删除当天文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_POTEN_CUST_FAMILYINFO/"+V_DT+".parquet")

F_CI_XDXT_IND_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_INFO/*')
F_CI_XDXT_IND_INFO.registerTempTable("F_CI_XDXT_IND_INFO")
OCRM_F_CI_POTENTIAL_CUSTOMER = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_POTENTIAL_CUSTOMER/*')
OCRM_F_CI_POTENTIAL_CUSTOMER.registerTempTable("OCRM_F_CI_POTENTIAL_CUSTOMER")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()      AS ID 
       ,C.HOUSEMASTERNAME       AS HZ_NAME 
       ,C.CENSUSBOOKID          AS CUST_FANID 
       ,C.HOUSEMASTERCERTTYPE   AS HZ_CERTYPE 
       ,C.HOUSEMASTERCERTID     AS HZ_CERTID 
       ,CAST(C.FAMILYNUM     AS DECIMAL(18))        AS CUST_FAMNUM 
       ,CAST(C.WEALTHVALUE   AS DECIMAL(18))         AS CUST_WEAVALUE 
       ,CAST(''      AS DECIMAL(18))              AS CUST_HOUSECOUNT 
       ,''                   AS CUST_PRIVATECAR 
       ,C.CONLANDAREA           AS CONLANDAREA 
       ,CAST(C.CONPOOLAREA AS DECIMAL(24) )        AS CONPOOLAREA 
       ,''                   AS CUST_CHILDREN 
       ,CAST(''     AS DECIMAL(18))       AS NUM_OF_CHILD 
       ,CAST(''     AS DECIMAL(18))                AS TOTINCO_OF_CH 
       ,''                   AS CUST_HOUSE 
       ,CAST(''    AS DECIMAL(18))                AS CUST_HOUSEAREA 
       ,A.CUST_NAME             AS CONTACT_CUSTNAME 
       ,A.CERT_NO               AS CONTACT_CERTNO 
       ,A.CERT_TYP              AS CONTACT_CUSTTYPE 
       ,V_DT                  AS CREATE_DATE 
       ,''                   AS UPDATE_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_POTENTIAL_CUSTOMER A                         --潜在客户
   LEFT JOIN F_CI_XDXT_IND_INFO C                              --个人基本信息
     ON A.CUST_NAME             = C.FULLNAME 
    AND A.CERT_TYP              = C.CERTTYPE 
    AND A.CERT_NO               = C.CERTID 
    AND C.CORPORATEORGID        = C.FR_ID 
  WHERE A.POSITIVE_FLAG         = '2' 
    AND A.CUST_TYPE             = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_POTEN_CUST_FAMILYINFO = sqlContext.sql(sql)
OCRM_F_CI_POTEN_CUST_FAMILYINFO.registerTempTable("OCRM_F_CI_POTEN_CUST_FAMILYINFO")
dfn="OCRM_F_CI_POTEN_CUST_FAMILYINFO/"+V_DT+".parquet"
OCRM_F_CI_POTEN_CUST_FAMILYINFO.cache()
nrows = OCRM_F_CI_POTEN_CUST_FAMILYINFO.count()
OCRM_F_CI_POTEN_CUST_FAMILYINFO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_POTEN_CUST_FAMILYINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_POTEN_CUST_FAMILYINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
