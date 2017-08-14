#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_UPDOWNSTREAM').setMaster(sys.argv[2])
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

#清除数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_UPDOWNSTREAM/"+V_DT+".parquet")

F_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNACN/*')
F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")
FW_SYS_PROP_FR = sqlContext.read.parquet(hdfs+'/FW_SYS_PROP_FR/*')
FW_SYS_PROP_FR.registerTempTable("FW_SYS_PROP_FR")
FW_SYS_PROP = sqlContext.read.parquet(hdfs+'/FW_SYS_PROP/*')
FW_SYS_PROP.registerTempTable("FW_SYS_PROP")
ACRM_F_CI_NIN_TRANSLOG = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_NIN_TRANSLOG/*')
ACRM_F_CI_NIN_TRANSLOG.registerTempTable("ACRM_F_CI_NIN_TRANSLOG")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
OCRM_F_FID = sqlContext.read.parquet(hdfs+'/OCRM_F_FID/*')
OCRM_F_FID.registerTempTable("OCRM_F_FID")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.SA_TVARCHAR_DT        AS TRAN_DATE 
       ,A.SA_TVARCHAR_AMT       AS TRAN_AMT 
       ,CASE WHEN A.SA_CR_AMT > 0 THEN '2' WHEN A.SA_DR_AMT > 0 THEN '1' END AS LOANER 
       ,B.SA_CUST_NO            AS OP_CUST_ID 
       ,A.SA_OP_CUST_NAME       AS OP_ACCT_NAME 
       ,A.SA_OP_ACCT_NO_32      AS OP_ACCT_NO 
       ,A.SA_RMRK               AS REMARKS 
       ,A.SA_OP_BANK_NO         AS OP_BANK_NO 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_NIN_TRANSLOG A                               --交易流水表
  INNER JOIN OCRM_F_CI_CUST_DESC C                             --统一客户信息表
     ON A.FR_ID                 = C.FR_ID 
    AND A.CUST_ID               = C.CUST_ID 
    AND C.CUST_TYP              = '2' 
  INNER JOIN (SELECT A.FR_ID,COALESCE(B.PROP_VALUE,C.PROP_VALUE) AS PROP_VALUE
                FROM OCRM_F_FID A
                LEFT JOIN FW_SYS_PROP_FR B ON B.PROP_NAME = 'com_cust_updown' AND A.FR_ID = B.FR_ID
                LEFT JOIN FW_SYS_PROP C ON C.PROP_NAME = COALESCE(B.PROP_NAME,'com_cust_updown')) D ON C.OBJ_RATING <= D.PROP_VALUE AND D.FR_ID=A.FR_ID 
   LEFT JOIN F_DP_CBOD_SAACNACN B                              --活存主档
     ON A.SA_OP_ACCT_NO_32      = B.SA_ACCT_NO 
    AND B.FR_ID                 = A.FR_ID 
  WHERE A.SA_EC_FLG             = '0' 
    AND A.CUST_TYP              = '2' 
    AND A.SA_TVARCHAR_DT        = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT+"'", sql)
ACRM_F_CI_UPDOWNSTREAM = sqlContext.sql(sql)
ACRM_F_CI_UPDOWNSTREAM.registerTempTable("ACRM_F_CI_UPDOWNSTREAM")
dfn="ACRM_F_CI_UPDOWNSTREAM/"+V_DT+".parquet"
ACRM_F_CI_UPDOWNSTREAM.cache()
nrows = ACRM_F_CI_UPDOWNSTREAM.count()
ACRM_F_CI_UPDOWNSTREAM.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_UPDOWNSTREAM.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_UPDOWNSTREAM lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
