#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CUST_WATER_COST').setMaster(sys.argv[2])
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
F_TX_MID_MAINTRANSDTL = sqlContext.read.parquet(hdfs+'/F_TX_MID_MAINTRANSDTL/*')
F_TX_MID_MAINTRANSDTL.registerTempTable("F_TX_MID_MAINTRANSDTL")
F_CM_MID_SYSINFO = sqlContext.read.parquet(hdfs+'/F_CM_MID_SYSINFO/*')
F_CM_MID_SYSINFO.registerTempTable("F_CM_MID_SYSINFO")
F_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNACN/*')
F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")
OCRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_DP_CARD_INFO/*')
OCRM_F_DP_CARD_INFO.registerTempTable("OCRM_F_DP_CARD_INFO")
#目标表
#OCRM_F_CUST_WATER_COST 增量表
#OCRM_F_CUST_GAS_COST 增量表


#任务[1] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id()  AS BIGINT) AS ID
           ,CAST(NVL(B.SA_CUST_NO,C.CR_CUST_NO) AS VARCHAR(32)) AS CUST_ID
           ,CAST(NVL(B.SA_CUST_NAME,CR_CUST_NAME) AS VARCHAR(80)) AS CUST_NAME
           ,CAST(A.IDTYPE AS VARCHAR(4)) AS CERT_TYPE
           ,CAST(A.IDNO AS VARCHAR(32)) AS CERT_NO
           ,V_DT AS PAY_DT
           ,CAST('' AS DECIMAL(24,6)) AS WATER_CONSUMTION
           ,CAST('' AS DECIMAL(24,6)) AS PRICE
           ,CAST(SUM(A.REALAMOUNT) AS DECIMAL(24,6)) AS SUM_PRICE
           ,CAST(A.FR_ID AS VARCHAR(50)) AS FR_ID
      FROM F_TX_MID_MAINTRANSDTL A
      JOIN F_CM_MID_SYSINFO D ON A.SYSID = D.SYSID AND D.SYSNAME LIKE '%水费%'
      LEFT JOIN F_DP_CBOD_SAACNACN B ON A.ACCTNO = B.SA_ACCT_NO AND B.FR_ID = A.FR_ID
      LEFT JOIN OCRM_F_DP_CARD_INFO C ON A.ACCTNO = C.CR_CRD_NO AND C.FR_ID = A.FR_ID
     WHERE A.ODS_ST_DATE = V_DT 
       and status='S'
     GROUP BY B.SA_CUST_NO,C.CR_CUST_NO,B.SA_CUST_NAME,CR_CUST_NAME,A.IDTYPE,A.IDNO ,A.FR_ID
	 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CUST_WATER_COST = sqlContext.sql(sql)
OCRM_F_CUST_WATER_COST.registerTempTable("OCRM_F_CUST_WATER_COST")
dfn="OCRM_F_CUST_WATER_COST/"+V_DT+".parquet"
OCRM_F_CUST_WATER_COST.cache()
nrows = OCRM_F_CUST_WATER_COST.count()
OCRM_F_CUST_WATER_COST.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CUST_WATER_COST.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CUST_WATER_COST/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CUST_WATER_COST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[2] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id() AS BIGINT) AS ID
			,CAST(NVL(B.SA_CUST_NO,C.CR_CUST_NO) AS VARCHAR(32)) AS CUST_ID
           ,CAST(NVL(B.SA_CUST_NAME,CR_CUST_NAME) AS VARCHAR(80)) AS CUST_NAME
           ,CAST(A.IDTYPE AS VARCHAR(4)) AS CERT_TYPE
           ,CAST(A.IDNO AS VARCHAR(32)) AS CERT_NO
           ,V_DT AS PAY_DT
           ,CAST('' AS DECIMAL(24,6)) AS GAS_CONSUMTION
           ,CAST('' AS DECIMAL(24,6)) AS PRICE
           ,CAST(SUM(A.REALAMOUNT) AS DECIMAL(24,6)) AS SUM_PRICE
           ,CAST(A.FR_ID AS VARCHAR(50)) AS FR_ID
     FROM F_TX_MID_MAINTRANSDTL A
      JOIN F_CM_MID_SYSINFO D ON A.SYSID = D.SYSID AND D.SYSNAME LIKE '%气%'
      LEFT JOIN F_DP_CBOD_SAACNACN B ON A.ACCTNO = B.SA_ACCT_NO AND B.FR_ID = A.FR_ID
      LEFT JOIN OCRM_F_DP_CARD_INFO C ON A.ACCTNO = C.CR_CRD_NO AND C.FR_ID = A.FR_ID
     WHERE A.ODS_ST_DATE = V_DT 
       and status='S'
     GROUP BY B.SA_CUST_NO,C.CR_CUST_NO,B.SA_CUST_NAME,CR_CUST_NAME,A.IDTYPE,A.IDNO,A.FR_ID
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CUST_GAS_COST = sqlContext.sql(sql)
OCRM_F_CUST_GAS_COST.registerTempTable("OCRM_F_CUST_GAS_COST")
dfn="OCRM_F_CUST_GAS_COST/"+V_DT+".parquet"
OCRM_F_CUST_GAS_COST.cache()
nrows = OCRM_F_CUST_GAS_COST.count()
OCRM_F_CUST_GAS_COST.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CUST_GAS_COST.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CUST_GAS_COST/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CUST_GAS_COST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
