#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_ACRM_A_INOUTCOME').setMaster(sys.argv[2])
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
ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")
OCRM_F_CM_EXCHANGE_RATE = sqlContext.read.parquet(hdfs+'/OCRM_F_CM_EXCHANGE_RATE/*')
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
ACRM_F_CI_NIN_TRANSLOG = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_NIN_TRANSLOG/*')
ACRM_F_CI_NIN_TRANSLOG.registerTempTable("ACRM_F_CI_NIN_TRANSLOG")
#目标表
#ACRM_A_INOUTCOME 增量表

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()     AS ID 
       ,A.SA_BELONG_INSTN_COD   AS ORG_ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.FK_SAACN_KEY          AS ACC_NO 
       ,CAST(SUM(NVL(A.SA_DR_AMT, 0) * NVL(H.EXCHANGE_RATE, 1) - NVL(A.SA_CR_AMT, 0) * NVL(H.EXCHANGE_RATE, 1)) AS DECIMAL(24,6)) AS BAL 
       ,V_DT                    AS ODS_DATE 
       ,A.FR_ID                 AS FR_ID 
       ,''                    AS CUST_NAME 
   FROM ACRM_F_CI_NIN_TRANSLOG A                               --交易流水表
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE H                         --汇率表
     ON A.SA_CURR_COD           = H.CURRENCY_CD 
    AND H.OBJECT_CURRENCY_CD    = 'CNY' 
    AND H.ODS_ST_DATE           = V_DT 
   LEFT JOIN ACRM_F_RE_LENDSUMAVGINFO C                        --贷款积数表
     ON A.SA_OP_ACCT_NO_32      = C.ACCOUNT 
    AND C.FR_ID                 = A.FR_ID 
  WHERE A.SA_TVARCHAR_DT        = V_DT 
    AND C.FR_ID IS NULL 
  GROUP BY A.SA_BELONG_INSTN_COD 
       ,A.CUST_ID 
       ,A.FK_SAACN_KEY 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_INOUTCOME = sqlContext.sql(sql)
ACRM_A_INOUTCOME.registerTempTable("ACRM_A_INOUTCOME")
dfn="ACRM_A_INOUTCOME/"+V_DT+".parquet"
ACRM_A_INOUTCOME.cache()
nrows = ACRM_A_INOUTCOME.count()
ACRM_A_INOUTCOME.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_INOUTCOME.unpersist()
#增量表
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_INOUTCOME/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_INOUTCOME lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
