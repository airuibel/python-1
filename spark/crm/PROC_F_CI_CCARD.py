#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CCARD').setMaster(sys.argv[2])
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

F_CI_INTRO = sqlContext.read.parquet(hdfs+'/F_CI_INTRO/*')
F_CI_INTRO.registerTempTable("F_CI_INTRO")
F_CR_ACCT = sqlContext.read.parquet(hdfs+'/F_CR_ACCT/*')
F_CR_ACCT.registerTempTable("F_CR_ACCT")
F_CR_CARD = sqlContext.read.parquet(hdfs+'/F_CR_CARD/*')
F_CR_CARD.registerTempTable("F_CR_CARD")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT MAX(BANK)                       AS BANK 
       ,CARD_NBR                AS CARD_NBR 
       ,MAX(INTR_NBR)                       AS INTR_NBR 
       ,MAX(INTR_NAME)                       AS INTR_NAME 
   FROM F_CI_INTRO A                                           --推荐人信息修改
  GROUP BY CARD_NBR """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_F_CI_INTRO = sqlContext.sql(sql)
TMP_F_CI_INTRO.registerTempTable("TMP_F_CI_INTRO")
dfn="TMP_F_CI_INTRO/"+V_DT+".parquet"
TMP_F_CI_INTRO.cache()
nrows = TMP_F_CI_INTRO.count()
TMP_F_CI_INTRO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_F_CI_INTRO.unpersist()
F_CI_INTRO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_F_CI_INTRO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_F_CI_INTRO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
TMP_F_CI_INTRO = sqlContext.read.parquet(hdfs+'/TMP_F_CI_INTRO/*')
TMP_F_CI_INTRO.registerTempTable("TMP_F_CI_INTRO")
sql = """
 SELECT A.CARD_NBR              AS CARD_NBR 
       ,A.CANCL_CODE            AS CANCL_CODE 
       ,A.CANCL_TIME            AS CANCL_TIME 
       ,A.EXPIRY_DTE            AS EXPIRY_DTE 
       ,A.CUSTR_NBR             AS CUSTR_NBR 
       ,CAST(C.BRANCH AS INTEGER)               AS BRANCH 
       ,C.DAY_OPENED            AS DAY_OPENED 
       ,CAST(C.CRED_LIMIT   AS DECIMAL(20,2))         AS CRED_LIMIT 
       ,CAST(C.CREDLIM_X    AS DECIMAL(20,2))         AS CREDLIM_X 
       ,C.CLOSE_CODE            AS CLOSE_CODE 
       ,C.CURR_NUM              AS CURR_NUM 
       ,CAST(C.CYCLE_NBR  AS INTEGER )         AS CYCLE_NBR 
       ,B.INTR_NBR              AS INTR_NBR 
       ,B.INTR_NAME             AS INTR_NAME 
       ,CAST(B.BANK    AS INTEGER)              AS BANK 
   FROM F_CR_CARD A                                            --卡片资料
  INNER JOIN TMP_F_CI_INTRO B                                  --TMP_F_CI_INTRO
     ON A.CARD_NBR              = B.CARD_NBR 
  INNER JOIN F_CR_ACCT C                                       --帐户资料
     ON A.CARD_NBR              = C.CARD_NBR 
  WHERE A.CARD_NBR IS NOT NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CCARD = sqlContext.sql(sql)
OCRM_F_CI_CCARD.registerTempTable("OCRM_F_CI_CCARD")
dfn="OCRM_F_CI_CCARD/"+V_DT+".parquet"
OCRM_F_CI_CCARD.cache()
nrows = OCRM_F_CI_CCARD.count()
OCRM_F_CI_CCARD.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CR_CARD.unpersist()
TMP_F_CI_INTRO.unpersist()
F_CR_ACCT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CCARD/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CCARD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
