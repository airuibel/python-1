#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_DEPOSIT_MONTH_CHART_TEMP').setMaster(sys.argv[2])
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

ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT FR_ID                   AS FR_ID 
       ,ORG_ID                  AS ORG_ID 
       ,SUM(COALESCE(C.MONTH_RMB, 0))                       AS DEP_MONTH_AVG 
       ,SUM(COALESCE(C.MVAL_RMB, 0))                       AS DEP_YEAR_AVG 
   FROM ACRM_F_DP_SAVE_INFO C                                  --负债协议
  WHERE ACCT_STATUS = '01'
  GROUP BY C.FR_ID 
       ,C.ORG_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_DEPOSIT_MONTH_CHART_TEMP = sqlContext.sql(sql)
ACRM_A_DEPOSIT_MONTH_CHART_TEMP.registerTempTable("ACRM_A_DEPOSIT_MONTH_CHART_TEMP")
dfn="ACRM_A_DEPOSIT_MONTH_CHART_TEMP/"+V_DT+".parquet"
ACRM_A_DEPOSIT_MONTH_CHART_TEMP.cache()
nrows = ACRM_A_DEPOSIT_MONTH_CHART_TEMP.count()
ACRM_A_DEPOSIT_MONTH_CHART_TEMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_DEPOSIT_MONTH_CHART_TEMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEPOSIT_MONTH_CHART_TEMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_DEPOSIT_MONTH_CHART_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT FR_ID                   AS FR_ID 
       ,ORG_ID                  AS ORG_ID 
       ,PRODUCT_ID              AS PRODUCT_ID 
       ,SUM(COALESCE(A.MS_AC_BAL, 0))                       AS DEP_BAL 
       ,SUM(BAL_RMB)                       AS BAL_RMB 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议
  WHERE A.ACCT_STATUS           = '01' 
    AND A.DETP <> '98' 
    AND A.DETP <> '1A' 
    AND A.CYNO                  = 'CNY' 
  GROUP BY A.FR_ID 
       ,A.ORG_ID 
       ,A.PRODUCT_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_DEPOSIT_MONTH_CHART_TEMP1 = sqlContext.sql(sql)
ACRM_A_DEPOSIT_MONTH_CHART_TEMP1.registerTempTable("ACRM_A_DEPOSIT_MONTH_CHART_TEMP1")
dfn="ACRM_A_DEPOSIT_MONTH_CHART_TEMP1/"+V_DT+".parquet"
ACRM_A_DEPOSIT_MONTH_CHART_TEMP1.cache()
nrows = ACRM_A_DEPOSIT_MONTH_CHART_TEMP1.count()
ACRM_A_DEPOSIT_MONTH_CHART_TEMP1.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_DEPOSIT_MONTH_CHART_TEMP1.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEPOSIT_MONTH_CHART_TEMP1/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_DEPOSIT_MONTH_CHART_TEMP1 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
