#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_DEPANDLOAN').setMaster(sys.argv[2])
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

ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
OCRM_F_CM_EXCHANGE_RATE = sqlContext.read.parquet(hdfs+'/OCRM_F_CM_EXCHANGE_RATE/*')
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")

#ACRM_F_CI_DEPANDLOAN：全量
#TEMP_ACRM_F_CI_DEP：全量
#TEMP_ACRM_F_CI_LOAN：全量



#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,CAST(SUM(A.OLD_YEAR_BAL_SUM * NVL(B.EXCHANGE_RATE, 1))   AS DECIMAL(24,6))                    AS OLD_YEAR_SUM 
       ,CAST(SUM((MONTH_BAL_SUM_1 + MONTH_BAL_SUM_2 + MONTH_BAL_SUM_3 + MONTH_BAL_SUM_4 + MONTH_BAL_SUM_5 + MONTH_BAL_SUM_6 + MONTH_BAL_SUM_7 + MONTH_BAL_SUM_8 + MONTH_BAL_SUM_9 + MONTH_BAL_SUM_10 + MONTH_BAL_SUM_11 + MONTH_BAL_SUM_12) * NVL(B.EXCHANGE_RATE, 1))   AS DECIMAL(24,6))                    AS YEAR_SUM 
       ,CAST(SUM(A.AMOUNT * NVL(B.EXCHANGE_RATE, 1)) AS DECIMAL(24,6))                      AS DP_AMOUNT 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE B                         --汇率
     ON A.CURR                  = B.CURRENCY_CD 
    AND B.OBJECT_CURRENCY_CD    = 'CNY' 
    AND NVL(B.ETL_DT,0)                = V_DT 
  WHERE A.YEAR                  = YEAR(V_DT) 
  GROUP BY A.CUST_ID 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_ACRM_F_CI_DEP = sqlContext.sql(sql)
TEMP_ACRM_F_CI_DEP.registerTempTable("TEMP_ACRM_F_CI_DEP")
dfn="TEMP_ACRM_F_CI_DEP/"+V_DT+".parquet"
TEMP_ACRM_F_CI_DEP.cache()
nrows = TEMP_ACRM_F_CI_DEP.count()
TEMP_ACRM_F_CI_DEP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TEMP_ACRM_F_CI_DEP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_ACRM_F_CI_DEP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_ACRM_F_CI_DEP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,CAST(SUM(A.OLD_YEAR_BAL_SUM * NVL(B.EXCHANGE_RATE, 1))   AS DECIMAL(24,6))                    AS OLD_YEAR_SUM 
       ,CAST(SUM((MONTH_BAL_SUM_1 + MONTH_BAL_SUM_2 + MONTH_BAL_SUM_3 + MONTH_BAL_SUM_4 + MONTH_BAL_SUM_5 + MONTH_BAL_SUM_6 + MONTH_BAL_SUM_7 + MONTH_BAL_SUM_8 + MONTH_BAL_SUM_9 + MONTH_BAL_SUM_10 + MONTH_BAL_SUM_11 + MONTH_BAL_SUM_12) * NVL(B.EXCHANGE_RATE, 1))   AS DECIMAL(24,6))                    AS YEAR_SUM 
       ,CAST(SUM(A.AMOUNT * NVL(B.EXCHANGE_RATE, 1))    AS DECIMAL(24,6))                   AS LN_AMOUNT 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_RE_LENDSUMAVGINFO A                             --贷款账户积数表
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE B                         --汇率
     ON A.MONEY_TYPE            = B.CURRENCY_CD 
    AND B.OBJECT_CURRENCY_CD    = 'CNY' 
    AND B.ETL_DT                = V_DT 
  WHERE A.YEAR                  = YEAR(V_DT)
  GROUP BY A.CUST_ID 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_ACRM_F_CI_LOAN = sqlContext.sql(sql)
TEMP_ACRM_F_CI_LOAN.registerTempTable("TEMP_ACRM_F_CI_LOAN")
dfn="TEMP_ACRM_F_CI_LOAN/"+V_DT+".parquet"
TEMP_ACRM_F_CI_LOAN.cache()
nrows = TEMP_ACRM_F_CI_LOAN.count()
TEMP_ACRM_F_CI_LOAN.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TEMP_ACRM_F_CI_LOAN.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_ACRM_F_CI_LOAN/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_ACRM_F_CI_LOAN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1
TEMP_ACRM_F_CI_DEP = sqlContext.read.parquet(hdfs+'/TEMP_ACRM_F_CI_DEP/*')
TEMP_ACRM_F_CI_DEP.registerTempTable("TEMP_ACRM_F_CI_DEP")
sql = """
 SELECT monotonically_increasing_id() AS ID 
       ,SUBSTR(V_DT, 1, 7)                       AS YEAR_MONTH 
       ,A.CUST_ID               AS CUST_ID 
       ,CAST((CASE WHEN(C.OLD_YEAR_SUM IS NULL OR C.OLD_YEAR_SUM = 0) THEN 0 ELSE B.OLD_YEAR_SUM / C.OLD_YEAR_SUM END) AS DECIMAL(17,4)) AS DEPO_LOAN_LAST 
       ,CAST((CASE WHEN(C.YEAR_SUM IS NULL OR C.YEAR_SUM = 0) THEN 0 ELSE B.YEAR_SUM / C.YEAR_SUM END) AS DECIMAL(17,4)) AS DEPO_LOAN 
       ,V_DT                       AS ODS_ST_DATE 
       ,CAST(NVL(B.OLD_YEAR_SUM, 0)  AS DECIMAL(24,6))                     AS SAVING_YEAR_AMT_LAST 
       ,CAST(NVL(B.YEAR_SUM, 0)  AS DECIMAL(24,6))                     AS SAVING_YEAR_AMT 
       ,CAST(NVL(C.OLD_YEAR_SUM, 0) AS DECIMAL(24,6))                      AS LN_YEAR_AMT_LAST 
       ,CAST(NVL(C.YEAR_SUM, 0) AS DECIMAL(24,6))                      AS LN_YEAR_AMT 
       ,CAST('0' AS DECIMAL(24,6))                   AS PREF_INT_AMT 
       ,CAST('0' AS DECIMAL(24,6))                   AS ACT_PREF_INT_AMT 
       ,A.FR_ID                 AS FR_ORG_ID 
       ,CAST(NVL(B.DP_AMOUNT, 0)  AS DECIMAL(24,6))                     AS DP_AMOUNT 
       ,CAST(NVL(C.LN_AMOUNT, 0) AS DECIMAL(24,6))                      AS LN_AMOUNT 
       ,CAST('0'  AS DECIMAL(24,6))                  AS LN_AVG_SEASON 
       ,CAST('0'  AS DECIMAL(24,6))                  AS DP_AVG_MONTH 
       ,CAST('0'  AS DECIMAL(24,6))                  AS LN_AVG_MONTH 
       ,CAST('0'  AS DECIMAL(24,6))                 AS DP_AVG_SEASON 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息
   LEFT JOIN TEMP_ACRM_F_CI_DEP B                              --TEMP_ACRM_F_CI_DEP
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN TEMP_ACRM_F_CI_LOAN C                             --TEMP_ACRM_F_CI_LOAN
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_DEPANDLOAN = sqlContext.sql(sql)
ACRM_F_CI_DEPANDLOAN.registerTempTable("ACRM_F_CI_DEPANDLOAN")
dfn="ACRM_F_CI_DEPANDLOAN/"+V_DT+".parquet"
ACRM_F_CI_DEPANDLOAN.cache()
nrows = ACRM_F_CI_DEPANDLOAN.count()
ACRM_F_CI_DEPANDLOAN.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_DEPANDLOAN.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_DEPANDLOAN/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_DEPANDLOAN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
