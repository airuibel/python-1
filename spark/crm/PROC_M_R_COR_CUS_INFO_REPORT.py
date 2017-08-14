#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_COR_CUS_INFO_REPORT').setMaster(sys.argv[2])
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

#MCRM_COR_CUS_INFO_REPORT 增量 删除当天文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_COR_CUS_INFO_REPORT/"+V_DT+".parquet")

ACRM_F_AG_AGREEMENT = sqlContext.read.parquet(hdfs+'/ACRM_F_AG_AGREEMENT/*')
ACRM_F_AG_AGREEMENT.registerTempTable("ACRM_F_AG_AGREEMENT")
OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
OCRM_F_CM_EXCHANGE_RATE = sqlContext.read.parquet(hdfs+'/OCRM_F_CM_EXCHANGE_RATE/*')
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,MONEY_TYPE              AS CURR 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_LENDSUMAVGINFO A                             --贷款账户积数表
  WHERE CUST_TYP                = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_CUST_CURR = sqlContext.sql(sql)
TEMP_CUST_CURR.registerTempTable("TEMP_CUST_CURR")
dfn="TEMP_CUST_CURR/"+V_DT+".parquet"
TEMP_CUST_CURR.cache()
nrows = TEMP_CUST_CURR.count()
TEMP_CUST_CURR.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TEMP_CUST_CURR.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_CUST_CURR/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_CUST_CURR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CURR                    AS CURR 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --ACRM_F_RE_SAVESUMAVGINFO
  WHERE CUST_TYP                = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_CUST_CURR = sqlContext.sql(sql)
TEMP_CUST_CURR.registerTempTable("TEMP_CUST_CURR")
dfn="TEMP_CUST_CURR/"+V_DT+".parquet"
TEMP_CUST_CURR.cache()
nrows = TEMP_CUST_CURR.count()
TEMP_CUST_CURR.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_CUST_CURR.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_CUST_CURR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_ZH_NAME 
       ,A.CERT_NO               AS CUST_INT_CODE 
       ,TRIM(A.INDUS_CALSS_MAIN)                       AS HY_CLASS 
       ,C.OPEN_DT               AS OPEN_ACCOUNT_DATE 
       ,B.CURR                  AS CURR 
       ,CAST(''  AS DECIMAL(24,6))                   AS YEAR_DEP_AMOUNT 
       ,CAST(''  AS DECIMAL(24,6))                   AS YEAR_DEP_AVG 
       ,CAST(''  AS DECIMAL(24,6))                   AS YEAR_CRE_AMOUNT 
       ,CAST(''  AS DECIMAL(24,6))                   AS YEAR_CRE_AVG 
       ,CAST(''  AS DECIMAL(24,6))                   AS MONTH_DEP_AMOUNT 
       ,CAST(''  AS DECIMAL(24,6))                   AS MONTH_DEP_AVG 
       ,CAST(''  AS DECIMAL(24,6))                   AS MONTH_CRE_AMOUNT 
       ,CAST(''  AS DECIMAL(24,6))                   AS MONTH_CRE_AVG 
       ,CAST(''  AS DECIMAL(24,6))                   AS DEP_AMOUNT 
       ,CAST(''  AS DECIMAL(24,6))                   AS DEP_AVG 
       ,CAST(''  AS DECIMAL(24,6))                   AS CRE_AMOUNT 
       ,CAST(''  AS DECIMAL(24,6))                   AS CRE_AVG 
       ,CAST(''  AS DECIMAL(24,6))                   AS DEP_YEAR_AVG 
       ,CAST(''  AS DECIMAL(24,6))                   AS CRE_YEAR_AVG 
       ,''                    AS DEP_CRE_FLAG 
       ,''                    AS CUST_MANAGER_NAME 
       ,''                    AS CUST_MANAGER 
       ,''                    AS ORG_NAME 
       ,''                    AS ORG_ID 
       ,''                    AS CUST_MAIN_TYPE 
       ,''                    AS ORG_MAIN_TYPE 
       ,V_DT               AS REPORT_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户信息表
   LEFT JOIN TEMP_CUST_CURR B                                  --
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN 
 (SELECT CUST_ID ,FR_ID
       ,MIN(START_DATE)                       AS OPEN_DT 
   FROM ACRM_F_AG_AGREEMENT 
  GROUP BY CUST_ID 
       ,FR_ID) C                                                --
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_COR_CUS_INFO_REPORT_TMP = sqlContext.sql(sql)
MCRM_COR_CUS_INFO_REPORT_TMP.registerTempTable("MCRM_COR_CUS_INFO_REPORT_TMP")
dfn="MCRM_COR_CUS_INFO_REPORT_TMP/"+V_DT+".parquet"
MCRM_COR_CUS_INFO_REPORT_TMP.cache()
nrows = MCRM_COR_CUS_INFO_REPORT_TMP.count()
MCRM_COR_CUS_INFO_REPORT_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_COR_CUS_INFO_REPORT_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_COR_CUS_INFO_REPORT_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_COR_CUS_INFO_REPORT_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-04::
V_STEP = V_STEP + 1
MCRM_COR_CUS_INFO_REPORT_TMP = sqlContext.read.parquet(hdfs+'/MCRM_COR_CUS_INFO_REPORT_TMP/*')
MCRM_COR_CUS_INFO_REPORT_TMP.registerTempTable("MCRM_COR_CUS_INFO_REPORT_TMP")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_INT_CODE         AS CUST_INT_CODE 
       ,A.HY_CLASS              AS HY_CLASS 
       ,A.OPEN_ACCOUNT_DATE     AS OPEN_ACCOUNT_DATE 
       ,A.CURR                  AS CURR 
       ,CAST(SUM(NVL(B.OLD_YEAR_BAL, 0) * NVL(C.EXCHANGE_RATE, 1))    AS DECIMAL(24,6))                   AS YEAR_DEP_AMOUNT 
       ,CAST(SUM(NVL(B.OLD_YEAR_BAL_SUM, 0) * NVL(C.EXCHANGE_RATE, 1) / NVL(B.OLD_YEAR_DAYS, 1))   AS DECIMAL(24,6))                      AS YEAR_DEP_AVG 
       ,CAST(A.YEAR_CRE_AMOUNT  AS DECIMAL(24,6))       AS YEAR_CRE_AMOUNT 
       ,CAST(A.YEAR_CRE_AVG   AS DECIMAL(24,6))         AS YEAR_CRE_AVG 
       ,CAST(SUM(CASE MONTH(V_DT) 
           WHEN '1' THEN NVL(B.OLD_YEAR_BAL, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '2' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '3' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '4' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '5' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '6' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '7' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '8' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '9' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '10' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '11' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '12' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) END)   AS DECIMAL(24,6))                    AS MONTH_DEP_AMOUNT 
       ,CAST(A.MONTH_DEP_AVG  AS DECIMAL(24,6))         AS MONTH_DEP_AVG 
       ,CAST(A.MONTH_CRE_AMOUNT AS DECIMAL(24,6))       AS MONTH_CRE_AMOUNT 
       ,CAST(A.MONTH_CRE_AVG  AS DECIMAL(24,6))         AS MONTH_CRE_AVG 
       ,CAST(SUM(CASE MONTH(V_DT) 
           WHEN '1' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '2' THEN NVL(B.MONTH_BAL_2, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '3' THEN NVL(B.MONTH_BAL_3, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '4' THEN NVL(B.MONTH_BAL_4, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '5' THEN NVL(B.MONTH_BAL_5, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '6' THEN NVL(B.MONTH_BAL_6, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '7' THEN NVL(B.MONTH_BAL_7, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '8' THEN NVL(B.MONTH_BAL_8, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '9' THEN NVL(B.MONTH_BAL_9, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '10' THEN NVL(B.MONTH_BAL_10, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '11' THEN NVL(B.MONTH_BAL_11, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '12' THEN NVL(B.MONTH_BAL_12, 0) * NVL(C.EXCHANGE_RATE, 1) END)    AS DECIMAL(24,6))                     AS DEP_AMOUNT 
       ,CAST(A.DEP_AVG AS DECIMAL(24,6))                AS DEP_AVG 
       ,CAST(A.CRE_AMOUNT AS DECIMAL(24,6))             AS CRE_AMOUNT 
       ,CAST(A.CRE_AVG AS DECIMAL(24,6))                AS CRE_AVG 
       ,CAST(A.DEP_YEAR_AVG  AS DECIMAL(24,6))          AS DEP_YEAR_AVG 
       ,CAST(A.CRE_YEAR_AVG  AS DECIMAL(24,6))          AS CRE_YEAR_AVG 
       ,A.DEP_CRE_FLAG          AS DEP_CRE_FLAG 
       ,A.CUST_MANAGER_NAME     AS CUST_MANAGER_NAME 
       ,A.CUST_MANAGER          AS CUST_MANAGER 
       ,A.ORG_NAME              AS ORG_NAME 
       ,A.ORG_ID                AS ORG_ID 
       ,A.CUST_MAIN_TYPE        AS CUST_MAIN_TYPE 
       ,A.ORG_MAIN_TYPE         AS ORG_MAIN_TYPE 
       ,A.REPORT_DATE           AS REPORT_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM MCRM_COR_CUS_INFO_REPORT_TMP A                         --MCRM_COR_CUS_INFO_REPORT_TMP
  INNER JOIN ACRM_F_RE_SAVESUMAVGINFO B                        --ACRM_F_RE_SAVESUMAVGINFO
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.CURR                  = B.CURR 
    AND B.YEAR                  = YEAR(V_DT)
    AND B.CUST_TYP              = '2' 
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE C                         --汇率表
     ON B.CURR                  = C.CURRENCY_CD 
    AND C.OBJECT_CURRENCY_CD    = 'CNY' 
    AND C.ETL_DT                = V_DT8 
  WHERE A.REPORT_DATE           = V_DT 
  GROUP BY A.CUST_ID               
       ,A.CUST_ZH_NAME                                
       ,A.CUST_INT_CODE                               
       ,A.HY_CLASS                                    
       ,A.OPEN_ACCOUNT_DATE                           
       ,A.CURR                                        
       ,CAST(A.YEAR_CRE_AMOUNT     AS DECIMAL(24,6))                                                    
       ,CAST(A.YEAR_CRE_AVG   AS DECIMAL(24,6))                                
       ,CAST(A.MONTH_DEP_AVG  AS DECIMAL(24,6))                                
       ,CAST(A.MONTH_CRE_AMOUNT AS DECIMAL(24,6))                              
       ,CAST(A.MONTH_CRE_AVG  AS DECIMAL(24,6))                                
       ,CAST(A.DEP_AVG AS DECIMAL(24,6))                                       
       ,CAST(A.CRE_AMOUNT AS DECIMAL(24,6))                                    
       ,CAST(A.CRE_AVG AS DECIMAL(24,6))                                       
       ,CAST(A.DEP_YEAR_AVG  AS DECIMAL(24,6))                                 
       ,CAST(A.CRE_YEAR_AVG  AS DECIMAL(24,6))                                 
       ,A.DEP_CRE_FLAG                                   
       ,A.CUST_MANAGER_NAME                              
       ,A.CUST_MANAGER                                   
       ,A.ORG_NAME                                       
       ,A.ORG_ID                                         
       ,A.CUST_MAIN_TYPE                                 
       ,A.ORG_MAIN_TYPE                                  
       ,A.REPORT_DATE                                    
       ,A.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT8\b", "'"+V_DT+"'", sql)
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1 = sqlContext.sql(sql)
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1.registerTempTable("MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --:src.CUST_ID
       ,DST.CUST_ZH_NAME                                       --:src.CUST_ZH_NAME
       ,DST.CUST_INT_CODE                                      --:src.CUST_INT_CODE
       ,DST.HY_CLASS                                           --:src.HY_CLASS
       ,DST.OPEN_ACCOUNT_DATE                                  --:src.OPEN_ACCOUNT_DATE
       ,DST.CURR                                               --:src.CURR
       ,DST.YEAR_DEP_AMOUNT                                    --:src.YEAR_DEP_AMOUNT
       ,DST.YEAR_DEP_AVG                                       --:src.YEAR_DEP_AVG
       ,DST.YEAR_CRE_AMOUNT                                    --:src.YEAR_CRE_AMOUNT
       ,DST.YEAR_CRE_AVG                                       --:src.YEAR_CRE_AVG
       ,DST.MONTH_DEP_AMOUNT                                   --:src.MONTH_DEP_AMOUNT
       ,DST.MONTH_DEP_AVG                                      --:src.MONTH_DEP_AVG
       ,DST.MONTH_CRE_AMOUNT                                   --:src.MONTH_CRE_AMOUNT
       ,DST.MONTH_CRE_AVG                                      --:src.MONTH_CRE_AVG
       ,DST.DEP_AMOUNT                                         --:src.DEP_AMOUNT
       ,DST.DEP_AVG                                            --:src.DEP_AVG
       ,DST.CRE_AMOUNT                                         --:src.CRE_AMOUNT
       ,DST.CRE_AVG                                            --:src.CRE_AVG
       ,DST.DEP_YEAR_AVG                                       --:src.DEP_YEAR_AVG
       ,DST.CRE_YEAR_AVG                                       --:src.CRE_YEAR_AVG
       ,DST.DEP_CRE_FLAG                                       --:src.DEP_CRE_FLAG
       ,DST.CUST_MANAGER_NAME                                  --:src.CUST_MANAGER_NAME
       ,DST.CUST_MANAGER                                       --:src.CUST_MANAGER
       ,DST.ORG_NAME                                           --:src.ORG_NAME
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.CUST_MAIN_TYPE                                     --:src.CUST_MAIN_TYPE
       ,DST.ORG_MAIN_TYPE                                      --:src.ORG_MAIN_TYPE
       ,DST.REPORT_DATE                                        --:src.REPORT_DATE
       ,DST.FR_ID                                              --:src.FR_ID
   FROM MCRM_COR_CUS_INFO_REPORT_TMP DST 
   LEFT JOIN MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.CURR                = DST.CURR 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP2 = sqlContext.sql(sql)
dfn="MCRM_COR_CUS_INFO_REPORT_TMP/"+V_DT+".parquet"
UNION=MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP2.unionAll(MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1)
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1.cache()
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP2.cache()
nrowsi = MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1.count()
nrowsa = MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1.unpersist()
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_COR_CUS_INFO_REPORT_TMP lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/MCRM_COR_CUS_INFO_REPORT_TMP/"+V_DT_LD+".parquet /"+dbname+"/MCRM_COR_CUS_INFO_REPORT_TMP_BK/")

#任务[12] 001-05::
V_STEP = V_STEP + 1
MCRM_COR_CUS_INFO_REPORT_TMP = sqlContext.read.parquet(hdfs+'/MCRM_COR_CUS_INFO_REPORT_TMP/*')
MCRM_COR_CUS_INFO_REPORT_TMP.registerTempTable("MCRM_COR_CUS_INFO_REPORT_TMP")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_INT_CODE         AS CUST_INT_CODE 
       ,A.HY_CLASS              AS HY_CLASS 
       ,A.OPEN_ACCOUNT_DATE     AS OPEN_ACCOUNT_DATE 
       ,A.CURR                  AS CURR 
       ,CAST(A.YEAR_DEP_AMOUNT  AS DECIMAL(24,6))     AS YEAR_DEP_AMOUNT 
       ,CAST(A.YEAR_DEP_AVG  AS DECIMAL(24,6))        AS YEAR_DEP_AVG 
       ,CAST(SUM(NVL(B.OLD_YEAR_BAL, 0) * NVL(C.EXCHANGE_RATE, 1))  AS DECIMAL(24,6))                     AS YEAR_CRE_AMOUNT 
       ,CAST(SUM(NVL(B.OLD_YEAR_BAL_SUM, 0) * NVL(C.EXCHANGE_RATE, 1) / NVL(B.OLD_YEAR_DAYS, 1))  AS DECIMAL(24,6))                     AS YEAR_CRE_AVG 
       ,CAST(A.MONTH_DEP_AMOUNT  AS DECIMAL(24,6))    AS MONTH_DEP_AMOUNT 
       ,CAST(A.MONTH_DEP_AVG    AS DECIMAL(24,6))     AS MONTH_DEP_AVG 
       ,CAST(SUM(NVL(B.OLD_YEAR_BAL, 0) * NVL(C.EXCHANGE_RATE, 1))  AS DECIMAL(24,6))                     AS MONTH_CRE_AMOUNT 
       ,CAST(SUM(NVL(B.OLD_YEAR_BAL_SUM, 0) * NVL(C.EXCHANGE_RATE, 1) / NVL(OLD_YEAR_DAYS, 1))       AS DECIMAL(24,6))                AS MONTH_CRE_AVG 
       ,CAST(A.DEP_AMOUNT  AS DECIMAL(24,6))          AS DEP_AMOUNT 
       ,CAST(A.DEP_AVG   AS DECIMAL(24,6))            AS DEP_AVG 
       ,CAST(SUM(CASE MONTH(V_DT) 
           WHEN '1' THEN NVL(B.MONTH_BAL_1, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '2' THEN NVL(B.MONTH_BAL_2, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '3' THEN NVL(B.MONTH_BAL_3, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '4' THEN NVL(B.MONTH_BAL_4, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '5' THEN NVL(B.MONTH_BAL_5, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '6' THEN NVL(B.MONTH_BAL_6, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '7' THEN NVL(B.MONTH_BAL_7, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '8' THEN NVL(B.MONTH_BAL_8, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '9' THEN NVL(B.MONTH_BAL_9, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '10' THEN NVL(B.MONTH_BAL_10, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '11' THEN NVL(B.MONTH_BAL_11, 0) * NVL(C.EXCHANGE_RATE, 1) 
           WHEN '12' THEN NVL(B.MONTH_BAL_12, 0) * NVL(C.EXCHANGE_RATE, 1) END)    AS DECIMAL(24,6))                   AS CRE_AMOUNT 
       ,CAST(A.CRE_AVG      AS DECIMAL(24,6))         AS CRE_AVG 
       ,CAST(A.DEP_YEAR_AVG  AS DECIMAL(24,6))        AS DEP_YEAR_AVG 
       ,CAST(A.CRE_YEAR_AVG   AS DECIMAL(24,6))       AS CRE_YEAR_AVG 
       ,'1'                     AS DEP_CRE_FLAG 
       ,A.CUST_MANAGER_NAME     AS CUST_MANAGER_NAME 
       ,A.CUST_MANAGER          AS CUST_MANAGER 
       ,A.ORG_NAME              AS ORG_NAME 
       ,A.ORG_ID                AS ORG_ID 
       ,A.CUST_MAIN_TYPE        AS CUST_MAIN_TYPE 
       ,A.ORG_MAIN_TYPE         AS ORG_MAIN_TYPE 
       ,A.REPORT_DATE           AS REPORT_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM MCRM_COR_CUS_INFO_REPORT_TMP A                         --MCRM_COR_CUS_INFO_REPORT_TMP
  INNER JOIN ACRM_F_RE_LENDSUMAVGINFO B                        --贷款账户积数表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.CURR                  = B.MONEY_TYPE 
    AND B.YEAR                  = YEAR(V_DT)
    AND B.CUST_TYP              = '2' 
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE C                         --汇率表
     ON B.MONEY_TYPE            = C.CURRENCY_CD 
    AND C.OBJECT_CURRENCY_CD    = 'CNY' 
    AND C.ETL_DT                = V_DT8 
  WHERE A.REPORT_DATE           = V_DT 
  GROUP BY A.CUST_ID               
       ,A.CUST_ZH_NAME                                  
       ,A.CUST_INT_CODE                                 
       ,A.HY_CLASS                                      
       ,A.OPEN_ACCOUNT_DATE                             
       ,A.CURR                                          
       ,CAST(A.YEAR_DEP_AMOUNT  AS DECIMAL(24,6))                          
       ,CAST(A.YEAR_DEP_AVG  AS DECIMAL(24,6))                             
       ,CAST(A.MONTH_DEP_AMOUNT  AS DECIMAL(24,6))                         
       ,CAST(A.MONTH_DEP_AVG    AS DECIMAL(24,6))                          
       ,CAST(A.DEP_AMOUNT  AS DECIMAL(24,6))                               
       ,CAST(A.DEP_AVG   AS DECIMAL(24,6))                                 
       ,CAST(A.CRE_AVG      AS DECIMAL(24,6))                              
       ,CAST(A.DEP_YEAR_AVG  AS DECIMAL(24,6))                             
       ,CAST(A.CRE_YEAR_AVG   AS DECIMAL(24,6))                                                 
       ,A.CUST_MANAGER_NAME                               
       ,A.CUST_MANAGER                                    
       ,A.ORG_NAME                                        
       ,A.ORG_ID                                          
       ,A.CUST_MAIN_TYPE                                  
       ,A.ORG_MAIN_TYPE                                   
       ,A.REPORT_DATE                                     
       ,A.FR_ID                                            """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT8\b", "'"+V_DT+"'", sql)
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1 = sqlContext.sql(sql)
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1.registerTempTable("MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1")


sql = """
 SELECT DST.CUST_ID                                             --:src.CUST_ID
       ,DST.CUST_ZH_NAME                                       --:src.CUST_ZH_NAME
       ,DST.CUST_INT_CODE                                      --:src.CUST_INT_CODE
       ,DST.HY_CLASS                                           --:src.HY_CLASS
       ,DST.OPEN_ACCOUNT_DATE                                  --:src.OPEN_ACCOUNT_DATE
       ,DST.CURR                                               --:src.CURR
       ,DST.YEAR_DEP_AMOUNT                                    --:src.YEAR_DEP_AMOUNT
       ,DST.YEAR_DEP_AVG                                       --:src.YEAR_DEP_AVG
       ,DST.YEAR_CRE_AMOUNT                                    --:src.YEAR_CRE_AMOUNT
       ,DST.YEAR_CRE_AVG                                       --:src.YEAR_CRE_AVG
       ,DST.MONTH_DEP_AMOUNT                                   --:src.MONTH_DEP_AMOUNT
       ,DST.MONTH_DEP_AVG                                      --:src.MONTH_DEP_AVG
       ,DST.MONTH_CRE_AMOUNT                                   --:src.MONTH_CRE_AMOUNT
       ,DST.MONTH_CRE_AVG                                      --:src.MONTH_CRE_AVG
       ,DST.DEP_AMOUNT                                         --:src.DEP_AMOUNT
       ,DST.DEP_AVG                                            --:src.DEP_AVG
       ,DST.CRE_AMOUNT                                         --:src.CRE_AMOUNT
       ,DST.CRE_AVG                                            --:src.CRE_AVG
       ,DST.DEP_YEAR_AVG                                       --:src.DEP_YEAR_AVG
       ,DST.CRE_YEAR_AVG                                       --:src.CRE_YEAR_AVG
       ,DST.DEP_CRE_FLAG                                       --:src.DEP_CRE_FLAG
       ,DST.CUST_MANAGER_NAME                                  --:src.CUST_MANAGER_NAME
       ,DST.CUST_MANAGER                                       --:src.CUST_MANAGER
       ,DST.ORG_NAME                                           --:src.ORG_NAME
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.CUST_MAIN_TYPE                                     --:src.CUST_MAIN_TYPE
       ,DST.ORG_MAIN_TYPE                                      --:src.ORG_MAIN_TYPE
       ,DST.REPORT_DATE                                        --:src.REPORT_DATE
       ,DST.FR_ID                                              --:src.FR_ID
   FROM MCRM_COR_CUS_INFO_REPORT_TMP DST 
   LEFT JOIN MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.CURR                = DST.CURR 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP2 = sqlContext.sql(sql)
dfn="MCRM_COR_CUS_INFO_REPORT_TMP/"+V_DT+".parquet"
UNION=MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP2.unionAll(MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1)
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1.cache()
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP2.cache()
nrowsi = MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1.count()
nrowsa = MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP1.unpersist()
MCRM_COR_CUS_INFO_REPORT_TMP_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_COR_CUS_INFO_REPORT_TMP lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/MCRM_COR_CUS_INFO_REPORT_TMP/"+V_DT_LD+".parquet /"+dbname+"/MCRM_COR_CUS_INFO_REPORT_TMP_BK/")

#任务[11] 001-06::
V_STEP = V_STEP + 1
MCRM_COR_CUS_INFO_REPORT_TMP = sqlContext.read.parquet(hdfs+'/MCRM_COR_CUS_INFO_REPORT_TMP/*')
MCRM_COR_CUS_INFO_REPORT_TMP.registerTempTable("MCRM_COR_CUS_INFO_REPORT_TMP")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_INT_CODE         AS CUST_INT_CODE 
       ,A.HY_CLASS              AS HY_CLASS 
       ,A.OPEN_ACCOUNT_DATE     AS OPEN_ACCOUNT_DATE 
       ,A.CURR                  AS CURR 
       ,CAST(A.YEAR_DEP_AMOUNT  AS DECIMAL(24,6))      AS YEAR_DEP_AMOUNT 
       ,CAST(A.YEAR_DEP_AVG       AS DECIMAL(24,6))   AS YEAR_DEP_AVG 
       ,CAST(A.YEAR_CRE_AMOUNT    AS DECIMAL(24,6))   AS YEAR_CRE_AMOUNT 
       ,CAST(A.YEAR_CRE_AVG       AS DECIMAL(24,6))   AS YEAR_CRE_AVG 
       ,CAST(A.MONTH_DEP_AMOUNT   AS DECIMAL(24,6))   AS MONTH_DEP_AMOUNT 
       ,CAST(A.MONTH_DEP_AVG      AS DECIMAL(24,6))   AS MONTH_DEP_AVG 
       ,CAST(A.MONTH_CRE_AMOUNT   AS DECIMAL(24,6))   AS MONTH_CRE_AMOUNT 
       ,CAST(A.MONTH_CRE_AVG      AS DECIMAL(24,6))   AS MONTH_CRE_AVG 
       ,CAST(A.DEP_AMOUNT         AS DECIMAL(24,6))   AS DEP_AMOUNT 
       ,CAST(A.DEP_AVG            AS DECIMAL(24,6))   AS DEP_AVG 
       ,CAST(A.CRE_AMOUNT         AS DECIMAL(24,6))   AS CRE_AMOUNT 
       ,CAST(A.CRE_AVG            AS DECIMAL(24,6))   AS CRE_AVG 
       ,CAST(A.DEP_YEAR_AVG       AS DECIMAL(24,6))   AS DEP_YEAR_AVG 
       ,CAST(A.CRE_YEAR_AVG       AS DECIMAL(24,6))   AS CRE_YEAR_AVG 
       ,''                    AS DEP_CRE_FLAG 
       ,TRIM(C.MGR_NAME)                       AS CUST_MANAGER_NAME 
       ,TRIM(C.MGR_ID)                       AS CUST_MANAGER 
       ,TRIM(B.INSTITUTION_NAME)                       AS ORG_NAME 
       ,TRIM(B.INSTITUTION_CODE)                       AS ORG_ID 
       ,C.MAIN_TYPE             AS CUST_MAIN_TYPE 
       ,B.MAIN_TYPE             AS ORG_MAIN_TYPE 
       ,A.REPORT_DATE           AS REPORT_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM MCRM_COR_CUS_INFO_REPORT_TMP A                         --MCRM_COR_CUS_INFO_REPORT_TMP
   LEFT JOIN OCRM_F_CI_BELONG_ORG B                            --机构归属
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR C                        --客户归属
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_COR_CUS_INFO_REPORT = sqlContext.sql(sql)
MCRM_COR_CUS_INFO_REPORT.registerTempTable("MCRM_COR_CUS_INFO_REPORT")
dfn="MCRM_COR_CUS_INFO_REPORT/"+V_DT+".parquet"
MCRM_COR_CUS_INFO_REPORT.cache()
nrows = MCRM_COR_CUS_INFO_REPORT.count()
MCRM_COR_CUS_INFO_REPORT.write.save(path=hdfs + '/' + dfn, mode='append')
MCRM_COR_CUS_INFO_REPORT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_COR_CUS_INFO_REPORT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
