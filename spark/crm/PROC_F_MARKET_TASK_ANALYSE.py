#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_MARKET_TASK_ANALYSE').setMaster(sys.argv[2])
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

OCRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_DP_CARD_INFO/*')
OCRM_F_DP_CARD_INFO.registerTempTable("OCRM_F_DP_CARD_INFO")
OCRM_F_MARKET_TASK = sqlContext.read.parquet(hdfs+'/OCRM_F_MARKET_TASK/*')
OCRM_F_MARKET_TASK.registerTempTable("OCRM_F_MARKET_TASK")
OCRM_F_MARKET_TASK_CUST = sqlContext.read.parquet(hdfs+'/OCRM_F_MARKET_TASK_CUST/*')
OCRM_F_MARKET_TASK_CUST.registerTempTable("OCRM_F_MARKET_TASK_CUST")
ACRM_F_NI_FINANCING = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_FINANCING/*')
ACRM_F_NI_FINANCING.registerTempTable("ACRM_F_NI_FINANCING")
TMP_CUST_MARK_BEFORE = sqlContext.read.parquet(hdfs+'/TMP_CUST_MARK_BEFORE/*')
TMP_CUST_MARK_BEFORE.registerTempTable("TMP_CUST_MARK_BEFORE")
OCRM_F_CI_CUSTLNAINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUSTLNAINFO/*')
OCRM_F_CI_CUSTLNAINFO.registerTempTable("OCRM_F_CI_CUSTLNAINFO")
OCRM_F_MARKET_TASK_EXECUTOR = sqlContext.read.parquet(hdfs+'/OCRM_F_MARKET_TASK_EXECUTOR/*')
OCRM_F_MARKET_TASK_EXECUTOR.registerTempTable("OCRM_F_MARKET_TASK_EXECUTOR")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
TMP_CUST_MARK = sqlContext.read.parquet(hdfs+'/TMP_CUST_MARK/*')
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
OCRM_F_MARKET_TASK_TARGET = sqlContext.read.parquet(hdfs+'/OCRM_F_MARKET_TASK_TARGET/*')
OCRM_F_MARKET_TASK_TARGET.registerTempTable("OCRM_F_MARKET_TASK_TARGET")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT DISTINCT CAST(A.ID                    AS DECIMAL(27))                       AS EXEC_ID 
       ,CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.NORM_ID AS VARCHAR(20))           AS TASK_TARGET_ID 
       ,CAST(B.TASK_TARGET_VALUE     AS DECIMAL(22, 2))                       AS TASK_TARGET_VALUE 
       ,A.EXECUTOR_ID           AS CUSTMGR_ID 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
   LEFT JOIN OCRM_F_MARKET_TASK_TARGET B                       --营销任务指标分解表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_MARKET_TASK C                              --营销任务主表
     ON A.TASK_ID               = C.ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.TASK_BEGIN_DATE       = V_DT 
   LEFT JOIN OCRM_F_MARKET_TASK_CUST D                         --营销子任务关联客户表
     ON A.ID                    = D.EXEC_ID 
    AND A.FR_ID                 = D.FR_ID 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND D.CUST_STAT             = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.IF_CUST               = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUSTMGR_TASK = sqlContext.sql(sql)
TMP_CUSTMGR_TASK.registerTempTable("TMP_CUSTMGR_TASK")
dfn="TMP_CUSTMGR_TASK/"+V_DT+".parquet"
TMP_CUSTMGR_TASK.cache()
nrows = TMP_CUSTMGR_TASK.count()
TMP_CUSTMGR_TASK.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_CUSTMGR_TASK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUSTMGR_TASK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUSTMGR_TASK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
TMP_CUSTMGR_TASK = sqlContext.read.parquet(hdfs+'/TMP_CUSTMGR_TASK/*')
TMP_CUSTMGR_TASK.registerTempTable("TMP_CUSTMGR_TASK")
sql = """
 SELECT CAST(EXEC_ID                 AS DECIMAL(27))                       AS EXEC_ID 
       ,CAST(TASK_ID                 AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(PARENT_EXEC_ID          AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CUSTMGR_ID              AS EXECUTOR_ID 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10005' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS SAVE_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10006' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10007' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS DEB_CARD_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10008' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_CARD_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10009' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS FIN_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10010' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS IBK_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10011' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS MBK_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10012' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS WBK_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10013' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS MID_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10014' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS SAVE_BAL_AVG 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10015' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS SAVE_BAL 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10016' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_BAL_AVG 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10017' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_BAL 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10018' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS DEBIT_CARD 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10019' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CREDIT_CARD 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10020' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS FIN_BAL_AVG 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10021' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS FIN_BAL 
       ,FR_ID                   AS FR_ID 
   FROM TMP_CUSTMGR_TASK A                                     --营销任务客户经理任务临时表
  GROUP BY EXEC_ID 
       ,TASK_ID 
       ,PARENT_EXEC_ID 
       ,CUSTMGR_ID 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_BEFORE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_BEFORE")
dfn="OCRM_F_MACKET_TASK_ANALYSE_BEFORE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE_BEFORE.count()
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_MACKET_TASK_ANALYSE_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE_BEFORE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE_BEFORE/*')
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_BEFORE")
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_MACKET_TASK_ANALYSE/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_MACKET_TASK_ANALYSE_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet")
sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10005                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.SAVE_ACCOUNT          AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(COUNT(1)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.SAVE_ACCOUNT IS  NOT NULL 
  WHERE A.MARK_FLAG             = '1' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.SAVE_ACCOUNT 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10006                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CRE_ACCOUNT           AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(COUNT(1)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CRE_ACCOUNT IS 
    NOT NULL 
  WHERE A.MARK_FLAG             = '2' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.CRE_ACCOUNT 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10007                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.DEB_CARD_ACCOUNT      AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(COUNT(1)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.DEB_CARD_ACCOUNT IS 
    NOT NULL 
  WHERE A.MARK_FLAG             = '3' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.DEB_CARD_ACCOUNT 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-06::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10008                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CRE_CARD_ACCOUNT      AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(COUNT(1)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CRE_CARD_ACCOUNT IS 
    NOT NULL 
  WHERE A.MARK_FLAG             = '4' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.CRE_CARD_ACCOUNT 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-07::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10009                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.FIN_ACCOUNT           AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(COUNT(1)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.FIN_ACCOUNT IS 
    NOT NULL 
  WHERE A.MARK_FLAG             = '5' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.FIN_ACCOUNT 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-08::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10010                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.IBK_ACCOUNT           AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(COUNT(1)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.IBK_ACCOUNT IS 
    NOT NULL 
  WHERE A.MARK_FLAG             = '6' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.IBK_ACCOUNT 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-09::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10011                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.MBK_ACCOUNT           AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(COUNT(1)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MBK_ACCOUNT IS 
    NOT NULL 
  WHERE A.MARK_FLAG             = '7' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.MBK_ACCOUNT 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-10::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10012                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.WBK_ACCOUNT           AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(COUNT(1)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.WBK_ACCOUNT IS 
    NOT NULL 
  WHERE A.MARK_FLAG             = '8' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.WBK_ACCOUNT 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-11::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10013                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.MID_ACCOUNT           AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(COUNT(1)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MID_ACCOUNT IS 
    NOT NULL 
  WHERE A.MARK_FLAG             = '9' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.MID_ACCOUNT 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-12::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10014                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.SAVE_BAL_AVG          AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(SUM(C.MONTH_AVG)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --负债协议表
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.SAVE_BAL_AVG IS 
    NOT NULL 
   LEFT JOIN ACRM_F_DP_SAVE_INFO C                             --
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.ACCT_STATUS           = '01' 
  WHERE A.MARK_FLAG             = '1' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.SAVE_BAL_AVG 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-13::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10015                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.SAVE_BAL              AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(SUM(C.BAL_RMB)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --负债协议表
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.SAVE_BAL IS 
    NOT NULL 
   LEFT JOIN ACRM_F_DP_SAVE_INFO C                             --
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.ACCT_STATUS           = '01' 
  WHERE A.MARK_FLAG             = '1' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.SAVE_BAL 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-14::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10016                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CRE_BAL_AVG           AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(SUM(C.MONTH_AVG)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --资产协议表
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CRE_BAL_AVG IS 
    NOT NULL 
   LEFT JOIN ACRM_F_CI_ASSET_BUSI_PROTO C                      --
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.LN_APCL_FLG           = 'N' 
  WHERE A.MARK_FLAG             = '10' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.CRE_BAL_AVG 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-15::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10017                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CRE_BAL               AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(SUM(C.BAL_RMB)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --资产协议表
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CRE_BAL IS 
    NOT NULL 
   LEFT JOIN ACRM_F_CI_ASSET_BUSI_PROTO C                      --
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.LN_APCL_FLG           = 'N' 
  WHERE A.MARK_FLAG             = '10' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.CRE_BAL 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-16::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10018                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.DEBIT_CARD            AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(COUNT(C.CR_CRD_NO)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --借记卡卡信息表
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.DEBIT_CARD IS 
    NOT NULL 
   LEFT JOIN OCRM_F_DP_CARD_INFO C                             --
     ON A.CUST_ID               = C.CR_CUST_NO 
    AND A.FR_ID                 = C.FR_ID 
  WHERE A.MARK_FLAG             = '3' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.DEBIT_CARD 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-17::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10019                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CREDIT_CARD           AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(COUNT(C.CARD_NO)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --客户信用信息
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CREDIT_CARD IS 
    NOT NULL 
   LEFT JOIN OCRM_F_CI_CUSTLNAINFO C                           --
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
  WHERE A.MARK_FLAG             = '4' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.CREDIT_CARD 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-18::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10020                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.FIN_BAL_AVG           AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(SUM(C.MONTH_AVG)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --理财协议表
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.FIN_BAL_AVG IS 
    NOT NULL 
   LEFT JOIN ACRM_F_NI_FINANCING C                             --
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.END_DATE > V_DT 
  WHERE A.MARK_FLAG             = '5' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.FIN_BAL_AVG 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-19::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(B.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,B.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(10021                   AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.FIN_BAL               AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(SUM(C.CURRE_AMOUNT)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM TMP_CUST_MARK A                                        --营销任务客户清单临时标记表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B               --理财协议表
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.FIN_BAL IS 
    NOT NULL 
   LEFT JOIN ACRM_F_NI_FINANCING C                             --
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.END_DATE > V_DT 
  WHERE A.MARK_FLAG             = '5' 
  GROUP BY B.TASK_ID 
       ,B.EXEC_ID 
       ,B.EXECUTOR_ID 
       ,B.FIN_BAL 
       ,B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-20::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(''                      AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.ID                    AS DECIMAL(27))                       AS EXEC_ID 
       ,''                      AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,''                      AS EXECUTOR_NAME 
       ,CAST(B.NORM_ID               AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(0                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
   LEFT JOIN OCRM_F_MARKET_TASK_TARGET B                       --营销任务指标分解表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_MARKET_TASK C                              --营销任务主表
     ON A.TASK_ID               = C.ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.TASK_BEGIN_DATE       = V_DT 
   LEFT JOIN OCRM_F_MARKET_TASK_CUST D                         --营销子任务关联客户表
     ON A.ID                    = D.EXEC_ID 
    AND A.FR_ID                 = D.FR_ID 
    AND D.CUST_STAT             = '2' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
  GROUP BY A.TASK_ID 
       ,A.ID 
       ,A.EXECUTOR_ID 
       ,B.NORM_ID 
       ,B.TASK_TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-21::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(A.ID                    AS DECIMAL(27))                       AS EXEC_ID 
       ,CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.NORM_ID  AS VARCHAR(20))             AS TASK_TARGET_ID 
       ,CAST(B.TASK_TARGET_VALUE     AS DECIMAL(22, 2))                       AS TASK_TARGET_VALUE 
       ,A.EXECUTOR_ID           AS CUSTMGR_ID 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
   LEFT JOIN OCRM_F_MARKET_TASK_TARGET B                       --营销任务指标分解表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_MARKET_TASK C                              --营销任务主表
     ON A.TASK_ID               = C.ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.TASK_TYPE             = '2' 
    AND C.TASK_BEGIN_DATE       = V_DT 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.EXECUTE_STATUS        = '1' 
  GROUP BY A.ID 
       ,A.TASK_ID 
       ,A.PARENT_EXEC_ID 
       ,B.NORM_ID 
       ,B.TASK_TARGET_VALUE 
       ,A.EXECUTOR_ID 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUSTMGR_TASK = sqlContext.sql(sql)
TMP_CUSTMGR_TASK.registerTempTable("TMP_CUSTMGR_TASK")
dfn="TMP_CUSTMGR_TASK/"+V_DT+".parquet"
TMP_CUSTMGR_TASK.cache()
nrows = TMP_CUSTMGR_TASK.count()
TMP_CUSTMGR_TASK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUSTMGR_TASK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUSTMGR_TASK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUSTMGR_TASK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-22::
V_STEP = V_STEP + 1
TMP_CUSTMGR_TASK = sqlContext.read.parquet(hdfs+'/TMP_CUSTMGR_TASK/*')
TMP_CUSTMGR_TASK.registerTempTable("TMP_CUSTMGR_TASK")
sql = """
 SELECT CAST(EXEC_ID                 AS DECIMAL(27))                       AS EXEC_ID 
       ,CAST(TASK_ID                 AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(PARENT_EXEC_ID          AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CUSTMGR_ID              AS EXECUTOR_ID 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10005' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS SAVE_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10006' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10007' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS DEB_CARD_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10008' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_CARD_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10009' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS FIN_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10010' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS IBK_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10011' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS MBK_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10012' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS WBK_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10013' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS MID_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10014' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS SAVE_BAL_AVG 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10015' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS SAVE_BAL 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10016' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_BAL_AVG 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10017' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_BAL 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10018' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS DEBIT_CARD 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10019' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CREDIT_CARD 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10020' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS FIN_BAL_AVG 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10021' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS FIN_BAL 
       ,FR_ID                   AS FR_ID 
   FROM TMP_CUSTMGR_TASK A                                     --营销任务客户经理任务临时表
  GROUP BY EXEC_ID 
       ,TASK_ID 
       ,PARENT_EXEC_ID 
       ,CUSTMGR_ID 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_BEFORE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_BEFORE")
dfn="OCRM_F_MACKET_TASK_ANALYSE_BEFORE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE_BEFORE.count()
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_MACKET_TASK_ANALYSE_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-23::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
OCRM_F_MACKET_TASK_ANALYSE_BEFORE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE_BEFORE/*')
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_BEFORE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CURREN_VALUE   AS DECIMAL(20, 2))      AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
   SELECT     CAST (COUNT(1) * B.SAVE_ACCOUNT AS DECIMAL(10) ) AS CURREN_VALUE	
             ,A.FR_ID FR_ID
             ,A.EXEC_ID
			 ,B.EXECUTOR_ID EXECUTOR_ID
	           FROM TMP_CUST_MARK_BEFORE A
			   INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B  --营销任务客户分类临时表
			ON A.FR_ID=B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.SAVE_ACCOUNT IS NOT NULL 
	       WHERE  A.MARK_FLAG = '1'   --存款户
		   GROUP BY A.FR_ID,A.EXEC_ID,B.EXECUTOR_ID ,B.SAVE_ACCOUNT
			)B 
    ON 	A.EXEC_ID               = B.EXEC_ID
   AND 	A.FR_ID                 = B.FR_ID
   AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
   WHERE A.TASK_TARGET_ID        = 10005 
   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-24::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(  B.CURREN_VALUE    AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
  INNER JOIN (
		SELECT  CAST (COUNT(1) * B.CRE_ACCOUNT AS DECIMAL(10) )  AS CURREN_VALUE
				,A.FR_ID
				,A.EXEC_ID
				,B.EXECUTOR_ID
	            FROM TMP_CUST_MARK_BEFORE A
				INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B 
		    ON A.FR_ID = B.FR_ID AND A.EXEC_ID=B.EXEC_ID AND B.CRE_ACCOUNT IS NOT NULL
				WHERE   A.MARK_FLAG = '2'   --授信客户
			GROUP BY A.FR_ID
				,A.EXEC_ID
				,B.EXECUTOR_ID
				,B.CRE_ACCOUNT
		)B 
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10006 
   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-25::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CURREN_VALUE                 AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
		SELECT CAST (COUNT(1) * B.DEB_CARD_ACCOUNT AS DECIMAL(10) )	  AS CURREN_VALUE	 
				,A.FR_ID
				,A.EXEC_ID
				,B.EXECUTOR_ID
	            FROM TMP_CUST_MARK_BEFORE A
				INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B 
		ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.DEB_CARD_ACCOUNT IS  NOT NULL 
				WHERE A.MARK_FLAG = '3'   --借记卡客户
			GROUP BY A.FR_ID
				,A.EXEC_ID
				,B.EXECUTOR_ID
				,B.DEB_CARD_ACCOUNT
		)B
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10007  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")

sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-26::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CURREN_VALUE                      AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (SELECT  CAST (COUNT(1) * B.CRE_CARD_ACCOUNT AS DECIMAL(10) )   AS    CURREN_VALUE     
				,A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
	            FROM TMP_CUST_MARK_BEFORE A
				INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B 
				ON A.FR_ID = B.FR_ID  AND A.EXEC_ID = B.EXEC_ID  AND B.CRE_CARD_ACCOUNT IS NOT NULL
				WHERE  A.MARK_FLAG = '4'   --贷记卡客户
				GROUP BY A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
				, B.CRE_CARD_ACCOUNT
	         )B
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10008 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")

sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-27::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CURREN_VALUE                   AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
    INNER JOIN (SELECT  CAST (COUNT(1) * B.FIN_ACCOUNT AS DECIMAL(10) )   AS    CURREN_VALUE     
				,A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
	            FROM TMP_CUST_MARK_BEFORE A
				INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B 
				ON A.FR_ID = B.FR_ID  AND A.EXEC_ID = B.EXEC_ID  AND B.FIN_ACCOUNT IS NOT NULL
				WHERE  A.MARK_FLAG = '5'   --理财客户
				GROUP BY A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
				,B.FIN_ACCOUNT
	         )B
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
   WHERE A.TASK_TARGET_ID        = 10009 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")

sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-28::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CURREN_VALUE            AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (SELECT  CAST (COUNT(1) * B.IBK_ACCOUNT AS DECIMAL(10) )   AS    CURREN_VALUE     
				,A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
	            FROM TMP_CUST_MARK_BEFORE A
				INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B 
				ON A.FR_ID = B.FR_ID  AND A.EXEC_ID = B.EXEC_ID  AND B.IBK_ACCOUNT IS NOT NULL
				WHERE  A.MARK_FLAG = '6'   --网银客户
				GROUP BY A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
				,B.IBK_ACCOUNT
	         )B
		ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID
  WHERE A.TASK_TARGET_ID        = 10010 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-29::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CURREN_VALUE                 AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (SELECT  CAST (COUNT(1) * B.MBK_ACCOUNT AS DECIMAL(10) )   AS    CURREN_VALUE     
				,A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
	            FROM TMP_CUST_MARK_BEFORE A
				INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B 
				ON A.FR_ID = B.FR_ID  AND A.EXEC_ID = B.EXEC_ID  AND B.MBK_ACCOUNT IS NOT NULL
				WHERE  A.MARK_FLAG = '7'   --手机银行客户
				GROUP BY A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
				,B.MBK_ACCOUNT
	         )B
		ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID
  WHERE A.TASK_TARGET_ID  = 10011 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-30::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CURREN_VALUE           AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
    INNER JOIN (SELECT  CAST (COUNT(1) * B.WBK_ACCOUNT AS DECIMAL(10) )   AS    CURREN_VALUE     
				,A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
	            FROM TMP_CUST_MARK_BEFORE A
				INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B 
				ON A.FR_ID = B.FR_ID  AND A.EXEC_ID = B.EXEC_ID  AND B.WBK_ACCOUNT IS NOT NULL
				WHERE  A.MARK_FLAG = '8'   --微信银行客户
				GROUP BY A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
				,B.WBK_ACCOUNT
	         )B
		ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID
  WHERE A.TASK_TARGET_ID        = 10012 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-31::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CURREN_VALUE         AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (SELECT  CAST (COUNT(1) * B.MID_ACCOUNT AS DECIMAL(10) )   AS    CURREN_VALUE     
				,A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
	            FROM TMP_CUST_MARK_BEFORE A
				INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B 
				ON A.FR_ID = B.FR_ID  AND A.EXEC_ID = B.EXEC_ID  AND B.MID_ACCOUNT IS NOT NULL
				WHERE  A.MARK_FLAG = '9'   --中间业务客户
				GROUP BY A.FR_ID 
				,A.EXEC_ID
				,B.EXECUTOR_ID
				,B.MID_ACCOUNT
	         )B
		ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID
  WHERE A.TASK_TARGET_ID        = 10013 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-32::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.DEP_MONTH_AVG    AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN 
   (SELECT  SUM(B.MONTH_AVG) * C.SAVE_BAL_AVG AS DEP_MONTH_AVG
			,A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
		FROM  TMP_CUST_MARK_BEFORE A     --负债协议表
		LEFT  JOIN  ACRM_F_DP_SAVE_INFO B ON A.CUST_ID = B.CUST_ID AND B.FR_ID = A.FR_ID
		INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE C   --营销任务客户分类临时表
		ON A.FR_ID = C.FR_ID AND A.EXEC_ID = C.EXEC_ID AND C.SAVE_BAL_AVG IS  NOT NULL 
		WHERE A.MARK_FLAG = '1'
		AND  B.ACCT_STATUS = '01'
		GROUP BY A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			,C.SAVE_BAL_AVG
	)B
     ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10014 
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-33::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.DEP_MONTH_AVG     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN 
   (SELECT  SUM(B.BAL_RMB) * C.SAVE_BAL AS DEP_MONTH_AVG
			,A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
		FROM  TMP_CUST_MARK_BEFORE A     --负债协议表
		LEFT  JOIN  ACRM_F_DP_SAVE_INFO B ON A.CUST_ID = B.CUST_ID AND B.FR_ID = A.FR_ID
		INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE C   --营销任务客户分类临时表
		ON A.FR_ID = C.FR_ID AND A.EXEC_ID = C.EXEC_ID AND C.SAVE_BAL IS  NOT NULL 
		WHERE A.MARK_FLAG = '1'
		AND  B.ACCT_STATUS = '01'
		GROUP BY A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			,C.SAVE_BAL 
	)B
   ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10015 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-34::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CRE_MONTH_AVG    AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN 
   (SELECT SUM(B.MONTH_AVG) * C.CRE_BAL_AVG  AS CRE_MONTH_AVG \
			,A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			FROM TMP_CUST_MARK_BEFORE A
			LEFT  JOIN  ACRM_F_CI_ASSET_BUSI_PROTO B ON A.CUST_ID = B.CUST_ID AND B.FR_ID = A.FR_ID
			INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE C ON A.FR_ID = B.FR_ID AND A.EXEC_ID = C.EXEC_ID AND C.CRE_BAL_AVG IS NOT NULL
			WHERE A.MARK_FLAG = '10'    --贷款户
			AND  B.LN_APCL_FLG = 'N'
		GROUP BY A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			,C.CRE_BAL_AVG 
   )B
	ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10016 
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-35::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CRE_BAL       AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER  JOIN 
	(
		 SELECT  SUM(BAL_RMB) * C.CRE_BAL  AS CRE_BAL  
			,A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			FROM TMP_CUST_MARK_BEFORE A
			LEFT  JOIN  ACRM_F_CI_ASSET_BUSI_PROTO B ON A.CUST_ID = B.CUST_ID AND B.FR_ID = A.FR_ID
			INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE C ON A.FR_ID = C.FR_ID AND A.EXEC_ID = C.EXEC_ID AND C.CRE_BAL IS NOT NULL
			WHERE  A.MARK_FLAG = '10'    --贷款户
			AND  B.LN_APCL_FLG = 'N'  
			GROUP BY A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			,C.CRE_BAL 
	)B
   ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10017 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-36::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CR_CRD_NUM AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER  JOIN 
	(
		 SELECT   CAST(COUNT(B.CR_CRD_NO) * C.DEBIT_CARD AS DECIMAL(10)) AS CR_CRD_NUM 
			,A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			FROM TMP_CUST_MARK_BEFORE A
			LEFT  JOIN  OCRM_F_DP_CARD_INFO B ON A.CUST_ID = B.CR_CUST_NO AND B.FR_ID = A.FR_ID
			INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE C ON A.FR_ID = C.FR_ID AND A.EXEC_ID = C.EXEC_ID AND C.DEBIT_CARD IS NOT NULL
			WHERE  A.MARK_FLAG = '3'  --借记卡户
			GROUP BY A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			,C.DEBIT_CARD 
	)B
   ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID
  WHERE A.TASK_TARGET_ID        = 10018 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-37::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.CREDIT_CARD_NUM         AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER  JOIN 
	(
		 SELECT   CAST (COUNT(B.CARD_NO) * C.CREDIT_CARD AS DECIMAL(10) ) AS 	   CREDIT_CARD_NUM 
			,A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			FROM TMP_CUST_MARK_BEFORE A
			LEFT  JOIN  OCRM_F_CI_CUSTLNAINFO B ON A.CUST_ID = B.CUST_ID AND B.FR_ID = A.FR_ID
			INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE C ON A.FR_ID = C.FR_ID AND A.EXEC_ID = C.EXEC_ID AND C.CREDIT_CARD IS NOT NULL
			WHERE  A.MARK_FLAG = '4'    --贷记卡户
			GROUP BY A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			,C.CREDIT_CARD 
	)B
   ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID
  WHERE A.TASK_TARGET_ID        = 10019 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-38::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.MONTH_AVG         AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
   SELECT  CAST(SUM(MONTH_AVG)	*  C.FIN_BAL_AVG AS DECIMAL(10)) as MONTH_AVG 
			,A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			FROM TMP_CUST_MARK_BEFORE A
			LEFT  JOIN  ACRM_F_NI_FINANCING B ON A.CUST_ID = B.CUST_ID AND B.FR_ID = A.FR_ID
			INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE C ON A.FR_ID = C.FR_ID AND A.EXEC_ID = C.EXEC_ID AND C.FIN_BAL_AVG IS NOT NULL
			WHERE A.MARK_FLAG = '5'    --理财户
			AND B.END_DATE > V_DT
			GROUP BY A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			,C.FIN_BAL_AVG
   )B
   ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10020 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-39::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(B.MONTH_AVG         AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
   SELECT  CAST(SUM(MONTH_AVG)	*  C.FIN_BAL AS DECIMAL(10)) as MONTH_AVG 
			,A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			FROM TMP_CUST_MARK_BEFORE A
			LEFT  JOIN  ACRM_F_NI_FINANCING B ON A.CUST_ID = B.CUST_ID AND B.FR_ID = A.FR_ID
			INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE C ON A.FR_ID = C.FR_ID AND A.EXEC_ID = C.EXEC_ID AND C.FIN_BAL IS NOT NULL
			WHERE A.MARK_FLAG = '5'    --理财户
			AND B.END_DATE > V_DT
			GROUP BY A.FR_ID
			,A.EXEC_ID
			,C.EXECUTOR_ID
			, C.FIN_BAL 
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10021 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[21] 001-40::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(A.ID                    AS DECIMAL(27))                       AS EXEC_ID 
       ,CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(B.NORM_ID   AS VARCHAR(20))            AS TASK_TARGET_ID 
       ,CAST(B.TASK_TARGET_VALUE     AS DECIMAL(22, 2))                       AS TASK_TARGET_VALUE 
       ,A.EXECUTOR_ID           AS CUSTMGR_ID 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
   LEFT JOIN OCRM_F_MARKET_TASK_TARGET B                       --营销任务指标分解表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_MARKET_TASK C                              --营销任务主表
     ON A.TASK_ID               = C.ID 
    AND A.FR_ID                 = C.FR_ID 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT 
  GROUP BY A.ID 
       ,A.TASK_ID 
       ,A.PARENT_EXEC_ID 
       ,B.NORM_ID 
       ,B.TASK_TARGET_VALUE 
       ,A.EXECUTOR_ID 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUSTMGR_TASK = sqlContext.sql(sql)
TMP_CUSTMGR_TASK.registerTempTable("TMP_CUSTMGR_TASK")
dfn="TMP_CUSTMGR_TASK/"+V_DT+".parquet"
TMP_CUSTMGR_TASK.cache()
nrows = TMP_CUSTMGR_TASK.count()
TMP_CUSTMGR_TASK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUSTMGR_TASK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUSTMGR_TASK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUSTMGR_TASK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-41::
V_STEP = V_STEP + 1
TMP_CUSTMGR_TASK = sqlContext.read.parquet(hdfs+'/TMP_CUSTMGR_TASK/*')
TMP_CUSTMGR_TASK.registerTempTable("TMP_CUSTMGR_TASK")
sql = """
 SELECT CAST(EXEC_ID                 AS DECIMAL(27))                       AS EXEC_ID 
       ,CAST(TASK_ID                 AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(PARENT_EXEC_ID          AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CUSTMGR_ID              AS EXECUTOR_ID 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10005' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS SAVE_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10006' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10007' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS DEB_CARD_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10008' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_CARD_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10009' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS FIN_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10010' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS IBK_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10011' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS MBK_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10012' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS WBK_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10013' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS MID_ACCOUNT 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10014' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS SAVE_BAL_AVG 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10015' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS SAVE_BAL 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10016' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_BAL_AVG 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10017' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CRE_BAL 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10018' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS DEBIT_CARD 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10019' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS CREDIT_CARD 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10020' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS FIN_BAL_AVG 
       ,CAST(MIN(CASE TASK_TARGET_ID WHEN '10021' THEN TASK_TARGET_VALUE ELSE NULL END)                       AS DECIMAL(22, 2))                       AS FIN_BAL 
       ,FR_ID                   AS FR_ID 
   FROM TMP_CUSTMGR_TASK A                                     --营销任务客户经理任务临时表
  GROUP BY EXEC_ID 
       ,TASK_ID 
       ,PARENT_EXEC_ID 
       ,CUSTMGR_ID 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_BEFORE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_BEFORE")
dfn="OCRM_F_MACKET_TASK_ANALYSE_BEFORE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE_BEFORE.count()
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_MACKET_TASK_ANALYSE_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-42::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE_BEFORE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE_BEFORE/*')
OCRM_F_MACKET_TASK_ANALYSE_BEFORE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_BEFORE")
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CURREN_VALUE    AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT COUNT(1) AS  CURREN_VALUE	 
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.SAVE_ACCOUNT IS NOT NULL
	 WHERE A.MARK_FLAG = '1'   --存款户
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
  ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10005 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-43::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CURREN_VALUE  AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT COUNT(1) AS  CURREN_VALUE	 
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.CRE_ACCOUNT IS NOT NULL
	 WHERE A.MARK_FLAG = '2'   --授信客户
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10006 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-44::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CURREN_VALUE AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT COUNT(1) AS  CURREN_VALUE	 
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.DEB_CARD_ACCOUNT IS NOT NULL
	 WHERE A.MARK_FLAG = '3'   --借记卡客户
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
   ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10007 
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-45::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CURREN_VALUE        AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT COUNT(1) AS  CURREN_VALUE	 
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.CRE_CARD_ACCOUNT IS NOT NULL
	 WHERE A.MARK_FLAG = '4'   --贷记卡客户
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
    WHERE A.TASK_TARGET_ID        = 10008 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-46::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CURREN_VALUE    AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT COUNT(1) AS  CURREN_VALUE	 
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.FIN_ACCOUNT IS NOT NULL
	 WHERE A.MARK_FLAG = '5'   --理财客户
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10009 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-47::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CURREN_VALUE       AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT COUNT(1) AS  CURREN_VALUE	 
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.IBK_ACCOUNT IS NOT NULL
	 WHERE A.MARK_FLAG = '6'   --网银客户
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10010 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-48::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CURREN_VALUE    AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT COUNT(1) AS  CURREN_VALUE	 
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.MBK_ACCOUNT IS NOT NULL
	 WHERE A.MARK_FLAG = '7'   --手机银行客户
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10011 
  """
  
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-49::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CURREN_VALUE       AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT COUNT(1) AS  CURREN_VALUE	 
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.WBK_ACCOUNT IS NOT NULL
	 WHERE A.MARK_FLAG = '8'   --微信银行客户
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10012 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-50::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CURREN_VALUE   AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT COUNT(1) AS  CURREN_VALUE	 
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.MID_ACCOUNT IS NOT NULL
	 WHERE A.MARK_FLAG = '9'   --中间业务客户
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10013 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-51::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.DEP_MONTH_AVG            AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT SUM(C.MONTH_AVG)  AS DEP_MONTH_AVG 
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.SAVE_BAL_AVG IS NOT NULL
	 LEFT  JOIN  ACRM_F_DP_SAVE_INFO C ON A.CUST_ID = C.CUST_ID AND A.FR_ID = C.FR_ID
	 WHERE A.MARK_FLAG = '1'   --中间业务客户
		AND C.ACCT_STATUS = '01'
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10014 
   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-52::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.DEP_BAL        AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT SUM(C.BAL_RMB)  AS DEP_BAL 
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.SAVE_BAL IS NOT NULL
	 LEFT  JOIN  ACRM_F_DP_SAVE_INFO C ON A.CUST_ID = C.CUST_ID AND A.FR_ID = C.FR_ID
	 WHERE A.MARK_FLAG = '1'   --中间业务客户
		AND C.ACCT_STATUS = '01'
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10015 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-53::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CRE_MONTH_AVG     AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT SUM(C.MONTH_AVG)  AS CRE_MONTH_AVG
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.CRE_BAL_AVG IS NOT NULL
	 LEFT  JOIN  ACRM_F_CI_ASSET_BUSI_PROTO C ON A.CUST_ID = C.CUST_ID AND A.FR_ID = C.FR_ID
	 WHERE A.MARK_FLAG = '10'   --贷款户
		AND C.LN_APCL_FLG           = 'N' 
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10016 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-54::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CRE_BAL   AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT SUM(C.BAL_RMB)  AS CRE_BAL
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.CRE_BAL IS NOT NULL
	 LEFT  JOIN  ACRM_F_CI_ASSET_BUSI_PROTO C ON A.CUST_ID = C.CUST_ID AND A.FR_ID = C.FR_ID
	 WHERE A.MARK_FLAG = '10'   --贷款户
		AND C.LN_APCL_FLG           = 'N' 
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10017 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-55::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CR_CRD_NUM       AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT COUNT(C.CR_CRD_NO) AS CR_CRD_NUM
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.DEBIT_CARD IS NOT NULL
	 LEFT  JOIN  OCRM_F_DP_CARD_INFO C ON A.CUST_ID = C.CR_CUST_NO AND A.FR_ID = C.FR_ID
	 WHERE A.MARK_FLAG = '3'     --借记卡户
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10018
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-56::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CREDIT_CARD_NUM  AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
    INNER JOIN (
     SELECT COUNT(C.CARD_NO) AS 	   CREDIT_CARD_NUM
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.CREDIT_CARD IS NOT NULL
	 LEFT  JOIN  OCRM_F_CI_CUSTLNAINFO C ON A.CUST_ID = C.CUST_ID AND A.FR_ID = C.FR_ID
	 WHERE A.MARK_FLAG = '4'    --贷记卡户
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10019
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-57::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.MONTH_AVG          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
   INNER JOIN (
     SELECT SUM(C.MONTH_AVG)	as MONTH_AVG
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.FIN_BAL_AVG IS NOT NULL
	 LEFT  JOIN  ACRM_F_NI_FINANCING C ON A.CUST_ID = C.CUST_ID AND A.FR_ID = C.FR_ID
	 WHERE A.MARK_FLAG = '5'    --理财户
		AND C.END_DATE > V_DT 
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10020
  """


sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-58::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(B.CURRE_AMOUNT      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
    INNER JOIN (
     SELECT SUM(C.CURRE_AMOUNT) AS CURRE_AMOUNT	  
		,A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
	 FROM TMP_CUST_MARK A
	 INNER JOIN OCRM_F_MACKET_TASK_ANALYSE_BEFORE B ON A.FR_ID = B.FR_ID AND A.EXEC_ID = B.EXEC_ID AND B.FIN_BAL IS NOT NULL
	 LEFT  JOIN  ACRM_F_NI_FINANCING C ON A.CUST_ID = C.CUST_ID AND A.FR_ID = C.FR_ID
	 WHERE A.MARK_FLAG = '5'    --理财户
		AND C.END_DATE > V_DT 
	 GROUP BY A.FR_ID
		,A.EXEC_ID
		,B.EXECUTOR_ID
   )B
    ON A.EXEC_ID               = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.EXECUTOR_ID           = B.EXECUTOR_ID 
  WHERE A.TASK_TARGET_ID        = 10021
   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-59::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(B.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,B.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,B.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(A.TASK_TARGET_VALUE     AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(A.ORIGINAL_VALUE        AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(A.CURREN_VALUE          AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
  INNER JOIN OCRM_F_MARKET_TASK_EXECUTOR B                     --营销任务汇总表
     ON A.EXEC_ID               = B.ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[11] 001-60::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.ID                    AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(B.NORM_ID               AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(''                      AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
   LEFT JOIN OCRM_F_MARKET_TASK_TARGET B                       --营销任务指标分解表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT 
    AND A.EXECUTOR_TYPE         = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE.cache()
nrows = OCRM_F_MACKET_TASK_ANALYSE.count()
OCRM_F_MACKET_TASK_ANALYSE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_MACKET_TASK_ANALYSE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-61::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(A.TASK_ID               AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(A.EXEC_ID               AS DECIMAL(27))                       AS EXEC_ID 
       ,A.EXECUTOR_TYPE         AS EXECUTOR_TYPE 
       ,A.EXECUTOR_ID           AS EXECUTOR_ID 
       ,A.EXECUTOR_NAME         AS EXECUTOR_NAME 
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(SUM(B.TASK_TARGET_VALUE)                       AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(SUM(B.ORIGINAL_VALUE)                       AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(SUM(B.CURREN_VALUE)                       AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
  INNER JOIN OCRM_F_MACKET_TASK_ANALYSE B                      --营销任务统计表
     ON A.EXEC_ID               = B.PARENT_EXEC_ID 
    AND A.TASK_TARGET_ID        = B.TASK_TARGET_ID 
    AND A.FR_ID                 = B.FR_ID 
  GROUP BY CAST(A.TASK_ID               AS DECIMAL(27))  
       ,CAST(A.PARENT_EXEC_ID        AS DECIMAL(27))           
       ,CAST(A.EXEC_ID               AS DECIMAL(27))    
       ,A.EXECUTOR_TYPE      
       ,A.EXECUTOR_ID        
       ,A.EXECUTOR_NAME         
       ,CAST(A.TASK_TARGET_ID        AS DECIMAL(27))            
       ,CAST(A.ACHIEVE_VALUE         AS DECIMAL(20, 2))     
       ,CAST(A.ACHIEVE_PERCENT       AS DECIMAL(20, 2))  
       ,A.ETL_DATE        
       ,A.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.TASK_TARGET_ID      = DST.TASK_TARGET_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-62::
V_STEP = V_STEP + 1
OCRM_F_MACKET_TASK_ANALYSE = sqlContext.read.parquet(hdfs+'/OCRM_F_MACKET_TASK_ANALYSE/*')
OCRM_F_MACKET_TASK_ANALYSE.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE")
sql = """
 SELECT CAST(TASK_ID                 AS DECIMAL(27))                       AS TASK_ID 
       ,CAST(PARENT_EXEC_ID          AS DECIMAL(27))                       AS PARENT_EXEC_ID 
       ,CAST(EXEC_ID                 AS DECIMAL(27))                       AS EXEC_ID 
       ,EXECUTOR_TYPE           AS EXECUTOR_TYPE 
       ,EXECUTOR_ID             AS EXECUTOR_ID 
       ,EXECUTOR_NAME           AS EXECUTOR_NAME 
       ,CAST(TASK_TARGET_ID          AS DECIMAL(27))                       AS TASK_TARGET_ID 
       ,CAST(TASK_TARGET_VALUE       AS DECIMAL(20, 2))                       AS TASK_TARGET_VALUE 
       ,CAST(ORIGINAL_VALUE          AS DECIMAL(20, 2))                       AS ORIGINAL_VALUE 
       ,CAST(CURREN_VALUE            AS DECIMAL(20, 2))                       AS CURREN_VALUE 
       ,CAST(CURREN_VALUE - ORIGINAL_VALUE          AS DECIMAL(20, 2))                       AS ACHIEVE_VALUE 
       ,CAST((CASE TASK_TARGET_VALUE WHEN 0 THEN 1 ELSE(CURREN_VALUE - ORIGINAL_VALUE) / TASK_TARGET_VALUE END)                       AS DECIMAL(20, 2))                       AS ACHIEVE_PERCENT 
       ,ETL_DATE                AS ETL_DATE 
       ,FR_ID                   AS FR_ID 
   FROM OCRM_F_MACKET_TASK_ANALYSE A                           --营销任务统计表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.registerTempTable("OCRM_F_MACKET_TASK_ANALYSE_INNTMP1")


sql = """
 SELECT DST.TASK_ID                                             --总行任务编号:src.TASK_ID
       ,DST.PARENT_EXEC_ID                                     --上级任务编号:src.PARENT_EXEC_ID
       ,DST.EXEC_ID                                            --当前任务编号:src.EXEC_ID
       ,DST.EXECUTOR_TYPE                                      --执行者类别:src.EXECUTOR_TYPE
       ,DST.EXECUTOR_ID                                        --执行者编号:src.EXECUTOR_ID
       ,DST.EXECUTOR_NAME                                      --执行者名称:src.EXECUTOR_NAME
       ,DST.TASK_TARGET_ID                                     --任务指标代码:src.TASK_TARGET_ID
       ,DST.TASK_TARGET_VALUE                                  --任务指标值:src.TASK_TARGET_VALUE
       ,DST.ORIGINAL_VALUE                                     --指标初始值:src.ORIGINAL_VALUE
       ,DST.CURREN_VALUE                                       --指标当前值:src.CURREN_VALUE
       ,DST.ACHIEVE_VALUE                                      --指标完成值:src.ACHIEVE_VALUE
       ,DST.ACHIEVE_PERCENT                                    --指标完成率:src.ACHIEVE_PERCENT
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_MACKET_TASK_ANALYSE DST 
   LEFT JOIN OCRM_F_MACKET_TASK_ANALYSE_INNTMP1 SRC 
     ON SRC.EXEC_ID             = DST.EXEC_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.TASK_TARGET_ID      = DST.TASK_TARGET_ID 
    AND SRC.EXECUTOR_ID         = DST.EXECUTOR_ID 
  WHERE SRC.EXEC_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet"
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2=OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unionAll(OCRM_F_MACKET_TASK_ANALYSE_INNTMP1)
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.cache()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.cache()
nrowsi = OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.count()
nrowsa = OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.count()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_MACKET_TASK_ANALYSE_INNTMP1.unpersist()
OCRM_F_MACKET_TASK_ANALYSE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_MACKET_TASK_ANALYSE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)

#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_MACKET_TASK_ANALYSE_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_MACKET_TASK_ANALYSE/"+V_DT+".parquet /"+dbname+"/OCRM_F_MACKET_TASK_ANALYSE_BK/"+V_DT+".parquet")
