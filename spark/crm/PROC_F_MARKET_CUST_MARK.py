#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_MARKET_CUST_MARK').setMaster(sys.argv[2])
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
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
OCRM_F_MARKET_TASK_EXECUTOR = sqlContext.read.parquet(hdfs+'/OCRM_F_MARKET_TASK_EXECUTOR/*')
OCRM_F_MARKET_TASK_EXECUTOR.registerTempTable("OCRM_F_MARKET_TASK_EXECUTOR")
OCRM_F_MARKET_TASK_CUST = sqlContext.read.parquet(hdfs+'/OCRM_F_MARKET_TASK_CUST/*')
OCRM_F_MARKET_TASK_CUST.registerTempTable("OCRM_F_MARKET_TASK_CUST")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
ACRM_F_NI_COLL_PAY_INSURANCE = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_COLL_PAY_INSURANCE/*')
ACRM_F_NI_COLL_PAY_INSURANCE.registerTempTable("ACRM_F_NI_COLL_PAY_INSURANCE")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'1'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST B                         --营销子任务关联客户表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_STAT             = '1' 
  INNER JOIN ACRM_F_DP_SAVE_INFO C                             --负债协议表
     ON B.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.ACCT_STATUS           = '01' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'2'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST B                         --营销子任务关联客户表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_STAT             = '1' 
  INNER JOIN OCRM_F_CI_COM_CUST_INFO C                         --对公客户信息表
     ON B.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.IF_CREDIT_CUST        = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'3'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST B                         --营销子任务关联客户表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_STAT             = '1' 
  INNER JOIN OCRM_F_DP_CARD_INFO C                             --卡档
     ON B.CUST_ID               = C.CR_CUST_NO 
    AND A.FR_ID                 = C.FR_ID 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'4'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST B                         --营销子任务关联客户表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_STAT             = '1' 
  INNER JOIN OCRM_F_CI_CUST_DESC C                             --统一客户信息表
     ON B.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND SUBSTR(C.ODS_SYS_ID, 6, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'5'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST B                         --营销子任务关联客户表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_STAT             = '1' 
  INNER JOIN OCRM_F_CI_CUST_DESC C                             --统一客户信息表
     ON B.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND SUBSTR(C.ODS_SYS_ID, 10, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-06::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'6'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST B                         --营销子任务关联客户表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_STAT             = '1' 
  INNER JOIN OCRM_F_CI_CUST_DESC C                             --统一客户信息表
     ON B.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND SUBSTR(C.ODS_SYS_ID, 3, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-07::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'7'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST B                         --营销子任务关联客户表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_STAT             = '1' 
  INNER JOIN OCRM_F_CI_CUST_DESC C                             --统一客户信息表
     ON B.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND SUBSTR(C.ODS_SYS_ID, 11, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-08::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'9'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST B                         --营销子任务关联客户表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_STAT             = '1' 
  INNER JOIN ACRM_F_NI_COLL_PAY_INSURANCE C                    --代收代付/保险协议表
     ON B.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-09::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'10'                    AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST B                         --营销子任务关联客户表
     ON A.ID                    = B.EXEC_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_STAT             = '1' 
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO C                      --资产协议
     ON B.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.LN_APCL_FLG           = 'N' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-10::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,N.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'1'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST M                         --营销子任务关联客户表
     ON A.ID                    = M.EXEC_ID 
    AND M.FR_ID                 = A.FR_ID 
    AND M.CUST_STAT             = '2' 
  INNER JOIN OCRM_F_CI_CUST_DESC N                             --统一客户信息表
     ON M.CUST_NAME             = N.CUST_ZH_NAME 
    AND M.CERT_TYPE             = N.CERT_TYPE 
    AND M.CERT_NUM              = N.CERT_NUM 
    AND N.FR_ID                 = M.FR_ID 
  INNER JOIN ACRM_F_DP_SAVE_INFO C                             --负债协议表
     ON N.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.ACCT_STATUS           = '01' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-11::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,C.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'2'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST M                         --营销子任务关联客户表
     ON A.ID                    = M.EXEC_ID 
    AND M.FR_ID                 = A.FR_ID 
    AND M.CUST_STAT             = '2' 
  INNER JOIN OCRM_F_CI_CUST_DESC N                             --统一客户信息表
     ON M.CUST_NAME             = N.CUST_ZH_NAME 
    AND M.CERT_TYPE             = N.CERT_TYPE 
    AND M.CERT_NUM              = N.CERT_NUM 
    AND N.FR_ID                 = M.FR_ID 
  INNER JOIN OCRM_F_CI_COM_CUST_INFO C                         --对公客户信息表
     ON N.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.IF_CREDIT_CUST        = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-12::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,N.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'3'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST M                         --营销子任务关联客户表
     ON A.ID                    = M.EXEC_ID 
    AND M.FR_ID                 = A.FR_ID 
    AND M.CUST_STAT             = '2' 
  INNER JOIN OCRM_F_CI_CUST_DESC N                             --统一客户信息表
     ON M.CUST_NAME             = N.CUST_ZH_NAME 
    AND M.CERT_TYPE             = N.CERT_TYPE 
    AND M.CERT_NUM              = N.CERT_NUM 
    AND N.FR_ID                 = M.FR_ID 
  INNER JOIN OCRM_F_DP_CARD_INFO C                             --卡档
     ON N.CUST_ID               = C.CR_CUST_NO 
    AND A.FR_ID                 = C.FR_ID 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-13::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,N.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'4'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST M                         --营销子任务关联客户表
     ON A.ID                    = M.EXEC_ID 
    AND M.FR_ID                 = A.FR_ID 
    AND M.CUST_STAT             = '2' 
  INNER JOIN OCRM_F_CI_CUST_DESC N                             --统一客户信息表
     ON M.CUST_NAME             = N.CUST_ZH_NAME 
    AND M.CERT_TYPE             = N.CERT_TYPE 
    AND M.CERT_NUM              = N.CERT_NUM 
    AND N.FR_ID                 = M.FR_ID 
    AND SUBSTR(N.ODS_SYS_ID, 6, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-14::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,N.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'5'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST M                         --营销子任务关联客户表
     ON A.ID                    = M.EXEC_ID 
    AND M.FR_ID                 = A.FR_ID 
    AND M.CUST_STAT             = '2' 
  INNER JOIN OCRM_F_CI_CUST_DESC N                             --统一客户信息表
     ON M.CUST_NAME             = N.CUST_ZH_NAME 
    AND M.CERT_TYPE             = N.CERT_TYPE 
    AND M.CERT_NUM              = N.CERT_NUM 
    AND N.FR_ID                 = M.FR_ID 
    AND SUBSTR(N.ODS_SYS_ID, 10, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-15::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,N.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'6'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST M                         --营销子任务关联客户表
     ON A.ID                    = M.EXEC_ID 
    AND M.FR_ID                 = A.FR_ID 
    AND M.CUST_STAT             = '2' 
  INNER JOIN OCRM_F_CI_CUST_DESC N                             --统一客户信息表
     ON M.CUST_NAME             = N.CUST_ZH_NAME 
    AND M.CERT_TYPE             = N.CERT_TYPE 
    AND M.CERT_NUM              = N.CERT_NUM 
    AND N.FR_ID                 = M.FR_ID 
    AND SUBSTR(N.ODS_SYS_ID, 3, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-16::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,N.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'7'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST M                         --营销子任务关联客户表
     ON A.ID                    = M.EXEC_ID 
    AND M.FR_ID                 = A.FR_ID 
    AND M.CUST_STAT             = '2' 
  INNER JOIN OCRM_F_CI_CUST_DESC N                             --统一客户信息表
     ON M.CUST_NAME             = N.CUST_ZH_NAME 
    AND M.CERT_TYPE             = N.CERT_TYPE 
    AND M.CERT_NUM              = N.CERT_NUM 
    AND N.FR_ID                 = M.FR_ID 
    AND SUBSTR(N.ODS_SYS_ID, 11, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-17::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,N.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'9'                     AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST M                         --营销子任务关联客户表
     ON A.ID                    = M.EXEC_ID 
    AND M.FR_ID                 = A.FR_ID 
    AND M.CUST_STAT             = '2' 
  INNER JOIN OCRM_F_CI_CUST_DESC N                             --统一客户信息表
     ON M.CUST_NAME             = N.CUST_ZH_NAME 
    AND M.CERT_TYPE             = N.CERT_TYPE 
    AND M.CERT_NUM              = N.CERT_NUM 
    AND N.FR_ID                 = M.FR_ID 
    AND SUBSTR(N.ODS_SYS_ID, 3, 1)                       = '1' 
  INNER JOIN ACRM_F_NI_COLL_PAY_INSURANCE C                    --代收代付/保险协议表
     ON N.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-18::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,N.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'10'                    AS MARK_FLAG 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_MARKET_TASK_EXECUTOR A                          --营销任务汇总表
  INNER JOIN OCRM_F_MARKET_TASK_CUST M                         --营销子任务关联客户表
     ON A.ID                    = M.EXEC_ID 
    AND M.FR_ID                 = A.FR_ID 
    AND M.CUST_STAT             = '2' 
  INNER JOIN OCRM_F_CI_CUST_DESC N                             --统一客户信息表
     ON M.CUST_NAME             = N.CUST_ZH_NAME 
    AND M.CERT_TYPE             = N.CERT_TYPE 
    AND M.CERT_NUM              = N.CERT_NUM 
    AND N.FR_ID                 = M.FR_ID 
    AND SUBSTR(N.ODS_SYS_ID, 11, 1)                       = '1' 
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO C                      --资产协议
     ON N.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
    AND C.LN_APCL_FLG           = 'N' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.IF_CUST               = '1' 
    AND A.EXECUTE_STATUS        = '1' 
    AND A.BEGIN_DATE <= V_DT 
    AND A.END_DATE >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK = sqlContext.sql(sql)
TMP_CUST_MARK.registerTempTable("TMP_CUST_MARK")
dfn="TMP_CUST_MARK/"+V_DT+".parquet"
TMP_CUST_MARK.cache()
nrows = TMP_CUST_MARK.count()
TMP_CUST_MARK.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
