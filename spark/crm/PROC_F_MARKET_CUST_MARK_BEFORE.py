#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_MARKET_CUST_MARK_BEFORE').setMaster(sys.argv[2])
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
ACRM_F_NI_COLL_PAY_INSURANCE = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_COLL_PAY_INSURANCE/*')
ACRM_F_NI_COLL_PAY_INSURANCE.registerTempTable("ACRM_F_NI_COLL_PAY_INSURANCE")
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
OCRM_F_MARKET_TASK_EXECUTOR = sqlContext.read.parquet(hdfs+'/OCRM_F_MARKET_TASK_EXECUTOR/*')
OCRM_F_MARKET_TASK_EXECUTOR.registerTempTable("OCRM_F_MARKET_TASK_EXECUTOR")

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
  INNER JOIN OCRM_F_MARKET_TASK D                              --营销任务主表
     ON A.TASK_ID               = D.ID 
    AND D.FR_ID                 = A.FR_ID 
    AND D.TASK_TYPE             = '2' 
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --客户归属经理表
     ON A.EXECUTOR_ID           = B.MGR_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN ACRM_F_DP_SAVE_INFO C                             --负债协议表
     ON B.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
    AND C.ACCT_STATUS           = '01' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.BEGIN_DATE            = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK_BEFORE = sqlContext.sql(sql)
TMP_CUST_MARK_BEFORE.registerTempTable("TMP_CUST_MARK_BEFORE")
dfn="TMP_CUST_MARK_BEFORE/"+V_DT+".parquet"
TMP_CUST_MARK_BEFORE.cache()
nrows = TMP_CUST_MARK_BEFORE.count()
TMP_CUST_MARK_BEFORE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_CUST_MARK_BEFORE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'2' AS MARK_FLAG
		,V_DT AS ETL_DATE
		,A.FR_ID AS FR_ID 
from 
OCRM_F_MARKET_TASK_EXECUTOR A    --营销任务汇总表
		INNER JOIN
		OCRM_F_MARKET_TASK D    --营销任务主表
		on
		A.TASK_ID = D.ID AND D.FR_ID = A.FR_ID AND D.TASK_TYPE = '2' 
		INNER JOIN
		OCRM_F_CI_BELONG_CUSTMGR B    --客户归属经理表
		on
		A.EXECUTOR_ID = B.MGR_ID AND B.FR_ID = A.FR_ID AND B.MAIN_TYPE = '1' 
		INNER JOIN
		OCRM_F_CI_COM_CUST_INFO C    --对公客户信息表
		on
		B.CUST_ID = C.CUST_ID AND C.FR_ID = A.FR_ID  
where A.EXECUTOR_TYPE = '2' 
    AND A.BEGIN_DATE            = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK_BEFORE = sqlContext.sql(sql)
TMP_CUST_MARK_BEFORE.registerTempTable("TMP_CUST_MARK_BEFORE")
dfn="TMP_CUST_MARK_BEFORE/"+V_DT+".parquet"
TMP_CUST_MARK_BEFORE.cache()
nrows = TMP_CUST_MARK_BEFORE.count()
TMP_CUST_MARK_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK_BEFORE.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS EXEC_ID 
       ,A.TASK_ID               AS TASK_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.EXECUTOR_ID           AS MGR_ID 
       ,'3' AS MARK_FLAG
		,V_DT AS ETL_DATE
		,A.FR_ID AS FR_ID 
from 
OCRM_F_MARKET_TASK_EXECUTOR A    --营销任务汇总表
		INNER JOIN
		OCRM_F_MARKET_TASK D    --营销任务主表
		on
		A.TASK_ID = D.ID AND D.FR_ID = A.FR_ID AND D.TASK_TYPE = '2' 
		INNER JOIN
		OCRM_F_CI_BELONG_CUSTMGR B    --客户归属经理表
		on
	 A.EXECUTOR_ID = B.MGR_ID AND B.FR_ID = A.FR_ID AND B.MAIN_TYPE = '1' 
		INNER JOIN
		OCRM_F_DP_CARD_INFO C    --卡档
		on
		B.CUST_ID = C.CR_CUST_NO AND C.FR_ID = A.FR_ID  
where A.EXECUTOR_TYPE = '2'
    AND A.BEGIN_DATE            = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK_BEFORE = sqlContext.sql(sql)
TMP_CUST_MARK_BEFORE.registerTempTable("TMP_CUST_MARK_BEFORE")
dfn="TMP_CUST_MARK_BEFORE/"+V_DT+".parquet"
TMP_CUST_MARK_BEFORE.cache()
nrows = TMP_CUST_MARK_BEFORE.count()
TMP_CUST_MARK_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK_BEFORE.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

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
  INNER JOIN OCRM_F_MARKET_TASK D                              --营销任务主表
     ON A.TASK_ID               = D.ID 
    AND D.FR_ID                 = A.FR_ID 
    AND D.TASK_TYPE             = '2' 
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --客户归属经理表
     ON A.EXECUTOR_ID           = B.MGR_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN OCRM_F_CI_CUST_DESC C                             --统一客户信息表
     ON B.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
    AND SUBSTR(C.ODS_SYS_ID, 6, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.BEGIN_DATE            = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK_BEFORE = sqlContext.sql(sql)
TMP_CUST_MARK_BEFORE.registerTempTable("TMP_CUST_MARK_BEFORE")
dfn="TMP_CUST_MARK_BEFORE/"+V_DT+".parquet"
TMP_CUST_MARK_BEFORE.cache()
nrows = TMP_CUST_MARK_BEFORE.count()
TMP_CUST_MARK_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK_BEFORE.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

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
  INNER JOIN OCRM_F_MARKET_TASK D                              --营销任务主表
     ON A.TASK_ID               = D.ID 
    AND D.FR_ID                 = A.FR_ID 
    AND D.TASK_TYPE             = '2' 
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --客户归属经理表
     ON A.EXECUTOR_ID           = B.MGR_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN OCRM_F_CI_CUST_DESC C                             --统一客户信息表
     ON B.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
    AND SUBSTR(C.ODS_SYS_ID, 10, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.BEGIN_DATE            = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK_BEFORE = sqlContext.sql(sql)
TMP_CUST_MARK_BEFORE.registerTempTable("TMP_CUST_MARK_BEFORE")
dfn="TMP_CUST_MARK_BEFORE/"+V_DT+".parquet"
TMP_CUST_MARK_BEFORE.cache()
nrows = TMP_CUST_MARK_BEFORE.count()
TMP_CUST_MARK_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK_BEFORE.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

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
  INNER JOIN OCRM_F_MARKET_TASK D                              --营销任务主表
     ON A.TASK_ID               = D.ID 
    AND D.FR_ID                 = A.FR_ID 
    AND D.TASK_TYPE             = '2' 
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --客户归属经理表
     ON A.EXECUTOR_ID           = B.MGR_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN OCRM_F_CI_CUST_DESC C                             --统一客户信息表
     ON B.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
    AND SUBSTR(C.ODS_SYS_ID, 3, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.BEGIN_DATE            = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK_BEFORE = sqlContext.sql(sql)
TMP_CUST_MARK_BEFORE.registerTempTable("TMP_CUST_MARK_BEFORE")
dfn="TMP_CUST_MARK_BEFORE/"+V_DT+".parquet"
TMP_CUST_MARK_BEFORE.cache()
nrows = TMP_CUST_MARK_BEFORE.count()
TMP_CUST_MARK_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK_BEFORE.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

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
  INNER JOIN OCRM_F_MARKET_TASK D                              --营销任务主表
     ON A.TASK_ID               = D.ID 
    AND D.FR_ID                 = A.FR_ID 
    AND D.TASK_TYPE             = '2' 
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --客户归属经理表
     ON A.EXECUTOR_ID           = B.MGR_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN OCRM_F_CI_CUST_DESC C                             --统一客户信息表
     ON B.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
    AND SUBSTR(C.ODS_SYS_ID, 11, 1)                       = '1' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.BEGIN_DATE            = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK_BEFORE = sqlContext.sql(sql)
TMP_CUST_MARK_BEFORE.registerTempTable("TMP_CUST_MARK_BEFORE")
dfn="TMP_CUST_MARK_BEFORE/"+V_DT+".parquet"
TMP_CUST_MARK_BEFORE.cache()
nrows = TMP_CUST_MARK_BEFORE.count()
TMP_CUST_MARK_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK_BEFORE.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

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
  INNER JOIN OCRM_F_MARKET_TASK D                              --营销任务主表
     ON A.TASK_ID               = D.ID 
    AND D.FR_ID                 = A.FR_ID 
    AND D.TASK_TYPE             = '2' 
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --客户归属经理表
     ON A.EXECUTOR_ID           = B.MGR_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN ACRM_F_NI_COLL_PAY_INSURANCE C                    --代收代付/保险协议表
     ON B.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.BEGIN_DATE            = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK_BEFORE = sqlContext.sql(sql)
TMP_CUST_MARK_BEFORE.registerTempTable("TMP_CUST_MARK_BEFORE")
dfn="TMP_CUST_MARK_BEFORE/"+V_DT+".parquet"
TMP_CUST_MARK_BEFORE.cache()
nrows = TMP_CUST_MARK_BEFORE.count()
TMP_CUST_MARK_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK_BEFORE.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

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
  INNER JOIN OCRM_F_MARKET_TASK D                              --营销任务主表
     ON A.TASK_ID               = D.ID 
    AND D.FR_ID                 = A.FR_ID 
    AND D.TASK_TYPE             = '2' 
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --客户归属经理表
     ON A.EXECUTOR_ID           = B.MGR_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO C                      --资产协议表
     ON B.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
    AND C.LN_APCL_FLG           = 'N' 
  WHERE A.EXECUTOR_TYPE         = '2' 
    AND A.BEGIN_DATE            = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_MARK_BEFORE = sqlContext.sql(sql)
TMP_CUST_MARK_BEFORE.registerTempTable("TMP_CUST_MARK_BEFORE")
dfn="TMP_CUST_MARK_BEFORE/"+V_DT+".parquet"
TMP_CUST_MARK_BEFORE.cache()
nrows = TMP_CUST_MARK_BEFORE.count()
TMP_CUST_MARK_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_MARK_BEFORE.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_MARK_BEFORE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_MARK_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
