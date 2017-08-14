#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_PRODUCT_FEATURE').setMaster(sys.argv[2])
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

TARGET_FEATURE_CFG = sqlContext.read.parquet(hdfs+'/TARGET_FEATURE_CFG/*')
TARGET_FEATURE_CFG.registerTempTable("TARGET_FEATURE_CFG")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
ACRM_F_NI_FINANCING = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_FINANCING/*')
ACRM_F_NI_FINANCING.registerTempTable("ACRM_F_NI_FINANCING")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100001' AS TARGET
		,C.TARGET_VALUE AS TARGET_VALUE
		,COUNT(distinct A.CUST_ID) AS NUM_VALUE
		,SUM(A.BAL_RMB) AS AMT_VALUE
		,A.FR_ID AS FR_ID
		,V_DT AS ETL_DATE 
from 
ACRM_F_DP_SAVE_INFO A    --负债协议表
		INNER JOIN
		OCRM_F_CI_PER_CUST_INFO B    --零售客户基本信息表
		on
		A.CUST_ID = B.CUST_ID  AND A.FR_ID = B.FR_ID 
		INNER JOIN
		TARGET_FEATURE_CFG C    --产品特征项指标配置表
		on
		B.CUST_SEX = C.TARGET_VALUE AND C.TARGET_ID = '100001'  
where A.ACCT_STATUS = '01' 
    AND B.CUST_SEX IS 
    NOT NULL 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_PRODUCT_FEATURE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_PRODUCT_FEATURE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100002'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,COUNT(DISTINCT A.CUST_ID)                       AS NUM_VALUE 
       ,SUM(A.BAL_RMB)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON YEAR(DATE(V_DT)) - INT(SUBSTR(B.CUST_BIR, 1, 4)) > INT(C.TARGET_LOW) 
    AND YEAR(DATE(V_DT)) - INT(SUBSTR(B.CUST_BIR, 1, 4)) <= INT(C.TARGET_HIGH) 
    AND C.TARGET_ID             = '100002' 
  WHERE B.CUST_BIR IS 
    NOT NULL 
    AND B.CUST_BIR != '' 
    AND B.CUST_BIR != '1900-01-01' 
    AND SUBSTR(B.CUST_BIR, 3, 1) != '-' 
    AND SUBSTR(B.CUST_BIR, 3, 1) != '/' 
    AND A.ACCT_STATUS           = '01' 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100003'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,COUNT(DISTINCT A.CUST_ID)                       AS NUM_VALUE 
       ,SUM(A.BAL_RMB)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_ANNUAL_INCOME > INT(C.TARGET_LOW) 
    AND B.CUST_ANNUAL_INCOME <= INT(C.TARGET_HIGH) 
    AND C.TARGET_ID             = '100003' 
  WHERE B.CUST_ANNUAL_INCOME IS 
    NOT NULL 
    AND A.ACCT_STATUS           = '01' 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100004'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,COUNT(DISTINCT A.CUST_ID)                       AS NUM_VALUE 
       ,SUM(A.BAL_RMB)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_OCCUP_COD1       = C.TARGET_VALUE 
    AND C.TARGET_ID             = '100004' 
  WHERE B.CUST_OCCUP_COD1 IS 
    NOT NULL 
    AND A.ACCT_STATUS           = '01' 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100005'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,COUNT(DISTINCT A.CUST_ID)                       AS NUM_VALUE 
       ,SUM(A.BAL_RMB)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_EDU_LVL_COD      = C.TARGET_VALUE 
    AND C.TARGET_ID             = '100005' 
  WHERE B.CUST_EDU_LVL_COD IS 
    NOT NULL 
    AND A.ACCT_STATUS           = '01' 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-06::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100006'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,COUNT(DISTINCT A.CUST_ID)                       AS NUM_VALUE 
       ,SUM(A.BAL_RMB)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_MRG              = C.TARGET_VALUE 
    AND C.TARGET_ID             = '100006' 
  WHERE B.CUST_MRG IS 
    NOT NULL 
    AND A.ACCT_STATUS           = '01' 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-07::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100001'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,COUNT(DISTINCT A.CUST_ID)                       AS NUM_VALUE 
       ,SUM(A.BAL_RMB)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_SEX              = C.TARGET_VALUE 
    AND C.TARGET_ID             = '100001' 
  WHERE B.CUST_SEX IS 
    NOT NULL 
    AND A.LN_APCL_FLG           = 'N' 
    AND A.BAL_RMB > 0 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-08::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100002'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,COUNT(DISTINCT A.CUST_ID)                       AS NUM_VALUE 
       ,SUM(A.BAL_RMB)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON YEAR(V_DT) - INT(SUBSTR(B.CUST_BIR, 1, 4)) > INT(C.TARGET_LOW) 
    AND YEAR(V_DT) - INT(SUBSTR(B.CUST_BIR, 1, 4)) <= INT(C.TARGET_HIGH) 
    AND C.TARGET_ID             = '100002' 
  WHERE B.CUST_BIR IS 
    NOT NULL 
    AND B.CUST_BIR != '' 
    AND B.CUST_BIR != '1900-01-01' 
    AND SUBSTR(B.CUST_BIR, 3, 1) != '-' 
    AND SUBSTR(B.CUST_BIR, 3, 1) != '/' 
    AND A.LN_APCL_FLG           = 'N' 
    AND A.BAL_RMB > 0 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-09::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id() 
                        AS ID 
       ,A.PRODUCT_ID 
                        AS PRODUCT_ID 
       ,'100003' 
                        AS TARGET 
       ,C.TARGET_VALUE 
                        AS TARGET_VALUE 
       ,COUNT(DISTINCT A.CUST_ID) 
                        AS NUM_VALUE 
       ,SUM(A.BAL_RMB) 
                        AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_ANNUAL_INCOME > INT(C.TARGET_LOW) 
    AND B.CUST_ANNUAL_INCOME <= INT(C.TARGET_HIGH) 
    AND C.TARGET_ID             = '100003' 
  WHERE B.CUST_ANNUAL_INCOME IS 
    NOT NULL 
    AND A.LN_APCL_FLG           = 'N' 
    AND A.BAL_RMB > 0 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-10::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100004'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,COUNT(DISTINCT A.CUST_ID)                       AS NUM_VALUE 
       ,SUM(A.BAL_RMB)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_OCCUP_COD1       = C.TARGET_VALUE 
    AND C.TARGET_ID             = '100004' 
  WHERE B.CUST_OCCUP_COD1 IS 
    NOT NULL 
    AND A.LN_APCL_FLG           = 'N' 
    AND A.BAL_RMB > 0 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-11::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100005'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,COUNT(DISTINCT A.CUST_ID)                       AS NUM_VALUE 
       ,SUM(A.BAL_RMB)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_EDU_LVL_COD      = C.TARGET_VALUE 
    AND C.TARGET_ID             = '100005' 
  WHERE B.CUST_EDU_LVL_COD IS 
    NOT NULL 
    AND A.LN_APCL_FLG           = 'N' 
    AND A.BAL_RMB > 0 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-12::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100006'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,COUNT(DISTINCT A.CUST_ID)                       AS NUM_VALUE 
       ,SUM(A.BAL_RMB)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_MRG              = C.TARGET_VALUE 
    AND C.TARGET_ID             = '100006' 
  WHERE B.CUST_MRG IS 
    NOT NULL 
    AND A.LN_APCL_FLG           = 'N' 
    AND A.BAL_RMB > 0 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-13::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100001'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,SUM(A.APPLPER)                       AS NUM_VALUE 
       ,SUM(A.APPLAMT)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_NI_FINANCING A                                  --理财协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_SEX              = C.TARGET_VALUE 
    AND C.TARGET_ID             = '100001' 
  WHERE B.CUST_SEX IS 
    NOT NULL 
    AND A.END_DATE > V_DT 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-14::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100002'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,SUM(A.APPLPER)                       AS NUM_VALUE 
       ,SUM(A.APPLAMT)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_NI_FINANCING A                                  --理财协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON YEAR(V_DT) - INT(SUBSTR(B.CUST_BIR, 1, 4)) > INT(C.TARGET_LOW) 
    AND YEAR(V_DT) - INT(SUBSTR(B.CUST_BIR, 1, 4)) <= INT(C.TARGET_HIGH) 
    AND C.TARGET_ID             = '100002' 
  WHERE B.CUST_BIR IS 
    NOT NULL 
    AND B.CUST_BIR != '' 
    AND B.CUST_BIR != '1900-01-01' 
    AND SUBSTR(B.CUST_BIR, 3, 1) != '-' 
    AND SUBSTR(B.CUST_BIR, 3, 1) != '/' 
    AND V_DT < A.END_DATE 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-15::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100003'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,SUM(A.APPLPER)                       AS NUM_VALUE 
       ,SUM(A.APPLAMT)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_NI_FINANCING A                                  --理财协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_ANNUAL_INCOME > INT(C.TARGET_LOW) AND B.CUST_ANNUAL_INCOME <= INT(C.TARGET_HIGH) 
    AND C.TARGET_ID             = '100003' 
  WHERE B.CUST_ANNUAL_INCOME IS 
    NOT NULL 
    AND A.END_DATE > V_DT 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-16::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100004'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,SUM(A.APPLPER)                       AS NUM_VALUE 
       ,SUM(A.APPLAMT)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_NI_FINANCING A                                  --理财协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_OCCUP_COD1       = C.TARGET_VALUE 
    AND C.TARGET_ID             = '100004' 
  WHERE B.CUST_OCCUP_COD1 IS 
    NOT NULL 
    AND A.END_DATE > V_DT 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-17::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100005'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,SUM(A.APPLPER)                       AS NUM_VALUE 
       ,SUM(A.APPLAMT)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_NI_FINANCING A                                  --理财协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_EDU_LVL_COD      = C.TARGET_VALUE 
    AND C.TARGET_ID             = '100005' 
  WHERE B.CUST_EDU_LVL_COD IS 
    NOT NULL 
    AND A.END_DATE > V_DT 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-18::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,'100006'                AS TARGET 
       ,C.TARGET_VALUE          AS TARGET_VALUE 
       ,SUM(A.APPLPER)                       AS NUM_VALUE 
       ,SUM(A.APPLAMT)                       AS AMT_VALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM ACRM_F_NI_FINANCING A                                  --理财协议表
  INNER JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN TARGET_FEATURE_CFG C                              --产品特征项指标配置表
     ON B.CUST_MRG              = C.TARGET_VALUE 
    AND C.TARGET_ID             = '100006' 
  WHERE B.CUST_MRG IS 
    NOT NULL 
    AND A.END_DATE > V_DT 
  GROUP BY A.PRODUCT_ID 
       ,C.TARGET_VALUE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PRODUCT_FEATURE = sqlContext.sql(sql)
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
dfn="ACRM_A_PRODUCT_FEATURE/"+V_DT+".parquet"
ACRM_A_PRODUCT_FEATURE.cache()
nrows = ACRM_A_PRODUCT_FEATURE.count()
ACRM_A_PRODUCT_FEATURE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_PRODUCT_FEATURE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PRODUCT_FEATURE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
