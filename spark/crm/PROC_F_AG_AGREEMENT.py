#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_AG_AGREEMENT').setMaster(sys.argv[2])
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

ACRM_F_NI_FINANCING = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_FINANCING/*')
ACRM_F_NI_FINANCING.registerTempTable("ACRM_F_NI_FINANCING")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
ACRM_F_NI_COLL_PAY_INSURANCE = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_COLL_PAY_INSURANCE/*')
ACRM_F_NI_COLL_PAY_INSURANCE.registerTempTable("ACRM_F_NI_COLL_PAY_INSURANCE")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

#目标表：ACRM_F_AG_AGREEMENT，先全量后多次增量

#任务[21] 001-01：
V_STEP = V_STEP + 1
#插入负债协议数据
#结息日判断
if etl_date[4:8] == '0321' or etl_date[4:8] == '0621' or etl_date[4:8] == '0921' or etl_date[4:8] == '1221' :
	sql = """
	 SELECT CAST(''  AS BIGINT)                  AS AGREEMENT_ID 
		   ,CUST_ID                 AS CUST_ID 
		   ,CUST_NAME               AS CUST_NAME 
		   ,PRODUCT_ID              AS PRODUCT_ID 
		   ,''                    AS AGREEMENT_NAME 
		   ,ORG_ID                  AS ORG_NO 
		   ,''                    AS ITEM 
		   ,''                    AS SBIT 
		   ,''                    AS SSIT 
		   ,CYNO                    AS CYNO 
		   ,CAST(MS_AC_BAL   AS DECIMAL(24,6))            AS BAL 
		   ,CAST(INRT     AS DECIMAL(24,6))               AS RATE 
		   ,CASE WHEN OPEN_DT IS NULL THEN '' ELSE CONCAT(SUBSTR(OPEN_DT,1,4),'-',SUBSTR(OPEN_DT,5,2),'-',SUBSTR(OPEN_DT,7,2)) END                     AS START_DATE 
		   ,CASE CLOSE_DT WHEN NULL THEN '' WHEN '00000000' THEN ''  ELSE CONCAT(SUBSTR(CLOSE_DT,1,4),'-',SUBSTR(CLOSE_DT,5,2),'-',SUBSTR(CLOSE_DT,7,2)) END                     AS END_DATE 
		   ,''                    AS TYPE 
		   ,''                    AS STATUS 
		   ,''                    AS HANDLE_USER 
		   ,CRM_DT                  AS CRM_DT 
		   ,'CK'                    AS SYS_NO 
		   ,''                    AS DP_TYPE 
		   ,ODS_ACCT_NO             AS ACCT_NO 
		   ,''                    AS CATL_CODE 
		   ,CUST_TYP                AS CUST_TYP 
		   ,FR_ID                   AS FR_ID 
	   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议表
	   WHERE OPEN_DT                 = V_DT
	   """

	sql = re.sub(r"\bV_DT\b", "'"+V_DT+"'", sql)
	ACRM_F_AG_AGREEMENT = sqlContext.sql(sql)
	ACRM_F_AG_AGREEMENT.registerTempTable("ACRM_F_AG_AGREEMENT")
	dfn="ACRM_F_AG_AGREEMENT/"+V_DT+".parquet"
	ACRM_F_AG_AGREEMENT.cache()
	nrows = ACRM_F_AG_AGREEMENT.count()
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_AG_AGREEMENT/*.parquet")
	ACRM_F_AG_AGREEMENT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
	ACRM_F_AG_AGREEMENT.unpersist()
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_AG_AGREEMENT/"+V_DT_LD+".parquet")
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_AG_AGREEMENT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

else:
	sql = """
	 SELECT CAST(''  AS BIGINT)                    AS AGREEMENT_ID 
		   ,CUST_ID                 AS CUST_ID 
		   ,CUST_NAME               AS CUST_NAME 
		   ,PRODUCT_ID              AS PRODUCT_ID 
		   ,''                    AS AGREEMENT_NAME 
		   ,ORG_ID                  AS ORG_NO 
		   ,''                    AS ITEM 
		   ,''                    AS SBIT 
		   ,''                    AS SSIT 
		   ,CYNO                    AS CYNO 
		   ,CAST(MS_AC_BAL AS DECIMAL(24,6))              AS BAL 
		   ,CAST(INRT      AS DECIMAL(24,6))              AS RATE 
		   ,CASE WHEN OPEN_DT IS NULL THEN '' ELSE CONCAT(SUBSTR(OPEN_DT,1,4),'-',SUBSTR(OPEN_DT,5,2),'-',SUBSTR(OPEN_DT,7,2)) END                     AS START_DATE 
		   ,CASE CLOSE_DT WHEN NULL THEN '' WHEN '00000000' THEN ''  ELSE CONCAT(SUBSTR(CLOSE_DT,1,4),'-',SUBSTR(CLOSE_DT,5,2),'-',SUBSTR(CLOSE_DT,7,2)) END                     AS END_DATE 
		   ,''                    AS TYPE 
		   ,''                    AS STATUS 
		   ,''                    AS HANDLE_USER 
		   ,CRM_DT                  AS CRM_DT 
		   ,'CK'                    AS SYS_NO 
		   ,''                    AS DP_TYPE 
		   ,ODS_ACCT_NO             AS ACCT_NO 
		   ,''                    AS CATL_CODE 
		   ,CUST_TYP                AS CUST_TYP 
		   ,FR_ID                   AS FR_ID 
	   FROM ACRM_F_DP_SAVE_INFO A                                  --
	  WHERE CRM_DT                  = V_DT """

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	ACRM_F_AG_AGREEMENT = sqlContext.sql(sql)
	dfn="ACRM_F_AG_AGREEMENT/"+V_DT+".parquet"
	ACRM_F_AG_AGREEMENT.cache()
	nrows = ACRM_F_AG_AGREEMENT.count()
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_AG_AGREEMENT/*.parquet")
	ACRM_F_AG_AGREEMENT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
	ACRM_F_AG_AGREEMENT.unpersist()
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_AG_AGREEMENT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(''  AS BIGINT)                    AS AGREEMENT_ID 
       ,CUST_ID                 AS CUST_ID 
       ,''                    AS CUST_NAME 
       ,PRODUCT_ID              AS PRODUCT_ID 
       ,''                    AS AGREEMENT_NAME 
       ,AGENCY_BRAN             AS ORG_NO 
       ,''                    AS ITEM 
       ,''                    AS SBIT 
       ,''                    AS SSIT 
       ,CURR                    AS CYNO 
       ,CAST(BAL    AS DECIMAL(24,6))                 AS BAL 
       ,CAST(BM_MOT    AS DECIMAL(24,6))              AS RATE 
       ,CASE WHEN BEGIN_DATE IS NULL THEN ''  ELSE CONCAT(SUBSTR(BEGIN_DATE,1,4),'-',SUBSTR(BEGIN_DATE,5,2),'-',SUBSTR(BEGIN_DATE,7,2)) END                     AS START_DATE 
       ,CASE WHEN END_DATE IS NULL THEN ''  ELSE CONCAT(SUBSTR(END_DATE,1,4),'-',SUBSTR(END_DATE,5,2),'-',SUBSTR(END_DATE,7,2)) END                     AS END_DATE 
       ,''                    AS TYPE 
       ,''                    AS STATUS 
       ,REGISTRANT              AS HANDLE_USER 
       ,CRM_DT                  AS CRM_DT 
       ,'DK'                    AS SYS_NO 
       ,''                    AS DP_TYPE 
       ,''                    AS ACCT_NO 
       ,''                    AS CATL_CODE 
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_AG_AGREEMENT = sqlContext.sql(sql)
dfn="ACRM_F_AG_AGREEMENT/"+V_DT+".parquet"
ACRM_F_AG_AGREEMENT.cache()
nrows = ACRM_F_AG_AGREEMENT.count()
ACRM_F_AG_AGREEMENT.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_AG_AGREEMENT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_AG_AGREEMENT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(''  AS BIGINT)            AS AGREEMENT_ID 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,PRODUCT_ID              AS PRODUCT_ID 
       ,''                    AS AGREEMENT_NAME 
       ,ORG_ID                  AS ORG_NO 
       ,''                    AS ITEM 
       ,''                    AS SBIT 
       ,''                    AS SSIT 
       ,''                    AS CYNO 
       ,CAST(MONEY     AS DECIMAL(24,6))              AS BAL 
       ,CAST(''       AS DECIMAL(24,6))             AS RATE 
       ,BUY_DATE                       AS START_DATE 
       ,BUY_DATE                       AS END_DATE 
       ,''                    AS TYPE 
       ,''                    AS STATUS 
       ,''                    AS HANDLE_USER 
       ,CRM_DT                  AS CRM_DT 
       ,'ZJ'                    AS SYS_NO 
       ,''                    AS DP_TYPE 
       ,''                    AS ACCT_NO 
       ,''                    AS CATL_CODE 
       ,CUST_TYPE               AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_NI_COLL_PAY_INSURANCE A                         --代收代付/保险协议表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_AG_AGREEMENT = sqlContext.sql(sql)
dfn="ACRM_F_AG_AGREEMENT/"+V_DT+".parquet"
ACRM_F_AG_AGREEMENT.cache()
nrows = ACRM_F_AG_AGREEMENT.count()
ACRM_F_AG_AGREEMENT.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_AG_AGREEMENT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_AG_AGREEMENT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(''  AS BIGINT)                   AS AGREEMENT_ID 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,PRODUCT_ID              AS PRODUCT_ID 
       ,''                    AS AGREEMENT_NAME 
       ,ORG_ID                  AS ORG_NO 
       ,''                    AS ITEM 
       ,''                    AS SBIT 
       ,''                    AS SSIT 
       ,CNCY                    AS CYNO 
       ,CAST(CURRE_AMOUNT   AS DECIMAL(24,6))         AS BAL 
       ,CAST(''          AS DECIMAL(24,6))          AS RATE 
       ,START_DATE                       AS START_DATE 
       ,END_DATE                       AS END_DATE 
       ,''                    AS TYPE 
       ,''                    AS STATUS 
       ,''                    AS HANDLE_USER 
       ,CRM_DT                  AS CRM_DT 
       ,'LC'                    AS SYS_NO 
       ,''                    AS DP_TYPE 
       ,''                    AS ACCT_NO 
       ,''                    AS CATL_CODE 
       ,CUST_TYPE               AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_NI_FINANCING A                                  --理财协议表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_AG_AGREEMENT = sqlContext.sql(sql)
dfn="ACRM_F_AG_AGREEMENT/"+V_DT+".parquet"
ACRM_F_AG_AGREEMENT.cache()
nrows = ACRM_F_AG_AGREEMENT.count()
ACRM_F_AG_AGREEMENT.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_AG_AGREEMENT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_AG_AGREEMENT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
