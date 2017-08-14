#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CI_CUST_PROD').setMaster(sys.argv[2])
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

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
ACRM_F_NI_FINANCING = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_FINANCING/*')
ACRM_F_NI_FINANCING.registerTempTable("ACRM_F_NI_FINANCING")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

#目标表：ACRM_A_CI_CUST_PROD，先全量，后增量
#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,PRODUCT_ID              AS PRODUCT_ID 
       ,'DK'                    AS TYPE 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  WHERE BAL > 0 
  GROUP BY CUST_ID 
       ,PRODUCT_ID 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CI_CUST_PROD = sqlContext.sql(sql)
ACRM_A_CI_CUST_PROD.registerTempTable("ACRM_A_CI_CUST_PROD")
dfn="ACRM_A_CI_CUST_PROD/"+V_DT+".parquet"
ACRM_A_CI_CUST_PROD.cache()
nrows = ACRM_A_CI_CUST_PROD.count()
ACRM_A_CI_CUST_PROD.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_CI_CUST_PROD.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CI_CUST_PROD/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CI_CUST_PROD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,PRODUCT_ID              AS PRODUCT_ID 
       ,'CK'                    AS TYPE 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议表
  WHERE ACCT_STATUS             = '01' 
  GROUP BY CUST_ID 
       ,PRODUCT_ID 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CI_CUST_PROD = sqlContext.sql(sql)
dfn="ACRM_A_CI_CUST_PROD/"+V_DT+".parquet"
ACRM_A_CI_CUST_PROD.cache()
nrows = ACRM_A_CI_CUST_PROD.count()
ACRM_A_CI_CUST_PROD.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_CI_CUST_PROD.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CI_CUST_PROD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,PRODUCT_ID              AS PRODUCT_ID 
       ,'LC'                    AS TYPE 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_NI_FINANCING A                                  --理财协议表
  GROUP BY CUST_ID 
       ,PRODUCT_ID 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CI_CUST_PROD = sqlContext.sql(sql)
dfn="ACRM_A_CI_CUST_PROD/"+V_DT+".parquet"
ACRM_A_CI_CUST_PROD.cache()
nrows = ACRM_A_CI_CUST_PROD.count()
ACRM_A_CI_CUST_PROD.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_CI_CUST_PROD.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CI_CUST_PROD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
