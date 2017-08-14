#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_BASE_DEP_CRE').setMaster(sys.argv[2])
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
#年初日期
V_DT_FYD = date(int(etl_date[0:4]), 1, 1).strftime("%Y%m%d") 
#上月末日期
V_DT_LMD = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1) + timedelta(-1)).strftime("%Y%m%d")
#10位日期
V_DT10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8]))).strftime("%Y-%m-%d")
V_STEP = 0
#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
OCRM_F_CI_BASE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE/*')
OCRM_F_CI_BASE.registerTempTable("OCRM_F_CI_BASE")
OCRM_F_CI_RELATE_CUST_BASE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_RELATE_CUST_BASE/*')
OCRM_F_CI_RELATE_CUST_BASE.registerTempTable("OCRM_F_CI_RELATE_CUST_BASE")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
#目标表
#ACRM_A_BASE_DEP_CRE 全量表 

#任务[21] 001-01::
V_STEP = V_STEP + 1

#非月初,删除昨日数据,保留月末和当天数据
if V_DT != V_DT_FMD :
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_BASE_DEP_CRE/"+V_DT_LD+".parquet")

#年初清除上年数据,只保留当年数据
if V_DT == V_DT_FYD :
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_BASE_DEP_CRE/*.parquet")

sql = """
 SELECT monotonically_increasing_id()  AS ID 
       ,A.ID                    AS BASE_ID 
       ,SUM(C.BAL_RMB)                       AS DEP_BAL 
       ,SUM(C.MONTH_AVG)                       AS DEP_BAL_AVG 
       ,SUM(D.BAL_RMB)                       AS CRE_BAL 
       ,SUM(D.MONTH_AVG)                       AS CRE_BAL_AVG 
       ,A.CUST_BASE_CREATE_NAME AS CREATE_MGR 
       ,A.CUST_BASE_CREATE_ORG  AS CREATE_ORG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ETL_DATE 
   FROM OCRM_F_CI_BASE A                                       --客户群表
  INNER JOIN OCRM_F_CI_RELATE_CUST_BASE B                      --客户群关联表
     ON A.ID                    = B.CUST_BASE_ID 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN ACRM_F_DP_SAVE_INFO C                             --负债协议
     ON B.CUST_ID               = C.CUST_ID 
    AND C.ACCT_STATUS           = '01' 
    AND C.FR_ID                 = A.FR_ID 
   LEFT JOIN ACRM_F_CI_ASSET_BUSI_PROTO D                      --资产协议
     ON B.CUST_ID               = D.CUST_ID 
    AND D.LN_APCL_FLG           = 'N' 
    AND D.FR_ID                 = A.FR_ID 
  GROUP BY A.FR_ID 
       ,A.ID 
       ,A.CUST_BASE_CREATE_NAME 
       ,A.CUST_BASE_CREATE_ORG """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_BASE_DEP_CRE = sqlContext.sql(sql)
ACRM_A_BASE_DEP_CRE.registerTempTable("ACRM_A_BASE_DEP_CRE")
dfn="ACRM_A_BASE_DEP_CRE/"+V_DT+".parquet"
ACRM_A_BASE_DEP_CRE.cache()
nrows = ACRM_A_BASE_DEP_CRE.count()
ACRM_A_BASE_DEP_CRE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_BASE_DEP_CRE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_BASE_DEP_CRE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_BASE_DEP_CRE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
