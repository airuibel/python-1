#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_WP_REMIND_APCL_GUADEP').setMaster(sys.argv[2])
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

OCRM_F_LN_GUARANTY_CONT = sqlContext.read.parquet(hdfs+'/OCRM_F_LN_GUARANTY_CONT/*')
OCRM_F_LN_GUARANTY_CONT.registerTempTable("OCRM_F_LN_GUARANTY_CONT")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
OCRM_F_WP_REMIND_RULE = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_RULE/*')
OCRM_F_WP_REMIND_RULE.registerTempTable("OCRM_F_WP_REMIND_RULE")
OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
F_LN_XDXT_CONTRACT_RELATIVE = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_CONTRACT_RELATIVE/*')
F_LN_XDXT_CONTRACT_RELATIVE.registerTempTable("F_LN_XDXT_CONTRACT_RELATIVE")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()     AS ID 
       ,E.RULE_ID               AS RULE_ID 
       ,A1.CUST_ID              AS CUST_ID 
       ,A1.CORE_CUST_NAME       AS CUST_NAME 
       ,A4.ODS_CUST_ID          AS GUARANTOR_ID 
       ,A4.ODS_CUST_NAME        AS GUARANTOR_NAME 
       ,A1.ACCT_NO              AS CR_ACCT_NO 
       ,B.ODS_ACCT_NO           AS DP_ACCT_NO 
       ,C.PRODUCT_ID            AS PROD_ID 
       ,C.PROD_NAME             AS PROD_NAME 
       ,B.BAL_RMB               AS ACCT_BAL 
       ,B.MS_AC_BAL - B.UP_BAL                AS CHANGE_AMT 
       ,DATE(B.LTDT)                       AS CHANGE_DATE 
       ,A1.MANAGE_BRAN          AS MANAGE_BRAN 
       ,A1.AGENCY_BRAN          AS CREATE_BRAN 
       ,'A000113' AS REMIND_TYPE
       ,''                    AS REMIND_FLAG 
       ,V_DT                    AS MSG_CRT_DATE 
       ,E.ORG_ID                AS FR_ID 
   FROM OCRM_F_WP_REMIND_RULE E                                --提醒规则表
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO A1                     --资产协议表
     ON A1.FR_ID                = E.ORG_ID 
    AND A1.CUST_TYP             = E.CUST_TYPE 
    AND A1.LN_APCL_FLG          = '1' 
  INNER JOIN F_LN_XDXT_CONTRACT_RELATIVE A2                    --合同关联表
     ON A1.CONT_NO              = A2.SERIALNO 
    AND E.ORG_ID                = A2.FR_ID 
    AND A2.OBJECTTYPE           = 'GuarantyContract' 
  INNER JOIN OCRM_F_LN_GUARANTY_CONT A3                        --担保合同信息表
     ON A2.OBJECTNO             = A3.SERIALNO 
    AND E.ORG_ID                = A3.FR_ID 
  INNER JOIN OCRM_F_CI_SYS_RESOURCE A4                         --系统来源中间表
     ON A3.GUARANTORID          = A4.SOURCE_CUST_ID 
    AND A4.FR_ID                = E.ORG_ID 
    AND A4.ODS_SYS_ID           = 'LNA' 
  INNER JOIN ACRM_F_DP_SAVE_INFO B                             --负债协议
     ON A4.ODS_CUST_ID          = B.CUST_ID 
    AND B.FR_ID                 = E.ORG_ID 
    AND B.ACCT_STATUS           = '01' 
    AND B.MS_AC_BAL <> B.UP_BAL 
    AND DATE(B.LTDT)                       = V_DT 
  INNER JOIN OCRM_F_PD_PROD_INFO C                             --产品表
     ON B.PRODUCT_ID            = C.PRODUCT_ID 
    AND C.FR_ID                 = E.ORG_ID 
  WHERE E.REMIND_TYPE           = 'A000113' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND_APCL_GUADEP = sqlContext.sql(sql)
OCRM_F_WP_REMIND_APCL_GUADEP.registerTempTable("OCRM_F_WP_REMIND_APCL_GUADEP")
dfn="OCRM_F_WP_REMIND_APCL_GUADEP/"+V_DT+".parquet"
OCRM_F_WP_REMIND_APCL_GUADEP.cache()
nrows = OCRM_F_WP_REMIND_APCL_GUADEP.count()

#删除当天数据，支持重跑
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND_APCL_GUADEP/"+V_DT+".parquet ")
OCRM_F_WP_REMIND_APCL_GUADEP.write.save(path=hdfs + '/' + dfn, mode='append')

OCRM_F_WP_REMIND_APCL_GUADEP.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND_APCL_GUADEP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND_APCL_GUADEP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
