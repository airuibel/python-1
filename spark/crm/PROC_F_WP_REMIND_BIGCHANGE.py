#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_WP_REMIND_BIGCHANGE').setMaster(sys.argv[2])
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

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
OCRM_F_WP_REMIND_RULE = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_RULE/*')
OCRM_F_WP_REMIND_RULE.registerTempTable("OCRM_F_WP_REMIND_RULE")
ACRM_F_CI_NIN_TRANSLOG = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_NIN_TRANSLOG/*')
ACRM_F_CI_NIN_TRANSLOG.registerTempTable("ACRM_F_CI_NIN_TRANSLOG")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()    AS ID 
       ,E.RULE_ID               AS RULE_ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_NAME 
       ,A.LINK_TEL              AS LINK_TEL 
       ,B.ODS_ACCT_NO           AS ACCT_NO 
       ,C.PRODUCT_ID            AS PROD_ID 
       ,C.PROD_NAME             AS PROD_NAME 
       ,B.MS_AC_BAL             AS ACCT_BAL 
       ,B.MS_AC_BAL - B.UP_BAL_RMB            AS CHANGE_AMT 
       ,DATE(B.LTDT)                       AS CHANGE_DATE 
       ,''                    AS CHANNEL_FLAG 
       ,'A000101'               AS REMIND_TYPE 
       ,V_DT                    AS MSG_CRT_DATE 
       ,E.ORG_ID                AS FR_ID 
   FROM OCRM_F_WP_REMIND_RULE E                                --提醒规则表
  INNER JOIN OCRM_F_CI_CUST_DESC A                             --统一客户信息表
     ON A.CUST_TYP              = E.CUST_TYPE 
    AND A.FR_ID                 = E.ORG_ID 
  INNER JOIN ACRM_F_DP_SAVE_INFO B                             --负债协议
     ON A.CUST_ID               = B.CUST_ID 
    AND B.FR_ID                 = E.ORG_ID 
    AND ABS(B.MS_AC_BAL - B.UP_BAL_RMB) >= E.CHANGE_AMOUNT 
    AND DATE(B.LTDT)                       = V_DT 
  INNER JOIN OCRM_F_PD_PROD_INFO C                             --产品表
     ON B.PRODUCT_ID            = C.PRODUCT_ID 
    AND C.FR_ID                 = E.ORG_ID 
  LEFT JOIN (SELECT FR_ID,CUST_ID,FK_SAACN_KEY,CHANNEL_FLAG,
                     ROW_NUMBER() OVER(PARTITION BY FR_ID,CUST_ID,FK_SAACN_KEY ORDER BY SA_TVARCHAR_AMT DESC) AS ROW_ID 
                FROM ACRM_F_CI_NIN_TRANSLOG 
               WHERE SA_TVARCHAR_DT = V_8_DT ) D ON A.FR_ID = D.FR_ID AND A.CUST_ID = D.CUST_ID 
                     AND B.ODS_ACCT_NO = D.FK_SAACN_KEY AND D.ROW_ID = 1 
  WHERE E.REMIND_TYPE           = 'A000101' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_8_DT\b", "'"+V_DT+"'", sql)
OCRM_F_WP_REMIND_BIGCHANGE = sqlContext.sql(sql)
OCRM_F_WP_REMIND_BIGCHANGE.registerTempTable("OCRM_F_WP_REMIND_BIGCHANGE")
dfn="OCRM_F_WP_REMIND_BIGCHANGE/"+V_DT+".parquet"
OCRM_F_WP_REMIND_BIGCHANGE.cache()
nrows = OCRM_F_WP_REMIND_BIGCHANGE.count()

#删除当天数据，支持重跑
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_WP_REMIND_BIGCHANGE/"+V_DT+".parquet ")
OCRM_F_WP_REMIND_BIGCHANGE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND_BIGCHANGE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND_BIGCHANGE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

