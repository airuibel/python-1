#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUSTLNAINFO').setMaster(sys.argv[2])
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

F_CI_CUSTR2 = sqlContext.read.parquet(hdfs+'/F_CI_CUSTR2/*')
F_CI_CUSTR2.registerTempTable("F_CI_CUSTR2")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_CI_CUSTR = sqlContext.read.parquet(hdfs+'/F_CI_CUSTR/*')
F_CI_CUSTR.registerTempTable("F_CI_CUSTR")
F_TX_EVENT = sqlContext.read.parquet(hdfs+'/F_TX_EVENT/*')
F_TX_EVENT.registerTempTable("F_TX_EVENT")
F_CR_ODUE2 = sqlContext.read.parquet(hdfs+'/F_CR_ODUE2/*')
F_CR_ODUE2.registerTempTable("F_CR_ODUE2")
F_TX_MPUR = sqlContext.read.parquet(hdfs+'/F_TX_MPUR/*')
F_TX_MPUR.registerTempTable("F_TX_MPUR")
F_CR_CARD = sqlContext.read.parquet(hdfs+'/F_CR_CARD/*')
F_CR_CARD.registerTempTable("F_CR_CARD")
F_CR_ACCT = sqlContext.read.parquet(hdfs+'/F_CR_ACCT/*')
F_CR_ACCT.registerTempTable("F_CR_ACCT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id() AS BIGINT)      AS ID 
       ,S.ODS_CUST_ID           AS CUST_ID 
       ,E.SURNAME               AS CUSTOM_NAME 
       ,A.CARD_NBR              AS CARD_NO 
       ,CASE WHEN SUBSTR(B.IMP_FLAG,1,1) = '1' THEN '1' ELSE '2' END                     AS IF_IMP_CARD 
       ,CASE WHEN G.CARD_NBR IS NOT NULL THEN '1' END                     AS IF_FIRST_USE 
       ,CASE WHEN F.CARD_NBR IS NOT NULL THEN '1' END                     AS IF_BY_STAGE 
       ,CASE WHEN H.CARD_NBR IS NOT NULL THEN '1' END                     AS IF_OVD 
       ,CAST(B.CRED_LIMIT AS DECIMAL(24,6))           AS CREDIT_LINE 
       ,CAST(B.STM_OVERDU  AS DECIMAL(24,6))           AS OD_LINE 
       ,C.VALUE_LEVEL           AS CUST_MERIT_LEVEL 
       ,C.RISK_LEVEL            AS CUST_RISK_LEVEL 
       ,C.TRANS_TY              AS TRANS_TYP 
       ,CASE WHEN B.REPAY_CODE IS NOT NULL THEN '1' ELSE '2' END                     AS IF_AUTO_REPAY 
       ,B.BANKACCT_A1           AS REPAY_ACCT_NO 
       ,CAST(B.PRODUCT   AS VARCHAR(13))            AS CARD_TYP 
       ,CONCAT(SUBSTR(A.ISSUE_DAY,1,4),'-',SUBSTR(A.ISSUE_DAY,5,2),'-',SUBSTR(A.ISSUE_DAY,7,2)) AS PUB_DATE 
       ,CASE WHEN NVL(A.EXPIRY_NEW,0)=0 THEN CONCAT('20',SUBSTR(A.EXPIRY_DTE,1,2),'-',SUBSTR(A.EXPIRY_DTE,3,2),'-1') 
       ELSE CONCAT('20',SUBSTR(A.EXPIRY_NEW,1,2),'-',SUBSTR(A.EXPIRY_NEW,3,2),'-1') END  AS EXP_DATE
       ,CAST(B.POINT_ADJ   AS DECIMAL(10))          AS BONUS_INTG_ADJ 
       ,CAST(B.POINT_CLM   AS  DECIMAL(11))        AS BONUS_INTG_EXG 
       ,CAST(B.POINT_CUM   AS  DECIMAL(12))        AS BONUS_INTG_SUM 
       ,CAST(B.POINT_EAR   AS  DECIMAL(13))        AS BONUS_INTG_INC 
       ,V_DT                  AS ODS_ST_DATE 
       ,CAST(CASE WHEN B.PURCHASES_S = '+' THEN B.CRED_LIMIT + B.PURCHASES ELSE B.CRED_LIMIT - B.PURCHASES END AS DECIMAL(24,6)) AS USEFUL_LINE 
       ,cast(B.BRANCH  as   VARCHAR(32))            AS ORG_ID 
       ,C.FR_ID                 AS FR_ID 
       ,S.ODS_CUST_TYPE         AS CUST_TYPE 
       ,S.CERT_NO               AS CERT_NO 
   FROM F_CR_CARD A                                            --卡片资料
  INNER JOIN F_CR_ACCT B                                       --帐户资料
     ON A.CARD_NBR              = B.CARD_NBR 
  INNER JOIN F_CI_CUSTR2 C                                     --客户资料2
     ON A.CUSTR_NBR             = C.CUSTR_NBR 
  INNER JOIN OCRM_F_CI_SYS_RESOURCE S                          --系统来源中间表
     ON C.CUSTR_NBR             = S.SOURCE_CUST_ID 
    AND S.ODS_SYS_ID            = 'CRE' 
  INNER JOIN F_CI_CUSTR E                                      --客户资料
     ON E.CUSTR_NBR             = C.CUSTR_NBR 
   LEFT JOIN F_TX_MPUR F                                       --
     ON A.CARD_NBR              = F.CARD_NBR 
   LEFT JOIN F_TX_EVENT G                                      --
     ON A.CARD_NBR              = G.CARD_NBR 
   LEFT JOIN F_CR_ACCT H                                       --
     ON A.CARD_NBR              = H.CARD_NBR 
  INNER JOIN F_CR_ODUE2 I                                      --
     ON H.XACCOUNT              = I.XACCOUNT 
    AND I.ODUE_FLAG             = '1' 
  WHERE A.CANCL_CODE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUSTLNAINFO = sqlContext.sql(sql)
OCRM_F_CI_CUSTLNAINFO.registerTempTable("OCRM_F_CI_CUSTLNAINFO")
dfn="OCRM_F_CI_CUSTLNAINFO/"+V_DT+".parquet"
OCRM_F_CI_CUSTLNAINFO.cache()
nrows = OCRM_F_CI_CUSTLNAINFO.count()
OCRM_F_CI_CUSTLNAINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUSTLNAINFO.unpersist()
F_CI_CUSTR2.unpersist()
OCRM_F_CI_SYS_RESOURCE.unpersist()
F_CI_CUSTR.unpersist()
F_TX_EVENT.unpersist()
F_CR_ODUE2.unpersist()
F_TX_MPUR.unpersist()
F_CR_CARD.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUSTLNAINFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUSTLNAINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
