#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_NI_COLL_PAY_INSURANCE').setMaster(sys.argv[2])
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

F_NI_AFA_BIS_INFO = sqlContext.read.parquet(hdfs+'/F_NI_AFA_BIS_INFO/*')
F_NI_AFA_BIS_INFO.registerTempTable("F_NI_AFA_BIS_INFO")
F_DP_CBOD_CRCRDCOM = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_CRCRDCOM/*')
F_DP_CBOD_CRCRDCOM.registerTempTable("F_DP_CBOD_CRCRDCOM")
F_NI_AFA_ELEC_DKGX = sqlContext.read.parquet(hdfs+'/F_NI_AFA_ELEC_DKGX/*')
F_NI_AFA_ELEC_DKGX.registerTempTable("F_NI_AFA_ELEC_DKGX")
F_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNACN/*')
F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
OCRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_DP_CARD_INFO/*')
OCRM_F_DP_CARD_INFO.registerTempTable("OCRM_F_DP_CARD_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT B.ODS_CUST_NAME         AS CUST_NAME 
       ,A.ACCCODE               AS ACCOUNT 
       ,B.ODS_CUST_NAME         AS ACCOUNT_NAME 
       ,''                    AS IS_OPEN 
       ,CAST(A.SURALLAMOUNT AS DECIMAL(24,6))         AS MONEY 
       ,CAST(0 AS DECIMAL(24,6))                    AS CUST_BUY_COUNT 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,''                    AS PAY_TYP 
       ,CONCAT(SUBSTR(A.WORKDATE,1,4),'-',SUBSTR(A.WORKDATE,5,2),'-',SUBSTR(A.WORKDATE,7,2))  AS BUY_DATE 
       ,A.UNITNO                AS PRODUCT_ID 
       ,A.ODS_ST_DATE           AS CRM_DT 
       ,A.BRNO                  AS ORG_ID 
       ,CAST('' AS BIGINT)                    AS AGREEMENT_ID 
       ,A.FR_ID                 AS FR_ID 
       ,SUBSTR(UNITNO, 1, 1)                       AS CUST_TYPE 
       ,C.CARD_NO          AS CARD_NO 
       ,C.CARD_TYPE                       AS CARD_TYPE 
   FROM F_NI_AFA_BIS_INFO A                                    --缴款主信息登记薄
   LEFT JOIN (SELECT  DISTINCT ODS_CUST_ID ,ODS_CUST_NAME,CERT_TYPE ,CERT_NO,FR_ID FROM OCRM_F_CI_SYS_RESOURCE WHERE ODS_SYS_ID='CEN'
) B                          --系统来源中间表
     ON B.CERT_TYPE             = A.HOLDIDTYPE 
    AND A.HOLDIDNO              = B.CERT_NO 
    AND A.HOLDNAME              = B.ODS_CUST_NAME 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN (SELECT  MAX(A.FK_CRCRD_KEY) AS CARD_NO,MAX(B.CR_CRD_TYP_COD) AS CARD_TYPE,CR_RSV_ACCT_NO AS ODS_ACCT_NO,CR_CURR_COD AS CNCY,A.FR_ID
               FROM F_DP_CBOD_CRCRDCOM A
               JOIN OCRM_F_DP_CARD_INFO B
                 ON A.FK_CRCRD_KEY = B.CR_CRD_NO AND B.FR_ID = A.FR_ID
          WHERE  B.CR_CRD_STS = '2'
		  AND CR_RSV_ACCT_NO IS NOT NULL--卡状态为正常
              AND A.ODS_ST_DATE = V_DT
              AND B.ODS_ST_DATE = V_DT
          GROUP BY CR_RSV_ACCT_NO,CR_CURR_COD,A.FR_ID) C                              --卡公用档
     ON A.FR_ID                 = C.FR_ID  AND A.ACCCODE = C.ODS_ACCT_NO 
  WHERE A.UNITNO IN('000050', '000051', '000052', '000053', '000054', '000055', '000056', '000057', '000058', '000059', '000060', '000061', '000062', '000063', '000064', '000065') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_NI_COLL_PAY_INSURANCE = sqlContext.sql(sql)
ACRM_F_NI_COLL_PAY_INSURANCE.registerTempTable("ACRM_F_NI_COLL_PAY_INSURANCE")
dfn="ACRM_F_NI_COLL_PAY_INSURANCE/"+V_DT+".parquet"
ACRM_F_NI_COLL_PAY_INSURANCE.cache()
nrows = ACRM_F_NI_COLL_PAY_INSURANCE.count()
ACRM_F_NI_COLL_PAY_INSURANCE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_NI_COLL_PAY_INSURANCE.unpersist()
F_NI_AFA_BIS_INFO.unpersist()
OCRM_F_CI_SYS_RESOURCE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_NI_COLL_PAY_INSURANCE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_NI_COLL_PAY_INSURANCE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.HM                    AS CUST_NAME 
       ,A.YHZH                  AS ACCOUNT 
       ,A.ZHHM                  AS ACCOUNT_NAME 
       ,'1'                     AS IS_OPEN 
       ,CAST(0 AS DECIMAL(24,6))                    AS MONEY 
       ,CAST(0 AS DECIMAL(24,6))                    AS CUST_BUY_COUNT 
       ,B.SA_CUST_NO            AS CUST_ID 
       ,A.QYLX                  AS PAY_TYP 
       ,CONCAT(SUBSTR(A.QYRQ,1,4),'-',SUBSTR(A.QYRQ,5,2),'-',SUBSTR(A.QYRQ,7,2))                       AS BUY_DATE 
       ,'800001'                AS PRODUCT_ID 
       ,A.ODS_ST_DATE           AS CRM_DT 
       ,''                    AS ORG_ID 
       ,CAST('' AS BIGINT)                   AS AGREEMENT_ID 
       ,A.FR_ID                 AS FR_ID 
       ,SUBSTR(B.SA_CUST_NO, 1, 1)                       AS CUST_TYPE 
       ,C.CARD_NO          AS CARD_NO 
       ,C.CARD_TYPE                   AS CARD_TYPE 
   FROM F_NI_AFA_ELEC_DKGX A                                   --省级电费代扣关系表
   LEFT JOIN F_DP_CBOD_SAACNACN B                              --活期存款主档
     ON A.YHZH                  = B.SA_ACCT_NO 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN (SELECT  MAX(A.FK_CRCRD_KEY) AS CARD_NO,MAX(B.CR_CRD_TYP_COD) AS CARD_TYPE,CR_RSV_ACCT_NO AS ODS_ACCT_NO,CR_CURR_COD AS CNCY,A.FR_ID
               FROM F_DP_CBOD_CRCRDCOM A
               JOIN OCRM_F_DP_CARD_INFO B
                 ON A.FK_CRCRD_KEY = B.CR_CRD_NO AND B.FR_ID = A.FR_ID
          WHERE  B.CR_CRD_STS = '2'
		  AND CR_RSV_ACCT_NO IS NOT NULL--卡状态为正常
              AND A.ODS_ST_DATE = V_DT
              AND B.ODS_ST_DATE = V_DT
          GROUP BY CR_RSV_ACCT_NO,CR_CURR_COD,A.FR_ID) C                              --卡公用档
     ON A.FR_ID                 = C.FR_ID  AND A.YHZH = C.ODS_ACCT_NO 
  WHERE A.ZT                    = '0' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_NI_COLL_PAY_INSURANCE = sqlContext.sql(sql)
dfn="ACRM_F_NI_COLL_PAY_INSURANCE/"+V_DT+".parquet"
ACRM_F_NI_COLL_PAY_INSURANCE.cache()
nrows = ACRM_F_NI_COLL_PAY_INSURANCE.count()
ACRM_F_NI_COLL_PAY_INSURANCE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_NI_COLL_PAY_INSURANCE.unpersist()
F_NI_AFA_ELEC_DKGX.unpersist()
F_DP_CBOD_SAACNACN.unpersist()
F_DP_CBOD_CRCRDCOM.unpersist()
OCRM_F_DP_CARD_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_NI_COLL_PAY_INSURANCE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
