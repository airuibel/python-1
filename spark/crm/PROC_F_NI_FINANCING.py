#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_NI_FINANCING').setMaster(sys.argv[2])
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
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_TX_FIN_SALEINFO = sqlContext.read.parquet(hdfs+'/F_TX_FIN_SALEINFO/*')
F_TX_FIN_SALEINFO.registerTempTable("F_TX_FIN_SALEINFO")
F_DP_CBOD_CRCRDCOM = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_CRCRDCOM/*')
F_DP_CBOD_CRCRDCOM.registerTempTable("F_DP_CBOD_CRCRDCOM")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT DISTINCT CAST(''  AS   BIGINT)              AS AGREEMENT_ID 
       ,A.PRODCODE              AS PRODUCT_ID 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,B.ODS_CUST_NAME         AS CUST_NAME 
       ,A.ACCTNO                AS ACCOUNT 
       ,''                    AS CNCY 
       ,CAST(NVL(A.LEFTAMT,0) / 100  AS DECIMAL(24,6))                   AS CURRE_AMOUNT 
       ,A.ESTCUSTLYR            AS PRE_INCOME_LOW 
       ,A.ESTCUSTHYR            AS PRE_INCOME_HIGH 
       ,CAST(''     AS DECIMAL(24,6))                 AS MONTH_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS QUARTER_DAILY 
       ,CAST(''   AS DECIMAL(24,6))                   AS YEAR_AVG 
       ,CONCAT(SUBSTR(A.PRODSDATE,1,4), '-',SUBSTR(A.PRODSDATE,5,2),'-' ,SUBSTR(A.PRODSDATE,7,2) )  AS START_DATE 
       ,CONCAT(SUBSTR(A.PRODEDATE,1,4),'-', SUBSTR(A.PRODEDATE,5,2), '-' , SUBSTR(A.PRODEDATE,7,2) )  AS END_DATE 
       ,CAST(A.PRODCODE   AS DECIMAL(19))          AS PRD_TYP 
       ,CONCAT(SUBSTR(A.PRODSDATE,1,4),'-' ,SUBSTR(A.PRODSDATE,5,2),'-',SUBSTR(A.PRODSDATE,7,2)) AS BUY_DATE 
       ,''                    AS CRM_DT 
       ,A.SALEBRNO              AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,B.ODS_CUST_TYPE         AS CUST_TYPE 
       ,C.CARD_NO          AS CARD_NO 
       ,C.CARD_TYPE        AS CARD_TYPE 
       ,A.APPLPER               AS APPLPER 
       ,A.APPLAMT               AS APPLAMT 
   FROM F_TX_FIN_SALEINFO A                                    --认购明细表
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.IDTYPE                = B.CERT_TYPE 
    AND A.IDNO                  = B.CERT_NO 
    AND A.ACCTNAME              = B.SOURCE_CUST_NAME 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN (SELECT A.FK_CRCRD_KEY AS CARD_NO,B.CR_CRD_TYP_COD AS CARD_TYPE,CR_RSV_ACCT_NO AS ODS_ACCT_NO,CR_CURR_COD AS CNCY 
		,A.FR_ID FR_ID
            FROM F_DP_CBOD_CRCRDCOM A
              LEFT JOIN OCRM_F_DP_CARD_INFO B
                 ON A.FK_CRCRD_KEY = B.CR_CRD_NO 
          WHERE A.FR_ID = B.FR_ID
             AND A.ODS_ST_DATE = V_DT
             AND B.ODS_ST_DATE = V_DT
            AND  B.CR_CRD_STS = '2') C 	
	ON A.ACCTNO = C.ODS_ACCT_NO  AND A.FR_ID = C.FR_ID
	"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_NI_FINANCING = sqlContext.sql(sql)
ACRM_F_NI_FINANCING.registerTempTable("ACRM_F_NI_FINANCING")
dfn="ACRM_F_NI_FINANCING/"+V_DT+".parquet"
ACRM_F_NI_FINANCING.cache()
nrows = ACRM_F_NI_FINANCING.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_NI_FINANCING/*.parquet")
ACRM_F_NI_FINANCING.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_NI_FINANCING.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_NI_FINANCING lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
