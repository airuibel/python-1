#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_COM_CUST_INFO').setMaster(sys.argv[2])
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

#清除数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/*.parquet")
#恢复数据到今日数据文件
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet")



F_LN_XDXT_BUSINESS_CONTRACT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BUSINESS_CONTRACT/*')
F_LN_XDXT_BUSINESS_CONTRACT.registerTempTable("F_LN_XDXT_BUSINESS_CONTRACT")
F_CI_CBOD_CICIECIF = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIECIF/*')
F_CI_CBOD_CICIECIF.registerTempTable("F_CI_CBOD_CICIECIF")
F_CI_CBOD_ECCSIATT = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCSIATT/*')
F_CI_CBOD_ECCSIATT.registerTempTable("F_CI_CBOD_ECCSIATT")
F_TX_WSYH_ECCIF = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIF/*')
F_TX_WSYH_ECCIF.registerTempTable("F_TX_WSYH_ECCIF")
F_CI_XDXT_CUSTOMER_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_INFO/*')
F_CI_XDXT_CUSTOMER_INFO.registerTempTable("F_CI_XDXT_CUSTOMER_INFO")
F_CI_CBOD_ECCIFCCR = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCIFCCR/*')  
F_CI_CBOD_ECCIFCCR.registerTempTable("F_CI_CBOD_ECCIFCCR")
F_CI_CBOD_ECCIFIDI = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCIFIDI/*')
F_CI_CBOD_ECCIFIDI.registerTempTable("F_CI_CBOD_ECCIFIDI")
F_CI_WSYH_ECCIFTEL = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFTEL/*')
F_CI_WSYH_ECCIFTEL.registerTempTable("F_CI_WSYH_ECCIFTEL")
MID_CI_CUSTOMER_RELATIVE = sqlContext.read.parquet(hdfs+'/MID_CI_CUSTOMER_RELATIVE/*')
MID_CI_CUSTOMER_RELATIVE.registerTempTable("MID_CI_CUSTOMER_RELATIVE")
F_CI_WSYH_ECCIFID = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFID/*')
F_CI_WSYH_ECCIFID.registerTempTable("F_CI_WSYH_ECCIFID")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_CI_CBOD_ECCIFCCS = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCIFCCS/*')
F_CI_CBOD_ECCIFCCS.registerTempTable("F_CI_CBOD_ECCIFCCS")
MID_SA_ACCOUNT_INFO_USE = sqlContext.read.parquet(hdfs+'/MID_SA_ACCOUNT_INFO_USE/*')
MID_SA_ACCOUNT_INFO_USE.registerTempTable("MID_SA_ACCOUNT_INFO_USE")
F_CI_XDXT_ENT_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_ENT_INFO/*')
F_CI_XDXT_ENT_INFO.registerTempTable("F_CI_XDXT_ENT_INFO")
F_TX_WSYH_ECCIFMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIFMCH/*')
F_TX_WSYH_ECCIFMCH.registerTempTable("F_TX_WSYH_ECCIFMCH")
F_CI_CBOD_CICIEADR = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIEADR/*')
F_CI_CBOD_CICIEADR.registerTempTable("F_CI_CBOD_CICIEADR")
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")



#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT SA_CUST_NO              AS SA_CUST_NO 
       ,SA_CUST_NAME            AS SA_CUST_NAME 
       ,FR_ID                   AS FR_ID 
       ,SA_ACCT_NO              AS SA_ACCT_NO 
       ,ROW_NUMBER() OVER(
      PARTITION BY SA_CUST_NO 
               ,FR_ID ORDER BY SA_CUST_NO)                       AS RANK 
   FROM MID_SA_ACCOUNT_INFO_USE A                              --基本户中间表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_COM_CUST_INFO_01 = sqlContext.sql(sql)
TMP_OCRM_F_CI_COM_CUST_INFO_01.registerTempTable("TMP_OCRM_F_CI_COM_CUST_INFO_01")
dfn="TMP_OCRM_F_CI_COM_CUST_INFO_01/"+V_DT+".parquet"
TMP_OCRM_F_CI_COM_CUST_INFO_01.cache()
nrows = TMP_OCRM_F_CI_COM_CUST_INFO_01.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_COM_CUST_INFO_01/*.parquet")
TMP_OCRM_F_CI_COM_CUST_INFO_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_COM_CUST_INFO_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_COM_CUST_INFO_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT MAX(A.CIFSEQ)                       AS CIFSEQ 
       ,A.CIFNAME               AS CIFNAME 
       ,B.IDTYPE                AS ENTIDTYPE 
       ,B.IDNO                  AS ENTIDNO 
       ,D.TELNO                 AS PHONE 
       ,A.FR_ID                 AS FR_ID 
       ,ROW_NUMBER() OVER(
      PARTITION BY B.IDNO 
               ,A.CIFNAME ORDER BY B.IDNO)                       AS RANK 
   FROM F_TX_WSYH_ECCIF A                                      --电子银行参与方信息表
  INNER JOIN F_CI_WSYH_ECCIFID B                               --客户证件表
     ON A.CIFSEQ                = B.CIFSEQ 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN F_TX_WSYH_ECCIFMCH C                              --客户渠道表
     ON A.CIFSEQ                = C.CIFSEQ 
    AND A.FR_ID                 = C.FR_ID 
    AND C.MCHANNELID            = 'EIBS' 
  INNER JOIN F_CI_WSYH_ECCIFTEL D                              --客户电话表
     ON A.CIFSEQ                = D.CIFSEQ 
    AND A.FR_ID                 = D.FR_ID 
  GROUP BY A.CIFNAME 
       ,B.IDTYPE 
       ,B.IDNO 
       ,D.TELNO,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_COM_CUST_INFO_02 = sqlContext.sql(sql)
TMP_OCRM_F_CI_COM_CUST_INFO_02.registerTempTable("TMP_OCRM_F_CI_COM_CUST_INFO_02")
dfn="TMP_OCRM_F_CI_COM_CUST_INFO_02/"+V_DT+".parquet"
TMP_OCRM_F_CI_COM_CUST_INFO_02.cache()
nrows = TMP_OCRM_F_CI_COM_CUST_INFO_02.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_COM_CUST_INFO_02/*.parquet")
TMP_OCRM_F_CI_COM_CUST_INFO_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_COM_CUST_INFO_02.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_COM_CUST_INFO_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT DISTINCT CAST(0 AS bigint)                      AS ID 
       ,A.ODS_CUST_ID             AS CUST_ID 
       ,''                      AS CUST_NAME 
       ,''                      AS CUST_ZH_NAME 
       ,''                      AS CUST_EN_NAME 
       ,''                      AS CUST_EN_NAME2 
       ,A.CERT_TYPE               AS CERT_TYP 
       ,A.CERT_NO                 AS CERT_NO 
       ,''                      AS COM_SCALE 
       ,''                      AS COM_START_DATE 
       ,''                      AS COM_BELONG 
       ,''                      AS HOLDING_TYP 
       ,''                      AS INDUS_CALSS_MAIN 
       ,''                      AS INDUS_CLAS_DEPUTY 
       ,''                      AS BUS_TYP 
       ,''                      AS ORG_TYP 
       ,''                      AS ECO_TYP 
       ,''                      AS COM_TYP 
       ,''                      AS COM_LEVEL 
       ,''                      AS OTHER_NAME 
       ,''                      AS OBJECT_RATE 
       ,''                      AS SUBJECT_RATE 
       ,''                      AS EFF_DATE 
       ,''                      AS RATE_DATE 
       ,''                      AS CREDIT_LEVEL 
       ,''                      AS LISTING_CORP_TYP 
       ,''                      AS IF_AGRICULTRUE 
       ,''                      AS IF_BANK_SIGNING 
       ,''                      AS IF_SHAREHOLDER 
       ,''                      AS IF_SHARE_CUST 
       ,''                      AS IF_CREDIT_CUST 
       ,'N'                     AS IF_BASIC 
       ,''                      AS IF_ESTATE 
       ,''                      AS IF_HIGH_TECH 
       ,'N'                     AS IF_SMALL 
       ,'N'                     AS IF_IBK 
       ,''                      AS PLICY_TYP 
       ,''                      AS IF_EXPESS 
       ,''                      AS IF_MONITER 
       ,''                      AS IF_FANACING 
       ,'N'                     AS IF_INT 
       ,''                      AS IF_GROUP 
       ,''                      AS RIGHT_FLAG 
       ,''                      AS RATE_RESULT_OUTER 
       ,''                      AS RATE_DATE_OUTER 
       ,''                      AS RATE_ORG_NAME 
       ,''                      AS ENT_QUA_LEVEL 
       ,''                      AS SYN_FLAG 
       ,CAST(0 AS decimal(17,2))  AS FOR_BAL_LIMIT 
       ,''                      AS LINCENSE_NO 
       ,''                      AS ADJUST_TYP 
       ,''                      AS UPGRADE_FLAG 
       ,''                      AS EMERGING_TYP 
       ,''                      AS ESTATE_QUALIFICATION 
       ,''                      AS AREA_ID 
       ,''                      AS UNION_FLAG 
       ,'N'                     AS BLACKLIST_FLAG 
       ,''                      AS AUTH_ORG 
       ,''                      AS OPEN_ORG1 
       ,''                      AS FIRST_OPEN_TYP 
       ,''                      AS OTHER_BANK_ORG 
       ,''                      AS IF_EFFICT_LOANCARD 
       ,''                      AS LOAN_CARDNO 
       ,''                      AS LOAN_CARD_DATE 
       ,''                      AS FIRST_OPEN_DATE 
       ,''                      AS FIRST_LOAN_DATE 
       ,CAST(0 AS decimal(10,6)) AS LOAN_RATE 
       ,CAST(0 AS decimal(10,6)) AS DEP_RATE 
       ,CAST(0 AS decimal(10,6)) AS DEP_RATIO 
       ,CAST(0 AS decimal(10,6)) AS SETTLE_RATIO 
       ,''                      AS BASIC_ACCT 
       ,''                      AS LEGAL_NAME 
       ,''                      AS LEGAL_CERT_NO 
       ,''                      AS LINK_MOBILE 
       ,''                      AS FAX_NO 
       ,''                      AS LINK_TEL_FIN 
       ,''                      AS REGISTER_ADDRESS 
       ,''                      AS REGISTER_ZIP 
       ,''                      AS COUNTRY 
       ,''                      AS PROVINCE 
       ,''                      AS WORK_ADDRESS 
       ,''                      AS E_MAIL 
       ,''                      AS WEB_ADDRESS 
       ,''                      AS CONTROLLER_NAME 
       ,''                      AS CONTROLLER_CERT_TYP 
       ,''                      AS CONTROLLER_CERT_NO 
       ,''                      AS LINK_TEL 
       ,''                      AS OPEN_ORG2 
       ,''                      AS OPEN_DATE 
       ,''                      AS REG_CCY 
       ,''                      AS REG_CAPITAL 
       ,''                      AS BUSINESS 
       ,''                      AS EMPLOYEE_NUM 
       ,CAST(0 AS DECIMAL(24,6)) AS TOTAL_ASSET 
       ,CAST(0 AS DECIMAL(24,6)) AS SALE_ASSET 
       ,''                      AS TAX_NO 
       ,''                      AS RENT_NO 
       ,''                      AS LAST_DATE 
       ,''                      AS BOND_FLAG 
       ,CAST(0 AS DECIMAL(24,6)) AS BUS_AREA 
       ,''                      AS BUS_OWNER 
       ,''                      AS BUS_STAT 
       ,''                      AS INCOME_CCY 
       ,CAST(0 AS DECIMAL(24,6)) AS INCOME_SETTLE 
       ,''                      AS TAXPAYER_SCALE 
       ,''                      AS MERGE_SYS_ID 
       ,''                      AS BELONG_SYS_ID 
       ,''                      AS MERGE_ORG 
       ,''                      AS MERGE_DATE 
       ,''                      AS MERGE_OFFICER 
       ,''                      AS REMARK1 
       ,''                      AS BIRTH_DATE 
       ,''                      AS KEY_CERT_NO 
       ,''                      AS KEY_CERT_TYP 
       ,''                      AS KEY_CUST_ID 
       ,''                      AS KEY_PEOPLE_NAME 
       ,''                      AS EDU_LEVEL 
       ,''                      AS WORK_YEAR 
       ,''                      AS HOUSE_ADDRESS 
       ,''                      AS HOUSE_ZIP 
       ,''                      AS DUTY_TIME 
       ,''                      AS SHARE_HOLDING 
       ,''                      AS DUTY 
       ,''                      AS REMARK2 
       ,''                      AS SEX 
       ,''                      AS SOCIAL_INSURE_NO 
       ,V_DT                    AS ODS_ST_DATE 
       ,''                      AS GREEN_FLAG 
       ,''                      AS IS_MODIFY 
       ,A.FR_ID                   AS FR_ID 
       ,''                      AS LONGITUDE 
       ,''                      AS LATITUDE 
       ,''                      AS GPS 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO B                         --对公客户表
     ON A.ODS_CUST_ID           = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.ODS_CUST_TYPE         = '2' 
    AND CUST_STAT               = '1' 
    AND A.ODS_CUST_ID IS NOT NULL 
    AND A.ODS_SYS_ID            = 'CEN' 
    AND A.CERT_FLAG             = 'Y' 
    AND A.ODS_ST_DATE           = V_DT 
    AND B.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO.cache()
nrows = OCRM_F_CI_COM_CUST_INFO.count()
OCRM_F_CI_COM_CUST_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_COM_CUST_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT DISTINCT CAST(0 AS bigint)                      AS ID 
       ,ODS_CUST_ID             AS CUST_ID 
       ,''                      AS CUST_NAME 
       ,''                      AS CUST_ZH_NAME 
       ,''                      AS CUST_EN_NAME 
       ,''                      AS CUST_EN_NAME2 
       ,''                      AS CERT_TYP 
       ,''                      AS CERT_NO 
       ,''                      AS COM_SCALE 
       ,''                      AS COM_START_DATE 
       ,''                      AS COM_BELONG 
       ,''                      AS HOLDING_TYP 
       ,''                      AS INDUS_CALSS_MAIN 
       ,''                      AS INDUS_CLAS_DEPUTY 
       ,''                      AS BUS_TYP 
       ,''                      AS ORG_TYP 
       ,''                      AS ECO_TYP 
       ,''                      AS COM_TYP 
       ,''                      AS COM_LEVEL 
       ,''                      AS OTHER_NAME 
       ,''                      AS OBJECT_RATE 
       ,''                      AS SUBJECT_RATE 
       ,''                      AS EFF_DATE 
       ,''                      AS RATE_DATE 
       ,''                      AS CREDIT_LEVEL 
       ,''                      AS LISTING_CORP_TYP 
       ,''                      AS IF_AGRICULTRUE 
       ,''                      AS IF_BANK_SIGNING 
       ,''                      AS IF_SHAREHOLDER 
       ,''                      AS IF_SHARE_CUST 
       ,''                      AS IF_CREDIT_CUST 
       ,'N'                     AS IF_BASIC 
       ,''                      AS IF_ESTATE 
       ,''                      AS IF_HIGH_TECH 
       ,'N'                     AS IF_SMALL 
       ,'N'                     AS IF_IBK 
       ,''                      AS PLICY_TYP 
       ,''                      AS IF_EXPESS 
       ,''                      AS IF_MONITER 
       ,''                      AS IF_FANACING 
       ,'N'                     AS IF_INT 
       ,''                      AS IF_GROUP 
       ,''                      AS RIGHT_FLAG 
       ,''                      AS RATE_RESULT_OUTER 
       ,''                      AS RATE_DATE_OUTER 
       ,''                      AS RATE_ORG_NAME 
       ,''                      AS ENT_QUA_LEVEL 
       ,''                      AS SYN_FLAG 
       ,CAST(0 AS decimal(17,2))  AS FOR_BAL_LIMIT 
       ,''                      AS LINCENSE_NO 
       ,''                      AS ADJUST_TYP 
       ,''                      AS UPGRADE_FLAG 
       ,''                      AS EMERGING_TYP 
       ,''                      AS ESTATE_QUALIFICATION 
       ,''                      AS AREA_ID 
       ,''                      AS UNION_FLAG 
       ,'N'                     AS BLACKLIST_FLAG 
       ,''                      AS AUTH_ORG 
       ,''                      AS OPEN_ORG1 
       ,''                      AS FIRST_OPEN_TYP 
       ,''                      AS OTHER_BANK_ORG 
       ,''                      AS IF_EFFICT_LOANCARD 
       ,''                      AS LOAN_CARDNO 
       ,''                      AS LOAN_CARD_DATE 
       ,''                      AS FIRST_OPEN_DATE 
       ,''                      AS FIRST_LOAN_DATE 
       ,CAST(0 AS decimal(10,6))  AS LOAN_RATE 
       ,CAST(0 AS decimal(10,6))  AS DEP_RATE 
       ,CAST(0 AS decimal(10,6))  AS DEP_RATIO 
       ,CAST(0 AS decimal(10,6))  AS SETTLE_RATIO 
       ,''                      AS BASIC_ACCT 
       ,''                      AS LEGAL_NAME 
       ,''                      AS LEGAL_CERT_NO 
       ,''                      AS LINK_MOBILE 
       ,''                      AS FAX_NO 
       ,''                      AS LINK_TEL_FIN 
       ,''                      AS REGISTER_ADDRESS 
       ,''                      AS REGISTER_ZIP 
       ,''                      AS COUNTRY 
       ,''                      AS PROVINCE 
       ,''                      AS WORK_ADDRESS 
       ,''                      AS E_MAIL 
       ,''                      AS WEB_ADDRESS 
       ,''                      AS CONTROLLER_NAME 
       ,''                      AS CONTROLLER_CERT_TYP 
       ,''                      AS CONTROLLER_CERT_NO 
       ,''                      AS LINK_TEL 
       ,''                      AS OPEN_ORG2 
       ,''                      AS OPEN_DATE 
       ,''                      AS REG_CCY 
       ,''                      AS REG_CAPITAL 
       ,''                      AS BUSINESS 
       ,''                      AS EMPLOYEE_NUM 
       ,CAST(0 AS DECIMAL(24,6)) AS TOTAL_ASSET 
       ,CAST(0 AS DECIMAL(24,6)) AS SALE_ASSET 
       ,''                      AS TAX_NO 
       ,''                      AS RENT_NO 
       ,''                      AS LAST_DATE 
       ,''                      AS BOND_FLAG 
       ,CAST(0 AS DECIMAL(24,6)) AS BUS_AREA 
       ,''                      AS BUS_OWNER 
       ,''                      AS BUS_STAT 
       ,''                      AS INCOME_CCY 
       ,CAST(0 AS DECIMAL(24,6)) AS INCOME_SETTLE 
       ,''                      AS TAXPAYER_SCALE 
       ,''                      AS MERGE_SYS_ID 
       ,''                      AS BELONG_SYS_ID 
       ,''                      AS MERGE_ORG 
       ,''                      AS MERGE_DATE 
       ,''                      AS MERGE_OFFICER 
       ,''                      AS REMARK1 
       ,''                      AS BIRTH_DATE 
       ,''                      AS KEY_CERT_NO 
       ,''                      AS KEY_CERT_TYP 
       ,''                      AS KEY_CUST_ID 
       ,''                      AS KEY_PEOPLE_NAME 
       ,''                      AS EDU_LEVEL 
       ,''                      AS WORK_YEAR 
       ,''                      AS HOUSE_ADDRESS 
       ,''                      AS HOUSE_ZIP 
       ,''                      AS DUTY_TIME 
       ,''                      AS SHARE_HOLDING 
       ,''                      AS DUTY 
       ,''                      AS REMARK2 
       ,''                      AS SEX 
       ,''                      AS SOCIAL_INSURE_NO 
       ,V_DT                    AS ODS_ST_DATE 
       ,''                      AS GREEN_FLAG 
       ,''                      AS IS_MODIFY 
       ,A.FR_ID                   AS FR_ID 
       ,''                      AS LONGITUDE 
       ,''                      AS LATITUDE 
       ,''                      AS GPS 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO B                         --对公客户表
     ON A.ODS_CUST_ID           = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.ODS_CUST_TYPE         = '2' 
    AND CUST_STAT               = '1' 
    AND A.ODS_CUST_ID IS 
    NOT NULL 
    AND A.ODS_SYS_ID <> 'CEN' 
    AND A.ODS_ST_DATE           = V_DT 
    AND B.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO.cache()
nrows = OCRM_F_CI_COM_CUST_INFO.count()
OCRM_F_CI_COM_CUST_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_COM_CUST_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-05::
V_STEP = V_STEP + 1

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT M.ID                    AS ID 
       ,M.CUST_ID               AS CUST_ID 
       ,COALESCE(A.CI_CHN_NAME, M.CUST_NAME)                       AS CUST_NAME 
       ,COALESCE(A.CI_CHN_NAME, M.CUST_ZH_NAME)                       AS CUST_ZH_NAME 
       ,COALESCE(A.CI_ENG_SPL_NAME_1, M.CUST_EN_NAME)                       AS CUST_EN_NAME 
       ,COALESCE(A.CI_ENG_SPL_NAME_2, M.CUST_EN_NAME2)                       AS CUST_EN_NAME2 
       ,M.CERT_TYP              AS CERT_TYP 
       ,M.CERT_NO               AS CERT_NO 
       ,COALESCE(A.CI_ENTP_SCAL, M.COM_SCALE)                       AS COM_SCALE 
       ,M.COM_START_DATE        AS COM_START_DATE 
       ,M.COM_BELONG            AS COM_BELONG 
       ,M.HOLDING_TYP           AS HOLDING_TYP 
       ,COALESCE(CASE WHEN A.CI_BUSNTP = 'X' THEN NULL ELSE A.CI_BUSNTP END,M.INDUS_CALSS_MAIN) AS INDUS_CALSS_MAIN 
       --,COALESCE(DECODE(A.CI_BUSNTP, 'X', NULL, A.CI_BUSNTP), M.INDUS_CALSS_MAIN) AS INDUS_CALSS_MAIN 
       ,M.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,COALESCE(A.CI_ADMN_TYP, M.BUS_TYP)                       AS BUS_TYP 
       ,COALESCE(A.CI_ECON_CHAR, M.ORG_TYP)                       AS ORG_TYP 
       ,M.ECO_TYP               AS ECO_TYP 
       ,M.COM_TYP               AS COM_TYP 
       ,M.COM_LEVEL             AS COM_LEVEL 
       ,M.OTHER_NAME            AS OTHER_NAME 
       ,M.OBJECT_RATE           AS OBJECT_RATE 
       ,M.SUBJECT_RATE          AS SUBJECT_RATE 
       ,M.EFF_DATE              AS EFF_DATE 
       ,M.RATE_DATE             AS RATE_DATE 
       ,M.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,M.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,M.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,M.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,M.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,M.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,M.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,M.IF_BASIC              AS IF_BASIC 
       ,M.IF_ESTATE             AS IF_ESTATE 
       ,M.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,M.IF_SMALL              AS IF_SMALL 
       ,M.IF_IBK                AS IF_IBK 
       ,M.PLICY_TYP             AS PLICY_TYP 
       ,M.IF_EXPESS             AS IF_EXPESS 
       ,M.IF_MONITER            AS IF_MONITER 
       ,M.IF_FANACING           AS IF_FANACING 
       ,M.IF_INT                AS IF_INT 
       ,M.IF_GROUP              AS IF_GROUP 
       ,M.RIGHT_FLAG            AS RIGHT_FLAG 
       ,M.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,M.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,M.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,COALESCE(CASE WHEN A.CI_ENTP_QUAL_COD = 'X' THEN NULL ELSE A.CI_ENTP_QUAL_COD END, M.ENT_QUA_LEVEL) AS ENT_QUA_LEVEL
       --,COALESCE(DECODE(A.CI_ENTP_QUAL_COD, 'X', NULL, A.CI_ENTP_QUAL_COD), M.ENT_QUA_LEVEL) AS ENT_QUA_LEVEL 
       ,M.SYN_FLAG              AS SYN_FLAG 
       ,COALESCE(A.CI_FCURR_BAL_CONTR, M.FOR_BAL_LIMIT)                       AS FOR_BAL_LIMIT 
       ,COALESCE(A.CI_OPAC_PERM_NO, M.LINCENSE_NO)                       AS LINCENSE_NO 
       ,M.ADJUST_TYP            AS ADJUST_TYP 
       ,M.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,M.EMERGING_TYP          AS EMERGING_TYP 
       ,M.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,M.AREA_ID               AS AREA_ID 
       ,M.UNION_FLAG            AS UNION_FLAG 
       ,M.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,COALESCE(A.CI_MANG_DEPT, M.AUTH_ORG)                       AS AUTH_ORG 
       ,COALESCE(A.CI_CRT_ORG, M.OPEN_ORG1)                       AS OPEN_ORG1 
       ,M.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,M.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,M.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,M.LOAN_CARDNO           AS LOAN_CARDNO 
       ,M.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,M.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,M.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,M.LOAN_RATE             AS LOAN_RATE 
       ,M.DEP_RATE              AS DEP_RATE 
       ,M.DEP_RATIO             AS DEP_RATIO 
       ,M.SETTLE_RATIO          AS SETTLE_RATIO 
       ,M.BASIC_ACCT            AS BASIC_ACCT 
       ,M.LEGAL_NAME            AS LEGAL_NAME 
       ,M.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,M.LINK_MOBILE           AS LINK_MOBILE 
       ,M.FAX_NO                AS FAX_NO 
       ,M.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,M.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,M.REGISTER_ZIP          AS REGISTER_ZIP 
       ,M.COUNTRY               AS COUNTRY 
       ,M.PROVINCE              AS PROVINCE 
       ,M.WORK_ADDRESS          AS WORK_ADDRESS 
       ,M.E_MAIL                AS E_MAIL 
       ,M.WEB_ADDRESS           AS WEB_ADDRESS 
       ,M.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,M.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,M.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,M.LINK_TEL              AS LINK_TEL 
       ,M.OPEN_ORG2             AS OPEN_ORG2 
       ,M.OPEN_DATE             AS OPEN_DATE 
       --,COALESCE(DECODE(A.CI_REG_CAP_CURR_COD, 'X', NULL, A.CI_REG_CAP_CURR_COD), M.REG_CCY) AS REG_CCY 
       ,COALESCE(CASE WHEN A.CI_REG_CAP_CURR_COD = 'X' THEN NULL ELSE A.CI_REG_CAP_CURR_COD END, M.REG_CCY) AS REG_CCY 
       ,COALESCE(A.CI_REG_CAP, M.REG_CAPITAL)                       AS REG_CAPITAL 
       ,COALESCE(A.CI_ADMN_TYP, M.BUSINESS)                       AS BUSINESS 
       ,COALESCE(C.EC_STF_NUM_N, M.EMPLOYEE_NUM)                       AS EMPLOYEE_NUM 
       ,COALESCE(C.EC_TOT_ASSETS, M.TOTAL_ASSET)                       AS TOTAL_ASSET 
       ,M.SALE_ASSET            AS SALE_ASSET 
       ,COALESCE(A.CI_TABS_REG_NO, M.TAX_NO)                       AS TAX_NO 
       ,COALESCE(A.CI_TABS_REG_NO, M.RENT_NO)                       AS RENT_NO 
       ,M.LAST_DATE             AS LAST_DATE 
       ,M.BOND_FLAG             AS BOND_FLAG 
       ,M.BUS_AREA              AS BUS_AREA 
       ,M.BUS_OWNER             AS BUS_OWNER 
       ,M.BUS_STAT              AS BUS_STAT 
       ,COALESCE(A.CI_REG_CAP_CURR_COD, M.INCOME_CCY)                       AS INCOME_CCY 
       ,COALESCE(A.CI_CAP_INHD, M.INCOME_SETTLE)                       AS INCOME_SETTLE 
       --,COALESCE(DECODE(A.CI_TAXR_SCAL, 'X', NULL, A.CI_TAXR_SCAL), M.TAXPAYER_SCALE) AS TAXPAYER_SCALE
       ,COALESCE(CASE WHEN A.CI_TAXR_SCAL = 'X' THEN NULL ELSE A.CI_TAXR_SCAL END, M.TAXPAYER_SCALE) AS TAXPAYER_SCALE 
       ,M.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,M.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,M.MERGE_ORG             AS MERGE_ORG 
       ,M.MERGE_DATE            AS MERGE_DATE 
       ,M.MERGE_OFFICER         AS MERGE_OFFICER 
       ,M.REMARK1               AS REMARK1 
       ,M.BIRTH_DATE            AS BIRTH_DATE 
       ,M.KEY_CERT_NO           AS KEY_CERT_NO 
       ,M.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,M.KEY_CUST_ID           AS KEY_CUST_ID 
       ,M.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,M.EDU_LEVEL             AS EDU_LEVEL 
       ,M.WORK_YEAR             AS WORK_YEAR 
       ,M.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,M.HOUSE_ZIP             AS HOUSE_ZIP 
       ,M.DUTY_TIME             AS DUTY_TIME 
       ,M.SHARE_HOLDING         AS SHARE_HOLDING 
       ,M.DUTY                  AS DUTY 
       ,M.REMARK2               AS REMARK2 
       ,M.SEX                   AS SEX 
       ,M.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,V_DT                    AS ODS_ST_DATE 
       ,M.GREEN_FLAG            AS GREEN_FLAG 
       ,M.IS_MODIFY             AS IS_MODIFY 
       ,M.FR_ID                 AS FR_ID 
       ,M.LONGITUDE             AS LONGITUDE 
       ,M.LATITUDE              AS LATITUDE 
       ,M.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO M                              --对公客户信息表
  INNER JOIN F_CI_CBOD_CICIECIF A                              --单位客户信息档
     ON M.FR_ID                 = A.FR_ID 
    AND M.CUST_ID               = A.CI_CUST_NO 
   LEFT JOIN F_CI_CBOD_ECCIFCCS C                              --对公客户补充信息档
     ON A.CI_CUST_NO            = C.EC_CUST_NO 
    AND C.FR_ID                 = A.FR_ID 
  WHERE A.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --0:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/")

#任务[12] 001-06::
V_STEP = V_STEP + 1

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.COM_BELONG            AS COM_BELONG 
       ,A.HOLDING_TYP           AS HOLDING_TYP 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.OBJECT_RATE           AS OBJECT_RATE 
       ,A.SUBJECT_RATE          AS SUBJECT_RATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.RATE_DATE             AS RATE_DATE 
       ,A.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,'Y'                     AS IF_BASIC 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,A.IF_SMALL              AS IF_SMALL 
       ,A.IF_IBK                AS IF_IBK 
       ,A.PLICY_TYP             AS PLICY_TYP 
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,A.AREA_ID               AS AREA_ID 
       ,A.UNION_FLAG            AS UNION_FLAG 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.AUTH_ORG              AS AUTH_ORG 
       ,A.OPEN_ORG1             AS OPEN_ORG1 
       ,A.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,A.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LOAN_CARDNO           AS LOAN_CARDNO 
       ,A.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,A.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,A.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,A.LOAN_RATE             AS LOAN_RATE 
       ,A.DEP_RATE              AS DEP_RATE 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.SETTLE_RATIO          AS SETTLE_RATIO 
       ,COALESCE(B.SA_ACCT_NO, A.BASIC_ACCT)                       AS BASIC_ACCT 
       ,A.LEGAL_NAME            AS LEGAL_NAME 
       ,A.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,A.LINK_MOBILE           AS LINK_MOBILE 
       ,A.FAX_NO                AS FAX_NO 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.REGISTER_ZIP          AS REGISTER_ZIP 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.E_MAIL                AS E_MAIL 
       ,A.WEB_ADDRESS           AS WEB_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,A.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.OPEN_DATE             AS OPEN_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.LAST_DATE             AS LAST_DATE 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_OWNER             AS BUS_OWNER 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,A.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,A.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,A.MERGE_ORG             AS MERGE_ORG 
       ,A.MERGE_DATE            AS MERGE_DATE 
       ,A.MERGE_OFFICER         AS MERGE_OFFICER 
       ,A.REMARK1               AS REMARK1 
       ,A.BIRTH_DATE            AS BIRTH_DATE 
       ,A.KEY_CERT_NO           AS KEY_CERT_NO 
       ,A.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,A.KEY_CUST_ID           AS KEY_CUST_ID 
       ,A.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,A.EDU_LEVEL             AS EDU_LEVEL 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,A.HOUSE_ZIP             AS HOUSE_ZIP 
       ,A.DUTY_TIME             AS DUTY_TIME 
       ,A.SHARE_HOLDING         AS SHARE_HOLDING 
       ,A.DUTY                  AS DUTY 
       ,A.REMARK2               AS REMARK2 
       ,A.SEX                   AS SEX 
       ,A.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.GREEN_FLAG            AS GREEN_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户信息表
  INNER JOIN TMP_OCRM_F_CI_COM_CUST_INFO_01 B                  --对公客户信息表临时表01
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.SA_CUST_NO 
    AND CUST_NAME               = B.SA_CUST_NAME 
  WHERE B.RANK                  = 1 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --0:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.CUST_NAME           = DST.CUST_NAME 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/")

#任务[12] 001-07::
V_STEP = V_STEP + 1

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.COM_BELONG            AS COM_BELONG 
       ,A.HOLDING_TYP           AS HOLDING_TYP 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.OBJECT_RATE           AS OBJECT_RATE 
       ,A.SUBJECT_RATE          AS SUBJECT_RATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.RATE_DATE             AS RATE_DATE 
       ,A.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,'Y'                     AS IF_SMALL 
       ,A.IF_IBK                AS IF_IBK 
       ,A.PLICY_TYP             AS PLICY_TYP 
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,A.AREA_ID               AS AREA_ID 
       ,A.UNION_FLAG            AS UNION_FLAG 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.AUTH_ORG              AS AUTH_ORG 
       ,A.OPEN_ORG1             AS OPEN_ORG1 
       ,A.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,A.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LOAN_CARDNO           AS LOAN_CARDNO 
       ,A.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,A.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,A.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,A.LOAN_RATE             AS LOAN_RATE 
       ,A.DEP_RATE              AS DEP_RATE 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.SETTLE_RATIO          AS SETTLE_RATIO 
       ,A.BASIC_ACCT            AS BASIC_ACCT 
       ,A.LEGAL_NAME            AS LEGAL_NAME 
       ,A.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,A.LINK_MOBILE           AS LINK_MOBILE 
       ,A.FAX_NO                AS FAX_NO 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.REGISTER_ZIP          AS REGISTER_ZIP 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.E_MAIL                AS E_MAIL 
       ,A.WEB_ADDRESS           AS WEB_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,A.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.OPEN_DATE             AS OPEN_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.LAST_DATE             AS LAST_DATE 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_OWNER             AS BUS_OWNER 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,A.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,A.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,A.MERGE_ORG             AS MERGE_ORG 
       ,A.MERGE_DATE            AS MERGE_DATE 
       ,A.MERGE_OFFICER         AS MERGE_OFFICER 
       ,A.REMARK1               AS REMARK1 
       ,A.BIRTH_DATE            AS BIRTH_DATE 
       ,A.KEY_CERT_NO           AS KEY_CERT_NO 
       ,A.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,A.KEY_CUST_ID           AS KEY_CUST_ID 
       ,A.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,A.EDU_LEVEL             AS EDU_LEVEL 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,A.HOUSE_ZIP             AS HOUSE_ZIP 
       ,A.DUTY_TIME             AS DUTY_TIME 
       ,A.SHARE_HOLDING         AS SHARE_HOLDING 
       ,A.DUTY                  AS DUTY 
       ,A.REMARK2               AS REMARK2 
       ,A.SEX                   AS SEX 
       ,A.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.GREEN_FLAG            AS GREEN_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户信息表
  INNER JOIN F_CI_CBOD_CICIECIF B                              --单位客户信息档
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CI_CUST_NO 
  WHERE CI_ENTP_SCAL IN('CS03', 'CS04') 
    AND B.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --0:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/")

#任务[12] 001-08::
V_STEP = V_STEP + 1

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.COM_BELONG            AS COM_BELONG 
       ,A.HOLDING_TYP           AS HOLDING_TYP 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.OBJECT_RATE           AS OBJECT_RATE 
       ,A.SUBJECT_RATE          AS SUBJECT_RATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.RATE_DATE             AS RATE_DATE 
       ,A.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,A.IF_SMALL              AS IF_SMALL 
       ,A.IF_IBK                AS IF_IBK 
       ,A.PLICY_TYP             AS PLICY_TYP 
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,A.AREA_ID               AS AREA_ID 
       ,A.UNION_FLAG            AS UNION_FLAG 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.AUTH_ORG              AS AUTH_ORG 
       ,A.OPEN_ORG1             AS OPEN_ORG1 
       ,A.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,A.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LOAN_CARDNO           AS LOAN_CARDNO 
       ,A.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,A.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,A.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,A.LOAN_RATE             AS LOAN_RATE 
       ,A.DEP_RATE              AS DEP_RATE 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.SETTLE_RATIO          AS SETTLE_RATIO 
       ,A.BASIC_ACCT            AS BASIC_ACCT 
       ,A.LEGAL_NAME            AS LEGAL_NAME 
       ,A.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,COALESCE(B.CI_TEL_NO, A.LINK_MOBILE)                       AS LINK_MOBILE 
       ,A.FAX_NO                AS FAX_NO 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,COALESCE(B.CI_ADDR, A.REGISTER_ADDRESS)                       AS REGISTER_ADDRESS 
       ,A.REGISTER_ZIP          AS REGISTER_ZIP 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,COALESCE(B.CI_ADDR, A.WORK_ADDRESS)                       AS WORK_ADDRESS 
       ,A.E_MAIL                AS E_MAIL 
       ,A.WEB_ADDRESS           AS WEB_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,A.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.OPEN_DATE             AS OPEN_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.LAST_DATE             AS LAST_DATE 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_OWNER             AS BUS_OWNER 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,A.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,A.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,A.MERGE_ORG             AS MERGE_ORG 
       ,A.MERGE_DATE            AS MERGE_DATE 
       ,A.MERGE_OFFICER         AS MERGE_OFFICER 
       ,A.REMARK1               AS REMARK1 
       ,A.BIRTH_DATE            AS BIRTH_DATE 
       ,A.KEY_CERT_NO           AS KEY_CERT_NO 
       ,A.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,A.KEY_CUST_ID           AS KEY_CUST_ID 
       ,A.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,A.EDU_LEVEL             AS EDU_LEVEL 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,A.HOUSE_ZIP             AS HOUSE_ZIP 
       ,A.DUTY_TIME             AS DUTY_TIME 
       ,A.SHARE_HOLDING         AS SHARE_HOLDING 
       ,A.DUTY                  AS DUTY 
       ,A.REMARK2               AS REMARK2 
       ,A.SEX                   AS SEX 
       ,A.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.GREEN_FLAG            AS GREEN_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户信息表
  INNER JOIN F_CI_CBOD_CICIEADR B                              --对公客户地址信息表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.FK_CICIF_KEY 
  WHERE CI_ADDR_COD             = 'OFF' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --0:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/")

#任务[12] 001-09::
V_STEP = V_STEP + 1

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.COM_BELONG            AS COM_BELONG 
       ,A.HOLDING_TYP           AS HOLDING_TYP 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.OBJECT_RATE           AS OBJECT_RATE 
       ,A.SUBJECT_RATE          AS SUBJECT_RATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.RATE_DATE             AS RATE_DATE 
       ,A.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,A.IF_SMALL              AS IF_SMALL 
       ,A.IF_IBK                AS IF_IBK 
       ,A.PLICY_TYP             AS PLICY_TYP 
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,A.AREA_ID               AS AREA_ID 
       ,A.UNION_FLAG            AS UNION_FLAG 
       ,'Y'                     AS BLACKLIST_FLAG 
       ,A.AUTH_ORG              AS AUTH_ORG 
       ,A.OPEN_ORG1             AS OPEN_ORG1 
       ,A.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,A.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LOAN_CARDNO           AS LOAN_CARDNO 
       ,A.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,A.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,A.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,A.LOAN_RATE             AS LOAN_RATE 
       ,A.DEP_RATE              AS DEP_RATE 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.SETTLE_RATIO          AS SETTLE_RATIO 
       ,A.BASIC_ACCT            AS BASIC_ACCT 
       ,A.LEGAL_NAME            AS LEGAL_NAME 
       ,A.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,A.LINK_MOBILE           AS LINK_MOBILE 
       ,A.FAX_NO                AS FAX_NO 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.REGISTER_ZIP          AS REGISTER_ZIP 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.E_MAIL                AS E_MAIL 
       ,A.WEB_ADDRESS           AS WEB_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,A.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.OPEN_DATE             AS OPEN_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.LAST_DATE             AS LAST_DATE 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_OWNER             AS BUS_OWNER 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,A.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,A.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,A.MERGE_ORG             AS MERGE_ORG 
       ,A.MERGE_DATE            AS MERGE_DATE 
       ,A.MERGE_OFFICER         AS MERGE_OFFICER 
       ,A.REMARK1               AS REMARK1 
       ,A.BIRTH_DATE            AS BIRTH_DATE 
       ,A.KEY_CERT_NO           AS KEY_CERT_NO 
       ,A.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,A.KEY_CUST_ID           AS KEY_CUST_ID 
       ,A.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,A.EDU_LEVEL             AS EDU_LEVEL 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,A.HOUSE_ZIP             AS HOUSE_ZIP 
       ,A.DUTY_TIME             AS DUTY_TIME 
       ,A.SHARE_HOLDING         AS SHARE_HOLDING 
       ,A.DUTY                  AS DUTY 
       ,A.REMARK2               AS REMARK2 
       ,A.SEX                   AS SEX 
       ,A.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.GREEN_FLAG            AS GREEN_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户信息表
  INNER JOIN F_CI_CBOD_ECCSIATT B                              --关注客户清单档
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.EC_CUST_NO 
  WHERE EC_ATT_TYP              = '04' 
    AND B.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --0:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/")

#任务[12] 001-10::
V_STEP = V_STEP + 1

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT E.ID                    AS ID 
       ,E.CUST_ID               AS CUST_ID 
       ,COALESCE(A.CUSTOMERNAME, E.CUST_NAME)                       AS CUST_NAME 
       ,E.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,COALESCE(B.ENGLISHNAME, E.CUST_EN_NAME)                       AS CUST_EN_NAME 
       ,E.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,COALESCE(CASE WHEN A.CERTTYPE = 'X' THEN NULL ELSE A.CERTTYPE END, E.CERT_TYP) AS CERT_TYP 
       ,COALESCE(A.CERTID, E.CERT_NO)                       AS CERT_NO 
       ,COALESCE(B.SCOPE, E.COM_SCALE)                       AS COM_SCALE 
       ,COALESCE(B.SETUPDATE, E.COM_START_DATE)                       AS COM_START_DATE 
       ,COALESCE(B.ENTERPRISEBELONG, E.COM_BELONG)                       AS COM_BELONG 
       ,E.HOLDING_TYP           AS HOLDING_TYP 
       ,E.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,COALESCE(CASE WHEN B.INDUSTRYTYPE = '-' THEN NULL ELSE B.INDUSTRYTYPE END, E.INDUS_CLAS_DEPUTY) AS INDUS_CLAS_DEPUTY 
       ,E.BUS_TYP               AS BUS_TYP 
       ,COALESCE(CASE WHEN B.ORGTYPE = '-' THEN NULL ELSE B.ORGTYPE END, E.ORG_TYP) AS ORG_TYP 
       ,COALESCE(B.ECONOMYTYPE, E.ECO_TYP)                       AS ECO_TYP 
       ,COALESCE(B.ORGNATURE, E.COM_TYP)                       AS COM_TYP 
       ,COALESCE(B.AGRLEVEL, E.COM_LEVEL)                       AS COM_LEVEL 
       ,E.OTHER_NAME            AS OTHER_NAME 
       ,E.OBJECT_RATE           AS OBJECT_RATE 
       ,E.SUBJECT_RATE          AS SUBJECT_RATE 
       ,COALESCE(B.ACCCREDITUSETIME, E.EFF_DATE)                       AS EFF_DATE 
       ,COALESCE(B.RATETIME, E.RATE_DATE)                       AS RATE_DATE 
       ,COALESCE(B.CREDITLEVEL, E.CREDIT_LEVEL)                       AS CREDIT_LEVEL 
       ,COALESCE(B.LISTINGCORPORNOT, E.LISTING_CORP_TYP)                       AS LISTING_CORP_TYP 
       ,COALESCE(B.AGRRELATE, E.IF_AGRICULTRUE)                       AS IF_AGRICULTRUE 
       ,E.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,COALESCE(B.MYBANKDORM, E.IF_SHAREHOLDER)                       AS IF_SHAREHOLDER 
       ,E.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,E.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,E.IF_BASIC              AS IF_BASIC 
       ,COALESCE(B.REALTYFLAG, E.IF_ESTATE)                       AS IF_ESTATE 
       ,COALESCE(B.NEWTECHCORPORNOT, E.IF_HIGH_TECH)                       AS IF_HIGH_TECH 
       ,COALESCE(CASE WHEN A.CUSTOMERTYPE          = '0120' THEN 'Y' ELSE 'N' END, E.IF_SMALL)                       AS IF_SMALL 
       ,E.IF_IBK                AS IF_IBK 
       ,E.PLICY_TYP             AS PLICY_TYP 
       ,E.IF_EXPESS             AS IF_EXPESS 
       ,E.IF_MONITER            AS IF_MONITER 
       ,E.IF_FANACING           AS IF_FANACING 
       ,E.IF_INT                AS IF_INT 
       ,COALESCE(CASE WHEN B.ECGROUPFLAG           = '1' THEN 'Y' ELSE 'N' END, E.IF_GROUP)                       AS IF_GROUP 
       ,COALESCE(B.HASIERIGHT, E.RIGHT_FLAG)                       AS RIGHT_FLAG 
       ,COALESCE(B.OTHERCREDITLEVEL, E.RATE_RESULT_OUTER)                       AS RATE_RESULT_OUTER 
       ,COALESCE(B.OTHEREVALUATEDATE, E.RATE_DATE_OUTER)                       AS RATE_DATE_OUTER 
       ,COALESCE(B.OTHERORGNAME, E.RATE_ORG_NAME)                       AS RATE_ORG_NAME 
       ,E.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,E.SYN_FLAG              AS SYN_FLAG 
       ,E.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,E.LINCENSE_NO           AS LINCENSE_NO 
       ,E.ADJUST_TYP            AS ADJUST_TYP 
       ,E.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,E.EMERGING_TYP          AS EMERGING_TYP 
       ,E.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,COALESCE(B.REGIONCODE, E.AREA_ID)                       AS AREA_ID 
       ,E.UNION_FLAG            AS UNION_FLAG 
       ,COALESCE(CASE WHEN A.BLACKSHEETORNOT = 'X' THEN NULL ELSE A.BLACKSHEETORNOT END, E.BLACKLIST_FLAG) AS BLACKLIST_FLAG 
       ,E.AUTH_ORG              AS AUTH_ORG 
       ,COALESCE(B.MYBANK, E.OPEN_ORG1)                       AS OPEN_ORG1 
       ,E.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,COALESCE(B.OTHERBANKACCOUNT, E.OTHER_BANK_ORG)                       AS OTHER_BANK_ORG 
       ,COALESCE(B.LOANFLAG, E.IF_EFFICT_LOANCARD)                       AS IF_EFFICT_LOANCARD 
       ,COALESCE(B.LOANCARDNO, E.LOAN_CARDNO)                       AS LOAN_CARDNO 
       ,COALESCE(B.LOANCARDINSYEAR, E.LOAN_CARD_DATE)                       AS LOAN_CARD_DATE 
       ,E.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,COALESCE(B.CREDITDATE, E.FIRST_LOAN_DATE)                       AS FIRST_LOAN_DATE 
       ,E.LOAN_RATE             AS LOAN_RATE 
       ,E.DEP_RATE              AS DEP_RATE 
       ,E.DEP_RATIO             AS DEP_RATIO 
       ,E.SETTLE_RATIO          AS SETTLE_RATIO 
       ,E.BASIC_ACCT            AS BASIC_ACCT 
       ,COALESCE(B.FICTITIOUSPERSON, E.LEGAL_NAME)                       AS LEGAL_NAME 
       ,COALESCE(B.FICTITIOUSPERSONID, E.LEGAL_CERT_NO)                       AS LEGAL_CERT_NO 
       ,COALESCE(B.OFFICETEL, E.LINK_MOBILE)                       AS LINK_MOBILE 
       ,COALESCE(B.OFFICEFAX, E.FAX_NO)                       AS FAX_NO 
       ,COALESCE(B.FINANCEDEPTTEL, E.LINK_TEL_FIN)                       AS LINK_TEL_FIN 
       ,COALESCE(B.REGISTERADD, E.REGISTER_ADDRESS)                       AS REGISTER_ADDRESS 
       ,COALESCE(B.OFFICEZIP, REGISTER_ZIP)                       AS REGISTER_ZIP 
       ,COALESCE(B.COUNTRYCODE, E.COUNTRY)                       AS COUNTRY 
       ,COALESCE(B.REGIONCODE, E.PROVINCE)                       AS PROVINCE 
       ,COALESCE(B.OFFICEADD, E.WORK_ADDRESS)                       AS WORK_ADDRESS 
       ,COALESCE(B.EMAILADD, E.E_MAIL)                       AS E_MAIL 
       ,COALESCE(B.WEBADD, E.WEB_ADDRESS)                       AS WEB_ADDRESS 
       ,E.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,E.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,E.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,E.LINK_TEL              AS LINK_TEL 
       ,E.OPEN_ORG2             AS OPEN_ORG2 
       ,E.OPEN_DATE             AS OPEN_DATE 
       ,COALESCE(B.RCCURRENCY, E.REG_CCY)                       AS REG_CCY 
       ,COALESCE(B.REGISTERCAPITAL, E.REG_CAPITAL)                       AS REG_CAPITAL 
       ,COALESCE(B.MOSTBUSINESS, E.BUSINESS)                       AS BUSINESS 
       ,COALESCE(B.EMPLOYEENUMBER, E.EMPLOYEE_NUM)                       AS EMPLOYEE_NUM 
       ,COALESCE(B.WHOLEPROPERTY, E.TOTAL_ASSET)                       AS TOTAL_ASSET 
       ,COALESCE(B.SALEROOM, E.SALE_ASSET)                       AS SALE_ASSET 
       ,COALESCE(B.TAXNO, E.TAX_NO)                       AS TAX_NO 
       ,COALESCE(B.TAXNO1, E.RENT_NO)                       AS RENT_NO 
       ,COALESCE(B.STEPRLASTTIME, E.LAST_DATE)                       AS LAST_DATE 
       ,COALESCE(B.HASDIRECTORATE, E.BOND_FLAG)                       AS BOND_FLAG 
       ,COALESCE(B.WORKFIELDAREA, E.BUS_AREA)                       AS BUS_AREA 
       ,COALESCE(B.WORKFIELDFEE, E.BUS_OWNER)                       AS BUS_OWNER 
       ,COALESCE(B.MANAGEINFO, BUS_STAT)                       AS BUS_STAT 
       ,COALESCE(B.PCCURRENCY, E.INCOME_CCY)                       AS INCOME_CCY 
       ,COALESCE(B.PAICLUPCAPITAL, E.INCOME_SETTLE)                       AS INCOME_SETTLE 
       ,E.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,E.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,E.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,E.MERGE_ORG             AS MERGE_ORG 
       ,E.MERGE_DATE            AS MERGE_DATE 
       ,E.MERGE_OFFICER         AS MERGE_OFFICER 
       ,E.REMARK1               AS REMARK1 
       ,COALESCE(D.BIRTHDAY, E.BIRTH_DATE)                       AS BIRTH_DATE 
       ,COALESCE(D.CERTID, E.KEY_CERT_NO)                       AS KEY_CERT_NO 
       ,COALESCE(D.CERTTYPE, E.KEY_CERT_TYP)                       AS KEY_CERT_TYP 
       ,COALESCE(D.RELATIVEID, E.KEY_CUST_ID)                       AS KEY_CUST_ID 
       ,COALESCE(D.CUSTOMERNAME, E.KEY_PEOPLE_NAME)                       AS KEY_PEOPLE_NAME 
       ,COALESCE(D.DUTY, E.EDU_LEVEL)                       AS EDU_LEVEL 
       ,COALESCE(D.ENGAGETERM, E.WORK_YEAR)                       AS WORK_YEAR 
       ,COALESCE(D.FAMILYADD, E.HOUSE_ADDRESS)                       AS HOUSE_ADDRESS 
       ,COALESCE(D.FAMILYZIP, E.HOUSE_ZIP)                       AS HOUSE_ZIP 
       ,COALESCE(D.HOLDDATE, E.DUTY_TIME)                       AS DUTY_TIME 
       ,COALESCE(D.HOLDSTOCK, E.SHARE_HOLDING)                       AS SHARE_HOLDING 
       ,COALESCE(D.RELATIONSHIP, E.DUTY)                       AS DUTY 
       ,COALESCE(D.REMARK, E.REMARK2)                       AS REMARK2 
       ,COALESCE(D.SEX, E.SEX)                       AS SEX 
       ,COALESCE(D.SINO, E.SOCIAL_INSURE_NO)                       AS SOCIAL_INSURE_NO 
       ,V_DT                    AS ODS_ST_DATE 
       ,E.GREEN_FLAG            AS GREEN_FLAG 
       ,E.IS_MODIFY             AS IS_MODIFY 
       ,E.FR_ID                 AS FR_ID 
       ,E.LONGITUDE             AS LONGITUDE 
       ,E.LATITUDE              AS LATITUDE 
       ,E.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO E                              --对公客户信息表
  INNER JOIN(
         SELECT DISTINCT FR_ID 
               ,ODS_CUST_ID 
               ,MAX(SOURCE_CUST_ID)                       AS SOURCE_CUST_ID 
           FROM OCRM_F_CI_SYS_RESOURCE 
          WHERE ODS_SYS_ID              = 'LNA' 
            AND ODS_CUST_TYPE           = '2' 
          GROUP BY FR_ID 
               ,ODS_CUST_ID) C                                 --
     ON E.CUST_ID               = C.ODS_CUST_ID 
    AND C.FR_ID                 = E.FR_ID 
  INNER JOIN F_CI_XDXT_CUSTOMER_INFO A                         --
     ON A.FR_ID                 = E.FR_ID 
    AND A.CUSTOMERID            = C.SOURCE_CUST_ID 
  INNER JOIN F_CI_XDXT_ENT_INFO B                              --
     ON A.CUSTOMERID            = B.CUSTOMERID 
    AND B.FR_ID                 = E.FR_ID 
  INNER JOIN MID_CI_CUSTOMER_RELATIVE D                        --
     ON D.CUSTOMERID            = A.CUSTOMERID 
    AND D.RELATIONSHIP          = '5100' 
    AND D.FR_ID                 = E.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --0:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/")

#任务[12] 001-11::
V_STEP = V_STEP + 1

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT DISTINCT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.COM_BELONG            AS COM_BELONG 
       ,A.HOLDING_TYP           AS HOLDING_TYP 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.OBJECT_RATE           AS OBJECT_RATE 
       ,A.SUBJECT_RATE          AS SUBJECT_RATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.RATE_DATE             AS RATE_DATE 
       ,A.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,A.IF_SMALL              AS IF_SMALL 
       ,A.IF_IBK                AS IF_IBK 
       ,A.PLICY_TYP             AS PLICY_TYP 
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,'Y'                     AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,A.AREA_ID               AS AREA_ID 
       ,A.UNION_FLAG            AS UNION_FLAG 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.AUTH_ORG              AS AUTH_ORG 
       ,A.OPEN_ORG1             AS OPEN_ORG1 
       ,A.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,A.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LOAN_CARDNO           AS LOAN_CARDNO 
       ,A.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,A.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,A.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,A.LOAN_RATE             AS LOAN_RATE 
       ,A.DEP_RATE              AS DEP_RATE 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.SETTLE_RATIO          AS SETTLE_RATIO 
       ,A.BASIC_ACCT            AS BASIC_ACCT 
       ,A.LEGAL_NAME            AS LEGAL_NAME 
       ,A.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,A.LINK_MOBILE           AS LINK_MOBILE 
       ,A.FAX_NO                AS FAX_NO 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.REGISTER_ZIP          AS REGISTER_ZIP 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.E_MAIL                AS E_MAIL 
       ,A.WEB_ADDRESS           AS WEB_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,A.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.OPEN_DATE             AS OPEN_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.LAST_DATE             AS LAST_DATE 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_OWNER             AS BUS_OWNER 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,A.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,A.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,A.MERGE_ORG             AS MERGE_ORG 
       ,A.MERGE_DATE            AS MERGE_DATE 
       ,A.MERGE_OFFICER         AS MERGE_OFFICER 
       ,A.REMARK1               AS REMARK1 
       ,A.BIRTH_DATE            AS BIRTH_DATE 
       ,A.KEY_CERT_NO           AS KEY_CERT_NO 
       ,A.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,A.KEY_CUST_ID           AS KEY_CUST_ID 
       ,A.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,A.EDU_LEVEL             AS EDU_LEVEL 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,A.HOUSE_ZIP             AS HOUSE_ZIP 
       ,A.DUTY_TIME             AS DUTY_TIME 
       ,A.SHARE_HOLDING         AS SHARE_HOLDING 
       ,A.DUTY                  AS DUTY 
       ,A.REMARK2               AS REMARK2 
       ,A.SEX                   AS SEX 
       ,A.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.GREEN_FLAG            AS GREEN_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户信息表
  INNER JOIN F_CI_XDXT_CUSTOMER_INFO C                         --客户信息表
     ON A.CUST_ID               = C.MFCUSTOMERID 
    AND A.FR_ID                 = C.FR_ID 
  INNER JOIN F_LN_XDXT_BUSINESS_CONTRACT B                     --贷款合同表
     ON B.OPERATEORGID          = C.INPUTORGID 
    AND B.CUSTOMERID            = C.CUSTOMERID 
    AND B.ODS_ST_DATE           = V_DT 
    AND B.FR_ID                 = C.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --0:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/")

#任务[12] 001-12::
V_STEP = V_STEP + 1

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,COALESCE(C.CIFNAME, A.CUST_NAME)                       AS CUST_NAME 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,COALESCE(C.ENTIDTYPE, A.CERT_TYP)                       AS CERT_TYP 
       ,COALESCE(C.ENTIDNO, A.CERT_NO)                       AS CERT_NO 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.COM_BELONG            AS COM_BELONG 
       ,A.HOLDING_TYP           AS HOLDING_TYP 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.OBJECT_RATE           AS OBJECT_RATE 
       ,A.SUBJECT_RATE          AS SUBJECT_RATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.RATE_DATE             AS RATE_DATE 
       ,A.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,A.IF_SMALL              AS IF_SMALL 
       ,'Y'                     AS IF_IBK 
       ,A.PLICY_TYP             AS PLICY_TYP 
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,A.AREA_ID               AS AREA_ID 
       ,A.UNION_FLAG            AS UNION_FLAG 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.AUTH_ORG              AS AUTH_ORG 
       ,A.OPEN_ORG1             AS OPEN_ORG1 
       ,A.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,A.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LOAN_CARDNO           AS LOAN_CARDNO 
       ,A.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,A.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,A.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,A.LOAN_RATE             AS LOAN_RATE 
       ,A.DEP_RATE              AS DEP_RATE 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.SETTLE_RATIO          AS SETTLE_RATIO 
       ,A.BASIC_ACCT            AS BASIC_ACCT 
       ,A.LEGAL_NAME            AS LEGAL_NAME 
       ,A.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,COALESCE(C.PHONE, A.LINK_MOBILE)                       AS LINK_MOBILE 
       ,A.FAX_NO                AS FAX_NO 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.REGISTER_ZIP          AS REGISTER_ZIP 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.E_MAIL                AS E_MAIL 
       ,A.WEB_ADDRESS           AS WEB_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,A.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.OPEN_DATE             AS OPEN_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.LAST_DATE             AS LAST_DATE 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_OWNER             AS BUS_OWNER 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,A.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,A.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,A.MERGE_ORG             AS MERGE_ORG 
       ,A.MERGE_DATE            AS MERGE_DATE 
       ,A.MERGE_OFFICER         AS MERGE_OFFICER 
       ,A.REMARK1               AS REMARK1 
       ,A.BIRTH_DATE            AS BIRTH_DATE 
       ,A.KEY_CERT_NO           AS KEY_CERT_NO 
       ,A.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,A.KEY_CUST_ID           AS KEY_CUST_ID 
       ,A.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,A.EDU_LEVEL             AS EDU_LEVEL 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,A.HOUSE_ZIP             AS HOUSE_ZIP 
       ,A.DUTY_TIME             AS DUTY_TIME 
       ,A.SHARE_HOLDING         AS SHARE_HOLDING 
       ,A.DUTY                  AS DUTY 
       ,A.REMARK2               AS REMARK2 
       ,A.SEX                   AS SEX 
       ,A.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.GREEN_FLAG            AS GREEN_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户信息表
  INNER JOIN OCRM_F_CI_SYS_RESOURCE E                          --系统来源中间表
     ON A.FR_ID                 = E.FR_ID 
    AND A.CUST_ID               = E.ODS_CUST_ID 
    AND E.ODS_CUST_TYPE         = '2' 
    AND E.ODS_SYS_ID            = 'IBK' 
    AND E.CERT_FLAG             = 'Y' 
  INNER JOIN TMP_OCRM_F_CI_COM_CUST_INFO_02 C                  --对公客户信息表临时表02
     ON E.CERT_NO               = C.ENTIDNO 
    AND E.CERT_TYPE             = C.ENTIDTYPE 
    AND E.SOURCE_CUST_NAME      = C.CIFNAME 
    AND E.FR_ID                 = C.FR_ID 
    AND SUBSTR(ENTIDNO, 1, 1) <> 'L' 
    AND C.CIFSEQ                = E.SOURCE_CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --0:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/")

#任务[12] 001-13::
V_STEP = V_STEP + 1

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,CUST_ID                 AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.COM_BELONG            AS COM_BELONG 
       ,A.HOLDING_TYP           AS HOLDING_TYP 
       ,SUBSTR(INDUS_CLAS_DEPUTY, 1, 1)                       AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.OBJECT_RATE           AS OBJECT_RATE 
       ,A.SUBJECT_RATE          AS SUBJECT_RATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.RATE_DATE             AS RATE_DATE 
       ,A.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,A.IF_SMALL              AS IF_SMALL 
       ,A.IF_IBK                AS IF_IBK 
       ,A.PLICY_TYP             AS PLICY_TYP 
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,A.AREA_ID               AS AREA_ID 
       ,A.UNION_FLAG            AS UNION_FLAG 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.AUTH_ORG              AS AUTH_ORG 
       ,A.OPEN_ORG1             AS OPEN_ORG1 
       ,A.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,A.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LOAN_CARDNO           AS LOAN_CARDNO 
       ,A.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,A.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,A.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,A.LOAN_RATE             AS LOAN_RATE 
       ,A.DEP_RATE              AS DEP_RATE 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.SETTLE_RATIO          AS SETTLE_RATIO 
       ,A.BASIC_ACCT            AS BASIC_ACCT 
       ,A.LEGAL_NAME            AS LEGAL_NAME 
       ,A.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,A.LINK_MOBILE           AS LINK_MOBILE 
       ,A.FAX_NO                AS FAX_NO 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.REGISTER_ZIP          AS REGISTER_ZIP 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.E_MAIL                AS E_MAIL 
       ,A.WEB_ADDRESS           AS WEB_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,A.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.OPEN_DATE             AS OPEN_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.LAST_DATE             AS LAST_DATE 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_OWNER             AS BUS_OWNER 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,A.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,A.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,A.MERGE_ORG             AS MERGE_ORG 
       ,A.MERGE_DATE            AS MERGE_DATE 
       ,A.MERGE_OFFICER         AS MERGE_OFFICER 
       ,A.REMARK1               AS REMARK1 
       ,A.BIRTH_DATE            AS BIRTH_DATE 
       ,A.KEY_CERT_NO           AS KEY_CERT_NO 
       ,A.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,A.KEY_CUST_ID           AS KEY_CUST_ID 
       ,A.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,A.EDU_LEVEL             AS EDU_LEVEL 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,A.HOUSE_ZIP             AS HOUSE_ZIP 
       ,A.DUTY_TIME             AS DUTY_TIME 
       ,A.SHARE_HOLDING         AS SHARE_HOLDING 
       ,A.DUTY                  AS DUTY 
       ,A.REMARK2               AS REMARK2 
       ,A.SEX                   AS SEX 
       ,A.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.GREEN_FLAG            AS GREEN_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,FR_ID                   AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户信息表
  WHERE TRIM(INDUS_CLAS_DEPUTY) <> '' 
    AND INDUS_CLAS_DEPUTY IS 
    NOT NULL 
    AND ODS_ST_DATE             = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --0:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/")

#任务[12] 001-14::
V_STEP = V_STEP + 1

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT M.ID                    AS ID 
       ,M.CUST_ID               AS CUST_ID 
       ,M.CUST_NAME             AS CUST_NAME 
       ,M.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,M.CUST_EN_NAME          AS CUST_EN_NAME 
       ,M.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,M.CERT_TYP              AS CERT_TYP 
       ,M.CERT_NO               AS CERT_NO 
       ,M.COM_SCALE             AS COM_SCALE 
       ,M.COM_START_DATE        AS COM_START_DATE 
       ,M.COM_BELONG            AS COM_BELONG 
       ,M.HOLDING_TYP           AS HOLDING_TYP 
       ,M.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,M.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,M.BUS_TYP               AS BUS_TYP 
       ,M.ORG_TYP               AS ORG_TYP 
       ,M.ECO_TYP               AS ECO_TYP 
       ,M.COM_TYP               AS COM_TYP 
       ,M.COM_LEVEL             AS COM_LEVEL 
       ,M.OTHER_NAME            AS OTHER_NAME 
       ,M.OBJECT_RATE           AS OBJECT_RATE 
       ,M.SUBJECT_RATE          AS SUBJECT_RATE 
       ,M.EFF_DATE              AS EFF_DATE 
       ,M.RATE_DATE             AS RATE_DATE 
       ,M.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,M.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,M.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,M.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,M.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,M.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,M.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,M.IF_BASIC              AS IF_BASIC 
       ,M.IF_ESTATE             AS IF_ESTATE 
       ,M.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,M.IF_SMALL              AS IF_SMALL 
       ,M.IF_IBK                AS IF_IBK 
       ,M.PLICY_TYP             AS PLICY_TYP 
       ,M.IF_EXPESS             AS IF_EXPESS 
       ,M.IF_MONITER            AS IF_MONITER 
       ,M.IF_FANACING           AS IF_FANACING 
       ,M.IF_INT                AS IF_INT 
       ,M.IF_GROUP              AS IF_GROUP 
       ,M.RIGHT_FLAG            AS RIGHT_FLAG 
       ,M.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,M.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,M.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,M.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,M.SYN_FLAG              AS SYN_FLAG 
       ,M.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,M.LINCENSE_NO           AS LINCENSE_NO 
       ,M.ADJUST_TYP            AS ADJUST_TYP 
       ,M.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,M.EMERGING_TYP          AS EMERGING_TYP 
       ,M.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,M.AREA_ID               AS AREA_ID 
       ,M.UNION_FLAG            AS UNION_FLAG 
       ,M.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,M.AUTH_ORG              AS AUTH_ORG 
       ,M.OPEN_ORG1             AS OPEN_ORG1 
       ,M.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,M.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,M.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,M.LOAN_CARDNO           AS LOAN_CARDNO 
       ,M.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,M.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,M.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,M.LOAN_RATE             AS LOAN_RATE 
       ,M.DEP_RATE              AS DEP_RATE 
       ,M.DEP_RATIO             AS DEP_RATIO 
       ,M.SETTLE_RATIO          AS SETTLE_RATIO 
       ,M.BASIC_ACCT            AS BASIC_ACCT 
       ,COALESCE(N.EC_FULL_NAM, M.LEGAL_NAME)                       AS LEGAL_NAME 
       ,COALESCE(N.EC_CER_NO, M.LEGAL_CERT_NO)                       AS LEGAL_CERT_NO 
       ,M.LINK_MOBILE           AS LINK_MOBILE 
       ,M.FAX_NO                AS FAX_NO 
       ,M.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,M.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,M.REGISTER_ZIP          AS REGISTER_ZIP 
       ,M.COUNTRY               AS COUNTRY 
       ,M.PROVINCE              AS PROVINCE 
       ,M.WORK_ADDRESS          AS WORK_ADDRESS 
       ,M.E_MAIL                AS E_MAIL 
       ,M.WEB_ADDRESS           AS WEB_ADDRESS 
       ,M.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,M.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,M.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,M.LINK_TEL              AS LINK_TEL 
       ,M.OPEN_ORG2             AS OPEN_ORG2 
       ,M.OPEN_DATE             AS OPEN_DATE 
       ,M.REG_CCY               AS REG_CCY 
       ,M.REG_CAPITAL           AS REG_CAPITAL 
       ,M.BUSINESS              AS BUSINESS 
       ,M.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,M.TOTAL_ASSET           AS TOTAL_ASSET 
       ,M.SALE_ASSET            AS SALE_ASSET 
       ,M.TAX_NO                AS TAX_NO 
       ,M.RENT_NO               AS RENT_NO 
       ,M.LAST_DATE             AS LAST_DATE 
       ,M.BOND_FLAG             AS BOND_FLAG 
       ,M.BUS_AREA              AS BUS_AREA 
       ,M.BUS_OWNER             AS BUS_OWNER 
       ,M.BUS_STAT              AS BUS_STAT 
       ,M.INCOME_CCY            AS INCOME_CCY 
       ,M.INCOME_SETTLE         AS INCOME_SETTLE 
       ,M.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,M.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,M.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,M.MERGE_ORG             AS MERGE_ORG 
       ,M.MERGE_DATE            AS MERGE_DATE 
       ,M.MERGE_OFFICER         AS MERGE_OFFICER 
       ,M.REMARK1               AS REMARK1 
       ,M.BIRTH_DATE            AS BIRTH_DATE 
       ,M.KEY_CERT_NO           AS KEY_CERT_NO 
       ,M.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,M.KEY_CUST_ID           AS KEY_CUST_ID 
       ,M.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,M.EDU_LEVEL             AS EDU_LEVEL 
       ,M.WORK_YEAR             AS WORK_YEAR 
       ,M.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,M.HOUSE_ZIP             AS HOUSE_ZIP 
       ,M.DUTY_TIME             AS DUTY_TIME 
       ,M.SHARE_HOLDING         AS SHARE_HOLDING 
       ,M.DUTY                  AS DUTY 
       ,M.REMARK2               AS REMARK2 
       ,M.SEX                   AS SEX 
       ,M.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,V_DT                    AS ODS_ST_DATE 
       ,M.GREEN_FLAG            AS GREEN_FLAG 
       ,M.IS_MODIFY             AS IS_MODIFY 
       ,M.FR_ID                 AS FR_ID 
       ,M.LONGITUDE             AS LONGITUDE 
       ,M.LATITUDE              AS LATITUDE 
       ,M.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO M                              --对公客户信息表
  INNER JOIN (SELECT A.FR_ID,A.EC_CUST_ID_A,B.EC_CER_NO,B.EC_FULL_NAM,
                     ROW_NUMBER() OVER(PARTITION BY A.EC_CUST_ID_A ORDER BY EC_CCR_FRDAT_N DESC ) RN 
                FROM F_CI_CBOD_ECCIFCCR A,F_CI_CBOD_ECCIFIDI B
               WHERE A.EC_CUST_TYP_A = '2' AND A.EC_REL_TYP = '01' 
                     AND A.EC_CUST_ID_B = B.EC_CUST_NO AND B.EC_CUST_TYP = '1' 
                     AND B.EC_PRI_CER_FLG = 'Y' AND A.FR_ID = B.FR_ID
                     AND A.ODS_ST_DATE = V_DT)  N                                              --
     ON M.CUST_ID               = N.EC_CUST_ID_A 
    AND M.FR_ID                 = N.FR_ID 
    AND N.RN                    = 1 
  WHERE TRIM(INDUS_CLAS_DEPUTY) <> '' 
    AND INDUS_CLAS_DEPUTY IS NOT NULL 
    AND ODS_ST_DATE             = V_DT
    AND M.CUST_NAME != '' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --0:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL
    AND DST.CUST_NAME != ''    """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()

#装载数据
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet")

OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/")
