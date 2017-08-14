#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_POTENTIAL_CUSTOMER').setMaster(sys.argv[2])
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

OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
OCRM_F_CI_POTENTIAL_CUSTOMER = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_POTENTIAL_CUSTOMER_BK/'+V_DT_LD+'.parquet/*')
OCRM_F_CI_POTENTIAL_CUSTOMER.registerTempTable("OCRM_F_CI_POTENTIAL_CUSTOMER")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(ID     AS BIGINT)                 AS ID 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,CUST_EN_NAME            AS CUST_EN_NAME 
       ,OTHER_NAME              AS OTHER_NAME 
       ,CUST_TYPE               AS CUST_TYPE 
       ,CERT_TYP                AS CERT_TYP 
       ,CERT_NO                 AS CERT_NO 
       ,CUST_CLASS              AS CUST_CLASS 
       ,LINK_MAN                AS LINK_MAN 
       ,LINK_PHONE              AS LINK_PHONE 
       ,ZIP_CODE                AS ZIP_CODE 
       ,ADDRESS                 AS ADDRESS 
       ,POSITIVE_FLAG           AS POSITIVE_FLAG 
       ,BEGIN_DATE              AS BEGIN_DATE 
       ,POSITIVE_DATE           AS POSITIVE_DATE 
       ,CRM_DT                  AS CRM_DT 
       ,CREATOR                 AS CREATOR 
       ,CREATE_ORG              AS CREATE_ORG 
       ,FR_ID                   AS FR_ID 
       ,CUST_SOURCE             AS CUST_SOURCE 
       ,ASSIGN_ORG_ID           AS ASSIGN_ORG_ID 
       ,CUST_SEX                AS CUST_SEX 
       ,CUST_BIR                AS CUST_BIR 
       ,CUST_MRG                AS CUST_MRG 
       ,CUST_NATION             AS CUST_NATION 
       ,CUST_REGISTER           AS CUST_REGISTER 
       ,CUST_REGADDR            AS CUST_REGADDR 
       ,CUST_FAMSTATUS          AS CUST_FAMSTATUS 
       ,CUST_HEALTH             AS CUST_HEALTH 
       ,CUST_WEIGHT             AS CUST_WEIGHT 
       ,CUST_HEIGHT             AS CUST_HEIGHT 
       ,HIGHEST_DEGREE          AS HIGHEST_DEGREE 
       ,CUST_OCCUP_COD          AS CUST_OCCUP_COD 
       ,CUST_EDU_LVL_COD        AS CUST_EDU_LVL_COD 
       ,CUST_WORK_UNIT_NAME     AS CUST_WORK_UNIT_NAME 
       ,CUST_UTELL              AS CUST_UTELL 
       ,CUST_POSN               AS CUST_POSN 
       ,CUST_TITL               AS CUST_TITL 
       ,IS_MEDICARE             AS IS_MEDICARE 
       ,MAINPROORINCOME         AS MAINPROORINCOME 
       ,CUST_POSTCOD            AS CUST_POSTCOD 
       ,CUST_TEL_NO             AS CUST_TEL_NO 
       ,CUST_MBTELNO            AS CUST_MBTELNO 
       ,CUST_FAMADDR            AS CUST_FAMADDR 
       ,CAST(CUST_FAM_NUM AS INTEGER)           AS CUST_FAM_NUM 
       ,INDUSTRYTYPE            AS INDUSTRYTYPE 
       ,CUST_WORKADDR           AS CUST_WORKADDR 
       ,CUST_COMMADD            AS CUST_COMMADD 
       ,CI_ADDR                 AS CI_ADDR 
       ,CUST_PECON_RESUR        AS CUST_PECON_RESUR 
       ,FAM_INCOMEACC           AS FAM_INCOMEACC 
       ,CUST_TOT_ASS            AS CUST_TOT_ASS 
       ,CRE_RECORD              AS CRE_RECORD 
       ,CAST(CUST_TOT_DEBT  AS DECIMAL(10))         AS CUST_TOT_DEBT 
       ,COM_START_DATE          AS COM_START_DATE 
       ,REG_CCY                 AS REG_CCY 
       ,REG_CAPITAL             AS REG_CAPITAL 
       ,INCOME_CCY              AS INCOME_CCY 
       ,INCOME_SETTLE           AS INCOME_SETTLE 
       ,INDUS_CLASS_MAIN        AS INDUS_CLASS_MAIN 
       ,INDUS_CLASS_DEPUTY      AS INDUS_CLASS_DEPUTY 
       ,ORG_TYP                 AS ORG_TYP 
       ,COM_TYP                 AS COM_TYP 
       ,LINCENSE_NO             AS LINCENSE_NO 
       ,IF_AGRICULTRUE          AS IF_AGRICULTRUE 
       ,LISTING_CORP_TYP        AS LISTING_CORP_TYP 
       ,IF_HIGH_TECH            AS IF_HIGH_TECH 
       ,IF_ESTATE               AS IF_ESTATE 
       ,COM_SCALE               AS COM_SCALE 
       ,IF_SHARE_CUST           AS IF_SHARE_CUST 
       ,ENT_QUA_LEVEL           AS ENT_QUA_LEVEL 
       ,IF_FANACING             AS IF_FANACING 
       ,LEGAL_NAME              AS LEGAL_NAME 
       ,FR_CERT_TYP             AS FR_CERT_TYP 
       ,FR_SEX                  AS FR_SEX 
       ,LEGAL_CERT_NO           AS LEGAL_CERT_NO 
       ,LINK_MOBILE             AS LINK_MOBILE 
       ,LINK_TEL_FIN            AS LINK_TEL_FIN 
       ,WORK_ADDRESS            AS WORK_ADDRESS 
       ,BUSINESS                AS BUSINESS 
       ,SALE_ASSET              AS SALE_ASSET 
       ,EMPLOYEE_NUM            AS EMPLOYEE_NUM 
       ,LONGITUDE               AS LONGITUDE 
       ,LATITUDE                AS LATITUDE 
       ,CUST_LIVE_STATUS        AS CUST_LIVE_STATUS 
       ,COM_LOGO                AS COM_LOGO 
       ,CAST(ANNUAL_FAMILY_INCOME AS  DECIMAL(12,2))  AS ANNUAL_FAMILY_INCOME 
       ,NOTE                    AS NOTE 
       ,CREATE_DATE             AS CREATE_DATE 
       ,UPDATE_DATE             AS UPDATE_DATE 
       ,UPDATE_ORG              AS UPDATE_ORG 
       ,UPDATE_USER             AS UPDATE_USER 
       ,GIHHEST_DEGRE           AS GIHHEST_DEGRE 
       ,DEGREE                  AS DEGREE 
       ,CUST_YEL_NO             AS CUST_YEL_NO 
       ,INDUS_CALSS_MAIN        AS INDUS_CALSS_MAIN 
       ,INDUS_CLAS_DEPUTY       AS INDUS_CLAS_DEPUTY 
       ,SEX                     AS SEX 
       ,TOTAL_ASSET             AS TOTAL_ASSET 
       ,LINK_MAN_CERT_TYPE      AS LINK_MAN_CERT_TYPE 
       ,LINK_MAN_CERT_NO        AS LINK_MAN_CERT_NO 
       ,BACK                    AS BACK 
   FROM OCRM_F_CI_POTENTIAL_CUSTOMER A                         --
  WHERE CUST_SOURCE <> '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_POTENTIAL_CUSTOMER_TMP = sqlContext.sql(sql)
OCRM_F_CI_POTENTIAL_CUSTOMER_TMP.registerTempTable("OCRM_F_CI_POTENTIAL_CUSTOMER_TMP")
dfn="OCRM_F_CI_POTENTIAL_CUSTOMER_TMP/"+V_DT+".parquet"
OCRM_F_CI_POTENTIAL_CUSTOMER_TMP.cache()
nrows = OCRM_F_CI_POTENTIAL_CUSTOMER_TMP.count()
OCRM_F_CI_POTENTIAL_CUSTOMER_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_POTENTIAL_CUSTOMER_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_POTENTIAL_CUSTOMER_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_POTENTIAL_CUSTOMER_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
OCRM_F_CI_POTENTIAL_CUSTOMER_TMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_POTENTIAL_CUSTOMER_TMP/*')
OCRM_F_CI_POTENTIAL_CUSTOMER_TMP.registerTempTable("OCRM_F_CI_POTENTIAL_CUSTOMER_TMP")
sql = """
 SELECT CAST(ID   AS BIGINT)                   AS ID 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,CUST_EN_NAME            AS CUST_EN_NAME 
       ,OTHER_NAME              AS OTHER_NAME 
       ,CUST_TYPE               AS CUST_TYPE 
       ,CERT_TYP                AS CERT_TYP 
       ,CERT_NO                 AS CERT_NO 
       ,CUST_CLASS              AS CUST_CLASS 
       ,LINK_MAN                AS LINK_MAN 
       ,LINK_PHONE              AS LINK_PHONE 
       ,ZIP_CODE                AS ZIP_CODE 
       ,ADDRESS                 AS ADDRESS 
       ,POSITIVE_FLAG           AS POSITIVE_FLAG 
       ,BEGIN_DATE              AS BEGIN_DATE 
       ,POSITIVE_DATE           AS POSITIVE_DATE 
       ,CRM_DT                  AS CRM_DT 
       ,CREATOR                 AS CREATOR 
       ,CREATE_ORG              AS CREATE_ORG 
       ,FR_ID                   AS FR_ID 
       ,CUST_SOURCE             AS CUST_SOURCE 
       ,ASSIGN_ORG_ID           AS ASSIGN_ORG_ID 
       ,CUST_SEX                AS CUST_SEX 
       ,CUST_BIR                AS CUST_BIR 
       ,CUST_MRG                AS CUST_MRG 
       ,CUST_NATION             AS CUST_NATION 
       ,CUST_REGISTER           AS CUST_REGISTER 
       ,CUST_REGADDR            AS CUST_REGADDR 
       ,CUST_FAMSTATUS          AS CUST_FAMSTATUS 
       ,CUST_HEALTH             AS CUST_HEALTH 
       ,CUST_WEIGHT             AS CUST_WEIGHT 
       ,CUST_HEIGHT             AS CUST_HEIGHT 
       ,HIGHEST_DEGREE          AS HIGHEST_DEGREE 
       ,CUST_OCCUP_COD          AS CUST_OCCUP_COD 
       ,CUST_EDU_LVL_COD        AS CUST_EDU_LVL_COD 
       ,CUST_WORK_UNIT_NAME     AS CUST_WORK_UNIT_NAME 
       ,CUST_UTELL              AS CUST_UTELL 
       ,CUST_POSN               AS CUST_POSN 
       ,CUST_TITL               AS CUST_TITL 
       ,IS_MEDICARE             AS IS_MEDICARE 
       ,MAINPROORINCOME         AS MAINPROORINCOME 
       ,CUST_POSTCOD            AS CUST_POSTCOD 
       ,CUST_TEL_NO             AS CUST_TEL_NO 
       ,CUST_MBTELNO            AS CUST_MBTELNO 
       ,CUST_FAMADDR            AS CUST_FAMADDR 
       ,CAST(CUST_FAM_NUM AS INTEGER)            AS CUST_FAM_NUM 
       ,INDUSTRYTYPE            AS INDUSTRYTYPE 
       ,CUST_WORKADDR           AS CUST_WORKADDR 
       ,CUST_COMMADD            AS CUST_COMMADD 
       ,CI_ADDR                 AS CI_ADDR 
       ,CUST_PECON_RESUR        AS CUST_PECON_RESUR 
       ,FAM_INCOMEACC           AS FAM_INCOMEACC 
       ,CUST_TOT_ASS            AS CUST_TOT_ASS 
       ,CRE_RECORD              AS CRE_RECORD 
       ,CAST(CUST_TOT_DEBT  AS DECIMAL(10))         AS CUST_TOT_DEBT 
       ,COM_START_DATE          AS COM_START_DATE 
       ,REG_CCY                 AS REG_CCY 
       ,REG_CAPITAL             AS REG_CAPITAL 
       ,INCOME_CCY              AS INCOME_CCY 
       ,INCOME_SETTLE           AS INCOME_SETTLE 
       ,INDUS_CLASS_MAIN        AS INDUS_CLASS_MAIN 
       ,INDUS_CLASS_DEPUTY      AS INDUS_CLASS_DEPUTY 
       ,ORG_TYP                 AS ORG_TYP 
       ,COM_TYP                 AS COM_TYP 
       ,LINCENSE_NO             AS LINCENSE_NO 
       ,IF_AGRICULTRUE          AS IF_AGRICULTRUE 
       ,LISTING_CORP_TYP        AS LISTING_CORP_TYP 
       ,IF_HIGH_TECH            AS IF_HIGH_TECH 
       ,IF_ESTATE               AS IF_ESTATE 
       ,COM_SCALE               AS COM_SCALE 
       ,IF_SHARE_CUST           AS IF_SHARE_CUST 
       ,ENT_QUA_LEVEL           AS ENT_QUA_LEVEL 
       ,IF_FANACING             AS IF_FANACING 
       ,LEGAL_NAME              AS LEGAL_NAME 
       ,FR_CERT_TYP             AS FR_CERT_TYP 
       ,FR_SEX                  AS FR_SEX 
       ,LEGAL_CERT_NO           AS LEGAL_CERT_NO 
       ,LINK_MOBILE             AS LINK_MOBILE 
       ,LINK_TEL_FIN            AS LINK_TEL_FIN 
       ,WORK_ADDRESS            AS WORK_ADDRESS 
       ,BUSINESS                AS BUSINESS 
       ,SALE_ASSET              AS SALE_ASSET 
       ,EMPLOYEE_NUM            AS EMPLOYEE_NUM 
       ,LONGITUDE               AS LONGITUDE 
       ,LATITUDE                AS LATITUDE 
       ,CUST_LIVE_STATUS        AS CUST_LIVE_STATUS 
       ,COM_LOGO                AS COM_LOGO 
       ,CAST(ANNUAL_FAMILY_INCOME AS  DECIMAL(12,2))     AS ANNUAL_FAMILY_INCOME 
       ,NOTE                    AS NOTE 
       ,CREATE_DATE             AS CREATE_DATE 
       ,UPDATE_DATE             AS UPDATE_DATE 
       ,UPDATE_ORG              AS UPDATE_ORG 
       ,UPDATE_USER             AS UPDATE_USER 
       ,GIHHEST_DEGRE           AS GIHHEST_DEGRE 
       ,DEGREE                  AS DEGREE 
       ,CUST_YEL_NO             AS CUST_YEL_NO 
       ,INDUS_CALSS_MAIN        AS INDUS_CALSS_MAIN 
       ,INDUS_CLAS_DEPUTY       AS INDUS_CLAS_DEPUTY 
       ,SEX                     AS SEX 
       ,TOTAL_ASSET             AS TOTAL_ASSET 
       ,LINK_MAN_CERT_TYPE      AS LINK_MAN_CERT_TYPE 
       ,LINK_MAN_CERT_NO        AS LINK_MAN_CERT_NO 
       ,BACK                    AS BACK 
   FROM OCRM_F_CI_POTENTIAL_CUSTOMER_TMP A                     --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_POTENTIAL_CUSTOMER = sqlContext.sql(sql)
dfn="OCRM_F_CI_POTENTIAL_CUSTOMER/"+V_DT+".parquet"
OCRM_F_CI_POTENTIAL_CUSTOMER.cache()
nrows = OCRM_F_CI_POTENTIAL_CUSTOMER.count()
OCRM_F_CI_POTENTIAL_CUSTOMER.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_POTENTIAL_CUSTOMER.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_POTENTIAL_CUSTOMER/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_POTENTIAL_CUSTOMER lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()               AS ID 
       ,''                      AS CUST_ID 
    ,'' AS CUST_NAME
		,''     AS CUST_EN_NAME 
    ,'' AS OTHER_NAME
		,ODS_CUST_TYPE AS CUST_TYPE
		,CERT_TYPE AS CERT_TYP
		,TRIM(CERT_NO) AS CERT_NO
		,'' AS CUST_CLASS 
    ,'' AS LINK_MAN
		,''      AS LINK_PHONE 
    ,'' AS ZIP_CODE
		,''      AS ADDRESS 
    ,'' AS POSITIVE_FLAG
		,BEGIN_DATE AS BEGIN_DATE
		,''     AS POSITIVE_DATE 
       ,ODS_ST_DATE             AS CRM_DT 
    ,'' AS CREATOR
		,''       AS CREATE_ORG 
       ,A.FR_ID                 AS FR_ID 
       ,'1'                     AS CUST_SOURCE 
    ,'' AS ASSIGN_ORG_ID
		,'' AS CUST_SEX 
    ,'' AS CUST_BIR
		,''      AS CUST_MRG 
    ,'' AS CUST_NATION
		,''   AS CUST_REGISTER 
    ,'' AS CUST_REGADDR
		,''  AS CUST_FAMSTATUS 
    ,'' AS CUST_HEALTH
		,''   AS CUST_WEIGHT 
    ,'' AS CUST_HEIGHT
		,''   AS HIGHEST_DEGREE 
    ,'' AS CUST_OCCUP_COD
		,''        AS CUST_EDU_LVL_COD 
    ,'' AS CUST_WORK_UNIT_NAME
		,''   AS CUST_UTELL 
    ,'' AS CUST_POSN
		,''     AS CUST_TITL 
    ,'' AS IS_MEDICARE
		,''   AS MAINPROORINCOME 
    ,'' AS CUST_POSTCOD
		,''  AS CUST_TEL_NO 
    ,'' AS CUST_MBTELNO
		,''  AS CUST_FAMADDR 
    ,CAST('' AS INTEGER) AS CUST_FAM_NUM
		,''  AS INDUSTRYTYPE 
    ,'' AS CUST_WORKADDR
		,'' AS CUST_COMMADD 
    ,'' AS CI_ADDR
		,''       AS CUST_PECON_RESUR 
    ,'' AS FAM_INCOMEACC
		,'' AS CUST_TOT_ASS 
    ,'' AS CRE_RECORD
		,CAST('' AS DECIMAL(10))    AS CUST_TOT_DEBT 
    ,'' AS COM_START_DATE
		,''        AS REG_CCY 
    ,'' AS REG_CAPITAL
		,''   AS INCOME_CCY 
    ,'' AS INCOME_SETTLE
		,'' AS INDUS_CLASS_MAIN 
    ,'' AS INDUS_CLASS_DEPUTY
		,''    AS ORG_TYP 
    ,'' AS COM_TYP
		,''       AS LINCENSE_NO 
    ,'' AS IF_AGRICULTRUE
		,''        AS LISTING_CORP_TYP 
    ,'' AS IF_HIGH_TECH
		,''  AS IF_ESTATE 
    ,'' AS COM_SCALE
		,''     AS IF_SHARE_CUST 
    ,'' AS ENT_QUA_LEVEL
		,'' AS IF_FANACING 
    ,'' AS LEGAL_NAME
		,''    AS FR_CERT_TYP 
    ,'' AS FR_SEX
		,''        AS LEGAL_CERT_NO 
    ,'' AS LINK_MOBILE
		,''   AS LINK_TEL_FIN 
    ,'' AS WORK_ADDRESS
		,''  AS BUSINESS 
    ,'' AS SALE_ASSET
		,''    AS EMPLOYEE_NUM 
    ,'' AS LONGITUDE
		,''     AS LATITUDE 
    ,'' AS CUST_LIVE_STATUS
		,''      AS COM_LOGO 
    ,CAST('' AS DECIMAL(12,2)) AS ANNUAL_FAMILY_INCOME
		,''  AS NOTE 
    ,'' AS CREATE_DATE
		,''   AS UPDATE_DATE 
    ,'' AS UPDATE_ORG
		,''    AS UPDATE_USER 
    ,'' AS GIHHEST_DEGRE
		,'' AS DEGREE 
    ,'' AS CUST_YEL_NO
		,''   AS INDUS_CALSS_MAIN 
    ,'' AS INDUS_CLAS_DEPUTY
		,''     AS SEX 
    ,'' AS TOTAL_ASSET
		,''   AS LINK_MAN_CERT_TYPE 
    ,'' AS LINK_MAN_CERT_NO
		,''      AS BACK 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --
  WHERE CUST_STAT               = '2' 
    AND ODS_ST_DATE             = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_POTENTIAL_CUSTOMER = sqlContext.sql(sql)
dfn="OCRM_F_CI_POTENTIAL_CUSTOMER/"+V_DT+".parquet"
OCRM_F_CI_POTENTIAL_CUSTOMER.cache()
nrows = OCRM_F_CI_POTENTIAL_CUSTOMER.count()
OCRM_F_CI_POTENTIAL_CUSTOMER.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_POTENTIAL_CUSTOMER.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_POTENTIAL_CUSTOMER lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-04::
V_STEP = V_STEP + 1
OCRM_F_CI_POTENTIAL_CUSTOMER = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_POTENTIAL_CUSTOMER/*')
OCRM_F_CI_POTENTIAL_CUSTOMER.registerTempTable("OCRM_F_CI_POTENTIAL_CUSTOMER")

sql = """
 SELECT A.ID                    AS ID 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.CUST_TYPE             AS CUST_TYPE 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.CUST_CLASS            AS CUST_CLASS 
       ,A.LINK_MAN              AS LINK_MAN 
       ,A.LINK_PHONE            AS LINK_PHONE 
       ,A.ZIP_CODE              AS ZIP_CODE 
       ,A.ADDRESS               AS ADDRESS 
       ,'Y'                     AS POSITIVE_FLAG 
       ,A.BEGIN_DATE            AS BEGIN_DATE 
       ,V_DT                    AS POSITIVE_DATE 
       ,A.CRM_DT                AS CRM_DT 
       ,A.CREATOR               AS CREATOR 
       ,A.CREATE_ORG            AS CREATE_ORG 
       ,A.FR_ID                 AS FR_ID 
       ,A.CUST_SOURCE           AS CUST_SOURCE 
       ,A.ASSIGN_ORG_ID         AS ASSIGN_ORG_ID 
       ,A.CUST_SEX              AS CUST_SEX 
       ,A.CUST_BIR              AS CUST_BIR 
       ,A.CUST_MRG              AS CUST_MRG 
       ,A.CUST_NATION           AS CUST_NATION 
       ,A.CUST_REGISTER         AS CUST_REGISTER 
       ,A.CUST_REGADDR          AS CUST_REGADDR 
       ,A.CUST_FAMSTATUS        AS CUST_FAMSTATUS 
       ,A.CUST_HEALTH           AS CUST_HEALTH 
       ,A.CUST_WEIGHT           AS CUST_WEIGHT 
       ,A.CUST_HEIGHT           AS CUST_HEIGHT 
       ,A.HIGHEST_DEGREE        AS HIGHEST_DEGREE 
       ,A.CUST_OCCUP_COD        AS CUST_OCCUP_COD 
       ,A.CUST_EDU_LVL_COD      AS CUST_EDU_LVL_COD 
       ,A.CUST_WORK_UNIT_NAME   AS CUST_WORK_UNIT_NAME 
       ,A.CUST_UTELL            AS CUST_UTELL 
       ,A.CUST_POSN             AS CUST_POSN 
       ,A.CUST_TITL             AS CUST_TITL 
       ,A.IS_MEDICARE           AS IS_MEDICARE 
       ,A.MAINPROORINCOME       AS MAINPROORINCOME 
       ,A.CUST_POSTCOD          AS CUST_POSTCOD 
       ,A.CUST_TEL_NO           AS CUST_TEL_NO 
       ,A.CUST_MBTELNO          AS CUST_MBTELNO 
       ,A.CUST_FAMADDR          AS CUST_FAMADDR 
       ,CAST(A.CUST_FAM_NUM AS INTEGER)          AS CUST_FAM_NUM 
       ,A.INDUSTRYTYPE          AS INDUSTRYTYPE 
       ,A.CUST_WORKADDR         AS CUST_WORKADDR 
       ,A.CUST_COMMADD          AS CUST_COMMADD 
       ,A.CI_ADDR               AS CI_ADDR 
       ,A.CUST_PECON_RESUR      AS CUST_PECON_RESUR 
       ,A.FAM_INCOMEACC         AS FAM_INCOMEACC 
       ,A.CUST_TOT_ASS          AS CUST_TOT_ASS 
       ,A.CRE_RECORD            AS CRE_RECORD 
       ,CAST(A.CUST_TOT_DEBT   AS DECIMAL(10))       AS CUST_TOT_DEBT 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.INDUS_CLASS_MAIN      AS INDUS_CLASS_MAIN 
       ,A.INDUS_CLASS_DEPUTY    AS INDUS_CLASS_DEPUTY 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.LEGAL_NAME            AS LEGAL_NAME 
       ,A.FR_CERT_TYP           AS FR_CERT_TYP 
       ,A.FR_SEX                AS FR_SEX 
       ,A.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,A.LINK_MOBILE           AS LINK_MOBILE 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.BUSINESS              AS BUSINESS 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.CUST_LIVE_STATUS      AS CUST_LIVE_STATUS 
       ,A.COM_LOGO              AS COM_LOGO 
       ,CAST(A.ANNUAL_FAMILY_INCOME AS  DECIMAL(12,2))   AS ANNUAL_FAMILY_INCOME 
       ,A.NOTE                  AS NOTE 
       ,A.CREATE_DATE           AS CREATE_DATE 
       ,A.UPDATE_DATE           AS UPDATE_DATE 
       ,A.UPDATE_ORG            AS UPDATE_ORG 
       ,A.UPDATE_USER           AS UPDATE_USER 
       ,A.GIHHEST_DEGRE         AS GIHHEST_DEGRE 
       ,A.DEGREE                AS DEGREE 
       ,A.CUST_YEL_NO           AS CUST_YEL_NO 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.SEX                   AS SEX 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.LINK_MAN_CERT_TYPE    AS LINK_MAN_CERT_TYPE 
       ,A.LINK_MAN_CERT_NO      AS LINK_MAN_CERT_NO 
       ,A.BACK                  AS BACK 
   FROM OCRM_F_CI_POTENTIAL_CUSTOMER A                         --
  INNER JOIN 
 (SELECT DISTINCT ODS_CUST_ID,ODS_CUST_NAME,CERT_TYPE,CERT_NO,FR_ID  
              FROM OCRM_F_CI_SYS_RESOURCE D 
                WHERE CUST_STAT='1' 
                  AND NOT EXISTS(
                        SELECT 1 FROM  OCRM_F_CI_POTENTIAL_CUSTOMER C 
                          WHERE D.ODS_CUST_ID = C.CUST_ID AND C.POSITIVE_FLAG = 'Y' AND C.FR_ID = D.FR_ID)) B                             --
     ON A.CUST_NAME             = B.ODS_CUST_NAME 
    AND A.CERT_TYP              = B.CERT_TYPE 
    AND A.CERT_NO               = B.CERT_NO 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP1.registerTempTable("OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP1")

sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_ID                                            --:src.CUST_ID
       ,DST.CUST_NAME                                          --:src.CUST_NAME
       ,DST.CUST_EN_NAME                                       --:src.CUST_EN_NAME
       ,DST.OTHER_NAME                                         --:src.OTHER_NAME
       ,DST.CUST_TYPE                                          --:src.CUST_TYPE
       ,DST.CERT_TYP                                           --:src.CERT_TYP
       ,DST.CERT_NO                                            --:src.CERT_NO
       ,DST.CUST_CLASS                                         --:src.CUST_CLASS
       ,DST.LINK_MAN                                           --:src.LINK_MAN
       ,DST.LINK_PHONE                                         --:src.LINK_PHONE
       ,DST.ZIP_CODE                                           --:src.ZIP_CODE
       ,DST.ADDRESS                                            --:src.ADDRESS
       ,DST.POSITIVE_FLAG                                      --:src.POSITIVE_FLAG
       ,DST.BEGIN_DATE                                         --:src.BEGIN_DATE
       ,DST.POSITIVE_DATE                                      --:src.POSITIVE_DATE
       ,DST.CRM_DT                                             --:src.CRM_DT
       ,DST.CREATOR                                            --:src.CREATOR
       ,DST.CREATE_ORG                                         --:src.CREATE_ORG
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.CUST_SOURCE                                        --:src.CUST_SOURCE
       ,DST.ASSIGN_ORG_ID                                      --:src.ASSIGN_ORG_ID
       ,DST.CUST_SEX                                           --:src.CUST_SEX
       ,DST.CUST_BIR                                           --:src.CUST_BIR
       ,DST.CUST_MRG                                           --:src.CUST_MRG
       ,DST.CUST_NATION                                        --:src.CUST_NATION
       ,DST.CUST_REGISTER                                      --:src.CUST_REGISTER
       ,DST.CUST_REGADDR                                       --:src.CUST_REGADDR
       ,DST.CUST_FAMSTATUS                                     --:src.CUST_FAMSTATUS
       ,DST.CUST_HEALTH                                        --:src.CUST_HEALTH
       ,DST.CUST_WEIGHT                                        --:src.CUST_WEIGHT
       ,DST.CUST_HEIGHT                                        --:src.CUST_HEIGHT
       ,DST.HIGHEST_DEGREE                                     --:src.HIGHEST_DEGREE
       ,DST.CUST_OCCUP_COD                                     --:src.CUST_OCCUP_COD
       ,DST.CUST_EDU_LVL_COD                                   --:src.CUST_EDU_LVL_COD
       ,DST.CUST_WORK_UNIT_NAME                                --:src.CUST_WORK_UNIT_NAME
       ,DST.CUST_UTELL                                         --:src.CUST_UTELL
       ,DST.CUST_POSN                                          --:src.CUST_POSN
       ,DST.CUST_TITL                                          --:src.CUST_TITL
       ,DST.IS_MEDICARE                                        --:src.IS_MEDICARE
       ,DST.MAINPROORINCOME                                    --:src.MAINPROORINCOME
       ,DST.CUST_POSTCOD                                       --:src.CUST_POSTCOD
       ,DST.CUST_TEL_NO                                        --:src.CUST_TEL_NO
       ,DST.CUST_MBTELNO                                       --:src.CUST_MBTELNO
       ,DST.CUST_FAMADDR                                       --:src.CUST_FAMADDR
       ,DST.CUST_FAM_NUM                                       --:src.CUST_FAM_NUM
       ,DST.INDUSTRYTYPE                                       --:src.INDUSTRYTYPE
       ,DST.CUST_WORKADDR                                      --:src.CUST_WORKADDR
       ,DST.CUST_COMMADD                                       --:src.CUST_COMMADD
       ,DST.CI_ADDR                                            --:src.CI_ADDR
       ,DST.CUST_PECON_RESUR                                   --:src.CUST_PECON_RESUR
       ,DST.FAM_INCOMEACC                                      --:src.FAM_INCOMEACC
       ,DST.CUST_TOT_ASS                                       --:src.CUST_TOT_ASS
       ,DST.CRE_RECORD                                         --:src.CRE_RECORD
       ,DST.CUST_TOT_DEBT                                      --:src.CUST_TOT_DEBT
       ,DST.COM_START_DATE                                     --:src.COM_START_DATE
       ,DST.REG_CCY                                            --:src.REG_CCY
       ,DST.REG_CAPITAL                                        --:src.REG_CAPITAL
       ,DST.INCOME_CCY                                         --:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --:src.INCOME_SETTLE
       ,DST.INDUS_CLASS_MAIN                                   --:src.INDUS_CLASS_MAIN
       ,DST.INDUS_CLASS_DEPUTY                                 --:src.INDUS_CLASS_DEPUTY
       ,DST.ORG_TYP                                            --:src.ORG_TYP
       ,DST.COM_TYP                                            --:src.COM_TYP
       ,DST.LINCENSE_NO                                        --:src.LINCENSE_NO
       ,DST.IF_AGRICULTRUE                                     --:src.IF_AGRICULTRUE
       ,DST.LISTING_CORP_TYP                                   --:src.LISTING_CORP_TYP
       ,DST.IF_HIGH_TECH                                       --:src.IF_HIGH_TECH
       ,DST.IF_ESTATE                                          --:src.IF_ESTATE
       ,DST.COM_SCALE                                          --:src.COM_SCALE
       ,DST.IF_SHARE_CUST                                      --:src.IF_SHARE_CUST
       ,DST.ENT_QUA_LEVEL                                      --:src.ENT_QUA_LEVEL
       ,DST.IF_FANACING                                        --:src.IF_FANACING
       ,DST.LEGAL_NAME                                         --:src.LEGAL_NAME
       ,DST.FR_CERT_TYP                                        --:src.FR_CERT_TYP
       ,DST.FR_SEX                                             --:src.FR_SEX
       ,DST.LEGAL_CERT_NO                                      --:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --:src.LINK_MOBILE
       ,DST.LINK_TEL_FIN                                       --:src.LINK_TEL_FIN
       ,DST.WORK_ADDRESS                                       --:src.WORK_ADDRESS
       ,DST.BUSINESS                                           --:src.BUSINESS
       ,DST.SALE_ASSET                                         --:src.SALE_ASSET
       ,DST.EMPLOYEE_NUM                                       --:src.EMPLOYEE_NUM
       ,DST.LONGITUDE                                          --:src.LONGITUDE
       ,DST.LATITUDE                                           --:src.LATITUDE
       ,DST.CUST_LIVE_STATUS                                   --:src.CUST_LIVE_STATUS
       ,DST.COM_LOGO                                           --:src.COM_LOGO
       ,DST.ANNUAL_FAMILY_INCOME                               --:src.ANNUAL_FAMILY_INCOME
       ,DST.NOTE                                               --:src.NOTE
       ,DST.CREATE_DATE                                        --:src.CREATE_DATE
       ,DST.UPDATE_DATE                                        --:src.UPDATE_DATE
       ,DST.UPDATE_ORG                                         --:src.UPDATE_ORG
       ,DST.UPDATE_USER                                        --:src.UPDATE_USER
       ,DST.GIHHEST_DEGRE                                      --:src.GIHHEST_DEGRE
       ,DST.DEGREE                                             --:src.DEGREE
       ,DST.CUST_YEL_NO                                        --:src.CUST_YEL_NO
       ,DST.INDUS_CALSS_MAIN                                   --:src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --:src.INDUS_CLAS_DEPUTY
       ,DST.SEX                                                --:src.SEX
       ,DST.TOTAL_ASSET                                        --:src.TOTAL_ASSET
       ,DST.LINK_MAN_CERT_TYPE                                 --:src.LINK_MAN_CERT_TYPE
       ,DST.LINK_MAN_CERT_NO                                   --:src.LINK_MAN_CERT_NO
       ,DST.BACK                                               --:src.BACK
   FROM OCRM_F_CI_POTENTIAL_CUSTOMER DST 
   LEFT JOIN OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP1 SRC 
     ON SRC.CUST_NAME           = DST.CUST_NAME 
    AND SRC.CERT_TYP            = DST.CERT_TYP 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_POTENTIAL_CUSTOMER/"+V_DT+".parquet"
UNION=OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP2.unionAll(OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP1)
OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP1.cache()
OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP2.cache()
nrowsi = OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP1.count()
nrowsa = OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP1.unpersist()
OCRM_F_CI_POTENTIAL_CUSTOMER_INNTMP2.unpersist()
OCRM_F_CI_SYS_RESOURCE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_POTENTIAL_CUSTOMER lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_POTENTIAL_CUSTOMER_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_POTENTIAL_CUSTOMER/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_POTENTIAL_CUSTOMER_BK/")
