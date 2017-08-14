#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_ELASTIC_SEARCH').setMaster(sys.argv[2])
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

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")

#任务[21] 001-01::客户基础信息获取
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_TYP              AS CUST_TYP 
       ,COALESCE(COALESCE(A.CUST_ZH_NAME, B.CUST_NAME), C.CUST_NAME)                       AS CUST_ZH_NAME 
       ,COALESCE(COALESCE(A.CUST_ZH_NAME, B.CUST_NAME), C.CUST_NAME)                       AS CUST_NAME 
       ,COALESCE(COALESCE(A.CERT_NUM, B.CUST_CER_NO), C.CERT_NO)                       AS CERT_NUM 
       ,B.CUST_BIR              AS CUST_BIR 
       ,B.CUST_MRG              AS CUST_MRG 
       ,B.CUST_SEX              AS CUST_SEX 
       ,B.CUST_NATION           AS CUST_NATION 
       ,B.CUST_REGISTER         AS CUST_REGISTER 
       ,B.CUST_REGADDR          AS CUST_REGADDR 
       ,B.CUST_CITISHIP         AS CUST_CITISHIP 
       ,B.IS_STAFF              AS IS_STAFF 
       ,B.CUS_TYPE              AS CUS_TYPE 
       ,B.CUST_EVADATE          AS CUST_EVADATE 
       ,B.IS_GUDONG             AS IS_GUDONG 
       ,B.IS_SY                 AS IS_SY 
       ,B.IS_BUSSMA             AS IS_BUSSMA 
       ,B.CUST_FAMSTATUS        AS CUST_FAMSTATUS 
       ,B.CUST_HEALTH           AS CUST_HEALTH 
       ,B.CUST_POLIFACE         AS CUST_POLIFACE 
       ,B.ID_BLACKLIST          AS ID_BLACKLIST 
       ,B.CUST_EDU_LVL_COD      AS CUST_EDU_LVL_COD 
       ,B.GIHHEST_DEGREE        AS GIHHEST_DEGREE 
       ,B.OCCP_STATE            AS OCCP_STATE 
       ,B.CUST_POSN             AS CUST_POSN 
       ,B.CUST_WORK_YEAR        AS CUST_WORK_YEAR 
       ,B.IS_FARMER_FLG         AS IS_FARMER_FLG 
       ,B.HOUHOLD_CLASS         AS HOUHOLD_CLASS 
       ,B.IS_MEDICARE           AS IS_MEDICARE 
       ,B.IS_POORISNO           AS IS_POORISNO 
       ,B.IS_LIFSUR             AS IS_LIFSUR 
       ,B.IS_ILLSUR             AS IS_ILLSUR 
       ,B.IS_ENDOSUR            AS IS_ENDOSUR 
       ,B.HAVE_CAR              AS HAVE_CAR 
       ,B.AVG_ASS               AS AVG_ASS 
       ,B.CUST_CHILDREN         AS CUST_CHILDREN 
       ,B.CUST_HOUSE            AS CUST_HOUSE 
       ,B.CUST_PRIVATECAR       AS CUST_PRIVATECAR 
       ,B.CRECARD_POINTS        AS CRECARD_POINTS 
       ,B.CUST_MIDBUSJF         AS CUST_MIDBUSJF 
       ,B.CUST_CHANNELJF        AS CUST_CHANNELJF 
       ,B.CUST_CONSUMEJF        AS CUST_CONSUMEJF 
       ,B.CUST_FZJF             AS CUST_FZJF 
       ,B.CUST_ZCJF             AS CUST_ZCJF 
       ,B.CUST_SUMJF            AS CUST_SUMJF 
       ,B.CUST_COMMADD          AS CUST_COMMADD 
       ,B.CUST_MBTELNO          AS CUST_MBTELNO 
       ,B.CUST_OCCUP_COD        AS CUST_OCCUP_COD 
       ,B.CUST_CER_NO           AS CUST_CER_NO 
       ,B.CUST_PECON_RESUR      AS CUST_PECON_RESUR 
       ,C.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,C.COUNTRY               AS COUNTRY 
       ,C.PROVINCE              AS PROVINCE 
       ,C.WORK_ADDRESS          AS WORK_ADDRESS 
       ,C.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,C.OPEN_ORG2             AS OPEN_ORG2 
       ,C.REG_CAPITAL           AS REG_CAPITAL 
       ,C.BUSINESS              AS BUSINESS 
       ,C.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,C.TOTAL_ASSET           AS TOTAL_ASSET 
       ,C.SALE_ASSET            AS SALE_ASSET 
       ,C.TAX_NO                AS TAX_NO 
       ,C.RENT_NO               AS RENT_NO 
       ,C.BOND_FLAG             AS BOND_FLAG 
       ,C.BUS_AREA              AS BUS_AREA 
       ,C.BUS_STAT              AS BUS_STAT 
       ,C.CERT_TYP              AS CERT_TYP 
       ,C.COM_SCALE             AS COM_SCALE 
       ,C.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,C.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,C.BUS_TYP               AS BUS_TYP 
       ,C.ORG_TYP               AS ORG_TYP 
       ,C.ECO_TYP               AS ECO_TYP 
       ,C.COM_TYP               AS COM_TYP 
       ,C.COM_LEVEL             AS COM_LEVEL 
       ,C.OBJECT_RATE           AS OBJECT_RATE 
       ,C.SUBJECT_RATE          AS SUBJECT_RATE 
       ,C.EFF_DATE              AS EFF_DATE 
       ,C.RATE_DATE             AS RATE_DATE 
       ,C.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,C.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,C.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,C.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,C.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,C.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,C.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,C.IF_BASIC              AS IF_BASIC 
       ,C.IF_ESTATE             AS IF_ESTATE 
       ,C.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,C.IF_SMALL              AS IF_SMALL 
       ,C.IF_IBK                AS IF_IBK 
       ,C.IF_EXPESS             AS IF_EXPESS 
       ,C.IF_MONITER            AS IF_MONITER 
       ,C.IF_FANACING           AS IF_FANACING 
       ,C.IF_INT                AS IF_INT 
       ,C.IF_GROUP              AS IF_GROUP 
       ,C.RIGHT_FLAG            AS RIGHT_FLAG 
       ,C.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,C.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,C.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,C.SYN_FLAG              AS SYN_FLAG 
       ,C.ADJUST_TYP            AS ADJUST_TYP 
       ,C.EMERGING_TYP          AS EMERGING_TYP 
       ,C.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,C.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,C.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,C.CERT_NO               AS CERT_NO 
       ,''                      AS PRODUCT_ID 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS CREDIT_LINE 
       ,CAST(0                       AS INTEGER)                       AS OVER_NUM 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS OVER_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS SXJE 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS YXJE 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS DBJE 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS DEP_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS LOWCOST_DEP_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS CARD_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS DEP_YEAR_AVG 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS DEP_MONTH_AVG 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS LOWCOST_DEP_YEAR_AVG 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS LOWCOST_DEP_MONTH_AVG 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS CARD_YEAR_AVG 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS CARD_MONTH_AVG 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS LOAN_YEAR_AVG 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS LOAN_MONTH_AVG 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS BL_LOAN_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS QX_LOAN_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS HX_LOAN_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS LOAN_BAL 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS CREDIT_BAL 
       ,''                      AS IF_NEW_CARD 
       ,''                      AS MGR_ID 
       ,''                      AS MGR_NAME 
       ,''                      AS MAIN_TYPE_MGR 
       ,''                      AS INSTITUTION_CODE 
       ,''                      AS INSTITUTION_NAME 
       ,''                      AS MAIN_TYPE_ORG 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS CUST_CONTRIBUTION 
       ,CAST(0                       AS DECIMAL(22, 2))                       AS LOYA_SCORE 
       ,CAST(0                       AS DECIMAL(22, 2))                       AS MONTH_TOTAL_INT 
       ,CAST(0                       AS DECIMAL(22, 2))                       AS CUST_TOTAL_INT 
       ,CAST(0                       AS DECIMAL(22, 2))                       AS MONTH_COST_INT 
       ,CAST(0                       AS DECIMAL(22, 2))                       AS CUST_TOTAL_COST 
       ,CAST(0                       AS DECIMAL(22, 2))                       AS CUST_USABLE_INT 
       ,A.OBJ_RATING            AS OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,CAST(0                       AS DECIMAL(22, 2))                       AS ALERT_SCORE 
       ,CAST(0                       AS DECIMAL(22, 2))                       AS LOST_SCORE 
       ,''                      AS LIFE_CYCLE 
       ,'N'                     AS IS_JJK 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 2, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_DJK 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 5, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_WY 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 6, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_SJYH 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 7, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_DXYH 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 4, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_ZFB 
       ,'N'                     AS IS_XNB 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 25, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_SB 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 26, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_JHSB 
       ,'N'                     AS IS_YB 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 8, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_ELEC 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 9, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_WATER 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 10, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_GAS 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 11, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_TV 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 12, 1)                       = '1' THEN 'Y' ELSE 'N' END                     AS IS_WIRE 
       ,CAST(0                       AS INTEGER)                       AS BNJJKBS 
       ,CAST(0                       AS INTEGER)                       AS BJJJKBS 
       ,CAST(0                       AS INTEGER)                       AS BYJJKBS 
       ,CAST(0                       AS INTEGER)                       AS BNDJKBS 
       ,CAST(0                       AS INTEGER)                       AS BJDJKBS 
       ,CAST(0                       AS INTEGER)                       AS BYDJKBS 
       ,CAST(0                       AS INTEGER)                       AS BNWYBS 
       ,CAST(0                       AS INTEGER)                       AS BJWYBS 
       ,CAST(0                       AS INTEGER)                       AS BYWYBS 
       ,CAST(0                       AS INTEGER)                       AS BNSJYHBS 
       ,CAST(0                       AS INTEGER)                       AS BJSJYHBS 
       ,CAST(0                       AS INTEGER)                       AS BYSJYHBS 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS BYJJKFSE 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS BJJJKFSE 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS BNJJKFSE 
       ,CAST(0                       AS INTEGER)                       AS M_OCCUR 
       ,CAST(0                       AS INTEGER)                       AS Q_OCCUR 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS M_OCCUR_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS Q_OCCUR_AMT 
       ,CAST(0                       AS INTEGER)                       AS M_COM_OCCUR 
       ,CAST(0                       AS INTEGER)                       AS Q_COM_OCCUR 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS M_COM_OCCUR_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS Q_COM_OCCUR_AMT 
       ,CAST(0                       AS INTEGER)                       AS M_COM_INCOME 
       ,CAST(0                       AS INTEGER)                       AS Q_COM_INCOME 
       ,CAST(0                       AS INTEGER)                       AS M_COM_OUTCOME 
       ,CAST(0                       AS INTEGER)                       AS Q_COM_OUTCOME 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS M_COM_INCOME_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS Q_COM_INCOME_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS M_COM_OUTCOME_AMT 
       ,CAST(0                       AS DECIMAL(24, 6))                       AS Q_COM_OUTCOME_AMT 
       ,''                      AS FULLNAME 
       ,''                      AS CERTID 
       ,''                      AS NATIVEPLACE 
       ,''                      AS MARRIAGE 
       ,''                      AS EDUEXPERIENCE 
       ,''                      AS FAMILYADD 
       ,''                      AS FAMILYZIP 
       ,''                      AS FAMILYTEL 
       ,''                      AS MOBILETELEPHONE 
       ,''                      AS ISHZ 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
   LEFT JOIN OCRM_F_CI_PER_CUST_INFO B                         --零售客户基本信息
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID 
    AND A.CUST_TYP              = '1' 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO C                         --对公客户基本信息
     ON A.FR_ID                 = C.FR_ID 
    AND A.CUST_ID               = C.CUST_ID 
    AND A.CUST_TYP              = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_ELASTIC_SEARCH = sqlContext.sql(sql)
print("*****************1********************")
print(ACRM_A_ELASTIC_SEARCH.count())
print("*************************************")
ACRM_A_ELASTIC_SEARCH.registerTempTable("ACRM_A_ELASTIC_SEARCH")
dfn="ACRM_A_ELASTIC_SEARCH/"+V_DT+".parquet"
ACRM_A_ELASTIC_SEARCH.cache()
nrows = ACRM_A_ELASTIC_SEARCH.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_ELASTIC_SEARCH/*.parquet")
ACRM_A_ELASTIC_SEARCH.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_ELASTIC_SEARCH.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_ELASTIC_SEARCH lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::阳光信贷客户增改
V_STEP = V_STEP + 1

ACRM_A_ELASTIC_SEARCH = sqlContext.read.parquet(hdfs+'/ACRM_A_ELASTIC_SEARCH/*')
ACRM_A_ELASTIC_SEARCH.registerTempTable("ACRM_A_ELASTIC_SEARCH")
OCRM_F_CI_SUN_IND_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_IND_INFO/*')
OCRM_F_CI_SUN_IND_INFO.registerTempTable("OCRM_F_CI_SUN_IND_INFO")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_NUM              AS CERT_NUM 
       ,A.CUST_BIR              AS CUST_BIR 
       ,A.CUST_MRG              AS CUST_MRG 
       ,A.CUST_SEX              AS CUST_SEX 
       ,A.CUST_NATION           AS CUST_NATION 
       ,A.CUST_REGISTER         AS CUST_REGISTER 
       ,A.CUST_REGADDR          AS CUST_REGADDR 
       ,A.CUST_CITISHIP         AS CUST_CITISHIP 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUS_TYPE              AS CUS_TYPE 
       ,A.CUST_EVADATE          AS CUST_EVADATE 
       ,A.IS_GUDONG             AS IS_GUDONG 
       ,A.IS_SY                 AS IS_SY 
       ,A.IS_BUSSMA             AS IS_BUSSMA 
       ,A.CUST_FAMSTATUS        AS CUST_FAMSTATUS 
       ,A.CUST_HEALTH           AS CUST_HEALTH 
       ,A.CUST_POLIFACE         AS CUST_POLIFACE 
       ,A.ID_BLACKLIST          AS ID_BLACKLIST 
       ,A.CUST_EDU_LVL_COD      AS CUST_EDU_LVL_COD 
       ,A.GIHHEST_DEGREE        AS GIHHEST_DEGREE 
       ,A.OCCP_STATE            AS OCCP_STATE 
       ,A.CUST_POSN             AS CUST_POSN 
       ,A.CUST_WORK_YEAR        AS CUST_WORK_YEAR 
       ,A.IS_FARMER_FLG         AS IS_FARMER_FLG 
       ,A.HOUHOLD_CLASS         AS HOUHOLD_CLASS 
       ,A.IS_MEDICARE           AS IS_MEDICARE 
       ,A.IS_POORISNO           AS IS_POORISNO 
       ,A.IS_LIFSUR             AS IS_LIFSUR 
       ,A.IS_ILLSUR             AS IS_ILLSUR 
       ,A.IS_ENDOSUR            AS IS_ENDOSUR 
       ,A.HAVE_CAR              AS HAVE_CAR 
       ,A.AVG_ASS               AS AVG_ASS 
       ,A.CUST_CHILDREN         AS CUST_CHILDREN 
       ,A.CUST_HOUSE            AS CUST_HOUSE 
       ,A.CUST_PRIVATECAR       AS CUST_PRIVATECAR 
       ,A.CRECARD_POINTS        AS CRECARD_POINTS 
       ,A.CUST_MIDBUSJF         AS CUST_MIDBUSJF 
       ,A.CUST_CHANNELJF        AS CUST_CHANNELJF 
       ,A.CUST_CONSUMEJF        AS CUST_CONSUMEJF 
       ,A.CUST_FZJF             AS CUST_FZJF 
       ,A.CUST_ZCJF             AS CUST_ZCJF 
       ,A.CUST_SUMJF            AS CUST_SUMJF 
       ,A.CUST_COMMADD          AS CUST_COMMADD 
       ,A.CUST_MBTELNO          AS CUST_MBTELNO 
       ,A.CUST_OCCUP_COD        AS CUST_OCCUP_COD 
       ,A.CUST_CER_NO           AS CUST_CER_NO 
       ,A.CUST_PECON_RESUR      AS CUST_PECON_RESUR 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
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
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.CERT_NO               AS CERT_NO 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,A.CREDIT_LINE           AS CREDIT_LINE 
       ,A.OVER_NUM              AS OVER_NUM 
       ,A.OVER_AMT              AS OVER_AMT 
       ,A.SXJE                  AS SXJE 
       ,A.YXJE                  AS YXJE 
       ,A.DBJE                  AS DBJE 
       ,A.DEP_AMT               AS DEP_AMT 
       ,A.LOWCOST_DEP_AMT       AS LOWCOST_DEP_AMT 
       ,A.CARD_AMT              AS CARD_AMT 
       ,A.DEP_YEAR_AVG          AS DEP_YEAR_AVG 
       ,A.DEP_MONTH_AVG         AS DEP_MONTH_AVG 
       ,A.LOWCOST_DEP_YEAR_AVG  AS LOWCOST_DEP_YEAR_AVG 
       ,A.LOWCOST_DEP_MONTH_AVG AS LOWCOST_DEP_MONTH_AVG 
       ,A.CARD_YEAR_AVG         AS CARD_YEAR_AVG 
       ,A.CARD_MONTH_AVG        AS CARD_MONTH_AVG 
       ,A.LOAN_YEAR_AVG         AS LOAN_YEAR_AVG 
       ,A.LOAN_MONTH_AVG        AS LOAN_MONTH_AVG 
       ,A.BL_LOAN_AMT           AS BL_LOAN_AMT 
       ,A.QX_LOAN_AMT           AS QX_LOAN_AMT 
       ,A.HX_LOAN_AMT           AS HX_LOAN_AMT 
       ,A.LOAN_BAL              AS LOAN_BAL 
       ,A.CREDIT_BAL            AS CREDIT_BAL 
       ,A.IF_NEW_CARD           AS IF_NEW_CARD 
       ,A.MGR_ID                AS MGR_ID 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.MAIN_TYPE_MGR         AS MAIN_TYPE_MGR 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MAIN_TYPE_ORG         AS MAIN_TYPE_ORG 
       ,A.CUST_CONTRIBUTION     AS CUST_CONTRIBUTION 
       ,A.LOYA_SCORE            AS LOYA_SCORE 
       ,A.MONTH_TOTAL_INT       AS MONTH_TOTAL_INT 
       ,A.CUST_TOTAL_INT        AS CUST_TOTAL_INT 
       ,A.MONTH_COST_INT        AS MONTH_COST_INT 
       ,A.CUST_TOTAL_COST       AS CUST_TOTAL_COST 
       ,A.CUST_USABLE_INT       AS CUST_USABLE_INT 
       ,A.OBJ_RATING            AS OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,A.ALERT_SCORE           AS ALERT_SCORE 
       ,A.LOST_SCORE            AS LOST_SCORE 
       ,A.LIFE_CYCLE            AS LIFE_CYCLE 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_JHSB               AS IS_JHSB 
       ,A.IS_YB                 AS IS_YB 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.BNJJKBS               AS BNJJKBS 
       ,A.BJJJKBS               AS BJJJKBS 
       ,A.BYJJKBS               AS BYJJKBS 
       ,A.BNDJKBS               AS BNDJKBS 
       ,A.BJDJKBS               AS BJDJKBS 
       ,A.BYDJKBS               AS BYDJKBS 
       ,A.BNWYBS                AS BNWYBS 
       ,A.BJWYBS                AS BJWYBS 
       ,A.BYWYBS                AS BYWYBS 
       ,A.BNSJYHBS              AS BNSJYHBS 
       ,A.BJSJYHBS              AS BJSJYHBS 
       ,A.BYSJYHBS              AS BYSJYHBS 
       ,A.BYJJKFSE              AS BYJJKFSE 
       ,A.BJJJKFSE              AS BJJJKFSE 
       ,A.BNJJKFSE              AS BNJJKFSE 
       ,A.M_OCCUR               AS M_OCCUR 
       ,A.Q_OCCUR               AS Q_OCCUR 
       ,A.M_OCCUR_AMT           AS M_OCCUR_AMT 
       ,A.Q_OCCUR_AMT           AS Q_OCCUR_AMT 
       ,A.M_COM_OCCUR           AS M_COM_OCCUR 
       ,A.Q_COM_OCCUR           AS Q_COM_OCCUR 
       ,A.M_COM_OCCUR_AMT       AS M_COM_OCCUR_AMT 
       ,A.Q_COM_OCCUR_AMT       AS Q_COM_OCCUR_AMT 
       ,A.M_COM_INCOME          AS M_COM_INCOME 
       ,A.Q_COM_INCOME          AS Q_COM_INCOME 
       ,A.M_COM_OUTCOME         AS M_COM_OUTCOME 
       ,A.Q_COM_OUTCOME         AS Q_COM_OUTCOME 
       ,A.M_COM_INCOME_AMT      AS M_COM_INCOME_AMT 
       ,A.Q_COM_INCOME_AMT      AS Q_COM_INCOME_AMT 
       ,A.M_COM_OUTCOME_AMT     AS M_COM_OUTCOME_AMT 
       ,A.Q_COM_OUTCOME_AMT     AS Q_COM_OUTCOME_AMT 
       ,B.FULLNAME              AS FULLNAME 
       ,B.CERTID                AS CERTID 
       ,B.NATIVEPLACE           AS NATIVEPLACE 
       ,B.MARRIAGE              AS MARRIAGE 
       ,B.EDUEXPERIENCE         AS EDUEXPERIENCE 
       ,B.FAMILYADD             AS FAMILYADD 
       ,B.FAMILYZIP             AS FAMILYZIP 
       ,B.FAMILYTEL             AS FAMILYTEL 
       ,B.MOBILETELEPHONE       AS MOBILETELEPHONE 
       ,B.ISHZ                  AS ISHZ 
       ,B.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_SUN_IND_INFO B                               --阳光信贷客户信息
   LEFT JOIN ACRM_A_ELASTIC_SEARCH A                           --高级查询汇总表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_ELASTIC_SEARCH_INNTMP1 = sqlContext.sql(sql)
ACRM_A_ELASTIC_SEARCH_INNTMP1.registerTempTable("ACRM_A_ELASTIC_SEARCH_INNTMP1")

#ACRM_A_ELASTIC_SEARCH = sqlContext.read.parquet(hdfs+'/ACRM_A_ELASTIC_SEARCH/*')
#ACRM_A_ELASTIC_SEARCH.registerTempTable("ACRM_A_ELASTIC_SEARCH")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CUST_ZH_NAME                                       --中文名称:src.CUST_ZH_NAME
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_BIR                                           --注册/出生日期:src.CUST_BIR
       ,DST.CUST_MRG                                           --婚姻状况:src.CUST_MRG
       ,DST.CUST_SEX                                           --性别:src.CUST_SEX
       ,DST.CUST_NATION                                        --民族:src.CUST_NATION
       ,DST.CUST_REGISTER                                      --户籍:src.CUST_REGISTER
       ,DST.CUST_REGADDR                                       --户籍地址:src.CUST_REGADDR
       ,DST.CUST_CITISHIP                                      --国籍:src.CUST_CITISHIP
       ,DST.IS_STAFF                                           --员工标志:src.IS_STAFF
       ,DST.CUS_TYPE                                           --客户类型:src.CUS_TYPE
       ,DST.CUST_EVADATE                                       --本行评估日期:src.CUST_EVADATE
       ,DST.IS_GUDONG                                          --是否本社股东:src.IS_GUDONG
       ,DST.IS_SY                                              --是否社员:src.IS_SY
       ,DST.IS_BUSSMA                                          --是否个体工商户:src.IS_BUSSMA
       ,DST.CUST_FAMSTATUS                                     --家庭状况:src.CUST_FAMSTATUS
       ,DST.CUST_HEALTH                                        --健康状况:src.CUST_HEALTH
       ,DST.CUST_POLIFACE                                      --政治面貌:src.CUST_POLIFACE
       ,DST.ID_BLACKLIST                                       --上本行黑名单标志:src.ID_BLACKLIST
       ,DST.CUST_EDU_LVL_COD                                   --最高学历代码:src.CUST_EDU_LVL_COD
       ,DST.GIHHEST_DEGREE                                     --最高学位:src.GIHHEST_DEGREE
       ,DST.OCCP_STATE                                         --职业状态:src.OCCP_STATE
       ,DST.CUST_POSN                                          --职务:src.CUST_POSN
       ,DST.CUST_WORK_YEAR                                     --参加工作年份:src.CUST_WORK_YEAR
       ,DST.IS_FARMER_FLG                                      --农户标识:src.IS_FARMER_FLG
       ,DST.HOUHOLD_CLASS                                      --农户分类:src.HOUHOLD_CLASS
       ,DST.IS_MEDICARE                                        --是否参加农村新型合作医疗保险:src.IS_MEDICARE
       ,DST.IS_POORISNO                                        --是否扶贫户:src.IS_POORISNO
       ,DST.IS_LIFSUR                                          --是否参加人寿保险:src.IS_LIFSUR
       ,DST.IS_ILLSUR                                          --是否参加大病保险:src.IS_ILLSUR
       ,DST.IS_ENDOSUR                                         --是否参加养老保险:src.IS_ENDOSUR
       ,DST.HAVE_CAR                                           --是否拥有车辆:src.HAVE_CAR
       ,DST.AVG_ASS                                            --日均资产:src.AVG_ASS
       ,DST.CUST_CHILDREN                                      --是否有子女:src.CUST_CHILDREN
       ,DST.CUST_HOUSE                                         --是否有房产:src.CUST_HOUSE
       ,DST.CUST_PRIVATECAR                                    --是否有私家车:src.CUST_PRIVATECAR
       ,DST.CRECARD_POINTS                                     --信用卡消费积分:src.CRECARD_POINTS
       ,DST.CUST_MIDBUSJF                                      --中间业务积分:src.CUST_MIDBUSJF
       ,DST.CUST_CHANNELJF                                     --渠道积分:src.CUST_CHANNELJF
       ,DST.CUST_CONSUMEJF                                     --消费积分:src.CUST_CONSUMEJF
       ,DST.CUST_FZJF                                          --负债积分:src.CUST_FZJF
       ,DST.CUST_ZCJF                                          --资产积分:src.CUST_ZCJF
       ,DST.CUST_SUMJF                                         --总积分:src.CUST_SUMJF
       ,DST.CUST_COMMADD                                       --通讯地址:src.CUST_COMMADD
       ,DST.CUST_MBTELNO                                       --联系方式(手机):src.CUST_MBTELNO
       ,DST.CUST_OCCUP_COD                                     --职业:src.CUST_OCCUP_COD
       ,DST.CUST_CER_NO                                        --证件号码:src.CUST_CER_NO
       ,DST.CUST_PECON_RESUR                                   --主要经济来源:src.CUST_PECON_RESUR
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
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
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.PRODUCT_ID                                         --持有此类产品:src.PRODUCT_ID
       ,DST.CREDIT_LINE                                        --贷记卡额度:src.CREDIT_LINE
       ,DST.OVER_NUM                                           --贷记卡逾期次数:src.OVER_NUM
       ,DST.OVER_AMT                                           --贷记卡逾期金额:src.OVER_AMT
       ,DST.SXJE                                               --授信金额:src.SXJE
       ,DST.YXJE                                               --用信金额:src.YXJE
       ,DST.DBJE                                               --担保金额:src.DBJE
       ,DST.DEP_AMT                                            --存款总额:src.DEP_AMT
       ,DST.LOWCOST_DEP_AMT                                    --低成本存款金额:src.LOWCOST_DEP_AMT
       ,DST.CARD_AMT                                           --借记卡余额:src.CARD_AMT
       ,DST.DEP_YEAR_AVG                                       --存款本年日平:src.DEP_YEAR_AVG
       ,DST.DEP_MONTH_AVG                                      --存款本月日平:src.DEP_MONTH_AVG
       ,DST.LOWCOST_DEP_YEAR_AVG                               --低成本存款本年日平:src.LOWCOST_DEP_YEAR_AVG
       ,DST.LOWCOST_DEP_MONTH_AVG                              --低成本存款本月日平:src.LOWCOST_DEP_MONTH_AVG
       ,DST.CARD_YEAR_AVG                                      --借记卡本年日平:src.CARD_YEAR_AVG
       ,DST.CARD_MONTH_AVG                                     --借记卡本月日平:src.CARD_MONTH_AVG
       ,DST.LOAN_YEAR_AVG                                      --贷款本年日平:src.LOAN_YEAR_AVG
       ,DST.LOAN_MONTH_AVG                                     --贷款本月日平:src.LOAN_MONTH_AVG
       ,DST.BL_LOAN_AMT                                        --不良贷款余额:src.BL_LOAN_AMT
       ,DST.QX_LOAN_AMT                                        --欠息金额:src.QX_LOAN_AMT
       ,DST.HX_LOAN_AMT                                        --已核销贷款余额:src.HX_LOAN_AMT
       ,DST.LOAN_BAL                                           --贷款余额:src.LOAN_BAL
       ,DST.CREDIT_BAL                                         --信用卡可用额度:src.CREDIT_BAL
       ,DST.IF_NEW_CARD                                        --是否本月开卡客户:src.IF_NEW_CARD
       ,DST.MGR_ID                                             --客户经理编码:src.MGR_ID
       ,DST.MGR_NAME                                           --客户经理名称:src.MGR_NAME
       ,DST.MAIN_TYPE_MGR                                      --经理主协办类型:src.MAIN_TYPE_MGR
       ,DST.INSTITUTION_CODE                                   --归属机构代码:src.INSTITUTION_CODE
       ,DST.INSTITUTION_NAME                                   --归属机构名称:src.INSTITUTION_NAME
       ,DST.MAIN_TYPE_ORG                                      --机构主协办类型:src.MAIN_TYPE_ORG
       ,DST.CUST_CONTRIBUTION                                  --客户贡献度:src.CUST_CONTRIBUTION
       ,DST.LOYA_SCORE                                         --客户忠诚度:src.LOYA_SCORE
       ,DST.MONTH_TOTAL_INT                                    --当月新增积分:src.MONTH_TOTAL_INT
       ,DST.CUST_TOTAL_INT                                     --累计总积分:src.CUST_TOTAL_INT
       ,DST.MONTH_COST_INT                                     --当月消费积分:src.MONTH_COST_INT
       ,DST.CUST_TOTAL_COST                                    --累计消费积分:src.CUST_TOTAL_COST
       ,DST.CUST_USABLE_INT                                    --当前可用积分:src.CUST_USABLE_INT
       ,DST.OBJ_RATING                                         --客观评级:src.OBJ_RATING
       ,DST.SUB_RATING                                         --主观评级:src.SUB_RATING
       ,DST.ALERT_SCORE                                        --风险评估得分:src.ALERT_SCORE
       ,DST.LOST_SCORE                                         --流失预警得分:src.LOST_SCORE
       ,DST.LIFE_CYCLE                                         --客户生命周期:src.LIFE_CYCLE
       ,DST.IS_JJK                                             --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK                                             --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY                                              --是否有网银:src.IS_WY
       ,DST.IS_SJYH                                            --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH                                            --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB                                             --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB                                             --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB                                              --是否开通社保:src.IS_SB
       ,DST.IS_JHSB                                            --是否激活社保卡:src.IS_JHSB
       ,DST.IS_YB                                              --是否开通医保:src.IS_YB
       ,DST.IS_ELEC                                            --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER                                           --是否水费签约:src.IS_WATER
       ,DST.IS_GAS                                             --是否燃气签约:src.IS_GAS
       ,DST.IS_TV                                              --是否广电签约:src.IS_TV
       ,DST.IS_WIRE                                            --是否电信签约:src.IS_WIRE
       ,DST.BNJJKBS                                            --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS                                            --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS                                            --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS                                            --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS                                            --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS                                            --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS                                             --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS                                             --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS                                             --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS                                           --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS                                           --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS                                           --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.BYJJKFSE                                           --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE                                           --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE                                           --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR                                            --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR                                            --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                                        --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                                        --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                                        --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                                        --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                                    --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                                    --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                                       --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                                       --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                                      --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                                      --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT                                   --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT                                   --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT                                  --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT                                  --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.FULLNAME                                           --客户姓名:src.FULLNAME
       ,DST.CERTID                                             --证件号码:src.CERTID
       ,DST.NATIVEPLACE                                        --户籍地址:src.NATIVEPLACE
       ,DST.MARRIAGE                                           --婚姻状况:src.MARRIAGE
       ,DST.EDUEXPERIENCE                                      --最高学历:src.EDUEXPERIENCE
       ,DST.FAMILYADD                                          --居住地址:src.FAMILYADD
       ,DST.FAMILYZIP                                          --居住地址邮编:src.FAMILYZIP
       ,DST.FAMILYTEL                                          --住宅电话:src.FAMILYTEL
       ,DST.MOBILETELEPHONE                                    --联系号码:src.MOBILETELEPHONE
       ,DST.ISHZ                                               --是否为户主:src.ISHZ
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_A_ELASTIC_SEARCH DST 
   LEFT JOIN ACRM_A_ELASTIC_SEARCH_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_ELASTIC_SEARCH_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_ELASTIC_SEARCH/"+V_DT+".parquet"
ACRM_A_ELASTIC_SEARCH_INNTMP2=ACRM_A_ELASTIC_SEARCH_INNTMP2.unionAll(ACRM_A_ELASTIC_SEARCH_INNTMP1)
ACRM_A_ELASTIC_SEARCH_INNTMP1.cache()
ACRM_A_ELASTIC_SEARCH_INNTMP2.cache()
nrowsi = ACRM_A_ELASTIC_SEARCH_INNTMP1.count()
nrowsa = ACRM_A_ELASTIC_SEARCH_INNTMP2.count()
ACRM_A_ELASTIC_SEARCH_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_ELASTIC_SEARCH_INNTMP1.unpersist()
ACRM_A_ELASTIC_SEARCH_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_ELASTIC_SEARCH lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_ELASTIC_SEARCH/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_ELASTIC_SEARCH_BK/")

#任务[12] 001-03::ACRM_A_BUSINESS_INFO
V_STEP = V_STEP + 1

ACRM_A_ELASTIC_SEARCH = sqlContext.read.parquet(hdfs+'/ACRM_A_ELASTIC_SEARCH/*')
ACRM_A_ELASTIC_SEARCH.registerTempTable("ACRM_A_ELASTIC_SEARCH")
ACRM_A_BUSINESS_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_BUSINESS_INFO/*')
ACRM_A_BUSINESS_INFO.registerTempTable("ACRM_A_BUSINESS_INFO")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_NUM              AS CERT_NUM 
       ,A.CUST_BIR              AS CUST_BIR 
       ,A.CUST_MRG              AS CUST_MRG 
       ,A.CUST_SEX              AS CUST_SEX 
       ,A.CUST_NATION           AS CUST_NATION 
       ,A.CUST_REGISTER         AS CUST_REGISTER 
       ,A.CUST_REGADDR          AS CUST_REGADDR 
       ,A.CUST_CITISHIP         AS CUST_CITISHIP 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUS_TYPE              AS CUS_TYPE 
       ,A.CUST_EVADATE          AS CUST_EVADATE 
       ,A.IS_GUDONG             AS IS_GUDONG 
       ,A.IS_SY                 AS IS_SY 
       ,A.IS_BUSSMA             AS IS_BUSSMA 
       ,A.CUST_FAMSTATUS        AS CUST_FAMSTATUS 
       ,A.CUST_HEALTH           AS CUST_HEALTH 
       ,A.CUST_POLIFACE         AS CUST_POLIFACE 
       ,A.ID_BLACKLIST          AS ID_BLACKLIST 
       ,A.CUST_EDU_LVL_COD      AS CUST_EDU_LVL_COD 
       ,A.GIHHEST_DEGREE        AS GIHHEST_DEGREE 
       ,A.OCCP_STATE            AS OCCP_STATE 
       ,A.CUST_POSN             AS CUST_POSN 
       ,A.CUST_WORK_YEAR        AS CUST_WORK_YEAR 
       ,A.IS_FARMER_FLG         AS IS_FARMER_FLG 
       ,A.HOUHOLD_CLASS         AS HOUHOLD_CLASS 
       ,A.IS_MEDICARE           AS IS_MEDICARE 
       ,A.IS_POORISNO           AS IS_POORISNO 
       ,A.IS_LIFSUR             AS IS_LIFSUR 
       ,A.IS_ILLSUR             AS IS_ILLSUR 
       ,A.IS_ENDOSUR            AS IS_ENDOSUR 
       ,A.HAVE_CAR              AS HAVE_CAR 
       ,A.AVG_ASS               AS AVG_ASS 
       ,A.CUST_CHILDREN         AS CUST_CHILDREN 
       ,A.CUST_HOUSE            AS CUST_HOUSE 
       ,A.CUST_PRIVATECAR       AS CUST_PRIVATECAR 
       ,A.CRECARD_POINTS        AS CRECARD_POINTS 
       ,A.CUST_MIDBUSJF         AS CUST_MIDBUSJF 
       ,A.CUST_CHANNELJF        AS CUST_CHANNELJF 
       ,A.CUST_CONSUMEJF        AS CUST_CONSUMEJF 
       ,A.CUST_FZJF             AS CUST_FZJF 
       ,A.CUST_ZCJF             AS CUST_ZCJF 
       ,A.CUST_SUMJF            AS CUST_SUMJF 
       ,A.CUST_COMMADD          AS CUST_COMMADD 
       ,A.CUST_MBTELNO          AS CUST_MBTELNO 
       ,A.CUST_OCCUP_COD        AS CUST_OCCUP_COD 
       ,A.CUST_CER_NO           AS CUST_CER_NO 
       ,A.CUST_PECON_RESUR      AS CUST_PECON_RESUR 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
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
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.CERT_NO               AS CERT_NO 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,B.CREDIT_LINE           AS CREDIT_LINE 
       ,B.OVER_NUM              AS OVER_NUM 
       ,B.OVER_AMT              AS OVER_AMT 
       ,B.SXJE                  AS SXJE 
       ,B.YXJE                  AS YXJE 
       ,B.DBJE                  AS DBJE 
       ,B.DEP_AMT               AS DEP_AMT 
       ,B.LOWCOST_DEP_AMT       AS LOWCOST_DEP_AMT 
       ,B.CARD_AMT              AS CARD_AMT 
       ,B.DEP_YEAR_AVG          AS DEP_YEAR_AVG 
       ,B.DEP_MONTH_AVG         AS DEP_MONTH_AVG 
       ,B.LOWCOST_DEP_YEAR_AVG  AS LOWCOST_DEP_YEAR_AVG 
       ,B.LOWCOST_DEP_MONTH_AVG AS LOWCOST_DEP_MONTH_AVG 
       ,B.CARD_YEAR_AVG         AS CARD_YEAR_AVG 
       ,B.CARD_MONTH_AVG        AS CARD_MONTH_AVG 
       ,B.LOAN_YEAR_AVG         AS LOAN_YEAR_AVG 
       ,B.LOAN_MONTH_AVG        AS LOAN_MONTH_AVG 
       ,B.BL_LOAN_AMT           AS BL_LOAN_AMT 
       ,B.QX_LOAN_AMT           AS QX_LOAN_AMT 
       ,B.HX_LOAN_AMT           AS HX_LOAN_AMT 
       ,B.LOAN_BAL              AS LOAN_BAL 
       ,B.CREDIT_BAL            AS CREDIT_BAL 
       ,B.IF_NEW_CARD           AS IF_NEW_CARD 
       ,A.MGR_ID                AS MGR_ID 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.MAIN_TYPE_MGR         AS MAIN_TYPE_MGR 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MAIN_TYPE_ORG         AS MAIN_TYPE_ORG 
       ,A.CUST_CONTRIBUTION     AS CUST_CONTRIBUTION 
       ,A.LOYA_SCORE            AS LOYA_SCORE 
       ,A.MONTH_TOTAL_INT       AS MONTH_TOTAL_INT 
       ,A.CUST_TOTAL_INT        AS CUST_TOTAL_INT 
       ,A.MONTH_COST_INT        AS MONTH_COST_INT 
       ,A.CUST_TOTAL_COST       AS CUST_TOTAL_COST 
       ,A.CUST_USABLE_INT       AS CUST_USABLE_INT 
       ,A.OBJ_RATING            AS OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,A.ALERT_SCORE           AS ALERT_SCORE 
       ,A.LOST_SCORE            AS LOST_SCORE 
       ,A.LIFE_CYCLE            AS LIFE_CYCLE 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_JHSB               AS IS_JHSB 
       ,A.IS_YB                 AS IS_YB 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.BNJJKBS               AS BNJJKBS 
       ,A.BJJJKBS               AS BJJJKBS 
       ,A.BYJJKBS               AS BYJJKBS 
       ,A.BNDJKBS               AS BNDJKBS 
       ,A.BJDJKBS               AS BJDJKBS 
       ,A.BYDJKBS               AS BYDJKBS 
       ,A.BNWYBS                AS BNWYBS 
       ,A.BJWYBS                AS BJWYBS 
       ,A.BYWYBS                AS BYWYBS 
       ,A.BNSJYHBS              AS BNSJYHBS 
       ,A.BJSJYHBS              AS BJSJYHBS 
       ,A.BYSJYHBS              AS BYSJYHBS 
       ,A.BYJJKFSE              AS BYJJKFSE 
       ,A.BJJJKFSE              AS BJJJKFSE 
       ,A.BNJJKFSE              AS BNJJKFSE 
       ,A.M_OCCUR               AS M_OCCUR 
       ,A.Q_OCCUR               AS Q_OCCUR 
       ,A.M_OCCUR_AMT           AS M_OCCUR_AMT 
       ,A.Q_OCCUR_AMT           AS Q_OCCUR_AMT 
       ,A.M_COM_OCCUR           AS M_COM_OCCUR 
       ,A.Q_COM_OCCUR           AS Q_COM_OCCUR 
       ,A.M_COM_OCCUR_AMT       AS M_COM_OCCUR_AMT 
       ,A.Q_COM_OCCUR_AMT       AS Q_COM_OCCUR_AMT 
       ,A.M_COM_INCOME          AS M_COM_INCOME 
       ,A.Q_COM_INCOME          AS Q_COM_INCOME 
       ,A.M_COM_OUTCOME         AS M_COM_OUTCOME 
       ,A.Q_COM_OUTCOME         AS Q_COM_OUTCOME 
       ,A.M_COM_INCOME_AMT      AS M_COM_INCOME_AMT 
       ,A.Q_COM_INCOME_AMT      AS Q_COM_INCOME_AMT 
       ,A.M_COM_OUTCOME_AMT     AS M_COM_OUTCOME_AMT 
       ,A.Q_COM_OUTCOME_AMT     AS Q_COM_OUTCOME_AMT 
       ,A.FULLNAME              AS FULLNAME 
       ,A.CERTID                AS CERTID 
       ,A.NATIVEPLACE           AS NATIVEPLACE 
       ,A.MARRIAGE              AS MARRIAGE 
       ,A.EDUEXPERIENCE         AS EDUEXPERIENCE 
       ,A.FAMILYADD             AS FAMILYADD 
       ,A.FAMILYZIP             AS FAMILYZIP 
       ,A.FAMILYTEL             AS FAMILYTEL 
       ,A.MOBILETELEPHONE       AS MOBILETELEPHONE 
       ,A.ISHZ                  AS ISHZ 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_A_ELASTIC_SEARCH A                                --高级查询汇总表
  INNER JOIN ACRM_A_BUSINESS_INFO B                            --业务信息表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_ELASTIC_SEARCH_INNTMP1 = sqlContext.sql(sql)
ACRM_A_ELASTIC_SEARCH_INNTMP1.registerTempTable("ACRM_A_ELASTIC_SEARCH_INNTMP1")

#ACRM_A_ELASTIC_SEARCH = sqlContext.read.parquet(hdfs+'/ACRM_A_ELASTIC_SEARCH/*')
#ACRM_A_ELASTIC_SEARCH.registerTempTable("ACRM_A_ELASTIC_SEARCH")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CUST_ZH_NAME                                       --中文名称:src.CUST_ZH_NAME
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_BIR                                           --注册/出生日期:src.CUST_BIR
       ,DST.CUST_MRG                                           --婚姻状况:src.CUST_MRG
       ,DST.CUST_SEX                                           --性别:src.CUST_SEX
       ,DST.CUST_NATION                                        --民族:src.CUST_NATION
       ,DST.CUST_REGISTER                                      --户籍:src.CUST_REGISTER
       ,DST.CUST_REGADDR                                       --户籍地址:src.CUST_REGADDR
       ,DST.CUST_CITISHIP                                      --国籍:src.CUST_CITISHIP
       ,DST.IS_STAFF                                           --员工标志:src.IS_STAFF
       ,DST.CUS_TYPE                                           --客户类型:src.CUS_TYPE
       ,DST.CUST_EVADATE                                       --本行评估日期:src.CUST_EVADATE
       ,DST.IS_GUDONG                                          --是否本社股东:src.IS_GUDONG
       ,DST.IS_SY                                              --是否社员:src.IS_SY
       ,DST.IS_BUSSMA                                          --是否个体工商户:src.IS_BUSSMA
       ,DST.CUST_FAMSTATUS                                     --家庭状况:src.CUST_FAMSTATUS
       ,DST.CUST_HEALTH                                        --健康状况:src.CUST_HEALTH
       ,DST.CUST_POLIFACE                                      --政治面貌:src.CUST_POLIFACE
       ,DST.ID_BLACKLIST                                       --上本行黑名单标志:src.ID_BLACKLIST
       ,DST.CUST_EDU_LVL_COD                                   --最高学历代码:src.CUST_EDU_LVL_COD
       ,DST.GIHHEST_DEGREE                                     --最高学位:src.GIHHEST_DEGREE
       ,DST.OCCP_STATE                                         --职业状态:src.OCCP_STATE
       ,DST.CUST_POSN                                          --职务:src.CUST_POSN
       ,DST.CUST_WORK_YEAR                                     --参加工作年份:src.CUST_WORK_YEAR
       ,DST.IS_FARMER_FLG                                      --农户标识:src.IS_FARMER_FLG
       ,DST.HOUHOLD_CLASS                                      --农户分类:src.HOUHOLD_CLASS
       ,DST.IS_MEDICARE                                        --是否参加农村新型合作医疗保险:src.IS_MEDICARE
       ,DST.IS_POORISNO                                        --是否扶贫户:src.IS_POORISNO
       ,DST.IS_LIFSUR                                          --是否参加人寿保险:src.IS_LIFSUR
       ,DST.IS_ILLSUR                                          --是否参加大病保险:src.IS_ILLSUR
       ,DST.IS_ENDOSUR                                         --是否参加养老保险:src.IS_ENDOSUR
       ,DST.HAVE_CAR                                           --是否拥有车辆:src.HAVE_CAR
       ,DST.AVG_ASS                                            --日均资产:src.AVG_ASS
       ,DST.CUST_CHILDREN                                      --是否有子女:src.CUST_CHILDREN
       ,DST.CUST_HOUSE                                         --是否有房产:src.CUST_HOUSE
       ,DST.CUST_PRIVATECAR                                    --是否有私家车:src.CUST_PRIVATECAR
       ,DST.CRECARD_POINTS                                     --信用卡消费积分:src.CRECARD_POINTS
       ,DST.CUST_MIDBUSJF                                      --中间业务积分:src.CUST_MIDBUSJF
       ,DST.CUST_CHANNELJF                                     --渠道积分:src.CUST_CHANNELJF
       ,DST.CUST_CONSUMEJF                                     --消费积分:src.CUST_CONSUMEJF
       ,DST.CUST_FZJF                                          --负债积分:src.CUST_FZJF
       ,DST.CUST_ZCJF                                          --资产积分:src.CUST_ZCJF
       ,DST.CUST_SUMJF                                         --总积分:src.CUST_SUMJF
       ,DST.CUST_COMMADD                                       --通讯地址:src.CUST_COMMADD
       ,DST.CUST_MBTELNO                                       --联系方式(手机):src.CUST_MBTELNO
       ,DST.CUST_OCCUP_COD                                     --职业:src.CUST_OCCUP_COD
       ,DST.CUST_CER_NO                                        --证件号码:src.CUST_CER_NO
       ,DST.CUST_PECON_RESUR                                   --主要经济来源:src.CUST_PECON_RESUR
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
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
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.PRODUCT_ID                                         --持有此类产品:src.PRODUCT_ID
       ,DST.CREDIT_LINE                                        --贷记卡额度:src.CREDIT_LINE
       ,DST.OVER_NUM                                           --贷记卡逾期次数:src.OVER_NUM
       ,DST.OVER_AMT                                           --贷记卡逾期金额:src.OVER_AMT
       ,DST.SXJE                                               --授信金额:src.SXJE
       ,DST.YXJE                                               --用信金额:src.YXJE
       ,DST.DBJE                                               --担保金额:src.DBJE
       ,DST.DEP_AMT                                            --存款总额:src.DEP_AMT
       ,DST.LOWCOST_DEP_AMT                                    --低成本存款金额:src.LOWCOST_DEP_AMT
       ,DST.CARD_AMT                                           --借记卡余额:src.CARD_AMT
       ,DST.DEP_YEAR_AVG                                       --存款本年日平:src.DEP_YEAR_AVG
       ,DST.DEP_MONTH_AVG                                      --存款本月日平:src.DEP_MONTH_AVG
       ,DST.LOWCOST_DEP_YEAR_AVG                               --低成本存款本年日平:src.LOWCOST_DEP_YEAR_AVG
       ,DST.LOWCOST_DEP_MONTH_AVG                              --低成本存款本月日平:src.LOWCOST_DEP_MONTH_AVG
       ,DST.CARD_YEAR_AVG                                      --借记卡本年日平:src.CARD_YEAR_AVG
       ,DST.CARD_MONTH_AVG                                     --借记卡本月日平:src.CARD_MONTH_AVG
       ,DST.LOAN_YEAR_AVG                                      --贷款本年日平:src.LOAN_YEAR_AVG
       ,DST.LOAN_MONTH_AVG                                     --贷款本月日平:src.LOAN_MONTH_AVG
       ,DST.BL_LOAN_AMT                                        --不良贷款余额:src.BL_LOAN_AMT
       ,DST.QX_LOAN_AMT                                        --欠息金额:src.QX_LOAN_AMT
       ,DST.HX_LOAN_AMT                                        --已核销贷款余额:src.HX_LOAN_AMT
       ,DST.LOAN_BAL                                           --贷款余额:src.LOAN_BAL
       ,DST.CREDIT_BAL                                         --信用卡可用额度:src.CREDIT_BAL
       ,DST.IF_NEW_CARD                                        --是否本月开卡客户:src.IF_NEW_CARD
       ,DST.MGR_ID                                             --客户经理编码:src.MGR_ID
       ,DST.MGR_NAME                                           --客户经理名称:src.MGR_NAME
       ,DST.MAIN_TYPE_MGR                                      --经理主协办类型:src.MAIN_TYPE_MGR
       ,DST.INSTITUTION_CODE                                   --归属机构代码:src.INSTITUTION_CODE
       ,DST.INSTITUTION_NAME                                   --归属机构名称:src.INSTITUTION_NAME
       ,DST.MAIN_TYPE_ORG                                      --机构主协办类型:src.MAIN_TYPE_ORG
       ,DST.CUST_CONTRIBUTION                                  --客户贡献度:src.CUST_CONTRIBUTION
       ,DST.LOYA_SCORE                                         --客户忠诚度:src.LOYA_SCORE
       ,DST.MONTH_TOTAL_INT                                    --当月新增积分:src.MONTH_TOTAL_INT
       ,DST.CUST_TOTAL_INT                                     --累计总积分:src.CUST_TOTAL_INT
       ,DST.MONTH_COST_INT                                     --当月消费积分:src.MONTH_COST_INT
       ,DST.CUST_TOTAL_COST                                    --累计消费积分:src.CUST_TOTAL_COST
       ,DST.CUST_USABLE_INT                                    --当前可用积分:src.CUST_USABLE_INT
       ,DST.OBJ_RATING                                         --客观评级:src.OBJ_RATING
       ,DST.SUB_RATING                                         --主观评级:src.SUB_RATING
       ,DST.ALERT_SCORE                                        --风险评估得分:src.ALERT_SCORE
       ,DST.LOST_SCORE                                         --流失预警得分:src.LOST_SCORE
       ,DST.LIFE_CYCLE                                         --客户生命周期:src.LIFE_CYCLE
       ,DST.IS_JJK                                             --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK                                             --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY                                              --是否有网银:src.IS_WY
       ,DST.IS_SJYH                                            --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH                                            --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB                                             --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB                                             --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB                                              --是否开通社保:src.IS_SB
       ,DST.IS_JHSB                                            --是否激活社保卡:src.IS_JHSB
       ,DST.IS_YB                                              --是否开通医保:src.IS_YB
       ,DST.IS_ELEC                                            --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER                                           --是否水费签约:src.IS_WATER
       ,DST.IS_GAS                                             --是否燃气签约:src.IS_GAS
       ,DST.IS_TV                                              --是否广电签约:src.IS_TV
       ,DST.IS_WIRE                                            --是否电信签约:src.IS_WIRE
       ,DST.BNJJKBS                                            --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS                                            --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS                                            --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS                                            --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS                                            --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS                                            --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS                                             --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS                                             --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS                                             --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS                                           --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS                                           --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS                                           --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.BYJJKFSE                                           --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE                                           --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE                                           --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR                                            --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR                                            --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                                        --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                                        --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                                        --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                                        --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                                    --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                                    --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                                       --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                                       --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                                      --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                                      --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT                                   --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT                                   --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT                                  --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT                                  --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.FULLNAME                                           --客户姓名:src.FULLNAME
       ,DST.CERTID                                             --证件号码:src.CERTID
       ,DST.NATIVEPLACE                                        --户籍地址:src.NATIVEPLACE
       ,DST.MARRIAGE                                           --婚姻状况:src.MARRIAGE
       ,DST.EDUEXPERIENCE                                      --最高学历:src.EDUEXPERIENCE
       ,DST.FAMILYADD                                          --居住地址:src.FAMILYADD
       ,DST.FAMILYZIP                                          --居住地址邮编:src.FAMILYZIP
       ,DST.FAMILYTEL                                          --住宅电话:src.FAMILYTEL
       ,DST.MOBILETELEPHONE                                    --联系号码:src.MOBILETELEPHONE
       ,DST.ISHZ                                               --是否为户主:src.ISHZ
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_A_ELASTIC_SEARCH DST 
   LEFT JOIN ACRM_A_ELASTIC_SEARCH_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_ELASTIC_SEARCH_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_ELASTIC_SEARCH/"+V_DT+".parquet"
ACRM_A_ELASTIC_SEARCH_INNTMP2=ACRM_A_ELASTIC_SEARCH_INNTMP2.unionAll(ACRM_A_ELASTIC_SEARCH_INNTMP1)
ACRM_A_ELASTIC_SEARCH_INNTMP1.cache()
ACRM_A_ELASTIC_SEARCH_INNTMP2.cache()
nrowsi = ACRM_A_ELASTIC_SEARCH_INNTMP1.count()
nrowsa = ACRM_A_ELASTIC_SEARCH_INNTMP2.count()
ACRM_A_ELASTIC_SEARCH_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_ELASTIC_SEARCH_INNTMP1.unpersist()
ACRM_A_ELASTIC_SEARCH_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_ELASTIC_SEARCH lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_ELASTIC_SEARCH/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_ELASTIC_SEARCH_BK/")

#任务[12] 001-04::ACRM_A_PROD_CHANNEL_INFO更新
V_STEP = V_STEP + 1

ACRM_A_ELASTIC_SEARCH = sqlContext.read.parquet(hdfs+'/ACRM_A_ELASTIC_SEARCH/*')
ACRM_A_ELASTIC_SEARCH.registerTempTable("ACRM_A_ELASTIC_SEARCH")
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_NUM              AS CERT_NUM 
       ,A.CUST_BIR              AS CUST_BIR 
       ,A.CUST_MRG              AS CUST_MRG 
       ,A.CUST_SEX              AS CUST_SEX 
       ,A.CUST_NATION           AS CUST_NATION 
       ,A.CUST_REGISTER         AS CUST_REGISTER 
       ,A.CUST_REGADDR          AS CUST_REGADDR 
       ,A.CUST_CITISHIP         AS CUST_CITISHIP 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUS_TYPE              AS CUS_TYPE 
       ,A.CUST_EVADATE          AS CUST_EVADATE 
       ,A.IS_GUDONG             AS IS_GUDONG 
       ,A.IS_SY                 AS IS_SY 
       ,A.IS_BUSSMA             AS IS_BUSSMA 
       ,A.CUST_FAMSTATUS        AS CUST_FAMSTATUS 
       ,A.CUST_HEALTH           AS CUST_HEALTH 
       ,A.CUST_POLIFACE         AS CUST_POLIFACE 
       ,A.ID_BLACKLIST          AS ID_BLACKLIST 
       ,A.CUST_EDU_LVL_COD      AS CUST_EDU_LVL_COD 
       ,A.GIHHEST_DEGREE        AS GIHHEST_DEGREE 
       ,A.OCCP_STATE            AS OCCP_STATE 
       ,A.CUST_POSN             AS CUST_POSN 
       ,A.CUST_WORK_YEAR        AS CUST_WORK_YEAR 
       ,A.IS_FARMER_FLG         AS IS_FARMER_FLG 
       ,A.HOUHOLD_CLASS         AS HOUHOLD_CLASS 
       ,A.IS_MEDICARE           AS IS_MEDICARE 
       ,A.IS_POORISNO           AS IS_POORISNO 
       ,A.IS_LIFSUR             AS IS_LIFSUR 
       ,A.IS_ILLSUR             AS IS_ILLSUR 
       ,A.IS_ENDOSUR            AS IS_ENDOSUR 
       ,A.HAVE_CAR              AS HAVE_CAR 
       ,A.AVG_ASS               AS AVG_ASS 
       ,A.CUST_CHILDREN         AS CUST_CHILDREN 
       ,A.CUST_HOUSE            AS CUST_HOUSE 
       ,A.CUST_PRIVATECAR       AS CUST_PRIVATECAR 
       ,A.CRECARD_POINTS        AS CRECARD_POINTS 
       ,A.CUST_MIDBUSJF         AS CUST_MIDBUSJF 
       ,A.CUST_CHANNELJF        AS CUST_CHANNELJF 
       ,A.CUST_CONSUMEJF        AS CUST_CONSUMEJF 
       ,A.CUST_FZJF             AS CUST_FZJF 
       ,A.CUST_ZCJF             AS CUST_ZCJF 
       ,A.CUST_SUMJF            AS CUST_SUMJF 
       ,A.CUST_COMMADD          AS CUST_COMMADD 
       ,A.CUST_MBTELNO          AS CUST_MBTELNO 
       ,A.CUST_OCCUP_COD        AS CUST_OCCUP_COD 
       ,A.CUST_CER_NO           AS CUST_CER_NO 
       ,A.CUST_PECON_RESUR      AS CUST_PECON_RESUR 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
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
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.CERT_NO               AS CERT_NO 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,A.CREDIT_LINE           AS CREDIT_LINE 
       ,A.OVER_NUM              AS OVER_NUM 
       ,A.OVER_AMT              AS OVER_AMT 
       ,A.SXJE                  AS SXJE 
       ,A.YXJE                  AS YXJE 
       ,A.DBJE                  AS DBJE 
       ,A.DEP_AMT               AS DEP_AMT 
       ,A.LOWCOST_DEP_AMT       AS LOWCOST_DEP_AMT 
       ,A.CARD_AMT              AS CARD_AMT 
       ,A.DEP_YEAR_AVG          AS DEP_YEAR_AVG 
       ,A.DEP_MONTH_AVG         AS DEP_MONTH_AVG 
       ,A.LOWCOST_DEP_YEAR_AVG  AS LOWCOST_DEP_YEAR_AVG 
       ,A.LOWCOST_DEP_MONTH_AVG AS LOWCOST_DEP_MONTH_AVG 
       ,A.CARD_YEAR_AVG         AS CARD_YEAR_AVG 
       ,A.CARD_MONTH_AVG        AS CARD_MONTH_AVG 
       ,A.LOAN_YEAR_AVG         AS LOAN_YEAR_AVG 
       ,A.LOAN_MONTH_AVG        AS LOAN_MONTH_AVG 
       ,A.BL_LOAN_AMT           AS BL_LOAN_AMT 
       ,A.QX_LOAN_AMT           AS QX_LOAN_AMT 
       ,A.HX_LOAN_AMT           AS HX_LOAN_AMT 
       ,A.LOAN_BAL              AS LOAN_BAL 
       ,A.CREDIT_BAL            AS CREDIT_BAL 
       ,A.IF_NEW_CARD           AS IF_NEW_CARD 
       ,A.MGR_ID                AS MGR_ID 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.MAIN_TYPE_MGR         AS MAIN_TYPE_MGR 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MAIN_TYPE_ORG         AS MAIN_TYPE_ORG 
       ,A.CUST_CONTRIBUTION     AS CUST_CONTRIBUTION 
       ,A.LOYA_SCORE            AS LOYA_SCORE 
       ,A.MONTH_TOTAL_INT       AS MONTH_TOTAL_INT 
       ,A.CUST_TOTAL_INT        AS CUST_TOTAL_INT 
       ,A.MONTH_COST_INT        AS MONTH_COST_INT 
       ,A.CUST_TOTAL_COST       AS CUST_TOTAL_COST 
       ,A.CUST_USABLE_INT       AS CUST_USABLE_INT 
       ,A.OBJ_RATING            AS OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,A.ALERT_SCORE           AS ALERT_SCORE 
       ,A.LOST_SCORE            AS LOST_SCORE 
       ,A.LIFE_CYCLE            AS LIFE_CYCLE 
       ,COALESCE(B.IS_JJK, A.IS_JJK)                       AS IS_JJK 
       ,COALESCE(B.IS_DJK, A.IS_DJK)                       AS IS_DJK 
       ,COALESCE(B.IS_WY, A.IS_WY)                       AS IS_WY 
       ,COALESCE(B.IS_SJYH, A.IS_SJYH)                       AS IS_SJYH 
       ,COALESCE(B.IS_DXYH, A.IS_DXYH)                       AS IS_DXYH 
       ,COALESCE(B.IS_ZFB, A.IS_ZFB)                       AS IS_ZFB 
       ,COALESCE(B.IS_XNB, A.IS_XNB)                       AS IS_XNB 
       ,COALESCE(B.IS_SB, A.IS_SB)                       AS IS_SB 
       ,COALESCE(B.IS_JHSB, A.IS_JHSB)                       AS IS_JHSB 
       ,COALESCE(B.IS_YB, A.IS_YB)                       AS IS_YB 
       ,COALESCE(B.IS_ELEC, A.IS_ELEC)                       AS IS_ELEC 
       ,COALESCE(B.IS_WATER, A.IS_WATER)                       AS IS_WATER 
       ,COALESCE(B.IS_GAS, A.IS_GAS)                       AS IS_GAS 
       ,COALESCE(B.IS_TV, A.IS_TV)                       AS IS_TV 
       ,COALESCE(B.IS_WIRE, A.IS_WIRE)                       AS IS_WIRE 
       ,COALESCE(B.BNJJKBS, A.BNJJKBS)                       AS BNJJKBS 
       ,COALESCE(B.BJJJKBS, A.BJJJKBS)                       AS BJJJKBS 
       ,COALESCE(B.BYJJKBS, A.BYJJKBS)                       AS BYJJKBS 
       ,COALESCE(B.BNDJKBS, A.BNDJKBS)                       AS BNDJKBS 
       ,COALESCE(B.BJDJKBS, A.BJDJKBS)                       AS BJDJKBS 
       ,COALESCE(B.BYDJKBS, A.BYDJKBS)                       AS BYDJKBS 
       ,COALESCE(B.BNWYBS, A.BNWYBS)                       AS BNWYBS 
       ,COALESCE(B.BJWYBS, A.BJWYBS)                       AS BJWYBS 
       ,COALESCE(B.BYWYBS, A.BYWYBS)                       AS BYWYBS 
       ,COALESCE(B.BNSJYHBS, A.BNSJYHBS)                       AS BNSJYHBS 
       ,COALESCE(B.BJSJYHBS, A.BJSJYHBS)                       AS BJSJYHBS 
       ,COALESCE(B.BYSJYHBS, A.BYSJYHBS)                       AS BYSJYHBS 
       ,COALESCE(B.BYJJKFSE, A.BYJJKFSE)                       AS BYJJKFSE 
       ,COALESCE(B.BJJJKFSE, A.BJJJKFSE)                       AS BJJJKFSE 
       ,COALESCE(B.BNJJKFSE, A.BNJJKFSE)                       AS BNJJKFSE 
       ,COALESCE(B.M_OCCUR, A.M_OCCUR)                       AS M_OCCUR 
       ,COALESCE(B.Q_OCCUR, A.Q_OCCUR)                       AS Q_OCCUR 
       ,COALESCE(B.M_OCCUR_AMT, A.M_OCCUR_AMT)                       AS M_OCCUR_AMT 
       ,COALESCE(B.Q_OCCUR_AMT, A.Q_OCCUR_AMT)                       AS Q_OCCUR_AMT 
       ,COALESCE(B.M_COM_OCCUR, A.M_COM_OCCUR)                       AS M_COM_OCCUR 
       ,COALESCE(B.Q_COM_OCCUR, A.Q_COM_OCCUR)                       AS Q_COM_OCCUR 
       ,COALESCE(B.M_COM_OCCUR_AMT, A.M_COM_OCCUR_AMT)                       AS M_COM_OCCUR_AMT 
       ,COALESCE(B.Q_COM_OCCUR_AMT, A.Q_COM_OCCUR_AMT)                       AS Q_COM_OCCUR_AMT 
       ,COALESCE(B.M_COM_INCOME, A.M_COM_INCOME)                       AS M_COM_INCOME 
       ,COALESCE(B.Q_COM_INCOME, A.Q_COM_INCOME)                       AS Q_COM_INCOME 
       ,COALESCE(B.M_COM_OUTCOME, A.M_COM_OUTCOME)                       AS M_COM_OUTCOME 
       ,COALESCE(B.Q_COM_OUTCOME, A.Q_COM_OUTCOME)                       AS Q_COM_OUTCOME 
       ,COALESCE(B.M_COM_INCOME_AMT, A.M_COM_INCOME_AMT)                       AS M_COM_INCOME_AMT 
       ,COALESCE(B.Q_COM_INCOME_AMT, A.Q_COM_INCOME_AMT)                       AS Q_COM_INCOME_AMT 
       ,COALESCE(B.M_COM_OUTCOME_AMT, A.M_COM_OUTCOME_AMT)                       AS M_COM_OUTCOME_AMT 
       ,COALESCE(B.Q_COM_OUTCOME_AMT, A.Q_COM_OUTCOME_AMT)                       AS Q_COM_OUTCOME_AMT 
       ,A.FULLNAME              AS FULLNAME 
       ,A.CERTID                AS CERTID 
       ,A.NATIVEPLACE           AS NATIVEPLACE 
       ,A.MARRIAGE              AS MARRIAGE 
       ,A.EDUEXPERIENCE         AS EDUEXPERIENCE 
       ,A.FAMILYADD             AS FAMILYADD 
       ,A.FAMILYZIP             AS FAMILYZIP 
       ,A.FAMILYTEL             AS FAMILYTEL 
       ,A.MOBILETELEPHONE       AS MOBILETELEPHONE 
       ,A.ISHZ                  AS ISHZ 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_A_ELASTIC_SEARCH A                                --高级查询汇总表
  INNER JOIN ACRM_A_PROD_CHANNEL_INFO B                        --客户渠道信息表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_ELASTIC_SEARCH_INNTMP1 = sqlContext.sql(sql)
ACRM_A_ELASTIC_SEARCH_INNTMP1.registerTempTable("ACRM_A_ELASTIC_SEARCH_INNTMP1")

#ACRM_A_ELASTIC_SEARCH = sqlContext.read.parquet(hdfs+'/ACRM_A_ELASTIC_SEARCH/*')
#ACRM_A_ELASTIC_SEARCH.registerTempTable("ACRM_A_ELASTIC_SEARCH")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CUST_ZH_NAME                                       --中文名称:src.CUST_ZH_NAME
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_BIR                                           --注册/出生日期:src.CUST_BIR
       ,DST.CUST_MRG                                           --婚姻状况:src.CUST_MRG
       ,DST.CUST_SEX                                           --性别:src.CUST_SEX
       ,DST.CUST_NATION                                        --民族:src.CUST_NATION
       ,DST.CUST_REGISTER                                      --户籍:src.CUST_REGISTER
       ,DST.CUST_REGADDR                                       --户籍地址:src.CUST_REGADDR
       ,DST.CUST_CITISHIP                                      --国籍:src.CUST_CITISHIP
       ,DST.IS_STAFF                                           --员工标志:src.IS_STAFF
       ,DST.CUS_TYPE                                           --客户类型:src.CUS_TYPE
       ,DST.CUST_EVADATE                                       --本行评估日期:src.CUST_EVADATE
       ,DST.IS_GUDONG                                          --是否本社股东:src.IS_GUDONG
       ,DST.IS_SY                                              --是否社员:src.IS_SY
       ,DST.IS_BUSSMA                                          --是否个体工商户:src.IS_BUSSMA
       ,DST.CUST_FAMSTATUS                                     --家庭状况:src.CUST_FAMSTATUS
       ,DST.CUST_HEALTH                                        --健康状况:src.CUST_HEALTH
       ,DST.CUST_POLIFACE                                      --政治面貌:src.CUST_POLIFACE
       ,DST.ID_BLACKLIST                                       --上本行黑名单标志:src.ID_BLACKLIST
       ,DST.CUST_EDU_LVL_COD                                   --最高学历代码:src.CUST_EDU_LVL_COD
       ,DST.GIHHEST_DEGREE                                     --最高学位:src.GIHHEST_DEGREE
       ,DST.OCCP_STATE                                         --职业状态:src.OCCP_STATE
       ,DST.CUST_POSN                                          --职务:src.CUST_POSN
       ,DST.CUST_WORK_YEAR                                     --参加工作年份:src.CUST_WORK_YEAR
       ,DST.IS_FARMER_FLG                                      --农户标识:src.IS_FARMER_FLG
       ,DST.HOUHOLD_CLASS                                      --农户分类:src.HOUHOLD_CLASS
       ,DST.IS_MEDICARE                                        --是否参加农村新型合作医疗保险:src.IS_MEDICARE
       ,DST.IS_POORISNO                                        --是否扶贫户:src.IS_POORISNO
       ,DST.IS_LIFSUR                                          --是否参加人寿保险:src.IS_LIFSUR
       ,DST.IS_ILLSUR                                          --是否参加大病保险:src.IS_ILLSUR
       ,DST.IS_ENDOSUR                                         --是否参加养老保险:src.IS_ENDOSUR
       ,DST.HAVE_CAR                                           --是否拥有车辆:src.HAVE_CAR
       ,DST.AVG_ASS                                            --日均资产:src.AVG_ASS
       ,DST.CUST_CHILDREN                                      --是否有子女:src.CUST_CHILDREN
       ,DST.CUST_HOUSE                                         --是否有房产:src.CUST_HOUSE
       ,DST.CUST_PRIVATECAR                                    --是否有私家车:src.CUST_PRIVATECAR
       ,DST.CRECARD_POINTS                                     --信用卡消费积分:src.CRECARD_POINTS
       ,DST.CUST_MIDBUSJF                                      --中间业务积分:src.CUST_MIDBUSJF
       ,DST.CUST_CHANNELJF                                     --渠道积分:src.CUST_CHANNELJF
       ,DST.CUST_CONSUMEJF                                     --消费积分:src.CUST_CONSUMEJF
       ,DST.CUST_FZJF                                          --负债积分:src.CUST_FZJF
       ,DST.CUST_ZCJF                                          --资产积分:src.CUST_ZCJF
       ,DST.CUST_SUMJF                                         --总积分:src.CUST_SUMJF
       ,DST.CUST_COMMADD                                       --通讯地址:src.CUST_COMMADD
       ,DST.CUST_MBTELNO                                       --联系方式(手机):src.CUST_MBTELNO
       ,DST.CUST_OCCUP_COD                                     --职业:src.CUST_OCCUP_COD
       ,DST.CUST_CER_NO                                        --证件号码:src.CUST_CER_NO
       ,DST.CUST_PECON_RESUR                                   --主要经济来源:src.CUST_PECON_RESUR
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
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
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.PRODUCT_ID                                         --持有此类产品:src.PRODUCT_ID
       ,DST.CREDIT_LINE                                        --贷记卡额度:src.CREDIT_LINE
       ,DST.OVER_NUM                                           --贷记卡逾期次数:src.OVER_NUM
       ,DST.OVER_AMT                                           --贷记卡逾期金额:src.OVER_AMT
       ,DST.SXJE                                               --授信金额:src.SXJE
       ,DST.YXJE                                               --用信金额:src.YXJE
       ,DST.DBJE                                               --担保金额:src.DBJE
       ,DST.DEP_AMT                                            --存款总额:src.DEP_AMT
       ,DST.LOWCOST_DEP_AMT                                    --低成本存款金额:src.LOWCOST_DEP_AMT
       ,DST.CARD_AMT                                           --借记卡余额:src.CARD_AMT
       ,DST.DEP_YEAR_AVG                                       --存款本年日平:src.DEP_YEAR_AVG
       ,DST.DEP_MONTH_AVG                                      --存款本月日平:src.DEP_MONTH_AVG
       ,DST.LOWCOST_DEP_YEAR_AVG                               --低成本存款本年日平:src.LOWCOST_DEP_YEAR_AVG
       ,DST.LOWCOST_DEP_MONTH_AVG                              --低成本存款本月日平:src.LOWCOST_DEP_MONTH_AVG
       ,DST.CARD_YEAR_AVG                                      --借记卡本年日平:src.CARD_YEAR_AVG
       ,DST.CARD_MONTH_AVG                                     --借记卡本月日平:src.CARD_MONTH_AVG
       ,DST.LOAN_YEAR_AVG                                      --贷款本年日平:src.LOAN_YEAR_AVG
       ,DST.LOAN_MONTH_AVG                                     --贷款本月日平:src.LOAN_MONTH_AVG
       ,DST.BL_LOAN_AMT                                        --不良贷款余额:src.BL_LOAN_AMT
       ,DST.QX_LOAN_AMT                                        --欠息金额:src.QX_LOAN_AMT
       ,DST.HX_LOAN_AMT                                        --已核销贷款余额:src.HX_LOAN_AMT
       ,DST.LOAN_BAL                                           --贷款余额:src.LOAN_BAL
       ,DST.CREDIT_BAL                                         --信用卡可用额度:src.CREDIT_BAL
       ,DST.IF_NEW_CARD                                        --是否本月开卡客户:src.IF_NEW_CARD
       ,DST.MGR_ID                                             --客户经理编码:src.MGR_ID
       ,DST.MGR_NAME                                           --客户经理名称:src.MGR_NAME
       ,DST.MAIN_TYPE_MGR                                      --经理主协办类型:src.MAIN_TYPE_MGR
       ,DST.INSTITUTION_CODE                                   --归属机构代码:src.INSTITUTION_CODE
       ,DST.INSTITUTION_NAME                                   --归属机构名称:src.INSTITUTION_NAME
       ,DST.MAIN_TYPE_ORG                                      --机构主协办类型:src.MAIN_TYPE_ORG
       ,DST.CUST_CONTRIBUTION                                  --客户贡献度:src.CUST_CONTRIBUTION
       ,DST.LOYA_SCORE                                         --客户忠诚度:src.LOYA_SCORE
       ,DST.MONTH_TOTAL_INT                                    --当月新增积分:src.MONTH_TOTAL_INT
       ,DST.CUST_TOTAL_INT                                     --累计总积分:src.CUST_TOTAL_INT
       ,DST.MONTH_COST_INT                                     --当月消费积分:src.MONTH_COST_INT
       ,DST.CUST_TOTAL_COST                                    --累计消费积分:src.CUST_TOTAL_COST
       ,DST.CUST_USABLE_INT                                    --当前可用积分:src.CUST_USABLE_INT
       ,DST.OBJ_RATING                                         --客观评级:src.OBJ_RATING
       ,DST.SUB_RATING                                         --主观评级:src.SUB_RATING
       ,DST.ALERT_SCORE                                        --风险评估得分:src.ALERT_SCORE
       ,DST.LOST_SCORE                                         --流失预警得分:src.LOST_SCORE
       ,DST.LIFE_CYCLE                                         --客户生命周期:src.LIFE_CYCLE
       ,DST.IS_JJK                                             --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK                                             --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY                                              --是否有网银:src.IS_WY
       ,DST.IS_SJYH                                            --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH                                            --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB                                             --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB                                             --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB                                              --是否开通社保:src.IS_SB
       ,DST.IS_JHSB                                            --是否激活社保卡:src.IS_JHSB
       ,DST.IS_YB                                              --是否开通医保:src.IS_YB
       ,DST.IS_ELEC                                            --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER                                           --是否水费签约:src.IS_WATER
       ,DST.IS_GAS                                             --是否燃气签约:src.IS_GAS
       ,DST.IS_TV                                              --是否广电签约:src.IS_TV
       ,DST.IS_WIRE                                            --是否电信签约:src.IS_WIRE
       ,DST.BNJJKBS                                            --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS                                            --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS                                            --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS                                            --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS                                            --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS                                            --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS                                             --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS                                             --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS                                             --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS                                           --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS                                           --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS                                           --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.BYJJKFSE                                           --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE                                           --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE                                           --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR                                            --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR                                            --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                                        --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                                        --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                                        --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                                        --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                                    --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                                    --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                                       --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                                       --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                                      --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                                      --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT                                   --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT                                   --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT                                  --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT                                  --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.FULLNAME                                           --客户姓名:src.FULLNAME
       ,DST.CERTID                                             --证件号码:src.CERTID
       ,DST.NATIVEPLACE                                        --户籍地址:src.NATIVEPLACE
       ,DST.MARRIAGE                                           --婚姻状况:src.MARRIAGE
       ,DST.EDUEXPERIENCE                                      --最高学历:src.EDUEXPERIENCE
       ,DST.FAMILYADD                                          --居住地址:src.FAMILYADD
       ,DST.FAMILYZIP                                          --居住地址邮编:src.FAMILYZIP
       ,DST.FAMILYTEL                                          --住宅电话:src.FAMILYTEL
       ,DST.MOBILETELEPHONE                                    --联系号码:src.MOBILETELEPHONE
       ,DST.ISHZ                                               --是否为户主:src.ISHZ
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_A_ELASTIC_SEARCH DST 
   LEFT JOIN ACRM_A_ELASTIC_SEARCH_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_ELASTIC_SEARCH_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_ELASTIC_SEARCH/"+V_DT+".parquet"
ACRM_A_ELASTIC_SEARCH_INNTMP2=ACRM_A_ELASTIC_SEARCH_INNTMP2.unionAll(ACRM_A_ELASTIC_SEARCH_INNTMP1)
ACRM_A_ELASTIC_SEARCH_INNTMP1.cache()
ACRM_A_ELASTIC_SEARCH_INNTMP2.cache()
nrowsi = ACRM_A_ELASTIC_SEARCH_INNTMP1.count()
nrowsa = ACRM_A_ELASTIC_SEARCH_INNTMP2.count()
ACRM_A_ELASTIC_SEARCH_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_ELASTIC_SEARCH_INNTMP1.unpersist()
ACRM_A_ELASTIC_SEARCH_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_ELASTIC_SEARCH lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_ELASTIC_SEARCH/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_ELASTIC_SEARCH_BK/")

#任务[12] 001-05::ACRM_A_ANALYSIS_INFO更新
V_STEP = V_STEP + 1

ACRM_A_ELASTIC_SEARCH = sqlContext.read.parquet(hdfs+'/ACRM_A_ELASTIC_SEARCH/*')
ACRM_A_ELASTIC_SEARCH.registerTempTable("ACRM_A_ELASTIC_SEARCH")
ACRM_A_ANALYSIS_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_ANALYSIS_INFO/*')
ACRM_A_ANALYSIS_INFO.registerTempTable("ACRM_A_ANALYSIS_INFO")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_NUM              AS CERT_NUM 
       ,A.CUST_BIR              AS CUST_BIR 
       ,A.CUST_MRG              AS CUST_MRG 
       ,A.CUST_SEX              AS CUST_SEX 
       ,A.CUST_NATION           AS CUST_NATION 
       ,A.CUST_REGISTER         AS CUST_REGISTER 
       ,A.CUST_REGADDR          AS CUST_REGADDR 
       ,A.CUST_CITISHIP         AS CUST_CITISHIP 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUS_TYPE              AS CUS_TYPE 
       ,A.CUST_EVADATE          AS CUST_EVADATE 
       ,A.IS_GUDONG             AS IS_GUDONG 
       ,A.IS_SY                 AS IS_SY 
       ,A.IS_BUSSMA             AS IS_BUSSMA 
       ,A.CUST_FAMSTATUS        AS CUST_FAMSTATUS 
       ,A.CUST_HEALTH           AS CUST_HEALTH 
       ,A.CUST_POLIFACE         AS CUST_POLIFACE 
       ,A.ID_BLACKLIST          AS ID_BLACKLIST 
       ,A.CUST_EDU_LVL_COD      AS CUST_EDU_LVL_COD 
       ,A.GIHHEST_DEGREE        AS GIHHEST_DEGREE 
       ,A.OCCP_STATE            AS OCCP_STATE 
       ,A.CUST_POSN             AS CUST_POSN 
       ,A.CUST_WORK_YEAR        AS CUST_WORK_YEAR 
       ,A.IS_FARMER_FLG         AS IS_FARMER_FLG 
       ,A.HOUHOLD_CLASS         AS HOUHOLD_CLASS 
       ,A.IS_MEDICARE           AS IS_MEDICARE 
       ,A.IS_POORISNO           AS IS_POORISNO 
       ,A.IS_LIFSUR             AS IS_LIFSUR 
       ,A.IS_ILLSUR             AS IS_ILLSUR 
       ,A.IS_ENDOSUR            AS IS_ENDOSUR 
       ,A.HAVE_CAR              AS HAVE_CAR 
       ,A.AVG_ASS               AS AVG_ASS 
       ,A.CUST_CHILDREN         AS CUST_CHILDREN 
       ,A.CUST_HOUSE            AS CUST_HOUSE 
       ,A.CUST_PRIVATECAR       AS CUST_PRIVATECAR 
       ,A.CRECARD_POINTS        AS CRECARD_POINTS 
       ,A.CUST_MIDBUSJF         AS CUST_MIDBUSJF 
       ,A.CUST_CHANNELJF        AS CUST_CHANNELJF 
       ,A.CUST_CONSUMEJF        AS CUST_CONSUMEJF 
       ,A.CUST_FZJF             AS CUST_FZJF 
       ,A.CUST_ZCJF             AS CUST_ZCJF 
       ,A.CUST_SUMJF            AS CUST_SUMJF 
       ,A.CUST_COMMADD          AS CUST_COMMADD 
       ,A.CUST_MBTELNO          AS CUST_MBTELNO 
       ,A.CUST_OCCUP_COD        AS CUST_OCCUP_COD 
       ,A.CUST_CER_NO           AS CUST_CER_NO 
       ,A.CUST_PECON_RESUR      AS CUST_PECON_RESUR 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
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
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.CERT_NO               AS CERT_NO 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,A.CREDIT_LINE           AS CREDIT_LINE 
       ,A.OVER_NUM              AS OVER_NUM 
       ,A.OVER_AMT              AS OVER_AMT 
       ,A.SXJE                  AS SXJE 
       ,A.YXJE                  AS YXJE 
       ,A.DBJE                  AS DBJE 
       ,A.DEP_AMT               AS DEP_AMT 
       ,A.LOWCOST_DEP_AMT       AS LOWCOST_DEP_AMT 
       ,A.CARD_AMT              AS CARD_AMT 
       ,A.DEP_YEAR_AVG          AS DEP_YEAR_AVG 
       ,A.DEP_MONTH_AVG         AS DEP_MONTH_AVG 
       ,A.LOWCOST_DEP_YEAR_AVG  AS LOWCOST_DEP_YEAR_AVG 
       ,A.LOWCOST_DEP_MONTH_AVG AS LOWCOST_DEP_MONTH_AVG 
       ,A.CARD_YEAR_AVG         AS CARD_YEAR_AVG 
       ,A.CARD_MONTH_AVG        AS CARD_MONTH_AVG 
       ,A.LOAN_YEAR_AVG         AS LOAN_YEAR_AVG 
       ,A.LOAN_MONTH_AVG        AS LOAN_MONTH_AVG 
       ,A.BL_LOAN_AMT           AS BL_LOAN_AMT 
       ,A.QX_LOAN_AMT           AS QX_LOAN_AMT 
       ,A.HX_LOAN_AMT           AS HX_LOAN_AMT 
       ,A.LOAN_BAL              AS LOAN_BAL 
       ,A.CREDIT_BAL            AS CREDIT_BAL 
       ,A.IF_NEW_CARD           AS IF_NEW_CARD 
       ,A.MGR_ID                AS MGR_ID 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.MAIN_TYPE_MGR         AS MAIN_TYPE_MGR 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MAIN_TYPE_ORG         AS MAIN_TYPE_ORG 
       ,COALESCE(B.CUST_CONTRIBUTION, A.CUST_CONTRIBUTION)                       AS CUST_CONTRIBUTION 
       ,COALESCE(B.LOYA_SCORE, A.LOYA_SCORE)                       AS LOYA_SCORE 
       ,COALESCE(B.MONTH_TOTAL_INT, A.MONTH_TOTAL_INT)                       AS MONTH_TOTAL_INT 
       ,COALESCE(B.CUST_TOTAL_INT, A.CUST_TOTAL_INT)                       AS CUST_TOTAL_INT 
       ,COALESCE(B.MONTH_COST_INT, A.MONTH_COST_INT)                       AS MONTH_COST_INT 
       ,COALESCE(B.CUST_TOTAL_COST, A.CUST_TOTAL_COST)                       AS CUST_TOTAL_COST 
       ,COALESCE(B.CUST_USABLE_INT, A.CUST_USABLE_INT)                       AS CUST_USABLE_INT 
       ,COALESCE(B.OBJ_RATING, A.OBJ_RATING)                       AS OBJ_RATING 
       ,COALESCE(B.SUB_RATING, A.SUB_RATING)                       AS SUB_RATING 
       ,COALESCE(B.ALERT_SCORE, A.ALERT_SCORE)                       AS ALERT_SCORE 
       ,COALESCE(B.LOST_SCORE, A.LOST_SCORE)                       AS LOST_SCORE 
       ,COALESCE(B.LIFE_CYCLE, A.LIFE_CYCLE)                       AS LIFE_CYCLE 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_JHSB               AS IS_JHSB 
       ,A.IS_YB                 AS IS_YB 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.BNJJKBS               AS BNJJKBS 
       ,A.BJJJKBS               AS BJJJKBS 
       ,A.BYJJKBS               AS BYJJKBS 
       ,A.BNDJKBS               AS BNDJKBS 
       ,A.BJDJKBS               AS BJDJKBS 
       ,A.BYDJKBS               AS BYDJKBS 
       ,A.BNWYBS                AS BNWYBS 
       ,A.BJWYBS                AS BJWYBS 
       ,A.BYWYBS                AS BYWYBS 
       ,A.BNSJYHBS              AS BNSJYHBS 
       ,A.BJSJYHBS              AS BJSJYHBS 
       ,A.BYSJYHBS              AS BYSJYHBS 
       ,A.BYJJKFSE              AS BYJJKFSE 
       ,A.BJJJKFSE              AS BJJJKFSE 
       ,A.BNJJKFSE              AS BNJJKFSE 
       ,A.M_OCCUR               AS M_OCCUR 
       ,A.Q_OCCUR               AS Q_OCCUR 
       ,A.M_OCCUR_AMT           AS M_OCCUR_AMT 
       ,A.Q_OCCUR_AMT           AS Q_OCCUR_AMT 
       ,A.M_COM_OCCUR           AS M_COM_OCCUR 
       ,A.Q_COM_OCCUR           AS Q_COM_OCCUR 
       ,A.M_COM_OCCUR_AMT       AS M_COM_OCCUR_AMT 
       ,A.Q_COM_OCCUR_AMT       AS Q_COM_OCCUR_AMT 
       ,A.M_COM_INCOME          AS M_COM_INCOME 
       ,A.Q_COM_INCOME          AS Q_COM_INCOME 
       ,A.M_COM_OUTCOME         AS M_COM_OUTCOME 
       ,A.Q_COM_OUTCOME         AS Q_COM_OUTCOME 
       ,A.M_COM_INCOME_AMT      AS M_COM_INCOME_AMT 
       ,A.Q_COM_INCOME_AMT      AS Q_COM_INCOME_AMT 
       ,A.M_COM_OUTCOME_AMT     AS M_COM_OUTCOME_AMT 
       ,A.Q_COM_OUTCOME_AMT     AS Q_COM_OUTCOME_AMT 
       ,A.FULLNAME              AS FULLNAME 
       ,A.CERTID                AS CERTID 
       ,A.NATIVEPLACE           AS NATIVEPLACE 
       ,A.MARRIAGE              AS MARRIAGE 
       ,A.EDUEXPERIENCE         AS EDUEXPERIENCE 
       ,A.FAMILYADD             AS FAMILYADD 
       ,A.FAMILYZIP             AS FAMILYZIP 
       ,A.FAMILYTEL             AS FAMILYTEL 
       ,A.MOBILETELEPHONE       AS MOBILETELEPHONE 
       ,A.ISHZ                  AS ISHZ 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_A_ELASTIC_SEARCH A                                --高级查询汇总表
  INNER JOIN ACRM_A_ANALYSIS_INFO B                            --客户分析信息表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_ELASTIC_SEARCH_INNTMP1 = sqlContext.sql(sql)
ACRM_A_ELASTIC_SEARCH_INNTMP1.registerTempTable("ACRM_A_ELASTIC_SEARCH_INNTMP1")

#ACRM_A_ELASTIC_SEARCH = sqlContext.read.parquet(hdfs+'/ACRM_A_ELASTIC_SEARCH/*')
#ACRM_A_ELASTIC_SEARCH.registerTempTable("ACRM_A_ELASTIC_SEARCH")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CUST_ZH_NAME                                       --中文名称:src.CUST_ZH_NAME
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_BIR                                           --注册/出生日期:src.CUST_BIR
       ,DST.CUST_MRG                                           --婚姻状况:src.CUST_MRG
       ,DST.CUST_SEX                                           --性别:src.CUST_SEX
       ,DST.CUST_NATION                                        --民族:src.CUST_NATION
       ,DST.CUST_REGISTER                                      --户籍:src.CUST_REGISTER
       ,DST.CUST_REGADDR                                       --户籍地址:src.CUST_REGADDR
       ,DST.CUST_CITISHIP                                      --国籍:src.CUST_CITISHIP
       ,DST.IS_STAFF                                           --员工标志:src.IS_STAFF
       ,DST.CUS_TYPE                                           --客户类型:src.CUS_TYPE
       ,DST.CUST_EVADATE                                       --本行评估日期:src.CUST_EVADATE
       ,DST.IS_GUDONG                                          --是否本社股东:src.IS_GUDONG
       ,DST.IS_SY                                              --是否社员:src.IS_SY
       ,DST.IS_BUSSMA                                          --是否个体工商户:src.IS_BUSSMA
       ,DST.CUST_FAMSTATUS                                     --家庭状况:src.CUST_FAMSTATUS
       ,DST.CUST_HEALTH                                        --健康状况:src.CUST_HEALTH
       ,DST.CUST_POLIFACE                                      --政治面貌:src.CUST_POLIFACE
       ,DST.ID_BLACKLIST                                       --上本行黑名单标志:src.ID_BLACKLIST
       ,DST.CUST_EDU_LVL_COD                                   --最高学历代码:src.CUST_EDU_LVL_COD
       ,DST.GIHHEST_DEGREE                                     --最高学位:src.GIHHEST_DEGREE
       ,DST.OCCP_STATE                                         --职业状态:src.OCCP_STATE
       ,DST.CUST_POSN                                          --职务:src.CUST_POSN
       ,DST.CUST_WORK_YEAR                                     --参加工作年份:src.CUST_WORK_YEAR
       ,DST.IS_FARMER_FLG                                      --农户标识:src.IS_FARMER_FLG
       ,DST.HOUHOLD_CLASS                                      --农户分类:src.HOUHOLD_CLASS
       ,DST.IS_MEDICARE                                        --是否参加农村新型合作医疗保险:src.IS_MEDICARE
       ,DST.IS_POORISNO                                        --是否扶贫户:src.IS_POORISNO
       ,DST.IS_LIFSUR                                          --是否参加人寿保险:src.IS_LIFSUR
       ,DST.IS_ILLSUR                                          --是否参加大病保险:src.IS_ILLSUR
       ,DST.IS_ENDOSUR                                         --是否参加养老保险:src.IS_ENDOSUR
       ,DST.HAVE_CAR                                           --是否拥有车辆:src.HAVE_CAR
       ,DST.AVG_ASS                                            --日均资产:src.AVG_ASS
       ,DST.CUST_CHILDREN                                      --是否有子女:src.CUST_CHILDREN
       ,DST.CUST_HOUSE                                         --是否有房产:src.CUST_HOUSE
       ,DST.CUST_PRIVATECAR                                    --是否有私家车:src.CUST_PRIVATECAR
       ,DST.CRECARD_POINTS                                     --信用卡消费积分:src.CRECARD_POINTS
       ,DST.CUST_MIDBUSJF                                      --中间业务积分:src.CUST_MIDBUSJF
       ,DST.CUST_CHANNELJF                                     --渠道积分:src.CUST_CHANNELJF
       ,DST.CUST_CONSUMEJF                                     --消费积分:src.CUST_CONSUMEJF
       ,DST.CUST_FZJF                                          --负债积分:src.CUST_FZJF
       ,DST.CUST_ZCJF                                          --资产积分:src.CUST_ZCJF
       ,DST.CUST_SUMJF                                         --总积分:src.CUST_SUMJF
       ,DST.CUST_COMMADD                                       --通讯地址:src.CUST_COMMADD
       ,DST.CUST_MBTELNO                                       --联系方式(手机):src.CUST_MBTELNO
       ,DST.CUST_OCCUP_COD                                     --职业:src.CUST_OCCUP_COD
       ,DST.CUST_CER_NO                                        --证件号码:src.CUST_CER_NO
       ,DST.CUST_PECON_RESUR                                   --主要经济来源:src.CUST_PECON_RESUR
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
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
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.PRODUCT_ID                                         --持有此类产品:src.PRODUCT_ID
       ,DST.CREDIT_LINE                                        --贷记卡额度:src.CREDIT_LINE
       ,DST.OVER_NUM                                           --贷记卡逾期次数:src.OVER_NUM
       ,DST.OVER_AMT                                           --贷记卡逾期金额:src.OVER_AMT
       ,DST.SXJE                                               --授信金额:src.SXJE
       ,DST.YXJE                                               --用信金额:src.YXJE
       ,DST.DBJE                                               --担保金额:src.DBJE
       ,DST.DEP_AMT                                            --存款总额:src.DEP_AMT
       ,DST.LOWCOST_DEP_AMT                                    --低成本存款金额:src.LOWCOST_DEP_AMT
       ,DST.CARD_AMT                                           --借记卡余额:src.CARD_AMT
       ,DST.DEP_YEAR_AVG                                       --存款本年日平:src.DEP_YEAR_AVG
       ,DST.DEP_MONTH_AVG                                      --存款本月日平:src.DEP_MONTH_AVG
       ,DST.LOWCOST_DEP_YEAR_AVG                               --低成本存款本年日平:src.LOWCOST_DEP_YEAR_AVG
       ,DST.LOWCOST_DEP_MONTH_AVG                              --低成本存款本月日平:src.LOWCOST_DEP_MONTH_AVG
       ,DST.CARD_YEAR_AVG                                      --借记卡本年日平:src.CARD_YEAR_AVG
       ,DST.CARD_MONTH_AVG                                     --借记卡本月日平:src.CARD_MONTH_AVG
       ,DST.LOAN_YEAR_AVG                                      --贷款本年日平:src.LOAN_YEAR_AVG
       ,DST.LOAN_MONTH_AVG                                     --贷款本月日平:src.LOAN_MONTH_AVG
       ,DST.BL_LOAN_AMT                                        --不良贷款余额:src.BL_LOAN_AMT
       ,DST.QX_LOAN_AMT                                        --欠息金额:src.QX_LOAN_AMT
       ,DST.HX_LOAN_AMT                                        --已核销贷款余额:src.HX_LOAN_AMT
       ,DST.LOAN_BAL                                           --贷款余额:src.LOAN_BAL
       ,DST.CREDIT_BAL                                         --信用卡可用额度:src.CREDIT_BAL
       ,DST.IF_NEW_CARD                                        --是否本月开卡客户:src.IF_NEW_CARD
       ,DST.MGR_ID                                             --客户经理编码:src.MGR_ID
       ,DST.MGR_NAME                                           --客户经理名称:src.MGR_NAME
       ,DST.MAIN_TYPE_MGR                                      --经理主协办类型:src.MAIN_TYPE_MGR
       ,DST.INSTITUTION_CODE                                   --归属机构代码:src.INSTITUTION_CODE
       ,DST.INSTITUTION_NAME                                   --归属机构名称:src.INSTITUTION_NAME
       ,DST.MAIN_TYPE_ORG                                      --机构主协办类型:src.MAIN_TYPE_ORG
       ,DST.CUST_CONTRIBUTION                                  --客户贡献度:src.CUST_CONTRIBUTION
       ,DST.LOYA_SCORE                                         --客户忠诚度:src.LOYA_SCORE
       ,DST.MONTH_TOTAL_INT                                    --当月新增积分:src.MONTH_TOTAL_INT
       ,DST.CUST_TOTAL_INT                                     --累计总积分:src.CUST_TOTAL_INT
       ,DST.MONTH_COST_INT                                     --当月消费积分:src.MONTH_COST_INT
       ,DST.CUST_TOTAL_COST                                    --累计消费积分:src.CUST_TOTAL_COST
       ,DST.CUST_USABLE_INT                                    --当前可用积分:src.CUST_USABLE_INT
       ,DST.OBJ_RATING                                         --客观评级:src.OBJ_RATING
       ,DST.SUB_RATING                                         --主观评级:src.SUB_RATING
       ,DST.ALERT_SCORE                                        --风险评估得分:src.ALERT_SCORE
       ,DST.LOST_SCORE                                         --流失预警得分:src.LOST_SCORE
       ,DST.LIFE_CYCLE                                         --客户生命周期:src.LIFE_CYCLE
       ,DST.IS_JJK                                             --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK                                             --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY                                              --是否有网银:src.IS_WY
       ,DST.IS_SJYH                                            --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH                                            --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB                                             --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB                                             --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB                                              --是否开通社保:src.IS_SB
       ,DST.IS_JHSB                                            --是否激活社保卡:src.IS_JHSB
       ,DST.IS_YB                                              --是否开通医保:src.IS_YB
       ,DST.IS_ELEC                                            --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER                                           --是否水费签约:src.IS_WATER
       ,DST.IS_GAS                                             --是否燃气签约:src.IS_GAS
       ,DST.IS_TV                                              --是否广电签约:src.IS_TV
       ,DST.IS_WIRE                                            --是否电信签约:src.IS_WIRE
       ,DST.BNJJKBS                                            --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS                                            --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS                                            --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS                                            --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS                                            --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS                                            --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS                                             --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS                                             --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS                                             --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS                                           --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS                                           --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS                                           --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.BYJJKFSE                                           --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE                                           --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE                                           --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR                                            --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR                                            --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                                        --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                                        --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                                        --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                                        --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                                    --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                                    --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                                       --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                                       --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                                      --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                                      --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT                                   --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT                                   --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT                                  --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT                                  --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.FULLNAME                                           --客户姓名:src.FULLNAME
       ,DST.CERTID                                             --证件号码:src.CERTID
       ,DST.NATIVEPLACE                                        --户籍地址:src.NATIVEPLACE
       ,DST.MARRIAGE                                           --婚姻状况:src.MARRIAGE
       ,DST.EDUEXPERIENCE                                      --最高学历:src.EDUEXPERIENCE
       ,DST.FAMILYADD                                          --居住地址:src.FAMILYADD
       ,DST.FAMILYZIP                                          --居住地址邮编:src.FAMILYZIP
       ,DST.FAMILYTEL                                          --住宅电话:src.FAMILYTEL
       ,DST.MOBILETELEPHONE                                    --联系号码:src.MOBILETELEPHONE
       ,DST.ISHZ                                               --是否为户主:src.ISHZ
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_A_ELASTIC_SEARCH DST 
   LEFT JOIN ACRM_A_ELASTIC_SEARCH_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_ELASTIC_SEARCH_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_ELASTIC_SEARCH/"+V_DT+".parquet"
ACRM_A_ELASTIC_SEARCH_INNTMP2=ACRM_A_ELASTIC_SEARCH_INNTMP2.unionAll(ACRM_A_ELASTIC_SEARCH_INNTMP1)
ACRM_A_ELASTIC_SEARCH_INNTMP1.cache()
ACRM_A_ELASTIC_SEARCH_INNTMP2.cache()
nrowsi = ACRM_A_ELASTIC_SEARCH_INNTMP1.count()
nrowsa = ACRM_A_ELASTIC_SEARCH_INNTMP2.count()
ACRM_A_ELASTIC_SEARCH_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_ELASTIC_SEARCH_INNTMP1.unpersist()
ACRM_A_ELASTIC_SEARCH_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_ELASTIC_SEARCH lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_ELASTIC_SEARCH/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_ELASTIC_SEARCH_BK/")
