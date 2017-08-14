#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUST_DESC').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_DESC/*.parquet")

OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
OCRM_F_CI_LINKMAN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_LINKMAN/*')
OCRM_F_CI_LINKMAN.registerTempTable("OCRM_F_CI_LINKMAN")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC_BK/'+V_DT_LD+'.parquet*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")


#任务[11] 001-00::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                     AS ID 
       ,A.CUST_ID                AS CUST_ID 
       ,A.CUST_ZH_NAME           AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME           AS CUST_EN_NAME 
       ,A.CUST_ABBR              AS CUST_ABBR 
       ,A.CUST_STAT              AS CUST_STAT 
       ,A.CUST_TYP               AS CUST_TYP 
       ,A.CERT_TYPE              AS CERT_TYPE 
       ,A.CERT_NUM               AS CERT_NUM 
       ,A.CUST_CRELVL            AS CUST_CRELVL 
       ,A.CUST_EVALUATEDATE      AS CUST_EVALUATEDATE 
       ,A.CUST_EVALUATEOVERDATE  AS CUST_EVALUATEOVERDATE 
       ,A.ODS_SYS_ID             AS ODS_SYS_ID 
       ,A.CRM_DT                 AS CRM_DT 
       ,A.IF_SHAREHOLDER         AS IF_SHAREHOLDER 
       ,A.IF_STRATEGY            AS IF_STRATEGY 
       ,A.OBJ_RATING            AS OBJ_RATING 
       ,A.OBJ_DATE              AS OBJ_DATE 
       ,A.OLD_OBJ_RATING        AS OLD_OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,A.SUB_DATE              AS SUB_DATE 
       ,A.OLD_SUB_RATING        AS OLD_SUB_RATING 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUST_LEV              AS CUST_LEV 
       ,A.HB_FLAG               AS HB_FLAG 
       ,A.BILL_REPLACE_FLAG     AS BILL_REPLACE_FLAG 
       ,A.OBLIGATION_FLAG       AS OBLIGATION_FLAG 
       ,A.WAGE_ARREAR_FALG      AS WAGE_ARREAR_FALG 
       ,A.TAXES_FLAG            AS TAXES_FLAG 
       ,A.USURY_FLAG            AS USURY_FLAG 
       ,A.OPERATION_SAFE_FALG   AS OPERATION_SAFE_FALG 
       ,A.OBJ_NAME              AS OBJ_NAME 
       ,A.OLD_OBJ_NAME          AS OLD_OBJ_NAME 
       ,A.GRADE_HAND_NAME       AS GRADE_HAND_NAME 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.IS_CRE                AS IS_CRE 
       ,A.FR_ID                 AS FR_ID 
       ,A.UN_NULL_RATE          AS UN_NULL_RATE 
       ,A.HOLD_PRO_FLAG         AS HOLD_PRO_FLAG 
       ,A.GPS                   AS GPS 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.LINK_MAN              AS LINK_MAN 
       ,A.CUST_ADDR             AS CUST_ADDR 
       ,A.VALID_FLG             AS VALID_FLG 
   FROM OCRM_F_CI_CUST_DESC A                              --对公客户信息表
   LEFT JOIN OCRM_F_CI_PER_CUST_INFO B ON A.CUST_ID = B.CUST_ID AND A.FR_ID = B.FR_ID AND B.ODS_ST_DATE = V_DT 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO C ON A.CUST_ID = C.CUST_ID AND A.FR_ID = C.FR_ID AND C.ODS_ST_DATE = V_DT 
  WHERE B.CUST_ID IS NULL AND C.CUST_ID IS NULL
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
dfn="OCRM_F_CI_CUST_DESC/"+V_DT+".parquet"
OCRM_F_CI_CUST_DESC.cache()
nrows = OCRM_F_CI_CUST_DESC.count()
OCRM_F_CI_CUST_DESC.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_DESC.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id() AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,B.CUST_ABBR             AS CUST_ABBR 
       ,'1'                     AS CUST_STAT 
       ,'2'                     AS CUST_TYP 
       ,A.CERT_TYP              AS CERT_TYPE 
       ,A.CERT_NO               AS CERT_NUM 
       ,B.CUST_CRELVL           AS CUST_CRELVL 
       ,B.CUST_EVALUATEDATE     AS CUST_EVALUATEDATE 
       ,B.CUST_EVALUATEOVERDATE AS CUST_EVALUATEOVERDATE 
       ,B.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.ODS_ST_DATE           AS CRM_DT 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,B.IF_STRATEGY           AS IF_STRATEGY 
       ,COALESCE(B.OBJ_RATING, '17')                       AS OBJ_RATING 
       ,B.OBJ_DATE              AS OBJ_DATE 
       ,B.OLD_OBJ_RATING        AS OLD_OBJ_RATING 
       ,B.SUB_RATING            AS SUB_RATING 
       ,B.SUB_DATE              AS SUB_DATE 
       ,B.OLD_SUB_RATING        AS OLD_SUB_RATING 
       ,'N'                     AS IS_STAFF 
       ,B.CUST_LEV              AS CUST_LEV 
       ,B.HB_FLAG               AS HB_FLAG 
       ,B.BILL_REPLACE_FLAG     AS BILL_REPLACE_FLAG 
       ,B.OBLIGATION_FLAG       AS OBLIGATION_FLAG 
       ,B.WAGE_ARREAR_FALG      AS WAGE_ARREAR_FALG 
       ,B.TAXES_FLAG            AS TAXES_FLAG 
       ,B.USURY_FLAG            AS USURY_FLAG 
       ,B.OPERATION_SAFE_FALG   AS OPERATION_SAFE_FALG 
       ,B.OBJ_NAME              AS OBJ_NAME 
       ,B.OLD_OBJ_NAME          AS OLD_OBJ_NAME 
       ,B.GRADE_HAND_NAME       AS GRADE_HAND_NAME 
       ,B.IS_MODIFY             AS IS_MODIFY 
       ,'N'                     AS IS_CRE 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(ROUND(FLOAT(CASE WHEN TRIM(A.CUST_NAME            ) = '' OR A.CUST_NAME            IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.INDUS_CALSS_MAIN     ) = '' OR A.INDUS_CALSS_MAIN     IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.ORG_TYP              ) = '' OR A.ORG_TYP              IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.COM_TYP              ) = '' OR A.COM_TYP              IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.INDUS_CLAS_DEPUTY    ) = '' OR A.INDUS_CLAS_DEPUTY    IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.LINCENSE_NO          ) = '' OR A.LINCENSE_NO          IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.IF_AGRICULTRUE       ) = '' OR A.IF_AGRICULTRUE       IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.ENT_QUA_LEVEL        ) = '' OR A.ENT_QUA_LEVEL        IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.LISTING_CORP_TYP     ) = '' OR A.LISTING_CORP_TYP     IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.IF_HIGH_TECH         ) = '' OR A.IF_HIGH_TECH         IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.OPEN_ORG1            ) = '' OR A.OPEN_ORG1            IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.FIRST_LOAN_DATE      ) = '' OR A.FIRST_LOAN_DATE      IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.LEGAL_NAME           ) = '' OR A.LEGAL_NAME           IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.LINK_TEL_FIN         ) = '' OR A.LINK_TEL_FIN         IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.LEGAL_CERT_NO        ) = '' OR A.LEGAL_CERT_NO        IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.WORK_ADDRESS         ) = '' OR A.WORK_ADDRESS         IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(E.CUST_ID              ) = '' OR E.CUST_ID              IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.BUSINESS             ) = '' OR A.BUSINESS             IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.TOTAL_ASSET          ) = '' OR A.TOTAL_ASSET          IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.SALE_ASSET           ) = '' OR A.SALE_ASSET           IS NULL THEN 0 ELSE 1 END )/ 20 * 100,2) 
         AS DECIMAL(10,2))  AS UN_NULL_RATE 
       ,B.HOLD_PRO_FLAG         AS HOLD_PRO_FLAG 
       ,B.GPS                   AS GPS 
       ,B.LONGITUDE             AS LONGITUDE 
       ,B.LATITUDE              AS LATITUDE 
       ,A.LINK_TEL_FIN          AS LINK_TEL 
       ,A.LEGAL_NAME            AS LINK_MAN 
       ,B.CUST_ADDR             AS CUST_ADDR 
       ,B.VALID_FLG             AS VALID_FLG 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户信息表
   LEFT JOIN(
         SELECT DISTINCT CUST_ID 
               ,FR_ID 
           FROM OCRM_F_CI_LINKMAN) E                           --联系人表
     ON A.CUST_ID               = E.CUST_ID 
    AND A.FR_ID                 = E.FR_ID 
   LEFT JOIN OCRM_F_CI_CUST_DESC B                             --统一客户信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
dfn="OCRM_F_CI_CUST_DESC/"+V_DT+".parquet"
OCRM_F_CI_CUST_DESC.cache()
nrows = OCRM_F_CI_CUST_DESC.count()
OCRM_F_CI_CUST_DESC.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_CUST_DESC.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")

sql = """
 SELECT monotonically_increasing_id() AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_ZH_NAME 
       ,A.CUST_ENAME            AS CUST_EN_NAME 
       ,B.CUST_ABBR             AS CUST_ABBR 
       ,'1'                     AS CUST_STAT 
       ,'1'                     AS CUST_TYP 
       ,A.CUST_CRE_TYP          AS CERT_TYPE 
       ,A.CUST_CER_NO           AS CERT_NUM 
       ,B.CUST_CRELVL           AS CUST_CRELVL 
       ,B.CUST_EVALUATEDATE     AS CUST_EVALUATEDATE 
       ,B.CUST_EVALUATEOVERDATE AS CUST_EVALUATEOVERDATE 
       ,B.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.ODS_ST_DATE           AS CRM_DT 
       ,A.IS_GUDONG             AS IF_SHAREHOLDER 
       ,B.IF_STRATEGY           AS IF_STRATEGY 
       ,COALESCE(B.OBJ_RATING, '17')                       AS OBJ_RATING 
       ,B.OBJ_DATE              AS OBJ_DATE 
       ,B.OLD_OBJ_RATING        AS OLD_OBJ_RATING 
       ,B.SUB_RATING            AS SUB_RATING 
       ,B.SUB_DATE              AS SUB_DATE 
       ,B.OLD_SUB_RATING        AS OLD_SUB_RATING 
       ,A.IS_STAFF              AS IS_STAFF 
       ,B.CUST_LEV              AS CUST_LEV 
       ,B.HB_FLAG               AS HB_FLAG 
       ,B.BILL_REPLACE_FLAG     AS BILL_REPLACE_FLAG 
       ,B.OBLIGATION_FLAG       AS OBLIGATION_FLAG 
       ,B.WAGE_ARREAR_FALG      AS WAGE_ARREAR_FALG 
       ,B.TAXES_FLAG            AS TAXES_FLAG 
       ,B.USURY_FLAG            AS USURY_FLAG 
       ,B.OPERATION_SAFE_FALG   AS OPERATION_SAFE_FALG 
       ,B.OBJ_NAME              AS OBJ_NAME 
       ,B.OLD_OBJ_NAME          AS OLD_OBJ_NAME 
       ,B.GRADE_HAND_NAME       AS GRADE_HAND_NAME 
       ,B.IS_MODIFY             AS IS_MODIFY 
       ,'N'                       AS IS_CRE 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(ROUND(FLOAT(CASE WHEN TRIM(A.CUST_SEX               ) = '' OR A.CUST_SEX               IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_MRG               ) = '' OR A.CUST_MRG               IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_BIR               ) = '' OR A.CUST_BIR               IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_NATION            ) = '' OR A.CUST_NATION            IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_REGISTER          ) = '' OR A.CUST_REGISTER          IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_REGADDR           ) = '' OR A.CUST_REGADDR           IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_FAMSTATUS         ) = '' OR A.CUST_FAMSTATUS         IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_HEALTH            ) = '' OR A.CUST_HEALTH            IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_EDU_LVL_COD       ) = '' OR A.CUST_EDU_LVL_COD       IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.GIHHEST_DEGREE         ) = '' OR A.GIHHEST_DEGREE         IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_WORK_UNIT_NAME    ) = '' OR A.CUST_WORK_UNIT_NAME    IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_UTELL             ) = '' OR A.CUST_UTELL             IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_POSN              ) = '' OR A.CUST_POSN              IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.MAINPROORINCOME        ) = '' OR A.MAINPROORINCOME        IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_OCCUP_COD         ) = '' OR A.CUST_OCCUP_COD         IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.IS_MEDICARE            ) = '' OR A.IS_MEDICARE            IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_YEL_NO            ) = '' OR A.CUST_YEL_NO            IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_MBTELNO           ) = '' OR A.CUST_MBTELNO           IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_WORKADDR          ) = '' OR A.CUST_WORKADDR          IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_PECON_RESUR       ) = '' OR A.CUST_PECON_RESUR       IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.FAM_INCOMEACC          ) = '' OR A.FAM_INCOMEACC          IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_TOT_ASS           ) = '' OR A.CUST_TOT_ASS           IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_TOT_DEBT          ) = '' OR A.CUST_TOT_DEBT          IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_FAM_NUM           ) = '' OR A.CUST_FAM_NUM           IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.HZ_CERTYPE             ) = '' OR A.HZ_CERTYPE             IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.HZ_CERTID              ) = '' OR A.HZ_CERTID              IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.HZ_NAME                ) = '' OR A.HZ_NAME                IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_HOUSE             ) = '' OR A.CUST_HOUSE             IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.CUST_HOUSEAREA         ) = '' OR A.CUST_HOUSEAREA         IS NULL THEN 0 ELSE 1 END + 
        CASE WHEN TRIM(A.INDUSTRYTYPE           ) = '' OR A.INDUSTRYTYPE           IS NULL THEN 0 ELSE 1 END) / 30*100,2) 
         AS DECIMAL(10,2)) AS UN_NULL_RATE 
       ,B.HOLD_PRO_FLAG         AS HOLD_PRO_FLAG 
       ,B.GPS                   AS GPS 
       ,B.LONGITUDE             AS LONGITUDE 
       ,B.LATITUDE              AS LATITUDE 
       ,A.CUST_MBTELNO          AS LINK_TEL 
       ,A.LINKMAN               AS LINK_MAN 
       ,B.CUST_ADDR             AS CUST_ADDR 
       ,B.VALID_FLG             AS VALID_FLG 
   FROM OCRM_F_CI_PER_CUST_INFO A                              --对私客户信息表
   LEFT JOIN OCRM_F_CI_CUST_DESC B                             --统一客户信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
dfn="OCRM_F_CI_CUST_DESC/"+V_DT+".parquet"
OCRM_F_CI_CUST_DESC.cache()
nrows = OCRM_F_CI_CUST_DESC.count()
OCRM_F_CI_CUST_DESC.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_CUST_DESC.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-03::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_ABBR             AS CUST_ABBR 
       ,A.CUST_STAT             AS CUST_STAT 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.CERT_TYPE             AS CERT_TYPE 
       ,A.CERT_NUM              AS CERT_NUM 
       ,A.CUST_CRELVL           AS CUST_CRELVL 
       ,A.CUST_EVALUATEDATE     AS CUST_EVALUATEDATE 
       ,A.CUST_EVALUATEOVERDATE AS CUST_EVALUATEOVERDATE 
       ,B.SYS_ID                AS ODS_SYS_ID 
       ,A.CRM_DT                AS CRM_DT 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_STRATEGY           AS IF_STRATEGY 
       ,A.OBJ_RATING            AS OBJ_RATING 
       ,A.OBJ_DATE              AS OBJ_DATE 
       ,A.OLD_OBJ_RATING        AS OLD_OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,A.SUB_DATE              AS SUB_DATE 
       ,A.OLD_SUB_RATING        AS OLD_SUB_RATING 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUST_LEV              AS CUST_LEV 
       ,A.HB_FLAG               AS HB_FLAG 
       ,A.BILL_REPLACE_FLAG     AS BILL_REPLACE_FLAG 
       ,A.OBLIGATION_FLAG       AS OBLIGATION_FLAG 
       ,A.WAGE_ARREAR_FALG      AS WAGE_ARREAR_FALG 
       ,A.TAXES_FLAG            AS TAXES_FLAG 
       ,A.USURY_FLAG            AS USURY_FLAG 
       ,A.OPERATION_SAFE_FALG   AS OPERATION_SAFE_FALG 
       ,A.OBJ_NAME              AS OBJ_NAME 
       ,A.OLD_OBJ_NAME          AS OLD_OBJ_NAME 
       ,A.GRADE_HAND_NAME       AS GRADE_HAND_NAME 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.IS_CRE                AS IS_CRE 
       ,A.FR_ID                 AS FR_ID 
       ,A.UN_NULL_RATE          AS UN_NULL_RATE 
       ,A.HOLD_PRO_FLAG         AS HOLD_PRO_FLAG 
       ,A.GPS                   AS GPS 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.LINK_MAN              AS LINK_MAN 
       ,A.CUST_ADDR             AS CUST_ADDR 
       ,A.VALID_FLG             AS VALID_FLG 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN(
         SELECT FR_ID,ODS_CUST_ID 
               ,SUM(DISTINCT SYS_ID_FLAG)                       AS SYS_ID 
           FROM OCRM_F_CI_SYS_RESOURCE 
          GROUP BY FR_ID,ODS_CUST_ID) B                              --系统来源中间表
     ON A.CUST_ID               = B.ODS_CUST_ID 
    AND A.CRM_DT                = V_DT 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC_INNTMP1.registerTempTable("OCRM_F_CI_CUST_DESC_INNTMP1")

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_ZH_NAME                                       --客户（中文）名称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名称:src.CUST_EN_NAME
       ,DST.CUST_ABBR                                          --0:src.CUST_ABBR
       ,DST.CUST_STAT                                          --客户状态:src.CUST_STAT
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_CRELVL                                        --0:src.CUST_CRELVL
       ,DST.CUST_EVALUATEDATE                                  --0:src.CUST_EVALUATEDATE
       ,DST.CUST_EVALUATEOVERDATE                              --0:src.CUST_EVALUATEOVERDATE
       ,DST.ODS_SYS_ID                                         --系统编号:src.ODS_SYS_ID
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.IF_SHAREHOLDER                                     --是否股东:src.IF_SHAREHOLDER
       ,DST.IF_STRATEGY                                        --0:src.IF_STRATEGY
       ,DST.OBJ_RATING                                         --客户客观评级:src.OBJ_RATING
       ,DST.OBJ_DATE                                           --评级日期:src.OBJ_DATE
       ,DST.OLD_OBJ_RATING                                     --上次评级:src.OLD_OBJ_RATING
       ,DST.SUB_RATING                                         --客户主观评级:src.SUB_RATING
       ,DST.SUB_DATE                                           --0:src.SUB_DATE
       ,DST.OLD_SUB_RATING                                     --0:src.OLD_SUB_RATING
       ,DST.IS_STAFF                                           --是否本行员工:src.IS_STAFF
       ,DST.CUST_LEV                                           --客户级别:src.CUST_LEV
       ,DST.HB_FLAG                                            --合并标志:src.HB_FLAG
       ,DST.BILL_REPLACE_FLAG                                  --0:src.BILL_REPLACE_FLAG
       ,DST.OBLIGATION_FLAG                                    --0:src.OBLIGATION_FLAG
       ,DST.WAGE_ARREAR_FALG                                   --0:src.WAGE_ARREAR_FALG
       ,DST.TAXES_FLAG                                         --0:src.TAXES_FLAG
       ,DST.USURY_FLAG                                         --0:src.USURY_FLAG
       ,DST.OPERATION_SAFE_FALG                                --0:src.OPERATION_SAFE_FALG
       ,DST.OBJ_NAME                                           --客观评级名称:src.OBJ_NAME
       ,DST.OLD_OBJ_NAME                                       --上次客观评级名称:src.OLD_OBJ_NAME
       ,DST.GRADE_HAND_NAME                                    --主观评级名称:src.GRADE_HAND_NAME
       ,DST.IS_MODIFY                                          --是否前台修改:src.IS_MODIFY
       ,DST.IS_CRE                                             --是否有贷户:src.IS_CRE
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.UN_NULL_RATE                                       --客户信息完整度:src.UN_NULL_RATE
       ,DST.HOLD_PRO_FLAG                                      --持有产品标识:src.HOLD_PRO_FLAG
       ,DST.GPS                                                --GPS:src.GPS
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.LINK_MAN                                           --联系人:src.LINK_MAN
       ,DST.CUST_ADDR                                          --客户地址:src.CUST_ADDR
       ,DST.VALID_FLG                                          --有效标志:src.VALID_FLG
   FROM OCRM_F_CI_CUST_DESC DST 
   LEFT JOIN OCRM_F_CI_CUST_DESC_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_DESC/"+V_DT+".parquet"
OCRM_F_CI_CUST_DESC_INNTMP2=OCRM_F_CI_CUST_DESC_INNTMP2.unionAll(OCRM_F_CI_CUST_DESC_INNTMP1)
OCRM_F_CI_CUST_DESC_INNTMP1.cache()
OCRM_F_CI_CUST_DESC_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_DESC_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_DESC_INNTMP2.count()
OCRM_F_CI_CUST_DESC_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_DESC_INNTMP1.unpersist()
OCRM_F_CI_CUST_DESC_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/")

#任务[12] 001-04::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_ABBR             AS CUST_ABBR 
       ,A.CUST_STAT             AS CUST_STAT 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.CERT_TYPE             AS CERT_TYPE 
       ,A.CERT_NUM              AS CERT_NUM 
       ,A.CUST_CRELVL           AS CUST_CRELVL 
       ,A.CUST_EVALUATEDATE     AS CUST_EVALUATEDATE 
       ,A.CUST_EVALUATEOVERDATE AS CUST_EVALUATEOVERDATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.CRM_DT                AS CRM_DT 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_STRATEGY           AS IF_STRATEGY 
       ,A.OBJ_RATING            AS OBJ_RATING 
       ,A.OBJ_DATE              AS OBJ_DATE 
       ,A.OLD_OBJ_RATING        AS OLD_OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,A.SUB_DATE              AS SUB_DATE 
       ,A.OLD_SUB_RATING        AS OLD_SUB_RATING 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUST_LEV              AS CUST_LEV 
       ,A.HB_FLAG               AS HB_FLAG 
       ,A.BILL_REPLACE_FLAG     AS BILL_REPLACE_FLAG 
       ,A.OBLIGATION_FLAG       AS OBLIGATION_FLAG 
       ,A.WAGE_ARREAR_FALG      AS WAGE_ARREAR_FALG 
       ,A.TAXES_FLAG            AS TAXES_FLAG 
       ,A.USURY_FLAG            AS USURY_FLAG 
       ,A.OPERATION_SAFE_FALG   AS OPERATION_SAFE_FALG 
       ,A.OBJ_NAME              AS OBJ_NAME 
       ,A.OLD_OBJ_NAME          AS OLD_OBJ_NAME 
       ,A.GRADE_HAND_NAME       AS GRADE_HAND_NAME 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,CASE WHEN B.CUST_ID IS NULL THEN 'N' ELSE 'Y' END                     AS IS_CRE 
       ,A.FR_ID                 AS FR_ID 
       ,A.UN_NULL_RATE          AS UN_NULL_RATE 
       ,A.HOLD_PRO_FLAG         AS HOLD_PRO_FLAG 
       ,A.GPS                   AS GPS 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.LINK_MAN              AS LINK_MAN 
       ,A.CUST_ADDR             AS CUST_ADDR 
       ,A.VALID_FLG             AS VALID_FLG 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
   LEFT JOIN(
         SELECT DISTINCT CUST_ID 
               ,FR_ID 
           FROM ACRM_F_CI_ASSET_BUSI_PROTO 
          WHERE LN_APCL_FLG             = 'N' 
            AND BAL > 0) B                                     --资产协议
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC_INNTMP1.registerTempTable("OCRM_F_CI_CUST_DESC_INNTMP1")

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_ZH_NAME                                       --客户（中文）名称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名称:src.CUST_EN_NAME
       ,DST.CUST_ABBR                                          --0:src.CUST_ABBR
       ,DST.CUST_STAT                                          --客户状态:src.CUST_STAT
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_CRELVL                                        --0:src.CUST_CRELVL
       ,DST.CUST_EVALUATEDATE                                  --0:src.CUST_EVALUATEDATE
       ,DST.CUST_EVALUATEOVERDATE                              --0:src.CUST_EVALUATEOVERDATE
       ,DST.ODS_SYS_ID                                         --系统编号:src.ODS_SYS_ID
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.IF_SHAREHOLDER                                     --是否股东:src.IF_SHAREHOLDER
       ,DST.IF_STRATEGY                                        --0:src.IF_STRATEGY
       ,DST.OBJ_RATING                                         --客户客观评级:src.OBJ_RATING
       ,DST.OBJ_DATE                                           --评级日期:src.OBJ_DATE
       ,DST.OLD_OBJ_RATING                                     --上次评级:src.OLD_OBJ_RATING
       ,DST.SUB_RATING                                         --客户主观评级:src.SUB_RATING
       ,DST.SUB_DATE                                           --0:src.SUB_DATE
       ,DST.OLD_SUB_RATING                                     --0:src.OLD_SUB_RATING
       ,DST.IS_STAFF                                           --是否本行员工:src.IS_STAFF
       ,DST.CUST_LEV                                           --客户级别:src.CUST_LEV
       ,DST.HB_FLAG                                            --合并标志:src.HB_FLAG
       ,DST.BILL_REPLACE_FLAG                                  --0:src.BILL_REPLACE_FLAG
       ,DST.OBLIGATION_FLAG                                    --0:src.OBLIGATION_FLAG
       ,DST.WAGE_ARREAR_FALG                                   --0:src.WAGE_ARREAR_FALG
       ,DST.TAXES_FLAG                                         --0:src.TAXES_FLAG
       ,DST.USURY_FLAG                                         --0:src.USURY_FLAG
       ,DST.OPERATION_SAFE_FALG                                --0:src.OPERATION_SAFE_FALG
       ,DST.OBJ_NAME                                           --客观评级名称:src.OBJ_NAME
       ,DST.OLD_OBJ_NAME                                       --上次客观评级名称:src.OLD_OBJ_NAME
       ,DST.GRADE_HAND_NAME                                    --主观评级名称:src.GRADE_HAND_NAME
       ,DST.IS_MODIFY                                          --是否前台修改:src.IS_MODIFY
       ,DST.IS_CRE                                             --是否有贷户:src.IS_CRE
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.UN_NULL_RATE                                       --客户信息完整度:src.UN_NULL_RATE
       ,DST.HOLD_PRO_FLAG                                      --持有产品标识:src.HOLD_PRO_FLAG
       ,DST.GPS                                                --GPS:src.GPS
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.LINK_MAN                                           --联系人:src.LINK_MAN
       ,DST.CUST_ADDR                                          --客户地址:src.CUST_ADDR
       ,DST.VALID_FLG                                          --有效标志:src.VALID_FLG
   FROM OCRM_F_CI_CUST_DESC DST 
   LEFT JOIN OCRM_F_CI_CUST_DESC_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_DESC/"+V_DT+".parquet"
OCRM_F_CI_CUST_DESC_INNTMP2=OCRM_F_CI_CUST_DESC_INNTMP2.unionAll(OCRM_F_CI_CUST_DESC_INNTMP1)
OCRM_F_CI_CUST_DESC_INNTMP1.cache()
OCRM_F_CI_CUST_DESC_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_DESC_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_DESC_INNTMP2.count()
OCRM_F_CI_CUST_DESC_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_DESC_INNTMP1.unpersist()
OCRM_F_CI_CUST_DESC_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/")

#任务[12] 001-05::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_ABBR             AS CUST_ABBR 
       ,A.CUST_STAT             AS CUST_STAT 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.CERT_TYPE             AS CERT_TYPE 
       ,A.CERT_NUM              AS CERT_NUM 
       ,A.CUST_CRELVL           AS CUST_CRELVL 
       ,A.CUST_EVALUATEDATE     AS CUST_EVALUATEDATE 
       ,A.CUST_EVALUATEOVERDATE AS CUST_EVALUATEOVERDATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.CRM_DT                AS CRM_DT 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_STRATEGY           AS IF_STRATEGY 
       ,CASE WHEN CUST_TYP                = '2' THEN '27' ELSE '17' END                     AS OBJ_RATING 
       ,A.OBJ_DATE              AS OBJ_DATE 
       ,A.OLD_OBJ_RATING        AS OLD_OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,A.SUB_DATE              AS SUB_DATE 
       ,A.OLD_SUB_RATING        AS OLD_SUB_RATING 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUST_LEV              AS CUST_LEV 
       ,A.HB_FLAG               AS HB_FLAG 
       ,A.BILL_REPLACE_FLAG     AS BILL_REPLACE_FLAG 
       ,A.OBLIGATION_FLAG       AS OBLIGATION_FLAG 
       ,A.WAGE_ARREAR_FALG      AS WAGE_ARREAR_FALG 
       ,A.TAXES_FLAG            AS TAXES_FLAG 
       ,A.USURY_FLAG            AS USURY_FLAG 
       ,A.OPERATION_SAFE_FALG   AS OPERATION_SAFE_FALG 
       ,A.OBJ_NAME              AS OBJ_NAME 
       ,A.OLD_OBJ_NAME          AS OLD_OBJ_NAME 
       ,A.GRADE_HAND_NAME       AS GRADE_HAND_NAME 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.IS_CRE                AS IS_CRE 
       ,A.FR_ID                 AS FR_ID 
       ,A.UN_NULL_RATE          AS UN_NULL_RATE 
       ,A.HOLD_PRO_FLAG         AS HOLD_PRO_FLAG 
       ,A.GPS                   AS GPS 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.LINK_MAN              AS LINK_MAN 
       ,A.CUST_ADDR             AS CUST_ADDR 
       ,A.VALID_FLG             AS VALID_FLG 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  WHERE OBJ_RATING IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC_INNTMP1.registerTempTable("OCRM_F_CI_CUST_DESC_INNTMP1")

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_ZH_NAME                                       --客户（中文）名称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名称:src.CUST_EN_NAME
       ,DST.CUST_ABBR                                          --0:src.CUST_ABBR
       ,DST.CUST_STAT                                          --客户状态:src.CUST_STAT
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_CRELVL                                        --0:src.CUST_CRELVL
       ,DST.CUST_EVALUATEDATE                                  --0:src.CUST_EVALUATEDATE
       ,DST.CUST_EVALUATEOVERDATE                              --0:src.CUST_EVALUATEOVERDATE
       ,DST.ODS_SYS_ID                                         --系统编号:src.ODS_SYS_ID
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.IF_SHAREHOLDER                                     --是否股东:src.IF_SHAREHOLDER
       ,DST.IF_STRATEGY                                        --0:src.IF_STRATEGY
       ,DST.OBJ_RATING                                         --客户客观评级:src.OBJ_RATING
       ,DST.OBJ_DATE                                           --评级日期:src.OBJ_DATE
       ,DST.OLD_OBJ_RATING                                     --上次评级:src.OLD_OBJ_RATING
       ,DST.SUB_RATING                                         --客户主观评级:src.SUB_RATING
       ,DST.SUB_DATE                                           --0:src.SUB_DATE
       ,DST.OLD_SUB_RATING                                     --0:src.OLD_SUB_RATING
       ,DST.IS_STAFF                                           --是否本行员工:src.IS_STAFF
       ,DST.CUST_LEV                                           --客户级别:src.CUST_LEV
       ,DST.HB_FLAG                                            --合并标志:src.HB_FLAG
       ,DST.BILL_REPLACE_FLAG                                  --0:src.BILL_REPLACE_FLAG
       ,DST.OBLIGATION_FLAG                                    --0:src.OBLIGATION_FLAG
       ,DST.WAGE_ARREAR_FALG                                   --0:src.WAGE_ARREAR_FALG
       ,DST.TAXES_FLAG                                         --0:src.TAXES_FLAG
       ,DST.USURY_FLAG                                         --0:src.USURY_FLAG
       ,DST.OPERATION_SAFE_FALG                                --0:src.OPERATION_SAFE_FALG
       ,DST.OBJ_NAME                                           --客观评级名称:src.OBJ_NAME
       ,DST.OLD_OBJ_NAME                                       --上次客观评级名称:src.OLD_OBJ_NAME
       ,DST.GRADE_HAND_NAME                                    --主观评级名称:src.GRADE_HAND_NAME
       ,DST.IS_MODIFY                                          --是否前台修改:src.IS_MODIFY
       ,DST.IS_CRE                                             --是否有贷户:src.IS_CRE
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.UN_NULL_RATE                                       --客户信息完整度:src.UN_NULL_RATE
       ,DST.HOLD_PRO_FLAG                                      --持有产品标识:src.HOLD_PRO_FLAG
       ,DST.GPS                                                --GPS:src.GPS
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.LINK_MAN                                           --联系人:src.LINK_MAN
       ,DST.CUST_ADDR                                          --客户地址:src.CUST_ADDR
       ,DST.VALID_FLG                                          --有效标志:src.VALID_FLG
   FROM OCRM_F_CI_CUST_DESC DST 
   LEFT JOIN OCRM_F_CI_CUST_DESC_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_DESC/"+V_DT+".parquet"
OCRM_F_CI_CUST_DESC_INNTMP2=OCRM_F_CI_CUST_DESC_INNTMP2.unionAll(OCRM_F_CI_CUST_DESC_INNTMP1)
OCRM_F_CI_CUST_DESC_INNTMP1.cache()
OCRM_F_CI_CUST_DESC_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_DESC_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_DESC_INNTMP2.count()
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT+".parquet")
OCRM_F_CI_CUST_DESC_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_DESC_INNTMP1.unpersist()
OCRM_F_CI_CUST_DESC_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/")
