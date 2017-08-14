#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_SUN_IND_INFO_DETAIL').setMaster(sys.argv[2])
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
F_TX_ZDH_ZZDH_HISTORY_LS = sqlContext.read.parquet(hdfs+'/F_TX_ZDH_ZZDH_HISTORY_LS/*')
F_TX_ZDH_ZZDH_HISTORY_LS.registerTempTable("F_TX_ZDH_ZZDH_HISTORY_LS")
ACRM_F_NI_FINANCING = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_FINANCING/*')
ACRM_F_NI_FINANCING.registerTempTable("ACRM_F_NI_FINANCING")
F_LN_SUN_COMMON_CHECKUSER = sqlContext.read.parquet(hdfs+'/F_LN_SUN_COMMON_CHECKUSER/*')
F_LN_SUN_COMMON_CHECKUSER.registerTempTable("F_LN_SUN_COMMON_CHECKUSER")
ACRM_F_CI_NIN_TRANSLOG = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_NIN_TRANSLOG/*')
ACRM_F_CI_NIN_TRANSLOG.registerTempTable("ACRM_F_CI_NIN_TRANSLOG")
F_CI_SUN_IND_INFO = sqlContext.read.parquet(hdfs+'/F_CI_SUN_IND_INFO/*')
F_CI_SUN_IND_INFO.registerTempTable("F_CI_SUN_IND_INFO")
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
F_TX_WSYH_ECCIFMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIFMCH/*')
F_TX_WSYH_ECCIFMCH.registerTempTable("F_TX_WSYH_ECCIFMCH")
F_CI_ZDH_ZZDH_SHOP = sqlContext.read.parquet(hdfs+'/F_CI_ZDH_ZZDH_SHOP/*')
F_CI_ZDH_ZZDH_SHOP.registerTempTable("F_CI_ZDH_ZZDH_SHOP")
MCRM_SUN_IND_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/MCRM_SUN_IND_INFO_DETAIL_BK/'+V_DT_LD+'.parquet/*')
MCRM_SUN_IND_INFO_DETAIL.registerTempTable("MCRM_SUN_IND_INFO_DETAIL")
#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,ROW_ID                  AS ROW_ID 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CERT_ID                 AS CERT_ID 
       ,TEMPSAVEFLAG            AS TEMPSAVEFLAG 
       ,APPROVEDATE             AS APPROVEDATE 
       ,REGISTERDATE            AS REGISTERDATE 
       ,DEP_BEGIN_DATE          AS DEP_BEGIN_DATE 
       ,DEP_END_DATE            AS DEP_END_DATE 
       ,DEP_BAL                 AS DEP_BAL 
       ,DEP_ACCT_NO             AS DEP_ACCT_NO 
       ,DEP_TYPE                AS DEP_TYPE 
       ,CRE_AMOUNT              AS CRE_AMOUNT 
       ,CRE_BEGIN_DATE          AS CRE_BEGIN_DATE 
       ,CRE_END_DATE            AS CRE_END_DATE 
       ,CRE_BAL                 AS CRE_BAL 
       ,CRE_ACCT_NO             AS CRE_ACCT_NO 
       ,FIN_BEGIN_DATE          AS FIN_BEGIN_DATE 
       ,FIN_END_DATE            AS FIN_END_DATE 
       ,FIN_BAL                 AS FIN_BAL 
       ,FIN_ACCT_NO             AS FIN_ACCT_NO 
       ,MOBILE_OPEN_DATE        AS MOBILE_OPEN_DATE 
       ,MOBILE_NUM              AS MOBILE_NUM 
       ,MOBILE_BAL              AS MOBILE_BAL 
       ,EBANK_OPEN_DATE         AS EBANK_OPEN_DATE 
       ,EBANK_NUM               AS EBANK_NUM 
       ,EBANK_BAL               AS EBANK_BAL 
       ,POS_OPEN_DATE           AS POS_OPEN_DATE 
       ,POS_NUM                 AS POS_NUM 
       ,POS_BAL                 AS POS_BAL 
       ,BMT_OPEN_DATE           AS BMT_OPEN_DATE 
       ,BMT_NUM                 AS BMT_NUM 
       ,BMT_BAL                 AS BMT_BAL 
       ,FR_ID                   AS FR_ID 
       ,REPORT_DATE             AS REPORT_DATE 
   FROM MCRM_SUN_IND_INFO_DETAIL A                             --
  WHERE REPORT_DATE >= DATE_ADD(TRUNC(V_DT, 'YY'),-1) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_TMP = sqlContext.sql(sql)
MCRM_SUN_IND_INFO_DETAIL_TMP.registerTempTable("MCRM_SUN_IND_INFO_DETAIL_TMP")
dfn="MCRM_SUN_IND_INFO_DETAIL_TMP/"+V_DT+".parquet"
MCRM_SUN_IND_INFO_DETAIL_TMP.cache()
nrows = MCRM_SUN_IND_INFO_DETAIL_TMP.count()
MCRM_SUN_IND_INFO_DETAIL_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_SUN_IND_INFO_DETAIL_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_SUN_IND_INFO_DETAIL_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
MCRM_SUN_IND_INFO_DETAIL_TMP = sqlContext.read.parquet(hdfs+'/MCRM_SUN_IND_INFO_DETAIL_TMP/*')
MCRM_SUN_IND_INFO_DETAIL_TMP.registerTempTable("MCRM_SUN_IND_INFO_DETAIL_TMP")
sql = """
 SELECT ID                      AS ID 
       ,ROW_ID                  AS ROW_ID 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CERT_ID                 AS CERT_ID 
       ,TEMPSAVEFLAG            AS TEMPSAVEFLAG 
       ,APPROVEDATE             AS APPROVEDATE 
       ,REGISTERDATE            AS REGISTERDATE 
       ,DEP_BEGIN_DATE          AS DEP_BEGIN_DATE 
       ,DEP_END_DATE            AS DEP_END_DATE 
       ,DEP_BAL                 AS DEP_BAL 
       ,DEP_ACCT_NO             AS DEP_ACCT_NO 
       ,DEP_TYPE                AS DEP_TYPE 
       ,CRE_AMOUNT              AS CRE_AMOUNT 
       ,CRE_BEGIN_DATE          AS CRE_BEGIN_DATE 
       ,CRE_END_DATE            AS CRE_END_DATE 
       ,CRE_BAL                 AS CRE_BAL 
       ,CRE_ACCT_NO             AS CRE_ACCT_NO 
       ,FIN_BEGIN_DATE          AS FIN_BEGIN_DATE 
       ,FIN_END_DATE            AS FIN_END_DATE 
       ,FIN_BAL                 AS FIN_BAL 
       ,FIN_ACCT_NO             AS FIN_ACCT_NO 
       ,MOBILE_OPEN_DATE        AS MOBILE_OPEN_DATE 
       ,MOBILE_NUM              AS MOBILE_NUM 
       ,MOBILE_BAL              AS MOBILE_BAL 
       ,EBANK_OPEN_DATE         AS EBANK_OPEN_DATE 
       ,EBANK_NUM               AS EBANK_NUM 
       ,EBANK_BAL               AS EBANK_BAL 
       ,POS_OPEN_DATE           AS POS_OPEN_DATE 
       ,POS_NUM                 AS POS_NUM 
       ,POS_BAL                 AS POS_BAL 
       ,BMT_OPEN_DATE           AS BMT_OPEN_DATE 
       ,BMT_NUM                 AS BMT_NUM 
       ,BMT_BAL                 AS BMT_BAL 
       ,FR_ID                   AS FR_ID 
       ,REPORT_DATE             AS REPORT_DATE 
   FROM MCRM_SUN_IND_INFO_DETAIL_TMP A                         --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL = sqlContext.sql(sql)
MCRM_SUN_IND_INFO_DETAIL.registerTempTable("MCRM_SUN_IND_INFO_DETAIL")
dfn="MCRM_SUN_IND_INFO_DETAIL/"+V_DT+".parquet"
MCRM_SUN_IND_INFO_DETAIL.cache()
nrows = MCRM_SUN_IND_INFO_DETAIL.count()
MCRM_SUN_IND_INFO_DETAIL.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_SUN_IND_INFO_DETAIL.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_SUN_IND_INFO_DETAIL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT B.ODS_CUST_ID           AS CUST_ID 
       ,B.ODS_CUST_NAME         AS CUST_NAME 
       ,A.INPUTORGID            AS ORG_ID 
       ,D.ORG_NAME              AS ORG_NAME 
       ,A.CERTID                AS CERT_ID 
       ,A.TEMPSAVEFLAG          AS TEMPSAVEFLAG 
       ,C.ENDTIME               AS APPROVEDATE 
       ,A.INPUTDATE             AS REGISTERDATE 
   FROM F_CI_SUN_IND_INFO A                                    --
  INNER JOIN OCRM_F_CI_SYS_RESOURCE B                          --
     ON A.CUSTOMERID            = B.SOURCE_CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ODS_SYS_ID            = 'SLNA' 
   LEFT JOIN F_LN_SUN_COMMON_CHECKUSER C                       --
     ON A.CUSTOMERID            = C.OBJECTNO 
    AND A.FR_ID                 = C.FR_ID 
   LEFT JOIN ADMIN_AUTH_ORG D                                  --
     ON A.INPUTORGID            = D.ORG_ID 
    AND A.FR_ID                 = D.FR_ID 
  WHERE SUBSTR(A.INPUTDATE, 1, 4)                       = SUBSTR(V_DT, 1, 4) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_MID_SUN_IND_INFO = sqlContext.sql(sql)
MCRM_MID_SUN_IND_INFO.registerTempTable("MCRM_MID_SUN_IND_INFO")
dfn="MCRM_MID_SUN_IND_INFO/"+V_DT+".parquet"
MCRM_MID_SUN_IND_INFO.cache()
nrows = MCRM_MID_SUN_IND_INFO.count()
MCRM_MID_SUN_IND_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_MID_SUN_IND_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_MID_SUN_IND_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_MID_SUN_IND_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1
MCRM_MID_SUN_IND_INFO = sqlContext.read.parquet(hdfs+'/MCRM_MID_SUN_IND_INFO/*')
MCRM_MID_SUN_IND_INFO.registerTempTable("MCRM_MID_SUN_IND_INFO")
sql = """
 SELECT monotonically_increasing_id()      AS ID 
       ,CAST(ROW_NUMBER() OVER(PARTITION BY B.CUST_ID  ORDER BY B.OPEN_DT ASC)  AS INTEGER) AS ROW_ID 
       ,CAST(A.CUST_ID          AS VARCHAR(20)   )    AS CUST_ID 
       ,CAST(A.CUST_NAME        AS VARCHAR(50)   )    AS CUST_NAME 
       ,CAST(A.ORG_ID           AS VARCHAR(10)   )    AS ORG_ID 
       ,CAST(A.ORG_NAME         AS VARCHAR(64)   )    AS ORG_NAME 
       ,CAST(A.CERT_ID          AS VARCHAR(64)   )    AS CERT_ID 
       ,CAST(A.TEMPSAVEFLAG     AS VARCHAR(2)    )    AS TEMPSAVEFLAG 
       ,CAST(A.APPROVEDATE      AS VARCHAR(10)   )    AS APPROVEDATE 
       ,CAST(A.REGISTERDATE     AS VARCHAR(10)   )    AS REGISTERDATE 
       ,CAST(B.OPEN_DT          AS VARCHAR(10)   )    AS DEP_BEGIN_DATE 
       ,CAST(B.TD_MU_DT         AS VARCHAR(10)   )    AS DEP_END_DATE 
       ,CAST(B.BAL_RMB          AS DECIMAL(24,6) )    AS DEP_BAL 
       ,CAST(B.ODS_ACCT_NO      AS VARCHAR(64)   )    AS DEP_ACCT_NO 
       ,CAST(B.ACCONT_TYPE      AS VARCHAR(2)    )    AS DEP_TYPE 
       ,CAST(''                 AS DECIMAL(24,6) )  AS CRE_AMOUNT 
       ,CAST(''                 AS VARCHAR(10)   )  AS CRE_BEGIN_DATE 
       ,CAST(''                 AS VARCHAR(10)   )  AS CRE_END_DATE 
       ,CAST(''                 AS DECIMAL(24,6) )  AS CRE_BAL 
       ,CAST(''                 AS VARCHAR(64)   )  AS CRE_ACCT_NO 
       ,CAST(''                 AS VARCHAR(10)   )  AS FIN_BEGIN_DATE 
       ,CAST(''                 AS VARCHAR(10)   )  AS FIN_END_DATE 
       ,CAST(''                 AS DECIMAL(24,6) )  AS FIN_BAL 
       ,CAST(''                 AS VARCHAR(64)   )  AS FIN_ACCT_NO 
       ,CAST(''                 AS VARCHAR(10)   )  AS MOBILE_OPEN_DATE 
       ,CAST(''                 AS INTEGER       )  AS MOBILE_NUM 
       ,CAST(''                 AS DECIMAL(24,6) )  AS MOBILE_BAL 
       ,CAST(''                 AS VARCHAR(10)   )  AS EBANK_OPEN_DATE 
       ,CAST(''                 AS INTEGER       )  AS EBANK_NUM 
       ,CAST(''                 AS DECIMAL(24,6) )  AS EBANK_BAL 
       ,CAST(''                 AS VARCHAR(10)   )  AS POS_OPEN_DATE 
       ,CAST(''                 AS INTEGER       )  AS POS_NUM 
       ,CAST(''                 AS DECIMAL(24,6) )  AS POS_BAL 
       ,CAST(''                 AS VARCHAR(10)   )  AS BMT_OPEN_DATE 
       ,CAST(''                 AS INTEGER       )  AS BMT_NUM 
       ,CAST(''                 AS DECIMAL(24,6) )  AS BMT_BAL 
       ,CAST(B.FR_ID            AS VARCHAR(5)    )    AS FR_ID 
       ,CAST(V_DT               AS VARCHAR(10)   )  AS REPORT_DATE 
   FROM MCRM_MID_SUN_IND_INFO A                                --
  INNER JOIN ACRM_F_DP_SAVE_INFO B                             --
     ON A.CUST_ID               = B.CUST_ID 
    AND B.ACCT_STATUS           = '01' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL = sqlContext.sql(sql)
MCRM_SUN_IND_INFO_DETAIL.registerTempTable("MCRM_SUN_IND_INFO_DETAIL")
dfn="MCRM_SUN_IND_INFO_DETAIL/"+V_DT+".parquet"
MCRM_SUN_IND_INFO_DETAIL.cache()
nrows = MCRM_SUN_IND_INFO_DETAIL.count()
MCRM_SUN_IND_INFO_DETAIL.write.save(path=hdfs + '/' + dfn, mode='append')
MCRM_SUN_IND_INFO_DETAIL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_SUN_IND_INFO_DETAIL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-05::
V_STEP = V_STEP + 1
MCRM_SUN_IND_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/MCRM_SUN_IND_INFO_DETAIL/*')
MCRM_SUN_IND_INFO_DETAIL.registerTempTable("MCRM_SUN_IND_INFO_DETAIL")
sql = """
 SELECT CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN monotonically_increasing_id()  ELSE M.ID END    AS BIGINT)                 AS ID 
       ,CAST(B.ROW_ID       AS INTEGER      )      AS ROW_ID 
       ,CAST(B.CUST_ID      AS VARCHAR(20)  )      AS CUST_ID 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.CUST_NAME ELSE M.CUST_NAME END         AS VARCHAR(50))            AS CUST_NAME 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.ORG_ID ELSE M.ORG_ID END               AS VARCHAR(10))      AS ORG_ID 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.ORG_NAME ELSE M.ORG_NAME END           AS VARCHAR(64))          AS ORG_NAME 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.CERT_ID ELSE M.CERT_ID END             AS VARCHAR(64))        AS CERT_ID 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.TEMPSAVEFLAG ELSE M.TEMPSAVEFLAG END   AS VARCHAR(2) )                  AS TEMPSAVEFLAG 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.APPROVEDATE ELSE M.APPROVEDATE END     AS VARCHAR(10))                AS APPROVEDATE 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.REGISTERDATE ELSE M.REGISTERDATE END   AS VARCHAR(10))                  AS REGISTERDATE 
       ,CAST(M.DEP_BEGIN_DATE     AS VARCHAR(10)   )  AS DEP_BEGIN_DATE 
       ,CAST(M.DEP_END_DATE       AS VARCHAR(10)   )  AS DEP_END_DATE 
       ,CAST(M.DEP_BAL            AS DECIMAL(24,6) )  AS DEP_BAL 
       ,CAST(M.DEP_ACCT_NO        AS VARCHAR(64)   )  AS DEP_ACCT_NO 
       ,CAST(M.DEP_TYPE           AS VARCHAR(2)    )  AS DEP_TYPE 
       ,CAST(NVL(B.CONT_AMT, 0)   AS DECIMAL(24,6) )                   AS CRE_AMOUNT 
       ,CAST(B.BEGIN_DATE         AS VARCHAR(10)   )  AS CRE_BEGIN_DATE 
       ,CAST(B.END_DATE           AS VARCHAR(10)   )  AS CRE_END_DATE 
       ,CAST(NVL(B.BAL_RMB, 0)    AS DECIMAL(24,6) )                  AS CRE_BAL 
       ,CAST(B.ACCT_NO            AS VARCHAR(64)   )  AS CRE_ACCT_NO 
       ,CAST(M.FIN_BEGIN_DATE     AS VARCHAR(10)   )  AS FIN_BEGIN_DATE 
       ,CAST(M.FIN_END_DATE       AS VARCHAR(10)   )  AS FIN_END_DATE 
       ,CAST(M.FIN_BAL            AS DECIMAL(24,6) )  AS FIN_BAL 
       ,CAST(M.FIN_ACCT_NO        AS VARCHAR(64)   )  AS FIN_ACCT_NO 
       ,CAST(M.MOBILE_OPEN_DATE   AS VARCHAR(10)   )  AS MOBILE_OPEN_DATE 
       ,CAST(M.MOBILE_NUM         AS INTEGER       )  AS MOBILE_NUM 
       ,CAST(M.MOBILE_BAL         AS DECIMAL(24,6) )  AS MOBILE_BAL 
       ,CAST(M.EBANK_OPEN_DATE    AS VARCHAR(10)   )  AS EBANK_OPEN_DATE 
       ,CAST(M.EBANK_NUM          AS INTEGER       )  AS EBANK_NUM 
       ,CAST(M.EBANK_BAL          AS DECIMAL(24,6) )  AS EBANK_BAL 
       ,CAST(M.POS_OPEN_DATE      AS VARCHAR(10)   )  AS POS_OPEN_DATE 
       ,CAST(M.POS_NUM            AS INTEGER       )  AS POS_NUM 
       ,CAST(M.POS_BAL            AS DECIMAL(24,6) )  AS POS_BAL 
       ,CAST(M.BMT_OPEN_DATE      AS VARCHAR(10)   )  AS BMT_OPEN_DATE 
       ,CAST(M.BMT_NUM            AS INTEGER       )  AS BMT_NUM 
       ,CAST(M.BMT_BAL            AS DECIMAL(24,6) )  AS BMT_BAL 
       ,CAST(M.FR_ID              AS VARCHAR(5)    )  AS FR_ID 
       ,CAST(M.REPORT_DATE        AS VARCHAR(10)   )  AS REPORT_DATE  
   FROM MCRM_MID_SUN_IND_INFO A                                --
  INNER JOIN 
 (SELECT ROW_NUMBER() OVER(PARTITION BY CUST_ID ORDER BY BEGIN_DATE ASC) ROW_ID
                ,CONT_AMT,BEGIN_DATE,END_DATE,BAL_RMB,ACCT_NO,CUST_ID
                FROM ACRM_F_CI_ASSET_BUSI_PROTO WHERE LN_APCL_FLG = 'N' AND  BAL_RMB > 0
) B                                                 --
     ON A.CUST_ID               = B.CUST_ID 
   LEFT JOIN MCRM_SUN_IND_INFO_DETAIL M                        --
     ON B.CUST_ID               = M.CUST_ID 
    AND M.ROW_ID                = B.ROW_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1 = sqlContext.sql(sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.registerTempTable("MCRM_SUN_IND_INFO_DETAIL_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.ROW_ID                                             --:src.ROW_ID
       ,DST.CUST_ID                                            --:src.CUST_ID
       ,DST.CUST_NAME                                          --:src.CUST_NAME
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.ORG_NAME                                           --:src.ORG_NAME
       ,DST.CERT_ID                                            --:src.CERT_ID
       ,DST.TEMPSAVEFLAG                                       --:src.TEMPSAVEFLAG
       ,DST.APPROVEDATE                                        --:src.APPROVEDATE
       ,DST.REGISTERDATE                                       --:src.REGISTERDATE
       ,DST.DEP_BEGIN_DATE                                     --:src.DEP_BEGIN_DATE
       ,DST.DEP_END_DATE                                       --:src.DEP_END_DATE
       ,DST.DEP_BAL                                            --:src.DEP_BAL
       ,DST.DEP_ACCT_NO                                        --:src.DEP_ACCT_NO
       ,DST.DEP_TYPE                                           --:src.DEP_TYPE
       ,DST.CRE_AMOUNT                                         --:src.CRE_AMOUNT
       ,DST.CRE_BEGIN_DATE                                     --:src.CRE_BEGIN_DATE
       ,DST.CRE_END_DATE                                       --:src.CRE_END_DATE
       ,DST.CRE_BAL                                            --:src.CRE_BAL
       ,DST.CRE_ACCT_NO                                        --:src.CRE_ACCT_NO
       ,DST.FIN_BEGIN_DATE                                     --:src.FIN_BEGIN_DATE
       ,DST.FIN_END_DATE                                       --:src.FIN_END_DATE
       ,DST.FIN_BAL                                            --:src.FIN_BAL
       ,DST.FIN_ACCT_NO                                        --:src.FIN_ACCT_NO
       ,DST.MOBILE_OPEN_DATE                                   --:src.MOBILE_OPEN_DATE
       ,DST.MOBILE_NUM                                         --:src.MOBILE_NUM
       ,DST.MOBILE_BAL                                         --:src.MOBILE_BAL
       ,DST.EBANK_OPEN_DATE                                    --:src.EBANK_OPEN_DATE
       ,DST.EBANK_NUM                                          --:src.EBANK_NUM
       ,DST.EBANK_BAL                                          --:src.EBANK_BAL
       ,DST.POS_OPEN_DATE                                      --:src.POS_OPEN_DATE
       ,DST.POS_NUM                                            --:src.POS_NUM
       ,DST.POS_BAL                                            --:src.POS_BAL
       ,DST.BMT_OPEN_DATE                                      --:src.BMT_OPEN_DATE
       ,DST.BMT_NUM                                            --:src.BMT_NUM
       ,DST.BMT_BAL                                            --:src.BMT_BAL
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.REPORT_DATE                                        --:src.REPORT_DATE
   FROM MCRM_SUN_IND_INFO_DETAIL DST 
   LEFT JOIN MCRM_SUN_IND_INFO_DETAIL_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.ROW_ID              = DST.ROW_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP2 = sqlContext.sql(sql)
dfn="MCRM_SUN_IND_INFO_DETAIL/"+V_DT+".parquet"
UNION=MCRM_SUN_IND_INFO_DETAIL_INNTMP2.unionAll(MCRM_SUN_IND_INFO_DETAIL_INNTMP1)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.cache()
MCRM_SUN_IND_INFO_DETAIL_INNTMP2.cache()
nrowsi = MCRM_SUN_IND_INFO_DETAIL_INNTMP1.count()
nrowsa = MCRM_SUN_IND_INFO_DETAIL_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.unpersist()
MCRM_SUN_IND_INFO_DETAIL_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_SUN_IND_INFO_DETAIL lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL/"+V_DT_LD+".parquet /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL_BK/")

#任务[12] 001-06::
V_STEP = V_STEP + 1
MCRM_SUN_IND_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/MCRM_SUN_IND_INFO_DETAIL/*')
MCRM_SUN_IND_INFO_DETAIL.registerTempTable("MCRM_SUN_IND_INFO_DETAIL")
sql = """
 SELECT CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN monotonically_increasing_id()  ELSE M.ID END   AS BIGINT)                  AS ID 
       ,CAST(B.ROW_ID         AS INTEGER       )    AS ROW_ID 
       ,CAST(B.CUST_ID        AS VARCHAR(20)   )    AS CUST_ID 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.CUST_NAME ELSE M.CUST_NAME END        AS VARCHAR(50) )           AS CUST_NAME 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.ORG_ID ELSE M.ORG_ID END              AS VARCHAR(10) )     AS ORG_ID 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.ORG_NAME ELSE M.ORG_NAME END          AS VARCHAR(64) )         AS ORG_NAME 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.CERT_ID ELSE M.CERT_ID END            AS VARCHAR(64) )       AS CERT_ID 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.TEMPSAVEFLAG ELSE M.TEMPSAVEFLAG END  AS VARCHAR(2)  )                 AS TEMPSAVEFLAG 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.APPROVEDATE ELSE M.APPROVEDATE END    AS VARCHAR(10) )               AS APPROVEDATE 
       ,CAST(CASE WHEN M.CUST_ID IS NULL AND M.ROW_ID IS NULL THEN A.REGISTERDATE ELSE M.REGISTERDATE END  AS VARCHAR(10) )                 AS REGISTERDATE 
       ,CAST(M.DEP_BEGIN_DATE    AS VARCHAR(10)   )    AS DEP_BEGIN_DATE 
       ,CAST(M.DEP_END_DATE      AS VARCHAR(10)   )    AS DEP_END_DATE 
       ,CAST(M.DEP_BAL           AS DECIMAL(24,6) )    AS DEP_BAL 
       ,CAST(M.DEP_ACCT_NO       AS VARCHAR(64)   )    AS DEP_ACCT_NO 
       ,CAST(M.DEP_TYPE          AS VARCHAR(2)    )    AS DEP_TYPE 
       ,CAST(M.CRE_AMOUNT        AS DECIMAL(24,6) )    AS CRE_AMOUNT 
       ,CAST(M.CRE_BEGIN_DATE    AS VARCHAR(10)   )    AS CRE_BEGIN_DATE 
       ,CAST(M.CRE_END_DATE      AS VARCHAR(10)   )    AS CRE_END_DATE 
       ,CAST(M.CRE_BAL           AS DECIMAL(24,6) )    AS CRE_BAL 
       ,CAST(M.CRE_ACCT_NO       AS VARCHAR(64)   )    AS CRE_ACCT_NO 
       ,CAST(B.START_DATE        AS VARCHAR(10)   )    AS FIN_BEGIN_DATE 
       ,CAST(B.END_DATE          AS VARCHAR(10)   )    AS FIN_END_DATE 
       ,CAST(B.CURRE_AMOUNT      AS DECIMAL(24,6) )    AS FIN_BAL 
       ,CAST(B.ACCOUNT           AS VARCHAR(64)   )    AS FIN_ACCT_NO 
       ,CAST(M.MOBILE_OPEN_DATE  AS VARCHAR(10)   )    AS MOBILE_OPEN_DATE 
       ,CAST(M.MOBILE_NUM        AS INTEGER       )    AS MOBILE_NUM 
       ,CAST(M.MOBILE_BAL        AS DECIMAL(24,6) )    AS MOBILE_BAL 
       ,CAST(M.EBANK_OPEN_DATE   AS VARCHAR(10)   )    AS EBANK_OPEN_DATE 
       ,CAST(M.EBANK_NUM         AS INTEGER       )    AS EBANK_NUM 
       ,CAST(M.EBANK_BAL         AS DECIMAL(24,6) )    AS EBANK_BAL 
       ,CAST(M.POS_OPEN_DATE     AS VARCHAR(10)   )    AS POS_OPEN_DATE 
       ,CAST(M.POS_NUM           AS INTEGER       )    AS POS_NUM 
       ,CAST(M.POS_BAL           AS DECIMAL(24,6) )    AS POS_BAL 
       ,CAST(M.BMT_OPEN_DATE     AS VARCHAR(10)   )    AS BMT_OPEN_DATE 
       ,CAST(M.BMT_NUM           AS INTEGER       )    AS BMT_NUM 
       ,CAST(M.BMT_BAL           AS DECIMAL(24,6) )    AS BMT_BAL 
       ,CAST(M.FR_ID             AS VARCHAR(5)    )    AS FR_ID 
       ,CAST(M.REPORT_DATE       AS VARCHAR(10)   )    AS REPORT_DATE 
   FROM MCRM_MID_SUN_IND_INFO A                                --
  INNER JOIN 
 (SELECT  ROW_NUMBER() OVER(PARTITION BY B.CUST_ID ORDER BY B.START_DATE ASC) AS ROW_ID,
                     B.START_DATE AS START_DATE,
					 B.CUST_ID AS CUST_ID,
                     B.END_DATE AS END_DATE,
                     NVL(B.CURRE_AMOUNT,0) AS CURRE_AMOUNT,
                     NVL(B.ACCOUNT ,0) AS ACCOUNT
                FROM ACRM_F_NI_FINANCING B WHERE END_DATE > V_DT
) B                                                 --
     ON A.CUST_ID               = B.CUST_ID 
   LEFT JOIN MCRM_SUN_IND_INFO_DETAIL M                        --
     ON B.CUST_ID               = M.CUST_ID 
    AND M.ROW_ID                = B.ROW_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1 = sqlContext.sql(sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.registerTempTable("MCRM_SUN_IND_INFO_DETAIL_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.ROW_ID                                             --:src.ROW_ID
       ,DST.CUST_ID                                            --:src.CUST_ID
       ,DST.CUST_NAME                                          --:src.CUST_NAME
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.ORG_NAME                                           --:src.ORG_NAME
       ,DST.CERT_ID                                            --:src.CERT_ID
       ,DST.TEMPSAVEFLAG                                       --:src.TEMPSAVEFLAG
       ,DST.APPROVEDATE                                        --:src.APPROVEDATE
       ,DST.REGISTERDATE                                       --:src.REGISTERDATE
       ,DST.DEP_BEGIN_DATE                                     --:src.DEP_BEGIN_DATE
       ,DST.DEP_END_DATE                                       --:src.DEP_END_DATE
       ,DST.DEP_BAL                                            --:src.DEP_BAL
       ,DST.DEP_ACCT_NO                                        --:src.DEP_ACCT_NO
       ,DST.DEP_TYPE                                           --:src.DEP_TYPE
       ,DST.CRE_AMOUNT                                         --:src.CRE_AMOUNT
       ,DST.CRE_BEGIN_DATE                                     --:src.CRE_BEGIN_DATE
       ,DST.CRE_END_DATE                                       --:src.CRE_END_DATE
       ,DST.CRE_BAL                                            --:src.CRE_BAL
       ,DST.CRE_ACCT_NO                                        --:src.CRE_ACCT_NO
       ,DST.FIN_BEGIN_DATE                                     --:src.FIN_BEGIN_DATE
       ,DST.FIN_END_DATE                                       --:src.FIN_END_DATE
       ,DST.FIN_BAL                                            --:src.FIN_BAL
       ,DST.FIN_ACCT_NO                                        --:src.FIN_ACCT_NO
       ,DST.MOBILE_OPEN_DATE                                   --:src.MOBILE_OPEN_DATE
       ,DST.MOBILE_NUM                                         --:src.MOBILE_NUM
       ,DST.MOBILE_BAL                                         --:src.MOBILE_BAL
       ,DST.EBANK_OPEN_DATE                                    --:src.EBANK_OPEN_DATE
       ,DST.EBANK_NUM                                          --:src.EBANK_NUM
       ,DST.EBANK_BAL                                          --:src.EBANK_BAL
       ,DST.POS_OPEN_DATE                                      --:src.POS_OPEN_DATE
       ,DST.POS_NUM                                            --:src.POS_NUM
       ,DST.POS_BAL                                            --:src.POS_BAL
       ,DST.BMT_OPEN_DATE                                      --:src.BMT_OPEN_DATE
       ,DST.BMT_NUM                                            --:src.BMT_NUM
       ,DST.BMT_BAL                                            --:src.BMT_BAL
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.REPORT_DATE                                        --:src.REPORT_DATE
   FROM MCRM_SUN_IND_INFO_DETAIL DST 
   LEFT JOIN MCRM_SUN_IND_INFO_DETAIL_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.ROW_ID              = DST.ROW_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP2 = sqlContext.sql(sql)
dfn="MCRM_SUN_IND_INFO_DETAIL/"+V_DT+".parquet"
UNION=MCRM_SUN_IND_INFO_DETAIL_INNTMP2.unionAll(MCRM_SUN_IND_INFO_DETAIL_INNTMP1)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.cache()
MCRM_SUN_IND_INFO_DETAIL_INNTMP2.cache()
nrowsi = MCRM_SUN_IND_INFO_DETAIL_INNTMP1.count()
nrowsa = MCRM_SUN_IND_INFO_DETAIL_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.unpersist()
MCRM_SUN_IND_INFO_DETAIL_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_SUN_IND_INFO_DETAIL lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL/"+V_DT_LD+".parquet /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL_BK/")

#任务[12] 001-07::
V_STEP = V_STEP + 1
MCRM_SUN_IND_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/MCRM_SUN_IND_INFO_DETAIL/*')
MCRM_SUN_IND_INFO_DETAIL.registerTempTable("MCRM_SUN_IND_INFO_DETAIL")
sql = """
 SELECT CAST(M.ID            AS BIGINT)        AS ID 
       ,CAST(M.ROW_ID            AS INTEGER      )    AS ROW_ID 
       ,CAST(A.CUST_ID           AS VARCHAR(20)  )    AS CUST_ID 
       ,CAST(M.CUST_NAME         AS VARCHAR(50)  )    AS CUST_NAME 
       ,CAST(M.ORG_ID            AS VARCHAR(10)  )    AS ORG_ID 
       ,CAST(M.ORG_NAME          AS VARCHAR(64)  )    AS ORG_NAME 
       ,CAST(M.CERT_ID           AS VARCHAR(64)  )    AS CERT_ID 
       ,CAST(M.TEMPSAVEFLAG      AS VARCHAR(2)   )    AS TEMPSAVEFLAG 
       ,CAST(M.APPROVEDATE       AS VARCHAR(10)  )    AS APPROVEDATE 
       ,CAST(M.REGISTERDATE      AS VARCHAR(10)  )    AS REGISTERDATE 
       ,CAST(M.DEP_BEGIN_DATE    AS VARCHAR(10)  )    AS DEP_BEGIN_DATE 
       ,CAST(M.DEP_END_DATE      AS VARCHAR(10)  )    AS DEP_END_DATE 
       ,CAST(M.DEP_BAL           AS DECIMAL(24,6))    AS DEP_BAL 
       ,CAST(M.DEP_ACCT_NO       AS VARCHAR(64)  )    AS DEP_ACCT_NO 
       ,CAST(M.DEP_TYPE          AS VARCHAR(2)   )    AS DEP_TYPE 
       ,CAST(M.CRE_AMOUNT        AS DECIMAL(24,6))    AS CRE_AMOUNT 
       ,CAST(M.CRE_BEGIN_DATE    AS VARCHAR(10)  )    AS CRE_BEGIN_DATE 
       ,CAST(M.CRE_END_DATE      AS VARCHAR(10)  )    AS CRE_END_DATE 
       ,CAST(M.CRE_BAL           AS DECIMAL(24,6))    AS CRE_BAL 
       ,CAST(M.CRE_ACCT_NO       AS VARCHAR(64)  )    AS CRE_ACCT_NO 
       ,CAST(M.FIN_BEGIN_DATE    AS VARCHAR(10)  )    AS FIN_BEGIN_DATE 
       ,CAST(M.FIN_END_DATE      AS VARCHAR(10)  )    AS FIN_END_DATE 
       ,CAST(M.FIN_BAL           AS DECIMAL(24,6))    AS FIN_BAL 
       ,CAST(M.FIN_ACCT_NO       AS VARCHAR(64)  )    AS FIN_ACCT_NO 
       ,CAST(CASE WHEN B.ODS_SYS_ID            = 'MBK' THEN MIN(CONCAT(SUBSTR(C.OPENTIME, 1, 4),'-',SUBSTR(C.OPENTIME, 5, 2),'-',SUBSTR(C.OPENTIME, 7, 2))) END   AS  VARCHAR(10))                 AS MOBILE_OPEN_DATE 
       ,CAST(CASE WHEN B.ODS_SYS_ID            = 'MBK' THEN NVL(D.BNSJYHBS, 0) END                AS  INTEGER    )    AS MOBILE_NUM 
       ,CAST(M.MOBILE_BAL   AS DECIMAL(24,6)  )       AS MOBILE_BAL 
       ,CAST(CASE WHEN B.ODS_SYS_ID            = 'IBK' THEN MIN(CONCAT(SUBSTR(C.OPENTIME, 1, 4),'-',SUBSTR(C.OPENTIME, 5, 2),'-',SUBSTR(C.OPENTIME, 7, 2)))
END   AS VARCHAR(10) )                 AS EBANK_OPEN_DATE 
       ,CAST(CASE WHEN B.ODS_SYS_ID            = 'IBK' THEN NVL(D.BNSJYHBS, 0) END                AS INTEGER     )    AS EBANK_NUM 
       ,CAST(M.EBANK_BAL         AS DECIMAL(24,6) )  AS EBANK_BAL 
       ,CAST(M.POS_OPEN_DATE     AS VARCHAR(10)   )  AS POS_OPEN_DATE 
       ,CAST(M.POS_NUM           AS INTEGER       )  AS POS_NUM 
       ,CAST(M.POS_BAL           AS DECIMAL(24,6) )  AS POS_BAL 
       ,CAST(M.BMT_OPEN_DATE     AS VARCHAR(10)   )  AS BMT_OPEN_DATE 
       ,CAST(M.BMT_NUM           AS INTEGER       )  AS BMT_NUM 
       ,CAST(M.BMT_BAL           AS DECIMAL(24,6) )  AS BMT_BAL 
       ,CAST(M.FR_ID             AS VARCHAR(5)    )  AS FR_ID 
       ,CAST(M.REPORT_DATE       AS VARCHAR(10)   )  AS REPORT_DATE 
   FROM MCRM_SUN_IND_INFO_DETAIL M                             --
  INNER JOIN MCRM_MID_SUN_IND_INFO A                           --
     ON M.CUST_ID               = A.CUST_ID 
  INNER JOIN OCRM_F_CI_SYS_RESOURCE B                          --
     ON A.CUST_ID               = B.ODS_CUST_ID 
    AND B.ODS_SYS_ID            = 'MBK' 
   LEFT JOIN F_TX_WSYH_ECCIFMCH C                              --
     ON B.SOURCE_CUST_ID        = C.CIFSEQ 
    AND B.FR_ID                 = C.FR_ID 
   LEFT JOIN ACRM_A_PROD_CHANNEL_INFO D                        --
     ON A.CUST_ID               = D.CUST_ID 
    AND D.FR_ID                 = C.FR_ID 
  WHERE M.ROW_ID                = 1 
  GROUP BY CAST(M.ID            AS BIGINT)  
       ,CAST(M.ROW_ID            AS INTEGER      )   
       ,CAST(A.CUST_ID           AS VARCHAR(20)  )   
       ,CAST(M.CUST_NAME         AS VARCHAR(50)  )   
       ,CAST(M.ORG_ID            AS VARCHAR(10)  )   
       ,CAST(M.ORG_NAME          AS VARCHAR(64)  )   
       ,CAST(M.CERT_ID           AS VARCHAR(64)  )   
       ,CAST(M.TEMPSAVEFLAG      AS VARCHAR(2)   )   
       ,CAST(M.APPROVEDATE       AS VARCHAR(10)  )   
       ,CAST(M.REGISTERDATE      AS VARCHAR(10)  )   
       ,CAST(M.DEP_BEGIN_DATE    AS VARCHAR(10)  )   
       ,CAST(M.DEP_END_DATE      AS VARCHAR(10)  )   
       ,CAST(M.DEP_BAL           AS DECIMAL(24,6))   
       ,CAST(M.DEP_ACCT_NO       AS VARCHAR(64)  )   
       ,CAST(M.DEP_TYPE          AS VARCHAR(2)   )   
       ,CAST(M.CRE_AMOUNT        AS DECIMAL(24,6))   
       ,CAST(M.CRE_BEGIN_DATE    AS VARCHAR(10)  )   
       ,CAST(M.CRE_END_DATE      AS VARCHAR(10)  )   
       ,CAST(M.CRE_BAL           AS DECIMAL(24,6))   
       ,CAST(M.CRE_ACCT_NO       AS VARCHAR(64)  )   
       ,CAST(M.FIN_BEGIN_DATE    AS VARCHAR(10)  )   
       ,CAST(M.FIN_END_DATE      AS VARCHAR(10)  )   
       ,CAST(M.FIN_BAL           AS DECIMAL(24,6))   
       ,CAST(M.FIN_ACCT_NO       AS VARCHAR(64)  )   
       ,CAST(CASE WHEN B.ODS_SYS_ID            = 'MBK' THEN NVL(D.BNSJYHBS, 0) END                AS  INTEGER    )    
       ,CAST(M.MOBILE_BAL   AS DECIMAL(24,6)  )  
       ,CAST(CASE WHEN B.ODS_SYS_ID            = 'IBK' THEN NVL(D.BNSJYHBS, 0) END                AS INTEGER     )   
       ,CAST(M.EBANK_BAL         AS DECIMAL(24,6) ) 
       ,CAST(M.POS_OPEN_DATE     AS VARCHAR(10)   ) 
       ,CAST(M.POS_NUM           AS INTEGER       ) 
       ,CAST(M.POS_BAL           AS DECIMAL(24,6) ) 
       ,CAST(M.BMT_OPEN_DATE     AS VARCHAR(10)   ) 
       ,CAST(M.BMT_NUM           AS INTEGER       ) 
       ,CAST(M.BMT_BAL           AS DECIMAL(24,6) ) 
       ,CAST(M.FR_ID             AS VARCHAR(5)    ) 
       ,CAST(M.REPORT_DATE       AS VARCHAR(10)   ) 
	   ,B.ODS_SYS_ID"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1 = sqlContext.sql(sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.registerTempTable("MCRM_SUN_IND_INFO_DETAIL_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.ROW_ID                                             --:src.ROW_ID
       ,DST.CUST_ID                                            --:src.CUST_ID
       ,DST.CUST_NAME                                          --:src.CUST_NAME
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.ORG_NAME                                           --:src.ORG_NAME
       ,DST.CERT_ID                                            --:src.CERT_ID
       ,DST.TEMPSAVEFLAG                                       --:src.TEMPSAVEFLAG
       ,DST.APPROVEDATE                                        --:src.APPROVEDATE
       ,DST.REGISTERDATE                                       --:src.REGISTERDATE
       ,DST.DEP_BEGIN_DATE                                     --:src.DEP_BEGIN_DATE
       ,DST.DEP_END_DATE                                       --:src.DEP_END_DATE
       ,DST.DEP_BAL                                            --:src.DEP_BAL
       ,DST.DEP_ACCT_NO                                        --:src.DEP_ACCT_NO
       ,DST.DEP_TYPE                                           --:src.DEP_TYPE
       ,DST.CRE_AMOUNT                                         --:src.CRE_AMOUNT
       ,DST.CRE_BEGIN_DATE                                     --:src.CRE_BEGIN_DATE
       ,DST.CRE_END_DATE                                       --:src.CRE_END_DATE
       ,DST.CRE_BAL                                            --:src.CRE_BAL
       ,DST.CRE_ACCT_NO                                        --:src.CRE_ACCT_NO
       ,DST.FIN_BEGIN_DATE                                     --:src.FIN_BEGIN_DATE
       ,DST.FIN_END_DATE                                       --:src.FIN_END_DATE
       ,DST.FIN_BAL                                            --:src.FIN_BAL
       ,DST.FIN_ACCT_NO                                        --:src.FIN_ACCT_NO
       ,DST.MOBILE_OPEN_DATE                                   --:src.MOBILE_OPEN_DATE
       ,DST.MOBILE_NUM                                         --:src.MOBILE_NUM
       ,DST.MOBILE_BAL                                         --:src.MOBILE_BAL
       ,DST.EBANK_OPEN_DATE                                    --:src.EBANK_OPEN_DATE
       ,DST.EBANK_NUM                                          --:src.EBANK_NUM
       ,DST.EBANK_BAL                                          --:src.EBANK_BAL
       ,DST.POS_OPEN_DATE                                      --:src.POS_OPEN_DATE
       ,DST.POS_NUM                                            --:src.POS_NUM
       ,DST.POS_BAL                                            --:src.POS_BAL
       ,DST.BMT_OPEN_DATE                                      --:src.BMT_OPEN_DATE
       ,DST.BMT_NUM                                            --:src.BMT_NUM
       ,DST.BMT_BAL                                            --:src.BMT_BAL
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.REPORT_DATE                                        --:src.REPORT_DATE
   FROM MCRM_SUN_IND_INFO_DETAIL DST 
   LEFT JOIN MCRM_SUN_IND_INFO_DETAIL_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.ROW_ID              = DST.ROW_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP2 = sqlContext.sql(sql)
dfn="MCRM_SUN_IND_INFO_DETAIL/"+V_DT+".parquet"
UNION=MCRM_SUN_IND_INFO_DETAIL_INNTMP2.unionAll(MCRM_SUN_IND_INFO_DETAIL_INNTMP1)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.cache()
MCRM_SUN_IND_INFO_DETAIL_INNTMP2.cache()
nrowsi = MCRM_SUN_IND_INFO_DETAIL_INNTMP1.count()
nrowsa = MCRM_SUN_IND_INFO_DETAIL_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.unpersist()
MCRM_SUN_IND_INFO_DETAIL_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_SUN_IND_INFO_DETAIL lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL/"+V_DT_LD+".parquet /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL_BK/")

#任务[12] 001-08::
V_STEP = V_STEP + 1
MCRM_SUN_IND_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/MCRM_SUN_IND_INFO_DETAIL/*')
MCRM_SUN_IND_INFO_DETAIL.registerTempTable("MCRM_SUN_IND_INFO_DETAIL")
sql = """
 SELECT CAST(M.ID       AS BIGINT)             AS ID 
       ,CAST(M.ROW_ID              AS INTEGER       ) AS ROW_ID 
       ,CAST(M.CUST_ID             AS VARCHAR(20)   ) AS CUST_ID 
       ,CAST(M.CUST_NAME           AS VARCHAR(50)   ) AS CUST_NAME 
       ,CAST(M.ORG_ID              AS VARCHAR(10)   ) AS ORG_ID 
       ,CAST(M.ORG_NAME            AS VARCHAR(64)   ) AS ORG_NAME 
       ,CAST(M.CERT_ID             AS VARCHAR(64)   ) AS CERT_ID 
       ,CAST(M.TEMPSAVEFLAG        AS VARCHAR(2)    ) AS TEMPSAVEFLAG 
       ,CAST(M.APPROVEDATE         AS VARCHAR(10)   ) AS APPROVEDATE 
       ,CAST(M.REGISTERDATE        AS VARCHAR(10)   ) AS REGISTERDATE 
       ,CAST(M.DEP_BEGIN_DATE      AS VARCHAR(10)   ) AS DEP_BEGIN_DATE 
       ,CAST(M.DEP_END_DATE        AS VARCHAR(10)   ) AS DEP_END_DATE 
       ,CAST(M.DEP_BAL             AS DECIMAL(24,6) ) AS DEP_BAL 
       ,CAST(M.DEP_ACCT_NO         AS VARCHAR(64)   ) AS DEP_ACCT_NO 
       ,CAST(M.DEP_TYPE            AS VARCHAR(2)    ) AS DEP_TYPE 
       ,CAST(M.CRE_AMOUNT          AS DECIMAL(24,6) ) AS CRE_AMOUNT 
       ,CAST(M.CRE_BEGIN_DATE      AS VARCHAR(10)   ) AS CRE_BEGIN_DATE 
       ,CAST(M.CRE_END_DATE        AS VARCHAR(10)   ) AS CRE_END_DATE 
       ,CAST(M.CRE_BAL             AS DECIMAL(24,6) ) AS CRE_BAL 
       ,CAST(M.CRE_ACCT_NO         AS VARCHAR(64)   ) AS CRE_ACCT_NO 
       ,CAST(M.FIN_BEGIN_DATE      AS VARCHAR(10)   ) AS FIN_BEGIN_DATE 
       ,CAST(M.FIN_END_DATE        AS VARCHAR(10)   ) AS FIN_END_DATE 
       ,CAST(M.FIN_BAL             AS DECIMAL(24,6) ) AS FIN_BAL 
       ,CAST(M.FIN_ACCT_NO         AS VARCHAR(64)   ) AS FIN_ACCT_NO 
       ,CAST(M.MOBILE_OPEN_DATE    AS VARCHAR(10)   ) AS MOBILE_OPEN_DATE 
       ,CAST(N.MOBILE_NUM     AS INTEGER  )             AS MOBILE_NUM 
       ,CAST(M.MOBILE_BAL         AS DECIMAL(24,6))   AS MOBILE_BAL 
       ,CAST(M.EBANK_OPEN_DATE    AS VARCHAR(10)  )   AS EBANK_OPEN_DATE 
       ,CAST(N.EBANK_NUM    AS INTEGER)              AS EBANK_NUM 
       ,CAST(M.EBANK_BAL        AS DECIMAL(24,6))     AS EBANK_BAL 
       ,CAST(M.POS_OPEN_DATE    AS VARCHAR(10)  )     AS POS_OPEN_DATE 
       ,CAST(N.POS_NUM          AS INTEGER       )  AS POS_NUM 
       ,CAST(N.POS_BAL    AS DECIMAL(24,6) )                AS POS_BAL 
       ,CAST(M.BMT_OPEN_DATE   AS VARCHAR(10)    )    AS BMT_OPEN_DATE 
       ,CAST(M.BMT_NUM         AS INTEGER        )    AS BMT_NUM 
       ,CAST(M.BMT_BAL         AS DECIMAL(24,6)  )    AS BMT_BAL 
       ,CAST(M.FR_ID           AS VARCHAR(5)     )    AS FR_ID 
       ,CAST(M.REPORT_DATE     AS VARCHAR(10)    )    AS REPORT_DATE 
   FROM MCRM_SUN_IND_INFO_DETAIL M                             --
   INNER JOIN (
		SELECT  A.CUST_ID,
					 SUM(CASE WHEN B.CHANNEL_FLAG = '02' THEN B.SA_TVARCHAR_AMT  END) AS EBANK_NUM,
					 SUM(CASE WHEN B.CHANNEL_FLAG = '03' THEN B.SA_TVARCHAR_AMT  END) AS MOBILE_NUM,
					 COUNT(1) AS POS_NUM,
                     SUM(CASE WHEN B.CHANNEL_FLAG = '07' THEN  NVL(B.SA_TVARCHAR_AMT,0)  END) AS POS_BAL
               FROM  MCRM_MID_SUN_IND_INFO A
               JOIN  ACRM_F_CI_NIN_TRANSLOG B ON A.CUST_ID = B.CUST_ID
              WHERE  B.SA_TVARCHAR_DT >= TRUNC(DATE(V_DT), 'YY') 
					AND B.SA_TVARCHAR_DT <= V_DT 
					AND B.CHANNEL_FLAG IN('02', '03', '07') 
              GROUP BY A.CUST_ID 
   )N
	ON M.CUST_ID = N.CUST_ID 
	WHERE M.ROW_ID                = 1 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1 = sqlContext.sql(sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.registerTempTable("MCRM_SUN_IND_INFO_DETAIL_INNTMP1")

sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.ROW_ID                                             --:src.ROW_ID
       ,DST.CUST_ID                                            --:src.CUST_ID
       ,DST.CUST_NAME                                          --:src.CUST_NAME
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.ORG_NAME                                           --:src.ORG_NAME
       ,DST.CERT_ID                                            --:src.CERT_ID
       ,DST.TEMPSAVEFLAG                                       --:src.TEMPSAVEFLAG
       ,DST.APPROVEDATE                                        --:src.APPROVEDATE
       ,DST.REGISTERDATE                                       --:src.REGISTERDATE
       ,DST.DEP_BEGIN_DATE                                     --:src.DEP_BEGIN_DATE
       ,DST.DEP_END_DATE                                       --:src.DEP_END_DATE
       ,DST.DEP_BAL                                            --:src.DEP_BAL
       ,DST.DEP_ACCT_NO                                        --:src.DEP_ACCT_NO
       ,DST.DEP_TYPE                                           --:src.DEP_TYPE
       ,DST.CRE_AMOUNT                                         --:src.CRE_AMOUNT
       ,DST.CRE_BEGIN_DATE                                     --:src.CRE_BEGIN_DATE
       ,DST.CRE_END_DATE                                       --:src.CRE_END_DATE
       ,DST.CRE_BAL                                            --:src.CRE_BAL
       ,DST.CRE_ACCT_NO                                        --:src.CRE_ACCT_NO
       ,DST.FIN_BEGIN_DATE                                     --:src.FIN_BEGIN_DATE
       ,DST.FIN_END_DATE                                       --:src.FIN_END_DATE
       ,DST.FIN_BAL                                            --:src.FIN_BAL
       ,DST.FIN_ACCT_NO                                        --:src.FIN_ACCT_NO
       ,DST.MOBILE_OPEN_DATE                                   --:src.MOBILE_OPEN_DATE
       ,DST.MOBILE_NUM                                         --:src.MOBILE_NUM
       ,DST.MOBILE_BAL                                         --:src.MOBILE_BAL
       ,DST.EBANK_OPEN_DATE                                    --:src.EBANK_OPEN_DATE
       ,DST.EBANK_NUM                                          --:src.EBANK_NUM
       ,DST.EBANK_BAL                                          --:src.EBANK_BAL
       ,DST.POS_OPEN_DATE                                      --:src.POS_OPEN_DATE
       ,DST.POS_NUM                                            --:src.POS_NUM
       ,DST.POS_BAL                                            --:src.POS_BAL
       ,DST.BMT_OPEN_DATE                                      --:src.BMT_OPEN_DATE
       ,DST.BMT_NUM                                            --:src.BMT_NUM
       ,DST.BMT_BAL                                            --:src.BMT_BAL
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.REPORT_DATE                                        --:src.REPORT_DATE
   FROM MCRM_SUN_IND_INFO_DETAIL DST 
   LEFT JOIN MCRM_SUN_IND_INFO_DETAIL_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.ROW_ID              = DST.ROW_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP2 = sqlContext.sql(sql)
dfn="MCRM_SUN_IND_INFO_DETAIL/"+V_DT+".parquet"
UNION=MCRM_SUN_IND_INFO_DETAIL_INNTMP2.unionAll(MCRM_SUN_IND_INFO_DETAIL_INNTMP1)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.cache()
MCRM_SUN_IND_INFO_DETAIL_INNTMP2.cache()
nrowsi = MCRM_SUN_IND_INFO_DETAIL_INNTMP1.count()
nrowsa = MCRM_SUN_IND_INFO_DETAIL_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.unpersist()
MCRM_SUN_IND_INFO_DETAIL_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_SUN_IND_INFO_DETAIL lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL/"+V_DT_LD+".parquet /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL_BK/")

#任务[12] 001-09::
V_STEP = V_STEP + 1
MCRM_SUN_IND_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/MCRM_SUN_IND_INFO_DETAIL/*')
MCRM_SUN_IND_INFO_DETAIL.registerTempTable("MCRM_SUN_IND_INFO_DETAIL")
sql = """
 SELECT CAST(M.ID       AS BIGINT)             AS ID 
       ,CAST(M.ROW_ID              AS INTEGER       )  AS ROW_ID 
       ,CAST(M.CUST_ID             AS VARCHAR(20)   )  AS CUST_ID 
       ,CAST(M.CUST_NAME           AS VARCHAR(50)   )  AS CUST_NAME 
       ,CAST(M.ORG_ID              AS VARCHAR(10)   )  AS ORG_ID 
       ,CAST(M.ORG_NAME            AS VARCHAR(64)   )  AS ORG_NAME 
       ,CAST(M.CERT_ID             AS VARCHAR(64)   )  AS CERT_ID 
       ,CAST(M.TEMPSAVEFLAG        AS VARCHAR(2)    )  AS TEMPSAVEFLAG 
       ,CAST(M.APPROVEDATE         AS VARCHAR(10)   )  AS APPROVEDATE 
       ,CAST(M.REGISTERDATE        AS VARCHAR(10)   )  AS REGISTERDATE 
       ,CAST(M.DEP_BEGIN_DATE      AS VARCHAR(10)   )  AS DEP_BEGIN_DATE 
       ,CAST(M.DEP_END_DATE        AS VARCHAR(10)   )  AS DEP_END_DATE 
       ,CAST(M.DEP_BAL             AS DECIMAL(24,6) )  AS DEP_BAL 
       ,CAST(M.DEP_ACCT_NO         AS VARCHAR(64)   )  AS DEP_ACCT_NO 
       ,CAST(M.DEP_TYPE            AS VARCHAR(2)    )  AS DEP_TYPE 
       ,CAST(M.CRE_AMOUNT          AS DECIMAL(24,6) )  AS CRE_AMOUNT 
       ,CAST(M.CRE_BEGIN_DATE      AS VARCHAR(10)   )  AS CRE_BEGIN_DATE 
       ,CAST(M.CRE_END_DATE        AS VARCHAR(10)   )  AS CRE_END_DATE 
       ,CAST(M.CRE_BAL             AS DECIMAL(24,6) )  AS CRE_BAL 
       ,CAST(M.CRE_ACCT_NO         AS VARCHAR(64)   )  AS CRE_ACCT_NO 
       ,CAST(M.FIN_BEGIN_DATE      AS VARCHAR(10)   )  AS FIN_BEGIN_DATE 
       ,CAST(M.FIN_END_DATE        AS VARCHAR(10)   )  AS FIN_END_DATE 
       ,CAST(M.FIN_BAL             AS DECIMAL(24,6) )  AS FIN_BAL 
       ,CAST(M.FIN_ACCT_NO         AS VARCHAR(64)   )  AS FIN_ACCT_NO 
       ,CAST(M.MOBILE_OPEN_DATE    AS VARCHAR(10)   )  AS MOBILE_OPEN_DATE 
       ,CAST(M.MOBILE_NUM       AS INTEGER)  AS MOBILE_NUM 
       ,CAST(M.MOBILE_BAL       AS DECIMAL(24,6))     AS MOBILE_BAL 
       ,CAST(M.EBANK_OPEN_DATE  AS VARCHAR(10)  )     AS EBANK_OPEN_DATE 
       ,CAST(M.EBANK_NUM     AS INTEGER)    AS EBANK_NUM 
       ,CAST(M.EBANK_BAL       AS DECIMAL(24,6) )     AS EBANK_BAL 
       ,CAST(M.POS_OPEN_DATE   AS VARCHAR(10)   )     AS POS_OPEN_DATE 
       ,CAST(M.POS_NUM               AS INTEGER       )   AS POS_NUM 
       ,CAST(M.POS_BAL  AS DECIMAL(24,6) )    AS POS_BAL 
       ,CAST(N.OPEN_DATE           AS VARCHAR(10)  )  AS BMT_OPEN_DATE 
       ,CAST(N.BMT_NUM  AS INTEGER      )                     AS BMT_NUM 
       ,CAST(N.BMT_BAL       AS DECIMAL(24,6))              AS BMT_BAL 
       ,CAST(M.FR_ID               AS VARCHAR(5)   )  AS FR_ID 
       ,CAST(M.REPORT_DATE         AS VARCHAR(10)  )  AS REPORT_DATE 
   FROM MCRM_SUN_IND_INFO_DETAIL M                             --
   INNER JOIN (
		SELECT A.FR_ID,A.ODS_CUST_ID AS CUST_ID,B.SIGN_DATE AS OPEN_DATE,COUNT(ODS_CUST_ID) AS BMT_NUM,SUM(AMOUNT) AS BMT_BAL
                FROM OCRM_F_CI_SYS_RESOURCE  A
                JOIN F_CI_ZDH_ZZDH_SHOP B ON A.CERT_NO = B.BUS_AREA AND B.FR_ID = A.FR_ID
                JOIN F_TX_ZDH_ZZDH_HISTORY_LS C ON B.SHOP_NO = C.MERCHANT_ID 
				AND SUBSTR(C.HOST_DATE,1,4) = YEAR(V_DT) 
				AND C.FR_ID = A.FR_ID
              WHERE A.ODS_SYS_ID = 'SLNA'
              GROUP BY   A.ODS_CUST_ID,B.SIGN_DATE,A.FR_ID
   )N
  ON M.CUST_ID = N.CUST_ID
  WHERE M.ROW_ID                = 1 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1 = sqlContext.sql(sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.registerTempTable("MCRM_SUN_IND_INFO_DETAIL_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.ROW_ID                                             --:src.ROW_ID
       ,DST.CUST_ID                                            --:src.CUST_ID
       ,DST.CUST_NAME                                          --:src.CUST_NAME
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.ORG_NAME                                           --:src.ORG_NAME
       ,DST.CERT_ID                                            --:src.CERT_ID
       ,DST.TEMPSAVEFLAG                                       --:src.TEMPSAVEFLAG
       ,DST.APPROVEDATE                                        --:src.APPROVEDATE
       ,DST.REGISTERDATE                                       --:src.REGISTERDATE
       ,DST.DEP_BEGIN_DATE                                     --:src.DEP_BEGIN_DATE
       ,DST.DEP_END_DATE                                       --:src.DEP_END_DATE
       ,DST.DEP_BAL                                            --:src.DEP_BAL
       ,DST.DEP_ACCT_NO                                        --:src.DEP_ACCT_NO
       ,DST.DEP_TYPE                                           --:src.DEP_TYPE
       ,DST.CRE_AMOUNT                                         --:src.CRE_AMOUNT
       ,DST.CRE_BEGIN_DATE                                     --:src.CRE_BEGIN_DATE
       ,DST.CRE_END_DATE                                       --:src.CRE_END_DATE
       ,DST.CRE_BAL                                            --:src.CRE_BAL
       ,DST.CRE_ACCT_NO                                        --:src.CRE_ACCT_NO
       ,DST.FIN_BEGIN_DATE                                     --:src.FIN_BEGIN_DATE
       ,DST.FIN_END_DATE                                       --:src.FIN_END_DATE
       ,DST.FIN_BAL                                            --:src.FIN_BAL
       ,DST.FIN_ACCT_NO                                        --:src.FIN_ACCT_NO
       ,DST.MOBILE_OPEN_DATE                                   --:src.MOBILE_OPEN_DATE
       ,DST.MOBILE_NUM                                         --:src.MOBILE_NUM
       ,DST.MOBILE_BAL                                         --:src.MOBILE_BAL
       ,DST.EBANK_OPEN_DATE                                    --:src.EBANK_OPEN_DATE
       ,DST.EBANK_NUM                                          --:src.EBANK_NUM
       ,DST.EBANK_BAL                                          --:src.EBANK_BAL
       ,DST.POS_OPEN_DATE                                      --:src.POS_OPEN_DATE
       ,DST.POS_NUM                                            --:src.POS_NUM
       ,DST.POS_BAL                                            --:src.POS_BAL
       ,DST.BMT_OPEN_DATE                                      --:src.BMT_OPEN_DATE
       ,DST.BMT_NUM                                            --:src.BMT_NUM
       ,DST.BMT_BAL                                            --:src.BMT_BAL
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.REPORT_DATE                                        --:src.REPORT_DATE
   FROM MCRM_SUN_IND_INFO_DETAIL DST 
   LEFT JOIN MCRM_SUN_IND_INFO_DETAIL_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.ROW_ID              = DST.ROW_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_SUN_IND_INFO_DETAIL_INNTMP2 = sqlContext.sql(sql)
dfn="MCRM_SUN_IND_INFO_DETAIL/"+V_DT+".parquet"
UNION=MCRM_SUN_IND_INFO_DETAIL_INNTMP2.unionAll(MCRM_SUN_IND_INFO_DETAIL_INNTMP1)
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.cache()
MCRM_SUN_IND_INFO_DETAIL_INNTMP2.cache()
nrowsi = MCRM_SUN_IND_INFO_DETAIL_INNTMP1.count()
nrowsa = MCRM_SUN_IND_INFO_DETAIL_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
MCRM_SUN_IND_INFO_DETAIL_INNTMP1.unpersist()
MCRM_SUN_IND_INFO_DETAIL_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_SUN_IND_INFO_DETAIL lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL/"+V_DT+".parquet /"+dbname+"/MCRM_SUN_IND_INFO_DETAIL_BK/")
