#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_NIN_TRANSLOG').setMaster(sys.argv[2])
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


#先删除原表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_NIN_TRANSLOG/"+V_DT+".parquet")


F_TX_ZDH_ZZDH_HISTORY_LS = sqlContext.read.parquet(hdfs+'/F_TX_ZDH_ZZDH_HISTORY_LS/*')
F_TX_ZDH_ZZDH_HISTORY_LS.registerTempTable("F_TX_ZDH_ZZDH_HISTORY_LS")
F_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNACN/*')
F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")
F_TX_EBPS_MAINTRANSDTL = sqlContext.read.parquet(hdfs+'/F_TX_EBPS_MAINTRANSDTL/*')
F_TX_EBPS_MAINTRANSDTL.registerTempTable("F_TX_EBPS_MAINTRANSDTL")
F_TX_AFA_MAINTRANSDTL = sqlContext.read.parquet(hdfs+'/F_TX_AFA_MAINTRANSDTL/*')
F_TX_AFA_MAINTRANSDTL.registerTempTable("F_TX_AFA_MAINTRANSDTL")
F_DP_CBOD_SAACNTXN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNTXN/*')
F_DP_CBOD_SAACNTXN.registerTempTable("F_DP_CBOD_SAACNTXN")
F_NI_CBOD_AGBXLBXL = sqlContext.read.parquet(hdfs+'/F_NI_CBOD_AGBXLBXL/*')
F_NI_CBOD_AGBXLBXL.registerTempTable("F_NI_CBOD_AGBXLBXL")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(''    AS DECIMAL(4))                AS SATVARCHARN_LL 
       ,A.FK_SAACN_KEY          AS FK_SAACN_KEY 
       ,CAST(A.SA_DDP_ACCT_NO_DET_N AS DECIMAL(7)) AS SA_DDP_ACCT_NO_DET_N 
       ,CAST(''   AS DECIMAL(15))                 AS SATVARCHARN_DB_TIMESTAMP 
       ,A.SA_CURR_COD           AS SA_CURR_COD 
       ,''                    AS SA_OPR_NO 
       ,A.SA_CURR_IDEN          AS SA_CURR_IDEN 
       ,A.SA_EC_FLG             AS SA_EC_FLG 
       ,CAST(''   AS DECIMAL(7))                 AS SA_EC_DET_NO 
       ,CAST(A.SA_CR_AMT   AS DECIMAL(15,2))          AS SA_CR_AMT 
       ,CAST(A.SA_DDP_ACCT_BAL  AS  DECIMAL(15,2))    AS SA_DDP_ACCT_BAL 
       ,CAST(A.SA_TX_AMT    AS DECIMAL(15,2))         AS SA_TVARCHAR_AMT 
       ,A.SA_TX_CRD_NO          AS SA_TVARCHAR_CRD_NO 
       ,A.SA_TX_TYP             AS SA_TVARCHAR_TYP 
       ,A.SA_TX_LOG_NO          AS SA_TVARCHAR_LOG_NO 
       ,CAST(A.SA_DR_AMT    AS DECIMAL(15,2))         AS SA_DR_AMT 
       ,''                    AS SA_DOC_NO 
       ,''                    AS SA_DOC_TYP 
       ,''                    AS SA_VAL_DT 
       ,CAST(''    AS DECIMAL(15,2))                    AS SA_SVC 
       ,''                    AS SA_AUTH_NO 
       ,''                    AS SA_CUST_DOCAG_STNO 
       ,A.SA_OPUN_COD           AS SA_OPUN_COD 
       ,''                    AS SA_DSCRP_COD 
       ,A.SA_RMRK               AS SA_RMRK 
       ,A.SA_TX_TM              AS SA_TVARCHAR_TM 
       ,A.SA_TX_DT              AS SA_TVARCHAR_DT 
       ,''                    AS SA_SYS_DT 
       ,CAST(''   AS DECIMAL(15,2))                     AS SA_DDP_PDT 
       ,''                    AS SA_APP_TVARCHAR_CODE 
       ,''                    AS SA_ETVARCHAR_FLG 
       ,''                    AS SA_OTVARCHAR_FLG 
       ,''                    AS SA_FLVARCHAR_FLG 
       ,''                    AS SA_GUIJI_FLAG 
       ,''                    AS SA_PRT_FLG 
       ,''                    AS FILLER 
       ,''                    AS SA_RMRK_1 
       ,A.SA_OP_CUST_NAME       AS SA_OP_CUST_NAME 
       ,''                    AS SA_AGT_CERT_TYP 
       ,''                    AS SA_AGT_CERT_ID 
       ,''                    AS SA_AGT_CUST_NAME 
       ,A.SA_CHANNEL_FLAG       AS SA_CHANNEL_FLAG 
       ,A.SA_BELONG_INSTN_COD   AS SA_BELONG_INSTN_COD 
       ,A.SA_PDP_CODE           AS SA_PDP_CODE 
       ,A.SA_OP_ACCT_NO_32      AS SA_OP_ACCT_NO_32 
       ,A.SA_OP_BANK_NO         AS SA_OP_BANK_NO 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,CASE WHEN C.FR_ID IS 
    NOT NULL THEN C.CHANNELCODE WHEN D.FR_ID IS 
    NOT NULL THEN '07' WHEN E.FR_ID IS 
    NOT NULL THEN E.AG_CHANNEL_FLG WHEN F.CHANNELSEQ            = 'C' THEN '03' WHEN F.FR_ID IS 
    NOT NULL 
    AND F.CHANNELSEQ <> 'C' THEN '02' ELSE COALESCE(A.SA_CHANNEL_FLAG, '99') END                     AS CHANNEL_FLAG 
       ,''                    AS CR_MCT_MCC 
       ,B.SA_CUST_NO            AS CUST_ID 
       ,SUBSTR(B.SA_CUST_NO, 1, 1)                       AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM F_DP_CBOD_SAACNTXN A                                   --活存明细表
  INNER JOIN F_DP_CBOD_SAACNACN B                              --活存主档
     ON A.FK_SAACN_KEY          = B.SA_ACCT_NO 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN(
         SELECT DISTINCT CHANNELCODE 
               ,AGENTSERIALNO 
               ,FR_ID 
           FROM F_TX_AFA_MAINTRANSDTL 
          WHERE ODS_ST_DATE             = V_DT) C              --
     ON C.AGENTSERIALNO         = A.SA_TX_LOG_NO 
    AND C.FR_ID                 = A.FR_ID 
   LEFT JOIN(
         SELECT DISTINCT RETRIREFNUM 
               ,FR_ID 
           FROM F_TX_ZDH_ZZDH_HISTORY_LS 
          WHERE ODS_ST_DATE             = V_DT) D              --
     ON D.RETRIREFNUM           = A.SA_TX_LOG_NO 
    AND D.FR_ID                 = A.FR_ID 
   LEFT JOIN(
         SELECT AG_CHANNEL_FLG 
               ,AG_HOST_LOGNO 
               ,AG_BUSN_DT 
               ,FR_ID 
           FROM F_NI_CBOD_AGBXLBXL 
          WHERE ODS_ST_DATE             = V_DT) E              --
     ON E.AG_HOST_LOGNO         = A.SA_TX_LOG_NO 
    AND A.SA_TX_DT        = E.AG_BUSN_DT 
    AND E.FR_ID                 = A.FR_ID 
   LEFT JOIN(
         SELECT DISTINCT BANKSYSSEQ 
               ,SUBSTR(CHANNELSEQ, 1, 1)                       AS CHANNELSEQ 
               ,FR_ID 
           FROM F_TX_EBPS_MAINTRANSDTL 
          WHERE ODS_ST_DATE             = V_DT) F              --
     ON A.SA_TX_LOG_NO    = F.BANKSYSSEQ 
    AND F.FR_ID                 = A.FR_ID 
  WHERE A.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_NIN_TRANSLOG = sqlContext.sql(sql)
ACRM_F_CI_NIN_TRANSLOG.registerTempTable("ACRM_F_CI_NIN_TRANSLOG")
dfn="ACRM_F_CI_NIN_TRANSLOG/"+V_DT+".parquet"
ACRM_F_CI_NIN_TRANSLOG.cache()
nrows = ACRM_F_CI_NIN_TRANSLOG.count()
ACRM_F_CI_NIN_TRANSLOG.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_NIN_TRANSLOG.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_NIN_TRANSLOG lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
