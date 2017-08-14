#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MID_DP_CBOD_LNLNSJRN1').setMaster(sys.argv[2])
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


#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
F_DP_CBOD_LNLNSJRN1 = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_LNLNSJRN1/*')
F_DP_CBOD_LNLNSJRN1.registerTempTable("F_DP_CBOD_LNLNSJRN1")
#目标表：
#MID_DP_CBOD_LNLNSJRN1_2 全量
#MID_DP_CBOD_LNLNSJRN1 全量


#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ETLDT                 AS ETLDT 
       ,A.LN_LN_ACCT_NO         AS LN_LN_ACCT_NO 
       ,A.LN_ACCD_TYP           AS LN_ACCD_TYP 
       ,A.LN_ACCD_DATA          AS LN_ACCD_DATA 
       ,A.LN_ENTR_DT_N          AS LN_ENTR_DT_N 
       ,A.LN_TX_TM_N            AS LN_TX_TM_N 
       ,A.LN_ACCT_SQ_NO_N       AS LN_ACCT_SQ_NO_N 
       ,CAST(''                      AS DECIMAL(15))                       AS LNJRN2_DB_TIMESTAMP 
       ,A.LN_DB_PART_ID         AS LN_DB_PART_ID 
       ,A.LN_TX_NO              AS LN_TX_NO 
       ,A.LN_ACCD_SUB_CLSFN     AS LN_ACCD_SUB_CLSFN 
       ,A.LN_COLI_DT_RFN_DT_N   AS LN_COLI_DT_RFN_DT_N 
       ,A.LN_TX_TYP             AS LN_TX_TYP 
       ,A.LN_DR_CR_COD          AS LN_DR_CR_COD 
       ,A.LN_INT                AS LN_INT 
       ,A.LN_CAC_INTC_PR        AS LN_CAC_INTC_PR 
       ,A.LN_TX_CURR_COD        AS LN_TX_CURR_COD 
       ,A.LN_FXR                AS LN_FXR 
       ,A.LN_TNRNO_N            AS LN_TNRNO_N 
       ,A.LN_TX_OPR_NO          AS LN_TX_OPR_NO 
       ,A.LN_APRV_PIC_NO        AS LN_APRV_PIC_NO 
       ,A.LN_INTC_STRT_DT_N     AS LN_INTC_STRT_DT_N 
       ,A.LN_INTC_CUTDT_N       AS LN_INTC_CUTDT_N 
       ,A.LN_INTC_DAYS          AS LN_INTC_DAYS 
       ,A.LN_INTR               AS LN_INTR 
       ,A.LN_INT_FREE           AS LN_INT_FREE 
       ,A.LN_NOTE_NO            AS LN_NOTE_NO 
       ,A.LN_ACRBW_INT          AS LN_ACRBW_INT 
       ,A.LN_DSCRP              AS LN_DSCRP 
       ,A.LN_DOC_NO             AS LN_DOC_NO 
       ,A.LN_DOC_TYP            AS LN_DOC_TYP 
       ,A.LN_TX_LOG_NO          AS LN_TX_LOG_NO 
       ,A.LN_DLAY_INTC_STRDT_N  AS LN_DLAY_INTC_STRDT_N 
       ,A.LN_DLAY_INTC_CUTDT_N  AS LN_DLAY_INTC_CUTDT_N 
       ,A.LN_RMRK               AS LN_RMRK 
       ,A.LN_APCL_FLG           AS LN_APCL_FLG 
       ,A.LN_INT_TYP            AS LN_INT_TYP 
       ,A.LN_TRLN_BRA_INT       AS LN_TRLN_BRA_INT 
       ,A.LN_TRLN_OFF_INT       AS LN_TRLN_OFF_INT 
       ,A.LN_BELONG_INSTN_COD   AS LN_BELONG_INSTN_COD 
       ,A.LN_ASS_OPUN_NO        AS LN_ASS_OPUN_NO 
       ,A.LN_TX_OPUN_NO         AS LN_TX_OPUN_NO 
       ,A.LN_FLST_OPUN_NO       AS LN_FLST_OPUN_NO 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
   FROM F_DP_CBOD_LNLNSJRN1 A                                  --
  WHERE SUBSTR(A.ETLDT, 1, 4)                       = YEAR(V_DT) 
    AND A.LN_ACCD_DATA          = '2' 
    AND A.LN_DR_CR_COD          = 'C' 
    AND SUBSTR(A.LN_TX_NO, 1, 7) IN('LN03003', 'LN03004', 'LN03211') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_DP_CBOD_LNLNSJRN1_2 = sqlContext.sql(sql)
MID_DP_CBOD_LNLNSJRN1_2.registerTempTable("MID_DP_CBOD_LNLNSJRN1_2")
dfn="MID_DP_CBOD_LNLNSJRN1_2/"+V_DT+".parquet"
#MID_DP_CBOD_LNLNSJRN1_2.cache()
#nrows = MID_DP_CBOD_LNLNSJRN1_2.count()
MID_DP_CBOD_LNLNSJRN1_2.write.save(path=hdfs + '/' + dfn, mode='overwrite')
#MID_DP_CBOD_LNLNSJRN1_2.unpersist()
#全量表,保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_DP_CBOD_LNLNSJRN1_2/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)


#任务[21] 001-02::
V_STEP = V_STEP + 1
sql = """
 SELECT ''                      AS ETLDT 
       ,A.LN_LN_ACCT_NO         AS LN_LN_ACCT_NO 
       ,''                      AS LN_ACCD_TYP 
       ,''                      AS LN_ACCD_DATA 
       ,CAST(''                      AS DECIMAL(8))                       AS LN_ENTR_DT_N 
       ,CAST(''                      AS DECIMAL(9))                       AS LN_TX_TM_N 
       ,CAST(''                      AS DECIMAL(3))                       AS LN_ACCT_SQ_NO_N 
       ,CAST(''                      AS DECIMAL(15))                       AS LNJRN2_DB_TIMESTAMP 
       ,''                      AS LN_DB_PART_ID 
       ,''                      AS LN_TX_NO 
       ,''                      AS LN_ACCD_SUB_CLSFN 
       ,CAST(''                      AS DECIMAL(8))                       AS LN_COLI_DT_RFN_DT_N 
       ,''                      AS LN_TX_TYP 
       ,''                      AS LN_DR_CR_COD 
       ,CAST(SUM(A.LN_INT)                       AS DECIMAL(15, 2))                       AS LN_INT 
       ,CAST(''                      AS DECIMAL(15, 2))                       AS LN_CAC_INTC_PR 
       ,''                      AS LN_TX_CURR_COD 
       ,CAST(''                      AS DECIMAL(12, 6))                       AS LN_FXR 
       ,CAST(''                      AS DECIMAL(3))                       AS LN_TNRNO_N 
       ,''                      AS LN_TX_OPR_NO 
       ,''                      AS LN_APRV_PIC_NO 
       ,CAST(''                      AS DECIMAL(8))                       AS LN_INTC_STRT_DT_N 
       ,CAST(''                      AS DECIMAL(8))                       AS LN_INTC_CUTDT_N 
       ,CAST(''                      AS DECIMAL(3))                       AS LN_INTC_DAYS 
       ,CAST(''                      AS DECIMAL(8, 5))                       AS LN_INTR 
       ,CAST(''                      AS DECIMAL(15, 2))                       AS LN_INT_FREE 
       ,''                      AS LN_NOTE_NO 
       ,''                      AS LN_ACRBW_INT 
       ,''                      AS LN_DSCRP 
       ,''                      AS LN_DOC_NO 
       ,''                      AS LN_DOC_TYP 
       ,''                      AS LN_TX_LOG_NO 
       ,CAST(''                      AS DECIMAL(8))                       AS LN_DLAY_INTC_STRDT_N 
       ,CAST(''                      AS DECIMAL(8))                       AS LN_DLAY_INTC_CUTDT_N 
       ,''                      AS LN_RMRK 
       ,''                      AS LN_APCL_FLG 
       ,''                      AS LN_INT_TYP 
       ,CAST(''                      AS DECIMAL(15, 2))                       AS LN_TRLN_BRA_INT 
       ,CAST(''                      AS DECIMAL(15, 2))                       AS LN_TRLN_OFF_INT 
       ,''                      AS LN_BELONG_INSTN_COD 
       ,''                      AS LN_ASS_OPUN_NO 
       ,''                      AS LN_TX_OPUN_NO 
       ,''                      AS LN_FLST_OPUN_NO 
       ,''                      AS ODS_ST_DATE 
       ,''                      AS ODS_SYS_ID 
   FROM MID_DP_CBOD_LNLNSJRN1_2 A                              --
  GROUP BY A.LN_LN_ACCT_NO """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_DP_CBOD_LNLNSJRN1 = sqlContext.sql(sql)
MID_DP_CBOD_LNLNSJRN1.registerTempTable("MID_DP_CBOD_LNLNSJRN1")
dfn="MID_DP_CBOD_LNLNSJRN1/"+V_DT+".parquet"
MID_DP_CBOD_LNLNSJRN1.cache()
nrows = MID_DP_CBOD_LNLNSJRN1.count()
MID_DP_CBOD_LNLNSJRN1.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_DP_CBOD_LNLNSJRN1.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_DP_CBOD_LNLNSJRN1/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_DP_CBOD_LNLNSJRN1 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
