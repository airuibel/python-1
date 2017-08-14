#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_LNLNSJRN0').setMaster(sys.argv[2])
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

#删除当天的
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_CBOD_LNLNSJRN0/"+V_DT+".parquet")

O_LN_CBOD_LNLNSJRN0 = sqlContext.read.parquet(hdfs+'/O_LN_CBOD_LNLNSJRN0/*')
O_LN_CBOD_LNLNSJRN0.registerTempTable("O_LN_CBOD_LNLNSJRN0")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,LN_LN_ACCT_NO           AS LN_LN_ACCT_NO 
       ,LN_ACCD_TYP             AS LN_ACCD_TYP 
       ,LN_ACCD_DATA            AS LN_ACCD_DATA 
       ,LN_ENTR_DT_N            AS LN_ENTR_DT_N 
       ,LN_TX_TM_N              AS LN_TX_TM_N 
       ,LN_ACCT_SQ_NO_N         AS LN_ACCT_SQ_NO_N 
       ,LN_DB_PART_ID           AS LN_DB_PART_ID 
       ,LN_TX_NO                AS LN_TX_NO 
       ,LN_ACCD_SUB_CLSFN       AS LN_ACCD_SUB_CLSFN 
       ,LN_ALFD_DT_REPAY_DT_N   AS LN_ALFD_DT_REPAY_DT_N 
       ,LN_TX_TYP               AS LN_TX_TYP 
       ,LN_DR_CR_COD            AS LN_DR_CR_COD 
       ,LN_TX_AMT               AS LN_TX_AMT 
       ,LN_ATX_BAL              AS LN_ATX_BAL 
       ,LN_TX_CURR_COD          AS LN_TX_CURR_COD 
       ,LN_FXR                  AS LN_FXR 
       ,LN_TNRNO_N              AS LN_TNRNO_N 
       ,LN_TX_OPR_NO            AS LN_TX_OPR_NO 
       ,LN_APRV_PIC_NO          AS LN_APRV_PIC_NO 
       ,LN_LN_DUEDT_N           AS LN_LN_DUEDT_N 
       ,LN_DLAY_LN_RFN_RSN      AS LN_DLAY_LN_RFN_RSN 
       ,LN_RECON_NO_LN          AS LN_RECON_NO_LN 
       ,LN_EXTN_CTRT_NO         AS LN_EXTN_CTRT_NO 
       ,LN_PPRD_RFN_AMT         AS LN_PPRD_RFN_AMT 
       ,LN_PPRD_AMOTZ_AVAL_INTR AS LN_PPRD_AMOTZ_AVAL_INTR 
       ,LN_TX_AMT_PR            AS LN_TX_AMT_PR 
       ,LN_TX_AMT_INT           AS LN_TX_AMT_INT 
       ,LN_TX_AMT_ADVPMT        AS LN_TX_AMT_ADVPMT 
       ,LN_NOTE_NO              AS LN_NOTE_NO 
       ,LN_ADVPMT_TYP           AS LN_ADVPMT_TYP 
       ,LN_PR_FREE              AS LN_PR_FREE 
       ,LN_DSCRP                AS LN_DSCRP 
       ,LN_DOC_NO               AS LN_DOC_NO 
       ,LN_DOC_TYP              AS LN_DOC_TYP 
       ,LN_TX_LOG_NO            AS LN_TX_LOG_NO 
       ,LN_RMRK                 AS LN_RMRK 
       ,LN_APCL_FLG             AS LN_APCL_FLG 
       ,LN_BELONG_INSTN_COD     AS LN_BELONG_INSTN_COD 
       ,LN_ASS_OPUN_NO          AS LN_ASS_OPUN_NO 
       ,LN_TX_OPUN_NO           AS LN_TX_OPUN_NO 
       ,LN_FLST_OPUN_NO         AS LN_FLST_OPUN_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_LN_CBOD_LNLNSJRN0 A                                  --放款帐卡档<本金资料>
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_CBOD_LNLNSJRN0 = sqlContext.sql(sql)
F_LN_CBOD_LNLNSJRN0.registerTempTable("F_LN_CBOD_LNLNSJRN0")
dfn="F_LN_CBOD_LNLNSJRN0/"+V_DT+".parquet"
F_LN_CBOD_LNLNSJRN0.cache()
nrows = F_LN_CBOD_LNLNSJRN0.count()
F_LN_CBOD_LNLNSJRN0.write.save(path=hdfs + '/' + dfn, mode='append')
F_LN_CBOD_LNLNSJRN0.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_CBOD_LNLNSJRN0 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
