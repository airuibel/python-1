#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_LNLNSJRN1').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_LNLNSJRN1/"+V_DT+".parquet")

O_DP_CBOD_LNLNSJRN1 = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_LNLNSJRN1/*')
O_DP_CBOD_LNLNSJRN1.registerTempTable("O_DP_CBOD_LNLNSJRN1")

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
       ,LN_COLI_DT_RFN_DT_N     AS LN_COLI_DT_RFN_DT_N 
       ,LN_TX_TYP               AS LN_TX_TYP 
       ,LN_DR_CR_COD            AS LN_DR_CR_COD 
       ,LN_INT                  AS LN_INT 
       ,LN_CAC_INTC_PR          AS LN_CAC_INTC_PR 
       ,LN_TX_CURR_COD          AS LN_TX_CURR_COD 
       ,LN_FXR                  AS LN_FXR 
       ,LN_TNRNO_N              AS LN_TNRNO_N 
       ,LN_TX_OPR_NO            AS LN_TX_OPR_NO 
       ,LN_APRV_PIC_NO          AS LN_APRV_PIC_NO 
       ,LN_INTC_STRT_DT_N       AS LN_INTC_STRT_DT_N 
       ,LN_INTC_CUTDT_N         AS LN_INTC_CUTDT_N 
       ,LN_INTC_DAYS            AS LN_INTC_DAYS 
       ,LN_INTR                 AS LN_INTR 
       ,LN_INT_FREE             AS LN_INT_FREE 
       ,LN_NOTE_NO              AS LN_NOTE_NO 
       ,LN_ACRBW_INT            AS LN_ACRBW_INT 
       ,LN_DSCRP                AS LN_DSCRP 
       ,LN_DOC_NO               AS LN_DOC_NO 
       ,LN_DOC_TYP              AS LN_DOC_TYP 
       ,LN_TX_LOG_NO            AS LN_TX_LOG_NO 
       ,LN_DLAY_INTC_STRDT_N    AS LN_DLAY_INTC_STRDT_N 
       ,LN_DLAY_INTC_CUTDT_N    AS LN_DLAY_INTC_CUTDT_N 
       ,LN_RMRK                 AS LN_RMRK 
       ,LN_APCL_FLG             AS LN_APCL_FLG 
       ,LN_INT_TYP              AS LN_INT_TYP 
       ,LN_TRLN_BRA_INT         AS LN_TRLN_BRA_INT 
       ,LN_TRLN_OFF_INT         AS LN_TRLN_OFF_INT 
       ,LN_BELONG_INSTN_COD     AS LN_BELONG_INSTN_COD 
       ,LN_ASS_OPUN_NO          AS LN_ASS_OPUN_NO 
       ,LN_TX_OPUN_NO           AS LN_TX_OPUN_NO 
       ,LN_FLST_OPUN_NO         AS LN_FLST_OPUN_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_DP_CBOD_LNLNSJRN1 A                                  --放款帐卡档<利息资料>
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_LNLNSJRN1 = sqlContext.sql(sql)
F_DP_CBOD_LNLNSJRN1.registerTempTable("F_DP_CBOD_LNLNSJRN1")
dfn="F_DP_CBOD_LNLNSJRN1/"+V_DT+".parquet"
F_DP_CBOD_LNLNSJRN1.cache()
nrows = F_DP_CBOD_LNLNSJRN1.count()
F_DP_CBOD_LNLNSJRN1.write.save(path=hdfs + '/' + dfn, mode='append')
F_DP_CBOD_LNLNSJRN1.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_LNLNSJRN1 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
