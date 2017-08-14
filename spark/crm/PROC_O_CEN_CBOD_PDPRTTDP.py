#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_PDPRTTDP').setMaster(sys.argv[2])
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

O_CM_CBOD_PDPRTTDP = sqlContext.read.parquet(hdfs+'/O_CM_CBOD_PDPRTTDP/*')
O_CM_CBOD_PDPRTTDP.registerTempTable("O_CM_CBOD_PDPRTTDP")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,PD_WORD_BANK_COD        AS PD_WORD_BANK_COD 
       ,PD_WORD_CLSFN           AS PD_WORD_CLSFN 
       ,PD_WORD_PROV_COD        AS PD_WORD_PROV_COD 
       ,PD_PD_SEQ               AS PD_PD_SEQ 
       ,PD_DEP_PRD              AS PD_DEP_PRD 
       ,PD_CUR_COD              AS PD_CUR_COD 
       ,PD_CURR_IDEN            AS PD_CURR_IDEN 
       ,PD_INTR_TYP             AS PD_INTR_TYP 
       ,PD_INTC_TYPE            AS PD_INTC_TYPE 
       ,PD_FDEP_AMT             AS PD_FDEP_AMT 
       ,PD_SYS_ACCT_NO_TYPE     AS PD_SYS_ACCT_NO_TYPE 
       ,PD_FX_BUSN_TYPE         AS PD_FX_BUSN_TYPE 
       ,PD_VALUE_SECURITY_MARK  AS PD_VALUE_SECURITY_MARK 
       ,PD_DEPO_BUSN_KIND       AS PD_DEPO_BUSN_KIND 
       ,PD_DAILY_INTR_TYP       AS PD_DAILY_INTR_TYP 
       ,PD_DRW_LOW_LIMIT        AS PD_DRW_LOW_LIMIT 
       ,PD_SA_INTR              AS PD_SA_INTR 
       ,PD_MT_INTC_FLG          AS PD_MT_INTC_FLG 
       ,PD_VAL_FLAG             AS PD_VAL_FLAG 
       ,PD_LARGE_DEP_FLG        AS PD_LARGE_DEP_FLG 
       ,PD_LIBRO_INTR           AS PD_LIBRO_INTR 
       ,PD_SA_LARGE_INTR        AS PD_SA_LARGE_INTR 
       ,PD_CVRT_CURR_COD        AS PD_CVRT_CURR_COD 
       ,PD_MAX_FDEP_AMT         AS PD_MAX_FDEP_AMT 
       ,PD_ACC_FDEP_AMT         AS PD_ACC_FDEP_AMT 
       ,PD_DOC_FDEP_AMT         AS PD_DOC_FDEP_AMT 
       ,PD_LAR_DRW_AMT          AS PD_LAR_DRW_AMT 
       ,PD_TR_DEP_AMT           AS PD_TR_DEP_AMT 
       ,PD_DOC_CURR_COD         AS PD_DOC_CURR_COD 
       ,PD_INTR_COD             AS PD_INTR_COD 
       ,PD_FLOAT_INTR_CYCL      AS PD_FLOAT_INTR_CYCL 
       ,PD_INTR_RULE            AS PD_INTR_RULE 
       ,PD_INTR_FLOAT_MAX_LMT   AS PD_INTR_FLOAT_MAX_LMT 
       ,PD_INTR_FLOAT_MIN_LMT   AS PD_INTR_FLOAT_MIN_LMT 
       ,PD_LARGE_FDEP_AMT1      AS PD_LARGE_FDEP_AMT1 
       ,PD_LARGE_FDEP_AMT2      AS PD_LARGE_FDEP_AMT2 
       ,PD_LARGE_FDEP_AMT3      AS PD_LARGE_FDEP_AMT3 
       ,PD_LARGE_FDEP_AMT4      AS PD_LARGE_FDEP_AMT4 
       ,PD_LARGE_FDEP_AMT5      AS PD_LARGE_FDEP_AMT5 
       ,PD_LARGE_FDEP_CURR_COD1 AS PD_LARGE_FDEP_CURR_COD1 
       ,PD_LARGE_FDEP_CURR_COD2 AS PD_LARGE_FDEP_CURR_COD2 
       ,PD_LARGE_FDEP_CURR_COD3 AS PD_LARGE_FDEP_CURR_COD3 
       ,PD_LARGE_FDEP_CURR_COD4 AS PD_LARGE_FDEP_CURR_COD4 
       ,PD_LARGE_FDEP_CURR_COD5 AS PD_LARGE_FDEP_CURR_COD5 
       ,PD_LARGE_TD_INTR_TYP1   AS PD_LARGE_TD_INTR_TYP1 
       ,PD_LARGE_TD_INTR_TYP2   AS PD_LARGE_TD_INTR_TYP2 
       ,PD_LARGE_TD_INTR_TYP3   AS PD_LARGE_TD_INTR_TYP3 
       ,PD_LARGE_TD_INTR_TYP4   AS PD_LARGE_TD_INTR_TYP4 
       ,PD_LARGE_TD_INTR_TYP5   AS PD_LARGE_TD_INTR_TYP5 
       ,PD_LARGE_SA_INTR_TYP1   AS PD_LARGE_SA_INTR_TYP1 
       ,PD_LARGE_SA_INTR_TYP2   AS PD_LARGE_SA_INTR_TYP2 
       ,PD_LARGE_SA_INTR_TYP3   AS PD_LARGE_SA_INTR_TYP3 
       ,PD_LARGE_SA_INTR_TYP4   AS PD_LARGE_SA_INTR_TYP4 
       ,PD_LARGE_SA_INTR_TYP5   AS PD_LARGE_SA_INTR_TYP5 
       ,PD_VALUE_SECURITY_INTR_COD      AS PD_VALUE_SECURITY_INTR_COD 
       ,PD_CACL_INTR_CYCL       AS PD_CACL_INTR_CYCL 
       ,PD_CACL_INTR_DT         AS PD_CACL_INTR_DT 
       ,PD_CACL_INTR_FLG        AS PD_CACL_INTR_FLG 
       ,PD_DEP_TYP_DESC         AS PD_DEP_TYP_DESC 
       ,PD_ODRW_CASH_AMT        AS PD_ODRW_CASH_AMT 
       ,PD_TDRW_CASH_AMT        AS PD_TDRW_CASH_AMT 
       ,PD_IDEP_CASH_AMT        AS PD_IDEP_CASH_AMT 
       ,PD_IDEP_TRAN_AMT        AS PD_IDEP_TRAN_AMT 
       ,PD_IDRW_CASH_AMT        AS PD_IDRW_CASH_AMT 
       ,PD_IDRW_TRAN_AMT        AS PD_IDRW_TRAN_AMT 
       ,PD_TRAN_FLG             AS PD_TRAN_FLG 
       ,PD_INFDRW_SAME_FLG      AS PD_INFDRW_SAME_FLG 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CM_CBOD_PDPRTTDP A                                   --定期利息部件
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_PDPRTTDP = sqlContext.sql(sql)
F_CM_CBOD_PDPRTTDP.registerTempTable("F_CM_CBOD_PDPRTTDP")
dfn="F_CM_CBOD_PDPRTTDP/"+V_DT+".parquet"
F_CM_CBOD_PDPRTTDP.cache()
nrows = F_CM_CBOD_PDPRTTDP.count()
F_CM_CBOD_PDPRTTDP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_CBOD_PDPRTTDP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_PDPRTTDP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_PDPRTTDP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
