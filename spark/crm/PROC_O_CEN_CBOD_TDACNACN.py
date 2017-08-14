#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_TDACNACN').setMaster(sys.argv[2])
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

O_DP_CBOD_TDACNACN = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_TDACNACN/*')
O_DP_CBOD_TDACNACN.registerTempTable("O_DP_CBOD_TDACNACN")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,TD_TD_ACCT_NO           AS TD_TD_ACCT_NO 
       ,TD_CURR_COD             AS TD_CURR_COD 
       ,TD_VIP_ACCT_FLG         AS TD_VIP_ACCT_FLG 
       ,TD_TDP_INTR             AS TD_TDP_INTR 
       ,TD_DEP_PRD_N            AS TD_DEP_PRD_N 
       ,TD_DEP_DRW_CYCL_N       AS TD_DEP_DRW_CYCL_N 
       ,TD_DUE_DT               AS TD_DUE_DT 
       ,TD_FRZ_STS              AS TD_FRZ_STS 
       ,TD_FLTR_FVR             AS TD_FLTR_FVR 
       ,TD_DL_FLG               AS TD_DL_FLG 
       ,TD_DL_DT                AS TD_DL_DT 
       ,TD_CLSD_DT              AS TD_CLSD_DT 
       ,TD_OPAC_TLR_NO          AS TD_OPAC_TLR_NO 
       ,TD_OPAC_DT              AS TD_OPAC_DT 
       ,TD_CUST_NO              AS TD_CUST_NO 
       ,TD_ACCUM_DEP_AMT        AS TD_ACCUM_DEP_AMT 
       ,TD_ACCUM_PDT            AS TD_ACCUM_PDT 
       ,TD_ACCUM_DRW_AMT        AS TD_ACCUM_DRW_AMT 
       ,TD_ACTU_AMT             AS TD_ACTU_AMT 
       ,TD_CACCT_TLR_NO         AS TD_CACCT_TLR_NO 
       ,TD_PRDS_DEP_PRD2_N      AS TD_PRDS_DEP_PRD2_N 
       ,TD_DRW_TYP              AS TD_DRW_TYP 
       ,TD_LHYPO_FLG            AS TD_LHYPO_FLG 
       ,TD_TRND_INTC_DT         AS TD_TRND_INTC_DT 
       ,TD_ACCT_STS             AS TD_ACCT_STS 
       ,TD_LST_TX_DT            AS TD_LST_TX_DT 
       ,TD_DEP_TYP              AS TD_DEP_TYP 
       ,TD_OPAC_INSTN_NO        AS TD_OPAC_INSTN_NO 
       ,TD_CACCT_INSTN_NO       AS TD_CACCT_INSTN_NO 
       ,TD_ACCT_CHAR            AS TD_ACCT_CHAR 
       ,TD_TDP_PSBK_FLG         AS TD_TDP_PSBK_FLG 
       ,TD_SEAL_STS             AS TD_SEAL_STS 
       ,TD_MNG_FLG              AS TD_MNG_FLG 
       ,TD_MNG_ACC_NO           AS TD_MNG_ACC_NO 
       ,TD_CUST_NAME            AS TD_CUST_NAME 
       ,TD_DOC_TYP              AS TD_DOC_TYP 
       ,TD_CURR_CHAR            AS TD_CURR_CHAR 
       ,TD_CURR_IDEN            AS TD_CURR_IDEN 
       ,TD_TX_DT                AS TD_TX_DT 
       ,TD_CONNTR_NO            AS TD_CONNTR_NO 
       ,TD_LARGE_DEP_FLG        AS TD_LARGE_DEP_FLG 
       ,TD_FLTR_TYP             AS TD_FLTR_TYP 
       ,TD_FLTR_FVR_SIGN        AS TD_FLTR_FVR_SIGN 
       ,TD_TDP_PSBK_PRT_NO      AS TD_TDP_PSBK_PRT_NO 
       ,TD_PDP_CODE             AS TD_PDP_CODE 
       ,TD_INTR_COD             AS TD_INTR_COD 
       ,TD_PART_FRZ_AMT         AS TD_PART_FRZ_AMT 
       ,TD_BELONG_INSTN_COD     AS TD_BELONG_INSTN_COD 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
       ,TD_PSBK_NO_N            AS TD_PSBK_NO_N 
       ,TD_ACC1_NO              AS TD_ACC1_NO 
   FROM O_DP_CBOD_TDACNACN A                                   --定期存款账户档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_TDACNACN = sqlContext.sql(sql)
F_DP_CBOD_TDACNACN.registerTempTable("F_DP_CBOD_TDACNACN")
dfn="F_DP_CBOD_TDACNACN/"+V_DT+".parquet"
F_DP_CBOD_TDACNACN.cache()
nrows = F_DP_CBOD_TDACNACN.count()
F_DP_CBOD_TDACNACN.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_DP_CBOD_TDACNACN.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_TDACNACN/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_TDACNACN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
