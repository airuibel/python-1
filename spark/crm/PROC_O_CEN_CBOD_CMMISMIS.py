#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMMISMIS').setMaster(sys.argv[2])
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

O_NI_CBOD_CMMISMIS = sqlContext.read.parquet(hdfs+'/O_NI_CBOD_CMMISMIS/*')
O_NI_CBOD_CMMISMIS.registerTempTable("O_NI_CBOD_CMMISMIS")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_CHEGED_DAT           AS CM_CHEGED_DAT 
       ,CM_TX_LOG_NO            AS CM_TX_LOG_NO 
       ,CM_TX_SN                AS CM_TX_SN 
       ,CM_CAL_TX_LOG_NO        AS CM_CAL_TX_LOG_NO 
       ,CM_FEE_CAL_DAT          AS CM_FEE_CAL_DAT 
       ,CM_REC_BRH_COD          AS CM_REC_BRH_COD 
       ,CM_FEE_TIME             AS CM_FEE_TIME 
       ,CM_TX_BRH_COD           AS CM_TX_BRH_COD 
       ,CM_ACT_BRH_COD          AS CM_ACT_BRH_COD 
       ,CM_MDU_COD              AS CM_MDU_COD 
       ,CM_CORE_EVT_COD         AS CM_CORE_EVT_COD 
       ,CM_DEP_COD              AS CM_DEP_COD 
       ,CM_CHNL_COD             AS CM_CHNL_COD 
       ,CM_CHRG_COD             AS CM_CHRG_COD 
       ,CM_CHRG_NAME            AS CM_CHRG_NAME 
       ,CM_CURR_COD             AS CM_CURR_COD 
       ,CM_CHRG_STANRD_AMT      AS CM_CHRG_STANRD_AMT 
       ,CM_CHRG_SHOULD_AMT      AS CM_CHRG_SHOULD_AMT 
       ,CM_CHRG_CHRGED_AMT      AS CM_CHRG_CHRGED_AMT 
       ,CM_PD_CODE              AS CM_PD_CODE 
       ,CM_CUST_NO              AS CM_CUST_NO 
       ,CM_CUST_NAME            AS CM_CUST_NAME 
       ,CM_CUST_ACCT_NO         AS CM_CUST_ACCT_NO 
       ,CM_CASH_TRN_FLG         AS CM_CASH_TRN_FLG 
       ,CM_CURR_IDEN            AS CM_CURR_IDEN 
       ,CM_CHRG_ACCT_NO         AS CM_CHRG_ACCT_NO 
       ,CM_CHRG_ACLG_COD        AS CM_CHRG_ACLG_COD 
       ,CM_CHRG_ACLG_NO         AS CM_CHRG_ACLG_NO 
       ,CM_DEFER_FLG            AS CM_DEFER_FLG 
       ,CM_DEFER_COD            AS CM_DEFER_COD 
       ,CM_DEFER_ACLG_COD       AS CM_DEFER_ACLG_COD 
       ,CM_DEFER_ACLG_NO        AS CM_DEFER_ACLG_NO 
       ,CM_DSC_FLG              AS CM_DSC_FLG 
       ,CM_SCENE_SN             AS CM_SCENE_SN 
       ,CM_DSC_TYP              AS CM_DSC_TYP 
       ,CM_REDUCED_AMT          AS CM_REDUCED_AMT 
       ,CM_REDUCED_RAT          AS CM_REDUCED_RAT 
       ,CM_CHRG_RSN             AS CM_CHRG_RSN 
       ,CM_CM_CHRG_ACCT_NAME    AS CM_CM_CHRG_ACCT_NAME 
       ,CM_DSCRP_COD            AS CM_DSCRP_COD 
       ,CM_TX_TELLER            AS CM_TX_TELLER 
       ,CM_CF_TELLER            AS CM_CF_TELLER 
       ,CM_TX_ID                AS CM_TX_ID 
       ,CM_RCD_STS              AS CM_RCD_STS 
       ,CM_SOCK_TX_LOG_NO       AS CM_SOCK_TX_LOG_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_NI_CBOD_CMMISMIS A                                   --收费明细表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_NI_CBOD_CMMISMIS = sqlContext.sql(sql)
F_NI_CBOD_CMMISMIS.registerTempTable("F_NI_CBOD_CMMISMIS")
dfn="F_NI_CBOD_CMMISMIS/"+V_DT+".parquet"
F_NI_CBOD_CMMISMIS.cache()
nrows = F_NI_CBOD_CMMISMIS.count()
F_NI_CBOD_CMMISMIS.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_NI_CBOD_CMMISMIS.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_NI_CBOD_CMMISMIS/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_NI_CBOD_CMMISMIS lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
