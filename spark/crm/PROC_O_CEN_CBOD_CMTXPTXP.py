#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMTXPTXP').setMaster(sys.argv[2])
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
#----------来源表---------------
O_CM_CBOD_CMTXPTXP = sqlContext.read.parquet(hdfs+'/O_CM_CBOD_CMTXPTXP/*')
O_CM_CBOD_CMTXPTXP.registerTempTable("O_CM_CBOD_CMTXPTXP")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_BUSN_TYP             AS CM_BUSN_TYP 
       ,CM_TX_IDEN_COD          AS CM_TX_IDEN_COD 
       ,CM_STX_IDEN_COD         AS CM_STX_IDEN_COD 
       ,CM_PROGM_NAME           AS CM_PROGM_NAME 
       ,CM_TX_SECN              AS CM_TX_SECN 
       ,CM_RNEW_CONF            AS CM_RNEW_CONF 
       ,CM_ACTG_OP_TYP          AS CM_ACTG_OP_TYP 
       ,CM_TX_CONT_TX_TYP       AS CM_TX_CONT_TX_TYP 
       ,CM_EC_TX_IDEN_COD       AS CM_EC_TX_IDEN_COD 
       ,CM_AACT_SQ_NO_FLAG      AS CM_AACT_SQ_NO_FLAG 
       ,CM_ANACT_SQ_NO_FLAG     AS CM_ANACT_SQ_NO_FLAG 
       ,CM_EXCS_TRAC_STS_ADJ    AS CM_EXCS_TRAC_STS_ADJ 
       ,CM_TLR_TRAC_STS_INVS    AS CM_TLR_TRAC_STS_INVS 
       ,CM_TX_SUSP              AS CM_TX_SUSP 
       ,CM_OFL_OPT_PMT          AS CM_OFL_OPT_PMT 
       ,CM_TX_PIC_APRV          AS CM_TX_PIC_APRV 
       ,CM_DARY_CLSN_TX_PRMT    AS CM_DARY_CLSN_TX_PRMT 
       ,CM_TX_SQ_NO_PROB        AS CM_TX_SQ_NO_PROB 
       ,CM_DATA_TEST_KEYS       AS CM_DATA_TEST_KEYS 
       ,CM_SUSP_DT              AS CM_SUSP_DT 
       ,CM_SUSP_TM              AS CM_SUSP_TM 
       ,CM_SUSP_TLR             AS CM_SUSP_TLR 
       ,CM_TX_NAME              AS CM_TX_NAME 
       ,CM_TX_GS_FLG            AS CM_TX_GS_FLG 
       ,CM_TX_BUS_CLASS         AS CM_TX_BUS_CLASS 
       ,CM_TX_OPR_ATT           AS CM_TX_OPR_ATT 
       ,CM_TX_ACC_UNIT          AS CM_TX_ACC_UNIT 
       ,CM_RCK_FLG              AS CM_RCK_FLG 
       ,CM_TX_TERMINAL_SIDE     AS CM_TX_TERMINAL_SIDE 
       ,CM_TLR_SIGNOFF_PRMT     AS CM_TLR_SIGNOFF_PRMT 
       ,CM_OPEN_24_FLG          AS CM_OPEN_24_FLG 
       ,CM_VOD_ACC_FLG          AS CM_VOD_ACC_FLG 
       ,CM_BRH_SIGNOFF_PRMT     AS CM_BRH_SIGNOFF_PRMT 
       ,CM_OUTPUT_COMPRESS_FLAG AS CM_OUTPUT_COMPRESS_FLAG 
       ,CM_BATCH_OUTPUT_FORM    AS CM_BATCH_OUTPUT_FORM 
       ,CM_VERIFY_ACC_NO        AS CM_VERIFY_ACC_NO 
       ,CM_VERIFY_AMT1          AS CM_VERIFY_AMT1 
       ,CM_VERIFY_AMT2          AS CM_VERIFY_AMT2 
       ,CM_BATCH_VCH_TYPE       AS CM_BATCH_VCH_TYPE 
       ,CM_SAME_AS_TXID         AS CM_SAME_AS_TXID 
       ,CM_ACA_SEQ_MOD          AS CM_ACA_SEQ_MOD 
       ,CM_CRSCTR_SUBTX_CODE    AS CM_CRSCTR_SUBTX_CODE 
       ,CM_EXCS_SECN            AS CM_EXCS_SECN 
       ,CM_TLR_TTZ_FLAG         AS CM_TLR_TTZ_FLAG 
       ,CM_CROSS_CTR_FLG        AS CM_CROSS_CTR_FLG 
       ,CM_OPM_COMP_FLG         AS CM_OPM_COMP_FLG 
       ,CM_MONITOR_SHOW_FLG     AS CM_MONITOR_SHOW_FLG 
       ,CM_1VL_BRH_SUSP_MAP     AS CM_1VL_BRH_SUSP_MAP 
       ,CM_FORMAREA_OVER2K_FLG  AS CM_FORMAREA_OVER2K_FLG 
       ,CM_C2S_ATT_REQ_FLG      AS CM_C2S_ATT_REQ_FLG 
       ,CM_TXP_CHANNEL_FLAG     AS CM_TXP_CHANNEL_FLAG 
       ,CM_CC_FLG               AS CM_CC_FLG 
       ,CM_PRT_ENTR_FLG         AS CM_PRT_ENTR_FLG 
       ,CM_CHK_MAC_FLG          AS CM_CHK_MAC_FLG 
       ,CM_OPUN_FLAG            AS CM_OPUN_FLAG 
       ,CM_TLR_CAT              AS CM_TLR_CAT 
       ,CM_CAT_BIT              AS CM_CAT_BIT 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CM_CBOD_CMTXPTXP A                                   --交易信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_CMTXPTXP = sqlContext.sql(sql)
F_CM_CBOD_CMTXPTXP.registerTempTable("F_CM_CBOD_CMTXPTXP")
dfn="F_CM_CBOD_CMTXPTXP/"+V_DT+".parquet"
F_CM_CBOD_CMTXPTXP.cache()
nrows = F_CM_CBOD_CMTXPTXP.count()
F_CM_CBOD_CMTXPTXP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_CBOD_CMTXPTXP.unpersist()
O_CM_CBOD_CMTXPTXP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_CMTXPTXP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_CMTXPTXP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
