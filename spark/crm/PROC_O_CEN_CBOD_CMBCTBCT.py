#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMBCTBCT').setMaster(sys.argv[2])
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

O_CM_CBOD_CMBCTBCT = sqlContext.read.parquet(hdfs+'/O_CM_CBOD_CMBCTBCT/*')
O_CM_CBOD_CMBCTBCT.registerTempTable("O_CM_CBOD_CMBCTBCT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_OPUN_COD             AS CM_OPUN_COD 
       ,CM_SAVING_DPT_COD       AS CM_SAVING_DPT_COD 
       ,CM_OPUN_FLNM_CHN        AS CM_OPUN_FLNM_CHN 
       ,CM_OPUN_INIL_1_CHN      AS CM_OPUN_INIL_1_CHN 
       ,CM_OPUN_INIL_2_CHN      AS CM_OPUN_INIL_2_CHN 
       ,CM_OPUN_ADDR_CHN        AS CM_OPUN_ADDR_CHN 
       ,CM_OPUN_FLNM_ENG        AS CM_OPUN_FLNM_ENG 
       ,CM_OPUN_INIL_ENG        AS CM_OPUN_INIL_ENG 
       ,CM_OPUN_ADDR_ENG        AS CM_OPUN_ADDR_ENG 
       ,CM_OPUN_TEL_NO          AS CM_OPUN_TEL_NO 
       ,CM_BK_COD               AS CM_BK_COD 
       ,CM_INSTN_TYP            AS CM_INSTN_TYP 
       ,CM_OPUN_STS             AS CM_OPUN_STS 
       ,CM_CLN_BILL_MASR        AS CM_CLN_BILL_MASR 
       ,CM_POSTCODE             AS CM_POSTCODE 
       ,CM_CLN_BILL_MICR        AS CM_CLN_BILL_MICR 
       ,CM_OFC_CTBRH            AS CM_OFC_CTBRH 
       ,CM_CLS_TYP              AS CM_CLS_TYP 
       ,CM_CSH_VLT_LVL          AS CM_CSH_VLT_LVL 
       ,CM_DOC_VLT_LVL          AS CM_DOC_VLT_LVL 
       ,CM_BCT_RESERVED_FIELD1  AS CM_BCT_RESERVED_FIELD1 
       ,CM_FX_BUSN_OPN_DT       AS CM_FX_BUSN_OPN_DT 
       ,CM_NBUSN_OPN_DT         AS CM_NBUSN_OPN_DT 
       ,CM_OPUN_LTST_TX_DT      AS CM_OPUN_LTST_TX_DT 
       ,CM_SGNI_TM              AS CM_SGNI_TM 
       ,CM_SGNO_TM              AS CM_SGNO_TM 
       ,CM_OPRN_STS_CHG_TM      AS CM_OPRN_STS_CHG_TM 
       ,CM_OPN_STS              AS CM_OPN_STS 
       ,CM_PDAY_CLN_SHIFT       AS CM_PDAY_CLN_SHIFT 
       ,CM_OPUN                 AS CM_OPUN 
       ,CM_IB_CRNT_OPT_STS      AS CM_IB_CRNT_OPT_STS 
       ,CM_ACTG_ACCL_RMRK       AS CM_ACTG_ACCL_RMRK 
       ,CM_ACTG_ACCL_TM         AS CM_ACTG_ACCL_TM 
       ,CM_ACCL_TLR_COD         AS CM_ACCL_TLR_COD 
       ,CM_ACCL_PIC_COD         AS CM_ACCL_PIC_COD 
       ,CM_CNCL_ACCL_TLR_NO     AS CM_CNCL_ACCL_TLR_NO 
       ,CM_CNCL_ACCL_PIC_NO     AS CM_CNCL_ACCL_PIC_NO 
       ,CM_CNCL_ACCL_TM         AS CM_CNCL_ACCL_TM 
       ,CM_SWIFT_COD            AS CM_SWIFT_COD 
       ,CM_CMINST_COD           AS CM_CMINST_COD 
       ,CM_TAX_NO               AS CM_TAX_NO 
       ,CM_PRDS_AWB_DT          AS CM_PRDS_AWB_DT 
       ,CM_CUNT_TX_STRT_TM      AS CM_CUNT_TX_STRT_TM 
       ,CM_AREA_INIL            AS CM_AREA_INIL 
       ,CM_ENT_BK_COD           AS CM_ENT_BK_COD 
       ,CM_ENT_ACC_DEF          AS CM_ENT_ACC_DEF 
       ,CM_DISCREPANCY_MODE     AS CM_DISCREPANCY_MODE 
       ,CM_OFC_CARD_BRH         AS CM_OFC_CARD_BRH 
       ,CM_FRONT_MAC_CODE       AS CM_FRONT_MAC_CODE 
       ,CM_LOC_CLN_FLG          AS CM_LOC_CLN_FLG 
       ,CM_FX_BUSN_HQBK         AS CM_FX_BUSN_HQBK 
       ,CM_RPT_MANAGE_BRH       AS CM_RPT_MANAGE_BRH 
       ,CM_AREA_CODE            AS CM_AREA_CODE 
       ,CM_CLN_MODE             AS CM_CLN_MODE 
       ,CM_RMB_CASH_BRH         AS CM_RMB_CASH_BRH 
       ,CM_FX_CASH_BRH          AS CM_FX_CASH_BRH 
       ,CM_FINACE_ADM_BRH       AS CM_FINACE_ADM_BRH 
       ,CM_PAY_CROSS_AREA_CODE  AS CM_PAY_CROSS_AREA_CODE 
       ,CM_RPT_BNK_FLG          AS CM_RPT_BNK_FLG 
       ,CM_RPT_BNK_NO           AS CM_RPT_BNK_NO 
       ,CM_BCT_RESERVED_FIELD   AS CM_BCT_RESERVED_FIELD 
       ,CM_BNK_MON_FLAG         AS CM_BNK_MON_FLAG 
       ,CM_CR_MANA_GRAD         AS CM_CR_MANA_GRAD 
       ,CM_SGNI_RMRK            AS CM_SGNI_RMRK 
       ,CM_CTR_BNK_ID_COD       AS CM_CTR_BNK_ID_COD 
       ,CM_ACC_DT_MOD           AS CM_ACC_DT_MOD 
       ,CM_LGPS_FLG             AS CM_LGPS_FLG 
       ,CM_GLE_FLG              AS CM_GLE_FLG 
       ,CM_ECIF_BRH             AS CM_ECIF_BRH 
       ,CM_ECIF_BRH_FLAG        AS CM_ECIF_BRH_FLAG 
       ,CM_RMB_SETT_CLASS       AS CM_RMB_SETT_CLASS 
       ,CM_FX_SETT_CLASS        AS CM_FX_SETT_CLASS 
       ,CM_CUNT_TX_END_TM       AS CM_CUNT_TX_END_TM 
       ,CM_BRH_ABBR_COD         AS CM_BRH_ABBR_COD 
       ,CM_1LVL_BRH_ID          AS CM_1LVL_BRH_ID 
       ,CM_CLEAR_SEQ_NO         AS CM_CLEAR_SEQ_NO 
       ,CM_PARTITION_KEY        AS CM_PARTITION_KEY 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CM_CBOD_CMBCTBCT A                                   --营业单位主档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_CMBCTBCT = sqlContext.sql(sql)
F_CM_CBOD_CMBCTBCT.registerTempTable("F_CM_CBOD_CMBCTBCT")
dfn="F_CM_CBOD_CMBCTBCT/"+V_DT+".parquet"
F_CM_CBOD_CMBCTBCT.cache()
nrows = F_CM_CBOD_CMBCTBCT.count()
F_CM_CBOD_CMBCTBCT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_CBOD_CMBCTBCT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_CMBCTBCT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_CMBCTBCT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
