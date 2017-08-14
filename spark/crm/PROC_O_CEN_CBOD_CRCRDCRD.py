#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CRCRDCRD').setMaster(sys.argv[2])
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

O_DP_CBOD_CRCRDCRD = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_CRCRDCRD/*')
O_DP_CBOD_CRCRDCRD.registerTempTable("O_DP_CBOD_CRCRDCRD")


#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CR_CRD_NO               AS CR_CRD_NO 
       ,CR_CUST_NO              AS CR_CUST_NO 
       ,CR_CRD_TYP_COD          AS CR_CRD_TYP_COD 
       ,CR_JONT_COD             AS CR_JONT_COD 
       ,CR_AGRE_COD             AS CR_AGRE_COD 
       ,CR_SPUS_COD             AS CR_SPUS_COD 
       ,CR_EXPD_DT              AS CR_EXPD_DT 
       ,CR_CRDG                 AS CR_CRDG 
       ,CR_MNSUB_DRP            AS CR_MNSUB_DRP 
       ,CR_UNIT_CRD_FLG         AS CR_UNIT_CRD_FLG 
       ,CR_COLOR_PHOTO_FLAG     AS CR_COLOR_PHOTO_FLAG 
       ,CR_AGENT_PAY_FLG        AS CR_AGENT_PAY_FLG 
       ,CR_POS_CHK_FLG          AS CR_POS_CHK_FLG 
       ,CR_DR_CR_COD            AS CR_DR_CR_COD 
       ,CR_CRD_STS              AS CR_CRD_STS 
       ,CR_CRDMD_STS            AS CR_CRDMD_STS 
       ,CR_DL_STS               AS CR_DL_STS 
       ,CR_DELAY_STS            AS CR_DELAY_STS 
       ,CR_CRDNP_STS            AS CR_CRDNP_STS 
       ,CR_CNCL_STS             AS CR_CNCL_STS 
       ,CR_COLC_FLG6            AS CR_COLC_FLG6 
       ,CR_OPUN_COD             AS CR_OPUN_COD 
       ,CR_OFC_CARD_BRH         AS CR_OFC_CARD_BRH 
       ,CR_CHG_OPUN_COD         AS CR_CHG_OPUN_COD 
       ,CR_ASS_OPUN_NO          AS CR_ASS_OPUN_NO 
       ,CR_OPCR_DATE            AS CR_OPCR_DATE 
       ,CR_CRD_CHG_DT           AS CR_CRD_CHG_DT 
       ,CR_DL_DT                AS CR_DL_DT 
       ,CR_STPMT_DATE           AS CR_STPMT_DATE 
       ,CR_CNCLC_DT             AS CR_CNCLC_DT 
       ,CR_PDP_CODE             AS CR_PDP_CODE 
       ,CR_CERT_TYP             AS CR_CERT_TYP 
       ,CR_CERT_ID              AS CR_CERT_ID 
       ,CR_CUST_NAME            AS CR_CUST_NAME 
       ,CR_CRPK_FLG             AS CR_CRPK_FLG 
       ,CR_UNIT_CERT_TYP        AS CR_UNIT_CERT_TYP 
       ,CR_UNIT_CERT_ID         AS CR_UNIT_CERT_ID 
       ,CR_UNIT_CUST_NAME       AS CR_UNIT_CUST_NAME 
       ,CR_AWBK_NAME            AS CR_AWBK_NAME 
       ,CR_AWBK_ACCT_NO         AS CR_AWBK_ACCT_NO 
       ,CR_AWBK_ACCT_COD        AS CR_AWBK_ACCT_COD 
       ,CR_UNIT_CUST_NAME2      AS CR_UNIT_CUST_NAME2 
       ,CR_AWBK_ACCT_OPEN_FLG   AS CR_AWBK_ACCT_OPEN_FLG 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_DP_CBOD_CRCRDCRD A                                   --卡档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_CRCRDCRD = sqlContext.sql(sql)
F_DP_CBOD_CRCRDCRD.registerTempTable("F_DP_CBOD_CRCRDCRD")
dfn="F_DP_CBOD_CRCRDCRD/"+V_DT+".parquet"
F_DP_CBOD_CRCRDCRD.cache()
nrows = F_DP_CBOD_CRCRDCRD.count()
F_DP_CBOD_CRCRDCRD.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_DP_CBOD_CRCRDCRD.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_CRCRDCRD/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_CRCRDCRD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1


OCRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_DP_CARD_INFO_BK/'+V_DT_LD+'.parquet/*')
OCRM_F_DP_CARD_INFO.registerTempTable("OCRM_F_DP_CARD_INFO")

sql = """
 SELECT CAST(0                       AS BIGINT)                       AS ID 
       ,CAST(0                       AS DECIMAL(4))                       AS CRCRD_LL 
       ,CR_CRD_NO               AS CR_CRD_NO 
       ,CAST(0                       AS DECIMAL(15))                       AS CRCRD_DB_TIMESTAMP 
       ,''                      AS CR_PCRD_NO 
       ,CR_CUST_NO              AS CR_CUST_NO 
       ,''                      AS CR_UNIT_CUST_NO 
       ,CR_CRD_TYP_COD          AS CR_CRD_TYP_COD 
       ,CR_JONT_COD             AS CR_JONT_COD 
       ,CR_AGRE_COD             AS CR_AGRE_COD 
       ,CR_SPUS_COD             AS CR_SPUS_COD 
       ,CR_EXPD_DT              AS CR_EXPD_DT 
       ,CR_CRDG                 AS CR_CRDG 
       ,CR_MNSUB_DRP            AS CR_MNSUB_DRP 
       ,CR_UNIT_CRD_FLG         AS CR_UNIT_CRD_FLG 
       ,CR_COLOR_PHOTO_FLAG     AS CR_COLOR_PHOTO_FLAG 
       ,CR_AGENT_PAY_FLG        AS CR_AGENT_PAY_FLG 
       ,CR_POS_CHK_FLG          AS CR_POS_CHK_FLG 
       ,CR_DR_CR_COD            AS CR_DR_CR_COD 
       ,CR_CRD_STS              AS CR_CRD_STS 
       ,CR_CRDMD_STS            AS CR_CRDMD_STS 
       ,CR_DL_STS               AS CR_DL_STS 
       ,CR_DELAY_STS            AS CR_DELAY_STS 
       ,CR_CRDNP_STS            AS CR_CRDNP_STS 
       ,''                      AS CR_FILLER1 
       ,CR_CNCL_STS             AS CR_CNCL_STS 
       ,''                      AS CR_CNCL_STS1 
       ,''                      AS CR_CNCL_STS2 
       ,''                      AS CR_CNCL_STS3 
       ,''                      AS CR_CNCL_STS4 
       ,''                      AS CR_CNCL_STS5 
       ,''                      AS CR_CNCL_STS6 
       ,''                      AS CR_FILLER2 
       ,''                      AS CR_RSV_FLG_1 
       ,''                      AS CR_RSV_FLG_2 
       ,''                      AS CR_RSV_FLG_3 
       ,''                      AS CR_RSV_FLG_4 
       ,''                      AS CR_RSV_FLG_5 
       ,''                      AS CR_RSV_FLG_6 
       ,''                      AS CR_RSV_FLG_7 
       ,''                      AS CR_RSV_FLG_8 
       ,CAST(0                       AS DECIMAL(3))                       AS CR_CVV_VRSN 
       ,''                      AS CR_COLC_FLG1 
       ,''                      AS CR_COLC_FLG2 
       ,''                      AS CR_COLC_FLG3 
       ,''                      AS CR_COLC_FLG4 
       ,''                      AS CR_COLC_FLG5 
       ,CR_COLC_FLG6            AS CR_COLC_FLG6 
       ,''                      AS CR_COLC_FLG7 
       ,''                      AS CR_COLC_FLG8 
       ,''                      AS CR_FILLER4 
       ,CAST(0                       AS DECIMAL(4))                       AS CR_NO_OVD_DAYS 
       ,CAST(0                       AS DECIMAL(4))                       AS CR_ACCD_SQ_NO 
       ,CAST(0                       AS DECIMAL(3))                       AS CR_PVK_VRSN 
       ,CR_OPUN_COD             AS CR_OPUN_COD 
       ,CR_OFC_CARD_BRH         AS CR_OFC_CARD_BRH 
       ,CR_CHG_OPUN_COD         AS CR_CHG_OPUN_COD 
       ,CR_ASS_OPUN_NO          AS CR_ASS_OPUN_NO 
       ,CAST(0                       AS DECIMAL(2))                       AS CR_VRSN_NO 
       ,CR_OPCR_DATE            AS CR_OPCR_DATE 
       ,''                      AS CR_CHG_GRNTR_DT 
       ,CR_CRD_CHG_DT           AS CR_CRD_CHG_DT 
       ,CR_DL_DT                AS CR_DL_DT 
       ,CR_STPMT_DATE           AS CR_STPMT_DATE 
       ,CR_CNCLC_DT             AS CR_CNCLC_DT 
       ,''                      AS CR_ANFE_YEAR 
       ,CAST(0                       AS DECIMAL(2))                       AS CR_ANFE_UNPAY_MONTHS 
       ,CAST(0                       AS DECIMAL(3, 2))                       AS CR_ANFE_CHG_PCT 
       ,CAST(0                       AS DECIMAL(5, 2))                       AS CR_ANFE_REV_AMT 
       ,CAST(0                       AS DECIMAL(3, 2))                       AS CR_CRD_COST_REV_RTO 
       ,''                      AS CR_ATM_LTM_TX_DT 
       ,CAST(0                       AS DECIMAL(3))                       AS CR_CRNT_DT_ATM_DRW_TM 
       ,CAST(0                       AS DECIMAL(15, 2))                       AS CR_CRNT_DT_ATM_DRW_AMT 
       ,CAST(0                       AS DECIMAL(3))                       AS CR_CRNT_DT_ATM_TRN_TM 
       ,CAST(0                       AS DECIMAL(15, 2))                       AS CR_CRNT_DT_ATM_TRN_AMT 
       ,CAST(0                       AS DECIMAL(3))                       AS CR_CRNT_DT_TB_TRN_TM 
       ,CAST(0                       AS DECIMAL(15, 2))                       AS CR_CRNT_DT_TB_TRN_AMT 
       ,CAST(0                       AS DECIMAL(3))                       AS CR_CRNT_FX_ATM_DRW_TM 
       ,CAST(0                       AS DECIMAL(15, 2))                       AS CR_CRNT_FX_AMT_DRW_AMT 
       ,CAST(0                       AS DECIMAL(3))                       AS CR_CRNT_TEL_TRN_TM 
       ,CAST(0                       AS DECIMAL(15, 2))                       AS CR_CRNT_TEL_TRN_AMT 
       ,''                      AS CR_CVV2 
       ,''                      AS CR_PSWD_OFFSET 
       ,CAST(0                       AS DECIMAL(2))                       AS CR_PSWD_CNT 
       ,CAST(0                       AS DECIMAL(2))                       AS CR_IC_CNT 
       ,CAST(0                       AS DECIMAL(2))                       AS CR_CHK_INVALI_DT_TIMES 
       ,CAST(0                       AS DECIMAL(2))                       AS CR_SEC_MT_CON_CNT 
       ,''                      AS CR_SEC_MT_CON 
       ,''                      AS CR_IC_PCRD_NO 
       ,CR_PDP_CODE             AS CR_PDP_CODE 
       ,''                      AS CR_FEC_METHOD 
       ,CAST(0                       AS DECIMAL(15, 2))                       AS CR_STMT_CYCLE 
       ,CAST(0                       AS DECIMAL(15, 2))                       AS CR_STMT_DAY 
       ,''                      AS CR_CUST_APLY_NO 
       ,''                      AS CR_SVC_DSC_END_DT 
       ,''                      AS CR_SAPD_CODE 
       ,CAST(0                       AS DECIMAL(15, 2))                       AS CR_SUBCRD_USED_AMT 
       ,CAST(0                       AS DECIMAL(15, 2))                       AS CR_SUBCRD_AVL_AMT 
       ,''                      AS CR_SUBCRD_AVL_CTL_TYP 
       ,''                      AS CR_SUBCRD_LTM_CLN_DT 
       ,CR_CERT_TYP             AS CR_CERT_TYP 
       ,CR_CERT_ID              AS CR_CERT_ID 
       ,CR_CUST_NAME            AS CR_CUST_NAME 
       ,CR_CRPK_FLG             AS CR_CRPK_FLG 
       ,''                      AS CR_DOC_NO 
       ,CR_UNIT_CERT_TYP        AS CR_UNIT_CERT_TYP 
       ,CR_UNIT_CERT_ID         AS CR_UNIT_CERT_ID 
       ,CR_UNIT_CUST_NAME       AS CR_UNIT_CUST_NAME 
       ,CAST(0                       AS DECIMAL(2))                       AS CR_TB_ATM_DRW_FREE_TM 
       ,CAST(0                       AS DECIMAL(5))                       AS CR_TB_ATM_DRW_TM 
       ,''                      AS CR_TB_ATM_DRW_TX_DT 
       ,CR_AWBK_NAME            AS CR_AWBK_NAME 
       ,CR_AWBK_ACCT_NO         AS CR_AWBK_ACCT_NO 
       ,''                      AS CR_WORD_BANK_COD 
       ,''                      AS FILLER 
       ,CAST(0                       AS DECIMAL(1))                       AS CR_PRT_PSWD_NEVL_TM 
       ,''                      AS CR_CRDR_LOG_NO 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
       ,ODS_SYS_ID              AS ODS_SYS_ID 
       ,FR_ID                   AS FR_ID 
   FROM F_DP_CBOD_CRCRDCRD A                                   --卡档
   WHERE ODS_ST_DATE = V_DT
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_DP_CARD_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_DP_CARD_INFO_INNTMP1.registerTempTable("OCRM_F_DP_CARD_INFO_INNTMP1")

#OCRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_DP_CARD_INFO/*')
#OCRM_F_DP_CARD_INFO.registerTempTable("OCRM_F_DP_CARD_INFO")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CRCRD_LL                                           --CRCRD_LL:src.CRCRD_LL
       ,DST.CR_CRD_NO                                          --卡号:src.CR_CRD_NO
       ,DST.CRCRD_DB_TIMESTAMP                                 --时间戳:src.CRCRD_DB_TIMESTAMP
       ,DST.CR_PCRD_NO                                         --主卡卡号:src.CR_PCRD_NO
       ,DST.CR_CUST_NO                                         --客户编号:src.CR_CUST_NO
       ,DST.CR_UNIT_CUST_NO                                    --单位客户编号:src.CR_UNIT_CUST_NO
       ,DST.CR_CRD_TYP_COD                                     --卡种代码(TYP):src.CR_CRD_TYP_COD
       ,DST.CR_JONT_COD                                        --联名代码:src.CR_JONT_COD
       ,DST.CR_AGRE_COD                                        --认同代码:src.CR_AGRE_COD
       ,DST.CR_SPUS_COD                                        --专用代码:src.CR_SPUS_COD
       ,DST.CR_EXPD_DT                                         --有效期限:src.CR_EXPD_DT
       ,DST.CR_CRDG                                            --信用等级:src.CR_CRDG
       ,DST.CR_MNSUB_DRP                                       --主附卡标志:src.CR_MNSUB_DRP
       ,DST.CR_UNIT_CRD_FLG                                    --单位卡标志:src.CR_UNIT_CRD_FLG
       ,DST.CR_COLOR_PHOTO_FLAG                                --彩照标志:src.CR_COLOR_PHOTO_FLAG
       ,DST.CR_AGENT_PAY_FLG                                   --代发工资标志:src.CR_AGENT_PAY_FLG
       ,DST.CR_POS_CHK_FLG                                     --POS密码检查标志:src.CR_POS_CHK_FLG
       ,DST.CR_DR_CR_COD                                       --借贷别(COD):src.CR_DR_CR_COD
       ,DST.CR_CRD_STS                                         --卡状态(CRD):src.CR_CRD_STS
       ,DST.CR_CRDMD_STS                                       --制卡状态:src.CR_CRDMD_STS
       ,DST.CR_DL_STS                                          --挂失状态:src.CR_DL_STS
       ,DST.CR_DELAY_STS                                       --延期状态:src.CR_DELAY_STS
       ,DST.CR_CRDNP_STS                                       --止付状态:src.CR_CRDNP_STS
       ,DST.CR_FILLER1                                         --FILLER1:src.CR_FILLER1
       ,DST.CR_CNCL_STS                                        --注销状态:src.CR_CNCL_STS
       ,DST.CR_CNCL_STS1                                       --注销状态一:src.CR_CNCL_STS1
       ,DST.CR_CNCL_STS2                                       --注销状态二:src.CR_CNCL_STS2
       ,DST.CR_CNCL_STS3                                       --注销状态三:src.CR_CNCL_STS3
       ,DST.CR_CNCL_STS4                                       --注销状态四:src.CR_CNCL_STS4
       ,DST.CR_CNCL_STS5                                       --注销状态五:src.CR_CNCL_STS5
       ,DST.CR_CNCL_STS6                                       --注销状态六:src.CR_CNCL_STS6
       ,DST.CR_FILLER2                                         --FILLER2:src.CR_FILLER2
       ,DST.CR_RSV_FLG_1                                       --备用标志1:src.CR_RSV_FLG_1
       ,DST.CR_RSV_FLG_2                                       --备用标志2:src.CR_RSV_FLG_2
       ,DST.CR_RSV_FLG_3                                       --备用标志3:src.CR_RSV_FLG_3
       ,DST.CR_RSV_FLG_4                                       --备用标志4:src.CR_RSV_FLG_4
       ,DST.CR_RSV_FLG_5                                       --备用标志5:src.CR_RSV_FLG_5
       ,DST.CR_RSV_FLG_6                                       --备用标志6:src.CR_RSV_FLG_6
       ,DST.CR_RSV_FLG_7                                       --备用标志7:src.CR_RSV_FLG_7
       ,DST.CR_RSV_FLG_8                                       --备用标志8:src.CR_RSV_FLG_8
       ,DST.CR_CVV_VRSN                                        --CVV版本号(9型):src.CR_CVV_VRSN
       ,DST.CR_COLC_FLG1                                       --收费标志1:src.CR_COLC_FLG1
       ,DST.CR_COLC_FLG2                                       --收费标志2:src.CR_COLC_FLG2
       ,DST.CR_COLC_FLG3                                       --收费标志3:src.CR_COLC_FLG3
       ,DST.CR_COLC_FLG4                                       --收费标志4:src.CR_COLC_FLG4
       ,DST.CR_COLC_FLG5                                       --收费标志5:src.CR_COLC_FLG5
       ,DST.CR_COLC_FLG6                                       --收费标志6:src.CR_COLC_FLG6
       ,DST.CR_COLC_FLG7                                       --收费标志7:src.CR_COLC_FLG7
       ,DST.CR_COLC_FLG8                                       --收费标志8:src.CR_COLC_FLG8
       ,DST.CR_FILLER4                                         --FILLER4:src.CR_FILLER4
       ,DST.CR_NO_OVD_DAYS                                     --免息天数:src.CR_NO_OVD_DAYS
       ,DST.CR_ACCD_SQ_NO                                      --事故序号(ACCD2):src.CR_ACCD_SQ_NO
       ,DST.CR_PVK_VRSN                                        --PVK版本号(VRSN)(9型):src.CR_PVK_VRSN
       ,DST.CR_OPUN_COD                                        --营业单位代码:src.CR_OPUN_COD
       ,DST.CR_OFC_CARD_BRH                                    --发卡机构号:src.CR_OFC_CARD_BRH
       ,DST.CR_CHG_OPUN_COD                                    --换卡机构号:src.CR_CHG_OPUN_COD
       ,DST.CR_ASS_OPUN_NO                                     --考核机构号:src.CR_ASS_OPUN_NO
       ,DST.CR_VRSN_NO                                         --CR_VRSN_NO:src.CR_VRSN_NO
       ,DST.CR_OPCR_DATE                                       --开卡日期:src.CR_OPCR_DATE
       ,DST.CR_CHG_GRNTR_DT                                    --更改日期(GRNTR):src.CR_CHG_GRNTR_DT
       ,DST.CR_CRD_CHG_DT                                      --换卡日期:src.CR_CRD_CHG_DT
       ,DST.CR_DL_DT                                           --挂失日期:src.CR_DL_DT
       ,DST.CR_STPMT_DATE                                      --止付日期:src.CR_STPMT_DATE
       ,DST.CR_CNCLC_DT                                        --注销日期(CNCLC):src.CR_CNCLC_DT
       ,DST.CR_ANFE_YEAR                                       --年费收取年份:src.CR_ANFE_YEAR
       ,DST.CR_ANFE_UNPAY_MONTHS                               --年费未扣收月数:src.CR_ANFE_UNPAY_MONTHS
       ,DST.CR_ANFE_CHG_PCT                                    --年费收取比例:src.CR_ANFE_CHG_PCT
       ,DST.CR_ANFE_REV_AMT                                    --年费已扣收金额:src.CR_ANFE_REV_AMT
       ,DST.CR_CRD_COST_REV_RTO                                --卡片工本费收取比例:src.CR_CRD_COST_REV_RTO
       ,DST.CR_ATM_LTM_TX_DT                                   --CR_ATM_LTM_TX_DT:src.CR_ATM_LTM_TX_DT
       ,DST.CR_CRNT_DT_ATM_DRW_TM                              --CR_CRNT_DT_ATM_DRW_TM:src.CR_CRNT_DT_ATM_DRW_TM
       ,DST.CR_CRNT_DT_ATM_DRW_AMT                             --CR_CRNT_DT_ATM_DRW_AMT:src.CR_CRNT_DT_ATM_DRW_AMT
       ,DST.CR_CRNT_DT_ATM_TRN_TM                              --CR_CRNT_DT_ATM_TRN_TM:src.CR_CRNT_DT_ATM_TRN_TM
       ,DST.CR_CRNT_DT_ATM_TRN_AMT                             --CR_CRNT_DT_ATM_TRN_AMT:src.CR_CRNT_DT_ATM_TRN_AMT
       ,DST.CR_CRNT_DT_TB_TRN_TM                               --CR_CRNT_DT_TB_TRN_TM:src.CR_CRNT_DT_TB_TRN_TM
       ,DST.CR_CRNT_DT_TB_TRN_AMT                              --CR_CRNT_DT_TB_TRN_AMT:src.CR_CRNT_DT_TB_TRN_AMT
       ,DST.CR_CRNT_FX_ATM_DRW_TM                              --CR_CRNT_FX_ATM_DRW_TM:src.CR_CRNT_FX_ATM_DRW_TM
       ,DST.CR_CRNT_FX_AMT_DRW_AMT                             --CR_CRNT_FX_AMT_DRW_AMT:src.CR_CRNT_FX_AMT_DRW_AMT
       ,DST.CR_CRNT_TEL_TRN_TM                                 --CR_CRNT_TEL_TRN_TM:src.CR_CRNT_TEL_TRN_TM
       ,DST.CR_CRNT_TEL_TRN_AMT                                --CR_CRNT_TEL_TRN_AMT:src.CR_CRNT_TEL_TRN_AMT
       ,DST.CR_CVV2                                            --CVV2/CVN2:src.CR_CVV2
       ,DST.CR_PSWD_OFFSET                                     --密码OFFSET:src.CR_PSWD_OFFSET
       ,DST.CR_PSWD_CNT                                        --密码次数:src.CR_PSWD_CNT
       ,DST.CR_IC_CNT                                          --IC卡数量:src.CR_IC_CNT
       ,DST.CR_CHK_INVALI_DT_TIMES                             --失效日期出错次数:src.CR_CHK_INVALI_DT_TIMES
       ,DST.CR_SEC_MT_CON_CNT                                  --二磁道校验次数:src.CR_SEC_MT_CON_CNT
       ,DST.CR_SEC_MT_CON                                      --二磁道:src.CR_SEC_MT_CON
       ,DST.CR_IC_PCRD_NO                                      --IC卡主帐户:src.CR_IC_PCRD_NO
       ,DST.CR_PDP_CODE                                        --产品代码PDP:src.CR_PDP_CODE
       ,DST.CR_FEC_METHOD                                      --收费方式(FEC):src.CR_FEC_METHOD
       ,DST.CR_STMT_CYCLE                                      --账单周期(9型):src.CR_STMT_CYCLE
       ,DST.CR_STMT_DAY                                        --帐单日日(9型):src.CR_STMT_DAY
       ,DST.CR_CUST_APLY_NO                                    --客户申请编号:src.CR_CUST_APLY_NO
       ,DST.CR_SVC_DSC_END_DT                                  --手续费优惠截止期(字符):src.CR_SVC_DSC_END_DT
       ,DST.CR_SAPD_CODE                                       --活期产品代码:src.CR_SAPD_CODE
       ,DST.CR_SUBCRD_USED_AMT                                 --附属卡已使用金额:src.CR_SUBCRD_USED_AMT
       ,DST.CR_SUBCRD_AVL_AMT                                  --附属卡可用金额:src.CR_SUBCRD_AVL_AMT
       ,DST.CR_SUBCRD_AVL_CTL_TYP                              --附属卡可用金额控制类型:src.CR_SUBCRD_AVL_CTL_TYP
       ,DST.CR_SUBCRD_LTM_CLN_DT                               --附卡可用金额上次清零日期:src.CR_SUBCRD_LTM_CLN_DT
       ,DST.CR_CERT_TYP                                        --证件种类:src.CR_CERT_TYP
       ,DST.CR_CERT_ID                                         --证件号码:src.CR_CERT_ID
       ,DST.CR_CUST_NAME                                       --客户名称:src.CR_CUST_NAME
       ,DST.CR_CRPK_FLG                                        --卡本通标志:src.CR_CRPK_FLG
       ,DST.CR_DOC_NO                                          --凭证号码(20):src.CR_DOC_NO
       ,DST.CR_UNIT_CERT_TYP                                   --单位证件种类:src.CR_UNIT_CERT_TYP
       ,DST.CR_UNIT_CERT_ID                                    --单位证件号码:src.CR_UNIT_CERT_ID
       ,DST.CR_UNIT_CUST_NAME                                  --单位客户名称2:src.CR_UNIT_CUST_NAME
       ,DST.CR_TB_ATM_DRW_FREE_TM                              --他行ATM取现优惠次数:src.CR_TB_ATM_DRW_FREE_TM
       ,DST.CR_TB_ATM_DRW_TM                                   --他行ATM取现次数2:src.CR_TB_ATM_DRW_TM
       ,DST.CR_TB_ATM_DRW_TX_DT                                --他行ATM取现上次交易日期:src.CR_TB_ATM_DRW_TX_DT
       ,DST.CR_AWBK_NAME                                       --单位卡基本户开户行:src.CR_AWBK_NAME
       ,DST.CR_AWBK_ACCT_NO                                    --单位卡基本户账号:src.CR_AWBK_ACCT_NO
       ,DST.CR_WORD_BANK_COD                                   --CR_WORD_BANK_COD:src.CR_WORD_BANK_COD
       ,DST.FILLER                                             --FILLER:src.FILLER
       ,DST.CR_PRT_PSWD_NEVL_TM                                --打印密码信封次数:src.CR_PRT_PSWD_NEVL_TM
       ,DST.CR_CRDR_LOG_NO                                     --批量领卡流水号:src.CR_CRDR_LOG_NO
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_DP_CARD_INFO DST 
   LEFT JOIN OCRM_F_DP_CARD_INFO_INNTMP1 SRC 
     ON SRC.CR_CRD_NO           = DST.CR_CRD_NO 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CR_CRD_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_DP_CARD_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_DP_CARD_INFO/"+V_DT+".parquet"
UNION=OCRM_F_DP_CARD_INFO_INNTMP2.unionAll(OCRM_F_DP_CARD_INFO_INNTMP1)
OCRM_F_DP_CARD_INFO_INNTMP1.cache()
OCRM_F_DP_CARD_INFO_INNTMP2.cache()
nrowsi = OCRM_F_DP_CARD_INFO_INNTMP1.count()
nrowsa = OCRM_F_DP_CARD_INFO_INNTMP2.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_DP_CARD_INFO/*.parquet")
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_DP_CARD_INFO_INNTMP1.unpersist()
OCRM_F_DP_CARD_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_DP_CARD_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)

#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_DP_CARD_INFO_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_DP_CARD_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_DP_CARD_INFO_BK/"+V_DT+".parquet")
