#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_DP_SAVE_INFO').setMaster(sys.argv[2])
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

#清除数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_DP_SAVE_INFO/*.parquet")
#恢复数据到今日数据文件
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_DP_SAVE_INFO_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_DP_SAVE_INFO/"+V_DT+".parquet")



OCRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_DP_CARD_INFO/*')
OCRM_F_DP_CARD_INFO.registerTempTable("OCRM_F_DP_CARD_INFO")
F_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNACN/*')
F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")
F_DP_CBOD_TDACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_TDACNACN/*')
F_DP_CBOD_TDACNACN.registerTempTable("F_DP_CBOD_TDACNACN")
OCRM_F_CM_EXCHANGE_RATE = sqlContext.read.parquet(hdfs+'/OCRM_F_CM_EXCHANGE_RATE/*')
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
F_CM_CBOD_ECCMRCMS = sqlContext.read.parquet(hdfs+'/F_CM_CBOD_ECCMRCMS/*')
F_CM_CBOD_ECCMRCMS.registerTempTable("F_CM_CBOD_ECCMRCMS")
F_DP_CBOD_CRCRDCOM = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_CRCRDCOM/*')
F_DP_CBOD_CRCRDCOM.registerTempTable("F_DP_CBOD_CRCRDCOM")
MID_CBOD_ECCMRAMR = sqlContext.read.parquet(hdfs+'/MID_CBOD_ECCMRAMR/*')
MID_CBOD_ECCMRAMR.registerTempTable("MID_CBOD_ECCMRAMR")
F_DP_CBOD_SAACNAMT = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNAMT/*')
F_DP_CBOD_SAACNAMT.registerTempTable("F_DP_CBOD_SAACNAMT")
MID_F_DP_CBOD_SAACNEVT = sqlContext.read.parquet(hdfs+'/MID_F_DP_CBOD_SAACNEVT/*')
MID_F_DP_CBOD_SAACNEVT.registerTempTable("MID_F_DP_CBOD_SAACNEVT")
ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")
MID_CMIRTIRT = sqlContext.read.parquet(hdfs+'/MID_CMIRTIRT/*')
MID_CMIRTIRT.registerTempTable("MID_CMIRTIRT")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.FK_CRCRD_KEY          AS CARD_NO 
       ,B.CR_CRD_TYP_COD        AS CARD_TYPE 
       ,A.CR_RSV_ACCT_NO        AS ODS_ACCT_NO 
       ,A.CR_CURR_COD           AS CYNO 
       ,ROW_NUMBER() OVER(
      PARTITION BY CR_RSV_ACCT_NO 
          ORDER BY A.ETLDT DESC)                       AS RN 
       ,A.FR_ID                 AS FR_ID 
   FROM F_DP_CBOD_CRCRDCOM A                                   --卡公用档
   LEFT JOIN OCRM_F_DP_CARD_INFO B                             --借记卡表
     ON A.FK_CRCRD_KEY          = B.CR_CRD_NO 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CR_CRD_STS            = '2' 
  WHERE A.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_ACRM_F_DP_SAVE_INFO_01 = sqlContext.sql(sql)
TMP_ACRM_F_DP_SAVE_INFO_01.registerTempTable("TMP_ACRM_F_DP_SAVE_INFO_01")
dfn="TMP_ACRM_F_DP_SAVE_INFO_01/"+V_DT+".parquet"
TMP_ACRM_F_DP_SAVE_INFO_01.cache()
nrows = TMP_ACRM_F_DP_SAVE_INFO_01.count()
TMP_ACRM_F_DP_SAVE_INFO_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_ACRM_F_DP_SAVE_INFO_01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_ACRM_F_DP_SAVE_INFO_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_ACRM_F_DP_SAVE_INFO_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1
#插入活期存款数据
sql = """
 SELECT CAST(monotonically_increasing_id() AS DECIMAL(19))    AS AGREEMENT_ID 
       ,A.SA_PDP_CODE           AS PRODUCT_ID 
       ,A.SA_ACCT_NO            AS ODS_ACCT_NO 
       ,A.SA_CUST_NO            AS CUST_ID 
       ,A.SA_CUST_NAME          AS CUST_NAME 
       ,B.SA_CURR_COD           AS CYNO 
       ,A.SA_BELONG_INSTN_COD   AS ORG_ID 
       ,G.PERD                  AS PERD 
       ,B.SA_INTR               AS INRT 
       ,A.SA_ORG_DEP_TYPE       AS BSKD 
       ,B.SA_DDP_PDT            AS AMAL 
       ,A.SA_DEP_TYP            AS DETP 
       ,A.SA_OPAC_DT          AS OPEN_DT 
       ,A.SA_OPAC_TLR_NO        AS OPEN_TL 
       ,A.SA_CACCT_DT           AS CLOSE_DT 
       ,A.SA_CACCT_TLR_NO       AS CLOSE_TL 
       ,A.SA_CUST_NO            AS S_CUST_NO 
       ,G.TD_MU_DT              AS TD_MU_DT 
       ,G.UP_BAL                AS UP_BAL 
       ,G.UP_BAL_RMB            AS UP_BAL_RMB 
       ,B.SA_ACCT_BAL * D.EXCHANGE_RATE         AS BAL_RMB 
       ,B.SA_ACCT_BAL * E.EXCHANGE_RATE         AS BAL_US 
       ,CASE B.SA_INTR_COD WHEN '00' THEN B.SA_INTR ELSE C.CM_INTR1 END                     AS TD_IR_TP 
       ,CAST(CASE WHEN B.SA_FLTR_FVR_SIGN      = '1' 
	   THEN(CASE B.SA_INTR_COD 
				WHEN '00' THEN B.SA_INTR ELSE C.CM_INTR1 END) *(1 + CAST(B.SA_FLTR_FVR / 100   AS DECIMAL(8, 4))) 
				WHEN B.SA_FLTR_FVR_SIGN      = '2' THEN(CASE B.SA_INTR_COD
				WHEN '00' THEN B.SA_INTR ELSE C.CM_INTR1 END) *(1 - CAST(B.SA_FLTR_FVR / 100   AS DECIMAL(8, 4))) 
				WHEN B.SA_FLTR_FVR_SIGN      = '3' THEN(CASE B.SA_INTR_COD WHEN '00' THEN B.SA_INTR ELSE C.CM_INTR1 END) * CAST(B.SA_FLTR_FVR / 100                     AS DECIMAL(8, 4)) 
				WHEN B.SA_FLTR_FVR_SIGN IS NULL THEN 0 END                     AS DECIMAL(18, 6))        AS AGREENMENT_RATE 
       ,B.SA_DDP_ACCT_STS       AS ACCT_STATUS 
       ,concat(SUBSTR(A.SA_OPAC_DT,1,4),'-',SUBSTR(A.SA_OPAC_DT,5,2),'-',SUBSTR(A.SA_OPAC_DT,7,2))            AS TD_VL_DT 
       ,B.SA_ACCT_BAL           AS MS_AC_BAL 
       ,concat(SUBSTR(B.SA_LTM_TX_DT,1,4),'-',SUBSTR(B.SA_LTM_TX_DT,5,2),'-',SUBSTR(B.SA_LTM_TX_DT,7,2))    AS LTDT 
       ,F.SA_ACCD_COD           AS MSFG 
       ,B.SA_FRZ_STS            AS FSFG 
       ,B.SA_FRZ_AMT            AS FZAM 
       ,A.SA_SEAL_STS           AS SMFG 
       ,G.LKBL                  AS LKBL 
       ,G.LKFG                  AS LKFG 
       ,G.STCD                  AS STCD 
       ,'H'                     AS ACCONT_TYPE 
       ,G.MONTH_AVG             AS MONTH_AVG 
       ,G.QUARTER_DAILY         AS QUARTER_DAILY 
       ,G.YEAR_AVG              AS YEAR_AVG 
       ,G.MVAL_RMB              AS MVAL_RMB 
       ,G.SYS_NO                AS SYS_NO 
       ,G.PERD_UNIT             AS PERD_UNIT 
       ,G.INTE_BEAR_TERM        AS INTE_BEAR_TERM 
       ,G.INTE_BEAR_MODE        AS INTE_BEAR_MODE 
       ,G.INTEREST_SETTLEMENT   AS INTEREST_SETTLEMENT 
       ,G.DEPOSIT_RESE_REQ      AS DEPOSIT_RESE_REQ 
       ,V_DT                    AS CRM_DT 
       ,G.ITEM                  AS ITEM 
       ,G.SBIT                  AS SBIT 
       ,G.SSIT                  AS SSIT 
       ,A.SA_CONNTR_NO          AS CONNTR_NO 
       ,G.MONTH_RMB             AS MONTH_RMB 
       ,G.QUARTER_RMB           AS QUARTER_RMB 
       ,SUBSTR(A.SA_CUST_NO, 1, 1)                       AS CUST_TYP 
       ,G.IS_CARD_TERM          AS IS_CARD_TERM 
       ,G.CARD_NO               AS CARD_NO 
       ,COALESCE(A.FR_ID, 'UNK')                       AS FR_ID 
       ,B.SA_CURR_IDEN          AS CURR_IDEN 
       ,G.CARD_TYPE             AS CARD_TYPE 
   FROM F_DP_CBOD_SAACNACN A                                   --活存主档
  INNER JOIN F_DP_CBOD_SAACNAMT B                              --活存资金档
     ON A.SA_ACCT_NO            = B.FK_SAACN_KEY 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN MID_CMIRTIRT C                                    --利率表
     ON A.SA_BELONG_INSTN_COD   = C.CM_OPUN_COD 
    AND B.SA_CURR_COD           = C.CM_CURR_COD 
    AND A.SA_PDP_CODE           = C.PD_CODE 
    AND C.FR_ID                 = A.FR_ID 
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE D                         --汇率表
     ON D.ODS_ST_DATE           = V_DT 
    AND D.OBJECT_CURRENCY_CD    = 'CNY' 
    AND B.SA_CURR_COD           = D.CURRENCY_CD 
    AND D.EFF_DATE              = V_DT 
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE E                         --汇率表
     ON E.ODS_ST_DATE           = V_DT 
    AND E.OBJECT_CURRENCY_CD    = 'USD' 
    AND B.SA_CURR_COD           = E.CURRENCY_CD 
    AND E.EFF_DATE              = V_DT 
   LEFT JOIN MID_F_DP_CBOD_SAACNEVT F                          --活存事故档中间表
     ON A.SA_ACCT_NO            = F.FK_SAACN_KEY 
    AND A.FR_ID                 = F.FR_ID 
   LEFT JOIN ACRM_F_DP_SAVE_INFO G                             --负债协议
     ON G.ODS_ACCT_NO           = A.SA_ACCT_NO 
    AND G.FR_ID                 = A.FR_ID 
    AND G.CURR_IDEN             = B.SA_CURR_IDEN 
    AND G.ACCONT_TYPE           = 'H'
    AND G.CYNO                  = B.SA_CURR_COD 
  WHERE A.ODS_ST_DATE           = V_DT 
    AND B.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_DP_SAVE_INFO_INNTMP1.registerTempTable("ACRM_F_DP_SAVE_INFO_INNTMP1")

ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
sql = """
 SELECT DST.AGREEMENT_ID                                        --协议标识:src.AGREEMENT_ID
       ,DST.PRODUCT_ID                                         --产品标识:src.PRODUCT_ID
       ,DST.ODS_ACCT_NO                                        --账户:src.ODS_ACCT_NO
       ,DST.CUST_ID                                            --客户标识:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CYNO                                               --币种:src.CYNO
       ,DST.ORG_ID                                             --开户机构代码:src.ORG_ID
       ,DST.PERD                                               --期限:src.PERD
       ,DST.INRT                                               --利率:src.INRT
       ,DST.BSKD                                               --业务种类:src.BSKD
       ,DST.AMAL                                               --年积数:src.AMAL
       ,DST.DETP                                               --储种:src.DETP
       ,DST.OPEN_DT                                            --开户日期:src.OPEN_DT
       ,DST.OPEN_TL                                            --开户柜员:src.OPEN_TL
       ,DST.CLOSE_DT                                           --销户日期:src.CLOSE_DT
       ,DST.CLOSE_TL                                           --销户柜员:src.CLOSE_TL
       ,DST.S_CUST_NO                                          --原系统客户号:src.S_CUST_NO
       ,DST.TD_MU_DT                                           --到期日:src.TD_MU_DT
       ,DST.UP_BAL                                             --上日余额:src.UP_BAL
       ,DST.UP_BAL_RMB                                         --折人民币上日余额:src.UP_BAL_RMB
       ,DST.BAL_RMB                                            --折人民币余额:src.BAL_RMB
       ,DST.BAL_US                                             --折美元余额:src.BAL_US
       ,DST.TD_IR_TP                                           --利率种类:src.TD_IR_TP
       ,DST.AGREENMENT_RATE                                    --协议利率:src.AGREENMENT_RATE
       ,DST.ACCT_STATUS                                        --账户状态:src.ACCT_STATUS
       ,DST.TD_VL_DT                                           --起息日期:src.TD_VL_DT
       ,DST.MS_AC_BAL                                          --帐户余额:src.MS_AC_BAL
       ,DST.LTDT                                               --上次交易日:src.LTDT
       ,DST.MSFG                                               --挂失标志:src.MSFG
       ,DST.FSFG                                               --冻结暂禁标志:src.FSFG
       ,DST.FZAM                                               --冻结金额:src.FZAM
       ,DST.SMFG                                               --印鉴挂失标志:src.SMFG
       ,DST.LKBL                                               --看管余额:src.LKBL
       ,DST.LKFG                                               --抵押看管标志:src.LKFG
       ,DST.STCD                                               --记录状态:src.STCD
       ,DST.ACCONT_TYPE                                        --账户类型:src.ACCONT_TYPE
       ,DST.MONTH_AVG                                          --月日均:src.MONTH_AVG
       ,DST.QUARTER_DAILY                                      --季日均:src.QUARTER_DAILY
       ,DST.YEAR_AVG                                           --年日均:src.YEAR_AVG
       ,DST.MVAL_RMB                                           --折人民币年日均:src.MVAL_RMB
       ,DST.SYS_NO                                             --业务系统:src.SYS_NO
       ,DST.PERD_UNIT                                          --存期单位:src.PERD_UNIT
       ,DST.INTE_BEAR_TERM                                     --计息期限:src.INTE_BEAR_TERM
       ,DST.INTE_BEAR_MODE                                     --计息方式:src.INTE_BEAR_MODE
       ,DST.INTEREST_SETTLEMENT                                --结息方式:src.INTEREST_SETTLEMENT
       ,DST.DEPOSIT_RESE_REQ                                   --缴存存款准备金方式:src.DEPOSIT_RESE_REQ
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.ITEM                                               --科目号:src.ITEM
       ,DST.SBIT                                               --子目:src.SBIT
       ,DST.SSIT                                               --细目:src.SSIT
       ,DST.CONNTR_NO                                          --管户员编号:src.CONNTR_NO
       ,DST.MONTH_RMB                                          --月均折人:src.MONTH_RMB
       ,DST.QUARTER_RMB                                        --季均折人:src.QUARTER_RMB
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.IS_CARD_TERM                                       --0:src.IS_CARD_TERM
       ,DST.CARD_NO                                            --卡号:src.CARD_NO
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.CARD_TYPE                                          --卡种:src.CARD_TYPE
   FROM ACRM_F_DP_SAVE_INFO DST 
   LEFT JOIN ACRM_F_DP_SAVE_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ODS_ACCT_NO         = DST.ODS_ACCT_NO 
    AND SRC.CURR_IDEN           = DST.CURR_IDEN 
    AND SRC.CYNO                = DST.CYNO 
    AND SRC.ACCONT_TYPE         = DST.ACCONT_TYPE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_DP_SAVE_INFO/"+V_DT+".parquet"
ACRM_F_DP_SAVE_INFO_INNTMP2=ACRM_F_DP_SAVE_INFO_INNTMP2.unionAll(ACRM_F_DP_SAVE_INFO_INNTMP1)
ACRM_F_DP_SAVE_INFO_INNTMP1.cache()
ACRM_F_DP_SAVE_INFO_INNTMP2.cache()
nrowsi = ACRM_F_DP_SAVE_INFO_INNTMP1.count()
nrowsa = ACRM_F_DP_SAVE_INFO_INNTMP2.count()
ACRM_F_DP_SAVE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_DP_SAVE_INFO_INNTMP1.unpersist()
ACRM_F_DP_SAVE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_SAVE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_DP_SAVE_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_DP_SAVE_INFO_BK/")

#活存中这两种产品不需要结算，利率为0 
#001-03
V_STEP = V_STEP + 1
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
sql = """
 SELECT A.AGREEMENT_ID          AS AGREEMENT_ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,ODS_ACCT_NO             AS ODS_ACCT_NO 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,CYNO                    AS CYNO 
       ,A.ORG_ID                AS ORG_ID 
       ,A.PERD                  AS PERD 
       ,A.INRT                  AS INRT 
       ,A.BSKD                  AS BSKD 
       ,A.AMAL                  AS AMAL 
       ,A.DETP                  AS DETP 
       ,A.OPEN_DT               AS OPEN_DT 
       ,A.OPEN_TL               AS OPEN_TL 
       ,A.CLOSE_DT              AS CLOSE_DT 
       ,A.CLOSE_TL              AS CLOSE_TL 
       ,A.S_CUST_NO             AS S_CUST_NO 
       ,A.TD_MU_DT              AS TD_MU_DT 
       ,A.UP_BAL                AS UP_BAL 
       ,A.UP_BAL_RMB            AS UP_BAL_RMB 
       ,A.BAL_RMB               AS BAL_RMB 
       ,A.BAL_US                AS BAL_US 
       ,CASE WHEN A.ACCONT_TYPE = 'H' AND (PRODUCT_ID='999SA110404' OR PRODUCT_ID='999SA110301') AND CRM_DT=V_DT THEN 0 ELSE TD_IR_TP END       AS TD_IR_TP 
       ,A.AGREENMENT_RATE       AS AGREENMENT_RATE 
       ,A.ACCT_STATUS           AS ACCT_STATUS 
       ,A.TD_VL_DT              AS TD_VL_DT 
       ,A.MS_AC_BAL             AS MS_AC_BAL 
       ,A.LTDT                  AS LTDT 
       ,A.MSFG                  AS MSFG 
       ,A.FSFG                  AS FSFG 
       ,A.FZAM                  AS FZAM 
       ,A.SMFG                  AS SMFG 
       ,A.LKBL                  AS LKBL 
       ,A.LKFG                  AS LKFG 
       ,A.STCD                  AS STCD 
       ,A.ACCONT_TYPE                    AS ACCONT_TYPE 
       ,A.MONTH_AVG             AS MONTH_AVG 
       ,A.QUARTER_DAILY         AS QUARTER_DAILY 
       ,A.YEAR_AVG              AS YEAR_AVG 
       ,A.MVAL_RMB              AS MVAL_RMB 
       ,A.SYS_NO                AS SYS_NO 
       ,A.PERD_UNIT             AS PERD_UNIT 
       ,A.INTE_BEAR_TERM        AS INTE_BEAR_TERM 
       ,A.INTE_BEAR_MODE        AS INTE_BEAR_MODE 
       ,A.INTEREST_SETTLEMENT   AS INTEREST_SETTLEMENT 
       ,A.DEPOSIT_RESE_REQ      AS DEPOSIT_RESE_REQ 
       ,A.CRM_DT                AS CRM_DT 
       ,A.ITEM                  AS ITEM 
       ,A.SBIT                  AS SBIT 
       ,A.SSIT                  AS SSIT 
       ,A.CONNTR_NO             AS CONNTR_NO 
       ,A.MONTH_RMB             AS MONTH_RMB 
       ,A.QUARTER_RMB           AS QUARTER_RMB 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.IS_CARD_TERM          AS IS_CARD_TERM 
       ,A.CARD_NO               AS CARD_NO 
       ,FR_ID                   AS FR_ID 
       ,CURR_IDEN               AS CURR_IDEN 
       ,A.CARD_TYPE             AS CARD_TYPE 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议表
   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO = sqlContext.sql(sql)
dfn="ACRM_F_DP_SAVE_INFO/"+V_DT+".parquet"
ACRM_F_DP_SAVE_INFO.cache()
nrows = ACRM_F_DP_SAVE_INFO.count()
ACRM_F_DP_SAVE_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_DP_SAVE_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_SAVE_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


#任务[12] 001-04::
#插入定期存款数据   
V_STEP = V_STEP + 1
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
sql = """
 SELECT CAST(monotonically_increasing_id() AS DECIMAL(19))    AS AGREEMENT_ID 
       ,A.TD_PDP_CODE           AS PRODUCT_ID 
       ,A.TD_TD_ACCT_NO         AS ODS_ACCT_NO 
       ,A.TD_CUST_NO            AS CUST_ID 
       ,A.TD_CUST_NAME          AS CUST_NAME 
       ,A.TD_CURR_COD           AS CYNO 
       ,A.TD_BELONG_INSTN_COD   AS ORG_ID 
       ,A.TD_DEP_PRD_N          AS PERD 
       ,A.TD_INTR_COD           AS INRT 
       ,A.TD_DEP_TYP            AS BSKD 
       ,A.TD_ACCUM_PDT          AS AMAL 
       ,A.TD_DEP_TYP            AS DETP 
       ,A.TD_OPAC_DT            AS OPEN_DT 
       ,A.TD_OPAC_TLR_NO        AS OPEN_TL 
       ,A.TD_DUE_DT             AS CLOSE_DT 
       ,A.TD_CACCT_TLR_NO       AS CLOSE_TL 
       ,A.TD_CUST_NO            AS S_CUST_NO 
       ,CASE A.TD_DUE_DT WHEN '00000000' THEN V_DT ELSE CONCAT(SUBSTR(TD_DUE_DT, 1, 4),'-',SUBSTR(TD_DUE_DT, 5, 2),'-',SUBSTR(TD_DUE_DT, 7, 2)) END                     AS TD_MU_DT 
       ,COALESCE(G.UP_BAL, 0)                       AS UP_BAL 
       ,G.UP_BAL_RMB            AS UP_BAL_RMB 
       ,CASE A.TD_ACCT_STS WHEN '51' THEN 0 ELSE A.TD_ACTU_AMT * D.EXCHANGE_RATE END                     AS BAL_RMB 
       ,G.BAL_US                AS BAL_US 
       ,E.CM_INTR1              AS TD_IR_TP 
       ,CAST(CASE WHEN A.TD_FLTR_FVR_SIGN      = '1' THEN E.CM_INTR1 *(1 + CAST(A.TD_FLTR_FVR / 100                     AS DECIMAL(8, 4))) WHEN A.TD_FLTR_FVR_SIGN      = '2' THEN E.CM_INTR1 *(1 - CAST(A.TD_FLTR_FVR / 100                     AS DECIMAL(8, 4))) WHEN A.TD_FLTR_FVR_SIGN      = '3' THEN E.CM_INTR1 * CAST(A.TD_FLTR_FVR / 100                     AS DECIMAL(8, 4)) WHEN A.TD_FLTR_FVR_SIGN IS NULL THEN E.CM_INTR1 END                     AS DECIMAL(24, 6))                       AS AGREENMENT_RATE 
       ,A.TD_ACCT_STS           AS ACCT_STATUS 
       ,concat(SUBSTR(A.TD_TRND_INTC_DT,1,4),'-',SUBSTR(A.TD_TRND_INTC_DT,5,2),'-',SUBSTR(A.TD_TRND_INTC_DT,7,2))                       AS TD_VL_DT 
       ,CASE A.TD_ACCT_STS WHEN '51' THEN DECIMAL(0) ELSE A.TD_ACTU_AMT END                     AS MS_AC_BAL 
       ,concat(SUBSTR(A.TD_LST_TX_DT,1,4),'-',SUBSTR(A.TD_LST_TX_DT,5,2),'-',SUBSTR(A.TD_LST_TX_DT,7,2))                       AS LTDT 
       ,A.TD_DL_FLG             AS MSFG 
       ,A.TD_FRZ_STS            AS FSFG 
       ,A.TD_PART_FRZ_AMT       AS FZAM 
       ,A.TD_SEAL_STS           AS SMFG 
       ,G.LKBL                  AS LKBL 
       ,G.LKFG                  AS LKFG 
       ,G.STCD                  AS STCD 
       ,'D'                     AS ACCONT_TYPE 
       ,G.MONTH_AVG             AS MONTH_AVG 
       ,G.QUARTER_DAILY         AS QUARTER_DAILY 
       ,G.YEAR_AVG              AS YEAR_AVG 
       ,G.MVAL_RMB              AS MVAL_RMB 
       ,A.TD_DEP_PRD_N          AS SYS_NO 
       ,INT(A.TD_PRDS_DEP_PRD2_N / 30)                       AS PERD_UNIT 
       ,G.INTE_BEAR_TERM        AS INTE_BEAR_TERM 
       ,G.INTE_BEAR_MODE        AS INTE_BEAR_MODE 
       ,G.INTEREST_SETTLEMENT   AS INTEREST_SETTLEMENT 
       ,G.DEPOSIT_RESE_REQ      AS DEPOSIT_RESE_REQ 
       ,V_DT                    AS CRM_DT 
       ,G.ITEM                  AS ITEM 
       ,G.SBIT                  AS SBIT 
       ,G.SSIT                  AS SSIT 
       ,A.TD_CONNTR_NO          AS CONNTR_NO 
       ,G.MONTH_RMB             AS MONTH_RMB 
       ,G.QUARTER_RMB           AS QUARTER_RMB 
       ,SUBSTR(A.TD_CUST_NO, 1, 1)                       AS CUST_TYP 
       ,G.IS_CARD_TERM          AS IS_CARD_TERM 
       ,G.CARD_NO               AS CARD_NO 
       ,COALESCE(A.FR_ID, 'UNK')                       AS FR_ID 
       ,'3'                     AS CURR_IDEN 
       ,G.CARD_TYPE             AS CARD_TYPE 
   FROM F_DP_CBOD_TDACNACN A                                   --定存主档
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE D                         --汇率表
     ON D.ODS_ST_DATE           = V_DT 
    AND D.OBJECT_CURRENCY_CD    = 'CNY' 
    AND A.TD_CURR_COD           = D.CURRENCY_CD 
    AND D.EFF_DATE              = V_DT 
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE F                         --汇率表
     ON F.ODS_ST_DATE           = V_DT 
    AND F.OBJECT_CURRENCY_CD    = 'USD' 
    AND A.TD_CURR_COD           = F.CURRENCY_CD 
    AND F.EFF_DATE              = V_DT 
   LEFT JOIN MID_CMIRTIRT E                                    --利率表
     ON A.TD_BELONG_INSTN_COD   = E.CM_OPUN_COD 
    AND A.TD_CURR_COD           = E.CM_CURR_COD 
    AND A.TD_PDP_CODE           = E.PD_CODE 
    AND A.TD_DEP_PRD_N          = E.PD_DEP_PRD 
    AND E.FR_ID                 = A.FR_ID 
   LEFT JOIN ACRM_F_DP_SAVE_INFO G                             --负债协议
     ON G.ODS_ACCT_NO           = A.TD_TD_ACCT_NO 
    AND G.FR_ID                 = A.FR_ID 
    AND G.ACCONT_TYPE           = 'D'
  WHERE A.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_DP_SAVE_INFO_INNTMP1.registerTempTable("ACRM_F_DP_SAVE_INFO_INNTMP1")

ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
sql = """
 SELECT DST.AGREEMENT_ID                                        --协议标识:src.AGREEMENT_ID
       ,DST.PRODUCT_ID                                         --产品标识:src.PRODUCT_ID
       ,DST.ODS_ACCT_NO                                        --账户:src.ODS_ACCT_NO
       ,DST.CUST_ID                                            --客户标识:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CYNO                                               --币种:src.CYNO
       ,DST.ORG_ID                                             --开户机构代码:src.ORG_ID
       ,DST.PERD                                               --期限:src.PERD
       ,DST.INRT                                               --利率:src.INRT
       ,DST.BSKD                                               --业务种类:src.BSKD
       ,DST.AMAL                                               --年积数:src.AMAL
       ,DST.DETP                                               --储种:src.DETP
       ,DST.OPEN_DT                                            --开户日期:src.OPEN_DT
       ,DST.OPEN_TL                                            --开户柜员:src.OPEN_TL
       ,DST.CLOSE_DT                                           --销户日期:src.CLOSE_DT
       ,DST.CLOSE_TL                                           --销户柜员:src.CLOSE_TL
       ,DST.S_CUST_NO                                          --原系统客户号:src.S_CUST_NO
       ,DST.TD_MU_DT                                           --到期日:src.TD_MU_DT
       ,DST.UP_BAL                                             --上日余额:src.UP_BAL
       ,DST.UP_BAL_RMB                                         --折人民币上日余额:src.UP_BAL_RMB
       ,DST.BAL_RMB                                            --折人民币余额:src.BAL_RMB
       ,DST.BAL_US                                             --折美元余额:src.BAL_US
       ,DST.TD_IR_TP                                           --利率种类:src.TD_IR_TP
       ,DST.AGREENMENT_RATE                                    --协议利率:src.AGREENMENT_RATE
       ,DST.ACCT_STATUS                                        --账户状态:src.ACCT_STATUS
       ,DST.TD_VL_DT                                           --起息日期:src.TD_VL_DT
       ,DST.MS_AC_BAL                                          --帐户余额:src.MS_AC_BAL
       ,DST.LTDT                                               --上次交易日:src.LTDT
       ,DST.MSFG                                               --挂失标志:src.MSFG
       ,DST.FSFG                                               --冻结暂禁标志:src.FSFG
       ,DST.FZAM                                               --冻结金额:src.FZAM
       ,DST.SMFG                                               --印鉴挂失标志:src.SMFG
       ,DST.LKBL                                               --看管余额:src.LKBL
       ,DST.LKFG                                               --抵押看管标志:src.LKFG
       ,DST.STCD                                               --记录状态:src.STCD
       ,DST.ACCONT_TYPE                                        --账户类型:src.ACCONT_TYPE
       ,DST.MONTH_AVG                                          --月日均:src.MONTH_AVG
       ,DST.QUARTER_DAILY                                      --季日均:src.QUARTER_DAILY
       ,DST.YEAR_AVG                                           --年日均:src.YEAR_AVG
       ,DST.MVAL_RMB                                           --折人民币年日均:src.MVAL_RMB
       ,DST.SYS_NO                                             --业务系统:src.SYS_NO
       ,DST.PERD_UNIT                                          --存期单位:src.PERD_UNIT
       ,DST.INTE_BEAR_TERM                                     --计息期限:src.INTE_BEAR_TERM
       ,DST.INTE_BEAR_MODE                                     --计息方式:src.INTE_BEAR_MODE
       ,DST.INTEREST_SETTLEMENT                                --结息方式:src.INTEREST_SETTLEMENT
       ,DST.DEPOSIT_RESE_REQ                                   --缴存存款准备金方式:src.DEPOSIT_RESE_REQ
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.ITEM                                               --科目号:src.ITEM
       ,DST.SBIT                                               --子目:src.SBIT
       ,DST.SSIT                                               --细目:src.SSIT
       ,DST.CONNTR_NO                                          --管户员编号:src.CONNTR_NO
       ,DST.MONTH_RMB                                          --月均折人:src.MONTH_RMB
       ,DST.QUARTER_RMB                                        --季均折人:src.QUARTER_RMB
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.IS_CARD_TERM                                       --0:src.IS_CARD_TERM
       ,DST.CARD_NO                                            --卡号:src.CARD_NO
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.CARD_TYPE                                          --卡种:src.CARD_TYPE
   FROM ACRM_F_DP_SAVE_INFO DST 
   LEFT JOIN ACRM_F_DP_SAVE_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ODS_ACCT_NO         = DST.ODS_ACCT_NO 
    AND SRC.ACCONT_TYPE         = DST.ACCONT_TYPE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_DP_SAVE_INFO/"+V_DT+".parquet"
ACRM_F_DP_SAVE_INFO_INNTMP2=ACRM_F_DP_SAVE_INFO_INNTMP2.unionAll(ACRM_F_DP_SAVE_INFO_INNTMP1)
ACRM_F_DP_SAVE_INFO_INNTMP1.cache()
ACRM_F_DP_SAVE_INFO_INNTMP2.cache()
nrowsi = ACRM_F_DP_SAVE_INFO_INNTMP1.count()
nrowsa = ACRM_F_DP_SAVE_INFO_INNTMP2.count()
ACRM_F_DP_SAVE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_DP_SAVE_INFO_INNTMP1.unpersist()
ACRM_F_DP_SAVE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_SAVE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_DP_SAVE_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_DP_SAVE_INFO_BK/")


#任务[12] 001-05:
#更新活期存款积数
V_STEP = V_STEP + 1
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
sql = """
 SELECT C.AGREEMENT_ID          AS AGREEMENT_ID 
       ,C.PRODUCT_ID            AS PRODUCT_ID 
       ,A.ACCT_NO               AS ODS_ACCT_NO 
       ,C.CUST_ID               AS CUST_ID 
       ,C.CUST_NAME             AS CUST_NAME 
       ,A.CURR                  AS CYNO 
       ,C.ORG_ID                AS ORG_ID 
       ,C.PERD                  AS PERD 
       ,C.INRT                  AS INRT 
       ,C.BSKD                  AS BSKD 
       ,C.AMAL                  AS AMAL 
       ,C.DETP                  AS DETP 
       ,C.OPEN_DT               AS OPEN_DT 
       ,C.OPEN_TL               AS OPEN_TL 
       ,C.CLOSE_DT              AS CLOSE_DT 
       ,C.CLOSE_TL              AS CLOSE_TL 
       ,C.S_CUST_NO             AS S_CUST_NO 
       ,C.TD_MU_DT              AS TD_MU_DT 
       ,A.LAST_AMOUNT           AS UP_BAL 
       ,CAST(A.LAST_AMOUNT * B.EXCHANGE_RATE AS DECIMAL(24,6))         AS UP_BAL_RMB 
       ,C.BAL_RMB               AS BAL_RMB 
       ,C.BAL_US                AS BAL_US 
       ,C.TD_IR_TP              AS TD_IR_TP 
       ,C.AGREENMENT_RATE       AS AGREENMENT_RATE 
       ,C.ACCT_STATUS           AS ACCT_STATUS 
       ,C.TD_VL_DT              AS TD_VL_DT 
       ,C.MS_AC_BAL             AS MS_AC_BAL 
       ,C.LTDT                  AS LTDT 
       ,C.MSFG                  AS MSFG 
       ,C.FSFG                  AS FSFG 
       ,C.FZAM                  AS FZAM 
       ,C.SMFG                  AS SMFG 
       ,C.LKBL                  AS LKBL 
       ,C.LKFG                  AS LKFG 
       ,C.STCD                  AS STCD 
       ,'H'                     AS ACCONT_TYPE 
       ,CASE MONTH(V_DT) WHEN 1 THEN A.MONTH_BAL_SUM_1 / A.MONTH_DAYS_1 WHEN 2 THEN A.MONTH_BAL_SUM_2 / A.MONTH_DAYS_2 WHEN 3 THEN A.MONTH_BAL_SUM_3 / A.MONTH_DAYS_3 WHEN 4 THEN A.MONTH_BAL_SUM_4 / A.MONTH_DAYS_4 WHEN 5 THEN A.MONTH_BAL_SUM_5 / A.MONTH_DAYS_5 WHEN 6 THEN A.MONTH_BAL_SUM_6 / A.MONTH_DAYS_6 WHEN 7 THEN A.MONTH_BAL_SUM_7 / A.MONTH_DAYS_7 WHEN 8 THEN A.MONTH_BAL_SUM_8 / A.MONTH_DAYS_8 WHEN 9 THEN A.MONTH_BAL_SUM_9 / A.MONTH_DAYS_9 WHEN 10 THEN A.MONTH_BAL_SUM_10 / A.MONTH_DAYS_10 WHEN 11 THEN A.MONTH_BAL_SUM_11 / A.MONTH_DAYS_11 WHEN 12 THEN A.MONTH_BAL_SUM_12 / A.MONTH_DAYS_12 END                     AS MONTH_AVG 
       ,CASE QUARTER(V_DT) WHEN 1 THEN(COALESCE(A.MONTH_BAL_SUM_1, 0) + COALESCE(A.MONTH_BAL_SUM_2, 0) + COALESCE(A.MONTH_BAL_SUM_3, 0)) /(COALESCE(MONTH_DAYS_1, 0) + COALESCE(MONTH_DAYS_2, 0) + COALESCE(MONTH_DAYS_3, 0)) WHEN 2 THEN(COALESCE(A.MONTH_BAL_SUM_4, 0) + COALESCE(A.MONTH_BAL_SUM_5, 0) + COALESCE(A.MONTH_BAL_SUM_6, 0)) /(COALESCE(MONTH_DAYS_4, 0) + COALESCE(MONTH_DAYS_5, 0) + COALESCE(MONTH_DAYS_6, 0)) WHEN 3 THEN(COALESCE(A.MONTH_BAL_SUM_7, 0) + COALESCE(A.MONTH_BAL_SUM_8, 0) + COALESCE(A.MONTH_BAL_SUM_9, 0)) /(COALESCE(MONTH_DAYS_7, 0) + COALESCE(MONTH_DAYS_8, 0) + COALESCE(MONTH_DAYS_9, 0)) ELSE(COALESCE(A.MONTH_BAL_SUM_10, 0) + COALESCE(A.MONTH_BAL_SUM_11, 0) + COALESCE(A.MONTH_BAL_SUM_12, 0)) /(COALESCE(MONTH_DAYS_10, 0) + COALESCE(MONTH_DAYS_11, 0) + COALESCE(MONTH_DAYS_12, 0)) END                     AS QUARTER_DAILY 
       ,YEAR_BAL_SUM /(COALESCE(MONTH_DAYS_1, 0) + COALESCE(MONTH_DAYS_2, 0) + COALESCE(MONTH_DAYS_3, 0) + COALESCE(MONTH_DAYS_4, 0) + COALESCE(MONTH_DAYS_5, 0) + COALESCE(MONTH_DAYS_6, 0) + COALESCE(MONTH_DAYS_7, 0) + COALESCE(MONTH_DAYS_8, 0) + COALESCE(MONTH_DAYS_9, 0) + COALESCE(MONTH_DAYS_10, 0) + COALESCE(MONTH_DAYS_11, 0) + COALESCE(MONTH_DAYS_12, 0))                       AS YEAR_AVG 
       ,CAST(YEAR_BAL_SUM /(COALESCE(MONTH_DAYS_1, 0) + COALESCE(MONTH_DAYS_2, 0) + COALESCE(MONTH_DAYS_3, 0) + COALESCE(MONTH_DAYS_4, 0) + COALESCE(MONTH_DAYS_5, 0) + COALESCE(MONTH_DAYS_6, 0) + COALESCE(MONTH_DAYS_7, 0) + COALESCE(MONTH_DAYS_8, 0) + COALESCE(MONTH_DAYS_9, 0) + COALESCE(MONTH_DAYS_10, 0) + COALESCE(MONTH_DAYS_11, 0) + COALESCE(MONTH_DAYS_12, 0)) * B.EXCHANGE_RATE AS DECIMAL(24,6))         AS MVAL_RMB 
       ,C.SYS_NO                AS SYS_NO 
       ,C.PERD_UNIT             AS PERD_UNIT 
       ,C.INTE_BEAR_TERM        AS INTE_BEAR_TERM 
       ,C.INTE_BEAR_MODE        AS INTE_BEAR_MODE 
       ,C.INTEREST_SETTLEMENT   AS INTEREST_SETTLEMENT 
       ,C.DEPOSIT_RESE_REQ      AS DEPOSIT_RESE_REQ 
       ,C.CRM_DT                AS CRM_DT 
       ,C.ITEM                  AS ITEM 
       ,C.SBIT                  AS SBIT 
       ,C.SSIT                  AS SSIT 
       ,C.CONNTR_NO             AS CONNTR_NO 
       ,CAST((CASE MONTH(V_DT) WHEN 1 THEN A.MONTH_BAL_SUM_1 / A.MONTH_DAYS_1 WHEN 2 THEN A.MONTH_BAL_SUM_2 / A.MONTH_DAYS_2 WHEN 3 THEN A.MONTH_BAL_SUM_3 / A.MONTH_DAYS_3 WHEN 4 THEN A.MONTH_BAL_SUM_4 / A.MONTH_DAYS_4 WHEN 5 THEN A.MONTH_BAL_SUM_5 / A.MONTH_DAYS_5 WHEN 6 THEN A.MONTH_BAL_SUM_6 / A.MONTH_DAYS_6 WHEN 7 THEN A.MONTH_BAL_SUM_7 / A.MONTH_DAYS_7 WHEN 8 THEN A.MONTH_BAL_SUM_8 / A.MONTH_DAYS_8 WHEN 9 THEN A.MONTH_BAL_SUM_9 / A.MONTH_DAYS_9 WHEN 10 THEN A.MONTH_BAL_SUM_10 / A.MONTH_DAYS_10 WHEN 11 THEN A.MONTH_BAL_SUM_11 / A.MONTH_DAYS_11 WHEN 12 THEN A.MONTH_BAL_SUM_12 / A.MONTH_DAYS_12 END) * B.EXCHANGE_RATE AS DECIMAL(24,6))        AS MONTH_RMB 
       ,CAST((CASE QUARTER(V_DT) WHEN 1 THEN(COALESCE(A.MONTH_BAL_SUM_1, 0) + COALESCE(A.MONTH_BAL_SUM_2, 0) + COALESCE(A.MONTH_BAL_SUM_3, 0)) /(COALESCE(MONTH_DAYS_1, 0) + COALESCE(MONTH_DAYS_2, 0) + COALESCE(MONTH_DAYS_3, 0)) WHEN 2 THEN(COALESCE(A.MONTH_BAL_SUM_4, 0) + COALESCE(A.MONTH_BAL_SUM_5, 0) + COALESCE(A.MONTH_BAL_SUM_6, 0)) /(COALESCE(MONTH_DAYS_4, 0) + COALESCE(MONTH_DAYS_5, 0) + COALESCE(MONTH_DAYS_6, 0)) WHEN 3 THEN(COALESCE(A.MONTH_BAL_SUM_7, 0) + COALESCE(A.MONTH_BAL_SUM_8, 0) + COALESCE(A.MONTH_BAL_SUM_9, 0)) /(COALESCE(MONTH_DAYS_7, 0) + COALESCE(MONTH_DAYS_8, 0) + COALESCE(MONTH_DAYS_9, 0)) ELSE(COALESCE(A.MONTH_BAL_SUM_10, 0) + COALESCE(A.MONTH_BAL_SUM_11, 0) + COALESCE(A.MONTH_BAL_SUM_12, 0)) /(COALESCE(MONTH_DAYS_10, 0) + COALESCE(MONTH_DAYS_11, 0) + COALESCE(MONTH_DAYS_12, 0)) END) * B.EXCHANGE_RATE AS DECIMAL(24,6))         AS QUARTER_RMB 
       ,C.CUST_TYP              AS CUST_TYP 
       ,C.IS_CARD_TERM          AS IS_CARD_TERM 
       ,C.CARD_NO               AS CARD_NO 
       ,A.FR_ID                 AS FR_ID 
       ,A.CURR_IDEN             AS CURR_IDEN 
       ,C.CARD_TYPE             AS CARD_TYPE 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --存款积数表
  INNER JOIN OCRM_F_CM_EXCHANGE_RATE B                         --汇率表
     ON B.ODS_ST_DATE           = V_DT 
    AND B.OBJECT_CURRENCY_CD    = 'CNY' 
    AND A.CURR                  = B.CURRENCY_CD 
  INNER JOIN ACRM_F_DP_SAVE_INFO C                             --负债协议表
     ON A.FR_ID                 = C.FR_ID 
    AND A.ACCT_NO               = C.ODS_ACCT_NO 
    AND A.CURR                  = C.CYNO 
    AND A.CURR_IDEN             = C.CURR_IDEN 
    AND C.ACCONT_TYPE           = 'H' 
  WHERE A.YEAR                  = YEAR(V_DT) 
    AND A.TYPE                    = 'H' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_DP_SAVE_INFO_INNTMP1.registerTempTable("ACRM_F_DP_SAVE_INFO_INNTMP1")

ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
sql = """
 SELECT DST.AGREEMENT_ID                                        --协议标识:src.AGREEMENT_ID
       ,DST.PRODUCT_ID                                         --产品标识:src.PRODUCT_ID
       ,DST.ODS_ACCT_NO                                        --账户:src.ODS_ACCT_NO
       ,DST.CUST_ID                                            --客户标识:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CYNO                                               --币种:src.CYNO
       ,DST.ORG_ID                                             --开户机构代码:src.ORG_ID
       ,DST.PERD                                               --期限:src.PERD
       ,DST.INRT                                               --利率:src.INRT
       ,DST.BSKD                                               --业务种类:src.BSKD
       ,DST.AMAL                                               --年积数:src.AMAL
       ,DST.DETP                                               --储种:src.DETP
       ,DST.OPEN_DT                                            --开户日期:src.OPEN_DT
       ,DST.OPEN_TL                                            --开户柜员:src.OPEN_TL
       ,DST.CLOSE_DT                                           --销户日期:src.CLOSE_DT
       ,DST.CLOSE_TL                                           --销户柜员:src.CLOSE_TL
       ,DST.S_CUST_NO                                          --原系统客户号:src.S_CUST_NO
       ,DST.TD_MU_DT                                           --到期日:src.TD_MU_DT
       ,DST.UP_BAL                                             --上日余额:src.UP_BAL
       ,DST.UP_BAL_RMB                                         --折人民币上日余额:src.UP_BAL_RMB
       ,DST.BAL_RMB                                            --折人民币余额:src.BAL_RMB
       ,DST.BAL_US                                             --折美元余额:src.BAL_US
       ,DST.TD_IR_TP                                           --利率种类:src.TD_IR_TP
       ,DST.AGREENMENT_RATE                                    --协议利率:src.AGREENMENT_RATE
       ,DST.ACCT_STATUS                                        --账户状态:src.ACCT_STATUS
       ,DST.TD_VL_DT                                           --起息日期:src.TD_VL_DT
       ,DST.MS_AC_BAL                                          --帐户余额:src.MS_AC_BAL
       ,DST.LTDT                                               --上次交易日:src.LTDT
       ,DST.MSFG                                               --挂失标志:src.MSFG
       ,DST.FSFG                                               --冻结暂禁标志:src.FSFG
       ,DST.FZAM                                               --冻结金额:src.FZAM
       ,DST.SMFG                                               --印鉴挂失标志:src.SMFG
       ,DST.LKBL                                               --看管余额:src.LKBL
       ,DST.LKFG                                               --抵押看管标志:src.LKFG
       ,DST.STCD                                               --记录状态:src.STCD
       ,DST.ACCONT_TYPE                                        --账户类型:src.ACCONT_TYPE
       ,DST.MONTH_AVG                                          --月日均:src.MONTH_AVG
       ,DST.QUARTER_DAILY                                      --季日均:src.QUARTER_DAILY
       ,DST.YEAR_AVG                                           --年日均:src.YEAR_AVG
       ,DST.MVAL_RMB                                           --折人民币年日均:src.MVAL_RMB
       ,DST.SYS_NO                                             --业务系统:src.SYS_NO
       ,DST.PERD_UNIT                                          --存期单位:src.PERD_UNIT
       ,DST.INTE_BEAR_TERM                                     --计息期限:src.INTE_BEAR_TERM
       ,DST.INTE_BEAR_MODE                                     --计息方式:src.INTE_BEAR_MODE
       ,DST.INTEREST_SETTLEMENT                                --结息方式:src.INTEREST_SETTLEMENT
       ,DST.DEPOSIT_RESE_REQ                                   --缴存存款准备金方式:src.DEPOSIT_RESE_REQ
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.ITEM                                               --科目号:src.ITEM
       ,DST.SBIT                                               --子目:src.SBIT
       ,DST.SSIT                                               --细目:src.SSIT
       ,DST.CONNTR_NO                                          --管户员编号:src.CONNTR_NO
       ,DST.MONTH_RMB                                          --月均折人:src.MONTH_RMB
       ,DST.QUARTER_RMB                                        --季均折人:src.QUARTER_RMB
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.IS_CARD_TERM                                       --0:src.IS_CARD_TERM
       ,DST.CARD_NO                                            --卡号:src.CARD_NO
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.CARD_TYPE                                          --卡种:src.CARD_TYPE
   FROM ACRM_F_DP_SAVE_INFO DST 
   LEFT JOIN ACRM_F_DP_SAVE_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ODS_ACCT_NO         = DST.ODS_ACCT_NO 
    AND SRC.CURR_IDEN           = DST.CURR_IDEN 
    AND SRC.CYNO                = DST.CYNO 
    AND SRC.ACCONT_TYPE         = DST.ACCONT_TYPE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_DP_SAVE_INFO/"+V_DT+".parquet"
ACRM_F_DP_SAVE_INFO_INNTMP2=ACRM_F_DP_SAVE_INFO_INNTMP2.unionAll(ACRM_F_DP_SAVE_INFO_INNTMP1)
ACRM_F_DP_SAVE_INFO_INNTMP1.cache()
ACRM_F_DP_SAVE_INFO_INNTMP2.cache()
nrowsi = ACRM_F_DP_SAVE_INFO_INNTMP1.count()
nrowsa = ACRM_F_DP_SAVE_INFO_INNTMP2.count()
ACRM_F_DP_SAVE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_DP_SAVE_INFO_INNTMP1.unpersist()
ACRM_F_DP_SAVE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_SAVE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_DP_SAVE_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_DP_SAVE_INFO_BK/")

#任务[12] 001-06::
V_STEP = V_STEP + 1
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
#更新定期存款积数
sql = """
 SELECT C.AGREEMENT_ID          AS AGREEMENT_ID 
       ,C.PRODUCT_ID            AS PRODUCT_ID 
       ,A.ACCT_NO               AS ODS_ACCT_NO 
       ,C.CUST_ID               AS CUST_ID 
       ,C.CUST_NAME             AS CUST_NAME 
       ,C.CYNO                  AS CYNO 
       ,C.ORG_ID                AS ORG_ID 
       ,C.PERD                  AS PERD 
       ,C.INRT                  AS INRT 
       ,C.BSKD                  AS BSKD 
       ,C.AMAL                  AS AMAL 
       ,C.DETP                  AS DETP 
       ,C.OPEN_DT               AS OPEN_DT 
       ,C.OPEN_TL               AS OPEN_TL 
       ,C.CLOSE_DT              AS CLOSE_DT 
       ,C.CLOSE_TL              AS CLOSE_TL 
       ,C.S_CUST_NO             AS S_CUST_NO 
       ,C.TD_MU_DT              AS TD_MU_DT 
       ,A.LAST_AMOUNT           AS UP_BAL 
       ,CAST(A.LAST_AMOUNT * B.EXCHANGE_RATE AS DECIMAL(24,6))         AS UP_BAL_RMB 
       ,C.BAL_RMB               AS BAL_RMB 
       ,C.BAL_US                AS BAL_US 
       ,C.TD_IR_TP              AS TD_IR_TP 
       ,C.AGREENMENT_RATE       AS AGREENMENT_RATE 
       ,C.ACCT_STATUS           AS ACCT_STATUS 
       ,C.TD_VL_DT              AS TD_VL_DT 
       ,C.MS_AC_BAL             AS MS_AC_BAL 
       ,C.LTDT                  AS LTDT 
       ,C.MSFG                  AS MSFG 
       ,C.FSFG                  AS FSFG 
       ,C.FZAM                  AS FZAM 
       ,C.SMFG                  AS SMFG 
       ,C.LKBL                  AS LKBL 
       ,C.LKFG                  AS LKFG 
       ,C.STCD                  AS STCD 
       ,'D'                     AS ACCONT_TYPE 
       ,CASE MONTH(V_DT) WHEN 1 THEN A.MONTH_BAL_SUM_1 / A.MONTH_DAYS_1 WHEN 2 THEN A.MONTH_BAL_SUM_2 / A.MONTH_DAYS_2 WHEN 3 THEN A.MONTH_BAL_SUM_3 / A.MONTH_DAYS_3 WHEN 4 THEN A.MONTH_BAL_SUM_4 / A.MONTH_DAYS_4 WHEN 5 THEN A.MONTH_BAL_SUM_5 / A.MONTH_DAYS_5 WHEN 6 THEN A.MONTH_BAL_SUM_6 / A.MONTH_DAYS_6 WHEN 7 THEN A.MONTH_BAL_SUM_7 / A.MONTH_DAYS_7 WHEN 8 THEN A.MONTH_BAL_SUM_8 / A.MONTH_DAYS_8 WHEN 9 THEN A.MONTH_BAL_SUM_9 / A.MONTH_DAYS_9 WHEN 10 THEN A.MONTH_BAL_SUM_10 / A.MONTH_DAYS_10 WHEN 11 THEN A.MONTH_BAL_SUM_11 / A.MONTH_DAYS_11 WHEN 12 THEN A.MONTH_BAL_SUM_12 / A.MONTH_DAYS_12 END                     AS MONTH_AVG 
       ,CASE QUARTER(V_DT) WHEN 1 THEN(COALESCE(A.MONTH_BAL_SUM_1, 0) + COALESCE(A.MONTH_BAL_SUM_2, 0) + COALESCE(A.MONTH_BAL_SUM_3, 0)) /(COALESCE(MONTH_DAYS_1, 0) + COALESCE(MONTH_DAYS_2, 0) + COALESCE(MONTH_DAYS_3, 0)) WHEN 2 THEN(COALESCE(A.MONTH_BAL_SUM_4, 0) + COALESCE(A.MONTH_BAL_SUM_5, 0) + COALESCE(A.MONTH_BAL_SUM_6, 0)) /(COALESCE(MONTH_DAYS_4, 0) + COALESCE(MONTH_DAYS_5, 0) + COALESCE(MONTH_DAYS_6, 0)) WHEN 3 THEN(COALESCE(A.MONTH_BAL_SUM_7, 0) + COALESCE(A.MONTH_BAL_SUM_8, 0) + COALESCE(A.MONTH_BAL_SUM_9, 0)) /(COALESCE(MONTH_DAYS_7, 0) + COALESCE(MONTH_DAYS_8, 0) + COALESCE(MONTH_DAYS_9, 0)) ELSE(COALESCE(A.MONTH_BAL_SUM_10, 0) + COALESCE(A.MONTH_BAL_SUM_11, 0) + COALESCE(A.MONTH_BAL_SUM_12, 0)) /(COALESCE(MONTH_DAYS_10, 0) + COALESCE(MONTH_DAYS_11, 0) + COALESCE(MONTH_DAYS_12, 0)) END                     AS QUARTER_DAILY 
       ,YEAR_BAL_SUM /(COALESCE(MONTH_DAYS_1, 0) + COALESCE(MONTH_DAYS_2, 0) + COALESCE(MONTH_DAYS_3, 0) + COALESCE(MONTH_DAYS_4, 0) + COALESCE(MONTH_DAYS_5, 0) + COALESCE(MONTH_DAYS_6, 0) + COALESCE(MONTH_DAYS_7, 0) + COALESCE(MONTH_DAYS_8, 0) + COALESCE(MONTH_DAYS_9, 0) + COALESCE(MONTH_DAYS_10, 0) + COALESCE(MONTH_DAYS_11, 0) + COALESCE(MONTH_DAYS_12, 0))                       AS YEAR_AVG 
       ,CAST(YEAR_BAL_SUM /(COALESCE(MONTH_DAYS_1, 0) + COALESCE(MONTH_DAYS_2, 0) + COALESCE(MONTH_DAYS_3, 0) + COALESCE(MONTH_DAYS_4, 0) + COALESCE(MONTH_DAYS_5, 0) + COALESCE(MONTH_DAYS_6, 0) + COALESCE(MONTH_DAYS_7, 0) + COALESCE(MONTH_DAYS_8, 0) + COALESCE(MONTH_DAYS_9, 0) + COALESCE(MONTH_DAYS_10, 0) + COALESCE(MONTH_DAYS_11, 0) + COALESCE(MONTH_DAYS_12, 0)) * B.EXCHANGE_RATE  AS DECIMAL(24,6))        AS MVAL_RMB 
       ,C.SYS_NO                AS SYS_NO 
       ,C.PERD_UNIT             AS PERD_UNIT 
       ,C.INTE_BEAR_TERM        AS INTE_BEAR_TERM 
       ,C.INTE_BEAR_MODE        AS INTE_BEAR_MODE 
       ,C.INTEREST_SETTLEMENT   AS INTEREST_SETTLEMENT 
       ,C.DEPOSIT_RESE_REQ      AS DEPOSIT_RESE_REQ 
       ,C.CRM_DT                AS CRM_DT 
       ,C.ITEM                  AS ITEM 
       ,C.SBIT                  AS SBIT 
       ,C.SSIT                  AS SSIT 
       ,C.CONNTR_NO             AS CONNTR_NO 
       ,CAST((CASE MONTH(V_DT) WHEN 1 THEN A.MONTH_BAL_SUM_1 / A.MONTH_DAYS_1 WHEN 2 THEN A.MONTH_BAL_SUM_2 / A.MONTH_DAYS_2 WHEN 3 THEN A.MONTH_BAL_SUM_3 / A.MONTH_DAYS_3 WHEN 4 THEN A.MONTH_BAL_SUM_4 / A.MONTH_DAYS_4 WHEN 5 THEN A.MONTH_BAL_SUM_5 / A.MONTH_DAYS_5 WHEN 6 THEN A.MONTH_BAL_SUM_6 / A.MONTH_DAYS_6 WHEN 7 THEN A.MONTH_BAL_SUM_7 / A.MONTH_DAYS_7 WHEN 8 THEN A.MONTH_BAL_SUM_8 / A.MONTH_DAYS_8 WHEN 9 THEN A.MONTH_BAL_SUM_9 / A.MONTH_DAYS_9 WHEN 10 THEN A.MONTH_BAL_SUM_10 / A.MONTH_DAYS_10 WHEN 11 THEN A.MONTH_BAL_SUM_11 / A.MONTH_DAYS_11 WHEN 12 THEN A.MONTH_BAL_SUM_12 / A.MONTH_DAYS_12 END) * B.EXCHANGE_RATE  AS DECIMAL(24,6))        AS MONTH_RMB 
       ,CAST((CASE QUARTER(V_DT) WHEN 1 THEN(COALESCE(A.MONTH_BAL_SUM_1, 0) + COALESCE(A.MONTH_BAL_SUM_2, 0) + COALESCE(A.MONTH_BAL_SUM_3, 0)) /(COALESCE(MONTH_DAYS_1, 0) + COALESCE(MONTH_DAYS_2, 0) + COALESCE(MONTH_DAYS_3, 0)) WHEN 2 THEN(COALESCE(A.MONTH_BAL_SUM_4, 0) + COALESCE(A.MONTH_BAL_SUM_5, 0) + COALESCE(A.MONTH_BAL_SUM_6, 0)) /(COALESCE(MONTH_DAYS_4, 0) + COALESCE(MONTH_DAYS_5, 0) + COALESCE(MONTH_DAYS_6, 0)) WHEN 3 THEN(COALESCE(A.MONTH_BAL_SUM_7, 0) + COALESCE(A.MONTH_BAL_SUM_8, 0) + COALESCE(A.MONTH_BAL_SUM_9, 0)) /(COALESCE(MONTH_DAYS_7, 0) + COALESCE(MONTH_DAYS_8, 0) + COALESCE(MONTH_DAYS_9, 0)) ELSE(COALESCE(A.MONTH_BAL_SUM_10, 0) + COALESCE(A.MONTH_BAL_SUM_11, 0) + COALESCE(A.MONTH_BAL_SUM_12, 0)) /(COALESCE(MONTH_DAYS_10, 0) + COALESCE(MONTH_DAYS_11, 0) + COALESCE(MONTH_DAYS_12, 0)) END) * B.EXCHANGE_RATE  AS DECIMAL(24,6))        AS QUARTER_RMB 
       ,C.CUST_TYP              AS CUST_TYP 
       ,C.IS_CARD_TERM          AS IS_CARD_TERM 
       ,C.CARD_NO               AS CARD_NO 
       ,A.FR_ID                 AS FR_ID 
       ,C.CURR_IDEN             AS CURR_IDEN 
       ,C.CARD_TYPE             AS CARD_TYPE 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --存款积数表
  INNER JOIN OCRM_F_CM_EXCHANGE_RATE B                         --汇率表
     ON B.ODS_ST_DATE           = V_DT 
    AND B.OBJECT_CURRENCY_CD    = 'CNY' 
    AND A.CURR                  = B.CURRENCY_CD 
  INNER JOIN ACRM_F_DP_SAVE_INFO C                             --负债协议表
     ON A.FR_ID                 = C.FR_ID 
    AND A.ACCT_NO               = C.ODS_ACCT_NO 
    AND C.ACCONT_TYPE           = 'D' 
  WHERE A.YEAR                  = YEAR(V_DT) 
    AND A.TYPE                    = 'D' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_DP_SAVE_INFO_INNTMP1.registerTempTable("ACRM_F_DP_SAVE_INFO_INNTMP1")

ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
sql = """
 SELECT DST.AGREEMENT_ID                                        --协议标识:src.AGREEMENT_ID
       ,DST.PRODUCT_ID                                         --产品标识:src.PRODUCT_ID
       ,DST.ODS_ACCT_NO                                        --账户:src.ODS_ACCT_NO
       ,DST.CUST_ID                                            --客户标识:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CYNO                                               --币种:src.CYNO
       ,DST.ORG_ID                                             --开户机构代码:src.ORG_ID
       ,DST.PERD                                               --期限:src.PERD
       ,DST.INRT                                               --利率:src.INRT
       ,DST.BSKD                                               --业务种类:src.BSKD
       ,DST.AMAL                                               --年积数:src.AMAL
       ,DST.DETP                                               --储种:src.DETP
       ,DST.OPEN_DT                                            --开户日期:src.OPEN_DT
       ,DST.OPEN_TL                                            --开户柜员:src.OPEN_TL
       ,DST.CLOSE_DT                                           --销户日期:src.CLOSE_DT
       ,DST.CLOSE_TL                                           --销户柜员:src.CLOSE_TL
       ,DST.S_CUST_NO                                          --原系统客户号:src.S_CUST_NO
       ,DST.TD_MU_DT                                           --到期日:src.TD_MU_DT
       ,DST.UP_BAL                                             --上日余额:src.UP_BAL
       ,DST.UP_BAL_RMB                                         --折人民币上日余额:src.UP_BAL_RMB
       ,DST.BAL_RMB                                            --折人民币余额:src.BAL_RMB
       ,DST.BAL_US                                             --折美元余额:src.BAL_US
       ,DST.TD_IR_TP                                           --利率种类:src.TD_IR_TP
       ,DST.AGREENMENT_RATE                                    --协议利率:src.AGREENMENT_RATE
       ,DST.ACCT_STATUS                                        --账户状态:src.ACCT_STATUS
       ,DST.TD_VL_DT                                           --起息日期:src.TD_VL_DT
       ,DST.MS_AC_BAL                                          --帐户余额:src.MS_AC_BAL
       ,DST.LTDT                                               --上次交易日:src.LTDT
       ,DST.MSFG                                               --挂失标志:src.MSFG
       ,DST.FSFG                                               --冻结暂禁标志:src.FSFG
       ,DST.FZAM                                               --冻结金额:src.FZAM
       ,DST.SMFG                                               --印鉴挂失标志:src.SMFG
       ,DST.LKBL                                               --看管余额:src.LKBL
       ,DST.LKFG                                               --抵押看管标志:src.LKFG
       ,DST.STCD                                               --记录状态:src.STCD
       ,DST.ACCONT_TYPE                                        --账户类型:src.ACCONT_TYPE
       ,DST.MONTH_AVG                                          --月日均:src.MONTH_AVG
       ,DST.QUARTER_DAILY                                      --季日均:src.QUARTER_DAILY
       ,DST.YEAR_AVG                                           --年日均:src.YEAR_AVG
       ,DST.MVAL_RMB                                           --折人民币年日均:src.MVAL_RMB
       ,DST.SYS_NO                                             --业务系统:src.SYS_NO
       ,DST.PERD_UNIT                                          --存期单位:src.PERD_UNIT
       ,DST.INTE_BEAR_TERM                                     --计息期限:src.INTE_BEAR_TERM
       ,DST.INTE_BEAR_MODE                                     --计息方式:src.INTE_BEAR_MODE
       ,DST.INTEREST_SETTLEMENT                                --结息方式:src.INTEREST_SETTLEMENT
       ,DST.DEPOSIT_RESE_REQ                                   --缴存存款准备金方式:src.DEPOSIT_RESE_REQ
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.ITEM                                               --科目号:src.ITEM
       ,DST.SBIT                                               --子目:src.SBIT
       ,DST.SSIT                                               --细目:src.SSIT
       ,DST.CONNTR_NO                                          --管户员编号:src.CONNTR_NO
       ,DST.MONTH_RMB                                          --月均折人:src.MONTH_RMB
       ,DST.QUARTER_RMB                                        --季均折人:src.QUARTER_RMB
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.IS_CARD_TERM                                       --0:src.IS_CARD_TERM
       ,DST.CARD_NO                                            --卡号:src.CARD_NO
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.CARD_TYPE                                          --卡种:src.CARD_TYPE
   FROM ACRM_F_DP_SAVE_INFO DST 
   LEFT JOIN ACRM_F_DP_SAVE_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ODS_ACCT_NO         = DST.ODS_ACCT_NO 
    AND SRC.ACCONT_TYPE         = DST.ACCONT_TYPE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_DP_SAVE_INFO/"+V_DT+".parquet"
ACRM_F_DP_SAVE_INFO_INNTMP2=ACRM_F_DP_SAVE_INFO_INNTMP2.unionAll(ACRM_F_DP_SAVE_INFO_INNTMP1)
ACRM_F_DP_SAVE_INFO_INNTMP1.cache()
ACRM_F_DP_SAVE_INFO_INNTMP2.cache()
nrowsi = ACRM_F_DP_SAVE_INFO_INNTMP1.count()
nrowsa = ACRM_F_DP_SAVE_INFO_INNTMP2.count()
ACRM_F_DP_SAVE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_DP_SAVE_INFO_INNTMP1.unpersist()
ACRM_F_DP_SAVE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_SAVE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_DP_SAVE_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_DP_SAVE_INFO_BK/")

#任务[12] 001-07::
#更新客户经理编号
V_STEP = V_STEP + 1
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

sql = """
 SELECT C.AGREEMENT_ID          AS AGREEMENT_ID 
       ,C.PRODUCT_ID            AS PRODUCT_ID 
       ,C.ODS_ACCT_NO           AS ODS_ACCT_NO 
       ,C.CUST_ID               AS CUST_ID 
       ,C.CUST_NAME             AS CUST_NAME 
       ,C.CYNO                  AS CYNO 
       ,C.ORG_ID                AS ORG_ID 
       ,C.PERD                  AS PERD 
       ,C.INRT                  AS INRT 
       ,C.BSKD                  AS BSKD 
       ,C.AMAL                  AS AMAL 
       ,C.DETP                  AS DETP 
       ,C.OPEN_DT               AS OPEN_DT 
       ,C.OPEN_TL               AS OPEN_TL 
       ,C.CLOSE_DT              AS CLOSE_DT 
       ,C.CLOSE_TL              AS CLOSE_TL 
       ,C.S_CUST_NO             AS S_CUST_NO 
       ,C.TD_MU_DT              AS TD_MU_DT 
       ,C.UP_BAL                AS UP_BAL 
       ,C.UP_BAL_RMB            AS UP_BAL_RMB 
       ,C.BAL_RMB               AS BAL_RMB 
       ,C.BAL_US                AS BAL_US 
       ,C.TD_IR_TP              AS TD_IR_TP 
       ,C.AGREENMENT_RATE       AS AGREENMENT_RATE 
       ,C.ACCT_STATUS           AS ACCT_STATUS 
       ,C.TD_VL_DT              AS TD_VL_DT 
       ,C.MS_AC_BAL             AS MS_AC_BAL 
       ,C.LTDT                  AS LTDT 
       ,C.MSFG                  AS MSFG 
       ,C.FSFG                  AS FSFG 
       ,C.FZAM                  AS FZAM 
       ,C.SMFG                  AS SMFG 
       ,C.LKBL                  AS LKBL 
       ,C.LKFG                  AS LKFG 
       ,C.STCD                  AS STCD 
       ,C.ACCONT_TYPE           AS ACCONT_TYPE 
       ,C.MONTH_AVG             AS MONTH_AVG 
       ,C.QUARTER_DAILY         AS QUARTER_DAILY 
       ,C.YEAR_AVG              AS YEAR_AVG 
       ,C.MVAL_RMB              AS MVAL_RMB 
       ,C.SYS_NO                AS SYS_NO 
       ,C.PERD_UNIT             AS PERD_UNIT 
       ,C.INTE_BEAR_TERM        AS INTE_BEAR_TERM 
       ,C.INTE_BEAR_MODE        AS INTE_BEAR_MODE 
       ,C.INTEREST_SETTLEMENT   AS INTEREST_SETTLEMENT 
       ,C.DEPOSIT_RESE_REQ      AS DEPOSIT_RESE_REQ 
       ,C.CRM_DT                AS CRM_DT 
       ,C.ITEM                  AS ITEM 
       ,C.SBIT                  AS SBIT 
       ,C.SSIT                  AS SSIT 
       ,COALESCE(A.EC_CUST_MANAGER_ID, C.CONNTR_NO)                       AS CONNTR_NO 
       ,C.MONTH_RMB             AS MONTH_RMB 
       ,C.QUARTER_RMB           AS QUARTER_RMB 
       ,C.CUST_TYP              AS CUST_TYP 
       ,C.IS_CARD_TERM          AS IS_CARD_TERM 
       ,C.CARD_NO               AS CARD_NO 
       ,C.FR_ID                 AS FR_ID 
       ,C.CURR_IDEN             AS CURR_IDEN 
       ,C.CARD_TYPE             AS CARD_TYPE 
   FROM ACRM_F_DP_SAVE_INFO C                                  --负债协议
  LEFT JOIN (SELECT A.FR_ID,A.EC_ACCT_NO,MAX(B.EC_CUST_MANAGER_ID) AS EC_CUST_MANAGER_ID
	              FROM MID_CBOD_ECCMRAMR A,F_CM_CBOD_ECCMRCMS B
               WHERE a.EC_CMS_ID=B.EC_SEQ_NO
                 AND A.FR_ID=B.FR_ID
                 GROUP BY A.EC_ACCT_NO,A.FR_ID) A                               --
     ON C.ODS_ACCT_NO           = A.EC_ACCT_NO 
    AND C.FR_ID                 = A.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO = sqlContext.sql(sql)
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
dfn="ACRM_F_DP_SAVE_INFO/"+V_DT+".parquet"
ACRM_F_DP_SAVE_INFO.cache()
nrows = ACRM_F_DP_SAVE_INFO.count()
ACRM_F_DP_SAVE_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_SAVE_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-08::
V_STEP = V_STEP + 1
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
#更新卡号、卡种
sql = """
 SELECT A.AGREEMENT_ID          AS AGREEMENT_ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,A.ODS_ACCT_NO           AS ODS_ACCT_NO 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CYNO                  AS CYNO 
       ,A.ORG_ID                AS ORG_ID 
       ,A.PERD                  AS PERD 
       ,A.INRT                  AS INRT 
       ,A.BSKD                  AS BSKD 
       ,A.AMAL                  AS AMAL 
       ,A.DETP                  AS DETP 
       ,A.OPEN_DT               AS OPEN_DT 
       ,A.OPEN_TL               AS OPEN_TL 
       ,A.CLOSE_DT              AS CLOSE_DT 
       ,A.CLOSE_TL              AS CLOSE_TL 
       ,A.S_CUST_NO             AS S_CUST_NO 
       ,A.TD_MU_DT              AS TD_MU_DT 
       ,A.UP_BAL                AS UP_BAL 
       ,A.UP_BAL_RMB            AS UP_BAL_RMB 
       ,A.BAL_RMB               AS BAL_RMB 
       ,A.BAL_US                AS BAL_US 
       ,A.TD_IR_TP              AS TD_IR_TP 
       ,A.AGREENMENT_RATE       AS AGREENMENT_RATE 
       ,A.ACCT_STATUS           AS ACCT_STATUS 
       ,A.TD_VL_DT              AS TD_VL_DT 
       ,A.MS_AC_BAL             AS MS_AC_BAL 
       ,A.LTDT                  AS LTDT 
       ,A.MSFG                  AS MSFG 
       ,A.FSFG                  AS FSFG 
       ,A.FZAM                  AS FZAM 
       ,A.SMFG                  AS SMFG 
       ,A.LKBL                  AS LKBL 
       ,A.LKFG                  AS LKFG 
       ,A.STCD                  AS STCD 
       ,A.ACCONT_TYPE           AS ACCONT_TYPE 
       ,A.MONTH_AVG             AS MONTH_AVG 
       ,A.QUARTER_DAILY         AS QUARTER_DAILY 
       ,A.YEAR_AVG              AS YEAR_AVG 
       ,A.MVAL_RMB              AS MVAL_RMB 
       ,A.SYS_NO                AS SYS_NO 
       ,A.PERD_UNIT             AS PERD_UNIT 
       ,A.INTE_BEAR_TERM        AS INTE_BEAR_TERM 
       ,A.INTE_BEAR_MODE        AS INTE_BEAR_MODE 
       ,A.INTEREST_SETTLEMENT   AS INTEREST_SETTLEMENT 
       ,A.DEPOSIT_RESE_REQ      AS DEPOSIT_RESE_REQ 
       ,A.CRM_DT                    AS CRM_DT 
       ,A.ITEM                  AS ITEM 
       ,A.SBIT                  AS SBIT 
       ,A.SSIT                  AS SSIT 
       ,A.CONNTR_NO             AS CONNTR_NO 
       ,A.MONTH_RMB             AS MONTH_RMB 
       ,A.QUARTER_RMB           AS QUARTER_RMB 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.IS_CARD_TERM          AS IS_CARD_TERM 
       ,B.CARD_NO               AS CARD_NO 
       ,A.FR_ID                 AS FR_ID 
       ,A.CURR_IDEN             AS CURR_IDEN 
       ,B.CARD_TYPE             AS CARD_TYPE 
   FROM ACRM_F_DP_SAVE_INFO A                                  --负债协议表
  LEFT JOIN TMP_ACRM_F_DP_SAVE_INFO_01 B                      --负债协议临时表01
     ON A.ODS_ACCT_NO           = B.ODS_ACCT_NO 
    AND A.CYNO                  = B.CYNO 
    AND A.FR_ID                 = B.FR_ID 
	AND B.RN = 1  AND A.CRM_DT=V_DT
	 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO = sqlContext.sql(sql)
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
dfn="ACRM_F_DP_SAVE_INFO/"+V_DT+".parquet"
ACRM_F_DP_SAVE_INFO.cache()
nrows = ACRM_F_DP_SAVE_INFO.count()
ACRM_F_DP_SAVE_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_SAVE_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_DP_SAVE_INFO_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_DP_SAVE_INFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_DP_SAVE_INFO_BK/"+V_DT+".parquet")
