#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_ASSET_BUSI_PROTO').setMaster(sys.argv[2])
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

F_LN_XDXT_BUSINESS_CONTRACT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BUSINESS_CONTRACT/*')
F_LN_XDXT_BUSINESS_CONTRACT.registerTempTable("F_LN_XDXT_BUSINESS_CONTRACT")
F_LN_CBOD_LNLNSUPY = sqlContext.read.parquet(hdfs+'/F_LN_CBOD_LNLNSUPY/*')
F_LN_CBOD_LNLNSUPY.registerTempTable("F_LN_CBOD_LNLNSUPY")
OCRM_F_CM_EXCHANGE_RATE = sqlContext.read.parquet(hdfs+'/OCRM_F_CM_EXCHANGE_RATE/*')
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
F_LN_CBOD_LNLNSRCV = sqlContext.read.parquet(hdfs+'/F_LN_CBOD_LNLNSRCV/*')
F_LN_CBOD_LNLNSRCV.registerTempTable("F_LN_CBOD_LNLNSRCV")
MID_DP_CBOD_LNLNSJRN1 = sqlContext.read.parquet(hdfs+'/MID_DP_CBOD_LNLNSJRN1/*')
MID_DP_CBOD_LNLNSJRN1.registerTempTable("MID_DP_CBOD_LNLNSJRN1")
ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")
F_LN_XDXT_BUSINESS_DUEBILL = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BUSINESS_DUEBILL/*')
F_LN_XDXT_BUSINESS_DUEBILL.registerTempTable("F_LN_XDXT_BUSINESS_DUEBILL")
MID_F_LN_XDXT_CLASSIFY_RESULT = sqlContext.read.parquet(hdfs+'/MID_F_LN_XDXT_CLASSIFY_RESULT/*')
MID_F_LN_XDXT_CLASSIFY_RESULT.registerTempTable("MID_F_LN_XDXT_CLASSIFY_RESULT")
MID_DP_CBOD_LNLNSLNS = sqlContext.read.parquet(hdfs+'/MID_DP_CBOD_LNLNSLNS/*')
MID_DP_CBOD_LNLNSLNS.registerTempTable("MID_DP_CBOD_LNLNSLNS")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_ASSET_BUSI_PROTO/*.parquet")
#ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
#ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")



#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ACCOUNT                 AS ACCOUNT 
       ,CASE MONTH(V_DT)
           WHEN '1' THEN MONTH_BAL_SUM_1 / MONTH_DAYS_1 
           WHEN '2' THEN MONTH_BAL_SUM_2 / MONTH_DAYS_2 
           WHEN '3' THEN MONTH_BAL_SUM_3 / MONTH_DAYS_3 
           WHEN '4' THEN MONTH_BAL_SUM_4 / MONTH_DAYS_4 
           WHEN '5' THEN MONTH_BAL_SUM_5 / MONTH_DAYS_5 
           WHEN '6' THEN MONTH_BAL_SUM_6 / MONTH_DAYS_6 
           WHEN '7' THEN MONTH_BAL_SUM_7 / MONTH_DAYS_7 
           WHEN '8' THEN MONTH_BAL_SUM_8 / MONTH_DAYS_8 
           WHEN '9' THEN MONTH_BAL_SUM_9 / MONTH_DAYS_9 
           WHEN '10' THEN MONTH_BAL_SUM_10 / MONTH_DAYS_10 
           WHEN '11' THEN MONTH_BAL_SUM_11 / MONTH_DAYS_11 
           WHEN '12' THEN MONTH_BAL_SUM_12 / MONTH_DAYS_12 
          END AS MONTH_AVG 
       ,CASE QUARTER(V_DT)
           WHEN 1 THEN (COALESCE(MONTH_BAL_SUM_1,0)+COALESCE(MONTH_BAL_SUM_2,0)+COALESCE(MONTH_BAL_SUM_3,0))/(COALESCE(MONTH_DAYS_1,0)+COALESCE(MONTH_DAYS_2,0)+COALESCE(MONTH_DAYS_3,0))
           WHEN 2 THEN (COALESCE(MONTH_BAL_SUM_4,0)+COALESCE(MONTH_BAL_SUM_5,0)+COALESCE(MONTH_BAL_SUM_6,0))/(COALESCE(MONTH_DAYS_4,0)+COALESCE(MONTH_DAYS_5,0)+COALESCE(MONTH_DAYS_6,0))
           WHEN 3 THEN (COALESCE(MONTH_BAL_SUM_7,0)+COALESCE(MONTH_BAL_SUM_8,0)+COALESCE(MONTH_BAL_SUM_9,0))/(COALESCE(MONTH_DAYS_7,0)+COALESCE(MONTH_DAYS_8,0)+COALESCE(MONTH_DAYS_9,0))
           ELSE (COALESCE(MONTH_BAL_SUM_10,0)+COALESCE(MONTH_BAL_SUM_11,0)+COALESCE(MONTH_BAL_SUM_12,0))/(COALESCE(MONTH_DAYS_10,0)+COALESCE(MONTH_DAYS_11,0)+COALESCE(MONTH_DAYS_12,0))
           END AS QUARTER_DAILY
       ,(COALESCE(MONTH_BAL_SUM_1, 0) + COALESCE(MONTH_BAL_SUM_2, 0) + COALESCE(MONTH_BAL_SUM_3, 0) + COALESCE(MONTH_BAL_SUM_4, 0) + 
         COALESCE(MONTH_BAL_SUM_5, 0) + COALESCE(MONTH_BAL_SUM_6, 0) + COALESCE(MONTH_BAL_SUM_7, 0) + COALESCE(MONTH_BAL_SUM_8, 0) + 
         COALESCE(MONTH_BAL_SUM_9, 0) + COALESCE(MONTH_BAL_SUM_10, 0) + COALESCE(MONTH_BAL_SUM_11, 0) + COALESCE(MONTH_BAL_SUM_12, 0)) /
        (COALESCE(MONTH_DAYS_1, 0) + COALESCE(MONTH_DAYS_2, 0) + COALESCE(MONTH_DAYS_3, 0) + COALESCE(MONTH_DAYS_4, 0) + 
         COALESCE(MONTH_DAYS_5, 0) + COALESCE(MONTH_DAYS_6, 0) + COALESCE(MONTH_DAYS_7, 0) + COALESCE(MONTH_DAYS_8, 0) + 
         COALESCE(MONTH_DAYS_9, 0) + COALESCE(MONTH_DAYS_10, 0) + COALESCE(MONTH_DAYS_11, 0) + COALESCE(MONTH_DAYS_12, 0)) AS YEAR_AVG 
       ,FR_ID                   AS FR_ID 
       ,MONEY_TYPE              AS CURR 
   FROM ACRM_F_RE_LENDSUMAVGINFO A--贷款积数
  WHERE YEAR = TRIM(YEAR(V_DT)) """                             

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01 = sqlContext.sql(sql)
TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01.registerTempTable("TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01")
dfn="TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01/"+V_DT+".parquet"
TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01.cache()
nrows = TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01/*.parquet")
TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
#插入所有合同数据(其中：没有借据号且合同状态正常的为授信合同)
sql = """
 SELECT CAST(monotonically_increasing_id() AS decimal(19,0)) AS AGREEMENT_ID 
       ,COALESCE(E.ODS_CUST_ID, COALESCE(B.MFCUSTOMERID, A.CUSTOMERID)) AS CUST_ID 
       ,A.SERIALNO AS CONT_NO 
       ,A.ARTIFICIALNO AS CN_CONT_NO 
       ,B.SERIALNO AS ACCT_NO 
       ,E.ODS_CUST_ID AS CORE_CUST_NO 
       ,A.CUSTOMERNAME AS CORE_CUST_NAME 
       ,B.CREDITKIND AS LOAN_TYP 
       ,B.LOANTYPE AS LOAN_QUAL 
       ,'' AS ENT_LOAN_TYP 
       ,'' AS LOAN_POL_PRO_CLASS 
       ,'' AS SPEC_LOAN_TYE 
       ,A.BUSINESSTYPE AS PRODUCT_ID 
       ,A.VOUCHTYPE AS GRNT_TYP 
       ,'' AS GRNT_TYP2 
       ,'' AS GRNT_TYP3 
       ,A.DIRECTION AS LOAN_INVEST 
       ,D.EXCHANGE_RATE AS FOREXCH_RATE 
       ,A.BUSINESSSUM AS CONT_AMT 
       ,CAST(COALESCE(B.BUSINESSSUM, 0) AS decimal(24,6)) AS GIVE_OUT_AMT 
       ,CAST(0 AS DECIMAL(24,6)) AS RECOVER_AMT 
       ,CAST(A.BUSINESSRATE / 12 AS DECIMAL(15,8)) AS BM_MOT 
       ,CAST(CASE WHEN COALESCE(B.BUSINESSRATE, 0) = 0 THEN 0 ELSE B.ACTUALBUSINESSRATE / B.BUSINESSRATE END AS DECIMAL(15,8)) AS FLOAT_THAN 
       ,CAST(COALESCE(B.ACTUALBUSINESSRATE, A.BUSINESSRATE) / 12 AS DECIMAL(15,8)) AS MON_INTE_RATE 
       ,CAST(A.BASERATE * CYCUSE / 12 AS DECIMAL(15,8)) AS LATE_MOT 
       ,CAST(A.BASERATE *(1 + CONEXCFLOATPER) AS DECIMAL(15,8)) AS DEFUT_MOT 
       ,'' AS FLT_CYCL 
       ,'' AS ACTU_WITHDR 
       ,'' AS AGA_PRIC 
       ,CAST(0 AS DECIMAL(15,8)) AS AGA_PRIC_RAT 
       ,B.RETURNTYPE AS REPAY_MODE 
       ,'' AS REPAY_CYCL 
       ,'' AS INTC_CYCL 
       ,'' AS INTR_CORR_MODE 
       ,A.ISFINDIS AS YN_DISC 
       ,A.PURPOSE AS LOAN_USE 
       ,A.PAYSOURCE AS REPAY_ORG 
       ,CAST(COALESCE(B.BALANCE, 0) AS DECIMAL(24,6)) AS TAKE_CGT_LINE 
       ,CAST(COALESCE(B.BALANCE, 0) AS DECIMAL(24,6)) AS TAKE_MAX_GRNT_AMT 
       ,CASE WHEN A.CREDITAGGREEMENT IS  NOT NULL THEN '1' ELSE '2' END AS CGT_LINE_FLG 
       ,CASE WHEN A.CREDITCYCLE = 'Y' THEN '1' ELSE '2' END AS MAX_GRNT_FLG 
       ,CASE WHEN A.BUSINESSTYPE LIKE '1060%' THEN '1' ELSE '2' END AS SYN_LOAN_SIGN 
       ,'' AS BEFO_REPORT_FLG 
       ,'' AS MORT_PRO_NUM 
       ,'' AS SYN_PRO_NUM 
       ,A.PUTOUTDATE AS CONT_SIGN_DT 
       ,A.FLAG5 AS CONT_STS 
       ,A.MANAGEORGID AS SIGN_BANK 
       ,A.INPUTORGID AS AGENCY_BRAN 
       ,A.PUTOUTORGID AS LENDER 
       ,A.MANAGEORGID AS MANAGE_BRAN 
       ,CASE A.INPUTDATE WHEN NULL THEN NULL ELSE concat(SUBSTR(A.INPUTDATE,1,4),'-',SUBSTR(A.INPUTDATE,5,2),'-',SUBSTR(A.INPUTDATE,7,2)) END AS RECORD_DATE 
       ,A.INPUTUSERID AS REGISTRANT 
       ,CASE A.UPDATEDATE WHEN NULL THEN NULL ELSE concat(SUBSTR(A.INPUTDATE,1,4),'-',SUBSTR(A.INPUTDATE,5,2),'-',SUBSTR(A.INPUTDATE,7,2)) END AS FINAL_UPDATE_DATE 
       ,A.BUSINESSTYPE AS STATI_BUSIN_VAR 
       ,'' AS SUPP_LOAN_TYP 
       ,A.PAYTYPE AS MODE_PAYMENT 
       ,A.RATEFLOATTYPE AS YN_FIX_INTR 
       ,A.RATEFLOAT AS FIX_FLOAT_POINTS 
       ,A.BILLNUM AS BILL_QTY 
       ,A.BAILACCOUNT AS SDPT_ACCT_NO 
       ,CAST(A.PDGSUM AS DECIMAL(15,8)) AS FEE_RATIO 
       ,CAST(0 AS DECIMAL(15,8)) AS RSK_EXPO_RATIO 
       ,CAST(A.BAILRATIO AS DECIMAL(15,8)) AS SDPT_PATIO 
       ,A.BAILSUM AS SDPT_AMT 
       ,CAST(0 AS DECIMAL(24,6)) AS SDPT_BAL 
       ,CAST(0 AS DECIMAL(15,8)) AS DPTRPT_GRNT_PATIO 
       ,CAST(0 AS DECIMAL(24,6)) AS DPTRPT_FACE_AMT 
       ,CAST(0 AS DECIMAL(24,6)) AS DPTRPT_GRNT_AMT 
       ,CAST(0 AS DECIMAL(24,6)) AS DPTRPT_GRNT_BAL 
       ,CAST(0 AS DECIMAL(16,10)) AS BANNOTE_GRNT_PATIO 
       ,CAST(0 AS DECIMAL(24,6)) AS BANNOTE_GRNT_AMT 
       ,CAST(0 AS DECIMAL(24,6)) AS BANNOTE_GRNT_BAL 
       ,CAST(COALESCE(B.CANCELSUM, A.CANCELSUM) + COALESCE(B.CANCELINTEREST, A.CANCELINTEREST) AS DECIMAL(24,6)) AS WO_AMT 
       ,A.PAYTYPE AS SIGN_FLG 
       ,'' AS BILL_TYP 
       ,'' AS ACCEPTED_A_NUMBER 
       ,CAST(0 AS DECIMAL(24,6)) AS BANNOTE_FACE_AMT 
       ,A.BUSINESSTYPE AS FACTOR_TYP 
       ,'' AS BBK_DT 
       ,CAST(0 AS DECIMAL(15,8)) AS WEI_AVER_INTR 
       ,0 AS WEI_AVER_DAY 
       ,'' AS LOGRT_KIND 
       ,'' AS LOGRT_TYP 
       ,A.BUSINESSCURRENCY AS CURR 
       ,CAST(COALESCE(B.BALANCE, 0) AS DECIMAL(24,6)) AS BAL 
       ,B.PUTOUTDATE AS BEGIN_DATE 
       ,COALESCE(B.MATURITY, A.MATURITY) AS END_DATE 
       ,'' AS ASSURER 
       ,'' AS BENF 
       ,'' AS ASSET_SYS 
       ,CAST(0 AS DECIMAL(24,6)) AS MONTH_AVG 
       ,CAST(0 AS DECIMAL(24,6)) AS QUARTER_DAILY 
       ,CAST(0 AS DECIMAL(24,6)) AS YEAR_AVG 
       ,CAST(0 AS DECIMAL(24,6)) AS CHANGE_AMT 
       ,V_DT AS CRM_DT 
       ,CAST(LN_INT AS DECIMAL(24,6)) AS FACT_INT 
       ,CAST(0 AS DECIMAL(24,6)) AS SHOULD_INT 
       ,A.TERMYEAR AS TERMYEAR 
       ,A.TERMMONTH AS TERMMONTH 
       ,A.TERMDAY AS TERMDAY 
       ,'' AS POUNDAGE_CNCY 
       ,CAST(0 AS DECIMAL(24,6)) AS POUNDAGE_MONEY 
       ,CAST(0 AS DECIMAL(24,6)) AS TRANS_AMT_USD 
       ,CAST(0 AS DECIMAL(24,6)) AS FEE_AMT 
       ,C.LN_APCL_FLG AS LN_APCL_FLG 
       ,'2' AS IS_EXTEND 
       ,CASE WHEN COALESCE(B.BALANCE, 0) > 0  AND V_DT > concat(SUBSTR(B.MATURITY,1,4),'-',SUBSTR(B.MATURITY,5,2),'-',SUBSTR(B.MATURITY,7,2)) THEN '1' ELSE '2' END AS IS_OVERDUE 
       ,CASE WHEN COALESCE(B.SUBJECTNO, 0) LIKE '1308%' THEN '1' ELSE '2' END AS IS_ADVAN 
       ,CASE WHEN A.OCCURTYPE             = '020' THEN '1' ELSE '2' END AS IS_BORROW_NEW 
       ,CASE WHEN A.CLASSIFYRESULT IN('QL01', 'QL02')  AND((COALESCE(B.BALANCE, 0) > 0  AND V_DT > concat(SUBSTR(B.MATURITY,1,4),'-',SUBSTR(B.MATURITY,5,2),'-',SUBSTR(B.MATURITY,7,2))) 
             OR F.IS_INT_IN             = '1' 
             OR G.IS_INT_OUT            = '1' 
             OR COALESCE(B.SUBJECTNO, 0) LIKE '1308%' 
             OR A.OCCURTYPE = '020') THEN '1' ELSE '2' END AS IS_FLAW 
       ,B.SUBJECTNO AS SUBJECTNO 
       ,COALESCE(A.CLASSIFYRESULT, H.CURRENTFIVERESULT) AS CLASSIFYRESULT 
       ,A.MANAGEUSERID AS MANAGE_USERID 
       ,COALESCE(F.IS_INT_IN, '2') AS IS_INT_IN 
       ,COALESCE(G.IS_INT_OUT, '2') AS IS_INT_OUT 
       ,COALESCE(CASE WHEN B.TENCLASSIFYRESULT = 'X' then NULL ELSE B.TENCLASSIFYRESULT END,H.CURRENTTENRESULT) AS CURRENTTENRESULT
       ,CAST(0 AS DECIMAL(24,6)) AS MVAL_RMB 
       ,CAST(0 AS DECIMAL(24,6)) AS MONTH_RMB 
       ,CAST(0 AS DECIMAL(24,6)) AS QUARTER_RMB 
       ,CAST(COALESCE(B.BALANCE, 0) * D.EXCHANGE_RATE AS DECIMAL(24,6)) AS BAL_RMB 
       ,SUBSTR(COALESCE(E.ODS_CUST_ID, COALESCE(B.MFCUSTOMERID, A.CUSTOMERID)), 1, 1) AS CUST_TYP 
       ,A.FR_ID AS FR_ID 
   FROM F_LN_XDXT_BUSINESS_CONTRACT A                          --贷款合同表
   LEFT JOIN F_LN_XDXT_BUSINESS_DUEBILL B                      --借据表
     ON A.SERIALNO              = B.RELATIVESERIALNO2 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE D                         --汇率表
     ON A.BUSINESSCURRENCY      = D.CURRENCY_CD 
    AND D.OBJECT_CURRENCY_CD    = 'CNY' 
    AND D.EFF_DATE              = V_DT 
   LEFT JOIN(									--更新放款主档信息(通过贷款帐号关联)
         SELECT DISTINCT FR_ID 
               ,LN_LN_ACCT_NO 
               ,LN_APCL_FLG 
           FROM MID_DP_CBOD_LNLNSLNS) C                        --放款主档
     ON A.FR_ID                 = C.FR_ID 
    AND B.SERIALNO              = C.LN_LN_ACCT_NO 
   LEFT JOIN(									--更新客户号(取xdxt客户信息中 核心客户号)
         SELECT DISTINCT FR_ID,ODS_CUST_ID 
               ,SOURCE_CUST_ID 
           FROM OCRM_F_CI_SYS_RESOURCE 
          WHERE ODS_SYS_ID              = 'LNA') E             --系统来源中间表
     ON E.FR_ID                 = A.FR_ID 
    AND A.CUSTOMERID            = E.SOURCE_CUST_ID 
   LEFT JOIN (SELECT FR_ID,FK_LNLNS_KEY,  --更新是否表外欠息、是否表内欠息、是否瑕疵标志
                     CASE WHEN SUM(COALESCE(LN_INTRBL,0) - COALESCE(LN_ARFN_INT,0)) > 0 THEN '1' ELSE '2' END AS IS_INT_IN 
                FROM F_LN_CBOD_LNLNSUPY 
               WHERE LN_INT_TYP IN ('2','4') 
               GROUP BY FR_ID,FK_LNLNS_KEY) F                                              --表内欠息
     ON F.FR_ID                 = A.FR_ID 
    AND B.SERIALNO              = F.FK_LNLNS_KEY 
    AND F.IS_INT_IN             = '1' 
   LEFT JOIN (SELECT FR_ID,FK_LNLNS_KEY,
                     CASE WHEN SUM(COALESCE(LN_INTRBL,0) - COALESCE(LN_ARFN_INT,0)) > 0 THEN '1' ELSE '2' END AS IS_INT_OUT 
                FROM F_LN_CBOD_LNLNSUPY 
               WHERE LN_INT_TYP IN ('1','3','5','6') 
               GROUP BY FR_ID,FK_LNLNS_KEY) G                                              --表外欠息
     ON G.FR_ID                 = A.FR_ID 
    AND B.SERIALNO              = G.FK_LNLNS_KEY 
    AND G.IS_INT_OUT            = '1' 
   LEFT JOIN MID_F_LN_XDXT_CLASSIFY_RESULT H                   --资产风险分类结果中间表
     ON A.SERIALNO             = H.OBJECTNO 
    AND H.FR_ID                 = A.FR_ID 
   LEFT JOIN MID_DP_CBOD_LNLNSJRN1 I                           --放款帐卡档<利息资料>
     ON B.SERIALNO             = I.LN_LN_ACCT_NO 
  WHERE A.BUSINESSTYPE IS NOT NULL 
    AND A.BUSINESSTYPE <> '000000' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.sql(sql)
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
dfn="ACRM_F_CI_ASSET_BUSI_PROTO/"+V_DT+".parquet"
ACRM_F_CI_ASSET_BUSI_PROTO.cache()
nrows = ACRM_F_CI_ASSET_BUSI_PROTO.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_ASSET_BUSI_PROTO/*.parquet")
ACRM_F_CI_ASSET_BUSI_PROTO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_ASSET_BUSI_PROTO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_ASSET_BUSI_PROTO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-03::
V_STEP = V_STEP + 1

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")

TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01 = sqlContext.read.parquet(hdfs+'/TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01/*')
TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01.registerTempTable("TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01")
#更新月日均，季日均和年日均原币种
sql = """
 SELECT C.AGREEMENT_ID          AS AGREEMENT_ID 
       ,C.CUST_ID               AS CUST_ID 
       ,C.CONT_NO               AS CONT_NO 
       ,C.CN_CONT_NO            AS CN_CONT_NO 
       ,A.ACCOUNT               AS ACCT_NO 
       ,C.CORE_CUST_NO          AS CORE_CUST_NO 
       ,C.CORE_CUST_NAME        AS CORE_CUST_NAME 
       ,C.LOAN_TYP              AS LOAN_TYP 
       ,C.LOAN_QUAL             AS LOAN_QUAL 
       ,C.ENT_LOAN_TYP          AS ENT_LOAN_TYP 
       ,C.LOAN_POL_PRO_CLASS    AS LOAN_POL_PRO_CLASS 
       ,C.SPEC_LOAN_TYE         AS SPEC_LOAN_TYE 
       ,C.PRODUCT_ID            AS PRODUCT_ID 
       ,C.GRNT_TYP              AS GRNT_TYP 
       ,C.GRNT_TYP2             AS GRNT_TYP2 
       ,C.GRNT_TYP3             AS GRNT_TYP3 
       ,C.LOAN_INVEST           AS LOAN_INVEST 
       ,C.FOREXCH_RATE          AS FOREXCH_RATE 
       ,C.CONT_AMT              AS CONT_AMT 
       ,C.GIVE_OUT_AMT          AS GIVE_OUT_AMT 
       ,C.RECOVER_AMT           AS RECOVER_AMT 
       ,C.BM_MOT                AS BM_MOT 
       ,C.FLOAT_THAN            AS FLOAT_THAN 
       ,C.MON_INTE_RATE         AS MON_INTE_RATE 
       ,C.LATE_MOT              AS LATE_MOT 
       ,C.DEFUT_MOT             AS DEFUT_MOT 
       ,C.FLT_CYCL              AS FLT_CYCL 
       ,C.ACTU_WITHDR           AS ACTU_WITHDR 
       ,C.AGA_PRIC              AS AGA_PRIC 
       ,C.AGA_PRIC_RAT          AS AGA_PRIC_RAT 
       ,C.REPAY_MODE            AS REPAY_MODE 
       ,C.REPAY_CYCL            AS REPAY_CYCL 
       ,C.INTC_CYCL             AS INTC_CYCL 
       ,C.INTR_CORR_MODE        AS INTR_CORR_MODE 
       ,C.YN_DISC               AS YN_DISC 
       ,C.LOAN_USE              AS LOAN_USE 
       ,C.REPAY_ORG             AS REPAY_ORG 
       ,C.TAKE_CGT_LINE         AS TAKE_CGT_LINE 
       ,C.TAKE_MAX_GRNT_AMT     AS TAKE_MAX_GRNT_AMT 
       ,C.CGT_LINE_FLG          AS CGT_LINE_FLG 
       ,C.MAX_GRNT_FLG          AS MAX_GRNT_FLG 
       ,C.SYN_LOAN_SIGN         AS SYN_LOAN_SIGN 
       ,C.BEFO_REPORT_FLG       AS BEFO_REPORT_FLG 
       ,C.MORT_PRO_NUM          AS MORT_PRO_NUM 
       ,C.SYN_PRO_NUM           AS SYN_PRO_NUM 
       ,C.CONT_SIGN_DT          AS CONT_SIGN_DT 
       ,C.CONT_STS              AS CONT_STS 
       ,C.SIGN_BANK             AS SIGN_BANK 
       ,C.AGENCY_BRAN           AS AGENCY_BRAN 
       ,C.LENDER                AS LENDER 
       ,C.MANAGE_BRAN           AS MANAGE_BRAN 
       ,C.RECORD_DATE           AS RECORD_DATE 
       ,C.REGISTRANT            AS REGISTRANT 
       ,C.FINAL_UPDATE_DATE     AS FINAL_UPDATE_DATE 
       ,C.STATI_BUSIN_VAR       AS STATI_BUSIN_VAR 
       ,C.SUPP_LOAN_TYP         AS SUPP_LOAN_TYP 
       ,C.MODE_PAYMENT          AS MODE_PAYMENT 
       ,C.YN_FIX_INTR           AS YN_FIX_INTR 
       ,C.FIX_FLOAT_POINTS      AS FIX_FLOAT_POINTS 
       ,C.BILL_QTY              AS BILL_QTY 
       ,C.SDPT_ACCT_NO          AS SDPT_ACCT_NO 
       ,C.FEE_RATIO             AS FEE_RATIO 
       ,C.RSK_EXPO_RATIO        AS RSK_EXPO_RATIO 
       ,C.SDPT_PATIO            AS SDPT_PATIO 
       ,C.SDPT_AMT              AS SDPT_AMT 
       ,C.SDPT_BAL              AS SDPT_BAL 
       ,C.DPTRPT_GRNT_PATIO     AS DPTRPT_GRNT_PATIO 
       ,C.DPTRPT_FACE_AMT       AS DPTRPT_FACE_AMT 
       ,C.DPTRPT_GRNT_AMT       AS DPTRPT_GRNT_AMT 
       ,C.DPTRPT_GRNT_BAL       AS DPTRPT_GRNT_BAL 
       ,C.BANNOTE_GRNT_PATIO    AS BANNOTE_GRNT_PATIO 
       ,C.BANNOTE_GRNT_AMT      AS BANNOTE_GRNT_AMT 
       ,C.BANNOTE_GRNT_BAL      AS BANNOTE_GRNT_BAL 
       ,C.WO_AMT                AS WO_AMT 
       ,C.SIGN_FLG              AS SIGN_FLG 
       ,C.BILL_TYP              AS BILL_TYP 
       ,C.ACCEPTED_A_NUMBER     AS ACCEPTED_A_NUMBER 
       ,C.BANNOTE_FACE_AMT      AS BANNOTE_FACE_AMT 
       ,C.FACTOR_TYP            AS FACTOR_TYP 
       ,C.BBK_DT                AS BBK_DT 
       ,C.WEI_AVER_INTR         AS WEI_AVER_INTR 
       ,C.WEI_AVER_DAY          AS WEI_AVER_DAY 
       ,C.LOGRT_KIND            AS LOGRT_KIND 
       ,C.LOGRT_TYP             AS LOGRT_TYP 
       ,C.CURR                  AS CURR 
       ,C.BAL                   AS BAL 
       ,C.BEGIN_DATE            AS BEGIN_DATE 
       ,C.END_DATE              AS END_DATE 
       ,C.ASSURER               AS ASSURER 
       ,C.BENF                  AS BENF 
       ,C.ASSET_SYS             AS ASSET_SYS 
       ,A.MONTH_AVG             AS MONTH_AVG 
       ,A.QUARTER_DAILY         AS QUARTER_DAILY 
       ,A.YEAR_AVG              AS YEAR_AVG 
       ,C.CHANGE_AMT            AS CHANGE_AMT 
       ,C.CRM_DT                AS CRM_DT 
       ,C.FACT_INT              AS FACT_INT 
       ,C.SHOULD_INT            AS SHOULD_INT 
       ,C.TERMYEAR              AS TERMYEAR 
       ,C.TERMMONTH             AS TERMMONTH 
       ,C.TERMDAY               AS TERMDAY 
       ,C.POUNDAGE_CNCY         AS POUNDAGE_CNCY 
       ,C.POUNDAGE_MONEY        AS POUNDAGE_MONEY 
       ,C.TRANS_AMT_USD         AS TRANS_AMT_USD 
       ,C.FEE_AMT               AS FEE_AMT 
       ,C.LN_APCL_FLG           AS LN_APCL_FLG 
       ,C.IS_EXTEND             AS IS_EXTEND 
       ,C.IS_OVERDUE            AS IS_OVERDUE 
       ,C.IS_ADVAN              AS IS_ADVAN 
       ,C.IS_BORROW_NEW         AS IS_BORROW_NEW 
       ,C.IS_FLAW               AS IS_FLAW 
       ,C.SUBJECTNO             AS SUBJECTNO 
       ,C.CLASSIFYRESULT        AS CLASSIFYRESULT 
       ,C.MANAGE_USERID         AS MANAGE_USERID 
       ,C.IS_INT_IN             AS IS_INT_IN 
       ,C.IS_INT_OUT            AS IS_INT_OUT 
       ,C.CURRENTTENRESULT      AS CURRENTTENRESULT 
       ,A.YEAR_AVG * B.EXCHANGE_RATE         AS MVAL_RMB 
       ,A.MONTH_AVG * B.EXCHANGE_RATE         AS MONTH_RMB 
       ,A.QUARTER_DAILY * B.EXCHANGE_RATE         AS QUARTER_RMB 
       ,C.BAL_RMB               AS BAL_RMB 
       ,C.CUST_TYP              AS CUST_TYP 
       ,C.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO  C
   LEFT JOIN  TMP_ACRM_F_CI_ASSET_BUSI_PROTO_01 A                    --资产协议临时表01(贷款均值)
     ON C.FR_ID                 = A.FR_ID 
    AND C.ACCT_NO               = A.ACCOUNT 
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE B                         --汇率表
     ON C.CURR                  = B.CURRENCY_CD 
    AND B.OBJECT_CURRENCY_CD    = 'CNY' 
    AND B.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.sql(sql)
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
dfn="ACRM_F_CI_ASSET_BUSI_PROTO/"+V_DT+".parquet"
ACRM_F_CI_ASSET_BUSI_PROTO.cache()
nrows = ACRM_F_CI_ASSET_BUSI_PROTO.count()

ACRM_F_CI_ASSET_BUSI_PROTO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_ASSET_BUSI_PROTO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_ASSET_BUSI_PROTO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-04::
V_STEP = V_STEP + 1

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
#更新展期标识(事故档)
sql = """
 SELECT C.AGREEMENT_ID          AS AGREEMENT_ID 
       ,C.CUST_ID               AS CUST_ID 
       ,C.CONT_NO               AS CONT_NO 
       ,C.CN_CONT_NO            AS CN_CONT_NO 
       ,FK_LNLNS_KEY            AS ACCT_NO 
       ,C.CORE_CUST_NO          AS CORE_CUST_NO 
       ,C.CORE_CUST_NAME        AS CORE_CUST_NAME 
       ,C.LOAN_TYP              AS LOAN_TYP 
       ,C.LOAN_QUAL             AS LOAN_QUAL 
       ,C.ENT_LOAN_TYP          AS ENT_LOAN_TYP 
       ,C.LOAN_POL_PRO_CLASS    AS LOAN_POL_PRO_CLASS 
       ,C.SPEC_LOAN_TYE         AS SPEC_LOAN_TYE 
       ,C.PRODUCT_ID            AS PRODUCT_ID 
       ,C.GRNT_TYP              AS GRNT_TYP 
       ,C.GRNT_TYP2             AS GRNT_TYP2 
       ,C.GRNT_TYP3             AS GRNT_TYP3 
       ,C.LOAN_INVEST           AS LOAN_INVEST 
       ,C.FOREXCH_RATE          AS FOREXCH_RATE 
       ,C.CONT_AMT              AS CONT_AMT 
       ,C.GIVE_OUT_AMT          AS GIVE_OUT_AMT 
       ,C.RECOVER_AMT           AS RECOVER_AMT 
       ,C.BM_MOT                AS BM_MOT 
       ,C.FLOAT_THAN            AS FLOAT_THAN 
       ,C.MON_INTE_RATE         AS MON_INTE_RATE 
       ,C.LATE_MOT              AS LATE_MOT 
       ,C.DEFUT_MOT             AS DEFUT_MOT 
       ,C.FLT_CYCL              AS FLT_CYCL 
       ,C.ACTU_WITHDR           AS ACTU_WITHDR 
       ,C.AGA_PRIC              AS AGA_PRIC 
       ,C.AGA_PRIC_RAT          AS AGA_PRIC_RAT 
       ,C.REPAY_MODE            AS REPAY_MODE 
       ,C.REPAY_CYCL            AS REPAY_CYCL 
       ,C.INTC_CYCL             AS INTC_CYCL 
       ,C.INTR_CORR_MODE        AS INTR_CORR_MODE 
       ,C.YN_DISC               AS YN_DISC 
       ,C.LOAN_USE              AS LOAN_USE 
       ,C.REPAY_ORG             AS REPAY_ORG 
       ,C.TAKE_CGT_LINE         AS TAKE_CGT_LINE 
       ,C.TAKE_MAX_GRNT_AMT     AS TAKE_MAX_GRNT_AMT 
       ,C.CGT_LINE_FLG          AS CGT_LINE_FLG 
       ,C.MAX_GRNT_FLG          AS MAX_GRNT_FLG 
       ,C.SYN_LOAN_SIGN         AS SYN_LOAN_SIGN 
       ,C.BEFO_REPORT_FLG       AS BEFO_REPORT_FLG 
       ,C.MORT_PRO_NUM          AS MORT_PRO_NUM 
       ,C.SYN_PRO_NUM           AS SYN_PRO_NUM 
       ,C.CONT_SIGN_DT          AS CONT_SIGN_DT 
       ,C.CONT_STS              AS CONT_STS 
       ,C.SIGN_BANK             AS SIGN_BANK 
       ,C.AGENCY_BRAN           AS AGENCY_BRAN 
       ,C.LENDER                AS LENDER 
       ,C.MANAGE_BRAN           AS MANAGE_BRAN 
       ,C.RECORD_DATE           AS RECORD_DATE 
       ,C.REGISTRANT            AS REGISTRANT 
       ,C.FINAL_UPDATE_DATE     AS FINAL_UPDATE_DATE 
       ,C.STATI_BUSIN_VAR       AS STATI_BUSIN_VAR 
       ,C.SUPP_LOAN_TYP         AS SUPP_LOAN_TYP 
       ,C.MODE_PAYMENT          AS MODE_PAYMENT 
       ,C.YN_FIX_INTR           AS YN_FIX_INTR 
       ,C.FIX_FLOAT_POINTS      AS FIX_FLOAT_POINTS 
       ,C.BILL_QTY              AS BILL_QTY 
       ,C.SDPT_ACCT_NO          AS SDPT_ACCT_NO 
       ,C.FEE_RATIO             AS FEE_RATIO 
       ,C.RSK_EXPO_RATIO        AS RSK_EXPO_RATIO 
       ,C.SDPT_PATIO            AS SDPT_PATIO 
       ,C.SDPT_AMT              AS SDPT_AMT 
       ,C.SDPT_BAL              AS SDPT_BAL 
       ,C.DPTRPT_GRNT_PATIO     AS DPTRPT_GRNT_PATIO 
       ,C.DPTRPT_FACE_AMT       AS DPTRPT_FACE_AMT 
       ,C.DPTRPT_GRNT_AMT       AS DPTRPT_GRNT_AMT 
       ,C.DPTRPT_GRNT_BAL       AS DPTRPT_GRNT_BAL 
       ,C.BANNOTE_GRNT_PATIO    AS BANNOTE_GRNT_PATIO 
       ,C.BANNOTE_GRNT_AMT      AS BANNOTE_GRNT_AMT 
       ,C.BANNOTE_GRNT_BAL      AS BANNOTE_GRNT_BAL 
       ,C.WO_AMT                AS WO_AMT 
       ,C.SIGN_FLG              AS SIGN_FLG 
       ,C.BILL_TYP              AS BILL_TYP 
       ,C.ACCEPTED_A_NUMBER     AS ACCEPTED_A_NUMBER 
       ,C.BANNOTE_FACE_AMT      AS BANNOTE_FACE_AMT 
       ,C.FACTOR_TYP            AS FACTOR_TYP 
       ,C.BBK_DT                AS BBK_DT 
       ,C.WEI_AVER_INTR         AS WEI_AVER_INTR 
       ,C.WEI_AVER_DAY          AS WEI_AVER_DAY 
       ,C.LOGRT_KIND            AS LOGRT_KIND 
       ,C.LOGRT_TYP             AS LOGRT_TYP 
       ,C.CURR                  AS CURR 
       ,C.BAL                   AS BAL 
       ,C.BEGIN_DATE            AS BEGIN_DATE 
       ,C.END_DATE              AS END_DATE 
       ,C.ASSURER               AS ASSURER 
       ,C.BENF                  AS BENF 
       ,C.ASSET_SYS             AS ASSET_SYS 
       ,C.MONTH_AVG             AS MONTH_AVG 
       ,C.QUARTER_DAILY         AS QUARTER_DAILY 
       ,C.YEAR_AVG              AS YEAR_AVG 
       ,C.CHANGE_AMT            AS CHANGE_AMT 
       ,C.CRM_DT                AS CRM_DT 
       ,C.FACT_INT              AS FACT_INT 
       ,C.SHOULD_INT            AS SHOULD_INT 
       ,C.TERMYEAR              AS TERMYEAR 
       ,C.TERMMONTH             AS TERMMONTH 
       ,C.TERMDAY               AS TERMDAY 
       ,C.POUNDAGE_CNCY         AS POUNDAGE_CNCY 
       ,C.POUNDAGE_MONEY        AS POUNDAGE_MONEY 
       ,C.TRANS_AMT_USD         AS TRANS_AMT_USD 
       ,C.FEE_AMT               AS FEE_AMT 
       ,C.LN_APCL_FLG           AS LN_APCL_FLG 
       ,'1'                     AS IS_EXTEND 
       ,C.IS_OVERDUE            AS IS_OVERDUE 
       ,C.IS_ADVAN              AS IS_ADVAN 
       ,C.IS_BORROW_NEW         AS IS_BORROW_NEW 
       ,C.IS_FLAW               AS IS_FLAW 
       ,C.SUBJECTNO             AS SUBJECTNO 
       ,C.CLASSIFYRESULT        AS CLASSIFYRESULT 
       ,C.MANAGE_USERID         AS MANAGE_USERID 
       ,C.IS_INT_IN             AS IS_INT_IN 
       ,C.IS_INT_OUT            AS IS_INT_OUT 
       ,C.CURRENTTENRESULT      AS CURRENTTENRESULT 
       ,C.MVAL_RMB              AS MVAL_RMB 
       ,C.MONTH_RMB             AS MONTH_RMB 
       ,C.QUARTER_RMB           AS QUARTER_RMB 
       ,C.BAL_RMB               AS BAL_RMB 
       ,C.CUST_TYP              AS CUST_TYP 
       ,C.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO C
   LEFT JOIN
   (
         SELECT DISTINCT FK_LNLNS_KEY 
               ,FR_ID 
           FROM F_LN_CBOD_LNLNSRCV 
          WHERE LN_EVT_STS              = '1') A               --事故档
     ON C.FR_ID                 = A.FR_ID 
    AND C.ACCT_NO               = A.FK_LNLNS_KEY """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.sql(sql)
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
dfn="ACRM_F_CI_ASSET_BUSI_PROTO/"+V_DT+".parquet"
ACRM_F_CI_ASSET_BUSI_PROTO.cache()
nrows = ACRM_F_CI_ASSET_BUSI_PROTO.count()
ACRM_F_CI_ASSET_BUSI_PROTO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_ASSET_BUSI_PROTO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_ASSET_BUSI_PROTO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_ASSET_BUSI_PROTO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_CI_ASSET_BUSI_PROTO/"+V_DT+".parquet /"+dbname+"/ACRM_F_CI_ASSET_BUSI_PROTO_BK/")
