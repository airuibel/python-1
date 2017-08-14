#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_PROD_CHANNEL_INFO').setMaster(sys.argv[2])
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
#ACRM_F_CI_TRANS_DTL 增量导入
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_TRANS_DTL/"+V_DT+".parquet")


F_TX_EVENT = sqlContext.read.parquet(hdfs+'/F_TX_EVENT/*')
F_TX_EVENT.registerTempTable("F_TX_EVENT")
ACRM_F_DP_PAYROLL = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_PAYROLL/*')
ACRM_F_DP_PAYROLL.registerTempTable("ACRM_F_DP_PAYROLL")
ACRM_F_CI_NIN_TRANSLOG = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_NIN_TRANSLOG/*')
ACRM_F_CI_NIN_TRANSLOG.registerTempTable("ACRM_F_CI_NIN_TRANSLOG")
OCRM_F_CI_CUSTLNAINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUSTLNAINFO/*')
OCRM_F_CI_CUSTLNAINFO.registerTempTable("OCRM_F_CI_CUSTLNAINFO")
OCRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_DP_CARD_INFO/*')
OCRM_F_DP_CARD_INFO.registerTempTable("OCRM_F_DP_CARD_INFO")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT B.CUST_ID               AS CUST_ID 
       ,B.SA_CHANNEL_FLAG       AS SA_CHANNEL_FLAG 
       ,B.SA_TVARCHAR_DT        AS SA_TVARCHAR_DT 
       ,B.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_NIN_TRANSLOG B           --交易流水表--当日
  WHERE(B.CHANNEL_FLAG          = '02'  OR B.CHANNEL_FLAG          = '03') 
    AND B.SA_TVARCHAR_DT >= TRUNC(V_DT, 'MM') 
    AND B.SA_TVARCHAR_DT <= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_TRANS_DTL = sqlContext.sql(sql)
ACRM_F_CI_TRANS_DTL.registerTempTable("ACRM_F_CI_TRANS_DTL")
dfn="ACRM_F_CI_TRANS_DTL/"+V_DT+".parquet"
ACRM_F_CI_TRANS_DTL.cache()
nrows = ACRM_F_CI_TRANS_DTL.count()
ACRM_F_CI_TRANS_DTL.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_TRANS_DTL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_TRANS_DTL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_TRANS_DTL_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_CI_TRANS_DTL/"+V_DT+".parquet /"+dbname+"/ACRM_F_CI_TRANS_DTL_BK/")


#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,CASE WHEN B.CR_CUST_NO IS 
    NOT NULL THEN 'Y' ELSE 'N' END AS IS_JJK 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 2, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_DJK 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 5, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_WY 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 6, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_SJYH 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 7, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_DXYH 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 4, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_ZFB 
       ,CASE WHEN C.CUST_ID IS    NOT NULL 
    AND C.PRODUCT_ID            = '999SA000104' THEN 'Y' ELSE 'N' END AS IS_XNB 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 25, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_SB 
       ,CASE WHEN C.CUST_ID IS  NOT NULL 
    AND C.PRODUCT_ID            = '999CR000503' THEN 'Y' ELSE 'N' END AS IS_YB 
       ,CAST(0   AS INTEGER)   AS BNJJKBS 
       ,CAST(0   AS INTEGER)   AS BJJJKBS 
       ,CAST(0   AS INTEGER)   AS BYJJKBS 
       ,CAST(0   AS INTEGER)   AS BNDJKBS 
       ,CAST(0   AS INTEGER)   AS BJDJKBS 
       ,CAST(0   AS INTEGER)   AS BYDJKBS 
       ,CAST(0   AS INTEGER)   AS BNWYBS 
       ,CAST(0   AS INTEGER)   AS BJWYBS 
       ,CAST(0   AS INTEGER)   AS BYWYBS 
       ,CAST(0   AS INTEGER)   AS BNSJYHBS 
       ,CAST(0   AS INTEGER)   AS BJSJYHBS 
       ,CAST(0   AS INTEGER)   AS BYSJYHBS 
       ,V_DT                    AS ETLDT 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(''  AS DECIMAL(24, 6))   AS BYJJKFSE 
       ,CAST(''  AS DECIMAL(24, 6))   AS BJJJKFSE 
       ,CAST(''  AS DECIMAL(24, 6))   AS BNJJKFSE 
       ,CAST(''  AS INTEGER)   AS M_OCCUR 
       ,CAST(''  AS INTEGER)   AS Q_OCCUR 
       ,CAST(''  AS DECIMAL(24, 6))   AS M_OCCUR_AMT 
       ,CAST(''  AS DECIMAL(24, 6))   AS Q_OCCUR_AMT 
       ,CAST(''  AS INTEGER)   AS M_COM_OCCUR 
       ,CAST(''  AS INTEGER)   AS Q_COM_OCCUR 
       ,CAST(''  AS DECIMAL(24, 6))   AS M_COM_OCCUR_AMT 
       ,CAST(''  AS DECIMAL(24, 6))   AS Q_COM_OCCUR_AMT 
       ,CAST(''  AS INTEGER)   AS M_COM_INCOME 
       ,CAST(''  AS INTEGER)   AS Q_COM_INCOME 
       ,CAST(''  AS INTEGER)   AS M_COM_OUTCOME 
       ,CAST(''  AS INTEGER)   AS Q_COM_OUTCOME 
       ,CAST(''  AS DECIMAL(24, 6))   AS M_COM_INCOME_AMT 
       ,CAST(''  AS DECIMAL(24, 6))   AS Q_COM_INCOME_AMT 
       ,CAST(''  AS DECIMAL(24, 6))   AS M_COM_OUTCOME_AMT 
       ,CAST(''  AS DECIMAL(24, 6))   AS Q_COM_OUTCOME_AMT 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 8, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_ELEC 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 9, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_WATER 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 10, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_GAS 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 11, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_TV 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 12, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_WIRE 
       ,CASE WHEN SUBSTR(A.HOLD_PRO_FLAG, 26, 1)   = '1' THEN 'Y' ELSE 'N' END AS IS_JHSB 
   FROM OCRM_F_CI_CUST_DESC A              --统一客户信息
   LEFT JOIN 
 (SELECT DISTINCT CR_CUST_NO ,FR_ID
   FROM OCRM_F_DP_CARD_INFO) B              --负债协议表
     ON A.CUST_ID               = B.CR_CUST_NO 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN ACRM_F_DP_SAVE_INFO C         --
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.ACCT_STATUS           = '01' 
    AND C.PRODUCT_ID IN('999SA000104', '999CR000503') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO = sqlContext.sql(sql)
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
dfn="ACRM_A_PROD_CHANNEL_INFO/"+V_DT+".parquet"
ACRM_A_PROD_CHANNEL_INFO.cache()
nrows = ACRM_A_PROD_CHANNEL_INFO.count()
ACRM_A_PROD_CHANNEL_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_PROD_CHANNEL_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PROD_CHANNEL_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-03::
V_STEP = V_STEP + 1
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_YB                 AS IS_YB 
       ,CAST(B.TXN_NUM   AS INTEGER)   AS BNJJKBS 
       ,CAST(A.BJJJKBS               AS INTEGER)   AS BJJJKBS 
       ,CAST(A.BYJJKBS               AS INTEGER)   AS BYJJKBS 
       ,CAST(A.BNDJKBS               AS INTEGER)   AS BNDJKBS 
       ,CAST(A.BJDJKBS               AS INTEGER)   AS BJDJKBS 
       ,CAST(A.BYDJKBS               AS INTEGER)   AS BYDJKBS 
       ,CAST(A.BNWYBS                AS INTEGER)   AS BNWYBS 
       ,CAST(A.BJWYBS                AS INTEGER)   AS BJWYBS 
       ,CAST(A.BYWYBS                AS INTEGER)   AS BYWYBS 
       ,CAST(A.BNSJYHBS              AS INTEGER)   AS BNSJYHBS 
       ,CAST(A.BJSJYHBS              AS INTEGER)   AS BJSJYHBS 
       ,CAST(A.BYSJYHBS              AS INTEGER)   AS BYSJYHBS 
       ,A.ETLDT                 AS ETLDT 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(A.BYJJKFSE              AS DECIMAL(24, 6))   AS BYJJKFSE 
       ,CAST(A.BJJJKFSE              AS DECIMAL(24, 6))   AS BJJJKFSE 
       ,CAST(B.TXN_AMT   AS DECIMAL(24, 6))   AS BNJJKFSE 
       ,CAST(A.M_OCCUR               AS INTEGER)   AS M_OCCUR 
       ,CAST(A.Q_OCCUR               AS INTEGER)   AS Q_OCCUR 
       ,CAST(A.M_OCCUR_AMT           AS DECIMAL(24, 6))   AS M_OCCUR_AMT 
       ,CAST(A.Q_OCCUR_AMT           AS DECIMAL(24, 6))   AS Q_OCCUR_AMT 
       ,CAST(A.M_COM_OCCUR           AS INTEGER)   AS M_COM_OCCUR 
       ,CAST(A.Q_COM_OCCUR           AS INTEGER)   AS Q_COM_OCCUR 
       ,CAST(A.M_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS M_COM_OCCUR_AMT 
       ,CAST(A.Q_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS Q_COM_OCCUR_AMT 
       ,CAST(A.M_COM_INCOME          AS INTEGER)   AS M_COM_INCOME 
       ,CAST(A.Q_COM_INCOME          AS INTEGER)   AS Q_COM_INCOME 
       ,CAST(A.M_COM_OUTCOME         AS INTEGER)   AS M_COM_OUTCOME 
       ,CAST(A.Q_COM_OUTCOME         AS INTEGER)   AS Q_COM_OUTCOME 
       ,CAST(A.M_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS M_COM_INCOME_AMT 
       ,CAST(A.Q_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS Q_COM_INCOME_AMT 
       ,CAST(A.M_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS M_COM_OUTCOME_AMT 
       ,CAST(A.Q_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS Q_COM_OUTCOME_AMT 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.IS_JHSB               AS IS_JHSB 
   FROM ACRM_A_PROD_CHANNEL_INFO A         --ACRM_A_PROD_CHANNEL_INFO
  INNER JOIN (SELECT CUST_ID,FR_ID,
           count(1) AS TXN_NUM ,
           SUM( SA_TVARCHAR_AMT ) TXN_AMT
      FROM ACRM_F_CI_NIN_TRANSLOG
     WHERE SA_TVARCHAR_CRD_NO LIKE '6%' --借记卡
       AND SA_TVARCHAR_DT >= TRUNC(V_DT, 'YY')
       AND SA_TVARCHAR_DT <= V_DT
     GROUP BY CUST_ID,FR_ID ) B      --交易流水表--当日
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.registerTempTable("ACRM_A_PROD_CHANNEL_INFO_INNTMP1")


sql = """
 SELECT DST.CUST_ID     --客户号:src.CUST_ID
       ,DST.IS_JJK     --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK     --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY      --是否有网银:src.IS_WY
       ,DST.IS_SJYH    --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH    --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB     --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB     --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB      --是否认领社保卡:src.IS_SB
       ,DST.IS_YB      --是否开通医保:src.IS_YB
       ,DST.BNJJKBS    --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS    --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS    --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS    --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS    --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS    --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS     --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS     --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS     --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS   --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS   --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS   --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.ETLDT      --平台日期:src.ETLDT
       ,DST.FR_ID      --:src.FR_ID
       ,DST.BYJJKFSE   --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE   --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE   --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR    --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR    --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                    --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                    --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                    --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                    --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                   --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                   --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                  --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                  --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT               --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT               --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT              --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT              --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.IS_ELEC    --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER   --是否水费签约:src.IS_WATER
       ,DST.IS_GAS     --是否燃气签约:src.IS_GAS
       ,DST.IS_TV      --是否广电签约:src.IS_TV
       ,DST.IS_WIRE    --是否电信签约:src.IS_WIRE
       ,DST.IS_JHSB    --是否激活社保卡:src.IS_JHSB
   FROM ACRM_A_PROD_CHANNEL_INFO DST 
   LEFT JOIN ACRM_A_PROD_CHANNEL_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_PROD_CHANNEL_INFO/"+V_DT+".parquet"
UNION=UNION=ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unionAll(ACRM_A_PROD_CHANNEL_INFO_INNTMP1)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.cache()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.cache()
nrowsi = ACRM_A_PROD_CHANNEL_INFO_INNTMP1.count()
nrowsa = ACRM_A_PROD_CHANNEL_INFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.unpersist()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PROD_CHANNEL_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO_BK/")

#任务[12] 001-04::
V_STEP = V_STEP + 1
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_YB                 AS IS_YB 
       ,CAST(A.BNJJKBS               AS INTEGER)   AS BNJJKBS 
       ,CAST(B.TXN_NUM  AS INTEGER)   AS BJJJKBS 
       ,CAST(A.BYJJKBS               AS INTEGER)   AS BYJJKBS 
       ,CAST(A.BNDJKBS               AS INTEGER)   AS BNDJKBS 
       ,CAST(A.BJDJKBS               AS INTEGER)   AS BJDJKBS 
       ,CAST(A.BYDJKBS               AS INTEGER)   AS BYDJKBS 
       ,CAST(A.BNWYBS                AS INTEGER)   AS BNWYBS 
       ,CAST(A.BJWYBS                AS INTEGER)   AS BJWYBS 
       ,CAST(A.BYWYBS                AS INTEGER)   AS BYWYBS 
       ,CAST(A.BNSJYHBS              AS INTEGER)   AS BNSJYHBS 
       ,CAST(A.BJSJYHBS              AS INTEGER)   AS BJSJYHBS 
       ,CAST(A.BYSJYHBS              AS INTEGER)   AS BYSJYHBS 
       ,A.ETLDT                 AS ETLDT 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(A.BYJJKFSE              AS DECIMAL(24, 6))   AS BYJJKFSE 
       ,CAST(B.TXN_AMT  AS DECIMAL(24, 6))   AS BJJJKFSE 
       ,CAST(A.BNJJKFSE              AS DECIMAL(24, 6))   AS BNJJKFSE 
       ,CAST(A.M_OCCUR               AS INTEGER)   AS M_OCCUR 
       ,CAST(A.Q_OCCUR               AS INTEGER)   AS Q_OCCUR 
       ,CAST(A.M_OCCUR_AMT           AS DECIMAL(24, 6))   AS M_OCCUR_AMT 
       ,CAST(A.Q_OCCUR_AMT           AS DECIMAL(24, 6))   AS Q_OCCUR_AMT 
       ,CAST(A.M_COM_OCCUR           AS INTEGER)   AS M_COM_OCCUR 
       ,CAST(A.Q_COM_OCCUR           AS INTEGER)   AS Q_COM_OCCUR 
       ,CAST(A.M_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS M_COM_OCCUR_AMT 
       ,CAST(A.Q_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS Q_COM_OCCUR_AMT 
       ,CAST(A.M_COM_INCOME          AS INTEGER)   AS M_COM_INCOME 
       ,CAST(A.Q_COM_INCOME          AS INTEGER)   AS Q_COM_INCOME 
       ,CAST(A.M_COM_OUTCOME         AS INTEGER)   AS M_COM_OUTCOME 
       ,CAST(A.Q_COM_OUTCOME         AS INTEGER)   AS Q_COM_OUTCOME 
       ,CAST(A.M_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS M_COM_INCOME_AMT 
       ,CAST(A.Q_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS Q_COM_INCOME_AMT 
       ,CAST(A.M_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS M_COM_OUTCOME_AMT 
       ,CAST(A.Q_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS Q_COM_OUTCOME_AMT 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.IS_JHSB               AS IS_JHSB 
   FROM ACRM_A_PROD_CHANNEL_INFO A         --ACRM_A_PROD_CHANNEL_INFO
  INNER JOIN ( SELECT CUST_ID,FR_ID,
           count(1) AS TXN_NUM ,
           SUM( SA_TVARCHAR_AMT ) TXN_AMT
      FROM ACRM_F_CI_NIN_TRANSLOG
     WHERE SA_TVARCHAR_CRD_NO LIKE '6%' --借记卡
       AND SA_TVARCHAR_DT >= TRUNC(V_DT, 'Q') 
       AND SA_TVARCHAR_DT <= V_DT
     GROUP BY CUST_ID ,FR_ID ) B      --交易流水表--当日
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.registerTempTable("ACRM_A_PROD_CHANNEL_INFO_INNTMP1")

sql = """
 SELECT DST.CUST_ID     --客户号:src.CUST_ID
       ,DST.IS_JJK     --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK     --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY      --是否有网银:src.IS_WY
       ,DST.IS_SJYH    --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH    --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB     --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB     --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB      --是否认领社保卡:src.IS_SB
       ,DST.IS_YB      --是否开通医保:src.IS_YB
       ,DST.BNJJKBS    --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS    --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS    --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS    --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS    --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS    --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS     --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS     --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS     --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS   --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS   --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS   --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.ETLDT      --平台日期:src.ETLDT
       ,DST.FR_ID      --:src.FR_ID
       ,DST.BYJJKFSE   --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE   --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE   --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR    --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR    --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                    --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                    --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                    --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                    --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                   --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                   --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                  --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                  --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT               --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT               --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT              --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT              --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.IS_ELEC    --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER   --是否水费签约:src.IS_WATER
       ,DST.IS_GAS     --是否燃气签约:src.IS_GAS
       ,DST.IS_TV      --是否广电签约:src.IS_TV
       ,DST.IS_WIRE    --是否电信签约:src.IS_WIRE
       ,DST.IS_JHSB    --是否激活社保卡:src.IS_JHSB
   FROM ACRM_A_PROD_CHANNEL_INFO DST 
   LEFT JOIN ACRM_A_PROD_CHANNEL_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_PROD_CHANNEL_INFO/"+V_DT+".parquet"
UNION=ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unionAll(ACRM_A_PROD_CHANNEL_INFO_INNTMP1)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.cache()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.cache()
nrowsi = ACRM_A_PROD_CHANNEL_INFO_INNTMP1.count()
nrowsa = ACRM_A_PROD_CHANNEL_INFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.unpersist()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PROD_CHANNEL_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO_BK/")

#任务[12] 001-05::
V_STEP = V_STEP + 1
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_YB                 AS IS_YB 
       ,CAST(A.BNJJKBS               AS INTEGER)   AS BNJJKBS 
       ,CAST(A.BJJJKBS               AS INTEGER)   AS BJJJKBS 
       ,CAST(B.TXN_NUM   AS INTEGER)   AS BYJJKBS 
       ,CAST(A.BNDJKBS               AS INTEGER)   AS BNDJKBS 
       ,CAST(A.BJDJKBS               AS INTEGER)   AS BJDJKBS 
       ,CAST(A.BYDJKBS               AS INTEGER)   AS BYDJKBS 
       ,CAST(A.BNWYBS                AS INTEGER)   AS BNWYBS 
       ,CAST(A.BJWYBS                AS INTEGER)   AS BJWYBS 
       ,CAST(A.BYWYBS                AS INTEGER)   AS BYWYBS 
       ,CAST(A.BNSJYHBS              AS INTEGER)   AS BNSJYHBS 
       ,CAST(A.BJSJYHBS              AS INTEGER)   AS BJSJYHBS 
       ,CAST(A.BYSJYHBS              AS INTEGER)   AS BYSJYHBS 
       ,A.ETLDT                 AS ETLDT 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(B.TXN_AMT   AS DECIMAL(24, 6))   AS BYJJKFSE 
       ,CAST(A.BJJJKFSE              AS DECIMAL(24, 6))   AS BJJJKFSE 
       ,CAST(A.BNJJKFSE              AS DECIMAL(24, 6))   AS BNJJKFSE 
       ,CAST(A.M_OCCUR               AS INTEGER)   AS M_OCCUR 
       ,CAST(A.Q_OCCUR               AS INTEGER)   AS Q_OCCUR 
       ,CAST(A.M_OCCUR_AMT           AS DECIMAL(24, 6))   AS M_OCCUR_AMT 
       ,CAST(A.Q_OCCUR_AMT           AS DECIMAL(24, 6))   AS Q_OCCUR_AMT 
       ,CAST(A.M_COM_OCCUR           AS INTEGER)   AS M_COM_OCCUR 
       ,CAST(A.Q_COM_OCCUR           AS INTEGER)   AS Q_COM_OCCUR 
       ,CAST(A.M_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS M_COM_OCCUR_AMT 
       ,CAST(A.Q_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS Q_COM_OCCUR_AMT 
       ,CAST(A.M_COM_INCOME          AS INTEGER)   AS M_COM_INCOME 
       ,CAST(A.Q_COM_INCOME          AS INTEGER)   AS Q_COM_INCOME 
       ,CAST(A.M_COM_OUTCOME         AS INTEGER)   AS M_COM_OUTCOME 
       ,CAST(A.Q_COM_OUTCOME         AS INTEGER)   AS Q_COM_OUTCOME 
       ,CAST(A.M_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS M_COM_INCOME_AMT 
       ,CAST(A.Q_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS Q_COM_INCOME_AMT 
       ,CAST(A.M_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS M_COM_OUTCOME_AMT 
       ,CAST(A.Q_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS Q_COM_OUTCOME_AMT 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.IS_JHSB               AS IS_JHSB 
   FROM ACRM_A_PROD_CHANNEL_INFO A         --ACRM_A_PROD_CHANNEL_INFO
  INNER JOIN ( SELECT CUST_ID,FR_ID,
           count(1) AS TXN_NUM ,
           SUM( SA_TVARCHAR_AMT ) TXN_AMT
      FROM ACRM_F_CI_NIN_TRANSLOG
     WHERE SA_TVARCHAR_CRD_NO LIKE '6%' --借记卡
       AND SA_TVARCHAR_DT >= TRUNC(V_DT, 'MM') 
       AND SA_TVARCHAR_DT <= V_DT
     GROUP BY CUST_ID,FR_ID) B      --交易流水表--当日
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.registerTempTable("ACRM_A_PROD_CHANNEL_INFO_INNTMP1")


sql = """
 SELECT DST.CUST_ID     --客户号:src.CUST_ID
       ,DST.IS_JJK     --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK     --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY      --是否有网银:src.IS_WY
       ,DST.IS_SJYH    --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH    --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB     --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB     --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB      --是否认领社保卡:src.IS_SB
       ,DST.IS_YB      --是否开通医保:src.IS_YB
       ,DST.BNJJKBS    --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS    --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS    --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS    --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS    --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS    --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS     --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS     --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS     --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS   --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS   --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS   --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.ETLDT      --平台日期:src.ETLDT
       ,DST.FR_ID      --:src.FR_ID
       ,DST.BYJJKFSE   --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE   --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE   --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR    --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR    --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                    --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                    --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                    --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                    --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                   --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                   --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                  --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                  --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT               --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT               --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT              --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT              --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.IS_ELEC    --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER   --是否水费签约:src.IS_WATER
       ,DST.IS_GAS     --是否燃气签约:src.IS_GAS
       ,DST.IS_TV      --是否广电签约:src.IS_TV
       ,DST.IS_WIRE    --是否电信签约:src.IS_WIRE
       ,DST.IS_JHSB    --是否激活社保卡:src.IS_JHSB
   FROM ACRM_A_PROD_CHANNEL_INFO DST 
   LEFT JOIN ACRM_A_PROD_CHANNEL_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_PROD_CHANNEL_INFO/"+V_DT+".parquet"
UNION=ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unionAll(ACRM_A_PROD_CHANNEL_INFO_INNTMP1)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.cache()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.cache()
nrowsi = ACRM_A_PROD_CHANNEL_INFO_INNTMP1.count()
nrowsa = ACRM_A_PROD_CHANNEL_INFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.unpersist()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PROD_CHANNEL_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO_BK/")

#任务[12] 001-06::
V_STEP = V_STEP + 1
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_YB                 AS IS_YB 
       ,CAST(A.BNJJKBS               AS INTEGER)   AS BNJJKBS 
       ,CAST(A.BJJJKBS               AS INTEGER)   AS BJJJKBS 
       ,CAST(A.BYJJKBS               AS INTEGER)   AS BYJJKBS 
       ,CAST(B.Y_OCCUR   AS INTEGER)   AS BNDJKBS 
       ,CAST(B.Q_OCCUR  AS INTEGER)   AS BJDJKBS 
       ,CAST(B.M_OCCUR   AS INTEGER)   AS BYDJKBS 
       ,CAST(A.BNWYBS                AS INTEGER)   AS BNWYBS 
       ,CAST(A.BJWYBS                AS INTEGER)   AS BJWYBS 
       ,CAST(A.BYWYBS                AS INTEGER)   AS BYWYBS 
       ,CAST(A.BNSJYHBS              AS INTEGER)   AS BNSJYHBS 
       ,CAST(A.BJSJYHBS              AS INTEGER)   AS BJSJYHBS 
       ,CAST(A.BYSJYHBS              AS INTEGER)   AS BYSJYHBS 
       ,A.ETLDT                 AS ETLDT 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(A.BYJJKFSE              AS DECIMAL(24, 6))   AS BYJJKFSE 
       ,CAST(A.BJJJKFSE              AS DECIMAL(24, 6))   AS BJJJKFSE 
       ,CAST(A.BNJJKFSE              AS DECIMAL(24, 6))   AS BNJJKFSE 
       ,CAST(A.M_OCCUR               AS INTEGER)   AS M_OCCUR 
       ,CAST(A.Q_OCCUR               AS INTEGER)   AS Q_OCCUR 
       ,CAST(A.M_OCCUR_AMT           AS DECIMAL(24, 6))   AS M_OCCUR_AMT 
       ,CAST(A.Q_OCCUR_AMT           AS DECIMAL(24, 6))   AS Q_OCCUR_AMT 
       ,CAST(A.M_COM_OCCUR           AS INTEGER)   AS M_COM_OCCUR 
       ,CAST(A.Q_COM_OCCUR           AS INTEGER)   AS Q_COM_OCCUR 
       ,CAST(A.M_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS M_COM_OCCUR_AMT 
       ,CAST(A.Q_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS Q_COM_OCCUR_AMT 
       ,CAST(A.M_COM_INCOME          AS INTEGER)   AS M_COM_INCOME 
       ,CAST(A.Q_COM_INCOME          AS INTEGER)   AS Q_COM_INCOME 
       ,CAST(A.M_COM_OUTCOME         AS INTEGER)   AS M_COM_OUTCOME 
       ,CAST(A.Q_COM_OUTCOME         AS INTEGER)   AS Q_COM_OUTCOME 
       ,CAST(A.M_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS M_COM_INCOME_AMT 
       ,CAST(A.Q_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS Q_COM_INCOME_AMT 
       ,CAST(A.M_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS M_COM_OUTCOME_AMT 
       ,CAST(A.Q_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS Q_COM_OUTCOME_AMT 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.IS_JHSB               AS IS_JHSB 
   FROM ACRM_A_PROD_CHANNEL_INFO A         --ACRM_A_PROD_CHANNEL_INFO
  INNER JOIN (SELECT B.CUST_ID,B.FR_ID,
                       SUM (CASE WHEN SUBSTR (C.INP_DATE, 1, 4) = YEAR(V_DT) THEN 1 ELSE 0 END) Y_OCCUR,
                        SUM (CASE WHEN SUBSTR (C.INP_DATE, 1, 4) = YEAR(V_DT) 
						AND QUARTER(CONCAT(SUBSTR(C.INP_DATE,1,4),'-',SUBSTR(C.INP_DATE,5,2),'-',SUBSTR(C.INP_DATE,7,2)))=QUARTER(V_DT) THEN 1 ELSE 0 END) Q_OCCUR,
                        SUM(CASE WHEN SUBSTR(C.INP_DATE,1,6) = SUBSTR(V_DT,1,6) THEN 1 ELSE 0 END) M_OCCUR
                          FROM OCRM_F_CI_CUSTLNAINFO B,F_TX_EVENT C
                        WHERE   B.CARD_NO = C.CARD_NBR
                        GROUP BY B.CUST_ID,B.FR_ID) B       --客户信用信息
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.registerTempTable("ACRM_A_PROD_CHANNEL_INFO_INNTMP1")

sql = """
 SELECT DST.CUST_ID     --客户号:src.CUST_ID
       ,DST.IS_JJK     --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK     --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY      --是否有网银:src.IS_WY
       ,DST.IS_SJYH    --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH    --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB     --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB     --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB      --是否认领社保卡:src.IS_SB
       ,DST.IS_YB      --是否开通医保:src.IS_YB
       ,DST.BNJJKBS    --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS    --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS    --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS    --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS    --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS    --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS     --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS     --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS     --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS   --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS   --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS   --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.ETLDT      --平台日期:src.ETLDT
       ,DST.FR_ID      --:src.FR_ID
       ,DST.BYJJKFSE   --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE   --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE   --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR    --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR    --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                    --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                    --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                    --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                    --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                   --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                   --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                  --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                  --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT               --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT               --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT              --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT              --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.IS_ELEC    --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER   --是否水费签约:src.IS_WATER
       ,DST.IS_GAS     --是否燃气签约:src.IS_GAS
       ,DST.IS_TV      --是否广电签约:src.IS_TV
       ,DST.IS_WIRE    --是否电信签约:src.IS_WIRE
       ,DST.IS_JHSB    --是否激活社保卡:src.IS_JHSB
   FROM ACRM_A_PROD_CHANNEL_INFO DST 
   LEFT JOIN ACRM_A_PROD_CHANNEL_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_PROD_CHANNEL_INFO/"+V_DT+".parquet"
UNION=ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unionAll(ACRM_A_PROD_CHANNEL_INFO_INNTMP1)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.cache()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.cache()
nrowsi = ACRM_A_PROD_CHANNEL_INFO_INNTMP1.count()
nrowsa = ACRM_A_PROD_CHANNEL_INFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.unpersist()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PROD_CHANNEL_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO_BK/")

#任务[12] 001-07::
V_STEP = V_STEP + 1
ACRM_F_CI_TRANS_DTL = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_TRANS_DTL/*')
ACRM_F_CI_TRANS_DTL.registerTempTable("ACRM_F_CI_TRANS_DTL")
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_YB                 AS IS_YB 
       ,CAST(A.BNJJKBS               AS INTEGER)   AS BNJJKBS 
       ,CAST(A.BJJJKBS               AS INTEGER)   AS BJJJKBS 
       ,CAST(A.BYJJKBS               AS INTEGER)   AS BYJJKBS 
       ,CAST(A.BNDJKBS               AS INTEGER)   AS BNDJKBS 
       ,CAST(A.BJDJKBS               AS INTEGER)   AS BJDJKBS 
       ,CAST(A.BYDJKBS               AS INTEGER)   AS BYDJKBS 
       ,CAST(B.Y_OCCUR   AS INTEGER)   AS BNWYBS 
       ,CAST( B.Q_OCCUR   AS INTEGER)   AS BJWYBS 
       ,CAST(B.M_OCCUR   AS INTEGER)   AS BYWYBS 
       ,CAST(A.BNSJYHBS              AS INTEGER)   AS BNSJYHBS 
       ,CAST(A.BJSJYHBS              AS INTEGER)   AS BJSJYHBS 
       ,CAST(A.BYSJYHBS              AS INTEGER)   AS BYSJYHBS 
       ,A.ETLDT                 AS ETLDT 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(A.BYJJKFSE              AS DECIMAL(24, 6))   AS BYJJKFSE 
       ,CAST(A.BJJJKFSE              AS DECIMAL(24, 6))   AS BJJJKFSE 
       ,CAST(A.BNJJKFSE              AS DECIMAL(24, 6))   AS BNJJKFSE 
       ,CAST(A.M_OCCUR               AS INTEGER)   AS M_OCCUR 
       ,CAST(A.Q_OCCUR               AS INTEGER)   AS Q_OCCUR 
       ,CAST(A.M_OCCUR_AMT           AS DECIMAL(24, 6))   AS M_OCCUR_AMT 
       ,CAST(A.Q_OCCUR_AMT           AS DECIMAL(24, 6))   AS Q_OCCUR_AMT 
       ,CAST(A.M_COM_OCCUR           AS INTEGER)   AS M_COM_OCCUR 
       ,CAST(A.Q_COM_OCCUR           AS INTEGER)   AS Q_COM_OCCUR 
       ,CAST(A.M_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS M_COM_OCCUR_AMT 
       ,CAST(A.Q_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS Q_COM_OCCUR_AMT 
       ,CAST(A.M_COM_INCOME          AS INTEGER)   AS M_COM_INCOME 
       ,CAST(A.Q_COM_INCOME          AS INTEGER)   AS Q_COM_INCOME 
       ,CAST(A.M_COM_OUTCOME         AS INTEGER)   AS M_COM_OUTCOME 
       ,CAST(A.Q_COM_OUTCOME         AS INTEGER)   AS Q_COM_OUTCOME 
       ,CAST(A.M_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS M_COM_INCOME_AMT 
       ,CAST(A.Q_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS Q_COM_INCOME_AMT 
       ,CAST(A.M_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS M_COM_OUTCOME_AMT 
       ,CAST(A.Q_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS Q_COM_OUTCOME_AMT 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.IS_JHSB               AS IS_JHSB 
   FROM ACRM_A_PROD_CHANNEL_INFO A         --ACRM_A_PROD_CHANNEL_INFO
  INNER JOIN (SELECT B.CUST_ID,B.FR_ID,
            SUM (CASE WHEN SUBSTR (B.SA_TVARCHAR_DT, 1, 4) = YEAR(V_DT) THEN 1 ELSE 0 END) Y_OCCUR,
            SUM (CASE WHEN SUBSTR (B.SA_TVARCHAR_DT, 1, 4) = YEAR(V_DT) AND QUARTER(CONCAT(SUBSTR(B.SA_TVARCHAR_DT,1,4),'-',SUBSTR(B.SA_TVARCHAR_DT,5,2),'-',SUBSTR(B.SA_TVARCHAR_DT,7,2)))   = QUARTER(V_DT) THEN 1 ELSE 0 END) Q_OCCUR,
            SUM(CASE WHEN SUBSTR(B.SA_TVARCHAR_DT,1,6) = SUBSTR(V_DT,1,6) THEN 1 ELSE 0 END) M_OCCUR
       FROM ACRM_F_CI_TRANS_DTL B
      WHERE B.SA_CHANNEL_FLAG= '02' 
      GROUP BY B.CUST_ID ,B.FR_ID) B         --ACRM_F_CI_TRANS_DTL
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
	"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.registerTempTable("ACRM_A_PROD_CHANNEL_INFO_INNTMP1")


sql = """
 SELECT DST.CUST_ID     --客户号:src.CUST_ID
       ,DST.IS_JJK     --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK     --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY      --是否有网银:src.IS_WY
       ,DST.IS_SJYH    --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH    --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB     --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB     --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB      --是否认领社保卡:src.IS_SB
       ,DST.IS_YB      --是否开通医保:src.IS_YB
       ,DST.BNJJKBS    --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS    --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS    --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS    --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS    --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS    --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS     --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS     --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS     --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS   --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS   --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS   --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.ETLDT      --平台日期:src.ETLDT
       ,DST.FR_ID      --:src.FR_ID
       ,DST.BYJJKFSE   --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE   --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE   --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR    --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR    --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                    --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                    --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                    --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                    --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                   --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                   --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                  --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                  --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT               --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT               --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT              --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT              --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.IS_ELEC    --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER   --是否水费签约:src.IS_WATER
       ,DST.IS_GAS     --是否燃气签约:src.IS_GAS
       ,DST.IS_TV      --是否广电签约:src.IS_TV
       ,DST.IS_WIRE    --是否电信签约:src.IS_WIRE
       ,DST.IS_JHSB    --是否激活社保卡:src.IS_JHSB
   FROM ACRM_A_PROD_CHANNEL_INFO DST 
   LEFT JOIN ACRM_A_PROD_CHANNEL_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_PROD_CHANNEL_INFO/"+V_DT+".parquet"
UNION=ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unionAll(ACRM_A_PROD_CHANNEL_INFO_INNTMP1)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.cache()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.cache()
nrowsi = ACRM_A_PROD_CHANNEL_INFO_INNTMP1.count()
nrowsa = ACRM_A_PROD_CHANNEL_INFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.unpersist()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PROD_CHANNEL_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO_BK/")

#任务[12] 001-08::
V_STEP = V_STEP + 1
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_YB                 AS IS_YB 
       ,CAST(A.BNJJKBS               AS INTEGER)   AS BNJJKBS 
       ,CAST(A.BJJJKBS               AS INTEGER)   AS BJJJKBS 
       ,CAST(A.BYJJKBS               AS INTEGER)   AS BYJJKBS 
       ,CAST(A.BNDJKBS               AS INTEGER)   AS BNDJKBS 
       ,CAST(A.BJDJKBS               AS INTEGER)   AS BJDJKBS 
       ,CAST(A.BYDJKBS               AS INTEGER)   AS BYDJKBS 
       ,CAST(A.BNWYBS                AS INTEGER)   AS BNWYBS 
       ,CAST(A.BJWYBS                AS INTEGER)   AS BJWYBS 
       ,CAST(A.BYWYBS                AS INTEGER)   AS BYWYBS 
       ,CAST(B.Y_OCCUR  AS INTEGER)   AS BNSJYHBS 
       ,CAST(B.Q_OCCUR  AS INTEGER)   AS BJSJYHBS 
       ,CAST(B.M_OCCUR   AS INTEGER)   AS BYSJYHBS 
       ,A.ETLDT                 AS ETLDT 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(A.BYJJKFSE              AS DECIMAL(24, 6))   AS BYJJKFSE 
       ,CAST(A.BJJJKFSE              AS DECIMAL(24, 6))   AS BJJJKFSE 
       ,CAST(A.BNJJKFSE              AS DECIMAL(24, 6))   AS BNJJKFSE 
       ,CAST(A.M_OCCUR               AS INTEGER)   AS M_OCCUR 
       ,CAST(A.Q_OCCUR               AS INTEGER)   AS Q_OCCUR 
       ,CAST(A.M_OCCUR_AMT           AS DECIMAL(24, 6))   AS M_OCCUR_AMT 
       ,CAST(A.Q_OCCUR_AMT           AS DECIMAL(24, 6))   AS Q_OCCUR_AMT 
       ,CAST(A.M_COM_OCCUR           AS INTEGER)   AS M_COM_OCCUR 
       ,CAST(A.Q_COM_OCCUR           AS INTEGER)   AS Q_COM_OCCUR 
       ,CAST(A.M_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS M_COM_OCCUR_AMT 
       ,CAST(A.Q_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS Q_COM_OCCUR_AMT 
       ,CAST(A.M_COM_INCOME          AS INTEGER)   AS M_COM_INCOME 
       ,CAST(A.Q_COM_INCOME          AS INTEGER)   AS Q_COM_INCOME 
       ,CAST(A.M_COM_OUTCOME         AS INTEGER)   AS M_COM_OUTCOME 
       ,CAST(A.Q_COM_OUTCOME         AS INTEGER)   AS Q_COM_OUTCOME 
       ,CAST(A.M_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS M_COM_INCOME_AMT 
       ,CAST(A.Q_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS Q_COM_INCOME_AMT 
       ,CAST(A.M_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS M_COM_OUTCOME_AMT 
       ,CAST(A.Q_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS Q_COM_OUTCOME_AMT 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.IS_JHSB               AS IS_JHSB 
   FROM ACRM_A_PROD_CHANNEL_INFO A         --ACRM_A_PROD_CHANNEL_INFO
  INNER JOIN (SELECT B.CUST_ID,B.FR_ID,
            SUM (CASE WHEN SUBSTR (B.SA_TVARCHAR_DT, 1, 4) = YEAR(V_DT)  THEN 1 ELSE 0 END) Y_OCCUR,
            SUM (CASE WHEN SUBSTR (B.SA_TVARCHAR_DT, 1, 4) = YEAR(V_DT)  AND QUARTER(CONCAT(SUBSTR(B.SA_TVARCHAR_DT,1,4),'-',SUBSTR(B.SA_TVARCHAR_DT,5,2),'-',SUBSTR(B.SA_TVARCHAR_DT,7,2)))   = QUARTER(V_DT) THEN 1 ELSE 0 END)  Q_OCCUR,
            SUM(CASE WHEN SUBSTR(B.SA_TVARCHAR_DT,1,6) = SUBSTR(V_DT,1,6) THEN 1 ELSE 0 END) M_OCCUR
       FROM ACRM_F_CI_TRANS_DTL B
      WHERE B.SA_CHANNEL_FLAG= '03'
      GROUP BY B.CUST_ID,B.FR_ID) B         --ACRM_F_CI_TRANS_DTL
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.registerTempTable("ACRM_A_PROD_CHANNEL_INFO_INNTMP1")
  
sql = """
 SELECT DST.CUST_ID     --客户号:src.CUST_ID
       ,DST.IS_JJK     --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK     --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY      --是否有网银:src.IS_WY
       ,DST.IS_SJYH    --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH    --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB     --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB     --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB      --是否认领社保卡:src.IS_SB
       ,DST.IS_YB      --是否开通医保:src.IS_YB
       ,DST.BNJJKBS    --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS    --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS    --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS    --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS    --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS    --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS     --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS     --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS     --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS   --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS   --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS   --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.ETLDT      --平台日期:src.ETLDT
       ,DST.FR_ID      --:src.FR_ID
       ,DST.BYJJKFSE   --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE   --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE   --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR    --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR    --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                    --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                    --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                    --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                    --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                   --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                   --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                  --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                  --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT               --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT               --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT              --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT              --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.IS_ELEC    --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER   --是否水费签约:src.IS_WATER
       ,DST.IS_GAS     --是否燃气签约:src.IS_GAS
       ,DST.IS_TV      --是否广电签约:src.IS_TV
       ,DST.IS_WIRE    --是否电信签约:src.IS_WIRE
       ,DST.IS_JHSB    --是否激活社保卡:src.IS_JHSB
   FROM ACRM_A_PROD_CHANNEL_INFO DST 
   LEFT JOIN ACRM_A_PROD_CHANNEL_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_PROD_CHANNEL_INFO/"+V_DT+".parquet"
UNION=ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unionAll(ACRM_A_PROD_CHANNEL_INFO_INNTMP1)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.cache()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.cache()
nrowsi = ACRM_A_PROD_CHANNEL_INFO_INNTMP1.count()
nrowsa = ACRM_A_PROD_CHANNEL_INFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.unpersist()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PROD_CHANNEL_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO_BK/")

#任务[12] 001-09::
V_STEP = V_STEP + 1
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_YB                 AS IS_YB 
       ,CAST(A.BNJJKBS               AS INTEGER)   AS BNJJKBS 
       ,CAST(A.BJJJKBS               AS INTEGER)   AS BJJJKBS 
       ,CAST(A.BYJJKBS               AS INTEGER)   AS BYJJKBS 
       ,CAST(A.BNDJKBS               AS INTEGER)   AS BNDJKBS 
       ,CAST(A.BJDJKBS               AS INTEGER)   AS BJDJKBS 
       ,CAST(A.BYDJKBS               AS INTEGER)   AS BYDJKBS 
       ,CAST(A.BNWYBS                AS INTEGER)   AS BNWYBS 
       ,CAST(A.BJWYBS                AS INTEGER)   AS BJWYBS 
       ,CAST(A.BYWYBS                AS INTEGER)   AS BYWYBS 
       ,CAST(A.BNSJYHBS              AS INTEGER)   AS BNSJYHBS 
       ,CAST(A.BJSJYHBS              AS INTEGER)   AS BJSJYHBS 
       ,CAST(A.BYSJYHBS              AS INTEGER)   AS BYSJYHBS 
       ,A.ETLDT                 AS ETLDT 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(A.BYJJKFSE              AS DECIMAL(24, 6))   AS BYJJKFSE 
       ,CAST(A.BJJJKFSE              AS DECIMAL(24, 6))   AS BJJJKFSE 
       ,CAST(A.BNJJKFSE              AS DECIMAL(24, 6))   AS BNJJKFSE 
       ,CAST(B.M_OCCUR   AS INTEGER)   AS M_OCCUR 
       ,CAST( B.Q_OCCUR AS INTEGER)   AS Q_OCCUR 
       ,CAST(B.M_OCCUR_AMT   AS DECIMAL(24, 6))   AS M_OCCUR_AMT 
       ,CAST(B.Q_OCCUR_AMT  AS DECIMAL(24, 6))   AS Q_OCCUR_AMT 
       ,CAST(A.M_COM_OCCUR           AS INTEGER)   AS M_COM_OCCUR 
       ,CAST(A.Q_COM_OCCUR           AS INTEGER)   AS Q_COM_OCCUR 
       ,CAST(A.M_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS M_COM_OCCUR_AMT 
       ,CAST(A.Q_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS Q_COM_OCCUR_AMT 
       ,CAST(A.M_COM_INCOME          AS INTEGER)   AS M_COM_INCOME 
       ,CAST(A.Q_COM_INCOME          AS INTEGER)   AS Q_COM_INCOME 
       ,CAST(A.M_COM_OUTCOME         AS INTEGER)   AS M_COM_OUTCOME 
       ,CAST(A.Q_COM_OUTCOME         AS INTEGER)   AS Q_COM_OUTCOME 
       ,CAST(A.M_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS M_COM_INCOME_AMT 
       ,CAST(A.Q_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS Q_COM_INCOME_AMT 
       ,CAST(A.M_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS M_COM_OUTCOME_AMT 
       ,CAST(A.Q_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS Q_COM_OUTCOME_AMT 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.IS_JHSB               AS IS_JHSB 
   FROM ACRM_A_PROD_CHANNEL_INFO A         --ACRM_A_PROD_CHANNEL_INFO
  INNER JOIN (SELECT B.CUST_ID,B.FR_ID,
                          SUM (CASE WHEN SUBSTR (B.JYRQ, 1, 4) = YEAR(V_DT) AND QUARTER(CONCAT(SUBSTR(B.JYRQ,1,4),'-',SUBSTR(B.JYRQ,5,2),'-',SUBSTR(B.JYRQ,7,2)))   = QUARTER(V_DT) THEN 1 ELSE 0 END) Q_OCCUR,
                          SUM(CASE WHEN SUBSTR(B.JYRQ,1,6) = SUBSTR(V_DT,1,6) THEN 1 ELSE 0 END) M_OCCUR,
                          SUM (CASE WHEN SUBSTR (B.JYRQ, 1, 4) = YEAR(V_DT) AND QUARTER(CONCAT(SUBSTR(B.JYRQ,1,4),'-',SUBSTR(B.JYRQ,5,2),'-',SUBSTR(B.JYRQ,7,2)))   = QUARTER(V_DT) THEN JYJE ELSE 0 END) Q_OCCUR_AMT,
                          SUM(CASE WHEN SUBSTR(B.JYRQ,1,6) = SUBSTR(V_DT,1,6) THEN JYJE ELSE 0 END) M_OCCUR_AMT
                            FROM ACRM_F_DP_PAYROLL B
                          GROUP BY B.CUST_ID,B.FR_ID) B           --代发工资
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.registerTempTable("ACRM_A_PROD_CHANNEL_INFO_INNTMP1")

sql = """
 SELECT DST.CUST_ID     --客户号:src.CUST_ID
       ,DST.IS_JJK     --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK     --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY      --是否有网银:src.IS_WY
       ,DST.IS_SJYH    --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH    --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB     --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB     --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB      --是否认领社保卡:src.IS_SB
       ,DST.IS_YB      --是否开通医保:src.IS_YB
       ,DST.BNJJKBS    --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS    --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS    --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS    --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS    --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS    --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS     --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS     --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS     --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS   --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS   --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS   --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.ETLDT      --平台日期:src.ETLDT
       ,DST.FR_ID      --:src.FR_ID
       ,DST.BYJJKFSE   --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE   --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE   --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR    --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR    --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                    --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                    --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                    --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                    --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                   --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                   --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                  --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                  --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT               --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT               --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT              --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT              --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.IS_ELEC    --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER   --是否水费签约:src.IS_WATER
       ,DST.IS_GAS     --是否燃气签约:src.IS_GAS
       ,DST.IS_TV      --是否广电签约:src.IS_TV
       ,DST.IS_WIRE    --是否电信签约:src.IS_WIRE
       ,DST.IS_JHSB    --是否激活社保卡:src.IS_JHSB
   FROM ACRM_A_PROD_CHANNEL_INFO DST 
   LEFT JOIN ACRM_A_PROD_CHANNEL_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_PROD_CHANNEL_INFO/"+V_DT+".parquet"
UNION=ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unionAll(ACRM_A_PROD_CHANNEL_INFO_INNTMP1)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.cache()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.cache()
nrowsi = ACRM_A_PROD_CHANNEL_INFO_INNTMP1.count()
nrowsa = ACRM_A_PROD_CHANNEL_INFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.unpersist()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PROD_CHANNEL_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO_BK/")

#任务[12] 001-10::
V_STEP = V_STEP + 1
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_YB                 AS IS_YB 
       ,CAST(A.BNJJKBS               AS INTEGER)   AS BNJJKBS 
       ,CAST(A.BJJJKBS               AS INTEGER)   AS BJJJKBS 
       ,CAST(A.BYJJKBS               AS INTEGER)   AS BYJJKBS 
       ,CAST(A.BNDJKBS               AS INTEGER)   AS BNDJKBS 
       ,CAST(A.BJDJKBS               AS INTEGER)   AS BJDJKBS 
       ,CAST(A.BYDJKBS               AS INTEGER)   AS BYDJKBS 
       ,CAST(A.BNWYBS                AS INTEGER)   AS BNWYBS 
       ,CAST(A.BJWYBS                AS INTEGER)   AS BJWYBS 
       ,CAST(A.BYWYBS                AS INTEGER)   AS BYWYBS 
       ,CAST(A.BNSJYHBS              AS INTEGER)   AS BNSJYHBS 
       ,CAST(A.BJSJYHBS              AS INTEGER)   AS BJSJYHBS 
       ,CAST(A.BYSJYHBS              AS INTEGER)   AS BYSJYHBS 
       ,A.ETLDT                 AS ETLDT 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(A.BYJJKFSE              AS DECIMAL(24, 6))   AS BYJJKFSE 
       ,CAST(A.BJJJKFSE              AS DECIMAL(24, 6))   AS BJJJKFSE 
       ,CAST(A.BNJJKFSE              AS DECIMAL(24, 6))   AS BNJJKFSE 
       ,CAST(A.M_OCCUR               AS INTEGER)   AS M_OCCUR 
       ,CAST(A.Q_OCCUR               AS INTEGER)   AS Q_OCCUR 
       ,CAST(A.M_OCCUR_AMT           AS DECIMAL(24, 6))   AS M_OCCUR_AMT 
       ,CAST(A.Q_OCCUR_AMT           AS DECIMAL(24, 6))   AS Q_OCCUR_AMT 
       ,CAST(A.M_COM_OCCUR           AS INTEGER)   AS M_COM_OCCUR 
       ,CAST(B.Q_COM_OCCUR   AS INTEGER)   AS Q_COM_OCCUR 
       ,CAST(A.M_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS M_COM_OCCUR_AMT 
       ,CAST(B.Q_COM_OCCUR_AMT  AS DECIMAL(24, 6))   AS Q_COM_OCCUR_AMT 
       ,CAST(A.M_COM_INCOME          AS INTEGER)   AS M_COM_INCOME 
       ,CAST(B.Q_COM_INCOME   AS INTEGER)   AS Q_COM_INCOME 
       ,CAST(A.M_COM_OUTCOME         AS INTEGER)   AS M_COM_OUTCOME 
       ,CAST(B.Q_COM_OUTCOME   AS INTEGER)   AS Q_COM_OUTCOME 
       ,CAST(A.M_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS M_COM_INCOME_AMT 
       ,CAST(B.Q_COM_INCOME_AMT   AS DECIMAL(24, 6))   AS Q_COM_INCOME_AMT 
       ,CAST(A.M_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS M_COM_OUTCOME_AMT 
       ,CAST(B.Q_COM_OUTCOME_AMT   AS DECIMAL(24, 6))   AS Q_COM_OUTCOME_AMT 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.IS_JHSB               AS IS_JHSB 
   FROM ACRM_A_PROD_CHANNEL_INFO A         --ACRM_A_PROD_CHANNEL_INFO
  INNER JOIN (SELECT B.CUST_ID,B.FR_ID ,
                          COUNT(1) AS Q_COM_OCCUR,
                          SUM(SA_TVARCHAR_AMT) AS Q_COM_OCCUR_AMT,
                          SUM(CASE WHEN  B.SA_CR_AMT > 0 THEN 1 ELSE 0 END) AS Q_COM_INCOME,
                          SUM(CASE WHEN  B.SA_CR_AMT > 0 THEN B.SA_TVARCHAR_AMT ELSE 0 END) AS Q_COM_INCOME_AMT,
                          SUM(CASE WHEN  B.SA_DR_AMT > 0 THEN 1 ELSE 0 END) AS Q_COM_OUTCOME,
                          SUM(CASE WHEN  B.SA_DR_AMT > 0 THEN B.SA_TVARCHAR_AMT ELSE 0 END) AS Q_COM_OUTCOME_AMT
             FROM ACRM_F_CI_NIN_TRANSLOG B
            WHERE  B.SA_EC_FLG = '0'  --非冲正
               AND B.CUST_TYP = '2'
               AND B.SA_TVARCHAR_DT >=TRUNC(V_DT, 'Q')
               AND B.SA_TVARCHAR_DT <= V_DT
          GROUP BY B.CUST_ID,B.FR_ID ) B      --交易流水表--当日
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
	        """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.registerTempTable("ACRM_A_PROD_CHANNEL_INFO_INNTMP1")


sql = """
 SELECT DST.CUST_ID     --客户号:src.CUST_ID
       ,DST.IS_JJK     --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK     --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY      --是否有网银:src.IS_WY
       ,DST.IS_SJYH    --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH    --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB     --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB     --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB      --是否认领社保卡:src.IS_SB
       ,DST.IS_YB      --是否开通医保:src.IS_YB
       ,DST.BNJJKBS    --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS    --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS    --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS    --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS    --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS    --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS     --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS     --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS     --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS   --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS   --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS   --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.ETLDT      --平台日期:src.ETLDT
       ,DST.FR_ID      --:src.FR_ID
       ,DST.BYJJKFSE   --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE   --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE   --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR    --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR    --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                    --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                    --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                    --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                    --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                   --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                   --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                  --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                  --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT               --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT               --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT              --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT              --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.IS_ELEC    --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER   --是否水费签约:src.IS_WATER
       ,DST.IS_GAS     --是否燃气签约:src.IS_GAS
       ,DST.IS_TV      --是否广电签约:src.IS_TV
       ,DST.IS_WIRE    --是否电信签约:src.IS_WIRE
       ,DST.IS_JHSB    --是否激活社保卡:src.IS_JHSB
   FROM ACRM_A_PROD_CHANNEL_INFO DST 
   LEFT JOIN ACRM_A_PROD_CHANNEL_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_PROD_CHANNEL_INFO/"+V_DT+".parquet"
UNION=ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unionAll(ACRM_A_PROD_CHANNEL_INFO_INNTMP1)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.cache()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.cache()
nrowsi = ACRM_A_PROD_CHANNEL_INFO_INNTMP1.count()
nrowsa = ACRM_A_PROD_CHANNEL_INFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.unpersist()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PROD_CHANNEL_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO_BK/")

#任务[12] 001-11::
V_STEP = V_STEP + 1
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.IS_JJK                AS IS_JJK 
       ,A.IS_DJK                AS IS_DJK 
       ,A.IS_WY                 AS IS_WY 
       ,A.IS_SJYH               AS IS_SJYH 
       ,A.IS_DXYH               AS IS_DXYH 
       ,A.IS_ZFB                AS IS_ZFB 
       ,A.IS_XNB                AS IS_XNB 
       ,A.IS_SB                 AS IS_SB 
       ,A.IS_YB                 AS IS_YB 
       ,CAST(A.BNJJKBS               AS INTEGER)   AS BNJJKBS 
       ,CAST(A.BJJJKBS               AS INTEGER)   AS BJJJKBS 
       ,CAST(A.BYJJKBS               AS INTEGER)   AS BYJJKBS 
       ,CAST(A.BNDJKBS               AS INTEGER)   AS BNDJKBS 
       ,CAST(A.BJDJKBS               AS INTEGER)   AS BJDJKBS 
       ,CAST(A.BYDJKBS               AS INTEGER)   AS BYDJKBS 
       ,CAST(A.BNWYBS                AS INTEGER)   AS BNWYBS 
       ,CAST(A.BJWYBS                AS INTEGER)   AS BJWYBS 
       ,CAST(A.BYWYBS                AS INTEGER)   AS BYWYBS 
       ,CAST(A.BNSJYHBS              AS INTEGER)   AS BNSJYHBS 
       ,CAST(A.BJSJYHBS              AS INTEGER)   AS BJSJYHBS 
       ,CAST(A.BYSJYHBS              AS INTEGER)   AS BYSJYHBS 
       ,A.ETLDT                 AS ETLDT 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(A.BYJJKFSE              AS DECIMAL(24, 6))   AS BYJJKFSE 
       ,CAST(A.BJJJKFSE              AS DECIMAL(24, 6))   AS BJJJKFSE 
       ,CAST(A.BNJJKFSE              AS DECIMAL(24, 6))   AS BNJJKFSE 
       ,CAST(A.M_OCCUR               AS INTEGER)   AS M_OCCUR 
       ,CAST(A.Q_OCCUR               AS INTEGER)   AS Q_OCCUR 
       ,CAST(A.M_OCCUR_AMT           AS DECIMAL(24, 6))   AS M_OCCUR_AMT 
       ,CAST(A.Q_OCCUR_AMT           AS DECIMAL(24, 6))   AS Q_OCCUR_AMT 
       ,CAST(B.M_COM_OCCUR  AS INTEGER)   AS M_COM_OCCUR 
       ,CAST(A.Q_COM_OCCUR           AS INTEGER)   AS Q_COM_OCCUR 
       ,CAST(B.M_COM_OCCUR_AMT  AS DECIMAL(24, 6))   AS M_COM_OCCUR_AMT 
       ,CAST(A.Q_COM_OCCUR_AMT       AS DECIMAL(24, 6))   AS Q_COM_OCCUR_AMT 
       ,CAST(B.M_COM_INCOME  AS INTEGER)   AS M_COM_INCOME 
       ,CAST(A.Q_COM_INCOME          AS INTEGER)   AS Q_COM_INCOME 
       ,CAST(B.M_COM_OUTCOME  AS INTEGER)   AS M_COM_OUTCOME 
       ,CAST(A.Q_COM_OUTCOME         AS INTEGER)   AS Q_COM_OUTCOME 
       ,CAST(B.M_COM_INCOME_AMT AS DECIMAL(24, 6))   AS M_COM_INCOME_AMT 
       ,CAST(A.Q_COM_INCOME_AMT      AS DECIMAL(24, 6))   AS Q_COM_INCOME_AMT 
       ,CAST(B.M_COM_OUTCOME_AMT  AS DECIMAL(24, 6))   AS M_COM_OUTCOME_AMT 
       ,CAST(A.Q_COM_OUTCOME_AMT     AS DECIMAL(24, 6))   AS Q_COM_OUTCOME_AMT 
       ,A.IS_ELEC               AS IS_ELEC 
       ,A.IS_WATER              AS IS_WATER 
       ,A.IS_GAS                AS IS_GAS 
       ,A.IS_TV                 AS IS_TV 
       ,A.IS_WIRE               AS IS_WIRE 
       ,A.IS_JHSB               AS IS_JHSB 
   FROM ACRM_A_PROD_CHANNEL_INFO A         --ACRM_A_PROD_CHANNEL_INFO
  INNER JOIN (SELECT B.CUST_ID,B.FR_ID,
                   COUNT(1) AS M_COM_OCCUR,
                   SUM(SA_TVARCHAR_AMT) AS M_COM_OCCUR_AMT,
                   SUM(CASE WHEN B.SA_CR_AMT > 0 THEN 1 ELSE 0 END) AS M_COM_INCOME,
                   SUM(CASE WHEN B.SA_CR_AMT > 0 THEN B.SA_TVARCHAR_AMT ELSE 0 END) AS M_COM_INCOME_AMT,
                   SUM(CASE WHEN B.SA_DR_AMT > 0 THEN 1 ELSE 0 END) AS M_COM_OUTCOME,
                   SUM(CASE WHEN B.SA_DR_AMT > 0 THEN B.SA_TVARCHAR_AMT ELSE 0 END) AS M_COM_OUTCOME_AMT
              FROM ACRM_F_CI_NIN_TRANSLOG B
            WHERE B.SA_EC_FLG = '0'  --非冲正
               AND B.CUST_TYP = '2'
               AND B.SA_TVARCHAR_DT >= TRUNC(V_DT, 'MM') 
               AND B.SA_TVARCHAR_DT <=  V_DT
          GROUP BY B.CUST_ID,B.FR_ID) B      --交易流水表--当日
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 	 
	"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.registerTempTable("ACRM_A_PROD_CHANNEL_INFO_INNTMP1")

sql = """
 SELECT DST.CUST_ID     --客户号:src.CUST_ID
       ,DST.IS_JJK     --是否有借记卡:src.IS_JJK
       ,DST.IS_DJK     --是否有贷记卡:src.IS_DJK
       ,DST.IS_WY      --是否有网银:src.IS_WY
       ,DST.IS_SJYH    --是否有手机银行:src.IS_SJYH
       ,DST.IS_DXYH    --是否开通短信银行:src.IS_DXYH
       ,DST.IS_ZFB     --是否开通支付宝:src.IS_ZFB
       ,DST.IS_XNB     --是否是开通新农保:src.IS_XNB
       ,DST.IS_SB      --是否认领社保卡:src.IS_SB
       ,DST.IS_YB      --是否开通医保:src.IS_YB
       ,DST.BNJJKBS    --本年借记卡发生笔数:src.BNJJKBS
       ,DST.BJJJKBS    --本季借记卡发生笔数:src.BJJJKBS
       ,DST.BYJJKBS    --本月借记卡发生笔数:src.BYJJKBS
       ,DST.BNDJKBS    --本年贷记卡发生笔数:src.BNDJKBS
       ,DST.BJDJKBS    --本季贷记卡发生笔数:src.BJDJKBS
       ,DST.BYDJKBS    --本月贷记卡发生笔数:src.BYDJKBS
       ,DST.BNWYBS     --本年网银发生笔数:src.BNWYBS
       ,DST.BJWYBS     --本季网银发生笔数:src.BJWYBS
       ,DST.BYWYBS     --本月网银发生笔数:src.BYWYBS
       ,DST.BNSJYHBS   --本年手机银行发生笔数:src.BNSJYHBS
       ,DST.BJSJYHBS   --本季手机银行发生笔数:src.BJSJYHBS
       ,DST.BYSJYHBS   --本月手机银行发生笔数:src.BYSJYHBS
       ,DST.ETLDT      --平台日期:src.ETLDT
       ,DST.FR_ID      --:src.FR_ID
       ,DST.BYJJKFSE   --本月借记卡发生额:src.BYJJKFSE
       ,DST.BJJJKFSE   --本季借记卡发生额:src.BJJJKFSE
       ,DST.BNJJKFSE   --本年借记卡发生额:src.BNJJKFSE
       ,DST.M_OCCUR    --本月代发工资次数:src.M_OCCUR
       ,DST.Q_OCCUR    --本季代发工资次数:src.Q_OCCUR
       ,DST.M_OCCUR_AMT                    --本月代发工资金额:src.M_OCCUR_AMT
       ,DST.Q_OCCUR_AMT                    --本季代发工资金额:src.Q_OCCUR_AMT
       ,DST.M_COM_OCCUR                    --本月账户资金流动次数:src.M_COM_OCCUR
       ,DST.Q_COM_OCCUR                    --本季账户资金流动次数:src.Q_COM_OCCUR
       ,DST.M_COM_OCCUR_AMT                --本月账户资金流动金额:src.M_COM_OCCUR_AMT
       ,DST.Q_COM_OCCUR_AMT                --本季账户资金流动金额:src.Q_COM_OCCUR_AMT
       ,DST.M_COM_INCOME                   --本月账户资金流入次数:src.M_COM_INCOME
       ,DST.Q_COM_INCOME                   --本季账户资金流入次数:src.Q_COM_INCOME
       ,DST.M_COM_OUTCOME                  --本月账户资金流出次数:src.M_COM_OUTCOME
       ,DST.Q_COM_OUTCOME                  --本季账户资金流出次数:src.Q_COM_OUTCOME
       ,DST.M_COM_INCOME_AMT               --本月账户资金流入金额:src.M_COM_INCOME_AMT
       ,DST.Q_COM_INCOME_AMT               --本季账户资金流入金额:src.Q_COM_INCOME_AMT
       ,DST.M_COM_OUTCOME_AMT              --本月账户资金流出金额:src.M_COM_OUTCOME_AMT
       ,DST.Q_COM_OUTCOME_AMT              --本季账户资金流出金额:src.Q_COM_OUTCOME_AMT
       ,DST.IS_ELEC    --是否电费签约:src.IS_ELEC
       ,DST.IS_WATER   --是否水费签约:src.IS_WATER
       ,DST.IS_GAS     --是否燃气签约:src.IS_GAS
       ,DST.IS_TV      --是否广电签约:src.IS_TV
       ,DST.IS_WIRE    --是否电信签约:src.IS_WIRE
       ,DST.IS_JHSB    --是否激活社保卡:src.IS_JHSB
   FROM ACRM_A_PROD_CHANNEL_INFO DST 
   LEFT JOIN ACRM_A_PROD_CHANNEL_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_PROD_CHANNEL_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_PROD_CHANNEL_INFO/"+V_DT+".parquet"
UNION=ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unionAll(ACRM_A_PROD_CHANNEL_INFO_INNTMP1)
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.cache()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.cache()
nrowsi = ACRM_A_PROD_CHANNEL_INFO_INNTMP1.count()
nrowsa = ACRM_A_PROD_CHANNEL_INFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_PROD_CHANNEL_INFO_INNTMP1.unpersist()
ACRM_A_PROD_CHANNEL_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_PROD_CHANNEL_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_PROD_CHANNEL_INFO_BK/")
