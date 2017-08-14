#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_BUSINESS_INFO').setMaster(sys.argv[2])
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

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
OCRM_F_CI_CUSTLNAINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUSTLNAINFO/*')
OCRM_F_CI_CUSTLNAINFO.registerTempTable("OCRM_F_CI_CUSTLNAINFO")
ACRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_CARD_INFO/*')
ACRM_F_DP_CARD_INFO.registerTempTable("ACRM_F_DP_CARD_INFO")
#目标表：ACRM_A_BUSINESS_INFO，先做全量操作后做merge操作
#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID                       AS CUST_ID 
       ,CAST(''  AS DECIMAL(24,6))                   AS CREDIT_LINE 
       ,CAST(''  AS INTEGER)                    AS OVER_NUM 
       ,CAST(''  AS DECIMAL(24,6))                    AS OVER_AMT 
       ,C.SXJE   AS SXJE 
       ,C.YXJE          AS YXJE 
       ,CAST(''  AS DECIMAL(24,6))                    AS DBJE 
       ,A.DEP_AMT        AS DEP_AMT 
       ,B.LOWCOST_DEP_AMT     AS LOWCOST_DEP_AMT 
       ,F.CARD_AMT      AS CARD_AMT 
       ,A.DEP_YEAR_AVG        AS DEP_YEAR_AVG 
       ,A.DEP_MONTH_AVG                AS DEP_MONTH_AVG 
       ,B.LOWCOST_DEP_YEAR_AVG            AS LOWCOST_DEP_YEAR_AVG 
       ,B.LOWCOST_DEP_MONTH_AVG             AS LOWCOST_DEP_MONTH_AVG 
       ,F.CARD_YEAR_AVG                 AS CARD_YEAR_AVG 
       ,F.CARD_MONTH_AVG                  AS CARD_MONTH_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOAN_YEAR_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOAN_MONTH_AVG 
       ,D.BL_LOAN_AMT                AS BL_LOAN_AMT 
       ,CAST(''  AS DECIMAL(24,6))                      AS QX_LOAN_AMT 
       ,E.HX_LOAN_AMT                AS HX_LOAN_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOAN_BAL 
       ,CAST(''  AS DECIMAL(24,6))                    AS CREDIT_BAL 
       ,A.FR_ID                 AS FR_ID 
       ,'1'                     AS IF_NEW_CARD 
   FROM (SELECT FR_ID,CUST_ID
                ,CAST(SUM(NVL(MS_AC_BAL, 0)) AS DECIMAL(24,6)) AS DEP_AMT 
                ,CAST(SUM(NVL(YEAR_AVG, 0)) AS DECIMAL(24,6)) AS DEP_YEAR_AVG 
                ,CAST(SUM(NVL(MONTH_AVG, 0)) AS DECIMAL(24,6)) AS DEP_MONTH_AVG 
           FROM ACRM_F_DP_SAVE_INFO 
          WHERE ACCT_STATUS = '01' 
         GROUP BY FR_ID,CUST_ID) A                                  --负债协议表
   LEFT JOIN (SELECT FR_ID,CUST_ID,
                     CAST(SUM(NVL(BAL_RMB, 0))        AS DECIMAL(24,6))                AS LOWCOST_DEP_AMT 
                    ,CAST(SUM(NVL(YEAR_AVG, 0))       AS DECIMAL(24,6))                 AS LOWCOST_DEP_YEAR_AVG 
                    ,CAST(SUM(NVL(MONTH_AVG, 0))      AS DECIMAL(24,6))                  AS LOWCOST_DEP_MONTH_AVG 
                FROM ACRM_F_DP_SAVE_INFO 
               WHERE PRODUCT_ID NOT IN('999SA000101', '999SA000600', '999SA110600', '999SA000100') 
                 AND((PERD IN('ST01', 'ST02', 'ST03', 'ST04', 'ST05', 'ST06', 'ST07', 'ST12') 
                      AND ACCONT_TYPE = 'D')  OR ACCONT_TYPE = 'H') 
               GROUP BY FR_ID,CUST_ID)B                             --负债协议表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN (SELECT FR_ID,CUST_ID
                     ,CAST(SUM(CASE WHEN CONT_STS = '1000' AND PRODUCT_ID LIKE '3%' THEN NVL(CONT_AMT, 0) END) AS DECIMAL(24,6)) AS SXJE 
                     ,CAST(SUM(CASE WHEN CONT_STS = '1000' AND PRODUCT_ID LIKE '3%' THEN NVL(CONT_AMT, 0) END) - SUM(NVL(BAL, 0)) AS DECIMAL(24,6)) AS YXJE 
                FROM ACRM_F_CI_ASSET_BUSI_PROTO 
               GROUP BY FR_ID,CUST_ID) C                      --资产协议表
     ON A.FR_ID                 = C.FR_ID 
    AND A.CUST_ID               = C.CUST_ID 
   LEFT JOIN (SELECT FR_ID,CUST_ID
                     ,CAST(SUM(NVL(BAL_RMB, 0)) AS DECIMAL(24,6))                        AS BL_LOAN_AMT 
                FROM ACRM_F_CI_ASSET_BUSI_PROTO
               WHERE CLASSIFYRESULT NOT IN('QL01', 'QL02')
               GROUP BY FR_ID,CUST_ID) D                      --资产协议表
     ON A.CUST_ID               = D.CUST_ID 
    AND A.FR_ID                 = D.FR_ID 
   LEFT JOIN (SELECT FR_ID,CUST_ID,
                     CAST(SUM(NVL(BAL_RMB, 0)) AS DECIMAL(24,6)) AS HX_LOAN_AMT 
                FROM ACRM_F_CI_ASSET_BUSI_PROTO
               WHERE LN_APCL_FLG = 'Y'
               GROUP BY FR_ID,CUST_ID) E                      --资产协议表
     ON A.CUST_ID               = E.CUST_ID 
    AND A.FR_ID                 = E.FR_ID 
   LEFT JOIN (SELECT FR_ID,CUST_ID
                    ,CAST(SUM(NVL(CARD_BAL, 0)) AS DECIMAL(24,6)) AS CARD_AMT 
                    ,CAST(SUM(NVL(CARD_MONTH_BAL, 0)) AS DECIMAL(24,6)) AS CARD_YEAR_AVG 
                    ,CAST(SUM(NVL(CARD_YEAR_BAL, 0)) AS DECIMAL(24,6)) AS CARD_MONTH_AVG 
                FROM ACRM_F_DP_CARD_INFO
              GROUP BY FR_ID,CUST_ID) F                             --借记卡信息表
     ON A.CUST_ID               = F.CUST_ID 
    AND A.FR_ID                 = F.FR_ID 
   LEFT JOIN (SELECT DISTINCT CUST_ID ,FR_ID 
                FROM ACRM_F_DP_CARD_INFO 
               WHERE IF_NEW_CARD = '1') G                        --
     ON A.CUST_ID               = G.CUST_ID 
    AND A.FR_ID                 = G.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_BUSINESS_INFO = sqlContext.sql(sql)
ACRM_A_BUSINESS_INFO.registerTempTable("ACRM_A_BUSINESS_INFO")
dfn="ACRM_A_BUSINESS_INFO/"+V_DT+".parquet"
ACRM_A_BUSINESS_INFO.cache()
nrows = ACRM_A_BUSINESS_INFO.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_BUSINESS_INFO/*.parquet")
ACRM_A_BUSINESS_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_BUSINESS_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_BUSINESS_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CAST(''  AS DECIMAL(24,6))                    AS CREDIT_LINE 
       ,CAST(''  AS INTEGER)                    AS OVER_NUM 
       ,CAST(''  AS DECIMAL(24,6))                    AS OVER_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS SXJE 
       ,CAST(''  AS DECIMAL(24,6))                    AS YXJE 
       ,CAST(SUM(NVL(TAKE_MAX_GRNT_AMT, 0))    AS DECIMAL(24,6))                   AS DBJE 
       ,CAST(''  AS DECIMAL(24,6))                    AS DEP_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOWCOST_DEP_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS CARD_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS DEP_YEAR_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS DEP_MONTH_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOWCOST_DEP_YEAR_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOWCOST_DEP_MONTH_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS CARD_YEAR_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS CARD_MONTH_AVG 
       ,CAST(SUM(YEAR_AVG)  AS DECIMAL(24,6))                     AS LOAN_YEAR_AVG 
       ,CAST(SUM(MONTH_AVG)  AS DECIMAL(24,6))                     AS LOAN_MONTH_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS BL_LOAN_AMT 
       ,CAST(SUM(NVL(SHOULD_INT, 0)) AS DECIMAL(24,6))                        AS QX_LOAN_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS HX_LOAN_AMT 
       ,CAST(SUM(NVL(BAL_RMB, 0))  AS DECIMAL(24,6))                      AS LOAN_BAL 
       ,CAST(''  AS DECIMAL(24,6))                    AS CREDIT_BAL 
       ,FR_ID                   AS FR_ID 
       ,''                    AS IF_NEW_CARD 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  WHERE LN_APCL_FLG             = 'N' 
  GROUP BY CUST_ID 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_BUSINESS_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_A_BUSINESS_INFO_INNTMP1.registerTempTable("ACRM_A_BUSINESS_INFO_INNTMP1")

ACRM_A_BUSINESS_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_BUSINESS_INFO/*')
ACRM_A_BUSINESS_INFO.registerTempTable("ACRM_A_BUSINESS_INFO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CREDIT_LINE                                        --贷记卡额度:src.CREDIT_LINE
       ,DST.OVER_NUM                                           --贷记卡逾期次数:src.OVER_NUM
       ,DST.OVER_AMT                                           --贷记卡逾期金额:src.OVER_AMT
       ,DST.SXJE                                               --授信金额:src.SXJE
       ,DST.YXJE                                               --用信金额:src.YXJE
       ,DST.DBJE                                               --担保金额:src.DBJE
       ,DST.DEP_AMT                                            --存款总额:src.DEP_AMT
       ,DST.LOWCOST_DEP_AMT                                    --低成本存款金额:src.LOWCOST_DEP_AMT
       ,DST.CARD_AMT                                           --借记卡余额:src.CARD_AMT
       ,DST.DEP_YEAR_AVG                                       --存款本年日平:src.DEP_YEAR_AVG
       ,DST.DEP_MONTH_AVG                                      --存款本月日平:src.DEP_MONTH_AVG
       ,DST.LOWCOST_DEP_YEAR_AVG                               --低成本存款本年日平:src.LOWCOST_DEP_YEAR_AVG
       ,DST.LOWCOST_DEP_MONTH_AVG                              --低成本存款本月日平:src.LOWCOST_DEP_MONTH_AVG
       ,DST.CARD_YEAR_AVG                                      --借记卡本年日平:src.CARD_YEAR_AVG
       ,DST.CARD_MONTH_AVG                                     --借记卡本月日平:src.CARD_MONTH_AVG
       ,DST.LOAN_YEAR_AVG                                      --贷款本年日平:src.LOAN_YEAR_AVG
       ,DST.LOAN_MONTH_AVG                                     --贷款本月日平:src.LOAN_MONTH_AVG
       ,DST.BL_LOAN_AMT                                        --不良贷款余额:src.BL_LOAN_AMT
       ,DST.QX_LOAN_AMT                                        --欠息金额:src.QX_LOAN_AMT
       ,DST.HX_LOAN_AMT                                        --已核销贷款余额:src.HX_LOAN_AMT
       ,DST.LOAN_BAL                                           --贷款余额:src.LOAN_BAL
       ,DST.CREDIT_BAL                                         --信用卡可用额度:src.CREDIT_BAL
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.IF_NEW_CARD                                        --是否本月开卡客户:src.IF_NEW_CARD
   FROM ACRM_A_BUSINESS_INFO DST 
   LEFT JOIN ACRM_A_BUSINESS_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_BUSINESS_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_BUSINESS_INFO/"+V_DT+".parquet"
ACRM_A_BUSINESS_INFO_INNTMP2=ACRM_A_BUSINESS_INFO_INNTMP2.unionAll(ACRM_A_BUSINESS_INFO_INNTMP1)
ACRM_A_BUSINESS_INFO_INNTMP1.cache()
ACRM_A_BUSINESS_INFO_INNTMP2.cache()
nrowsi = ACRM_A_BUSINESS_INFO_INNTMP1.count()
nrowsa = ACRM_A_BUSINESS_INFO_INNTMP2.count()
ACRM_A_BUSINESS_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_BUSINESS_INFO_INNTMP1.unpersist()
ACRM_A_BUSINESS_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_BUSINESS_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_BUSINESS_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_BUSINESS_INFO_BK/")

#任务[12] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CAST(SUM(NVL(CREDIT_LINE, 0))  AS DECIMAL(24,6))                     AS CREDIT_LINE 
       ,CAST(SUM(CASE WHEN CAST((NVL(OD_LINE, 0)) AS INTEGER) > 0 THEN 1 ELSE 0 END) AS INTEGER)                      AS OVER_NUM 
       ,CAST(SUM(NVL(OD_LINE, 0)) AS DECIMAL(24,6))                      AS OVER_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS SXJE 
       ,CAST(''  AS DECIMAL(24,6))                    AS YXJE 
       ,CAST(''  AS DECIMAL(24,6))                    AS DBJE 
       ,CAST(''  AS DECIMAL(24,6))                    AS DEP_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOWCOST_DEP_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS CARD_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS DEP_YEAR_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS DEP_MONTH_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOWCOST_DEP_YEAR_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOWCOST_DEP_MONTH_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS CARD_YEAR_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS CARD_MONTH_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOAN_YEAR_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOAN_MONTH_AVG 
       ,CAST(''  AS DECIMAL(24,6))                    AS BL_LOAN_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS QX_LOAN_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS HX_LOAN_AMT 
       ,CAST(''  AS DECIMAL(24,6))                    AS LOAN_BAL 
       ,CAST(SUM(NVL(USEFUL_LINE, 0)) AS DECIMAL(24,6))                      AS CREDIT_BAL 
       ,FR_ID                   AS FR_ID 
       ,''                    AS IF_NEW_CARD 
   FROM OCRM_F_CI_CUSTLNAINFO A                                --客户信用信息
  GROUP BY CUST_ID 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_BUSINESS_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_A_BUSINESS_INFO_INNTMP1.registerTempTable("ACRM_A_BUSINESS_INFO_INNTMP1")

ACRM_A_BUSINESS_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_BUSINESS_INFO/*')
ACRM_A_BUSINESS_INFO.registerTempTable("ACRM_A_BUSINESS_INFO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CREDIT_LINE                                        --贷记卡额度:src.CREDIT_LINE
       ,DST.OVER_NUM                                           --贷记卡逾期次数:src.OVER_NUM
       ,DST.OVER_AMT                                           --贷记卡逾期金额:src.OVER_AMT
       ,DST.SXJE                                               --授信金额:src.SXJE
       ,DST.YXJE                                               --用信金额:src.YXJE
       ,DST.DBJE                                               --担保金额:src.DBJE
       ,DST.DEP_AMT                                            --存款总额:src.DEP_AMT
       ,DST.LOWCOST_DEP_AMT                                    --低成本存款金额:src.LOWCOST_DEP_AMT
       ,DST.CARD_AMT                                           --借记卡余额:src.CARD_AMT
       ,DST.DEP_YEAR_AVG                                       --存款本年日平:src.DEP_YEAR_AVG
       ,DST.DEP_MONTH_AVG                                      --存款本月日平:src.DEP_MONTH_AVG
       ,DST.LOWCOST_DEP_YEAR_AVG                               --低成本存款本年日平:src.LOWCOST_DEP_YEAR_AVG
       ,DST.LOWCOST_DEP_MONTH_AVG                              --低成本存款本月日平:src.LOWCOST_DEP_MONTH_AVG
       ,DST.CARD_YEAR_AVG                                      --借记卡本年日平:src.CARD_YEAR_AVG
       ,DST.CARD_MONTH_AVG                                     --借记卡本月日平:src.CARD_MONTH_AVG
       ,DST.LOAN_YEAR_AVG                                      --贷款本年日平:src.LOAN_YEAR_AVG
       ,DST.LOAN_MONTH_AVG                                     --贷款本月日平:src.LOAN_MONTH_AVG
       ,DST.BL_LOAN_AMT                                        --不良贷款余额:src.BL_LOAN_AMT
       ,DST.QX_LOAN_AMT                                        --欠息金额:src.QX_LOAN_AMT
       ,DST.HX_LOAN_AMT                                        --已核销贷款余额:src.HX_LOAN_AMT
       ,DST.LOAN_BAL                                           --贷款余额:src.LOAN_BAL
       ,DST.CREDIT_BAL                                         --信用卡可用额度:src.CREDIT_BAL
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.IF_NEW_CARD                                        --是否本月开卡客户:src.IF_NEW_CARD
   FROM ACRM_A_BUSINESS_INFO DST 
   LEFT JOIN ACRM_A_BUSINESS_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_BUSINESS_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_BUSINESS_INFO/"+V_DT+".parquet"
ACRM_A_BUSINESS_INFO_INNTMP2=ACRM_A_BUSINESS_INFO_INNTMP2.unionAll(ACRM_A_BUSINESS_INFO_INNTMP1)
ACRM_A_BUSINESS_INFO_INNTMP1.cache()
ACRM_A_BUSINESS_INFO_INNTMP2.cache()
nrowsi = ACRM_A_BUSINESS_INFO_INNTMP1.count()
nrowsa = ACRM_A_BUSINESS_INFO_INNTMP2.count()
ACRM_A_BUSINESS_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_A_BUSINESS_INFO_INNTMP1.unpersist()
ACRM_A_BUSINESS_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_BUSINESS_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_A_BUSINESS_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_BUSINESS_INFO_BK/")
