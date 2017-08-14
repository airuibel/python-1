#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_WP_REMIND_LOANINT').setMaster(sys.argv[2])
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

F_LN_CBOD_LNLNSUPY = sqlContext.read.parquet(hdfs+'/F_LN_CBOD_LNLNSUPY/*')
F_LN_CBOD_LNLNSUPY.registerTempTable("F_LN_CBOD_LNLNSUPY")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
OCRM_F_WP_REMIND_RULE = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_RULE/*')
OCRM_F_WP_REMIND_RULE.registerTempTable("OCRM_F_WP_REMIND_RULE")
OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
OCRM_F_WP_REMIND_LOANINT = sqlContext.read.parquet(hdfs+'/OCRM_F_WP_REMIND_LOANINT/*')
OCRM_F_WP_REMIND_LOANINT.registerTempTable("OCRM_F_WP_REMIND_LOANINT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT FK_LNLNS_KEY            AS ACCT_NO 
       ,SUM(LN_INTRBL - LN_ARFN_INT)                       AS SHOULD_INT 
       ,CONCAT(SUBSTR(LN_ENTR_DT_N, 1, 4),'-',SUBSTR(LN_ENTR_DT_N, 5, 2),'-',SUBSTR(LN_ENTR_DT_N, 7, 2)) AS LOAN_DATE 
       ,FR_ID
   FROM F_LN_CBOD_LNLNSUPY A                                   --放款利息明细档
  WHERE LN_INTRBL > LN_ARFN_INT 
    AND LN_ENTR_DT_N            = V_DT 
    AND LN_INT_TYP              = '5' 
  GROUP BY FK_LNLNS_KEY 
       ,LN_ENTR_DT_N
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT+"'", sql)
MID_CBOD_LNLNSUPY_01 = sqlContext.sql(sql)
MID_CBOD_LNLNSUPY_01.registerTempTable("MID_CBOD_LNLNSUPY_01")
dfn="MID_CBOD_LNLNSUPY_01/"+V_DT+".parquet"
MID_CBOD_LNLNSUPY_01.cache()
nrows = MID_CBOD_LNLNSUPY_01.count()
MID_CBOD_LNLNSUPY_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_CBOD_LNLNSUPY_01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_CBOD_LNLNSUPY_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_CBOD_LNLNSUPY_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()     AS ID 
       ,E.RULE_ID               AS RULE_ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CORE_CUST_NAME        AS CUST_NAME 
       ,A.ACCT_NO               AS ACCT_NO 
       ,A.PRODUCT_ID            AS PROD_ID 
       ,C.PROD_NAME             AS PROD_NAME 
       ,A.BAL                   AS ACCT_BAL 
       ,A.BEGIN_DATE            AS BEGIN_DATE 
       ,A.END_DATE              AS END_DATE 
       ,B.LOAN_DATE             AS LOAN_DATE 
       ,B.SHOULD_INT            AS LOAN_AMT 
       ,'A000106'               AS REMIND_TYPE 
       ,'1'                     AS REMIND_FLAG 
       ,V_DT                    AS MSG_CRT_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_WP_REMIND_RULE E                                --提醒规则表
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO A                      --资产协议表
     ON A.FR_ID                 = E.ORG_ID
    AND E.CUST_TYPE             = A.CUST_TYP 
  INNER JOIN MID_CBOD_LNLNSUPY_01 B                            --放款利息明细档临时表01
     ON A.ACCT_NO               = B.ACCT_NO 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN OCRM_F_PD_PROD_INFO C                             --产品表
     ON A.PRODUCT_ID            = C.PRODUCT_ID 
    AND C.FR_ID                 = A.FR_ID 
   LEFT JOIN OCRM_F_WP_REMIND_LOANINT M                        --贷款欠息提醒信息表
     ON M.FR_ID                 = A.FR_ID 
    AND A.ACCT_NO               = M.ACCT_NO 
    AND M.LOAN_DATE             = B.LOAN_DATE 
  WHERE(A.IS_INT_IN             = '1' 
             OR A.IS_INT_OUT            = '1') 
    AND A.LN_APCL_FLG           = 'N' 
    AND E.REMIND_TYPE           = 'A000106' 
    AND M.ACCT_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_WP_REMIND_LOANINT = sqlContext.sql(sql)
OCRM_F_WP_REMIND_LOANINT.registerTempTable("OCRM_F_WP_REMIND_LOANINT")
dfn="OCRM_F_WP_REMIND_LOANINT/"+V_DT+".parquet"
OCRM_F_WP_REMIND_LOANINT.cache()
nrows = OCRM_F_WP_REMIND_LOANINT.count()
OCRM_F_WP_REMIND_LOANINT.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_WP_REMIND_LOANINT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_WP_REMIND_LOANINT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
