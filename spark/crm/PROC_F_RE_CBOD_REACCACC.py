#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_RE_CBOD_REACCACC').setMaster(sys.argv[2])
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
#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
F_RE_CBOD_REACCACC = sqlContext.read.parquet(hdfs+'/F_RE_CBOD_REACCACC/*')
F_RE_CBOD_REACCACC.registerTempTable("F_RE_CBOD_REACCACC")

#目标表：
#OCRM_F_RE_CBOD_REACCACC 全量


#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT AS ETLDT
           ,'' AS RE_1LVL_BRH_ID
           ,RE_DOC_TYP AS RE_DOC_TYP
           ,RE_DD_NO AS RE_DD_NO
           ,CAST('' AS DECIMAL(15)) AS REACC_DB_TIMESTAMP
           ,RE_ACCP_APPLN_ACCT_NO AS RE_ACCP_APPLN_ACCT_NO
           ,CAST(RE_DP_AMT AS DECIMAL(15,2)) AS RE_DP_AMT
           ,'' AS RE_CTRT_NO
           ,CAST(RE_RMT_AMT AS DECIMAL(15,2)) AS RE_RMT_AMT
           ,RE_DD_STS AS RE_DD_STS
           ,'' AS RE_BILL_ISS_DT
           ,RE_DD_DUEDT AS RE_DD_DUEDT
           ,RE_CUST_NO AS RE_CUST_NO
           ,'' AS RE_TMPSA_ACCT_NO
           ,'' AS RE_PAYEE_NAME
           ,'' AS RE_PAYEE_ACCT_NO
           ,'' AS RE_PAYEE_AWBK_NO
           ,RE_BA_STS AS RE_BA_STS
           ,RE_OPUN_COD AS RE_OPUN_COD
           ,RE_TX_SEQ_NO AS RE_TX_SEQ_NO
           ,RE_DL_STS AS RE_DL_STS
           ,RE_DL_DT AS RE_DL_DT
           ,CAST(RE_DL_SVC AS DECIMAL(15,2)) AS RE_DL_SVC
           ,RE_LST_TX_DT AS RE_LST_TX_DT
           ,CAST(RE_ACCP_LN_AMT AS DECIMAL(15,2)) AS RE_ACCP_LN_AMT
           ,RE_CRLMT_NO AS RE_CRLMT_NO
           ,RE_DL_DUE_DT AS RE_DL_DUE_DT
           ,RE_ACCP_APPLN_NAME AS RE_ACCP_APPLN_NAME
           ,RE_PAYEE_AWBK_NAME AS RE_PAYEE_AWBK_NAME
           ,RE_DP_ACCT_NO AS RE_DP_ACCT_NO
           ,RE_PAYEE_AWBK_NO_FL AS RE_PAYEE_AWBK_NO_FL
           ,RE_PAYEE_AWBK_NAME_FL AS RE_PAYEE_AWBK_NAME_FL
           ,RE_HOLDER_NAME AS RE_HOLDER_NAME
           ,RE_HOLDER_ACCT_NO AS RE_HOLDER_ACCT_NO
           ,'' AS RE_PSBK_NO
           ,'' AS RE_PSBK_SQ_NO
           ,CAST('' AS DECIMAL(5)) AS RE_EVT_SRL_NO_N
           ,CAST(RE_SVC AS DECIMAL(15,2)) AS RE_SVC
           ,'' AS RE_ENCL_NO
           ,'' AS RE_DRA_RECON_NO
           ,'' AS RE_RMRK
           ,RE_APPL_BRH_STD AS RE_APPL_BRH_STD
           ,RE_SIG_DT AS RE_SIG_DT
           ,CAST(RE_GUAR_RATE AS DECIMAL(8,5)) AS RE_GUAR_RATE
           ,'' AS RE_LN_BRH
           ,'' AS RE_TO_RECON_NO1
           ,RE_BILL_BANK_NO AS RE_BILL_BANK_NO
           ,'' AS RE_BILL_BANK_NAME
           ,RE_BILL_TELLER_NO AS RE_BILL_TELLER_NO
           ,RE_PAY_TELLER_NO AS RE_PAY_TELLER_NO
           ,'' AS RE_TX_LOG_NO
           ,'' AS RE_DB_PART_ID
           ,ODS_ST_DATE AS CRM_DT
   FROM F_RE_CBOD_REACCACC A                                   --银行承兑汇票主档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_RE_CBOD_REACCACC = sqlContext.sql(sql)
OCRM_F_RE_CBOD_REACCACC.registerTempTable("OCRM_F_RE_CBOD_REACCACC")
dfn="OCRM_F_RE_CBOD_REACCACC/"+V_DT+".parquet"
OCRM_F_RE_CBOD_REACCACC.cache()
nrows = OCRM_F_RE_CBOD_REACCACC.count()
OCRM_F_RE_CBOD_REACCACC.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_RE_CBOD_REACCACC.unpersist()

#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_RE_CBOD_REACCACC/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_RE_CBOD_REACCACC lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
