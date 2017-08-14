#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_SAACNACN').setMaster(sys.argv[2])
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

O_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_SAACNACN/*')
O_DP_CBOD_SAACNACN.registerTempTable("O_DP_CBOD_SAACNACN")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,SA_ACCT_NO              AS SA_ACCT_NO 
       ,SA_PSBK_PRT_NO          AS SA_PSBK_PRT_NO 
       ,SA_PSBK_STS             AS SA_PSBK_STS 
       ,SA_MBSTMT_FLG           AS SA_MBSTMT_FLG 
       ,SA_OPAC_INSTN_NO        AS SA_OPAC_INSTN_NO 
       ,SA_OPAC_AMT             AS SA_OPAC_AMT 
       ,SA_OPAC_DT              AS SA_OPAC_DT 
       ,SA_OPAC_PERM_NO         AS SA_OPAC_PERM_NO 
       ,SA_CUST_NO              AS SA_CUST_NO 
       ,SA_CUST_NAME            AS SA_CUST_NAME 
       ,SA_CONNTR_NO            AS SA_CONNTR_NO 
       ,SA_CACCT_INSTN_NO       AS SA_CACCT_INSTN_NO 
       ,SA_CACCT_DT             AS SA_CACCT_DT 
       ,SA_ACCT_CHAR            AS SA_ACCT_CHAR 
       ,SA_DRW_TYP              AS SA_DRW_TYP 
       ,SA_SEAL_STS             AS SA_SEAL_STS 
       ,SA_OPAC_TLR_NO          AS SA_OPAC_TLR_NO 
       ,SA_CACCT_TLR_NO         AS SA_CACCT_TLR_NO 
       ,SA_INTC_FLG             AS SA_INTC_FLG 
       ,SA_DEP_TYP              AS SA_DEP_TYP 
       ,SA_CURR_TYP             AS SA_CURR_TYP 
       ,SA_VIR_ACCT_FLG         AS SA_VIR_ACCT_FLG 
       ,SA_GRP_SIGN_FLG         AS SA_GRP_SIGN_FLG 
       ,SA_VIP_ACCT_FLG         AS SA_VIP_ACCT_FLG 
       ,SA_RPT_ACCT_FLG         AS SA_RPT_ACCT_FLG 
       ,SA_CARD_NO              AS SA_CARD_NO 
       ,SA_ORG_DEP_TYPE         AS SA_ORG_DEP_TYPE 
       ,SA_PDP_CODE             AS SA_PDP_CODE 
       ,SA_DOC_TYP              AS SA_DOC_TYP 
       ,SA_REF_CNT_N            AS SA_REF_CNT_N 
       ,SA_BELONG_INSTN_COD     AS SA_BELONG_INSTN_COD 
       ,SA_RISK_LVL             AS SA_RISK_LVL 
       ,SA_INSP_FLG             AS SA_INSP_FLG 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_DP_CBOD_SAACNACN A                                   --活期存款主档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_SAACNACN = sqlContext.sql(sql)
F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")
dfn="F_DP_CBOD_SAACNACN/"+V_DT+".parquet"
F_DP_CBOD_SAACNACN.cache()
nrows = F_DP_CBOD_SAACNACN.count()
F_DP_CBOD_SAACNACN.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_DP_CBOD_SAACNACN.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_SAACNACN/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_SAACNACN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
