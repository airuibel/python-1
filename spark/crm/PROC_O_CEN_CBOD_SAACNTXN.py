#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_SAACNTXN').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_SAACNTXN/"+V_DT+".parquet")

O_DP_CBOD_SAACNTXN = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_SAACNTXN/*')
O_DP_CBOD_SAACNTXN.registerTempTable("O_DP_CBOD_SAACNTXN")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,FK_SAACN_KEY            AS FK_SAACN_KEY 
       ,SA_DDP_ACCT_NO_DET_N    AS SA_DDP_ACCT_NO_DET_N 
       ,SA_CURR_COD             AS SA_CURR_COD 
       ,SA_CURR_IDEN            AS SA_CURR_IDEN 
       ,SA_EC_FLG               AS SA_EC_FLG 
       ,SA_CR_AMT               AS SA_CR_AMT 
       ,SA_DDP_ACCT_BAL         AS SA_DDP_ACCT_BAL 
       ,SA_TX_AMT               AS SA_TX_AMT 
       ,SA_TX_CRD_NO            AS SA_TX_CRD_NO 
       ,SA_TX_TYP               AS SA_TX_TYP 
       ,SA_TX_LOG_NO            AS SA_TX_LOG_NO 
       ,SA_DR_AMT               AS SA_DR_AMT 
       ,SA_OPUN_COD             AS SA_OPUN_COD 
       ,SA_RMRK                 AS SA_RMRK 
       ,SA_TX_TM                AS SA_TX_TM 
       ,SA_TX_DT                AS SA_TX_DT 
       ,SA_OP_CUST_NAME         AS SA_OP_CUST_NAME 
       ,SA_BELONG_INSTN_COD     AS SA_BELONG_INSTN_COD 
       ,SA_CHANNEL_FLAG         AS SA_CHANNEL_FLAG 
       ,SA_PDP_CODE             AS SA_PDP_CODE 
       ,SA_OP_ACCT_NO_32        AS SA_OP_ACCT_NO_32 
       ,SA_OP_BANK_NO           AS SA_OP_BANK_NO 
       ,SA_SUP_TLR              AS SA_SUP_TLR 
       ,SA_APP_TX_CODE          AS SA_APP_TX_CODE 
       ,SA_DB_PART_ID           AS SA_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_DP_CBOD_SAACNTXN A                                   --活存明细档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_SAACNTXN = sqlContext.sql(sql)
F_DP_CBOD_SAACNTXN.registerTempTable("F_DP_CBOD_SAACNTXN")
dfn="F_DP_CBOD_SAACNTXN/"+V_DT+".parquet"
F_DP_CBOD_SAACNTXN.cache()
nrows = F_DP_CBOD_SAACNTXN.count()
F_DP_CBOD_SAACNTXN.write.save(path=hdfs + '/' + dfn, mode='append')
F_DP_CBOD_SAACNTXN.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_SAACNTXN/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_SAACNTXN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
