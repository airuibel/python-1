#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_SAACNAMT').setMaster(sys.argv[2])
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

O_DP_CBOD_SAACNAMT = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_SAACNAMT/*')
O_DP_CBOD_SAACNAMT.registerTempTable("O_DP_CBOD_SAACNAMT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,FK_SAACN_KEY            AS FK_SAACN_KEY 
       ,SA_CURR_COD             AS SA_CURR_COD 
       ,SA_CURR_IDEN            AS SA_CURR_IDEN 
       ,SA_FRZ_AMT              AS SA_FRZ_AMT 
       ,SA_ACLG                 AS SA_ACLG 
       ,SA_COM_FLG              AS SA_COM_FLG 
       ,SA_SIGN_PDT             AS SA_SIGN_PDT 
       ,SA_DDP_PDT              AS SA_DDP_PDT 
       ,SA_DDP_ACCT_STS         AS SA_DDP_ACCT_STS 
       ,SA_LTM_TX_DT            AS SA_LTM_TX_DT 
       ,SA_OD_TM                AS SA_OD_TM 
       ,SA_OD_PDT               AS SA_OD_PDT 
       ,SA_OD_AMT               AS SA_OD_AMT 
       ,SA_OD_DAYS_N            AS SA_OD_DAYS_N 
       ,SA_OD_INT               AS SA_OD_INT 
       ,SA_ACCT_BAL             AS SA_ACCT_BAL 
       ,SA_SVC                  AS SA_SVC 
       ,SA_INTR_COD             AS SA_INTR_COD 
       ,SA_INTR                 AS SA_INTR 
       ,SA_FLTR_FVR_SIGN        AS SA_FLTR_FVR_SIGN 
       ,SA_FLTR_FVR             AS SA_FLTR_FVR 
       ,SA_SLEEP_STS            AS SA_SLEEP_STS 
       ,SA_BELONG_INSTN_COD     AS SA_BELONG_INSTN_COD 
       ,SA_FRZ_STS              AS SA_FRZ_STS 
       ,SA_STP_STS              AS SA_STP_STS 
       ,SA_PDP_CODE             AS SA_PDP_CODE 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_DP_CBOD_SAACNAMT A                                   --活存资金档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_SAACNAMT = sqlContext.sql(sql)
F_DP_CBOD_SAACNAMT.registerTempTable("F_DP_CBOD_SAACNAMT")
dfn="F_DP_CBOD_SAACNAMT/"+V_DT+".parquet"
F_DP_CBOD_SAACNAMT.cache()
nrows = F_DP_CBOD_SAACNAMT.count()
F_DP_CBOD_SAACNAMT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_DP_CBOD_SAACNAMT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_SAACNAMT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_SAACNAMT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
