#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CRCUPDTL').setMaster(sys.argv[2])
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

ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_CRCUPDTL/"+V_DT+".parquet")
O_DP_CBOD_CRCUPDTL = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_CRCUPDTL/*')
O_DP_CBOD_CRCUPDTL.registerTempTable("O_DP_CBOD_CRCUPDTL")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CR_REC_TYP              AS CR_REC_TYP 
       ,CR_CARD_NO              AS CR_CARD_NO 
       ,CR_TRAN_AMT             AS CR_TRAN_AMT 
       ,CR_MCT_MCC              AS CR_MCT_MCC 
       ,CR_TRAN_FEE             AS CR_TRAN_FEE 
       ,CR_MID                  AS CR_MID 
       ,CR_ACT_SVC_FEE          AS CR_ACT_SVC_FEE 
       ,CR_PAY_SVC_FEE          AS CR_PAY_SVC_FEE 
       ,CR_TRANS_FEE            AS CR_TRANS_FEE 
       ,CR_OUT_CARD_NO          AS CR_OUT_CARD_NO 
       ,CR_IN_CARD_NO           AS CR_IN_CARD_NO 
       ,CR_VALID_FLG            AS CR_VALID_FLG 
       ,CR_ASSURE_DT            AS CR_ASSURE_DT 
       ,CR_BRH                  AS CR_BRH 
       ,CR_CARD_KIND            AS CR_CARD_KIND 
       ,CR_CRD_TYP              AS CR_CRD_TYP 
       ,CR_PROC_FLG             AS CR_PROC_FLG 
       ,CR_OFC_INCOME           AS CR_OFC_INCOME 
       ,CR_CUP_INCOME           AS CR_CUP_INCOME 
       ,CR_OVS_TX_FLG           AS CR_OVS_TX_FLG 
       ,CR_CHANNEL_TYPE         AS CR_CHANNEL_TYPE 
       ,CR_CYCLE_PAY_FEE        AS CR_CYCLE_PAY_FEE 
       ,CR_BELONGTO_BRH         AS CR_BELONGTO_BRH 
       ,CR_AGT_INSTN            AS CR_AGT_INSTN 
       ,CR_LOG_FLG              AS CR_LOG_FLG 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_DP_CBOD_CRCUPDTL A                                   --银联交易流水档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_CRCUPDTL = sqlContext.sql(sql)
F_DP_CBOD_CRCUPDTL.registerTempTable("F_DP_CBOD_CRCUPDTL")
dfn="F_DP_CBOD_CRCUPDTL/"+V_DT+".parquet"
F_DP_CBOD_CRCUPDTL.cache()
nrows = F_DP_CBOD_CRCUPDTL.count()
F_DP_CBOD_CRCUPDTL.write.save(path=hdfs + '/' + dfn, mode='append')
F_DP_CBOD_CRCUPDTL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_CRCUPDTL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
