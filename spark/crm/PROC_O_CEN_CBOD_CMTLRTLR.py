#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMTLRTLR').setMaster(sys.argv[2])
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
#----------来源表---------------
O_CM_CBOD_CMTLRTLR = sqlContext.read.parquet(hdfs+'/O_CM_CBOD_CMTLRTLR/*')
O_CM_CBOD_CMTLRTLR.registerTempTable("O_CM_CBOD_CMTLRTLR")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_OPR_NO               AS CM_OPR_NO 
       ,CM_OPR_BELONG_BRH       AS CM_OPR_BELONG_BRH 
       ,CM_SUB_TELLER_ID        AS CM_SUB_TELLER_ID 
       ,CM_OPR_NAME             AS CM_OPR_NAME 
       ,CM_ID_NO                AS CM_ID_NO 
       ,CM_TLR_RMRK             AS CM_TLR_RMRK 
       ,CM_PSWD_FINAL_UPDT_DT   AS CM_PSWD_FINAL_UPDT_DT 
       ,CM_LTST_OPN_DT          AS CM_LTST_OPN_DT 
       ,CM_DATA_LST_UPDT_DT     AS CM_DATA_LST_UPDT_DT 
       ,CM_TLR_STS              AS CM_TLR_STS 
       ,CM_OPR_CRD_NO           AS CM_OPR_CRD_NO 
       ,CM_TLR_CHANNEL_FLAG     AS CM_TLR_CHANNEL_FLAG 
       ,CM_EMP_NO               AS CM_EMP_NO 
       ,CM_TLR_TRM_NO           AS CM_TLR_TRM_NO 
       ,CM_TLR_CSH_TYP          AS CM_TLR_CSH_TYP 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CM_CBOD_CMTLRTLR A                                   --柜员信息档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_CMTLRTLR = sqlContext.sql(sql)
F_CM_CBOD_CMTLRTLR.registerTempTable("F_CM_CBOD_CMTLRTLR")
dfn="F_CM_CBOD_CMTLRTLR/"+V_DT+".parquet"
F_CM_CBOD_CMTLRTLR.cache()
nrows = F_CM_CBOD_CMTLRTLR.count()
F_CM_CBOD_CMTLRTLR.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_CBOD_CMTLRTLR.unpersist()
O_CM_CBOD_CMTLRTLR.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_CMTLRTLR/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_CMTLRTLR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
