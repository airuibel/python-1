#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMCURCUR').setMaster(sys.argv[2])
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

O_CM_CBOD_CMCURCUR = sqlContext.read.parquet(hdfs+'/O_CM_CBOD_CMCURCUR/*')
O_CM_CBOD_CMCURCUR.registerTempTable("O_CM_CBOD_CMCURCUR")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_CURR_COD             AS CM_CURR_COD 
       ,CM_CURR                 AS CM_CURR 
       ,CM_CURR_COD_CHN         AS CM_CURR_COD_CHN 
       ,CM_CURR_COD_ENG         AS CM_CURR_COD_ENG 
       ,CM_DCM_AMT              AS CM_DCM_AMT 
       ,CM_DCM_FXR              AS CM_DCM_FXR 
       ,CM_CAC_COD_FXR_USD      AS CM_CAC_COD_FXR_USD 
       ,CM_CAC_COD_FXR_RMB      AS CM_CAC_COD_FXR_RMB 
       ,CM_FX_TRX_FLG           AS CM_FX_TRX_FLG 
       ,CM_CURR_COD_PROT_COD    AS CM_CURR_COD_PROT_COD 
       ,CM_UNIT_AMT_NAME        AS CM_UNIT_AMT_NAME 
       ,CM_CM_UNIT_INT_CAL      AS CM_CM_UNIT_INT_CAL 
       ,CM_FX_RESERVE_FUND_FLAG AS CM_FX_RESERVE_FUND_FLAG 
       ,CM_FX_EUR_CUR           AS CM_FX_EUR_CUR 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CM_CBOD_CMCURCUR A                                   --币种信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_CMCURCUR = sqlContext.sql(sql)
F_CM_CBOD_CMCURCUR.registerTempTable("F_CM_CBOD_CMCURCUR")
dfn="F_CM_CBOD_CMCURCUR/"+V_DT+".parquet"
F_CM_CBOD_CMCURCUR.cache()
nrows = F_CM_CBOD_CMCURCUR.count()
F_CM_CBOD_CMCURCUR.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_CBOD_CMCURCUR.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_CMCURCUR/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_CMCURCUR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
