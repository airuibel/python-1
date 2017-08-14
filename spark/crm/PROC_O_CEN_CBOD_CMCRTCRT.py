#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMCRTCRT').setMaster(sys.argv[2])
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

O_CM_CBOD_CMCRTCRT = sqlContext.read.parquet(hdfs+'/O_CM_CBOD_CMCRTCRT/*')
O_CM_CBOD_CMCRTCRT.registerTempTable("O_CM_CBOD_CMCRTCRT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_FXR_EFFDT            AS CM_FXR_EFFDT 
       ,CM_CONV_CURR_COD_OUT    AS CM_CONV_CURR_COD_OUT 
       ,CM_CONV_CURR_COD_IN     AS CM_CONV_CURR_COD_IN 
       ,CM_FXR_TYP              AS CM_FXR_TYP 
       ,CM_TX_TM                AS CM_TX_TM 
       ,CM_FXR_STS              AS CM_FXR_STS 
       ,CM_LITM_TX_TM           AS CM_LITM_TX_TM 
       ,CM_BUG                  AS CM_BUG 
       ,CM_SLD                  AS CM_SLD 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CM_CBOD_CMCRTCRT A                                   --汇率
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_CMCRTCRT = sqlContext.sql(sql)
F_CM_CBOD_CMCRTCRT.registerTempTable("F_CM_CBOD_CMCRTCRT")
dfn="F_CM_CBOD_CMCRTCRT/"+V_DT+".parquet"
F_CM_CBOD_CMCRTCRT.cache()
nrows = F_CM_CBOD_CMCRTCRT.count()
F_CM_CBOD_CMCRTCRT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_CBOD_CMCRTCRT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_CMCRTCRT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_CMCRTCRT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
