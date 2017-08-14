#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMCASCAS').setMaster(sys.argv[2])
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

#清除数据，支持重跑
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_CBOD_CMCASCAS/"+V_DT+".parquet")


O_TX_CBOD_CMCASCAS = sqlContext.read.parquet(hdfs+'/O_TX_CBOD_CMCASCAS/*')
O_TX_CBOD_CMCASCAS.registerTempTable("O_TX_CBOD_CMCASCAS")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_TX_LOG_NO            AS CM_TX_LOG_NO 
       ,CM_CAS_CNT              AS CM_CAS_CNT 
       ,CM_OPUN_CODE            AS CM_OPUN_CODE 
       ,CM_TLR_NO               AS CM_TLR_NO 
       ,CM_CURR_COD             AS CM_CURR_COD 
       ,CM_CSH_TYP              AS CM_CSH_TYP 
       ,CM_AMT                  AS CM_AMT 
       ,CM_TX_DATE              AS CM_TX_DATE 
       ,CM_STAT                 AS CM_STAT 
       ,CM_CSH_ITM_NO           AS CM_CSH_ITM_NO 
       ,CM_DB_PART_ID           AS CM_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_TX_CBOD_CMCASCAS A                                   --现金收付明细档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_CBOD_CMCASCAS = sqlContext.sql(sql)
F_TX_CBOD_CMCASCAS.registerTempTable("F_TX_CBOD_CMCASCAS")
dfn="F_TX_CBOD_CMCASCAS/"+V_DT+".parquet"
F_TX_CBOD_CMCASCAS.cache()
nrows = F_TX_CBOD_CMCASCAS.count()
F_TX_CBOD_CMCASCAS.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_TX_CBOD_CMCASCAS.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_CBOD_CMCASCAS lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
