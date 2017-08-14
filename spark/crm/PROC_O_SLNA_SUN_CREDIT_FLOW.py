#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_SLNA_SUN_CREDIT_FLOW').setMaster(sys.argv[2])
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

O_LN_SUN_CREDIT_FLOW = sqlContext.read.parquet(hdfs+'/O_LN_SUN_CREDIT_FLOW/*')
O_LN_SUN_CREDIT_FLOW.registerTempTable("O_LN_SUN_CREDIT_FLOW")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.SERIALNO              AS SERIALNO 
       ,A.OBJECTTYPE            AS OBJECTTYPE 
       ,A.FLOWNO                AS FLOWNO 
       ,A.ISINUSE               AS ISINUSE 
       ,A.UPDATEDATE            AS UPDATEDATE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'SLNA'                  AS ODS_SYS_ID 
   FROM O_LN_SUN_CREDIT_FLOW A                                 --审批流程模型表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_SUN_CREDIT_FLOW = sqlContext.sql(sql)
F_LN_SUN_CREDIT_FLOW.registerTempTable("F_LN_SUN_CREDIT_FLOW")
dfn="F_LN_SUN_CREDIT_FLOW/"+V_DT+".parquet"
F_LN_SUN_CREDIT_FLOW.cache()
nrows = F_LN_SUN_CREDIT_FLOW.count()
F_LN_SUN_CREDIT_FLOW.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_LN_SUN_CREDIT_FLOW.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_SUN_CREDIT_FLOW/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_SUN_CREDIT_FLOW lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
