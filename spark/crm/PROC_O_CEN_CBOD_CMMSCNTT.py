#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMMSCNTT').setMaster(sys.argv[2])
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
O_CM_CBOD_CMMSCNTT = sqlContext.read.parquet(hdfs+'/O_CM_CBOD_CMMSCNTT/*')
O_CM_CBOD_CMMSCNTT.registerTempTable("O_CM_CBOD_CMMSCNTT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_WORD_CLSFN           AS CM_WORD_CLSFN 
       ,CM_FILLER_NTT           AS CM_FILLER_NTT 
       ,CM_INTR_TYP             AS CM_INTR_TYP 
       ,CM_INTR_TYP_DESCRP      AS CM_INTR_TYP_DESCRP 
       ,CM_INTSN_NO             AS CM_INTSN_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CM_CBOD_CMMSCNTT A                                   --利率种类
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_CMMSCNTT = sqlContext.sql(sql)
F_CM_CBOD_CMMSCNTT.registerTempTable("F_CM_CBOD_CMMSCNTT")
dfn="F_CM_CBOD_CMMSCNTT/"+V_DT+".parquet"
F_CM_CBOD_CMMSCNTT.cache()
nrows = F_CM_CBOD_CMMSCNTT.count()
F_CM_CBOD_CMMSCNTT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_CBOD_CMMSCNTT.unpersist()
O_CM_CBOD_CMMSCNTT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_CMMSCNTT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_CMMSCNTT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
