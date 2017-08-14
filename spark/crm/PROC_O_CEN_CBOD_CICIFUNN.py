#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CICIFUNN').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_CICIFUNN/"+V_DT+".parquet")


O_CI_CBOD_CICIFUNN = sqlContext.read.parquet(hdfs+'/O_CI_CBOD_CICIFUNN/*')
O_CI_CBOD_CICIFUNN.registerTempTable("O_CI_CBOD_CICIFUNN")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,FK_CICIF_KEY            AS FK_CICIF_KEY 
       ,CI_CER_TYP_1            AS CI_CER_TYP_1 
       ,CI_CER_NO_1             AS CI_CER_NO_1 
       ,CI_FULL_NAM             AS CI_FULL_NAM 
       ,CI_NEW_CUST_NO          AS CI_NEW_CUST_NO 
       ,CI_CANCEL_FLG           AS CI_CANCEL_FLG 
       ,CI_CUST_TYP             AS CI_CUST_TYP 
       ,CI_CRT_SYS              AS CI_CRT_SYS 
       ,CI_CRT_SCT_N            AS CI_CRT_SCT_N 
       ,CI_CRT_ORG              AS CI_CRT_ORG 
       ,CI_CRT_OPR              AS CI_CRT_OPR 
       ,CI_UPD_SYS              AS CI_UPD_SYS 
       ,CI_UPD_ORG              AS CI_UPD_ORG 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CI_CBOD_CICIFUNN A                                   --对私合并档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_CICIFUNN = sqlContext.sql(sql)
F_CI_CBOD_CICIFUNN.registerTempTable("F_CI_CBOD_CICIFUNN")
dfn="F_CI_CBOD_CICIFUNN/"+V_DT+".parquet"
F_CI_CBOD_CICIFUNN.cache()
nrows = F_CI_CBOD_CICIFUNN.count()
F_CI_CBOD_CICIFUNN.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_CBOD_CICIFUNN.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CBOD_CICIFUNN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
