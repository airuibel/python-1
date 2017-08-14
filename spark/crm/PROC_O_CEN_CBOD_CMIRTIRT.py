#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMIRTIRT').setMaster(sys.argv[2])
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

O_CM_CBOD_CMIRTIRT = sqlContext.read.parquet(hdfs+'/O_CM_CBOD_CMIRTIRT/*')
O_CM_CBOD_CMIRTIRT.registerTempTable("O_CM_CBOD_CMIRTIRT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_OPUN_COD             AS CM_OPUN_COD 
       ,CM_BUSN_TYP             AS CM_BUSN_TYP 
       ,CM_CURR_COD             AS CM_CURR_COD 
       ,CM_INTR_TYP             AS CM_INTR_TYP 
       ,CM_MASS_AMT_RANG        AS CM_MASS_AMT_RANG 
       ,CM_AVL_DT               AS CM_AVL_DT 
       ,CM_INTR1                AS CM_INTR1 
       ,CM_INTR2                AS CM_INTR2 
       ,CM_DOC_NO_CM            AS CM_DOC_NO_CM 
       ,CM_INTR_DSCRP           AS CM_INTR_DSCRP 
       ,CM_IRT_STS              AS CM_IRT_STS 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CM_CBOD_CMIRTIRT A                                   --利率主档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_CMIRTIRT = sqlContext.sql(sql)
F_CM_CBOD_CMIRTIRT.registerTempTable("F_CM_CBOD_CMIRTIRT")
dfn="F_CM_CBOD_CMIRTIRT/"+V_DT+".parquet"
F_CM_CBOD_CMIRTIRT.cache()
nrows = F_CM_CBOD_CMIRTIRT.count()
F_CM_CBOD_CMIRTIRT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_CBOD_CMIRTIRT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_CMIRTIRT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_CMIRTIRT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
