#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_ECCSIATT').setMaster(sys.argv[2])
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

O_CI_CBOD_ECCSIATT = sqlContext.read.parquet(hdfs+'/O_CI_CBOD_ECCSIATT/*')
O_CI_CBOD_ECCSIATT.registerTempTable("O_CI_CBOD_ECCSIATT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,EC_BNK_NO               AS EC_BNK_NO 
       ,EC_SEQ_NO               AS EC_SEQ_NO 
       ,EC_CUST_NO              AS EC_CUST_NO 
       ,EC_ATT_TYP              AS EC_ATT_TYP 
       ,EC_ATT_TODAT_N          AS EC_ATT_TODAT_N 
       ,EC_CUST_TYP             AS EC_CUST_TYP 
       ,EC_ATT_FMDAT_N          AS EC_ATT_FMDAT_N 
       ,EC_ATT_DESC             AS EC_ATT_DESC 
       ,EC_CRT_SYS              AS EC_CRT_SYS 
       ,EC_CRT_SCT_N            AS EC_CRT_SCT_N 
       ,EC_CRT_OPR              AS EC_CRT_OPR 
       ,EC_UPD_SYS              AS EC_UPD_SYS 
       ,EC_UPD_OPR              AS EC_UPD_OPR 
       ,EC_CRT_ORG              AS EC_CRT_ORG 
       ,EC_UPD_ORG              AS EC_UPD_ORG 
       ,EC_DB_PART_ID           AS EC_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CI_CBOD_ECCSIATT A                                   --关注客户清单档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_ECCSIATT = sqlContext.sql(sql)
F_CI_CBOD_ECCSIATT.registerTempTable("F_CI_CBOD_ECCSIATT")
dfn="F_CI_CBOD_ECCSIATT/"+V_DT+".parquet"
F_CI_CBOD_ECCSIATT.cache()
nrows = F_CI_CBOD_ECCSIATT.count()
F_CI_CBOD_ECCSIATT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_CBOD_ECCSIATT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_ECCSIATT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CBOD_ECCSIATT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
