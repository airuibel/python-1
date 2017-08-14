#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_CI_CUST_MERGERLST').setMaster(sys.argv[2])
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

OCRM_F_CI_HHB_MAPPING = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_HHB_MAPPING/*')
OCRM_F_CI_HHB_MAPPING.registerTempTable("OCRM_F_CI_HHB_MAPPING")
#目标表：F_CI_CUST_MERGERLST，增量
#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT TARGET_CUST_ID          AS CUSTOM_ID 
       ,TRIM(REGEXP_REPLACE(SOURCE_CUST_ID, ',', ''))                       AS CUSTOM_ID_BEFORE 
       ,CAST(GROUP_ID    AS INTEGER)            AS GROUP_ID 
       ,V_DT                      AS ODS_ST_DATE 
       ,'1'                     AS UNION_FLAG 
       ,FR_ID                   AS FR_ID 
   FROM OCRM_F_CI_HHB_MAPPING A                                --
  WHERE APPLY_STS               = '1' 
    AND(UNION_FLAG = '0' OR UNION_FLAG IS NULL) 
  GROUP BY TARGET_CUST_ID 
       ,TRIM(REGEXP_REPLACE(SOURCE_CUST_ID, ',', '')) 
       ,GROUP_ID 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CUST_MERGERLST = sqlContext.sql(sql)
F_CI_CUST_MERGERLST.registerTempTable("F_CI_CUST_MERGERLST")
dfn="F_CI_CUST_MERGERLST/"+V_DT+".parquet"
F_CI_CUST_MERGERLST.cache()
nrows = F_CI_CUST_MERGERLST.count()
F_CI_CUST_MERGERLST.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_CUST_MERGERLST.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CUST_MERGERLST/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CUST_MERGERLST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
