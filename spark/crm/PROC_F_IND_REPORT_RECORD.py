#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_IND_REPORT_RECORD').setMaster(sys.argv[2])
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

#OCRM_F_IND_REPORT_RECORD  增量文件 删除当天文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_IND_REPORT_RECORD/"+V_DT+".parquet")

F_CI_XDXT_IND_REPORT_RECORD = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_REPORT_RECORD/*')
F_CI_XDXT_IND_REPORT_RECORD.registerTempTable("F_CI_XDXT_IND_REPORT_RECORD")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id() AS ID 
       ,REPORTNO                AS REPORTNO 
       ,OBJECTTYPE              AS OBJECTTYPE 
       ,OBJECTNO                AS OBJECTNO 
       ,MODELNO                 AS MODELNO 
       ,REPORTNAME              AS REPORTNAME 
       ,INPUTTIME               AS INPUTTIME 
       ,ORGID                   AS ORGID 
       ,USERID                  AS USERID 
       ,UPDATETIME              AS UPDATETIME 
       ,VALIDFLAG               AS VALIDFLAG 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
   FROM F_CI_XDXT_IND_REPORT_RECORD A                          --个人财务报表数据记录表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_IND_REPORT_RECORD = sqlContext.sql(sql)
OCRM_F_IND_REPORT_RECORD.registerTempTable("OCRM_F_IND_REPORT_RECORD")
dfn="OCRM_F_IND_REPORT_RECORD/"+V_DT+".parquet"
OCRM_F_IND_REPORT_RECORD.cache()
nrows = OCRM_F_IND_REPORT_RECORD.count()
OCRM_F_IND_REPORT_RECORD.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_IND_REPORT_RECORD.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_IND_REPORT_RECORD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
