#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_RET_CUST_LEVEL_CHANGE_TG').setMaster(sys.argv[2])
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

#MCRM_RET_CUST_LEVEL_CHANGE_TG 增量  删除当天数据文件

ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_CUST_LEVEL_CHANGE_TG/"+V_DT+".parquet")

MCRM_RET_CUST_LEVEL_CHANGE = sqlContext.read.parquet(hdfs+'/MCRM_RET_CUST_LEVEL_CHANGE/*')
MCRM_RET_CUST_LEVEL_CHANGE.registerTempTable("MCRM_RET_CUST_LEVEL_CHANGE")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT COUNT(1)                       AS C_CUST_ID 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,CURRENT_CUST_LEVEL      AS CURRENT_CUST_LEVEL 
       ,LAST_MONTH_CUST_LEVEL   AS LAST_MONTH_CUST_LEVEL 
       ,''                    AS ST_DATE 
   FROM MCRM_RET_CUST_LEVEL_CHANGE A                           --客户等级变化表
  GROUP BY ORG_ID 
       ,ORG_NAME 
       ,CURRENT_CUST_LEVEL 
       ,LAST_MONTH_CUST_LEVEL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_CUST_LEVEL_CHANGE_TG = sqlContext.sql(sql)
MCRM_RET_CUST_LEVEL_CHANGE_TG.registerTempTable("MCRM_RET_CUST_LEVEL_CHANGE_TG")
dfn="MCRM_RET_CUST_LEVEL_CHANGE_TG/"+V_DT+".parquet"
MCRM_RET_CUST_LEVEL_CHANGE_TG.cache()
nrows = MCRM_RET_CUST_LEVEL_CHANGE_TG.count()
MCRM_RET_CUST_LEVEL_CHANGE_TG.write.save(path=hdfs + '/' + dfn, mode='append')
MCRM_RET_CUST_LEVEL_CHANGE_TG.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_RET_CUST_LEVEL_CHANGE_TG lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
