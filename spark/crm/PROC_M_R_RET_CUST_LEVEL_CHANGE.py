#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_RET_CUST_LEVEL_CHANGE').setMaster(sys.argv[2])
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
#MCRM_RET_CUST_LEVEL_CHANGE 增量  删除当天数据文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_CUST_LEVEL_CHANGE/"+V_DT+".parquet")

OCRM_F_CUST_ORG_MGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CUST_ORG_MGR/*')
OCRM_F_CUST_ORG_MGR.registerTempTable("OCRM_F_CUST_ORG_MGR")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_ZH_NAME 
       ,''                    AS CUST_MANAGER 
       ,''                    AS CUST_MANAGER_NAME 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_NAME                AS ORG_NAME 
       ,OLD_OBJ_RATING          AS LAST_MONTH_CUST_LEVEL 
       ,OBJ_RATING              AS CURRENT_CUST_LEVEL 
       ,V_DT                    AS ST_DATE 
       ,O_MAIN_TYPE             AS MAIN_TYPE 
   FROM OCRM_F_CUST_ORG_MGR B                                  --客户分配信息表
  WHERE B.O_MAIN_TYPE           = '1' 
    AND B.CUST_TYP              = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_CUST_LEVEL_CHANGE = sqlContext.sql(sql)
MCRM_RET_CUST_LEVEL_CHANGE.registerTempTable("MCRM_RET_CUST_LEVEL_CHANGE")
dfn="MCRM_RET_CUST_LEVEL_CHANGE/"+V_DT+".parquet"
MCRM_RET_CUST_LEVEL_CHANGE.cache()
nrows = MCRM_RET_CUST_LEVEL_CHANGE.count()
MCRM_RET_CUST_LEVEL_CHANGE.write.save(path=hdfs + '/' + dfn, mode='append')
MCRM_RET_CUST_LEVEL_CHANGE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_RET_CUST_LEVEL_CHANGE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
