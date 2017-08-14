#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_RET_CUST_FLOW').setMaster(sys.argv[2])
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
#MCRM_RET_CUST_FLOW 增量 删除当天文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_CUST_FLOW/"+V_DT+".parquet")

MCRM_RET_CUST_ASSETS = sqlContext.read.parquet(hdfs+'/MCRM_RET_CUST_ASSETS/*')
MCRM_RET_CUST_ASSETS.registerTempTable("MCRM_RET_CUST_ASSETS")
ACRM_F_AG_AGREEMENT = sqlContext.read.parquet(hdfs+'/ACRM_F_AG_AGREEMENT/*')
ACRM_F_AG_AGREEMENT.registerTempTable("ACRM_F_AG_AGREEMENT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,FR_ID                   AS FR_ID 
       ,MIN(concat(SUBSTR(START_DATE, 1, 4),'-',SUBSTR(START_DATE, 6, 2),'-',SUBSTR(START_DATE, 9, 2))) AS OPEN_DATE 
       ,MAX(concat(SUBSTR(END_DATE, 1, 4),'-',SUBSTR(END_DATE, 6, 2),'-',SUBSTR(END_DATE, 9, 2))) AS CANCEL_DATE 
   FROM ACRM_F_AG_AGREEMENT A                                  --客户协议表
  GROUP BY FR_ID 
       ,CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_MCRM_RET_CUST_FLOW_01 = sqlContext.sql(sql)
TMP_MCRM_RET_CUST_FLOW_01.registerTempTable("TMP_MCRM_RET_CUST_FLOW_01")
dfn="TMP_MCRM_RET_CUST_FLOW_01/"+V_DT+".parquet"
TMP_MCRM_RET_CUST_FLOW_01.cache()
nrows = TMP_MCRM_RET_CUST_FLOW_01.count()
TMP_MCRM_RET_CUST_FLOW_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_MCRM_RET_CUST_FLOW_01.unpersist()
ACRM_F_AG_AGREEMENT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_MCRM_RET_CUST_FLOW_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_MCRM_RET_CUST_FLOW_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_MANAGER          AS CUST_MANAGER 
       ,A.CUST_MANAGER_NAME     AS CUST_MANAGER_NAME 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_NAME              AS ORG_NAME 
       ,A.CUST_LEVEL            AS CUST_LEVEL 
       ,A.GRADE_DATE            AS GRADE_DATE 
       ,B.OPEN_DATE             AS OPEN_DATE 
       ,C.CANCEL_DATE           AS CANCEL_DATE 
       ,A.MONTH_BAL             AS CUST_ASSETS 
       ,A.OLD_CUST_LEVEL        AS CUST_LEVEL_FU 
       ,A.ST_DATE               AS ST_DATE 
       ,''                    AS O_MAIN_TYPE 
       ,''                    AS M_MAIN_TYPE 
   FROM MCRM_RET_CUST_ASSETS A                                 --客户资产情况表
   LEFT JOIN TMP_MCRM_RET_CUST_FLOW_01 B                       --客户流入流出机构统计表临时表01
     ON A.CUST_ID               = B.CUST_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND SUBSTR(B.OPEN_DATE, 1, 7)                       = SUBSTR(V_DT, 1, 7) 
   LEFT JOIN TMP_MCRM_RET_CUST_FLOW_01 C                       --客户流入流出机构统计表临时表01
     ON A.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
    AND SUBSTR(C.CANCEL_DATE, 1, 7)                       = SUBSTR(V_DT, 1, 7) 
  WHERE A.ST_DATE               = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_CUST_FLOW = sqlContext.sql(sql)
MCRM_RET_CUST_FLOW.registerTempTable("MCRM_RET_CUST_FLOW")
dfn="MCRM_RET_CUST_FLOW/"+V_DT+".parquet"
MCRM_RET_CUST_FLOW.cache()
nrows = MCRM_RET_CUST_FLOW.count()
MCRM_RET_CUST_FLOW.write.save(path=hdfs + '/' + dfn, mode='append')
MCRM_RET_CUST_FLOW.unpersist()
MCRM_RET_CUST_ASSETS.unpersist()
TMP_MCRM_RET_CUST_FLOW_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_RET_CUST_FLOW lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
