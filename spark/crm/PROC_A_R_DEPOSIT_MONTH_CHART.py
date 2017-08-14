#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_DEPOSIT_MONTH_CHART').setMaster(sys.argv[2])
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

#删除当天的
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEPOSIT_MONTH_CHART/"+V_DT+".parquet")
#----------来源表---------------
ACRM_A_DEPOSIT_MONTH_CHART_TEMP1 = sqlContext.read.parquet(hdfs+'/ACRM_A_DEPOSIT_MONTH_CHART_TEMP1/*')
ACRM_A_DEPOSIT_MONTH_CHART_TEMP1.registerTempTable("ACRM_A_DEPOSIT_MONTH_CHART_TEMP1")
ACRM_A_DEPOSIT_MONTH_CHART_TEMP = sqlContext.read.parquet(hdfs+'/ACRM_A_DEPOSIT_MONTH_CHART_TEMP/*')
ACRM_A_DEPOSIT_MONTH_CHART_TEMP.registerTempTable("ACRM_A_DEPOSIT_MONTH_CHART_TEMP")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")

#任务[11] 001-01::
V_STEP = V_STEP + 1
#判断为月初时
if V_DT != V_DT_FMD :
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_DEPOSIT_MONTH_CHART/"+V_DT_LD+".parquet")

sql = """
 SELECT CAST(''  AS BIGINT)              AS ID 
       ,C.ORG_ID                AS ORG_ID 
       ,B.ORG_NAME              AS ORG_NAME 
       ,CAST(YEAR(V_DT)    AS VARCHAR(4))                   AS COUNT_YEAR 
       ,CAST(MONTH(V_DT)  AS VARCHAR(2))                     AS COUNT_MONTH 
       ,CAST(NVL(SUM(A.DEP_BAL), 0)  AS DECIMAL(24,6))                     AS DEP_BAL 
       ,CAST(NVL(SUM(C.DEP_YEAR_AVG), 0) AS DECIMAL(24,6))                      AS DEP_YEAR_AVG 
       ,V_DT                       AS REPORT_DATE 
       ,A.FR_ID                 AS FR_ID 
       ,B.FR_NAME               AS FR_NAME 
   FROM ACRM_A_DEPOSIT_MONTH_CHART_TEMP C                      --
   LEFT JOIN ( SELECT FR_ID,
           ORG_ID,
           SUM(DEP_BAL) DEP_BAL
    FROM ACRM_A_DEPOSIT_MONTH_CHART_TEMP1  
    WHERE PRODUCT_ID NOT IN ('999SA110308','999SA110301','999SA110204','999SA110200')
    GROUP BY FR_ID,ORG_ID) A                --
     ON A.ORG_ID                = C.ORG_ID 
   LEFT JOIN ADMIN_AUTH_ORG B  
   ON C.ORG_ID=B.ORG_ID
  GROUP BY A.FR_ID
        ,B.FR_NAME 
       ,C.ORG_ID 
       ,B.ORG_NAME """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_DEPOSIT_MONTH_CHART = sqlContext.sql(sql)
ACRM_A_DEPOSIT_MONTH_CHART.registerTempTable("ACRM_A_DEPOSIT_MONTH_CHART")
dfn="ACRM_A_DEPOSIT_MONTH_CHART/"+V_DT+".parquet"
ACRM_A_DEPOSIT_MONTH_CHART.cache()
nrows = ACRM_A_DEPOSIT_MONTH_CHART.count()
ACRM_A_DEPOSIT_MONTH_CHART.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_DEPOSIT_MONTH_CHART.unpersist()
ACRM_A_DEPOSIT_MONTH_CHART_TEMP1.unpersist()
ACRM_A_DEPOSIT_MONTH_CHART_TEMP.unpersist()
ADMIN_AUTH_ORG.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_DEPOSIT_MONTH_CHART lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
 