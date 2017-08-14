#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_CREDIT_MONTH_CHART_WQ').setMaster(sys.argv[2])
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
#月初1号日期
V_DT_LD10 = date(int(etl_date[0:4]), int(etl_date[4:6]), 1).strftime("%Y-%m-%d")


ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
ACRM_A_CREDIT_MONTH_CHART = sqlContext.read.parquet(hdfs+'/ACRM_A_CREDIT_MONTH_CHART_BK/'+V_DT_LD+'.parquet/*')
ACRM_A_CREDIT_MONTH_CHART.registerTempTable("ACRM_A_CREDIT_MONTH_CHART")


#任务[21] 001-01::
V_STEP = V_STEP + 1
#判断不为月初1号时，删除昨日数据
if V_DT != V_DT_LD10 :
	sql = """
	 SELECT ID                    AS ID 
		   ,ORG_ID                  AS ORG_ID 
		   ,ORG_NAME                AS ORG_NAME 
		   ,COUNT_YEAR              AS COUNT_YEAR 
		   ,COUNT_MONTH             AS COUNT_MONTH 
		   ,CRE_BAL                 AS CRE_BAL 
		   ,CRE_YEAR_AVG            AS CRE_YEAR_AVG 
		   ,REPORT_DATE             AS REPORT_DATE 
		   ,FR_ID                   AS FR_ID 
		   ,FR_NAME                 AS FR_NAME 
	   FROM ACRM_A_CREDIT_MONTH_CHART A                            --
	  WHERE REPORT_DATE <> V_DT_LD"""

	sql = re.sub(r"\bV_DT_LD\b", "'"+V_DT_LD+"'", sql)
	ACRM_A_CREDIT_MONTH_CHART = sqlContext.sql(sql)
	ACRM_A_CREDIT_MONTH_CHART.registerTempTable("ACRM_A_CREDIT_MONTH_CHART")
	dfn="ACRM_A_CREDIT_MONTH_CHART/"+V_DT+".parquet"
	ACRM_A_CREDIT_MONTH_CHART.cache()
	nrows = ACRM_A_CREDIT_MONTH_CHART.count()
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CREDIT_MONTH_CHART/*.parquet")
	ACRM_A_CREDIT_MONTH_CHART.write.save(path=hdfs + '/' + dfn, mode='overwrite')
	ACRM_A_CREDIT_MONTH_CHART.unpersist()
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CREDIT_MONTH_CHART lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS BIGINT)  AS ID 
       ,B.ORG_ID                AS ORG_ID 
       ,B.ORG_NAME              AS ORG_NAME 
       ,CAST(YEAR(V_DT)  AS VARCHAR(4))                     AS COUNT_YEAR 
       ,CAST(MONTH(V_DT)      AS VARCHAR(2))                 AS COUNT_MONTH 
       ,CAST(SUM(A.BAL)   AS DECIMAL(24,6))                  AS CRE_BAL 
       ,CAST(SUM(A.MVAL_RMB)  AS DECIMAL(24,6))                     AS CRE_YEAR_AVG 
       ,V_DT                  AS REPORT_DATE 
       ,B.FR_ID                 AS FR_ID 
       ,B.FR_NAME               AS FR_NAME 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --
   LEFT JOIN ADMIN_AUTH_ORG B                                  --
     ON A.AGENCY_BRAN           = B.ORG_ID 
  WHERE A.LN_APCL_FLG           = 'N' 
  GROUP BY A.AGENCY_BRAN 
       ,B.ORG_ID 
       ,B.ORG_NAME 
       ,B.FR_ID 
       ,B.FR_NAME """

sql = re.sub(r"\bV_DT\b", "'"+V_DT+"'", sql)
ACRM_A_CREDIT_MONTH_CHART = sqlContext.sql(sql)
ACRM_A_CREDIT_MONTH_CHART.registerTempTable("ACRM_A_CREDIT_MONTH_CHART")
dfn="ACRM_A_CREDIT_MONTH_CHART/"+V_DT+".parquet"
ACRM_A_CREDIT_MONTH_CHART.cache()
nrows = ACRM_A_CREDIT_MONTH_CHART.count()
ACRM_A_CREDIT_MONTH_CHART.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_CREDIT_MONTH_CHART.unpersist()
ACRM_A_CREDIT_MONTH_CHART.unpersist()
ADMIN_AUTH_ORG.unpersist()
ACRM_F_CI_ASSET_BUSI_PROTO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CREDIT_MONTH_CHART lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CREDIT_MONTH_CHART_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_A_CREDIT_MONTH_CHART/"+V_DT+".parquet /"+dbname+"/ACRM_A_CREDIT_MONTH_CHART_BK/")

