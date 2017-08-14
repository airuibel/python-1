#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_CREDIT_DAY_CHART').setMaster(sys.argv[2])
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
#上三十日期
V_DT_30LD = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8])) + timedelta(-30)).strftime("%Y%m%d")
#月初日期
V_DT_FMD = date(int(etl_date[0:4]), int(etl_date[4:6]), 1).strftime("%Y%m%d") 
#上月末日期
V_DT_LMD = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1) + timedelta(-1)).strftime("%Y%m%d")
#10位日期
V_DT10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8]))).strftime("%Y-%m-%d")



V_STEP = 0

ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CREDIT_DAY_CHART/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_A_CREDIT_DAY_CHART_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_CREDIT_DAY_CHART/")    #方法一

#----------来源表---------------
ACRM_A_CREDIT_MONTH_CHART = sqlContext.read.parquet(hdfs+'/ACRM_A_CREDIT_MONTH_CHART/*')
ACRM_A_CREDIT_MONTH_CHART.registerTempTable("ACRM_A_CREDIT_MONTH_CHART")

#ACRM_A_CREDIT_DAY_CHART = sqlContext.read.parquet(hdfs+'/ACRM_A_CREDIT_DAY_CHART/*')
#ACRM_A_CREDIT_DAY_CHART.registerTempTable("ACRM_A_CREDIT_DAY_CHART")

#----------目标表---------------
ACRM_A_CREDIT_DAY_CHART = sqlContext.read.parquet(hdfs+'/ACRM_A_CREDIT_DAY_CHART/*')
ACRM_A_CREDIT_DAY_CHART.registerTempTable("ACRM_A_CREDIT_DAY_CHART")

#任务[21] 001-01::
V_STEP = V_STEP + 1
#只保留30天的数据
if  V_DT_30LD != V_DT_LMD :
	sql = """
	 SELECT ID                      AS ID 
		   ,ORG_ID                  AS ORG_ID 
		   ,ORG_NAME                AS ORG_NAME 
		   ,CRE_BAL                 AS CRE_BAL 
		   ,CRE_YEAR_AVG            AS CRE_YEAR_AVG 
		   ,ODS_DATE                AS ODS_DATE 
		   ,FR_ID                   AS FR_ID 
		   ,FR_NAME                 AS FR_NAME 
	   FROM ACRM_A_CREDIT_DAY_CHART A                              --机构存款趋势图（连续30天)
	  WHERE ODS_DATE <> V_DT_30LD """

	sql = re.sub(r"\bV_DT_30LD\b", "'"+V_DT_30LD+"'", sql)
	ACRM_A_CREDIT_DAY_CHART_TMP = sqlContext.sql(sql)
	ACRM_A_CREDIT_DAY_CHART_TMP.registerTempTable("ACRM_A_CREDIT_DAY_CHART_TMP")
	dfn="ACRM_A_CREDIT_DAY_CHART_TMP/"+V_DT+".parquet"
	ACRM_A_CREDIT_DAY_CHART_TMP.cache()
	nrows = ACRM_A_CREDIT_DAY_CHART_TMP.count()
	ACRM_A_CREDIT_DAY_CHART_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
	ACRM_A_CREDIT_DAY_CHART_TMP.unpersist()
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CREDIT_DAY_CHART_TMP/"+V_DT_LD+".parquet")
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CREDIT_DAY_CHART_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

	#任务[21] 001-02::
	V_STEP = V_STEP + 1

	sql = """
	 SELECT ID                      AS ID 
		   ,ORG_ID                  AS ORG_ID 
		   ,ORG_NAME                AS ORG_NAME 
		   ,CRE_BAL                 AS CRE_BAL 
		   ,CRE_YEAR_AVG            AS CRE_YEAR_AVG 
		   ,ODS_DATE                AS ODS_DATE 
		   ,FR_ID                   AS FR_ID 
		   ,FR_NAME                 AS FR_NAME 
	   FROM ACRM_A_CREDIT_DAY_CHART_TMP A                          --机构存款趋势图（连续30天)
	"""

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	ACRM_A_CREDIT_DAY_CHART = sqlContext.sql(sql)
	dfn="ACRM_A_CREDIT_DAY_CHART/"+V_DT+".parquet"
	ACRM_A_CREDIT_DAY_CHART.cache()
	nrows = ACRM_A_CREDIT_DAY_CHART.count()
	ACRM_A_CREDIT_DAY_CHART.write.save(path=hdfs + '/' + dfn, mode='overwrite')
	ACRM_A_CREDIT_DAY_CHART.unpersist()
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CREDIT_DAY_CHART/"+V_DT_LD+".parquet")
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CREDIT_DAY_CHART lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS INTEGER)       AS ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_NAME              AS ORG_NAME 
       ,A.CRE_BAL               AS CRE_BAL 
       ,A.CRE_YEAR_AVG          AS CRE_YEAR_AVG 
       ,A.REPORT_DATE           AS ODS_DATE 
       ,CAST(''    AS VARCHAR(100))                AS FR_ID 
       ,CAST(''   AS VARCHAR(100))                AS FR_NAME 
   FROM ACRM_A_CREDIT_MONTH_CHART A                            --机构贷款趋势图
  WHERE A.REPORT_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CREDIT_DAY_CHART = sqlContext.sql(sql)
dfn="ACRM_A_CREDIT_DAY_CHART/"+V_DT+".parquet"
ACRM_A_CREDIT_DAY_CHART.cache()
nrows = ACRM_A_CREDIT_DAY_CHART.count()
ACRM_A_CREDIT_DAY_CHART.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_A_CREDIT_DAY_CHART.unpersist()
ACRM_A_CREDIT_DAY_CHART_TMP.unpersist()
ACRM_A_CREDIT_MONTH_CHART.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CREDIT_DAY_CHART_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_A_CREDIT_DAY_CHART/"+V_DT+".parquet /"+dbname+"/ACRM_A_CREDIT_DAY_CHART_BK/")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CREDIT_DAY_CHART lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
