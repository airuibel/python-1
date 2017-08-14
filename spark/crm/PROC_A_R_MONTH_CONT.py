#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os
import sys, re, os , calendar

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_MONTH_CONT').setMaster(sys.argv[2])
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
#本月月末日期（10位）
monthRange = calendar.monthrange(int(etl_date[0:3]),int(etl_date[4:6]))#得到本月的天数   
V_LAST_DAY = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(str(monthRange[1])))).strftime("%Y-%m-%d") 
#上月末日期
V_DT_LMD = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1) + timedelta(-1)).strftime("%Y%m%d")
#10位日期
V_DT10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8]))).strftime("%Y-%m-%d")
V_STEP = 0

#判断是否月末
if V_DT10 == V_LAST_DAY :

	ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
	ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
	ACRM_F_CI_CUST_CONTRIBUTION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_CONTRIBUTION/*')
	ACRM_F_CI_CUST_CONTRIBUTION.registerTempTable("ACRM_F_CI_CUST_CONTRIBUTION")


	#任务[21] 001-01::
	V_STEP = V_STEP + 1

	sql = """
	 SELECT monotonically_increasing_id()    AS ID 
		   ,A.ORG_ID                AS ORG_ID 
		   ,B.ORG_NAME              AS ORG_NAME 
		   ,SUM(A.DEP_CONTRIBUTION)                       AS CONTRI_DEPOSIT 
		   ,SUM(A.CRE_CONTRIBUTION)                       AS CONTRIBUTION_LOAN 
		   ,SUM(A.CUST_CONTRIBUTION)                       AS CONTRIBUTION_CUST 
		   ,V_DT                    AS ODS_DATE 
		   ,A.FR_ID                 AS FR_ID 
		   ,B.FR_NAME               AS FR_NAME 
	   FROM ACRM_F_CI_CUST_CONTRIBUTION A                          --客户贡献度
	  INNER JOIN ADMIN_AUTH_ORG B                                  --机构表
		 ON A.ORG_ID                = B.ORG_ID 
		AND A.FR_ID                 = B.FR_ID 
	  WHERE ODS_DATE                = V_DT 
	  GROUP BY A.ORG_ID 
		   ,A.FR_ID 
		   ,B.ORG_NAME 
		   ,B.FR_NAME """

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	ACRM_A_MONTH_CONT = sqlContext.sql(sql)
	ACRM_A_MONTH_CONT.registerTempTable("ACRM_A_MONTH_CONT")
	dfn="ACRM_A_MONTH_CONT/"+V_DT+".parquet"
	ACRM_A_MONTH_CONT.cache()
	nrows = ACRM_A_MONTH_CONT.count()
	ACRM_A_MONTH_CONT.write.save(path=hdfs + '/' + dfn, mode='append')
	ACRM_A_MONTH_CONT.unpersist()
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_MONTH_CONT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
