#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_COR_CUST_TEN_CLASS_REPORT').setMaster(sys.argv[2])
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

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
OCRM_F_CM_EXCHANGE_RATE = sqlContext.read.parquet(hdfs+'/OCRM_F_CM_EXCHANGE_RATE/*')
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
	SELECT
		 C.CUST_ID
		,C.CORE_CUST_NAME 	as CUST_ZH_NAME
		,'' 				as CUST_INT_CODE
		,C.CUST_TYP 		as CUST_TYP
		,CAST(SUM(COALESCE(C.BAL ,0) * COALESCE(H.EXCHANGE_RATE,1)) AS DECIMAL(24,6))  AS CRE_AMOUNT
		,C.STATI_BUSIN_VAR as BUSINESSTYPE  --业务品种
		,CASE WHEN C.CUST_TYP ='1' THEN C.CLASSIFYRESULT
			  WHEN C.CUST_TYP ='2' THEN C.CURRENTTENRESULT END AS  CURRENTTENRESULT --当期十级分类结果
		,C.FR_ID       				  
	FROM ACRM_F_CI_ASSET_BUSI_PROTO C 
	LEFT JOIN  OCRM_F_CM_EXCHANGE_RATE H
			   ON C.CURR = H.CURRENCY_CD 
			   AND H.OBJECT_CURRENCY_CD = 'CNY' 
			   AND H.ETL_DT = V_DT            
	WHERE C.BAL>0      --合同已生效
	   AND C.LN_APCL_FLG = 'N'      --非核销账户
	   AND  (C.SUBJECTNO LIKE '1301%'
		   OR C.SUBJECTNO LIKE '1302%'
		   OR C.SUBJECTNO LIKE '1303%'
		   OR C.SUBJECTNO LIKE '1304%'
		   OR C.SUBJECTNO LIKE '1305%'
		   OR C.SUBJECTNO LIKE '1306%'
		   OR C.SUBJECTNO LIKE '1307%'
		   OR C.SUBJECTNO LIKE '1308%'
	  ) group by C.CUST_ID,
				 c.FR_ID,
				 c.CORE_CUST_NAME,
				 C.CUST_TYP,
				 C.STATI_BUSIN_VAR,
				CASE WHEN c.CUST_TYP ='1' THEN
						  C.CLASSIFYRESULT
					 WHEN c.CUST_TYP ='2' THEN
						  C.CURRENTTENRESULT 
				 END 	   
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_MCRM_COR_CUST_TEN_CLASS = sqlContext.sql(sql)
TEMP_MCRM_COR_CUST_TEN_CLASS.registerTempTable("TEMP_MCRM_COR_CUST_TEN_CLASS")
dfn="TEMP_MCRM_COR_CUST_TEN_CLASS/"+V_DT+".parquet"
TEMP_MCRM_COR_CUST_TEN_CLASS.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_MCRM_COR_CUST_TEN_CLASS/"+V_DT_LD+".parquet")
ACRM_F_CI_ASSET_BUSI_PROTO.unpersist()
OCRM_F_CM_EXCHANGE_RATE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT 
	     A.CUST_ID               AS CUST_ID
		,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
		,A.CUST_INT_CODE         AS CUST_INT_CODE 
		,TRIM(E.INSTITUTION_NAME)                       AS ORG_NAME 
		,TRIM(E.INSTITUTION_CODE)                       AS ORG_ID 
		,A.BUSINESSTYPE          AS BUSINESSTYPE
		,A.CURRENTTENRESULT      AS CURRENTTENRESULT 
		,A.CRE_AMOUNT            AS CRE_AMOUNT
		,A.CUST_TYP              AS CUST_TYP 
		,V_DT                    AS REPORT_DATE 
		,B.PROD_NAME             AS PROD_NAME 
	   
    FROM TEMP_MCRM_COR_CUST_TEN_CLASS A                         --
    LEFT JOIN OCRM_F_CI_BELONG_ORG E                            --
		ON A.CUST_ID               = E.CUST_ID 
		AND A.FR_ID                 = E.FR_ID 
		AND E.MAIN_TYPE             = '1'
	LEFT JOIN OCRM_F_PD_PROD_INFO B ON A.BUSINESSTYPE = B.PRODUCT_ID --
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_COR_CUST_TEN_CLASS_REPORT = sqlContext.sql(sql)
MCRM_COR_CUST_TEN_CLASS_REPORT.registerTempTable("MCRM_COR_CUST_TEN_CLASS_REPORT")
dfn="MCRM_COR_CUST_TEN_CLASS_REPORT/"+V_DT+".parquet"
MCRM_COR_CUST_TEN_CLASS_REPORT.cache()
nrows = MCRM_COR_CUST_TEN_CLASS_REPORT.count()
MCRM_COR_CUST_TEN_CLASS_REPORT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_COR_CUST_TEN_CLASS_REPORT.unpersist()
OCRM_F_CI_BELONG_ORG.unpersist()
TEMP_MCRM_COR_CUST_TEN_CLASS.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_COR_CUST_TEN_CLASS_REPORT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_COR_CUST_TEN_CLASS_REPORT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
