#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_GK_LOAN').setMaster(sys.argv[2])
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

ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1
#----------------------------------------------------------------------------------
V_YEAR = etl_date[0:4]
V_MONTH = str(int(etl_date[4:6])) #月
V_QUARTER = str((int(V_MONTH)-1)/3 + 1); #当月是第几季度

sql = """
 SELECT  
	 A.FR_ID
	,A.CUST_ID
	,PRDT_CODE--账户类型
	,cast(SUM(AMOUNT) as DECIMAL(18,2)) AS AMOUNT --本年余额
	,cast(SUM(MONTH_BAL_12) as DECIMAL(18,2)) AS LAST_YEAR_AMOUNT--上年余额
	,cast(SUM( (COALESCE(MONTH_BAL_SUM_1,0)+COALESCE(MONTH_BAL_SUM_2,0)+COALESCE(MONTH_BAL_SUM_3,0)+
		COALESCE(MONTH_BAL_SUM_4,0)+COALESCE(MONTH_BAL_SUM_5,0)+COALESCE(MONTH_BAL_SUM_6,0)+
		COALESCE(MONTH_BAL_SUM_7,0)+COALESCE(MONTH_BAL_SUM_8,0)+COALESCE(MONTH_BAL_SUM_9,0)+
		COALESCE(MONTH_BAL_SUM_10,0)+COALESCE(MONTH_BAL_SUM_11,0)+COALESCE(MONTH_BAL_SUM_12,0) )/
		(COALESCE(MONTH_DAYS_1,0)+COALESCE(MONTH_DAYS_2,0)+COALESCE(MONTH_DAYS_3,0)+
		COALESCE(MONTH_DAYS_4,0)+COALESCE(MONTH_DAYS_5,0)+COALESCE(MONTH_DAYS_6,0)+
		COALESCE(MONTH_DAYS_7,0)+COALESCE(MONTH_DAYS_8,0)+COALESCE(MONTH_DAYS_9,0)+
		COALESCE(MONTH_DAYS_10,0)+COALESCE(MONTH_DAYS_11,0)+COALESCE(MONTH_DAYS_12,0))) as DECIMAL(18,2))  AS CUR_YEAR_AVG  --本年日均
	,cast(SUM(COALESCE(MONTH_BAL_SUM_V_MONTH,0) / COALESCE(MONTH_DAYS_V_MONTH,0)) as DECIMAL(18,2)) AS CUR_MONTH_AVG 
	,cast(CASE V_QUARTER WHEN 1 THEN  SUM((COALESCE(MONTH_BAL_SUM_1,0)+COALESCE(MONTH_BAL_SUM_2,0)+COALESCE(MONTH_BAL_SUM_3,0))/
		(CASE  COALESCE(MONTH_DAYS_1,0) + COALESCE(MONTH_DAYS_2,0) + COALESCE(MONTH_DAYS_3,0) WHEN 0 THEN 1
		ELSE COALESCE(MONTH_DAYS_1,0) + COALESCE(MONTH_DAYS_2,0) + COALESCE(MONTH_DAYS_3,0)
		END 
		)) 
		WHEN 2 THEN  SUM((COALESCE(MONTH_BAL_SUM_4,0)+COALESCE(MONTH_BAL_SUM_5,0)+COALESCE(MONTH_BAL_SUM_6,0))/
		(CASE  COALESCE(MONTH_DAYS_4,0) + COALESCE(MONTH_DAYS_5,0) + COALESCE(MONTH_DAYS_6,0) WHEN 0 THEN 1
		ELSE COALESCE(MONTH_DAYS_4,0) + COALESCE(MONTH_DAYS_5,0) + COALESCE(MONTH_DAYS_6,0)
		END 
		))      
		WHEN 3 THEN   SUM((COALESCE(MONTH_BAL_SUM_7,0)+COALESCE(MONTH_BAL_SUM_8,0)+COALESCE(MONTH_BAL_SUM_9,0))/
		(CASE  COALESCE(MONTH_DAYS_7,0) + COALESCE(MONTH_DAYS_8,0) + COALESCE(MONTH_DAYS_9,0) WHEN 0 THEN 1
		ELSE COALESCE(MONTH_DAYS_7,0) + COALESCE(MONTH_DAYS_8,0) + COALESCE(MONTH_DAYS_9,0)
		END 
		))                     
		ELSE  SUM((COALESCE(MONTH_BAL_SUM_10,0)+COALESCE(MONTH_BAL_SUM_11,0)+COALESCE(MONTH_BAL_SUM_12,0))/
		(CASE  COALESCE(MONTH_DAYS_10,0) + COALESCE(MONTH_DAYS_11,0) + COALESCE(MONTH_DAYS_12,0) WHEN 0 THEN 1
		ELSE COALESCE(MONTH_DAYS_10,0) + COALESCE(MONTH_DAYS_11,0) + COALESCE(MONTH_DAYS_12,0)
		END 
		))                 
		END  as DECIMAL(18,2))       AS CUR_QUARTER_AVG --本年季均
	,CUST_TYP                                                   
FROM ACRM_F_RE_LENDSUMAVGINFO A 
WHERE YEAR=V_YEAR 
	AND a.FR_ID is not null AND A.PRDT_CODE IS NOT NULL 
GROUP BY a.FR_ID,CUST_ID,PRDT_CODE,CUST_TYP
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"V_MONTH", V_MONTH, sql)
sql = re.sub(r"\bV_QUARTER\b", V_QUARTER, sql)
sql = re.sub(r"\bV_YEAR\b", "'"+V_YEAR+"'", sql)
print(sql)
TMP_ACRM_F_CI_GK_LOAN_01 = sqlContext.sql(sql)
TMP_ACRM_F_CI_GK_LOAN_01.registerTempTable("TMP_ACRM_F_CI_GK_LOAN_01")
dfn="TMP_ACRM_F_CI_GK_LOAN_01/"+V_DT+".parquet"
TMP_ACRM_F_CI_GK_LOAN_01.cache()
nrows = TMP_ACRM_F_CI_GK_LOAN_01.count()
TMP_ACRM_F_CI_GK_LOAN_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_ACRM_F_CI_GK_LOAN_01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_ACRM_F_CI_GK_LOAN_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_ACRM_F_CI_GK_LOAN_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
	SELECT 
		A.FR_ID, 
		A.CUST_ID,
		PRDT_CODE,
		CUST_TYP,
		cast(SUM( (COALESCE(MONTH_BAL_SUM_1,0)+COALESCE(MONTH_BAL_SUM_2,0)+COALESCE(MONTH_BAL_SUM_3,0)+
			COALESCE(MONTH_BAL_SUM_4,0)+COALESCE(MONTH_BAL_SUM_5,0)+COALESCE(MONTH_BAL_SUM_6,0)+
			COALESCE(MONTH_BAL_SUM_7,0)+COALESCE(MONTH_BAL_SUM_8,0)+COALESCE(MONTH_BAL_SUM_9,0)+
			COALESCE(MONTH_BAL_SUM_10,0)+COALESCE(MONTH_BAL_SUM_11,0)+COALESCE(MONTH_BAL_SUM_12,0) )/
			(COALESCE(MONTH_DAYS_1,0)+COALESCE(MONTH_DAYS_2,0)+COALESCE(MONTH_DAYS_3,0)+
			COALESCE(MONTH_DAYS_4,0)+COALESCE(MONTH_DAYS_5,0)+COALESCE(MONTH_DAYS_6,0)+
			COALESCE(MONTH_DAYS_7,0)+COALESCE(MONTH_DAYS_8,0)+COALESCE(MONTH_DAYS_9,0)+
			COALESCE(MONTH_DAYS_10,0)+COALESCE(MONTH_DAYS_11,0)+COALESCE(MONTH_DAYS_12,0))) as DECIMAL(18,2))  AS LAST_YEAR_AVG  --上年年均
		,cast(SUM(COALESCE(MONTH_BAL_SUM_V_MONTH,0) / COALESCE(MONTH_DAYS_V_MONTH,0)) as DECIMAL(18,2)) AS LAST_MONTH_AVG  --上年月均
		,cast(CASE V_QUARTER WHEN 1 THEN  SUM((COALESCE(MONTH_BAL_SUM_1,0)+COALESCE(MONTH_BAL_SUM_2,0)+COALESCE(MONTH_BAL_SUM_3,0))/
			(CASE  COALESCE(MONTH_DAYS_1,0) + COALESCE(MONTH_DAYS_2,0) + COALESCE(MONTH_DAYS_3,0) WHEN 0 THEN 1
			ELSE COALESCE(MONTH_DAYS_1,0) + COALESCE(MONTH_DAYS_2,0) + COALESCE(MONTH_DAYS_3,0)
			END 
			)) 
			WHEN 2 THEN  SUM((COALESCE(MONTH_BAL_SUM_4,0)+COALESCE(MONTH_BAL_SUM_5,0)+COALESCE(MONTH_BAL_SUM_6,0))/
			(CASE  COALESCE(MONTH_DAYS_4,0) + COALESCE(MONTH_DAYS_5,0) + COALESCE(MONTH_DAYS_6,0) WHEN 0 THEN 1
			ELSE COALESCE(MONTH_DAYS_4,0) + COALESCE(MONTH_DAYS_5,0) + COALESCE(MONTH_DAYS_6,0)
			END 
			))      
			WHEN 3 THEN   SUM((COALESCE(MONTH_BAL_SUM_7,0)+COALESCE(MONTH_BAL_SUM_8,0)+COALESCE(MONTH_BAL_SUM_9,0))/
			(CASE  COALESCE(MONTH_DAYS_7,0) + COALESCE(MONTH_DAYS_8,0) + COALESCE(MONTH_DAYS_9,0) WHEN 0 THEN 1
			ELSE COALESCE(MONTH_DAYS_7,0) + COALESCE(MONTH_DAYS_8,0) + COALESCE(MONTH_DAYS_9,0)
			END 
			))                     
			ELSE  SUM((COALESCE(MONTH_BAL_SUM_10,0)+COALESCE(MONTH_BAL_SUM_11,0)+COALESCE(MONTH_BAL_SUM_12,0))/
			(CASE  COALESCE(MONTH_DAYS_10,0) + COALESCE(MONTH_DAYS_11,0) + COALESCE(MONTH_DAYS_12,0) WHEN 0 THEN 1
			ELSE COALESCE(MONTH_DAYS_10,0) + COALESCE(MONTH_DAYS_11,0) + COALESCE(MONTH_DAYS_12,0)
			END 
			))                 
			END as DECIMAL(18,2))  AS LAST_QUARTER_AVG --上年季均       
	FROM ACRM_F_RE_LENDSUMAVGINFO A 
	WHERE YEAR=V_YEAR  
		AND a.fr_id is not null  AND A.PRDT_CODE IS NOT NULL
	GROUP BY a.FR_ID,CUST_ID,PRDT_CODE,CUST_TYP           
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"V_MONTH", V_MONTH, sql)
sql = re.sub(r"\bV_QUARTER\b", V_QUARTER, sql)
sql = re.sub(r"\bV_YEAR\b", "'"+V_YEAR+"'", sql)
print(sql)
TMP_ACRM_F_CI_GK_LOAN_02 = sqlContext.sql(sql)
TMP_ACRM_F_CI_GK_LOAN_02.registerTempTable("TMP_ACRM_F_CI_GK_LOAN_02")
dfn="TMP_ACRM_F_CI_GK_LOAN_02/"+V_DT+".parquet"
TMP_ACRM_F_CI_GK_LOAN_02.cache()
nrows = TMP_ACRM_F_CI_GK_LOAN_02.count()
TMP_ACRM_F_CI_GK_LOAN_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_ACRM_F_CI_GK_LOAN_02.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_ACRM_F_CI_GK_LOAN_02/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_ACRM_F_CI_GK_LOAN_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT BB.CUST_ID              AS CUST_ID 
       ,BB.PRDT_CODE            AS LOAN_TYP 
       ,BB.AMOUNT               AS CUR_AC_BL 
       ,BB.LAST_YEAR_AMOUNT     AS LAST_AC_BL 
       ,AA.LAST_YEAR_AVG        AS LAST_YEAR_AVG 
       ,BB.CUR_YEAR_AVG         AS CUR_YEAR_AVG 
       ,AA.LAST_MONTH_AVG       AS LAST_MONTH_AVG 
       ,BB.CUR_MONTH_AVG        AS CUR_MONTH_AVG 
       ,AA.LAST_QUARTER_AVG     AS LAST_QUARTER_AVG 
       ,BB.CUR_QUARTER_AVG      AS CUR_QUARTER_AVG 
       ,BB.CUST_TYP             AS CUST_TYP 
       ,BB.FR_ID                AS FR_ORG_ID 
       ,V_DT                    AS ETL_DATE 
       ,BB.FR_ID                AS FR_ID 
   FROM TMP_ACRM_F_CI_GK_LOAN_01 BB                            --贷款业务概况临时表01(当年贷款)
   LEFT JOIN TMP_ACRM_F_CI_GK_LOAN_02 AA                       --贷款业务概况临时表02(去年贷款)
     ON BB.CUST_ID              = AA.CUST_ID 
    AND BB.FR_ID                = AA.FR_ID 
    AND BB.PRDT_CODE            = AA.PRDT_CODE 
    AND BB.CUST_TYP             = AA.CUST_TYP 
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_GK_LOAN = sqlContext.sql(sql)
ACRM_F_CI_GK_LOAN.registerTempTable("ACRM_F_CI_GK_LOAN")
dfn="ACRM_F_CI_GK_LOAN/"+V_DT+".parquet"
ACRM_F_CI_GK_LOAN.cache()
nrows = ACRM_F_CI_GK_LOAN.count()
ACRM_F_CI_GK_LOAN.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_GK_LOAN.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_GK_LOAN/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_GK_LOAN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
