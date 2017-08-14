#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_RET_PROD_SALES_CK').setMaster(sys.argv[2])
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

#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
OCRM_F_CM_EXCHANGE_RATE = sqlContext.read.parquet(hdfs+'/OCRM_F_CM_EXCHANGE_RATE/*')
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
OCRM_F_CUST_ORG_MGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CUST_ORG_MGR/*')
OCRM_F_CUST_ORG_MGR.registerTempTable("OCRM_F_CUST_ORG_MGR")
#目标表
#MCRM_RET_PROD_SALES 全量表

V_STEP = V_STEP + 1
sql="""
	SELECT
				 A.CUST_ID	 	as CUST_ID														
				,A.CUST_NAME 	as CUST_ZH_NAME
				,A.MGR_ID 	 	as CUST_MANAGER
				,A.MGR_NAME  	as CUST_MANAGER_NAME
				,A.ORG_ID		as ORG_ID
				,A.ORG_NAME		as ORG_NAME
				,A.OBJ_RATING	as CUST_LEVEL
				,C.PRODUCT_ID	as PROD_ID
				,D.PROD_NAME	as PROD_NAME
				,D.CATL_CODE	as CATL_CODE
				,C.CYNO			as CURR
				,0.0			as LAST_MONTH_BAL
				,0.0			as LAST_MONTH_CNY_BAL
				,C.MS_AC_BAL	as BAL
				,cast(C.MS_AC_BAL*COALESCE(EXCHANGE_RATE,1) as DECIMAL(24,6)) as CNY_BAL
				,V_DT 			as ST_DATE
				,A.O_MAIN_TYPE	as O_MAIN_TYPE
				,A.M_MAIN_TYPE	as M_MAIN_TYPE
				,A.FR_ID		as FR_ID
				,'CK'			as SIGN
	FROM (SELECT PRODUCT_ID,CUST_ID,FR_ID,CYNO,SUM(MS_AC_BAL) AS MS_AC_BAL 
					FROM ACRM_F_DP_SAVE_INFO 
					WHERE ACCT_STATUS = '01' AND CUST_TYP = '1'
					GROUP BY PRODUCT_ID,CUST_ID,FR_ID,CYNO)C 
			INNER JOIN OCRM_F_CUST_ORG_MGR A ON C.CUST_ID = A.CUST_ID AND A.FR_ID = C.FR_ID 
			LEFT JOIN OCRM_F_PD_PROD_INFO D ON C.PRODUCT_ID = D.PRODUCT_ID AND D.FR_ID = C.FR_ID 
			LEFT JOIN (SELECT CURRENCY_CD,EXCHANGE_RATE
						FROM OCRM_F_CM_EXCHANGE_RATE B 
						WHERE B.OBJECT_CURRENCY_CD = 'CNY'
							AND B.ETL_DT = V_DT
					)E ON C.CYNO = E.CURRENCY_CD
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_PROD_SALES = sqlContext.sql(sql)
MCRM_RET_PROD_SALES.registerTempTable("MCRM_RET_PROD_SALES")
dfn="MCRM_RET_PROD_SALES/"+V_DT+".parquet"
MCRM_RET_PROD_SALES.write.save(path=hdfs + '/' + dfn, mode='overwrite')
#全量表，删除前一天记录
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_PROD_SALES/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

#-- 更新上月末余额
V_STEP = V_STEP + 1
sql="""
	select CUST_ID,FR_ID,PROD_ID,CURR,BAL,CNY_BAL from MCRM_RET_PROD_SALES t where t.ST_DATE = add_months(V_DT,-1)
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_PROD_SALES_01 = sqlContext.sql(sql)
MCRM_RET_PROD_SALES_01.registerTempTable("MCRM_RET_PROD_SALES_01")

sql="""
	select 
		 A.CUST_ID
		,A.CUST_ZH_NAME
		,A.CUST_MANAGER
		,A.CUST_MANAGER_NAME
		,A.ORG_ID
		,A.ORG_NAME
		,A.CUST_LEVEL
		,A.PROD_ID
		,A.PROD_NAME
		,A.CATL_CODE
		,A.CURR
		,B.BAL as LAST_MONTH_BAL 			-- 上月末余额
		,B.CNY_BAL as LAST_MONTH_CNY_BAL  -- 上月末余额折人民币
		,A.BAL
		,A.CNY_BAL
		,A.ST_DATE
		,A.O_MAIN_TYPE
		,A.M_MAIN_TYPE
		,A.FR_ID
		,A.SIGN
	from MCRM_RET_PROD_SALES A
		inner join MCRM_RET_PROD_SALES_01 B ON A.CUST_ID = B.CUST_ID AND A.FR_ID = B.FR_ID
		AND A.PROD_ID = B.PROD_ID 
		AND A.CURR = B.CURR 
	where A.ST_DATE = V_DT  	 
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_PROD_SALES_INNER1 = sqlContext.sql(sql)
MCRM_RET_PROD_SALES_INNER1.registerTempTable("MCRM_RET_PROD_SALES_INNER1")

sql="""
	select 
		 A.CUST_ID
		,A.CUST_ZH_NAME
		,A.CUST_MANAGER
		,A.CUST_MANAGER_NAME
		,A.ORG_ID
		,A.ORG_NAME
		,A.CUST_LEVEL
		,A.PROD_ID
		,A.PROD_NAME
		,A.CATL_CODE
		,A.CURR
		,A.LAST_MONTH_BAL 			-- 上月末余额
		,A.LAST_MONTH_CNY_BAL  -- 上月末余额折人民币
		,A.BAL
		,A.CNY_BAL
		,A.ST_DATE
		,A.O_MAIN_TYPE
		,A.M_MAIN_TYPE
		,A.FR_ID
		,A.SIGN
	from MCRM_RET_PROD_SALES A
		left join MCRM_RET_PROD_SALES_INNER1 B ON A.CUST_ID = B.CUST_ID AND A.FR_ID = B.FR_ID
												AND A.PROD_ID = B.PROD_ID 
												AND A.CURR = B.CURR 
	where B.CUST_ID is null  	 
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_PROD_SALES_INNER2 = sqlContext.sql(sql)
#MCRM_RET_PROD_SALES_INNER2.registerTempTable("MCRM_RET_PROD_SALES_INNER2")
MCRM_RET_PROD_SALES_INNER2 = MCRM_RET_PROD_SALES_INNER2.unionAll(MCRM_RET_PROD_SALES_INNER1)
dfn="MCRM_RET_PROD_SALES/"+V_DT+".parquet"
MCRM_RET_PROD_SALES_INNER2.write.save(path=hdfs + '/' + dfn, mode='overwrite')
#全量表，删除前一天记录
#前面已做删除
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_PROD_SALES/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)


