#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CI_CUST_CONTRIBUTION').setMaster(sys.argv[2])
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
#---------------------------------------------------------------------------------------#
V_YEAR_MONTH =  etl_date[0:4]+"-" + etl_date[4:6]

ACRM_F_CI_CRE_CONTRIBUTION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CRE_CONTRIBUTION/*')
ACRM_F_CI_CRE_CONTRIBUTION.registerTempTable("ACRM_F_CI_CRE_CONTRIBUTION")

ACRM_F_CI_DEP_CONTRIBUTION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_DEP_CONTRIBUTION/*')
ACRM_F_CI_DEP_CONTRIBUTION.registerTempTable("ACRM_F_CI_DEP_CONTRIBUTION")


#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
	SELECT 
		 CUST_ID                  AS CUST_ID,
		 CUST_NAME                AS CUST_NAME,
		 ORG_ID                   AS ORG_ID, 
		 CAST(SUM(COALESCE(B.CRE_CONTRIBUTION_RMB,0)) AS DECIMAL(24,6))  AS CRE_CONTRIBUTION,--贷款贡献度
		 CAST(SUM(COALESCE(B.DEP_CONTRIBUTION_RMB,0)) AS DECIMAL(24,6))  AS DEP_CONTRIBUTION,--存款贡献度
		 CAST(SUM(COALESCE(B.CRE_CONTRIBUTION_RMB,0)) + SUM(COALESCE(B.DEP_CONTRIBUTION_RMB,0)) AS DECIMAL(24,6)) AS CUST_CONTRIBUTION,--客户贡献度
		 V_YEAR_MONTH 	AS YEAR_MONTH,
		 V_DT10 		AS ODS_DATE,
		 CAST(monotonically_increasing_id() AS DECIMAL(18)) AS ID,	 
		 FR_ID                    ,
		 FR_NAME                ,
		 CAST(SUM(COALESCE(B.CRE_BAL_RMB,0)) AS DECIMAL(24,6))       AS CRE_BAL_RMB ,
		 CAST(SUM(COALESCE(B.DEP_BAL_RMB,0)) AS DECIMAL(24,6))       AS DEP_BAL_RMB ,
		 CUST_TYP	 
	FROM	
		(SELECT	CUST_ID,CUST_TYP,ORG_ID,CUST_NAME,0 AS DEP_BAL_RMB,BAL_RMB AS CRE_BAL_RMB,
				CONTRIBUTION_RMB AS CRE_CONTRIBUTION_RMB,0 AS DEP_CONTRIBUTION_RMB,FR_ID,FR_NAME
				FROM ACRM_F_CI_CRE_CONTRIBUTION A 
				WHERE YEAR_MONTH=V_YEAR_MONTH
			UNION ALL
		SELECT	CUST_ID,CUST_TYP,ORG_ID,CUST_NAME,BAL_RMB AS DEP_BAL_RMB, 0 AS CRE_BAL_RMB,
			    0 AS CRE_CONTRIBUTION_RMB,CONTRIBUTION_RMB AS DEP_CONTRIBUTION_RMB,FR_ID,FR_NAME
				FROM ACRM_F_CI_DEP_CONTRIBUTION B
				WHERE  YEAR_MONTH=V_YEAR_MONTH 
		)B
		GROUP BY CUST_ID,CUST_TYP,ORG_ID,CUST_NAME,FR_ID,FR_NAME
"""

sql = re.sub(r"\bV_DT10\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_YEAR_MONTH\b", "'"+V_YEAR_MONTH+"'", sql)

ACRM_F_CI_CUST_CONTRIBUTION = sqlContext.sql(sql)
ACRM_F_CI_CUST_CONTRIBUTION.registerTempTable("ACRM_F_CI_CUST_CONTRIBUTION")
dfn="ACRM_F_CI_CUST_CONTRIBUTION/"+V_DT+".parquet"
ACRM_F_CI_CUST_CONTRIBUTION.cache()
nrows = ACRM_F_CI_CUST_CONTRIBUTION.count()

#清理当天数据，支持重跑
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_CONTRIBUTION/"+V_DT+".parquet")
ACRM_F_CI_CUST_CONTRIBUTION.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_CONTRIBUTION.unpersist()
ACRM_F_CI_CRE_CONTRIBUTION.unpersist()
ACRM_F_CI_DEP_CONTRIBUTION.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_CONTRIBUTION/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_CONTRIBUTION lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
