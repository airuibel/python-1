#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CI_DEP_CONTRIBUTION').setMaster(sys.argv[2])
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

OCRM_F_CI_CON_PARM = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CON_PARM/*')
OCRM_F_CI_CON_PARM.registerTempTable("OCRM_F_CI_CON_PARM")

ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT	CUST_ID                                         as CUST_ID,		
		CUST_NAME                                       as CUST_NAME,
		ODS_ACCT_NO                                     as ACCT_NO,
		AA.ORG_ID                                       as ORG_ID,
		CYNO                                            as CURR,		  
		COALESCE(MONTH_RMB,0)                           as MONTH_AVG,   
		COALESCE(MVAL_RMB,0)                            as YEAR_AVG,   
		PRODUCT_ID                                      as CONT_SUB_ID  ,
		cast(COALESCE(AA.AGREENMENT_RATE,AA.TD_IR_TP)/100/12 as decimal(24,6))    	as MONTH_RATE ,
		cast(COALESCE(B.INNER_PRICE,1)/12 as decimal(24,6))              			as INNER_PRICE,
		RUN_COST                                       								as RUN_COST ,
		CASE WHEN (COALESCE(YEAR_AVG,0) *(cast(COALESCE(B.INNER_PRICE,1)/12 as decimal(24,6)) - cast(COALESCE(AA.AGREENMENT_RATE,AA.TD_IR_TP)/100/12 as decimal(24,6))) - COALESCE(RUN_COST,0))>0 THEN (COALESCE(YEAR_AVG,0) *(cast(COALESCE(B.INNER_PRICE,1)/12 as decimal(24,6)) - cast(COALESCE(AA.AGREENMENT_RATE,AA.TD_IR_TP)/100/12 as decimal(24,6))) - COALESCE(RUN_COST,0)) ELSE 0 END  as CONTRIBUTION, 
		CASE WHEN (COALESCE(MVAL_RMB,0) *(cast(COALESCE(B.INNER_PRICE,1)/12 as decimal(24,6)) - cast(COALESCE(AA.AGREENMENT_RATE,AA.TD_IR_TP)/100/12 as decimal(24,6)))- COALESCE(RUN_COST,0))>0 THEN (COALESCE(MVAL_RMB,0) * (cast(COALESCE(B.INNER_PRICE,1)/12 as decimal(24,6)) - cast(COALESCE(AA.AGREENMENT_RATE,AA.TD_IR_TP)/100/12 as decimal(24,6)))- COALESCE(RUN_COST,0)) ELSE 0 END as CONTRIBUTION_RMB, 
		V_YEAR_MONTH as YEAR_MONTH,
		V_DT as ODS_DATE,                                       
		COALESCE(BAL_RMB,0) as BAL_RMB, 
		AA.FR_ID 			as FR_ID,
		'' 					as FR_NAME,
		CUST_TYP            as CUST_TYP											
	FROM
		(SELECT A.CUST_ID,
				A.CUST_TYP,  
				A.CUST_NAME,  
				A.ODS_ACCT_NO,  
				A.ORG_ID,  
				A.CYNO,
				A.BAL_RMB,
				A.MONTH_RMB,         
				A.MVAL_RMB,
				A.MONTH_AVG,
				A.YEAR_AVG,
				A.PRODUCT_ID,
				A.TD_IR_TP,
				A.AGREENMENT_RATE,
				  (CASE WHEN A.ACCONT_TYPE = 'H' THEN 'H001' 
						WHEN A.PRODUCT_ID = '999TD000100' THEN 'D001' 
						WHEN A.PRODUCT_ID = '999TD110600' OR  A.PRODUCT_ID = '999TD000600' THEN 'D002' 
						WHEN A.PRODUCT_ID = '999TD110700' OR  A.PRODUCT_ID = '999TD000700' THEN 'D003' 
						WHEN A.ACCONT_TYPE = 'D' AND A.PERD_UNIT = '3' THEN 'D004' 
						WHEN A.ACCONT_TYPE = 'D' AND A.PERD_UNIT = '6' THEN 'D005' 
						WHEN A.ACCONT_TYPE = 'D' AND A.PERD_UNIT = '9' THEN 'D006' 
						WHEN A.ACCONT_TYPE = 'D' AND A.PERD_UNIT = '12' THEN 'D007' 
						WHEN A.ACCONT_TYPE = 'D' AND A.PERD_UNIT = '24' THEN 'D008' 
						WHEN A.ACCONT_TYPE = 'D' AND A.PERD_UNIT = '36' THEN 'D009' 
						WHEN A.ACCONT_TYPE = 'D' AND A.PERD_UNIT = '60' THEN 'D010' 
						WHEN A.ACCONT_TYPE = 'D' AND A.PERD_UNIT > '60' THEN 'D011' END
				  ) AS SUB_ID,
				A.FR_ID
		FROM	ACRM_F_DP_SAVE_INFO A  
	   WHERE A.CUST_ID <> 'X9999999999999999999'
	         --AND A.FR_ID = V_FR_ID
   )  AA
LEFT JOIN OCRM_F_CI_CON_PARM B ON AA.SUB_ID = B.SUB_ID AND AA.FR_ID = B.ORG_ID
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_YEAR_MONTH\b", "'"+V_YEAR_MONTH+"'", sql)

ACRM_F_CI_DEP_CONTRIBUTION = sqlContext.sql(sql)
ACRM_F_CI_DEP_CONTRIBUTION.registerTempTable("ACRM_F_CI_DEP_CONTRIBUTION")
dfn="ACRM_F_CI_DEP_CONTRIBUTION/"+V_DT+".parquet"
ACRM_F_CI_DEP_CONTRIBUTION.cache()
nrows = ACRM_F_CI_DEP_CONTRIBUTION.count()
ACRM_F_CI_DEP_CONTRIBUTION.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_DEP_CONTRIBUTION.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_DEP_CONTRIBUTION/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_DEP_CONTRIBUTION lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
