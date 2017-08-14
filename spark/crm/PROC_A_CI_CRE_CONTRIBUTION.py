#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CI_CRE_CONTRIBUTION').setMaster(sys.argv[2])
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

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT	CUST_ID                                         AS CUST_ID,
		CORE_CUST_NAME                                  AS CUST_NAME,
		ACCT_NO				                            AS ACCT_NO,
		MANAGE_BRAN                                     AS ORG_ID,
		CURR                                            AS CURR,
		COALESCE(MONTH_RMB,0)                           AS MONTH_AVG,
		COALESCE(MVAL_RMB,0)                            AS YEAR_AVG,
		PRODUCT_ID                                      AS CONT_SUB_ID,
		cast(COALESCE(MON_INTE_RATE,1)/100  as decimal(24,6)) AS MONTH_RATE,
		cast(COALESCE(INNER_PRICE,1)/12 as decimal(24,6)) AS INNER_PRICE ,
		RUN_COST                                       AS RUN_COST ,
		CASE WHEN (COALESCE(YEAR_AVG,0) * (cast(COALESCE(MON_INTE_RATE,1)/100  as decimal(24,6)) - cast(COALESCE(INNER_PRICE,1)/12 as decimal(24,6))) 
			- cast(COALESCE(RUN_COST,0)as decimal(24,6)))<0 THEN 0 
			ELSE (COALESCE(YEAR_AVG,0) * (cast(COALESCE(MON_INTE_RATE,1)/100  as decimal(24,6)) - cast(COALESCE(INNER_PRICE,1)/12 as decimal(24,6))) 
			- cast(COALESCE(RUN_COST,0)as decimal(24,6))) END  AS CONTRIBUTION,
		CASE WHEN (COALESCE(MVAL_RMB,0) * 
				  (cast(COALESCE(MON_INTE_RATE,1)/100  as decimal(24,6)) - cast(COALESCE(INNER_PRICE,1)/12 as decimal(24,6))) 
			- cast(COALESCE(RUN_COST,0)as decimal(24,6))) <0 THEN 0 ELSE  (COALESCE(MVAL_RMB,0) * 
				  (cast(COALESCE(MON_INTE_RATE,1)/100  as decimal(24,6)) - cast(COALESCE(INNER_PRICE,1)/12 as decimal(24,6))) 
			- cast(COALESCE(RUN_COST,0)as decimal(24,6))) END AS CONTRIBUTION_RMB,
		V_YEAR_MONTH 			AS  YEAR_MONTH,
		V_DT                    AS  ODS_DATE ,
		COALESCE(BAL_RMB ,0)    AS  BAL_RMB,
		AA.FR_ID 				AS  FR_ID,
		''						AS  FR_NAME,
		CUST_TYP				AS  CUST_TYP                                      
	FROM
		(SELECT CUST_ID                                       ,
			    CUST_TYP,
					CORE_CUST_NAME                                  ,
					ACCT_NO				                                  ,
					MANAGE_BRAN                                     ,
					CURR                                            ,
					BAL_RMB                                         ,
					MVAL_RMB                                        ,
					MONTH_RMB                                       ,
					MONTH_AVG                                       ,
					YEAR_AVG                                        ,
					PRODUCT_ID                                      ,
					MON_INTE_RATE		                                ,
					(CASE WHEN ((COALESCE(A.TERMYEAR,'0') * 12) + COALESCE(TERMMONTH,0)) < 3 THEN 'C001'--3个月以下贷款
								WHEN ((COALESCE(A.TERMYEAR,'0') * 12) + COALESCE(TERMMONTH,0)) < 6 THEN 'C002'--3个月贷款
								WHEN ((COALESCE(A.TERMYEAR,'0') * 12) + COALESCE(TERMMONTH,0)) < 9 THEN 'C003'--6个月贷款
								WHEN ((COALESCE(A.TERMYEAR,'0') * 12) + COALESCE(TERMMONTH,0)) < 12 THEN 'C004'--9个月贷款
								WHEN ((COALESCE(A.TERMYEAR,'0') * 12) + COALESCE(TERMMONTH,0)) < 24 THEN 'C005'--12个月贷款
								WHEN ((COALESCE(A.TERMYEAR,'0') * 12) + COALESCE(TERMMONTH,0)) < 36 THEN 'C006'--24个月贷款
								WHEN ((COALESCE(A.TERMYEAR,'0') * 12) + COALESCE(TERMMONTH,0)) < 60 THEN 'C007'--36个月贷款
								WHEN ((COALESCE(A.TERMYEAR,'0') * 12) + COALESCE(TERMMONTH,0)) = 60 THEN 'C008'--60个月贷款
								WHEN ((COALESCE(A.TERMYEAR,'0') * 12) + COALESCE(TERMMONTH,0)) > 60 THEN 'C009'--大于60个月贷款
					END) AS SUB_ID,
					FR_ID
		FROM	ACRM_F_CI_ASSET_BUSI_PROTO A
		WHERE 	A.BAL>0--账户状态正常
				AND A.LN_APCL_FLG = 'N'--非呆账核销
				AND (A.SUBJECTNO LIKE '1301%'
				   OR A.SUBJECTNO LIKE '1302%'
				   OR A.SUBJECTNO LIKE '1303%'
				   OR A.SUBJECTNO LIKE '1304%'
				   OR A.SUBJECTNO LIKE '1305%'
				   OR A.SUBJECTNO LIKE '1306%'
				   OR A.SUBJECTNO LIKE '1307%'
				   OR A.SUBJECTNO LIKE '1308%')
				--AND A.FR_ID = V_FR_ID 
		)AA
		LEFT JOIN OCRM_F_CI_CON_PARM B ON AA.SUB_ID = B.SUB_ID AND AA.FR_ID = B.ORG_ID
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_YEAR_MONTH\b", "'"+V_YEAR_MONTH+"'", sql)

ACRM_F_CI_CRE_CONTRIBUTION = sqlContext.sql(sql)
ACRM_F_CI_CRE_CONTRIBUTION.registerTempTable("ACRM_F_CI_CRE_CONTRIBUTION")
dfn="ACRM_F_CI_CRE_CONTRIBUTION/"+V_DT+".parquet"
ACRM_F_CI_CRE_CONTRIBUTION.cache()
nrows = ACRM_F_CI_CRE_CONTRIBUTION.count()
ACRM_F_CI_CRE_CONTRIBUTION.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CRE_CONTRIBUTION.unpersist()
ACRM_F_CI_ASSET_BUSI_PROTO.unpersist()
OCRM_F_CI_CON_PARM.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CRE_CONTRIBUTION/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CRE_CONTRIBUTION lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
