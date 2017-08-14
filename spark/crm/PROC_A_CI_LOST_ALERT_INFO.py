#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CI_LOST_ALERT_INFO').setMaster(sys.argv[2])
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

OCRM_F_CI_GRADE_SCHEME = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_GRADE_SCHEME/*')
OCRM_F_CI_GRADE_SCHEME.registerTempTable("OCRM_F_CI_GRADE_SCHEME")

OCRM_F_CI_GRADE_LEVEL = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_GRADE_LEVEL/*')
OCRM_F_CI_GRADE_LEVEL.registerTempTable("OCRM_F_CI_GRADE_LEVEL")

ACRM_A_D004_SCORE_DETAIL = sqlContext.read.parquet(hdfs+'/ACRM_A_D004_SCORE_DETAIL/*')
ACRM_A_D004_SCORE_DETAIL.registerTempTable("ACRM_A_D004_SCORE_DETAIL")

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
#任务[11] 001-01::
V_STEP = V_STEP + 1
#-- 定义游标
#-- 取出计算公式计算最终得分
sql = """
	SELECT SCHEME_ID,      -- 方案ID
		 ORG_SCOPE_ID,   -- 法人号
		 GRADE_FORMULA,  -- 评级公式
		 GRADE_TYPE,     -- 客户类型 0 全部客户 1 对私客户 2 对公客户
		 GRADE_USEAGE    -- 评级方案类型
	FROM OCRM_F_CI_GRADE_SCHEME 
	WHERE IS_USED = '1' 
		AND GRADE_USEAGE = '4'
		--AND ORG_SCOPE_ID=V_FR_ID
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
CURSOR_DATA = sqlContext.sql(sql).collect()  
#删除当天日期文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_LOST_ALERT_INFO/"+V_DT+".parquet")
for row in CURSOR_DATA:
	 V_SCHEME_ID = row.SCHEME_ID
	 V_FR_ID = row.ORG_SCOPE_ID
	 V_GRADE_FORMULA = row.GRADE_FORMULA
	 V_CUST_TYPE = row.GRADE_TYPE
	 V_GRADE_USEAGE = row.GRADE_USEAGE
	 if V_CUST_TYPE == '0':
		sql = """
		SELECT  monotonically_increasing_id() as ID,
			 A.CUST_ID,
			 A.ORG_ID,
			 A.ALERT_SCORE,
			 B.LEVEL_NAME  as ALERT_LEVEL,
			 V_DT as ETL_DATE,
			 cast(A.SCHEME_ID as BIGINT) as SCHEME_ID,
			 A.CUST_NAME, 
			 A.ORG_ID as FR_ID					 
		FROM
		(SELECT V_SCHEME_ID AS SCHEME_ID,
				A.CUST_ID,
				sum(A.score) AS ALERT_SCORE,
				V_DT AS ETL_DATE,
				A.FR_ID AS ORG_ID,
				D.CUST_ZH_NAME AS CUST_NAME															
			FROM  ACRM_A_D004_SCORE_DETAIL  A 
			LEFT JOIN OCRM_F_CI_CUST_DESC D ON A.CUST_ID = D.CUST_ID AND A.FR_ID = D.FR_ID
			WHERE  A.ETL_DATE = V_DT
			GROUP BY A.CUST_ID,A.FR_ID,D.CUST_ZH_NAME
		)A LEFT JOIN OCRM_F_CI_GRADE_LEVEL B
				ON A.SCHEME_ID = B.SCHEME_ID 
				AND COALESCE(B.LEVEL_LOWER,0) <= A.ALERT_SCORE 
				AND COALESCE(B.LEVEL_UPPER,999999999999999999) > A.ALERT_SCORE
		"""
	 else:
		sql = """
		SELECT 
		 monotonically_increasing_id() as ID,
		 A.CUST_ID,
		 A.ORG_ID,
		 A.ALERT_SCORE,
		 B.LEVEL_NAME  as ALERT_LEVEL,
		 V_DT as ETL_DATE,
		 cast(A.SCHEME_ID as BIGINT) as SCHEME_ID,
		 A.CUST_NAME, 
		 A.ORG_ID as FR_ID	
		 
		FROM
		(SELECT V_SCHEME_ID AS SCHEME_ID
				,A.CUST_ID,
				,sum(A.score) AS ALERT_SCORE
				V_DT AS ETL_DATE
				,A.FR_ID AS ORG_ID
				,D.CUST_ZH_NAME AS CUST_NAME																
		 FROM  ACRM_A_D004_SCORE_DETAIL  A 
		 LEFT JOIN OCRM_F_CI_CUST_DESC D ON A.CUST_ID = D.CUST_ID AND D.FR_ID = A.FR_ID
		 WHERE 
			 A.CUST_TYP = V_CUST_TYPE
			 AND A.FR_ID = V_FR_ID 
			 AND  A.ETL_DATE = V_DT
		 GROUP BY A.CUST_ID,A.FR_ID,D.CUST_ZH_NAME
		)A LEFT JOIN OCRM_F_CI_GRADE_LEVEL B
					ON A.SCHEME_ID = B.SCHEME_ID 
						AND COALESCE(B.LEVEL_LOWER,0) <= A.ALERT_SCORE 
						AND COALESCE(B.LEVEL_UPPER,999999999999999999) > A.ALERT_SCORE'
		"""
	 sql = re.sub(r"\bV_DT\b", "'"+V_DT+"'", sql)
	 sql = re.sub(r"\bV_SCHEME_ID\b", V_SCHEME_ID, sql)
	 sql = re.sub(r"\bV_FR_ID\b", "'"+V_FR_ID+"'", sql)
	 sql = re.sub(r"\bV_GRADE_FORMULA\b", "'"+V_GRADE_FORMULA+"'", sql)
	 sql = re.sub(r"\bV_CUST_TYPE\b", "'"+V_CUST_TYPE+"'", sql)
	 sql = re.sub(r"\bV_GRADE_USEAGE\b", "'"+V_GRADE_USEAGE+"'", sql)
	 INNTMP_TMP = sqlContext.sql(sql)
	 #生成目标文件	
	 dfn= "ACRM_F_CI_LOST_ALERT_INFO/"+V_DT+".parquet"
	 INNTMP_TMP.write.save(path = hdfs + '/' + dfn, mode='append')
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)