#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_EXEC_SUBJECT').setMaster(sys.argv[2])
sc = SparkContext(conf = conf)
sc.setLogLevel('WARN')
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

#----------------------------------------------自定义参数----------------------------------------------------------
V_I_SUB_ID = sys.argv[5]  #这个参数需要传
V_TAR_SUB_ID = V_I_SUB_ID[0:4]
V_YEAR_MONTH = V_DT[0:4] + "-" + V_DT[4:6]

V_SCORE_DETAIL_TNAME = 'ACRM_A_' + V_TAR_SUB_ID + '_SCORE_DETAIL_PART'
V_TARGET_TNAME = 'ACRM_A_TARGET_' + V_I_SUB_ID

print("---------------------------------------------------------------")
print(V_SCORE_DETAIL_TNAME)
print(V_TARGET_TNAME)
print("---------------------------------------------------------------")

ACRM_A_TARGET_I_SUB_ID = sqlContext.read.parquet(hdfs+'/'+V_TARGET_TNAME+'/*')
ACRM_A_TARGET_I_SUB_ID.registerTempTable("ACRM_A_TARGET_I_SUB_ID")

OCRM_F_SYS_RULE_SCORE_SET = sqlContext.read.parquet(hdfs+'/OCRM_F_SYS_RULE_SCORE_SET/*')
OCRM_F_SYS_RULE_SCORE_SET.registerTempTable("OCRM_F_SYS_RULE_SCORE_SET")

OCRM_F_SYS_RULE = sqlContext.read.parquet(hdfs+'/OCRM_F_SYS_RULE/*')
OCRM_F_SYS_RULE.registerTempTable("OCRM_F_SYS_RULE")

OCRM_F_CI_GRADE_FORMULA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_GRADE_FORMULA/*')
OCRM_F_CI_GRADE_FORMULA.registerTempTable("OCRM_F_CI_GRADE_FORMULA")

## 计算指标得分（直接折算）
sql = """

		SELECT	    		
		        A.CUST_ID     AS CUST_ID,
				''			  AS CUST_NAME,
				''			  AS ORG_ID,
				''			  AS ORG_NAME,
				A.INDEX_CODE  AS INDEX_CODE ,
				CAST(B.ID AS BIGINT)  AS RS_ID     ,
				CAST(COALESCE(A.INDEX_VALUE,0) * COALESCE(CONVERT_RATE,1)*D.WEIGHT*INT(concat(D.SIGN,'1')) as DECIMAL(22,2))  AS SCORE,
				V_YEAR_MONTH  AS YEAR_MONTH,
				V_DT          AS ETL_DATE,
			    A.CUST_TYPE   AS CUST_TYP ,																		
				A.FR_ID       AS FR_ID
		FROM ACRM_A_TARGET_I_SUB_ID A
		     LEFT JOIN OCRM_F_SYS_RULE_SCORE_SET B ON  A.INDEX_CODE = B.INDEX_CODE AND (A.CUST_TYPE = B.CUST_TYPE or B.CUST_TYPE = '0') 
			 LEFT JOIN OCRM_F_SYS_RULE C ON B.ID = C.RS_ID
			 INNER JOIN OCRM_F_CI_GRADE_FORMULA D ON A.INDEX_CODE = D.INDEX_CODE AND (A.CUST_TYPE=D.CUST_TYPE OR D.CUST_TYPE='0') AND A.FR_ID=D.FR_ID
		WHERE D.IS_USER='Y' AND COMPUTE_TYPE = '1' AND STATUS = 'Y'
    """
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_YEAR_MONTH\b", "'"+V_YEAR_MONTH+"'", sql)
print(sql)
INNTMP1 = sqlContext.sql(sql)
INNTMP1.registerTempTable("INNTMP1")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

V_STEP = V_STEP + 1
##-- 计算指标得分（按规则折算），遍历规则折算指标
sql = """
  SELECT 	 A.ID,           -- 折算规则ID
			 A.INDEX_CODE,   -- 指标号
			 B.RULE_EXPRESS, -- 规则公式
			 B.RULE_RESULT,  -- 结果值
			 B.OPERATE ,      -- 计算标识（1固定分值、2折算率）
			 A.CUST_TYPE
	FROM OCRM_F_SYS_RULE_SCORE_SET A
	LEFT JOIN OCRM_F_SYS_RULE B ON A.ID = B.RS_ID
		WHERE COMPUTE_TYPE = '2' 
		  AND STATUS = 'Y'
		  AND A.INDEX_CODE = V_I_SUB_ID 
 """
sql = re.sub(r"\bV_I_SUB_ID\b", "'"+V_I_SUB_ID+"'", sql)
INDEX_TMP = sqlContext.sql(sql)
CURSOR_DATA = INDEX_TMP.collect()  
for row in CURSOR_DATA:
	V_ID = row.ID
	V_INDEX_CODE = row.INDEX_CODE
	V_RULE_EXPRESS = row.RULE_EXPRESS
	V_RULE_RESULT = row.RULE_RESULT
	V_OPERATE = row.OPERATE
	V_CUST_TYPE = row.CUST_TYPE
	if V_CUST_TYPE == '0':
		sql = """
			SELECT      CUST_ID       AS CUST_ID,
						''			  AS CUST_NAME,
						''			  AS ORG_ID,
						''			  AS ORG_NAME,
						A.INDEX_CODE     as INDEX_CODE,
						B.ID             as RS_ID,
						CASE WHEN V_OPERATE = '1'
							 THEN V_RULE_RESULT * C.WEIGHT * INT(concat(C.SIGN,'1'))
							 WHEN V_OPERATE = '2' 
							 THEN V_RULE_RESULT * COALESCE(A.INDEX_VALUE,0) * C.WEIGHT * INT(concat(C.SIGN,'1'))   END as SCORE,
						V_YEAR_MONTH as YEAR_MONTH,
						V_DT  as ETL_DATE,	 					
						A.CUST_TYPE      as CUST_TYP,
						A.FR_ID          as FR_ID		
			FROM ACRM_A_TARGET_I_SUB_ID A  
			INNER JOIN OCRM_F_SYS_RULE_SCORE_SET B ON A.INDEX_CODE = B.INDEX_CODE AND COMPUTE_TYPE = '2' AND STATUS = 'Y' AND V_RULE_EXPRESS
			INNER JOIN OCRM_F_CI_GRADE_FORMULA C ON A.INDEX_CODE = C.INDEX_CODE  AND (A.CUST_TYPE = C.CUST_TYPE OR C.CUST_TYPE = '0') AND A.FR_ID=C.FR_ID AND C.IS_USER='Y'
		"""
	else :
		sql = """
			SELECT		CUST_ID       AS CUST_ID,
						''			  AS CUST_NAME,
						''			  AS ORG_ID,
						''			  AS ORG_NAME,
						A.INDEX_CODE     as INDEX_CODE,
						B.ID             as RS_ID,
						CASE WHEN V_OPERATE = '1'
						 THEN V_RULE_RESULT * C.WEIGHT * INT(concat(C.SIGN,'1'))
						 WHEN V_OPERATE = '2'
						 THEN V_RULE_RESULT * COALESCE(A.INDEX_VALUE,0) * C.WEIGHT * INT(concat(C.SIGN,'1'))  END as SCORE, 
						V_YEAR_MONTH as YEAR_MONTH,
						V_DT  as ETL_DATE,	 					
						A.CUST_TYPE      as CUST_TYP,
						A.FR_ID          as FR_ID
			FROM ACRM_A_TARGET_I_SUB_ID A  
			INNER JOIN OCRM_F_SYS_RULE_SCORE_SET B ON A.INDEX_CODE = B.INDEX_CODE AND COMPUTE_TYPE = '2' AND A.CUST_TYPE = B.CUST_TYPE AND STATUS = 'Y'
					   AND V_RULE_EXPRESS  AND A.CUST_TYPE = V_CUST_TYPE  
			INNER JOIN OCRM_F_CI_GRADE_FORMULA C ON A.INDEX_CODE = C.INDEX_CODE  AND C.CUST_TYPE = V_CUST_TYPE  AND A.FR_ID = C.FR_ID AND C.IS_USER = 'Y'
		"""
	sql = re.sub(r"\bV_DT\b", "'"+V_DT+"'", sql)
	sql = re.sub(r"\bV_YEAR_MONTH\b", "'"+V_YEAR_MONTH+"'", sql)
	sql = re.sub(r"\bV_RULE_EXPRESS\b", "'"+V_RULE_EXPRESS+"'", sql)
	sql = re.sub(r"\bV_RULE_RESULT\b", "'"+V_RULE_RESULT+"'", sql)
	sql = re.sub(r"\bV_OPERATE\b", "'"+V_OPERATE+"'", sql)
	sql = re.sub(r"\bV_CUST_TYPE\b", "'"+V_CUST_TYPE+"'", sql)

	INNTMP_TMP = sqlContext.sql(sql)
	INNTMP_TMP.registerTempTable("INNTMP_TMP")
	#数据合并
	INNTMP1 = INNTMP1.unionAll(INNTMP_TMP)	
#生成目标文件	
dfn= V_SCORE_DETAIL_TNAME+"/"+V_DT+".parquet"
INNTMP1.write.save(path = hdfs + '/' + dfn, mode='append')
INNTMP1.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
#移除前一天文件到备份目录
#ret = os.system("hdfs dfs -mv /"+dbname+"/"+V_SCORE_DETAIL_TNAME+"/"+V_DT_LD+".parquet /"+dbname+"/"+V_SCORE_DETAIL_TNAME+"_BK/")
