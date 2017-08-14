#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CI_CUST_GRADE').setMaster(sys.argv[2])
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

ACRM_A_D003_SCORE_DETAIL_PART = sqlContext.read.parquet(hdfs+'/ACRM_A_D003_SCORE_DETAIL_PART/*')
ACRM_A_D003_SCORE_DETAIL_PART.registerTempTable("ACRM_A_D003_SCORE_DETAIL_PART")
OCRM_F_CI_GRADE_LEVEL = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_GRADE_LEVEL/*')
OCRM_F_CI_GRADE_LEVEL.registerTempTable("OCRM_F_CI_GRADE_LEVEL")
OCRM_F_CI_GRADE_SCHEME = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_GRADE_SCHEME/*')
OCRM_F_CI_GRADE_SCHEME.registerTempTable("OCRM_F_CI_GRADE_SCHEME")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,SUM(SCORE)                       AS CUST_SCORE 
       ,CUST_TYP                AS CUST_TYPE 
       ,SUBSTR(V_DT, 1, 6)                       AS YEAR_MONTH 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_A_D003_SCORE_DETAIL_PART A                        --
  WHERE SCORE > 0 
  GROUP BY FR_ID 
       ,CUST_ID 
       ,CUST_TYP """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_CUST_SCORE = sqlContext.sql(sql)
ACRM_A_CUST_SCORE.registerTempTable("ACRM_A_CUST_SCORE")
dfn="ACRM_A_CUST_SCORE/"+V_DT+".parquet"
ACRM_A_CUST_SCORE.cache()
nrows = ACRM_A_CUST_SCORE.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CUST_SCORE/*.parquet")
ACRM_A_CUST_SCORE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_A_CUST_SCORE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CUST_SCORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

ACRM_A_CUST_SCORE = sqlContext.read.parquet(hdfs+'/ACRM_A_CUST_SCORE/*')
ACRM_A_CUST_SCORE.registerTempTable("ACRM_A_CUST_SCORE")
sql = """
 SELECT MONOTONICALLY_INCREASING_ID()                       AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,''                      AS ORG_ID 
       ,''                      AS CUST_NAME 
       ,A.CUST_SCORE            AS GRADE_SCORE 
       ,B.LEVEL_NAME            AS GRADE_LEVEL 
       ,''                      AS GRADE_HAND_LEVEL 
       ,''                      AS LAST_MODIFY_DATE 
       ,V_DT                    AS ETL_DT 
       ,B.SCHEME_ID             AS SCHEME_ID 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_A_CUST_SCORE A                                    --评级分数
   LEFT JOIN (SELECT N.ORG_SCOPE_ID AS FR_ID,M.SCHEME_ID,M.LEVEL_NAME,M.LEVEL_LOWER,M.LEVEL_UPPER,N.GRADE_TYPE
                FROM OCRM_F_CI_GRADE_LEVEL M
                JOIN OCRM_F_CI_GRADE_SCHEME N ON  M.SCHEME_ID = N.SCHEME_ID 
                WHERE N.IS_USED = 'Y' AND N.GRADE_USEAGE = '5') B                                              --
     ON A.CUST_TYPE             = B.GRADE_TYPE 
    AND A.CUST_SCORE >= B.LEVEL_LOWER 
    AND A.CUST_SCORE < B.LEVEL_UPPER 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_GRADE = sqlContext.sql(sql)
ACRM_F_CI_CUST_GRADE.registerTempTable("ACRM_F_CI_CUST_GRADE")
dfn="ACRM_F_CI_CUST_GRADE/"+V_DT+".parquet"
ACRM_F_CI_CUST_GRADE.cache()
nrows = ACRM_F_CI_CUST_GRADE.count()

#删除当天数据，支持重跑
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_GRADE/"+V_DT+".parquet")
ACRM_F_CI_CUST_GRADE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_GRADE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_GRADE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

