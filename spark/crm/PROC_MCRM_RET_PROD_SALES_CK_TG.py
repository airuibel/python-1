#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os , calendar  

st = datetime.now()
conf = SparkConf().setAppName('PROC_MCRM_RET_PROD_SALES_CK_TG').setMaster(sys.argv[2])
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
print(V_DT10)

#本月月末日期（10位）
monthRange = calendar.monthrange(int(etl_date[0:3]),int(etl_date[4:6]))#得到本月的天数   
V_LAST_DAY = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(str(monthRange[1])))).strftime("%Y-%m-%d") 
print(V_LAST_DAY)
#V_CURR_MONTH_LD = (date(int(etl_date[0:4]), int(etl_date[4:6])+1, 1) + timedelta(-1)).strftime("%Y%m%d");

V_STEP = 0

#MCRM_RET_PROD_SALES_CK_TG = sqlContext.read.parquet(hdfs+'/MCRM_RET_PROD_SALES_CK_TG/*')
#MCRM_RET_PROD_SALES_CK_TG.registerTempTable("MCRM_RET_PROD_SALES_CK_TG")

ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
MCRM_RET_PROD_SALES = sqlContext.read.parquet(hdfs+'/MCRM_RET_PROD_SALES/*')
MCRM_RET_PROD_SALES.registerTempTable("MCRM_RET_PROD_SALES")
#TEMP_MCRM_RET_PROD_SALES_CK_TG = sqlContext.read.parquet(hdfs+'/TEMP_MCRM_RET_PROD_SALES_CK_TG/*')
#TEMP_MCRM_RET_PROD_SALES_CK_TG.registerTempTable("TEMP_MCRM_RET_PROD_SALES_CK_TG")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT  cast(sum(A.CNY_BAL) AS decimal(24,6)) AS BAL,
         cast(sum(A.LAST_MONTH_CNY_BAL) AS decimal(24,6)) AS LAST_BAL,
         cast(CASE WHEN sum (A.CNY_BAL - A.LAST_MONTH_CNY_BAL) < 0 THEN 0
              ELSE sum (A.CNY_BAL - A.LAST_MONTH_CNY_BAL) END as decimal(24,6))  AS ADD_CUST_BAL,
         cast(CASE
                    WHEN SUM (A.LAST_MONTH_CNY_BAL - A.CNY_BAL) < 0 THEN 0
                    ELSE SUM (A.LAST_MONTH_CNY_BAL - A.CNY_BAL)
                 END as decimal(24,6))  AS LOSE_CUST_BAL,
                A.PROD_NAME,
                A.ORG_ID,
                A.CUST_LEVEL,
                A.ST_DATE
   FROM MCRM_RET_PROD_SALES A                                  
   INNER JOIN ADMIN_AUTH_ORG B                                  
     ON A.ORG_ID                = B.ORG_ID 
  WHERE A.O_MAIN_TYPE           = '1' 
    AND A.SIGN                  = 'CK' 
  GROUP BY A.ORG_ID 
       ,A.PROD_NAME 
       ,A.CUST_LEVEL 
       ,A.ST_DATE
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_PROD_SALES_CK_TG = sqlContext.sql(sql)
MCRM_RET_PROD_SALES_CK_TG.registerTempTable("MCRM_RET_PROD_SALES_CK_TG")
dfn="MCRM_RET_PROD_SALES_CK_TG/"+V_DT+".parquet"
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_PROD_SALES_CK_TG/"+V_DT+".parquet")
MCRM_RET_PROD_SALES_CK_TG.write.save(path = hdfs + '/' + dfn, mode='append')

V_STEP = V_STEP + 1
if V_DT10 == V_LAST_DAY:
	#更新 LAST_BAL = BAL 建TEMP_MCRM_RET_PROD_SALES_CK_TG 
	sql = """
	 SELECT ST_DATE                 AS ST_DATE 
		   ,CUST_LEVEL              AS CUST_LEVEL 
		   ,ORG_ID                  AS ORG_ID 
		   ,PROD_NAME               AS PROD_NAME 
		   ,LOSE_CUST_BAL           AS LOSE_CUST_BAL 
		   ,ADD_CUST_BAL            AS ADD_CUST_BAL 
		   ,BAL                		AS LAST_BAL 
		   ,BAL                     AS BAL 
	   FROM MCRM_RET_PROD_SALES_CK_TG WHERE ST_DATE=V_LAST_DAY
	"""
 
	sql = re.sub(r"\V_LAST_DAY\b", "'"+V_LAST_DAY+"'", sql)
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_MCRM_RET_PROD_SALES_CK_TG/*.parquet")
	TEMP_MCRM_RET_PROD_SALES_CK_TG = sqlContext.sql(sql)
	TEMP_MCRM_RET_PROD_SALES_CK_TG.registerTempTable("TEMP_MCRM_RET_PROD_SALES_CK_TG")
	dfn="TEMP_MCRM_RET_PROD_SALES_CK_TG/"+V_DT+".parquet"
	TEMP_MCRM_RET_PROD_SALES_CK_TG.write.save(path = hdfs + '/' + dfn, mode='overwrite')
	TEMP_MCRM_RET_PROD_SALES_CK_TG.unpersist()
	MCRM_RET_PROD_SALES_CK_TG.unpersist()
else:
	TEMP_MCRM_RET_PROD_SALES_CK_TG = sqlContext.read.parquet(hdfs+'/TEMP_MCRM_RET_PROD_SALES_CK_TG/*')
	TEMP_MCRM_RET_PROD_SALES_CK_TG.registerTempTable("TEMP_MCRM_RET_PROD_SALES_CK_TG")
	sql = """
	 SELECT B.ST_DATE               AS ST_DATE 
		   ,B.CUST_LEVEL            AS CUST_LEVEL 
		   ,B.ORG_ID                AS ORG_ID 
		   ,B.PROD_NAME               AS PROD_NAME 
		   ,cast(case  when B.LAST_BAL> A.BAL then  B.LAST_BAL - A.BAL else 0 end  as decimal(24,6))  AS LOSE_CUST_BAL 
		   ,cast(case when A.BAL> B.LAST_BAL then A.BAL - B.LAST_BAL else 0 end as decimal(24,6) )    AS ADD_CUST_BAL 
		   ,B.LAST_BAL              AS LAST_BAL
		   ,B.BAL                   AS BAL
	   FROM  MCRM_RET_PROD_SALES_CK_TG  B                       
	  LEFT JOIN TEMP_MCRM_RET_PROD_SALES_CK_TG A                     
		 ON A.PROD_NAME             = B.PROD_NAME 
		AND A.ORG_ID                = B.ORG_ID 
		AND A.CUST_LEVEL            = B.CUST_LEVEL 
	  WHERE A.ST_DATE               = V_DT 
	  """
	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	
	MCRM_RET_PROD_SALES_CK_TG_INNTMP1 = sqlContext.sql(sql)
	MCRM_RET_PROD_SALES_CK_TG_INNTMP1.registerTempTable("MCRM_RET_PROD_SALES_CK_TG_INNTMP1")
	dfn="MCRM_RET_PROD_SALES_CK_TG/"+V_DT+".parquet"

	ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_PROD_SALES_CK_TG/"+V_DT+".parquet")
	MCRM_RET_PROD_SALES_CK_TG_INNTMP1.write.save(path = hdfs + '/' + dfn, mode='append')
	MCRM_RET_PROD_SALES_CK_TG_INNTMP1.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
#ret = os.system("hdfs dfs -mv /"+dbname+"/MCRM_RET_PROD_SALES_CK_TG/"+V_DT_LD+".parquet /"+dbname+"/MCRM_RET_PROD_SALES_CK_TG_BK/")

