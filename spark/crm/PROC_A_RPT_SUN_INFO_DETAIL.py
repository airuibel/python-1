#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_RPT_SUN_INFO_DETAIL').setMaster(sys.argv[2])
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
OCRM_F_CI_SUN_BASEINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_BASEINFO/*')
OCRM_F_CI_SUN_BASEINFO.registerTempTable("OCRM_F_CI_SUN_BASEINFO")

OCRM_F_CI_SUN_BASE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_BASE/*')
OCRM_F_CI_SUN_BASE.registerTempTable("OCRM_F_CI_SUN_BASE")

OCRM_F_CUST_ELECTRICITY_COST = sqlContext.read.parquet(hdfs+'/OCRM_F_CUST_ELECTRICITY_COST/*')
OCRM_F_CUST_ELECTRICITY_COST.registerTempTable("OCRM_F_CUST_ELECTRICITY_COST")

OCRM_F_CUST_WATER_COST = sqlContext.read.parquet(hdfs+'/OCRM_F_CUST_WATER_COST/*')
OCRM_F_CUST_WATER_COST.registerTempTable("OCRM_F_CUST_WATER_COST")

OCRM_F_CI_CUSTOMER_TYPE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUSTOMER_TYPE/*')
OCRM_F_CI_CUSTOMER_TYPE.registerTempTable("OCRM_F_CI_CUSTOMER_TYPE")

OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")

#目标表：
#RPT_SUN_INFO_DETAIL 增改表 多个PY 属于第一个
#处理过程：先删除运算目录所有数据文件，然后复制BK目录前一天数据到运算目录，作为今天数据文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/RPT_SUN_INFO_DETAIL/*.parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/RPT_SUN_INFO_DETAIL_BK/"+V_DT_LD+".parquet /"+dbname+"/RPT_SUN_INFO_DETAIL/"+V_DT+".parquet")
RPT_SUN_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/RPT_SUN_INFO_DETAIL/*')
RPT_SUN_INFO_DETAIL.registerTempTable("RPT_SUN_INFO_DETAIL")



#任务[21] 001-01::
V_STEP = V_STEP + 1

#MERGE操作
sql="""
		SELECT CASE WHEN T.VILLAGE IS NULL THEN concat(T.TOWN,T.GROUPID)
				    WHEN T.TOWN IS NULL    THEN concat(T.COUNTY,T.GROUPID) 
				    ELSE concat(T.VILLAGE,T.GROUPID)  END AS ADDR,
			 T.CUST_ID,
			 CASE WHEN T.ISHOUSEHOST = '1' THEN T.CUST_NAME ELSE T7.CUST_NAME END AS CUST_NAME,
			 T1.FAM_TOTASSET,
			 T1.FAM_DEBT ,
			 T2.SUM_PRICE AS ELEC,
			 T3.SUM_PRICE AS WATER,
			 T.FR_ID,
			 CONCAT(T4.VALUE,'县',T5.VALUE,'镇',T6.VALUE,'村',T.GROUPID,'组') ADDR_DESC
	  FROM OCRM_F_CI_SUN_BASEINFO T
	  LEFT JOIN OCRM_F_CI_SUN_BASE T1 ON T.CUST_ID = T1.CUST_ID AND T1.FR_ID = T.FR_ID
	  LEFT JOIN OCRM_F_CUST_ELECTRICITY_COST T2 ON T1.CUST_ID = T2.CUST_ID AND T2.FR_ID = T1.FR_ID
	  LEFT JOIN OCRM_F_CUST_WATER_COST T3 ON T1.CUST_ID = T3.CUST_ID AND T3.FR_ID = T1.FR_ID
	  LEFT JOIN OCRM_F_CI_CUSTOMER_TYPE T4 ON T.COUNTY = T4.ID AND T4.FR_ID = T.FR_ID
	  LEFT JOIN OCRM_F_CI_CUSTOMER_TYPE T5 ON T.TOWN = T5.ID AND T5.FR_ID = T.FR_ID
	  LEFT JOIN OCRM_F_CI_CUSTOMER_TYPE T6 ON T.VILLAGE = T6.ID AND T6.FR_ID = T.FR_ID
	  LEFT JOIN OCRM_F_CI_SUN_BASEINFO T7 ON T7.FR_ID = T.FR_ID AND T.COUNTY = T7.COUNTY 
			AND T.VILLAGE = T7.VILLAGE AND T.TOWN = T7.TOWN AND T.GROUPID = T7.GROUPID AND T.DOORID = T7.DOORID AND T7.ISHOUSEHOST = '1'
	  --WHERE T.FR_ID = V_FR_ID
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_RPT_SUN_INFO_DETAIL_01 = sqlContext.sql(sql)
TMP_RPT_SUN_INFO_DETAIL_01.registerTempTable("TMP_RPT_SUN_INFO_DETAIL_01")

sql="""
	select 
		 src.ADDR						--VARCHAR
		,src.CUST_ID					--VARCHAR
		,src.CUST_NAME					--VARCHAR
		,src.FAM_TOTASSET				--DECIMAL(22,2)
		,src.FAM_DEBT					--DECIMAL(22,2)
		,dst.PRECREDIT					--DECIMAL(22,2)
		,dst.SIGNCREDIT					--DECIMAL(22,2)
		,dst.USECREDIT					--DECIMAL(22,2)
		,dst.FAM_CRE					--DECIMAL(22,2)
		,src.ELEC						--DECIMAL(22,2)
		,src.WATER						--DECIMAL(22,2)
		,src.FR_ID						--VARCHAR
		,src.ADDR_DESC					--VARCHAR
	from TMP_RPT_SUN_INFO_DETAIL_01 src 
	left join RPT_SUN_INFO_DETAIL dst on  src.CUST_ID=dst.CUST_ID and src.FR_ID=dst.FR_ID
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_RPT_SUN_INFO_DETAIL_INNER1 = sqlContext.sql(sql)
TMP_RPT_SUN_INFO_DETAIL_INNER1.registerTempTable("TMP_RPT_SUN_INFO_DETAIL_INNER1")

sql="""
	select 
		 src.ADDR						--VARCHAR
		,src.CUST_ID					--VARCHAR
		,src.CUST_NAME					--VARCHAR
		,src.FAM_TOTASSET				--DECIMAL(22,2)
		,src.FAM_DEBT					--DECIMAL(22,2)
		,dst.PRECREDIT					--DECIMAL(22,2)
		,dst.SIGNCREDIT					--DECIMAL(22,2)
		,dst.USECREDIT					--DECIMAL(22,2)
		,dst.FAM_CRE					--DECIMAL(22,2)
		,src.ELEC						--DECIMAL(22,2)
		,src.WATER						--DECIMAL(22,2)
		,src.FR_ID						--VARCHAR
		,src.ADDR_DESC					--VARCHAR
	from RPT_SUN_INFO_DETAIL src 
	left join TMP_RPT_SUN_INFO_DETAIL_INNER1 dst on  src.CUST_ID=dst.CUST_ID and src.FR_ID=dst.FR_ID
	where dst.CUST_ID is null
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_RPT_SUN_INFO_DETAIL_INNER2 = sqlContext.sql(sql)
TMP_RPT_SUN_INFO_DETAIL_INNER2.registerTempTable("TMP_RPT_SUN_INFO_DETAIL_INNER2")
RPT_SUN_INFO_DETAIL_UNION = TMP_RPT_SUN_INFO_DETAIL_INNER2.unionAll(TMP_RPT_SUN_INFO_DETAIL_INNER1)
dfn="RPT_SUN_INFO_DETAIL/"+V_DT+".parquet"
RPT_SUN_INFO_DETAIL_UNION.cache()
nrows = RPT_SUN_INFO_DETAIL_UNION.count()
RPT_SUN_INFO_DETAIL_UNION.write.save(path=hdfs + '/' + dfn, mode='overwrite')
RPT_SUN_INFO_DETAIL_UNION.unpersist()
#复制当天数据进BK
ret = os.system("hdfs dfs -rm -r /"+dbname+"/RPT_SUN_INFO_DETAIL_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/RPT_SUN_INFO_DETAIL/"+V_DT+".parquet /"+dbname+"/RPT_SUN_INFO_DETAIL_BK/"+V_DT+".parquet")

et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

#任务[21] 001-01::
V_STEP = V_STEP + 1
RPT_SUN_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/RPT_SUN_INFO_DETAIL/*')
RPT_SUN_INFO_DETAIL.registerTempTable("RPT_SUN_INFO_DETAIL")

#更新户主姓名 update操作
sql="""
	select 
		 dst.ADDR						--VARCHAR
		,dst.CUST_ID					--VARCHAR
		,COALESCE(dst.CUST_NAME,src.HZ_NAME) AS CUST_NAME					--VARCHAR
		,dst.FAM_TOTASSET				--DECIMAL(22,2)
		,dst.FAM_DEBT					--DECIMAL(22,2)
		,dst.PRECREDIT					--DECIMAL(22,2)
		,dst.SIGNCREDIT					--DECIMAL(22,2)
		,dst.USECREDIT					--DECIMAL(22,2)
		,dst.FAM_CRE					--DECIMAL(22,2)
		,dst.ELEC						--DECIMAL(22,2)
		,dst.WATER						--DECIMAL(22,2)
		,dst.FR_ID						--VARCHAR
		,dst.ADDR_DESC					--VARCHAR
	from RPT_SUN_INFO_DETAIL dst
	left join OCRM_F_CI_PER_CUST_INFO src on dst.CUST_ID=src.CUST_ID and dst.FR_ID=src.FR_ID
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_RPT_SUN_INFO_DETAIL_02 = sqlContext.sql(sql)
dfn="RPT_SUN_INFO_DETAIL/"+V_DT+".parquet"
TMP_RPT_SUN_INFO_DETAIL_02.cache()
nrows = TMP_RPT_SUN_INFO_DETAIL_02.count()
TMP_RPT_SUN_INFO_DETAIL_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_RPT_SUN_INFO_DETAIL_02.unpersist()
#复制当天数据进BK
ret = os.system("hdfs dfs -rm -r /"+dbname+"/RPT_SUN_INFO_DETAIL_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/RPT_SUN_INFO_DETAIL/"+V_DT+".parquet /"+dbname+"/RPT_SUN_INFO_DETAIL_BK/"+V_DT+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)


#任务[21] 001-01::
V_STEP = V_STEP + 1
RPT_SUN_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/RPT_SUN_INFO_DETAIL/*')
RPT_SUN_INFO_DETAIL.registerTempTable("RPT_SUN_INFO_DETAIL")

#MERGE操作,更新：授信金额（PRECREDIT）、可用信：USECREDIT
sql="""
	SELECT A.FR_ID,A.CUST_ID,
		cast(SUM(CASE WHEN A.CONT_STS = '1000' AND A.PRODUCT_ID LIKE '3%' THEN COALESCE(A.CONT_AMT,0) END) as DECIMAL(22,2)) as CONT_AMT ,
		cast(SUM(COALESCE(BAL,0)) as DECIMAL(22,2)) AS BAL 
	FROM ACRM_F_CI_ASSET_BUSI_PROTO A
	JOIN RPT_SUN_INFO_DETAIL B ON B.FR_ID = A.FR_ID AND A.CUST_ID = B.CUST_ID 
	GROUP BY A.FR_ID,A.CUST_ID
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_RPT_SUN_INFO_DETAIL_03 = sqlContext.sql(sql)
TMP_RPT_SUN_INFO_DETAIL_03.registerTempTable("TMP_RPT_SUN_INFO_DETAIL_03")

sql="""
	select 
		 src.ADDR						--VARCHAR
		,src.CUST_ID					--VARCHAR
		,src.CUST_NAME					--VARCHAR
		,src.FAM_TOTASSET				--DECIMAL(22,2)
		,src.FAM_DEBT					--DECIMAL(22,2)
		,cast(CASE WHEN dst.CUST_ID is null then src.PRECREDIT else dst.CONT_AMT end as DECIMAL(22,2)) as PRECREDIT  --DECIMAL(22,2)
		,src.SIGNCREDIT					--DECIMAL(22,2)
		,cast(CASE WHEN dst.CUST_ID is null then src.USECREDIT else (dst.CONT_AMT - dst.BAL) end as DECIMAL(22,2))  as USECREDIT --DECIMAL(22,2)
		,src.FAM_CRE					--DECIMAL(22,2)
		,src.ELEC						--DECIMAL(22,2)
		,src.WATER						--DECIMAL(22,2)
		,src.FR_ID						--VARCHAR
		,src.ADDR_DESC					--VARCHAR
	from RPT_SUN_INFO_DETAIL src 
	left join TMP_RPT_SUN_INFO_DETAIL_03 dst on  src.CUST_ID=dst.CUST_ID and src.FR_ID=dst.FR_ID
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_RPT_SUN_INFO_DETAIL_04 = sqlContext.sql(sql)
dfn="RPT_SUN_INFO_DETAIL/"+V_DT+".parquet"
TMP_RPT_SUN_INFO_DETAIL_04.cache()
nrows = TMP_RPT_SUN_INFO_DETAIL_04.count()
TMP_RPT_SUN_INFO_DETAIL_04.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_RPT_SUN_INFO_DETAIL_04.unpersist()
#复制当天数据进BK
ret = os.system("hdfs dfs -rm -r /"+dbname+"/RPT_SUN_INFO_DETAIL_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/RPT_SUN_INFO_DETAIL/"+V_DT+".parquet /"+dbname+"/RPT_SUN_INFO_DETAIL_BK/"+V_DT+".parquet")

et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

