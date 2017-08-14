#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_MID_PER_ASSETS').setMaster(sys.argv[2])
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
TMP_PER_ASSETS_LEND = sqlContext.read.parquet(hdfs+'/TMP_PER_ASSETS_LEND/*')
TMP_PER_ASSETS_LEND.registerTempTable("TMP_PER_ASSETS_LEND")

TMP_PER_ASSETS_INSU = sqlContext.read.parquet(hdfs+'/TMP_PER_ASSETS_INSU/*')
TMP_PER_ASSETS_INSU.registerTempTable("TMP_PER_ASSETS_INSU")

TMP_PER_ASSETS_ACCS = sqlContext.read.parquet(hdfs+'/TMP_PER_ASSETS_ACCS/*')
TMP_PER_ASSETS_ACCS.registerTempTable("TMP_PER_ASSETS_ACCS")

TMP_PER_ASSETS_SAVE = sqlContext.read.parquet(hdfs+'/TMP_PER_ASSETS_SAVE/*')
TMP_PER_ASSETS_SAVE.registerTempTable("TMP_PER_ASSETS_SAVE")

#目标表
#TMP_PER_ASSETS_SUM 全量表

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT 
		CUST_ID
		,'0'                     		AS PRD_TYP
		,SUM(MONTH_BAL)                 AS MONTH_BAL
		,SUM(MONTH_AVG_BAL)             AS MONTH_AVG_BAL
		,SUM(THREE_MONTH_AVG_BAL)       AS THREE_MONTH_AVG_BAL
		,SUM(LAST_MONTH_BAL)            AS LAST_MONTH_BAL 
		,SUM(LAST_MONTH_AVG_BAL)        AS LAST_MONTH_AVG_BAL
		,SUM(LTHREE_MONTH_AVG_BAL)      AS LTHREE_MONTH_AVG_BAL
		,SUM(YEAR_BAL)                  AS YEAR_BAL
		,SUM(YEAR_AVG_BAL)              AS YEAR_AVG_BAL
		,SUM(YEAR_THREE_AVG_BAL)        AS YEAR_THREE_AVG_BAL 
		,FR_ID
	 FROM 
        (SELECT * FROM TMP_PER_ASSETS_LEND 
        	UNION ALL 
         SELECT * FROM TMP_PER_ASSETS_INSU
          UNION ALL
         SELECT * FROM TMP_PER_ASSETS_ACCS
          UNION ALL
         SELECT * FROM TMP_PER_ASSETS_SAVE
        )A
     GROUP BY CUST_ID,FR_ID 
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_PER_ASSETS_SUM = sqlContext.sql(sql)
TMP_PER_ASSETS_SUM.registerTempTable("TMP_PER_ASSETS_SUM")
dfn="TMP_PER_ASSETS_SUM/"+V_DT+".parquet"
TMP_PER_ASSETS_SUM.cache()
nrows = TMP_PER_ASSETS_SUM.count()
TMP_PER_ASSETS_SUM.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_PER_ASSETS_SUM.unpersist()
#全量表，保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_PER_ASSETS_SUM/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_PER_ASSETS_SUM lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
