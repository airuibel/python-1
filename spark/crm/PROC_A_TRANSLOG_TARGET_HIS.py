#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_TRANSLOG_TARGET_HIS').setMaster(sys.argv[2])
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
ACRM_A_TRANSLOG_TARGET = sqlContext.read.parquet(hdfs+'/ACRM_A_TRANSLOG_TARGET/*')
ACRM_A_TRANSLOG_TARGET.registerTempTable("ACRM_A_TRANSLOG_TARGET")
#目标表：
#ACRM_A_TRANSLOG_TARGET_HIS 增改表 多个PY 第一个 ,拿BK目录前一天日期数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TRANSLOG_TARGET_HIS/*")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_A_TRANSLOG_TARGET_HIS_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_A_TRANSLOG_TARGET_HIS/"+V_DT_LD+".parquet")
ACRM_A_TRANSLOG_TARGET_HIS = sqlContext.read.parquet(hdfs+'/ACRM_A_TRANSLOG_TARGET_HIS/*')
ACRM_A_TRANSLOG_TARGET_HIS.registerTempTable("ACRM_A_TRANSLOG_TARGET_HIS")

#任务[12] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
       ,A.CHANNEL_FLAG          AS CHANNEL_FLAG 
       ,CAST(A.INDEX_VALUE_C + COALESCE(B.INDEX_VALUE_C, 0) AS DECIMAL(22,2))                     AS INDEX_VALUE_C 
       ,A.SA_AMT + COALESCE(B.SA_AMT, 0)                       AS SA_AMT 
       ,B.SA_TX_AMT             AS SA_TX_AMT 
       ,B.SA_TX_DT              AS SA_TX_DT 
       ,SUBSTR(V_DT, 1, 4)                       AS YEAR 
       ,SUBSTR(V_DT, 6, 2)                       AS MONTH 
       ,V_DT               AS ETL_DATE 
   FROM ACRM_A_TRANSLOG_TARGET A                               --
   LEFT JOIN ACRM_A_TRANSLOG_TARGET_HIS B                      --
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.CUST_TYP              = B.CUST_TYP 
    AND A.CHANNEL_FLAG          = B.CHANNEL_FLAG 
    AND B.MONTH                 = SUBSTR(V_DT,6,2) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_TRANSLOG_TARGET_HIS_INNTMP1 = sqlContext.sql(sql)
ACRM_A_TRANSLOG_TARGET_HIS_INNTMP1.registerTempTable("ACRM_A_TRANSLOG_TARGET_HIS_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --:src.CUST_ID
       ,DST.CUST_TYP                                           --:src.CUST_TYP
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.CHANNEL_FLAG                                       --:src.CHANNEL_FLAG
       ,DST.INDEX_VALUE_C                                      --:src.INDEX_VALUE_C
       ,DST.SA_AMT                                             --:src.SA_AMT
       ,DST.SA_TX_AMT                                          --:src.SA_TX_AMT
       ,DST.SA_TX_DT                                           --:src.SA_TX_DT
       ,DST.YEAR                                               --:src.YEAR
       ,DST.MONTH                                              --:src.MONTH
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM ACRM_A_TRANSLOG_TARGET_HIS DST 
   LEFT JOIN ACRM_A_TRANSLOG_TARGET_HIS_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_TYP            = DST.CUST_TYP 
    AND SRC.CHANNEL_FLAG        = DST.CHANNEL_FLAG 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_A_TRANSLOG_TARGET_HIS_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_A_TRANSLOG_TARGET_HIS/"+V_DT+".parquet"
ACRM_A_TRANSLOG_TARGET_HIS_INNTMP2=ACRM_A_TRANSLOG_TARGET_HIS_INNTMP2.unionAll(ACRM_A_TRANSLOG_TARGET_HIS_INNTMP1)
#ACRM_A_TRANSLOG_TARGET_HIS_INNTMP1.cache()
#ACRM_A_TRANSLOG_TARGET_HIS_INNTMP2.cache()
#nrowsi = ACRM_A_TRANSLOG_TARGET_HIS_INNTMP1.count()
#nrowsa = ACRM_A_TRANSLOG_TARGET_HIS_INNTMP2.count()
ACRM_A_TRANSLOG_TARGET_HIS_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#ACRM_A_TRANSLOG_TARGET_HIS_INNTMP1.unpersist()
ACRM_A_TRANSLOG_TARGET_HIS_INNTMP2.unpersist()
#增改表保存后，复制当天数据进BK：先删除BK当天日期，然后复制
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TRANSLOG_TARGET_HIS/"+V_DT_LD+".parquet")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_TRANSLOG_TARGET_HIS_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_A_TRANSLOG_TARGET_HIS/"+V_DT+".parquet /"+dbname+"/ACRM_A_TRANSLOG_TARGET_HIS_BK/"+V_DT+".parquet")

et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

