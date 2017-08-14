#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_GK_ZJYW').setMaster(sys.argv[2])
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

ACRM_F_NI_SIGN = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_SIGN/*')
ACRM_F_NI_SIGN.registerTempTable("ACRM_F_NI_SIGN")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.S_PRODUCT_ID          AS TYPE 
       ,CAST(COUNT(1)  AS INTEGER)                    AS COUNT 
       ,SUM(NVL(BAL,0))                       AS AMOUNT 
       ,CAST(SUM(NVL(FEE_AMT,0)) AS DECIMAL(18,2))                      AS INCOME 
       ,''                    AS CUST_TYP 
       ,A.FR_ID                 AS FR_ORG_ID 
       ,CASE S_PRODUCT_ID WHEN '800001' THEN '省级电费代扣' WHEN '000050' THEN '平安养老保险' WHEN '000051' THEN '人民财险保险' WHEN '000052' THEN '人民健康保险' WHEN '000053' THEN '太平洋人寿保险' WHEN '000054' THEN '泰康保险' WHEN '000055' THEN '幸福保险' WHEN '000056' THEN '阳光财险' WHEN '000057' THEN '中国人寿保险' WHEN '000058' THEN '新华保险' WHEN '000059' THEN '华夏人寿保险' WHEN '000060' THEN '天安保险' WHEN '000061' THEN '紫金保险' WHEN '000062' THEN '平安财险' WHEN '000063' THEN '人民人寿保险' WHEN '000064' THEN '太平洋财险' WHEN '000065' THEN '阳光人寿保险' END                     AS BUSI_NAME 
       ,V_DT                  AS ETL_DATE 
       ,CAST('' AS  BIGINT)   AS ID 
       ,''                    AS FR_ID 
   FROM ACRM_F_NI_SIGN A                                       --中间业务协议
  WHERE CUST_ID IS  NOT NULL 
  GROUP BY A.FR_ID 
       ,A.CUST_ID 
       ,A.S_PRODUCT_ID 
       ,CASE S_PRODUCT_ID WHEN '800001' THEN '省级电费代扣' WHEN '000050' THEN '平安养老保险' WHEN '000051' THEN '人民财险保险' WHEN '000052' THEN '人民健康保险' WHEN '000053' THEN '太平洋人寿保险' WHEN '000054' THEN '泰康保险' WHEN '000055' THEN '幸福保险' WHEN '000056' THEN '阳光财险' WHEN '000057' THEN '中国人寿保险' WHEN '000058' THEN '新华保险' WHEN '000059' THEN '华夏人寿保险' WHEN '000060' THEN '天安保险' WHEN '000061' THEN '紫金保险' WHEN '000062' THEN '平安财险' WHEN '000063' THEN '人民人寿保险' WHEN '000064' THEN '太平洋财险' WHEN '000065' THEN '阳光人寿保险' END 
       """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_GK_ZJYW = sqlContext.sql(sql)
ACRM_F_CI_GK_ZJYW.registerTempTable("ACRM_F_CI_GK_ZJYW")
dfn="ACRM_F_CI_GK_ZJYW/"+V_DT+".parquet"
ACRM_F_CI_GK_ZJYW.cache()
nrows = ACRM_F_CI_GK_ZJYW.count()
ACRM_F_CI_GK_ZJYW.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_GK_ZJYW.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_GK_ZJYW/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_GK_ZJYW lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
