#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUST_PORTRAIT_TARGET').setMaster(sys.argv[2])
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

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
OCRM_F_CI_CREDIT_REPORT_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CREDIT_REPORT_INFO/*')
OCRM_F_CI_CREDIT_REPORT_INFO.registerTempTable("OCRM_F_CI_CREDIT_REPORT_INFO")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
OCRM_F_CI_UNGUARANTEE_ATTENTION_TYPE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_UNGUARANTEE_ATTENTION_TYPE/*')
OCRM_F_CI_UNGUARANTEE_ATTENTION_TYPE.registerTempTable("OCRM_F_CI_UNGUARANTEE_ATTENTION_TYPE")
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS BIGINT)                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,CAST(CASE WHEN SUM(NVL(C.BAL_RMB, 0)) = 0 THEN 0 ELSE SUM(NVL(B.MONTH_AVG, 0)) / SUM(NVL(C.BAL_RMB, 0))  END    AS DECIMAL(20,2))    AS SALE_RATE 
       ,CAST('' AS DECIMAL(10,2))                    AS LOAN_CONCEN 
       ,V_DT               AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --
  INNER JOIN ACRM_F_DP_SAVE_INFO B                             --
     ON A.CUST_ID               = B.CUST_ID 
    AND B.CUST_TYP              = '2' 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO C                      --
     ON A.CUST_ID               = C.CUST_ID 
    AND C.CUST_TYP              = '2' 
    AND A.FR_ID                 = C.FR_ID 
	GROUP BY A.CUST_ID
			,A.FR_ID"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1.registerTempTable("OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1")

OCRM_F_CI_CUST_PORTRAIT_TARGET = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_PORTRAIT_TARGET_BK/'+V_DT_LD+'.parquet/*')
OCRM_F_CI_CUST_PORTRAIT_TARGET.registerTempTable("OCRM_F_CI_CUST_PORTRAIT_TARGET")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.SALE_RATE                                          --销售归行率:src.SALE_RATE
       ,DST.LOAN_CONCEN                                        --贷款集中度:src.LOAN_CONCEN
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_CUST_PORTRAIT_TARGET DST 
   LEFT JOIN OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_PORTRAIT_TARGET/"+V_DT+".parquet"
UNION=OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP2.unionAll(OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1)
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1.cache()
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1.unpersist()
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_PORTRAIT_TARGET lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_PORTRAIT_TARGET/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_PORTRAIT_TARGET_BK/")

#任务[12] 001-02::
V_STEP = V_STEP + 1
OCRM_F_CI_CUST_PORTRAIT_TARGET = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_PORTRAIT_TARGET/*')
OCRM_F_CI_CUST_PORTRAIT_TARGET.registerTempTable("OCRM_F_CI_CUST_PORTRAIT_TARGET")
sql = """
 SELECT CAST('' AS BIGINT)                    AS ID 
       ,T.CUST_ID               AS CUST_ID 
       ,CAST('' AS DECIMAL(20,2))                    AS SALE_RATE 
       ,CAST(SUM(CASE WHEN (NVL(REGEXP_REPLACE(F.TOTAL_BALANCE, ',',''), 0)) = 0 THEN 0 ELSE (NVL(D.BAL_RMB, 0)) / (NVL(REGEXP_REPLACE(F.TOTAL_BALANCE, ',',''), 0)) END)     AS DECIMAL(10,2))                  AS LOAN_CONCEN 
       ,V_DT                    AS ETL_DATE 
       ,T.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_PORTRAIT_TARGET T                       --
  INNER JOIN OCRM_F_CI_COM_CUST_INFO A                         --
     ON T.CUST_ID               = A.CUST_ID 
    AND T.FR_ID                 = A.FR_ID 
  INNER JOIN OCRM_F_CI_CREDIT_REPORT_INFO E                    --
     ON A.CUST_ID               = E.CUST_ID 
    AND A.FR_ID                 = E.FR_ID 
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO D                      --
     ON A.CUST_ID               = D.CUST_ID 
    AND A.FR_ID                 = D.FR_ID 
    AND D.CUST_TYP              = '2' 
   LEFT JOIN OCRM_F_CI_UNGUARANTEE_ATTENTION_TYPE F            --
     ON E.ID                    = F.CREDITREPORT_ID 
    AND F.UN_TYPE               = '0' 
  GROUP BY T.CUST_ID 
       ,T.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1.registerTempTable("OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1")

sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.SALE_RATE                                          --销售归行率:src.SALE_RATE
       ,DST.LOAN_CONCEN                                        --贷款集中度:src.LOAN_CONCEN
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_CUST_PORTRAIT_TARGET DST 
   LEFT JOIN OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_PORTRAIT_TARGET/"+V_DT+".parquet"
UNION=OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP2.unionAll(OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1)
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1.cache()
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP1.unpersist()
OCRM_F_CI_CUST_PORTRAIT_TARGET_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_PORTRAIT_TARGET lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_PORTRAIT_TARGET_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_CUST_PORTRAIT_TARGET/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_CUST_PORTRAIT_TARGET_BK/")