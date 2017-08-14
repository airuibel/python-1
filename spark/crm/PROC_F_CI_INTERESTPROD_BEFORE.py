#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_INTERESTPROD_BEFORE').setMaster(sys.argv[2])
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

OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
OCRM_F_PD_PROD_CATL_VIEW = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_CATL_VIEW/*')
OCRM_F_PD_PROD_CATL_VIEW.registerTempTable("OCRM_F_PD_PROD_CATL_VIEW")
ACRM_A_PRODUCT_FEATURE = sqlContext.read.parquet(hdfs+'/ACRM_A_PRODUCT_FEATURE/*')
ACRM_A_PRODUCT_FEATURE.registerTempTable("ACRM_A_PRODUCT_FEATURE")
OCRM_F_CI_INTERESTPROD_TMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INTERESTPROD_TMP/*')
OCRM_F_CI_INTERESTPROD_TMP.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT B.PRODUCT_ID            AS PRODUCT_ID 
       ,B.PROD_NAME             AS PROD_NAME 
       ,A.CATL_CODE             AS CATL_CODE 
       ,B.FR_ID                 AS FR_ID 
   FROM OCRM_F_PD_PROD_CATL_VIEW A                             --
  INNER JOIN OCRM_F_PD_PROD_CATL_VIEW C                        --产品表
     ON LOCATE(A.CATLSEQ, C.CATLSEQ) > 0 
  INNER JOIN OCRM_F_PD_PROD_INFO B                             --
     ON B.CATL_CODE             = C.CATL_CODE 
  WHERE A.CATL_LEVEL            = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INTERESTPROD_TMP_01 = sqlContext.sql(sql)
OCRM_F_CI_INTERESTPROD_TMP_01.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP_01")
dfn="OCRM_F_CI_INTERESTPROD_TMP_01/"+V_DT+".parquet"
OCRM_F_CI_INTERESTPROD_TMP_01.cache()
nrows = OCRM_F_CI_INTERESTPROD_TMP_01.count()
OCRM_F_CI_INTERESTPROD_TMP_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INTERESTPROD_TMP_01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_INTERESTPROD_TMP_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INTERESTPROD_TMP_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

OCRM_F_CI_INTERESTPROD_TMP_01 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INTERESTPROD_TMP_01/*')
OCRM_F_CI_INTERESTPROD_TMP_01.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP_01")

sql = """
 SELECT A.PRODUCT_ID            AS PRODUCT_ID 
       ,M.PROD_NAME             AS PROD_NAME 
       ,M.CATL_CODE             AS CATL_CODE 
       ,A.TARGET                AS TARGET 
       ,A.TARGET_VALUE          AS TARGET_VALUE 
       ,ROW_NUMBER() OVER(
      PARTITION BY A.PRODUCT_ID 
               ,M.CATL_CODE 
               ,A.TARGET 
          ORDER BY A.NUM_VALUE DESC)                       AS RANK 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_A_PRODUCT_FEATURE A                               --产品特征项分析
  INNER JOIN OCRM_F_CI_INTERESTPROD_TMP_01 M                   --感兴趣产品中间表临时表01
     ON A.PRODUCT_ID            = M.PRODUCT_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INTERESTPROD_TMP_02 = sqlContext.sql(sql)
OCRM_F_CI_INTERESTPROD_TMP_02.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP_02")
dfn="OCRM_F_CI_INTERESTPROD_TMP_02/"+V_DT+".parquet"
OCRM_F_CI_INTERESTPROD_TMP_02.cache()
nrows = OCRM_F_CI_INTERESTPROD_TMP_02.count()
OCRM_F_CI_INTERESTPROD_TMP_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INTERESTPROD_TMP_02.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_INTERESTPROD_TMP_02/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INTERESTPROD_TMP_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT A.PRODUCT_ID            AS PRODUCT_ID 
       ,B.PROD_NAME             AS PROD_NAME 
       ,B.CATL_CODE             AS CATL_CODE 
       ,A.TARGET                AS TARGET 
       ,A.TARGET_VALUE          AS TARGET_VALUE 
       ,ROW_NUMBER() OVER(
      PARTITION BY A.PRODUCT_ID 
               ,B.CATL_CODE 
               ,A.TARGET 
          ORDER BY A.NUM_VALUE DESC)                       AS RANK 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_A_PRODUCT_FEATURE A                               --产品特征项分析
  INNER JOIN OCRM_F_PD_PROD_INFO B                             --产品表
     ON A.PRODUCT_ID            = B.PRODUCT_ID 
    AND B.CATL_CODE IN('E', 'I') 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INTERESTPROD_TMP_03 = sqlContext.sql(sql)
OCRM_F_CI_INTERESTPROD_TMP_03.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP_03")
dfn="OCRM_F_CI_INTERESTPROD_TMP_03/"+V_DT+".parquet"
OCRM_F_CI_INTERESTPROD_TMP_03.cache()
nrows = OCRM_F_CI_INTERESTPROD_TMP_03.count()
OCRM_F_CI_INTERESTPROD_TMP_03.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INTERESTPROD_TMP_03.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_INTERESTPROD_TMP_03/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INTERESTPROD_TMP_03 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT A.PRODUCT_ID            AS PRODUCT_ID 
       ,M.PROD_NAME             AS PROD_NAME 
       ,M.CATL_CODE             AS CATL_CODE 
       ,A.TARGET                AS TARGET 
       ,A.TARGET_VALUE          AS TARGET_VALUE 
       ,ROW_NUMBER() OVER(
      PARTITION BY A.PRODUCT_ID 
               ,M.CATL_CODE 
               ,A.TARGET 
          ORDER BY A.NUM_VALUE DESC)                       AS RANK 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_A_PRODUCT_FEATURE A                               --产品特征项分析
  INNER JOIN OCRM_F_CI_INTERESTPROD_TMP_01 M                   --感兴趣产品中间表临时表01
     ON A.PRODUCT_ID            = M.PRODUCT_ID 
    AND A.FR_ID                 = M.FR_ID 
   LEFT JOIN OCRM_F_CI_INTERESTPROD_TMP N                      --感兴趣产品中间表
     ON A.FR_ID                 = N.FR_ID 
    AND A.PRODUCT_ID            = N.PRODUCT_ID 
  WHERE N.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INTERESTPROD_TMP_04 = sqlContext.sql(sql)
OCRM_F_CI_INTERESTPROD_TMP_04.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP_04")
dfn="OCRM_F_CI_INTERESTPROD_TMP_04/"+V_DT+".parquet"
OCRM_F_CI_INTERESTPROD_TMP_04.cache()
nrows = OCRM_F_CI_INTERESTPROD_TMP_04.count()
OCRM_F_CI_INTERESTPROD_TMP_04.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INTERESTPROD_TMP_04.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_INTERESTPROD_TMP_04/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INTERESTPROD_TMP_04 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT A.PRODUCT_ID            AS PRODUCT_ID 
       ,B.PROD_NAME             AS PROD_NAME 
       ,B.CATL_CODE             AS CATL_CODE 
       ,A.TARGET                AS TARGET 
       ,A.TARGET_VALUE          AS TARGET_VALUE 
       ,ROW_NUMBER() OVER(
      PARTITION BY A.PRODUCT_ID 
               ,B.CATL_CODE 
               ,A.TARGET 
          ORDER BY A.NUM_VALUE DESC)                       AS RANK 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_A_PRODUCT_FEATURE A                               --产品特征项分析
  INNER JOIN OCRM_F_PD_PROD_INFO B                             --产品表
     ON A.PRODUCT_ID            = B.PRODUCT_ID 
    AND B.CATL_CODE IN('E', 'I') 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_CI_INTERESTPROD_TMP N                      --感兴趣产品中间表
     ON A.FR_ID                 = N.FR_ID 
    AND A.PRODUCT_ID            = N.PRODUCT_ID 
  WHERE N.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INTERESTPROD_TMP_05 = sqlContext.sql(sql)
OCRM_F_CI_INTERESTPROD_TMP_05.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP_05")
dfn="OCRM_F_CI_INTERESTPROD_TMP_05/"+V_DT+".parquet"
OCRM_F_CI_INTERESTPROD_TMP_05.cache()
nrows = OCRM_F_CI_INTERESTPROD_TMP_05.count()
OCRM_F_CI_INTERESTPROD_TMP_05.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INTERESTPROD_TMP_05.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_INTERESTPROD_TMP_05/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INTERESTPROD_TMP_05 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-06::
V_STEP = V_STEP + 1
OCRM_F_CI_INTERESTPROD_TMP_02 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INTERESTPROD_TMP_02/*')
OCRM_F_CI_INTERESTPROD_TMP_02.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP_02")
sql = """
 SELECT PRODUCT_ID              AS PRODUCT_ID 
       ,PROD_NAME               AS PROD_NAME 
       ,CATL_CODE               AS CATL_CODE 
       ,TARGET                  AS TARGET 
       ,TARGET_VALUE            AS TARGET_VALUE 
       ,V_DT                    AS ETL_DATE 
       ,FR_ID                   AS FR_ID 
   FROM OCRM_F_CI_INTERESTPROD_TMP_02 A                        --感兴趣产品中间表临时表02
  WHERE RANK                    = 1 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INTERESTPROD_TMP = sqlContext.sql(sql)
OCRM_F_CI_INTERESTPROD_TMP.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP")
dfn="OCRM_F_CI_INTERESTPROD_TMP/"+V_DT+".parquet"
OCRM_F_CI_INTERESTPROD_TMP.cache()
nrows = OCRM_F_CI_INTERESTPROD_TMP.count()
OCRM_F_CI_INTERESTPROD_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INTERESTPROD_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_INTERESTPROD_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INTERESTPROD_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-07::
V_STEP = V_STEP + 1
OCRM_F_CI_INTERESTPROD_TMP_03 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INTERESTPROD_TMP_03/*')
OCRM_F_CI_INTERESTPROD_TMP_03.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP_03")
sql = """
 SELECT PRODUCT_ID              AS PRODUCT_ID 
       ,PROD_NAME               AS PROD_NAME 
       ,CATL_CODE               AS CATL_CODE 
       ,TARGET                  AS TARGET 
       ,TARGET_VALUE            AS TARGET_VALUE 
       ,V_DT                    AS ETL_DATE 
       ,FR_ID                   AS FR_ID 
   FROM OCRM_F_CI_INTERESTPROD_TMP_03 A                        --感兴趣产品中间表临时表03
  WHERE RANK                    = 1 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INTERESTPROD_TMP = sqlContext.sql(sql)
OCRM_F_CI_INTERESTPROD_TMP.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP")
dfn="OCRM_F_CI_INTERESTPROD_TMP/"+V_DT+".parquet"
OCRM_F_CI_INTERESTPROD_TMP.cache()
nrows = OCRM_F_CI_INTERESTPROD_TMP.count()
OCRM_F_CI_INTERESTPROD_TMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_INTERESTPROD_TMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INTERESTPROD_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-08::
V_STEP = V_STEP + 1
OCRM_F_CI_INTERESTPROD_TMP_04 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INTERESTPROD_TMP_04/*')
OCRM_F_CI_INTERESTPROD_TMP_04.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP_04")
sql = """
 SELECT PRODUCT_ID              AS PRODUCT_ID 
       ,PROD_NAME               AS PROD_NAME 
       ,CATL_CODE               AS CATL_CODE 
       ,TARGET                  AS TARGET 
       ,TARGET_VALUE            AS TARGET_VALUE 
       ,V_DT                    AS ETL_DATE 
       ,FR_ID                   AS FR_ID 
   FROM OCRM_F_CI_INTERESTPROD_TMP_04 A                        --感兴趣产品中间表临时表04
  WHERE RANK                    = 1 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INTERESTPROD_TMP = sqlContext.sql(sql)
OCRM_F_CI_INTERESTPROD_TMP.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP")
dfn="OCRM_F_CI_INTERESTPROD_TMP/"+V_DT+".parquet"
OCRM_F_CI_INTERESTPROD_TMP.cache()
nrows = OCRM_F_CI_INTERESTPROD_TMP.count()
OCRM_F_CI_INTERESTPROD_TMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_INTERESTPROD_TMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INTERESTPROD_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-09::
V_STEP = V_STEP + 1
OCRM_F_CI_INTERESTPROD_TMP_05 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INTERESTPROD_TMP_05/*')
OCRM_F_CI_INTERESTPROD_TMP_05.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP_05")
sql = """
 SELECT PRODUCT_ID              AS PRODUCT_ID 
       ,PROD_NAME               AS PROD_NAME 
       ,CATL_CODE               AS CATL_CODE 
       ,TARGET                  AS TARGET 
       ,TARGET_VALUE            AS TARGET_VALUE 
       ,V_DT                    AS ETL_DATE 
       ,FR_ID                   AS FR_ID 
   FROM OCRM_F_CI_INTERESTPROD_TMP_05 A                        --感兴趣产品中间表临时表05
  WHERE RANK                    = 1 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INTERESTPROD_TMP = sqlContext.sql(sql)
OCRM_F_CI_INTERESTPROD_TMP.registerTempTable("OCRM_F_CI_INTERESTPROD_TMP")
dfn="OCRM_F_CI_INTERESTPROD_TMP/"+V_DT+".parquet"
OCRM_F_CI_INTERESTPROD_TMP.cache()
nrows = OCRM_F_CI_INTERESTPROD_TMP.count()
OCRM_F_CI_INTERESTPROD_TMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_INTERESTPROD_TMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INTERESTPROD_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
