#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CM_EXCHANGE_RATE').setMaster(sys.argv[2])
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
#OCRM_F_CM_EXCHANGE_RATE	增量 跑批删除当天的文件

ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CM_EXCHANGE_RATE/"+V_DT+".parquet")

F_CM_CBOD_CMCRTCRT = sqlContext.read.parquet(hdfs+'/F_CM_CBOD_CMCRTCRT/*')
F_CM_CBOD_CMCRTCRT.registerTempTable("F_CM_CBOD_CMCRTCRT")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CM_CONV_CURR_COD_IN   AS CURRENCY_CD 
       ,A.CM_CONV_CURR_COD_OUT  AS OBJECT_CURRENCY_CD 
       ,V_DT               AS EFF_DATE 
       ,A.CM_FXR_TYP            AS EXCHANGE_TYPE_CD 
       ,CAST(1 AS DECIMAL(10,2))                    AS EXCHANGE_UNIT 
       ,CAST(A.CM_BUG AS DECIMAL(15,8))               AS BUY_RATE 
       ,CAST(A.CM_SLD AS DECIMAL(15,8))                AS SELL_RATE 
       ,CAST((CM_SLD + CM_BUG) / 2   AS DECIMAL(15,8))                    AS EXCHANGE_RATE 
       ,''                    AS ETL_DT 
       ,V_DT                  AS ODS_ST_DATE 
   FROM F_CM_CBOD_CMCRTCRT A                                   --汇率
  INNER JOIN(
         SELECT MAX(CM_FXR_EFFDT) CM_FXR_EFFDT 
           FROM F_CM_CBOD_CMCRTCRT) B                          --
     ON A.CM_FXR_EFFDT          = B.CM_FXR_EFFDT 
  WHERE A.CM_FXR_TYP            = '99' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CM_EXCHANGE_RATE = sqlContext.sql(sql)
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
dfn="OCRM_F_CM_EXCHANGE_RATE/"+V_DT+".parquet"
OCRM_F_CM_EXCHANGE_RATE.cache()
nrows = OCRM_F_CM_EXCHANGE_RATE.count()
OCRM_F_CM_EXCHANGE_RATE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CM_EXCHANGE_RATE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CM_EXCHANGE_RATE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CM_CONV_CURR_COD_OUT  AS CURRENCY_CD 
       ,A.CM_CONV_CURR_COD_IN   AS OBJECT_CURRENCY_CD 
       ,V_DT               AS EFF_DATE 
       ,A.CM_FXR_TYP            AS EXCHANGE_TYPE_CD 
       ,CAST(1 AS DECIMAL(10,2))                     AS EXCHANGE_UNIT 
       ,CAST(1 / CM_SLD AS DECIMAL(15,8))                 AS BUY_RATE 
       ,CAST(1 / CM_BUG   AS DECIMAL(15,8))               AS SELL_RATE 
       ,CAST((1 / CM_SLD + 1 / CM_BUG) / 2   AS DECIMAL(15,8))                    AS EXCHANGE_RATE 
       ,''                    AS ETL_DT 
       ,V_DT                  AS ODS_ST_DATE 
   FROM F_CM_CBOD_CMCRTCRT A                                   --汇率
  INNER JOIN(
         SELECT MAX(CM_FXR_EFFDT) CM_FXR_EFFDT 
           FROM F_CM_CBOD_CMCRTCRT) B                          --汇率
     ON A.CM_FXR_EFFDT          = B.CM_FXR_EFFDT 
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE C                         --
     ON C.CURRENCY_CD           = A.CM_CONV_CURR_COD_OUT 
    AND A.CM_CONV_CURR_COD_IN   = C.OBJECT_CURRENCY_CD 
    AND A.CM_FXR_TYP            = C.EXCHANGE_TYPE_CD 
    AND C.EFF_DATE              = V_DT 
  WHERE A.CM_FXR_TYP            = '99' 
    AND C.CURRENCY_CD IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CM_EXCHANGE_RATE = sqlContext.sql(sql)
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
dfn="OCRM_F_CM_EXCHANGE_RATE/"+V_DT+".parquet"
OCRM_F_CM_EXCHANGE_RATE.cache()
nrows = OCRM_F_CM_EXCHANGE_RATE.count()
OCRM_F_CM_EXCHANGE_RATE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CM_EXCHANGE_RATE.unpersist()
F_CM_CBOD_CMCRTCRT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CM_EXCHANGE_RATE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CURRENCY_CD           AS CURRENCY_CD 
       ,A.OBJECT_CURRENCY_CD    AS OBJECT_CURRENCY_CD 
       ,V_DT                  AS EFF_DATE 
       ,A.EXCHANGE_TYPE_CD      AS EXCHANGE_TYPE_CD 
       ,CAST(A.EXCHANGE_UNIT  AS DECIMAL(10,2))       AS EXCHANGE_UNIT 
       ,CAST(A.BUY_RATE AS DECIMAL(15,8))             AS BUY_RATE 
       ,CAST(A.SELL_RATE  AS DECIMAL(15,8))           AS SELL_RATE 
       ,CAST(A.EXCHANGE_RATE AS DECIMAL(15,8))        AS EXCHANGE_RATE 
       ,''                    AS ETL_DT 
       ,V_DT                  AS ODS_ST_DATE 
   FROM OCRM_F_CM_EXCHANGE_RATE A                              --汇率
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE B                         --汇率
     ON B.ODS_ST_DATE           = V_DT 
    AND A.CURRENCY_CD           = B.CURRENCY_CD 
    AND A.OBJECT_CURRENCY_CD    = B.OBJECT_CURRENCY_CD 
    AND A.EXCHANGE_TYPE_CD      = B.EXCHANGE_TYPE_CD 
    AND B.EFF_DATE              = V_DT 
  WHERE A.ODS_ST_DATE           = V_DT_LD
    AND B.CURRENCY_CD IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT_LD\b", "'"+V_DT_LD+"'", sql)
OCRM_F_CM_EXCHANGE_RATE = sqlContext.sql(sql)
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
dfn="OCRM_F_CM_EXCHANGE_RATE/"+V_DT+".parquet"
OCRM_F_CM_EXCHANGE_RATE.cache()
nrows = OCRM_F_CM_EXCHANGE_RATE.count()
OCRM_F_CM_EXCHANGE_RATE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CM_EXCHANGE_RATE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CM_EXCHANGE_RATE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
