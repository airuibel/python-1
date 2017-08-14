#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_RE_AUMSUMAVGINFO').setMaster(sys.argv[2])
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
#计算本年天数
V_YEAR_FIRST_DAY=(date(int(etl_date[0:4]),1,1)).strftime("%Y-%m-%d")

V_STEP = 0

ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")
ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")
ACRM_F_RE_INSUSUMINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_INSUSUMINFO/*')
ACRM_F_RE_INSUSUMINFO.registerTempTable("ACRM_F_RE_INSUSUMINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.OPEN_BRC              AS ORG_ID 
       ,'1'                     AS AUM_TYPE 
       ,CAST(SUM(A.AMOUNT)   AS DECIMAL(22,2))                    AS AMOUNT 
       ,CAST(CASE MONTH(V_DT) 
            WHEN '1' THEN  SUM(NVL(A.MONTH_BAL_SUM_1, 0))  
            WHEN '2' THEN SUM(NVL(A.MONTH_BAL_SUM_2, 0)) 
            WHEN '3' THEN SUM(NVL(A.MONTH_BAL_SUM_3, 0)) 
            WHEN '4' THEN SUM(NVL(A.MONTH_BAL_SUM_4, 0)) 
            WHEN '5' THEN SUM(NVL(A.MONTH_BAL_SUM_5, 0)) 
            WHEN '6' THEN SUM(NVL(A.MONTH_BAL_SUM_6, 0)) 
            WHEN '7' THEN SUM(NVL(A.MONTH_BAL_SUM_7, 0)) 
            WHEN '8' THEN SUM(NVL(A.MONTH_BAL_SUM_8, 0)) 
            WHEN '9' THEN SUM(NVL(A.MONTH_BAL_SUM_9, 0)) 
            WHEN '10' THEN SUM(NVL(A.MONTH_BAL_SUM_10, 0)) 
            WHEN '11' THEN SUM(NVL(A.MONTH_BAL_SUM_11, 0)) 
            WHEN '12' THEN SUM(NVL(A.MONTH_BAL_SUM_12, 0)) END   AS DECIMAL(22,2))                  AS MONTH_SUM 
       ,CAST(SUM(NVL(MONTH_BAL_SUM_1, 0) + NVL(MONTH_BAL_SUM_2, 0) + NVL(MONTH_BAL_SUM_3, 0) + NVL(MONTH_BAL_SUM_4, 0) + NVL(MONTH_BAL_SUM_5, 0) + NVL(MONTH_BAL_SUM_6, 0) + NVL(MONTH_BAL_SUM_7, 0) + NVL(MONTH_BAL_SUM_8, 0) + NVL(MONTH_BAL_SUM_9, 0) + NVL(MONTH_BAL_SUM_10, 0) + NVL(MONTH_BAL_SUM_11, 0) + NVL(MONTH_BAL_SUM_12, 0))  AS DECIMAL(22,2))                     AS YEAR_SUM 
       ,CAST(DAY(V_DT) AS INTEGER)                       AS MONTH_DAYS 
       ,CAST(datediff(V_DT,V_YEAR_FIRST) AS INTEGER)        AS YEAR_DAYS 
       ,V_DT                  AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --
  WHERE A.YEAR                  = YEAR(V_DT) 
  GROUP BY A.CUST_ID 
       ,A.OPEN_BRC
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_YEAR_FIRST\b", "'"+V_YEAR_FIRST_DAY+"'", sql)
ACRM_F_RE_AUMSUMAVGINFO = sqlContext.sql(sql)
ACRM_F_RE_AUMSUMAVGINFO.registerTempTable("ACRM_F_RE_AUMSUMAVGINFO")
dfn="ACRM_F_RE_AUMSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_AUMSUMAVGINFO.cache()
nrows = ACRM_F_RE_AUMSUMAVGINFO.count()
ACRM_F_RE_AUMSUMAVGINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_AUMSUMAVGINFO.unpersist()
ACRM_F_RE_SAVESUMAVGINFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_AUMSUMAVGINFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_AUMSUMAVGINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT B.CUST_ID               AS CUST_ID 
       ,B.ORG_ID                AS ORG_ID 
       ,'2'                     AS AUM_TYPE 
       ,CAST(SUM(B.AMT)  AS   DECIMAL(22,2) )                 AS AMOUNT 
       ,CAST(CASE MONTH(V_DT) 
           WHEN '1' THEN SUM(NVL(B.MONTH_BAL_SUM_1, 0)) 
           WHEN '2' THEN SUM(NVL(B.MONTH_BAL_SUM_2, 0)) 
           WHEN '3' THEN SUM(NVL(B.MONTH_BAL_SUM_3, 0)) 
           WHEN '4' THEN SUM(NVL(B.MONTH_BAL_SUM_4, 0)) 
           WHEN '5' THEN SUM(NVL(B.MONTH_BAL_SUM_5, 0)) 
           WHEN '6' THEN SUM(NVL(B.MONTH_BAL_SUM_6, 0)) 
           WHEN '7' THEN SUM(NVL(B.MONTH_BAL_SUM_7, 0)) 
           WHEN '8' THEN SUM(NVL(B.MONTH_BAL_SUM_8, 0)) 
           WHEN '9' THEN SUM(NVL(B.MONTH_BAL_SUM_9, 0)) 
           WHEN '10' THEN SUM(NVL(B.MONTH_BAL_SUM_10, 0)) 
           WHEN '11' THEN SUM(NVL(B.MONTH_BAL_SUM_11, 0)) 
           WHEN '12' THEN SUM(NVL(B.MONTH_BAL_SUM_12, 0)) END     AS DECIMAL(22,2))                   AS MONTH_SUM 
       ,CAST(SUM(NVL(MONTH_BAL_SUM_1, 0) + NVL(MONTH_BAL_SUM_2, 0) + NVL(MONTH_BAL_SUM_3, 0) + NVL(MONTH_BAL_SUM_4, 0) + NVL(MONTH_BAL_SUM_5, 0) + NVL(MONTH_BAL_SUM_6, 0) + NVL(MONTH_BAL_SUM_7, 0) + NVL(MONTH_BAL_SUM_8, 0) + NVL(MONTH_BAL_SUM_9, 0) + NVL(MONTH_BAL_SUM_10, 0) + NVL(MONTH_BAL_SUM_11, 0) + NVL(MONTH_BAL_SUM_12, 0)) AS DECIMAL(22,2))                         AS YEAR_SUM 
       ,CAST(DAY(V_DT) AS INTEGER)                       AS MONTH_DAYS 
       ,CAST(datediff(V_DT,V_YEAR_FIRST) AS INTEGER)          AS YEAR_DAYS 
       ,V_DT                  AS ETL_DATE 
       ,B.FR_ID                 AS FR_ID 
   FROM ACRM_F_RE_ACCSUMAVGINFO B                              --理财积数均值表
  WHERE YEAR                    = YEAR(V_DT) 
  GROUP BY B.CUST_ID 
       ,B.ORG_ID
	   ,B.FR_ID 	   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_YEAR_FIRST\b", "'"+V_YEAR_FIRST_DAY+"'", sql)
ACRM_F_RE_AUMSUMAVGINFO = sqlContext.sql(sql)
ACRM_F_RE_AUMSUMAVGINFO.registerTempTable("ACRM_F_RE_AUMSUMAVGINFO")
dfn="ACRM_F_RE_AUMSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_AUMSUMAVGINFO.cache()
nrows = ACRM_F_RE_AUMSUMAVGINFO.count()
ACRM_F_RE_AUMSUMAVGINFO.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_RE_AUMSUMAVGINFO.unpersist()
ACRM_F_RE_ACCSUMAVGINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_AUMSUMAVGINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT C.CUST_ID               AS CUST_ID 
       ,C.ORG_NO                AS ORG_ID 
       ,'3'                     AS AUM_TYPE 
       ,CAST(SUM(C.AMOUNT)  AS   DECIMAL(22,2) )                     AS AMOUNT 
       ,CAST(CASE MONTH(V_DT)
            WHEN '1' THEN SUM(NVL(C.MONTH_BAL_SUM_1, 0)) 
            WHEN '2' THEN SUM(NVL(C.MONTH_BAL_SUM_2, 0)) 
            WHEN '3' THEN SUM(NVL(C.MONTH_BAL_SUM_3, 0)) 
            WHEN '4' THEN SUM(NVL(C.MONTH_BAL_SUM_4, 0)) 
            WHEN '5' THEN SUM(NVL(C.MONTH_BAL_SUM_5, 0)) 
            WHEN '6' THEN SUM(NVL(C.MONTH_BAL_SUM_6, 0)) 
            WHEN '7' THEN SUM(NVL(C.MONTH_BAL_SUM_7, 0)) 
            WHEN '8' THEN SUM(NVL(C.MONTH_BAL_SUM_8, 0)) 
            WHEN '9' THEN SUM(NVL(C.MONTH_BAL_SUM_9, 0)) 
            WHEN '10' THEN SUM(NVL(C.MONTH_BAL_SUM_10, 0)) 
            WHEN '11' THEN SUM(NVL(C.MONTH_BAL_SUM_11, 0)) 
            WHEN '12' THEN SUM(NVL(C.MONTH_BAL_SUM_12, 0)) END    AS   DECIMAL(22,2) )                   AS MONTH_SUM 
       ,CAST(SUM(NVL(MONTH_BAL_SUM_1, 0) + NVL(MONTH_BAL_SUM_2, 0) + NVL(MONTH_BAL_SUM_3, 0) + NVL(MONTH_BAL_SUM_4, 0) + NVL(MONTH_BAL_SUM_5, 0) + NVL(MONTH_BAL_SUM_6, 0) + NVL(MONTH_BAL_SUM_7, 0) + NVL(MONTH_BAL_SUM_8, 0) + NVL(MONTH_BAL_SUM_9, 0) + NVL(MONTH_BAL_SUM_10, 0) + NVL(MONTH_BAL_SUM_11, 0) + NVL(MONTH_BAL_SUM_12, 0))    AS   DECIMAL(22,2) )                     AS YEAR_SUM 
       ,CAST(DAY(V_DT) AS INTEGER)                 AS MONTH_DAYS 
       ,CAST(datediff(V_DT,V_YEAR_FIRST) AS INTEGER)                       AS YEAR_DAYS 
       ,V_DT     AS ETL_DATE 
       ,C.FR_ID                  AS FR_ID 
   FROM ACRM_F_RE_INSUSUMINFO C                                --保险账户累计表
  WHERE YEAR                    = YEAR(V_DT) 
  GROUP BY C.CUST_ID 
       ,C.ORG_NO
       ,C.FR_ID	   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_YEAR_FIRST\b", "'"+V_YEAR_FIRST_DAY+"'", sql)
ACRM_F_RE_AUMSUMAVGINFO = sqlContext.sql(sql)
ACRM_F_RE_AUMSUMAVGINFO.registerTempTable("ACRM_F_RE_AUMSUMAVGINFO")
dfn="ACRM_F_RE_AUMSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_AUMSUMAVGINFO.cache()
nrows = ACRM_F_RE_AUMSUMAVGINFO.count()
ACRM_F_RE_AUMSUMAVGINFO.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_RE_AUMSUMAVGINFO.unpersist()
ACRM_F_RE_INSUSUMINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_AUMSUMAVGINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
