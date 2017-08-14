#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_BANK_COM_RELATION').setMaster(sys.argv[2])
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

ACRM_F_CI_DEPANDLOAN = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_DEPANDLOAN/*')
ACRM_F_CI_DEPANDLOAN.registerTempTable("ACRM_F_CI_DEPANDLOAN")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
ACRM_F_CI_NIN_TRANSLOG = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_NIN_TRANSLOG/*')
ACRM_F_CI_NIN_TRANSLOG.registerTempTable("ACRM_F_CI_NIN_TRANSLOG")
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id() AS DECIMAL(27))  AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,CAST(CASE WHEN(A.FIRST_OPEN_DATE IS NULL 
             OR A.FIRST_OPEN_DATE = '') THEN NULL ELSE INT(SUBSTR(V_DT, 1, 4)) - INT(SUBSTR(A.FIRST_OPEN_DATE, 1, 4)) END AS VARCHAR(20)) AS COOPERATION_YEAR 
       ,A.IF_BASIC              AS IF_BASIC 
       ,'0'                     AS DEP_RATIO 
       ,CAST(0 AS DECIMAL(22,2))                     AS CREDIT_LIMIT 
       ,CAST(0 AS DECIMAL(24,6))                     AS CASH_1 
       ,CAST(0 AS DECIMAL(24,6))                     AS CASH_2 
       ,CAST(0 AS DECIMAL(24,6))                     AS CASH_3 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT               AS ETL_DATE 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_BANK_COM_RELATION = sqlContext.sql(sql)
ACRM_F_CI_BANK_COM_RELATION.registerTempTable("ACRM_F_CI_BANK_COM_RELATION")
dfn="ACRM_F_CI_BANK_COM_RELATION/"+V_DT+".parquet"
ACRM_F_CI_BANK_COM_RELATION.cache()
nrows = ACRM_F_CI_BANK_COM_RELATION.count()
ACRM_F_CI_BANK_COM_RELATION.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_BANK_COM_RELATION.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_BANK_COM_RELATION/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_BANK_COM_RELATION lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1

ACRM_F_CI_BANK_COM_RELATION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_BANK_COM_RELATION/*')
ACRM_F_CI_BANK_COM_RELATION.registerTempTable("ACRM_F_CI_BANK_COM_RELATION")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.COOPERATION_YEAR      AS COOPERATION_YEAR 
       ,A.IF_BASIC              AS IF_BASIC 
       ,B.DEPO_LOAN             AS DEP_RATIO 
       ,A.CREDIT_LIMIT          AS CREDIT_LIMIT 
       ,A.CASH_1                AS CASH_1 
       ,A.CASH_2                AS CASH_2 
       ,A.CASH_3                AS CASH_3 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM ACRM_F_CI_BANK_COM_RELATION A                          --
  INNER JOIN ACRM_F_CI_DEPANDLOAN B                            --
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ORG_ID 
    AND B.CUST_ID LIKE '2%' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1 = sqlContext.sql(sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.registerTempTable("ACRM_F_CI_BANK_COM_RELATION_INNTMP1")

ACRM_F_CI_BANK_COM_RELATION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_BANK_COM_RELATION/*')
ACRM_F_CI_BANK_COM_RELATION.registerTempTable("ACRM_F_CI_BANK_COM_RELATION")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.COOPERATION_YEAR                                   --合作年限:src.COOPERATION_YEAR
       ,DST.IF_BASIC                                           --是否基本户:src.IF_BASIC
       ,DST.DEP_RATIO                                          --存贷比:src.DEP_RATIO
       ,DST.CREDIT_LIMIT                                       --授信额度:src.CREDIT_LIMIT
       ,DST.CASH_1                                             --现金流前一个月:src.CASH_1
       ,DST.CASH_2                                             --现金流前两个月:src.CASH_2
       ,DST.CASH_3                                             --现金流前三个月:src.CASH_3
       ,DST.FR_ID                                              --法人ID:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM ACRM_F_CI_BANK_COM_RELATION DST 
   LEFT JOIN ACRM_F_CI_BANK_COM_RELATION_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_CI_BANK_COM_RELATION/"+V_DT+".parquet"
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.unionAll(ACRM_F_CI_BANK_COM_RELATION_INNTMP1)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.cache()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.cache()
nrowsi = ACRM_F_CI_BANK_COM_RELATION_INNTMP1.count()
nrowsa = ACRM_F_CI_BANK_COM_RELATION_INNTMP2.count()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.unpersist()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_BANK_COM_RELATION lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_CI_BANK_COM_RELATION/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_CI_BANK_COM_RELATION_BK/")

#任务[12] 001-03::
V_STEP = V_STEP + 1

ACRM_F_CI_BANK_COM_RELATION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_BANK_COM_RELATION/*')
ACRM_F_CI_BANK_COM_RELATION.registerTempTable("ACRM_F_CI_BANK_COM_RELATION")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.COOPERATION_YEAR      AS COOPERATION_YEAR 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,B.CREDIT_LIMIT          AS CREDIT_LIMIT 
       ,A.CASH_1                AS CASH_1 
       ,A.CASH_2                AS CASH_2 
       ,A.CASH_3                AS CASH_3 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM ACRM_F_CI_BANK_COM_RELATION A                          --
  INNER JOIN (SELECT A.CUST_ID,
	                    A.FR_ID,
	                    CAST(SUM(COALESCE (C.CONT_AMT, 0)) AS DECIMAL(22,2)) AS CREDIT_LIMIT
	                FROM  OCRM_F_CI_COM_CUST_INFO A
	                LEFT  JOIN ACRM_F_CI_ASSET_BUSI_PROTO C
	                ON A.CUST_ID = C.CUST_ID 
	                AND C.FR_ID = A.FR_ID
	                AND C.CONT_STS = '1000'
                  AND (   C.SUBJECTNO LIKE '1301%'
                       OR C.SUBJECTNO LIKE '1302%'
                       OR C.SUBJECTNO LIKE '1303%'
                       OR C.SUBJECTNO LIKE '1304%'
                       OR C.SUBJECTNO LIKE '1305%'
                       OR C.SUBJECTNO LIKE '1306%'
                       OR C.SUBJECTNO LIKE '1307%'
                       OR C.SUBJECTNO LIKE '1308%')
                 GROUP BY A.CUST_ID,A.FR_ID) B  ON A.CUST_ID = B.CUST_ID AND A.FR_ID = B.FR_ID      
                 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1 = sqlContext.sql(sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.registerTempTable("ACRM_F_CI_BANK_COM_RELATION_INNTMP1")

ACRM_F_CI_BANK_COM_RELATION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_BANK_COM_RELATION/*')
ACRM_F_CI_BANK_COM_RELATION.registerTempTable("ACRM_F_CI_BANK_COM_RELATION")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.COOPERATION_YEAR                                   --合作年限:src.COOPERATION_YEAR
       ,DST.IF_BASIC                                           --是否基本户:src.IF_BASIC
       ,DST.DEP_RATIO                                          --存贷比:src.DEP_RATIO
       ,DST.CREDIT_LIMIT                                       --授信额度:src.CREDIT_LIMIT
       ,DST.CASH_1                                             --现金流前一个月:src.CASH_1
       ,DST.CASH_2                                             --现金流前两个月:src.CASH_2
       ,DST.CASH_3                                             --现金流前三个月:src.CASH_3
       ,DST.FR_ID                                              --法人ID:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM ACRM_F_CI_BANK_COM_RELATION DST 
   LEFT JOIN ACRM_F_CI_BANK_COM_RELATION_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_CI_BANK_COM_RELATION/"+V_DT+".parquet"
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.unionAll(ACRM_F_CI_BANK_COM_RELATION_INNTMP1)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.cache()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.cache()
nrowsi = ACRM_F_CI_BANK_COM_RELATION_INNTMP1.count()
nrowsa = ACRM_F_CI_BANK_COM_RELATION_INNTMP2.count()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.unpersist()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_BANK_COM_RELATION lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_CI_BANK_COM_RELATION/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_CI_BANK_COM_RELATION_BK/")

#任务[12] 001-04::
V_STEP = V_STEP + 1

ACRM_F_CI_BANK_COM_RELATION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_BANK_COM_RELATION/*')
ACRM_F_CI_BANK_COM_RELATION.registerTempTable("ACRM_F_CI_BANK_COM_RELATION")

sql = """
  SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.COOPERATION_YEAR      AS COOPERATION_YEAR 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.CREDIT_LIMIT          AS CREDIT_LIMIT 
       ,B.CASH_1                AS CASH_1 
       ,A.CASH_2                AS CASH_2 
       ,A.CASH_3                AS CASH_3 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM ACRM_F_CI_BANK_COM_RELATION A                          --
  INNER JOIN(SELECT CUST_ID,FR_ID ,
                CAST(SUM(ABS(COALESCE(SA_DR_AMT, 0) - COALESCE(SA_CR_AMT, 0))) AS DECIMAL(24,6)) AS CASH_1
           FROM  ACRM_F_CI_NIN_TRANSLOG 
          WHERE SA_TVARCHAR_DT >= V_DT_FMD
            AND SA_TVARCHAR_DT <= V_DT
            AND CUST_TYP = '2' 
          GROUP BY CUST_ID,FR_ID
         )B                          --
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID    """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT_FMD\b", "'"+V_DT_FMD+"'", sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1 = sqlContext.sql(sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.registerTempTable("ACRM_F_CI_BANK_COM_RELATION_INNTMP1")

ACRM_F_CI_BANK_COM_RELATION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_BANK_COM_RELATION/*')
ACRM_F_CI_BANK_COM_RELATION.registerTempTable("ACRM_F_CI_BANK_COM_RELATION")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.COOPERATION_YEAR                                   --合作年限:src.COOPERATION_YEAR
       ,DST.IF_BASIC                                           --是否基本户:src.IF_BASIC
       ,DST.DEP_RATIO                                          --存贷比:src.DEP_RATIO
       ,DST.CREDIT_LIMIT                                       --授信额度:src.CREDIT_LIMIT
       ,DST.CASH_1                                             --现金流前一个月:src.CASH_1
       ,DST.CASH_2                                             --现金流前两个月:src.CASH_2
       ,DST.CASH_3                                             --现金流前三个月:src.CASH_3
       ,DST.FR_ID                                              --法人ID:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM ACRM_F_CI_BANK_COM_RELATION DST 
   LEFT JOIN ACRM_F_CI_BANK_COM_RELATION_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_CI_BANK_COM_RELATION/"+V_DT+".parquet"
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.unionAll(ACRM_F_CI_BANK_COM_RELATION_INNTMP1)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.cache()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.cache()
nrowsi = ACRM_F_CI_BANK_COM_RELATION_INNTMP1.count()
nrowsa = ACRM_F_CI_BANK_COM_RELATION_INNTMP2.count()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.unpersist()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_BANK_COM_RELATION lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_CI_BANK_COM_RELATION/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_CI_BANK_COM_RELATION_BK/")

#任务[12] 001-05::
V_STEP = V_STEP + 1

ACRM_F_CI_BANK_COM_RELATION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_BANK_COM_RELATION/*')
ACRM_F_CI_BANK_COM_RELATION.registerTempTable("ACRM_F_CI_BANK_COM_RELATION")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.COOPERATION_YEAR      AS COOPERATION_YEAR 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.CREDIT_LIMIT          AS CREDIT_LIMIT 
       ,A.CASH_1                AS CASH_1 
       ,B.CASH_2                AS CASH_2 
       ,A.CASH_3                AS CASH_3 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM ACRM_F_CI_BANK_COM_RELATION A 
  INNER JOIN (SELECT CUST_ID,FR_ID,CAST(SUM(ABS(COALESCE(SA_DR_AMT, 0) - COALESCE(SA_CR_AMT, 0))) AS DECIMAL(24,6)) AS CASH_2
                FROM ACRM_F_CI_NIN_TRANSLOG
               WHERE SA_TVARCHAR_DT >= ADD_MONTHS(V_DT_FMD,-1)
                 AND SA_TVARCHAR_DT <= DATE_ADD(V_DT_FMD,-1)
                 AND CUST_TYP = '2'
               GROUP BY CUST_ID,FR_ID
                 ) B 
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT_FMD\b", "'"+V_DT_FMD+"'", sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1 = sqlContext.sql(sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.registerTempTable("ACRM_F_CI_BANK_COM_RELATION_INNTMP1")

ACRM_F_CI_BANK_COM_RELATION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_BANK_COM_RELATION/*')
ACRM_F_CI_BANK_COM_RELATION.registerTempTable("ACRM_F_CI_BANK_COM_RELATION")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.COOPERATION_YEAR                                   --合作年限:src.COOPERATION_YEAR
       ,DST.IF_BASIC                                           --是否基本户:src.IF_BASIC
       ,DST.DEP_RATIO                                          --存贷比:src.DEP_RATIO
       ,DST.CREDIT_LIMIT                                       --授信额度:src.CREDIT_LIMIT
       ,DST.CASH_1                                             --现金流前一个月:src.CASH_1
       ,DST.CASH_2                                             --现金流前两个月:src.CASH_2
       ,DST.CASH_3                                             --现金流前三个月:src.CASH_3
       ,DST.FR_ID                                              --法人ID:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM ACRM_F_CI_BANK_COM_RELATION DST 
   LEFT JOIN ACRM_F_CI_BANK_COM_RELATION_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_CI_BANK_COM_RELATION/"+V_DT+".parquet"
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.unionAll(ACRM_F_CI_BANK_COM_RELATION_INNTMP1)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.cache()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.cache()
nrowsi = ACRM_F_CI_BANK_COM_RELATION_INNTMP1.count()
nrowsa = ACRM_F_CI_BANK_COM_RELATION_INNTMP2.count()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.unpersist()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_BANK_COM_RELATION lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_CI_BANK_COM_RELATION/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_CI_BANK_COM_RELATION_BK/")

#任务[12] 001-06::
V_STEP = V_STEP + 1

ACRM_F_CI_BANK_COM_RELATION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_BANK_COM_RELATION/*')
ACRM_F_CI_BANK_COM_RELATION.registerTempTable("ACRM_F_CI_BANK_COM_RELATION")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.COOPERATION_YEAR      AS COOPERATION_YEAR 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.CREDIT_LIMIT          AS CREDIT_LIMIT 
       ,A.CASH_1                AS CASH_1 
       ,A.CASH_2                AS CASH_2 
       ,B.CASH_3                AS CASH_3 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM ACRM_F_CI_BANK_COM_RELATION A                          --
  INNER JOIN (SELECT CUST_ID,FR_ID,CAST(SUM(ABS(COALESCE(SA_DR_AMT, 0) - COALESCE(SA_CR_AMT, 0))) AS DECIMAL(24,6)) AS CASH_3
                FROM ACRM_F_CI_NIN_TRANSLOG
               WHERE SA_TVARCHAR_DT >= ADD_MONTHS(V_DT_FMD,-2)
                 AND SA_TVARCHAR_DT <= ADD_MONTHS(DATE_ADD(V_DT_FMD,-1),-1)
                 AND CUST_TYP = '2'
               GROUP BY CUST_ID,FR_ID
                 ) B                          --
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT_FMD\b", "'"+V_DT_FMD+"'", sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1 = sqlContext.sql(sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.registerTempTable("ACRM_F_CI_BANK_COM_RELATION_INNTMP1")

ACRM_F_CI_BANK_COM_RELATION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_BANK_COM_RELATION/*')
ACRM_F_CI_BANK_COM_RELATION.registerTempTable("ACRM_F_CI_BANK_COM_RELATION")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.COOPERATION_YEAR                                   --合作年限:src.COOPERATION_YEAR
       ,DST.IF_BASIC                                           --是否基本户:src.IF_BASIC
       ,DST.DEP_RATIO                                          --存贷比:src.DEP_RATIO
       ,DST.CREDIT_LIMIT                                       --授信额度:src.CREDIT_LIMIT
       ,DST.CASH_1                                             --现金流前一个月:src.CASH_1
       ,DST.CASH_2                                             --现金流前两个月:src.CASH_2
       ,DST.CASH_3                                             --现金流前三个月:src.CASH_3
       ,DST.FR_ID                                              --法人ID:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM ACRM_F_CI_BANK_COM_RELATION DST 
   LEFT JOIN ACRM_F_CI_BANK_COM_RELATION_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_BANK_COM_RELATION_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_CI_BANK_COM_RELATION/"+V_DT+".parquet"
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.unionAll(ACRM_F_CI_BANK_COM_RELATION_INNTMP1)
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.cache()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.cache()
nrowsi = ACRM_F_CI_BANK_COM_RELATION_INNTMP1.count()
nrowsa = ACRM_F_CI_BANK_COM_RELATION_INNTMP2.count()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_BANK_COM_RELATION_INNTMP1.unpersist()
ACRM_F_CI_BANK_COM_RELATION_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_BANK_COM_RELATION lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_CI_BANK_COM_RELATION/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_CI_BANK_COM_RELATION_BK/")
