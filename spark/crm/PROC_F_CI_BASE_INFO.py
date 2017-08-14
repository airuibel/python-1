#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_BASE_INFO').setMaster(sys.argv[2])
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
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
ACRM_F_RE_AUMSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_AUMSUMAVGINFO/*')
ACRM_F_RE_AUMSUMAVGINFO.registerTempTable("ACRM_F_RE_AUMSUMAVGINFO")
ACRM_A_CI_CUST_PROD= sqlContext.read.parquet(hdfs+'/ACRM_A_CI_CUST_PROD/*')
ACRM_A_CI_CUST_PROD.registerTempTable("ACRM_A_CI_CUST_PROD")
OCRM_F_CI_RELATE_CUST_BASE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_RELATE_CUST_BASE/*')
OCRM_F_CI_RELATE_CUST_BASE.registerTempTable("OCRM_F_CI_RELATE_CUST_BASE")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
OCRM_F_CI_BASE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE/*')
OCRM_F_CI_BASE.registerTempTable("OCRM_F_CI_BASE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()  AS ID 
       ,CAST(A.CUST_BASE_ID AS DECIMAL(18,2))         AS CUST_BASE_NUMBER 
       ,''                      AS GROUP_TYPE 
       ,CAST(ROUND(AVG(FLOAT(COALESCE((LENGTH(regexp_replace(SUBSTR(B.HOLD_PRO_FLAG, 1, 9), '0', '')) +(CASE WHEN SUBSTR(B.HOLD_PRO_FLAG, 24, 1) = '1' THEN 1 ELSE 0 END)), 0))), 2) AS DECIMAL(18,2)) AS QDCOUNT_BASE 
       ,CAST(0 AS DECIMAL(18,2))                      AS PRODCOUNT_BASE 
       ,CAST(0 AS DECIMAL(18,2))                      AS SAVEAVG_BASE 
       ,CAST(0 AS DECIMAL(18,2))                      AS LOANAVG_BASE 
       ,CAST(0 AS DECIMAL(18,2))                      AS AUMAVG_BASE 
       ,CAST(0 AS DECIMAL(18,2))                      AS QDCOUNT_FR 
       ,CAST(0 AS DECIMAL(18,2))                      AS PRODCOUNT_FR 
       ,CAST(0 AS DECIMAL(18,2))                      AS SAVEAVG_FR 
       ,CAST(0 AS DECIMAL(18,2))                      AS LOANAVG_FR 
       ,CAST(0 AS DECIMAL(18,2))                      AS AUMAVG_FR 
       ,''                      AS BELONG_MGR 
       ,''                      AS BELONG_ORG 
       ,V_DT               AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_RELATE_CUST_BASE A                           --
  INNER JOIN OCRM_F_CI_CUST_DESC B                             --
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  GROUP BY A.CUST_BASE_ID 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO.cache()
nrows = OCRM_F_CI_BASE_INFO.count()
OCRM_F_CI_BASE_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,CAST(A.CUST_BASE_NUMBER AS DECIMAL(18,2))      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,CAST(A.QDCOUNT_BASE AS DECIMAL(18,2))          AS QDCOUNT_BASE 
       ,CAST(B.PROD_NUM AS DECIMAL(18,2))              AS PRODCOUNT_BASE 
       ,CAST(A.SAVEAVG_BASE AS DECIMAL(18,2))          AS SAVEAVG_BASE 
       ,CAST(A.LOANAVG_BASE AS DECIMAL(18,2))          AS LOANAVG_BASE 
       ,CAST(A.AUMAVG_BASE AS DECIMAL(18,2))           AS AUMAVG_BASE 
       ,CAST(A.QDCOUNT_FR AS DECIMAL(18,2))            AS QDCOUNT_FR 
       ,CAST(A.PRODCOUNT_FR AS DECIMAL(18,2))          AS PRODCOUNT_FR 
       ,CAST(A.SAVEAVG_FR AS DECIMAL(18,2))            AS SAVEAVG_FR 
       ,CAST(A.LOANAVG_FR AS DECIMAL(18,2))            AS LOANAVG_FR 
       ,CAST(A.AUMAVG_FR AS DECIMAL(18,2))             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT CUST_BASE_ID,CAST(ROUND(AVG(FLOAT(COALESCE(PROD_NUM,0))),2) AS DECIMAL(18,2)) AS PROD_NUM,FR_ID
                 FROM (
                        SELECT A.CUST_ID,A.CUST_BASE_ID,COUNT(1) AS PROD_NUM ,A.FR_ID
                          FROM OCRM_F_CI_RELATE_CUST_BASE A
                          JOIN ACRM_A_CI_CUST_PROD B ON A.CUST_ID = B.CUST_ID AND A.FR_ID = B.FR_ID
                          WHERE  (TYPE = 'DK' OR TYPE = 'CK') 
                          GROUP BY A.CUST_ID,A.CUST_BASE_ID,A.FR_ID )
             GROUP BY CUST_BASE_ID,FR_ID) B                                                 --
     ON A.CUST_BASE_NUMBER      = B.CUST_BASE_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.CUST_BASE_NUMBER    = DST.CUST_BASE_NUMBER 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_BASE_NUMBER IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-03::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,B.SAVEAVG               AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT CUST_BASE_ID,ROUND(AVG(COALESCE(MONTH_AVG,0)),2) AS SAVEAVG,FR_ID
                FROM 
                  (
                    SELECT A.CUST_BASE_ID,B.CUST_ID,SUM(COALESCE(B.MONTH_AVG,0)) AS MONTH_AVG,A.FR_ID
                        FROM OCRM_F_CI_RELATE_CUST_BASE A
                        LEFT JOIN ACRM_F_DP_SAVE_INFO B ON A.CUST_ID = B.CUST_ID AND A.FR_ID = B.FR_ID AND B.ACCT_STATUS = '01'
                GROUP BY A.CUST_BASE_ID,B.CUST_ID ,A.FR_ID
              )
          GROUP BY CUST_BASE_ID,FR_ID) B                                                 --
     ON A.CUST_BASE_NUMBER      = B.CUST_BASE_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.CUST_BASE_NUMBER    = DST.CUST_BASE_NUMBER 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_BASE_NUMBER IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-04::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,B.LOANAVG               AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT CUST_BASE_ID,ROUND(AVG(COALESCE(MONTH_AVG,0)),2) AS LOANAVG,FR_ID
                FROM (
                      SELECT A.CUST_BASE_ID,B.CUST_ID,SUM(COALESCE(B.MONTH_AVG,0)) AS MONTH_AVG,A.FR_ID
                           FROM OCRM_F_CI_RELATE_CUST_BASE A
                     LEFT JOIN ACRM_F_CI_ASSET_BUSI_PROTO B 
                             ON A.CUST_ID = B.CUST_ID AND A.FR_ID=B.FR_ID AND B.LN_APCL_FLG = 'N' 
               GROUP BY A.CUST_BASE_ID,B.CUST_ID ,A.FR_ID
               )
           GROUP BY CUST_BASE_ID,FR_ID) B                                                 --
     ON A.CUST_BASE_NUMBER      = B.CUST_BASE_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.CUST_BASE_NUMBER    = DST.CUST_BASE_NUMBER 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_BASE_NUMBER IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-05::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,B.AUMAVG                AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT CUST_BASE_ID,ROUND(AVG(COALESCE(MONTH_AVG,0)),2) AS AUMAVG,FR_ID
               FROM (
                 SELECT  A.CUST_BASE_ID,B.CUST_ID,SUM(COALESCE(B.MONTH_SUM,0)/B.MONTH_DAYS) AS MONTH_AVG,A.FR_ID
                    FROM  OCRM_F_CI_RELATE_CUST_BASE  A
              LEFT JOIN  ACRM_F_RE_AUMSUMAVGINFO B
                     ON   A.CUST_ID = B.CUST_ID AND A.FR_ID=B.FR_ID
              GROUP  BY  A.CUST_BASE_ID,B.CUST_ID,A.FR_ID)
            GROUP BY CUST_BASE_ID,FR_ID) B                                                 --
     ON A.CUST_BASE_NUMBER      = B.CUST_BASE_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.CUST_BASE_NUMBER    = DST.CUST_BASE_NUMBER 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_BASE_NUMBER IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-06::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,B.GROUP_MEMBER_TYPE     AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN OCRM_F_CI_BASE B                                  --
     ON A.CUST_BASE_NUMBER      = B.ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.CUST_BASE_NUMBER    = DST.CUST_BASE_NUMBER 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_BASE_NUMBER IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-07::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,B.QDCOUNT AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN (SELECT FR_ID,CUST_TYP,CAST(ROUND(AVG(FLOAT(COALESCE(CHANNEL_NUM,0))),2) AS DECIMAL(18,2)) AS QDCOUNT 
                 FROM 
                     (
                         SELECT  A.CUST_ID,
                                 A.CUST_TYP,
                                 A.FR_ID,
                                (CASE WHEN SUBSTR (A.HOLD_PRO_FLAG, 5, 1) = '1' THEN 1 ELSE 0 END)
                              + (CASE WHEN SUBSTR (A.HOLD_PRO_FLAG, 6, 1) = '1' THEN 1 ELSE 0  END)
                              + (CASE WHEN SUBSTR (A.HOLD_PRO_FLAG, 7, 1) = '1' THEN 1 ELSE 0 END)
                              + (CASE WHEN SUBSTR (A.HOLD_PRO_FLAG, 24,1) = '1' THEN 1 ELSE 0  END )
                              + (CASE WHEN SUBSTR (A.HOLD_PRO_FLAG, 1, 1) = '1' THEN 1 ELSE 0 END)
                              + (CASE WHEN SUBSTR (A.HOLD_PRO_FLAG, 8, 1) = '1' THEN 1 ELSE 0 END)
                              + (CASE WHEN SUBSTR (A.HOLD_PRO_FLAG, 9, 1) = '1' THEN 1 ELSE 0 END)
                              + (CASE WHEN SUBSTR (A.HOLD_PRO_FLAG, 4, 1) = '1' THEN 1 ELSE 0 END)
                              + (CASE WHEN SUBSTR (A.HOLD_PRO_FLAG, 2, 1) = '1' THEN 1 ELSE 0  END)
                              + (CASE WHEN SUBSTR (A.HOLD_PRO_FLAG, 3, 1) = '1' THEN 1 ELSE 0  END)
                              AS CHANNEL_NUM
                           FROM  OCRM_F_CI_CUST_DESC A
                       )
        GROUP  BY  FR_ID,CUST_TYP) B                             --
     ON A.GROUP_TYPE            = B.CUST_TYP 
    AND A.FR_ID                 = B.FR_ID 
    AND A.ETL_DATE = V_DT"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.GROUP_TYPE          = DST.GROUP_TYPE 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_TYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-08::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,B.PROD_NUM              AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT   CUST_TYP,
                      ROUND(AVG(FLOAT(COALESCE(PROD_NUM,0))),2) AS PROD_NUM,FR_ID
                 FROM 
                      (
                        SELECT A.CUST_ID,B.CUST_TYP,COUNT(1) AS PROD_NUM,A.FR_ID
                            FROM
                                 ACRM_A_CI_CUST_PROD A
                            JOIN OCRM_F_CI_CUST_DESC B ON A.CUST_ID = B.CUST_ID AND A.FR_ID = B.FR_ID
                          WHERE (A.TYPE = 'DK' OR A.TYPE = 'CK')
                       GROUP BY A.CUST_ID,B.CUST_TYP,A.FR_ID
                    )
             GROUP BY CUST_TYP,FR_ID) B                                                 --
     ON A.GROUP_TYPE            = B.CUST_TYP 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.GROUP_TYPE          = DST.GROUP_TYPE 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_TYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-09::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,B.SAVEAVG               AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT CUST_TYP,
                   ROUND(AVG(COALESCE(MONTH_AVG,0)),2) AS SAVEAVG,FR_ID
                FROM 
                  (
                    SELECT CUST_ID,CUST_TYP,CAST(SUM(COALESCE(MONTH_AVG,0)) AS DECIMAL(18,2)) AS MONTH_AVG,FR_ID
                       FROM ACRM_F_DP_SAVE_INFO B                                   
                      WHERE B.ACCT_STATUS = '01'
                    GROUP BY FR_ID,CUST_ID,CUST_TYP
              )
           GROUP BY CUST_TYP,FR_ID) B                                                 --
     ON A.GROUP_TYPE            = B.CUST_TYP 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.GROUP_TYPE          = DST.GROUP_TYPE 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_TYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-10::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,B.LOANAVG               AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT  CUST_TYP,CAST(ROUND(AVG(COALESCE(MONTH_AVG,0)),2) AS DECIMAL(18,2)) AS LOANAVG,FR_ID
                FROM (
                        SELECT  CUST_ID,CUST_TYP,SUM(COALESCE(MONTH_AVG,0)) AS MONTH_AVG,FR_ID                       
                          FROM  ACRM_F_CI_ASSET_BUSI_PROTO B 
                        WHERE   B.LN_APCL_FLG = 'N'                        
                        GROUP BY CUST_ID,CUST_TYP,FR_ID
               )
               GROUP BY CUST_TYP,FR_ID
) B                                                 --
     ON A.GROUP_TYPE            = B.CUST_TYP 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.GROUP_TYPE          = DST.GROUP_TYPE 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_TYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-11::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,B.AUMAVG                AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT  CUST_TYP,CAST(ROUND(AVG(COALESCE(MONTH_AVG,0)),2)AS DECIMAL(18,2)) AS AUMAVG,FR_ID
                FROM (
                        SELECT  B.CUST_ID,C.CUST_TYP,SUM(COALESCE(B.MONTH_SUM,0)/B.MONTH_DAYS) AS MONTH_AVG,B.FR_ID
                          FROM  ACRM_F_RE_AUMSUMAVGINFO B
                          JOIN  OCRM_F_CI_CUST_DESC C ON B.FR_ID = C.FR_ID AND B.CUST_ID = C.CUST_ID
                        GROUP BY B.CUST_ID,C.CUST_TYP,B.FR_ID
               )
               GROUP BY CUST_TYP,FR_ID) B                                                 --
     ON A.GROUP_TYPE            = B.CUST_TYP 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.GROUP_TYPE          = DST.GROUP_TYPE 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_TYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-12::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,B.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN (SELECT FR_ID,
                     CAST(ROUND(AVG(FLOAT(COALESCE((LENGTH(regexp_replace(SUBSTR(B.HOLD_PRO_FLAG, 1, 9), '0', '')) +
                          (CASE WHEN SUBSTR(B.HOLD_PRO_FLAG, 24, 1) = '1' THEN 1 ELSE 0 END)), 0))), 2) AS DECIMAL(18,2)) AS QDCOUNT_FR   
                FROM OCRM_F_CI_CUST_DESC B
               GROUP BY FR_ID ) B
     ON A.FR_ID                 = B.FR_ID 
    AND A.GROUP_TYPE            = '3' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.GROUP_TYPE          = DST.GROUP_TYPE 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_TYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-13::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,B.PROD_NUM              AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT  CAST(ROUND(AVG(FLOAT(COALESCE(PROD_NUM,0))),2) AS DECIMAL(18,2)) AS PROD_NUM,FR_ID
                 FROM 
                      (
                        SELECT CUST_ID,COUNT(1) AS PROD_NUM,FR_ID
                        FROM   ACRM_A_CI_CUST_PROD A
                        WHERE (A.TYPE = 'DK' OR A.TYPE = 'CK')
                        GROUP BY CUST_ID,FR_ID
                    )
                   GROUP BY FR_ID
) B                                                 --
     ON A.FR_ID                 = B.FR_ID 
    AND A.GROUP_TYPE            = '3' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.GROUP_TYPE          = DST.GROUP_TYPE 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_TYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-14::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,B.SAVEAVG               AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT CAST(ROUND(AVG(COALESCE(MONTH_AVG,0)),2) AS DECIMAL(18,2)) AS SAVEAVG,FR_ID
                FROM 
                  (
                    SELECT CUST_ID,SUM(COALESCE(MONTH_AVG,0)) AS MONTH_AVG,FR_ID
                       FROM ACRM_F_DP_SAVE_INFO B                                   
                      WHERE B.ACCT_STATUS = '01'
                    GROUP BY CUST_ID,FR_ID
              )  
              GROUP BY FR_ID
) B                                                 --
     ON A.FR_ID                 = B.FR_ID 
    AND A.GROUP_TYPE            = '3' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.GROUP_TYPE          = DST.GROUP_TYPE 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_TYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-15::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,B.LOANAVG               AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT  CAST(ROUND(AVG(COALESCE(MONTH_AVG,0)),2) AS DECIMAL(18,2)) AS LOANAVG,FR_ID
                FROM (
                        SELECT  CUST_ID,SUM(COALESCE(MONTH_AVG,0)) AS MONTH_AVG ,FR_ID                      
                          FROM  ACRM_F_CI_ASSET_BUSI_PROTO B 
                        WHERE B.LN_APCL_FLG = 'N'                        
                        GROUP BY CUST_ID,FR_ID
               )GROUP BY FR_ID
) B                                                 --
     ON A.FR_ID                 = B.FR_ID 
    AND A.GROUP_TYPE            = '3' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.GROUP_TYPE          = DST.GROUP_TYPE 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_TYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-16::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,B.AUMAVG                AS AUMAVG_FR 
       ,A.BELONG_MGR            AS BELONG_MGR 
       ,A.BELONG_ORG            AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN 
 (SELECT  CAST(ROUND(AVG(COALESCE(MONTH_AVG,0)),2) AS DECIMAL(18,2)) AS AUMAVG,FR_ID
                FROM (
                        SELECT  CUST_ID,SUM(COALESCE(MONTH_AVG,0)) AS MONTH_AVG ,FR_ID                      
                          FROM  ACRM_F_CI_ASSET_BUSI_PROTO B 
                        WHERE B.LN_APCL_FLG = 'N'                        
                        GROUP BY CUST_ID,FR_ID
               )GROUP BY FR_ID
) B                                                 --
     ON A.FR_ID                 = B.FR_ID 
    AND A.GROUP_TYPE            = '3' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.GROUP_TYPE          = DST.GROUP_TYPE 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_TYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")

#任务[12] 001-17::
V_STEP = V_STEP + 1

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_BASE_NUMBER      AS CUST_BASE_NUMBER 
       ,A.GROUP_TYPE            AS GROUP_TYPE 
       ,A.QDCOUNT_BASE          AS QDCOUNT_BASE 
       ,A.PRODCOUNT_BASE        AS PRODCOUNT_BASE 
       ,A.SAVEAVG_BASE          AS SAVEAVG_BASE 
       ,A.LOANAVG_BASE          AS LOANAVG_BASE 
       ,A.AUMAVG_BASE           AS AUMAVG_BASE 
       ,A.QDCOUNT_FR            AS QDCOUNT_FR 
       ,A.PRODCOUNT_FR          AS PRODCOUNT_FR 
       ,A.SAVEAVG_FR            AS SAVEAVG_FR 
       ,A.LOANAVG_FR            AS LOANAVG_FR 
       ,A.AUMAVG_FR             AS AUMAVG_FR 
       ,B.CUST_BASE_CREATE_NAME AS BELONG_MGR 
       ,B.CUST_BASE_CREATE_ORG  AS BELONG_ORG 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BASE_INFO A                                  --
  INNER JOIN OCRM_F_CI_BASE B                                  --
     ON A.CUST_BASE_NUMBER      = B.ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BASE_INFO_INNTMP1.registerTempTable("OCRM_F_CI_BASE_INFO_INNTMP1")

OCRM_F_CI_BASE_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_INFO/*')
OCRM_F_CI_BASE_INFO.registerTempTable("OCRM_F_CI_BASE_INFO")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_BASE_NUMBER                                   --客户群ID:src.CUST_BASE_NUMBER
       ,DST.GROUP_TYPE                                         --客户群类型:src.GROUP_TYPE
       ,DST.QDCOUNT_BASE                                       --:src.QDCOUNT_BASE
       ,DST.PRODCOUNT_BASE                                     --:src.PRODCOUNT_BASE
       ,DST.SAVEAVG_BASE                                       --客户群存款月日均均值:src.SAVEAVG_BASE
       ,DST.LOANAVG_BASE                                       --客户群贷款余额月日均均值:src.LOANAVG_BASE
       ,DST.AUMAVG_BASE                                        --客户群AUM月日均均值:src.AUMAVG_BASE
       ,DST.QDCOUNT_FR                                         --全行渠道开通个数均值:src.QDCOUNT_FR
       ,DST.PRODCOUNT_FR                                       --全行持有产品个数均值:src.PRODCOUNT_FR
       ,DST.SAVEAVG_FR                                         --全行存款月日均均值:src.SAVEAVG_FR
       ,DST.LOANAVG_FR                                         --全行贷款月日均均值:src.LOANAVG_FR
       ,DST.AUMAVG_FR                                          --全行AUM月日均均值:src.AUMAVG_FR
       ,DST.BELONG_MGR                                         --归属客户经理:src.BELONG_MGR
       ,DST.BELONG_ORG                                         --归属机构:src.BELONG_ORG
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BASE_INFO DST 
   LEFT JOIN OCRM_F_CI_BASE_INFO_INNTMP1 SRC 
     ON SRC.CUST_BASE_NUMBER    = DST.CUST_BASE_NUMBER 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_BASE_NUMBER IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BASE_INFO/"+V_DT+".parquet"
OCRM_F_CI_BASE_INFO_INNTMP2.unionAll(OCRM_F_CI_BASE_INFO_INNTMP1)
OCRM_F_CI_BASE_INFO_INNTMP1.cache()
OCRM_F_CI_BASE_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_BASE_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_BASE_INFO_INNTMP2.count()
OCRM_F_CI_BASE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_INFO_INNTMP1.unpersist()
OCRM_F_CI_BASE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BASE_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BASE_INFO_BK/")
