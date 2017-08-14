#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CUST_ASSIGN_AUM').setMaster(sys.argv[2])
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
CUSTOMER_BELONG_TMP = sqlContext.read.parquet(hdfs+'/CUSTOMER_BELONG_TMP/*')
CUSTOMER_BELONG_TMP.registerTempTable("CUSTOMER_BELONG_TMP")
ADMIN_AUTH_ACCOUNT_COMPARE = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ACCOUNT_COMPARE/*')
ADMIN_AUTH_ACCOUNT_COMPARE.registerTempTable("ADMIN_AUTH_ACCOUNT_COMPARE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,CAST(SUM(A.MONTH_RMB) AS DECIMAL(24,6))                        AS VALUE 
       ,CAST(0 AS DECIMAL(24,6))                    AS COUNT 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(ROW_NUMBER() OVER(
      PARTITION BY A.CUST_ID 
               ,A.FR_ID 
          ORDER BY SUM(A.MONTH_RMB) DESC) AS DECIMAL(10))                       AS RANK 
   FROM ACRM_F_DP_SAVE_INFO  A
  INNER JOIN CUSTOMER_BELONG_TMP B                             --待分配客户表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.ODS_CUSTOM_ID 
  WHERE A.ACCT_STATUS           = '01' 
  GROUP BY A.CUST_ID 
       ,A.ORG_ID 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_CUST_AUM_VALUE = sqlContext.sql(sql)
OCRM_CUST_AUM_VALUE.registerTempTable("OCRM_CUST_AUM_VALUE")
dfn="OCRM_CUST_AUM_VALUE/"+V_DT+".parquet"
OCRM_CUST_AUM_VALUE.cache()
nrows = OCRM_CUST_AUM_VALUE.count()
OCRM_CUST_AUM_VALUE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_CUST_AUM_VALUE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_CUST_AUM_VALUE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_CUST_AUM_VALUE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CONNTR_NO             AS CUST_MGR 
       ,CAST(SUM(A.MONTH_RMB)   AS DECIMAL(24,6))                      AS VALUE 
       ,CAST(0 AS DECIMAL(24,6))                    AS COUNT 
       ,A.FR_ID                 AS FR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,CAST(ROW_NUMBER() OVER(
      PARTITION BY A.CUST_ID 
               ,A.CONNTR_NO 
               ,A.FR_ID 
          ORDER BY SUM(A.MONTH_RMB) DESC) AS DECIMAL(10))                       AS RANK 
   FROM ACRM_F_DP_SAVE_INFO A
  INNER JOIN CUSTOMER_BELONG_TMP B                             --待分配客户表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.ODS_CUSTOM_ID 
  WHERE A.ACCT_STATUS           = '01' 
    AND A.BAL_RMB > 0 
  GROUP BY A.CUST_ID 
       ,A.ORG_ID 
       ,A.CONNTR_NO 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_CUST_AUM_VALUE1 = sqlContext.sql(sql)
OCRM_CUST_AUM_VALUE1.registerTempTable("OCRM_CUST_AUM_VALUE1")
dfn="OCRM_CUST_AUM_VALUE1/"+V_DT+".parquet"
OCRM_CUST_AUM_VALUE1.cache()
nrows = OCRM_CUST_AUM_VALUE1.count()
OCRM_CUST_AUM_VALUE1.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_CUST_AUM_VALUE1.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_CUST_AUM_VALUE1/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_CUST_AUM_VALUE1 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-03::
V_STEP = V_STEP + 1

OCRM_CUST_AUM_VALUE1 = sqlContext.read.parquet(hdfs+'/OCRM_CUST_AUM_VALUE1/*')
OCRM_CUST_AUM_VALUE1.registerTempTable("OCRM_CUST_AUM_VALUE1")

sql = """
 SELECT C.CUST_ID               AS CUST_ID 
       ,A.CUST_MGR                AS CUST_MGR 
       ,CAST(-1 AS DECIMAL(24,6))                       AS VALUE 
       ,C.COUNT                 AS COUNT 
       ,A.FR_ID                   AS FR_ID 
       ,C.ORG_ID                AS ORG_ID 
       ,C.RANK                  AS RANK 
   FROM(
         SELECT DISTINCT FR_ID 
               ,CUST_MGR 
           FROM OCRM_CUST_AUM_VALUE1) A                        --客户-经理AUM
   LEFT JOIN ADMIN_AUTH_ACCOUNT_COMPARE B                      --源系统操作号对照表
     ON A.CUST_MGR              = B.SOURCE_SYS_USER_ID 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN OCRM_CUST_AUM_VALUE1 C                            --客户-经理存款AUM
     ON A.FR_ID                 = C.FR_ID 
    AND A.CUST_MGR              = C.CUST_MGR 
  WHERE B.FR_ID IS NULL 
     OR A.CUST_MGR IS NULL 
     OR TRIM(A.CUST_MGR)                       = '' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_CUST_AUM_VALUE1_INNTMP1 = sqlContext.sql(sql)
OCRM_CUST_AUM_VALUE1_INNTMP1.registerTempTable("OCRM_CUST_AUM_VALUE1_INNTMP1")

OCRM_CUST_AUM_VALUE1 = sqlContext.read.parquet(hdfs+'/OCRM_CUST_AUM_VALUE1/*')
OCRM_CUST_AUM_VALUE1.registerTempTable("OCRM_CUST_AUM_VALUE1")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_MGR                                           --经理号:src.CUST_MGR
       ,DST.VALUE                                              --AUM:src.VALUE
       ,DST.COUNT                                              --数量:src.COUNT
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.RANK                                               --序号:src.RANK
   FROM OCRM_CUST_AUM_VALUE1 DST 
   LEFT JOIN OCRM_CUST_AUM_VALUE1_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_MGR            = DST.CUST_MGR 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_CUST_AUM_VALUE1_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_CUST_AUM_VALUE1/"+V_DT+".parquet"
OCRM_CUST_AUM_VALUE1_INNTMP2=OCRM_CUST_AUM_VALUE1_INNTMP2.unionAll(OCRM_CUST_AUM_VALUE1_INNTMP1)
OCRM_CUST_AUM_VALUE1_INNTMP1.cache()
OCRM_CUST_AUM_VALUE1_INNTMP2.cache()
nrowsi = OCRM_CUST_AUM_VALUE1_INNTMP1.count()
nrowsa = OCRM_CUST_AUM_VALUE1_INNTMP2.count()
OCRM_CUST_AUM_VALUE1_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_CUST_AUM_VALUE1_INNTMP1.unpersist()
OCRM_CUST_AUM_VALUE1_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_CUST_AUM_VALUE1 lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_CUST_AUM_VALUE1/"+V_DT_LD+".parquet /"+dbname+"/OCRM_CUST_AUM_VALUE1_BK/")

#任务[21] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.MANAGE_BRAN           AS ORG_ID 
       ,ROW_NUMBER() OVER(
      PARTITION BY A.CUST_ID 
          ORDER BY SUM(A.BAL) DESC)                       AS RN 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN CUSTOMER_BELONG_TMP B                             --待分配客户表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.ODS_CUSTOM_ID 
  WHERE A.BAL > 0 
    AND A.LN_APCL_FLG           = 'N' 
    AND A.FR_ID                 = '095' 
    AND(A.SUBJECTNO LIKE '1301%' 
             OR A.SUBJECTNO LIKE '1302%' 
             OR A.SUBJECTNO LIKE '1303%' 
             OR A.SUBJECTNO LIKE '1304%' 
             OR A.SUBJECTNO LIKE '1305%' 
             OR A.SUBJECTNO LIKE '1306%' 
             OR A.SUBJECTNO LIKE '1307%' 
             OR A.SUBJECTNO LIKE '1308%') 
    AND A.FR_ID                 = '095' 
  GROUP BY A.CUST_ID 
       ,A.MANAGE_BRAN 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_CUST_LOAN_AUM = sqlContext.sql(sql)
OCRM_CUST_LOAN_AUM.registerTempTable("OCRM_CUST_LOAN_AUM")
dfn="OCRM_CUST_LOAN_AUM/"+V_DT+".parquet"
OCRM_CUST_LOAN_AUM.cache()
nrows = OCRM_CUST_LOAN_AUM.count()
OCRM_CUST_LOAN_AUM.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_CUST_LOAN_AUM.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_CUST_LOAN_AUM/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_CUST_LOAN_AUM lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
