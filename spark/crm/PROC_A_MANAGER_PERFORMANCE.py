#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_MANAGER_PERFORMANCE').setMaster(sys.argv[2])
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
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
ACRM_F_RE_AUMSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_AUMSUMAVGINFO/*')
ACRM_F_RE_AUMSUMAVGINFO.registerTempTable("ACRM_F_RE_AUMSUMAVGINFO")
TMP_VALUABAL_CUST_AUM = sqlContext.read.parquet(hdfs+'/TMP_VALUABAL_CUST_AUM_BK/'+V_DT_LD+'.parquet/*')
TMP_VALUABAL_CUST_AUM.registerTempTable("TMP_VALUABAL_CUST_AUM")
OCRM_F_CI_CUSTLNAINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUSTLNAINFO/*')
OCRM_F_CI_CUSTLNAINFO.registerTempTable("OCRM_F_CI_CUSTLNAINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_TYPE               AS CUST_TYPE 
       ,AUM_BAL                 AS AUM_BAL 
       ,IS_CREDIT               AS IS_CREDIT 
       ,MGR_ID                  AS MGR_ID 
       ,ORG_ID                  AS ORG_ID 
       ,ETL_DATE                AS ETL_DATE 
       ,FR_ID                   AS FR_ID 
   FROM TMP_VALUABAL_CUST_AUM A                                --
  WHERE DATE(ETL_DATE) >= DATE_ADD(TRUNC(V_DT, 'YY'),-1) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_VALUABAL_CUST_AUM_1 = sqlContext.sql(sql)
TMP_VALUABAL_CUST_AUM_1.registerTempTable("TMP_VALUABAL_CUST_AUM_1")
dfn="TMP_VALUABAL_CUST_AUM_1/"+V_DT+".parquet"
TMP_VALUABAL_CUST_AUM_1.cache()
nrows = TMP_VALUABAL_CUST_AUM_1.count()
TMP_VALUABAL_CUST_AUM_1.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_VALUABAL_CUST_AUM_1.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_VALUABAL_CUST_AUM_1/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_VALUABAL_CUST_AUM_1 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_TYPE               AS CUST_TYPE 
       ,AUM_BAL                 AS AUM_BAL 
       ,IS_CREDIT               AS IS_CREDIT 
       ,MGR_ID                  AS MGR_ID 
       ,ORG_ID                  AS ORG_ID 
       ,ETL_DATE                AS ETL_DATE 
       ,FR_ID                   AS FR_ID 
   FROM TMP_VALUABAL_CUST_AUM_1 A                              --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_VALUABAL_CUST_AUM = sqlContext.sql(sql)
TMP_VALUABAL_CUST_AUM.registerTempTable("TMP_VALUABAL_CUST_AUM")
dfn="TMP_VALUABAL_CUST_AUM/"+V_DT+".parquet"
TMP_VALUABAL_CUST_AUM.cache()
nrows = TMP_VALUABAL_CUST_AUM.count()
TMP_VALUABAL_CUST_AUM.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_VALUABAL_CUST_AUM.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_VALUABAL_CUST_AUM/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_VALUABAL_CUST_AUM lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id() AS DECIMAL(27))        AS ID 
       ,CAST(SUM(A.MONTH_SUM / A.MONTH_DAYS) / 10000  AS DECIMAL(22,6))    AS AUM_MONTH 
       ,CAST(SUM(A.YEAR_SUM / A.YEAR_DAYS) / 10000  AS DECIMAL(22,6))    AS AUM_YEAR 
       ,CAST(''   AS DECIMAL(22,6)) AS LOAN_BALANCE
       ,CAST(''  AS DECIMAL(22))                  AS LAST_YEAR_VAL_CUST_NUM 
       ,CAST('' AS DECIMAL(22))                   AS LAST_MONTH_VAL_CUST_NUM 
       ,CAST('' AS DECIMAL(22))                   AS CURR_VAL_CUST_NUM 
       ,CAST('' AS DECIMAL(22))                   AS NEW_YEAR_VAL_CUST_NUM 
       ,CAST('' AS DECIMAL(22))                   AS NEW_MONTH_VAL_CUST_NUM 
       ,CAST('' AS DECIMAL(22))                   AS LAST_MONTH_CREDIT_CARD_NUM 
       ,CAST('' AS DECIMAL(22))                   AS CURR_CREDIT_CARD_NUM 
       ,CAST('' AS DECIMAL(22))                   AS NEW_CREDIT_CARD_NUM 
       ,CAST(''  AS DECIMAL(22))                  AS LAST_YEAR_CUST_NUM 
       ,CAST(''  AS DECIMAL(22))                  AS LAST_MONTH_CUST_NUM 
       ,CAST(''    AS DECIMAL(22))                AS CURR_CUST_NUM 
       ,CAST(''      AS DECIMAL(22,4))              AS CUST_ADD_RATE_MONTH 
       ,CAST(''   AS DECIMAL(22,4))                  AS CUST_ADD_RATE_YEAR 
       ,CAST(''  AS DECIMAL(22))                   AS LAST_MONTH_CREDIT_CUST_NUM 
       ,CAST('' AS DECIMAL(22))                   AS CURR_CREDIT_CUST_NUM 
       ,CAST('' AS DECIMAL(22))                    AS NEW_CREDIT_CUST_NUM 
       ,B.MGR_ID                AS MGR_ID 
       ,B.INSTITUTION           AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT               AS ETL_DATE 
   FROM ACRM_F_RE_AUMSUMAVGINFO A                              --客户AUM积数均值表
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --OCRM_F_CI_BELONG_CUSTMGR
     ON A.CUST_ID               = B.CUST_ID 
    AND B.MAIN_TYPE             = '1' 
    AND A.FR_ID                 = B.FR_ID 
  GROUP BY B.MGR_ID 
       ,B.INSTITUTION 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
OCRM_F_CI_MANAGER_PERFORMANCE.cache()
nrows = OCRM_F_CI_MANAGER_PERFORMANCE.count()
OCRM_F_CI_MANAGER_PERFORMANCE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-04::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,CAST(SUM(NVL(C.BAL_RMB, 0)) / 10000    AS DECIMAL(22,6))               AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --OCRM_F_CI_BELONG_CUSTMGR
     ON A.MGR_ID                = B.MGR_ID 
    AND A.ORG_ID                = B.INSTITUTION 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO C                      --资产协议表
     ON B.CUST_ID               = C.CUST_ID 
    AND B.FR_ID                 = C.FR_ID 
    AND C.LN_APCL_FLG           = 'N' 
    AND C.BAL > 0 
  WHERE A.ETL_DATE              = V_DT 
  GROUP BY A.ID
       ,A.AUM_MONTH
       ,A.AUM_YEAR
       --,A.LOAN_BALANCE
       ,A.LAST_YEAR_VAL_CUST_NUM
       ,A.LAST_MONTH_VAL_CUST_NUM
       ,A.CURR_VAL_CUST_NUM
       ,A.NEW_YEAR_VAL_CUST_NUM
       ,A.NEW_MONTH_VAL_CUST_NUM
       ,A.LAST_MONTH_CREDIT_CARD_NUM
       ,A.CURR_CREDIT_CARD_NUM
       ,A.NEW_CREDIT_CARD_NUM
       ,A.LAST_YEAR_CUST_NUM
       ,A.LAST_MONTH_CUST_NUM
       ,A.CURR_CUST_NUM
       ,A.CUST_ADD_RATE_MONTH
       ,A.CUST_ADD_RATE_YEAR
       ,A.LAST_MONTH_CREDIT_CUST_NUM
       ,A.CURR_CREDIT_CUST_NUM
       ,A.NEW_CREDIT_CUST_NUM
       ,A.MGR_ID
       ,A.ORG_ID
       ,A.FR_ID
       ,A.ETL_DATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")

sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[11] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,'2'                     AS CUST_TYPE 
       ,CAST('' AS DECIMAL(22,2) )                 AS AUM_BAL 
       ,'1'                     AS IS_CREDIT 
       ,B.MGR_ID                AS MGR_ID 
       ,B.INSTITUTION           AS ORG_ID 
       ,V_DT               AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户基本信息表
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --OCRM_F_CI_BELONG_CUSTMGR
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  WHERE A.IF_CREDIT_CUST        = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_VALUABAL_CUST_AUM = sqlContext.sql(sql)
TMP_VALUABAL_CUST_AUM.registerTempTable("TMP_VALUABAL_CUST_AUM")
dfn="TMP_VALUABAL_CUST_AUM/"+V_DT+".parquet"
TMP_VALUABAL_CUST_AUM.cache()
nrows = TMP_VALUABAL_CUST_AUM.count()
TMP_VALUABAL_CUST_AUM.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_VALUABAL_CUST_AUM.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_VALUABAL_CUST_AUM lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-06::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,''                    AS CUST_TYPE 
       ,CAST(A.AMOUNT  AS DECIMAL(22,2))              AS AUM_BAL 
       ,'2'                     AS IS_CREDIT 
       ,B.MGR_ID                AS MGR_ID 
       ,B.INSTITUTION           AS ORG_ID 
       ,V_DT               AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM 
 (SELECT CUST_ID 
       ,FR_ID 
       ,SUM(NVL(AMOUNT,0)) AMOUNT 
   FROM ACRM_F_RE_AUMSUMAVGINFO 
  GROUP BY CUST_ID 
       ,FR_ID) A                                                --客户AUM积数均值表
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --OCRM_F_CI_BELONG_CUSTMGR
     ON A.CUST_ID               = B.CUST_ID 
    AND B.MAIN_TYPE             = '1' 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN TMP_VALUABAL_CUST_AUM C                           --去年、上月、本月有价值客户临时表
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.ETL_DATE              = V_DT 
  WHERE A.AMOUNT > 50000 
    AND C.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_VALUABAL_CUST_AUM = sqlContext.sql(sql)
TMP_VALUABAL_CUST_AUM.registerTempTable("TMP_VALUABAL_CUST_AUM")
dfn="TMP_VALUABAL_CUST_AUM/"+V_DT+".parquet"
TMP_VALUABAL_CUST_AUM.cache()
nrows = TMP_VALUABAL_CUST_AUM.count()
TMP_VALUABAL_CUST_AUM.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_VALUABAL_CUST_AUM.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_VALUABAL_CUST_AUM lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_VALUABAL_CUST_AUM_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/TMP_VALUABAL_CUST_AUM/"+V_DT+".parquet /"+dbname+"/TMP_VALUABAL_CUST_AUM_BK/")

#任务[12] 001-07::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
TMP_VALUABAL_CUST_AUM = sqlContext.read.parquet(hdfs+'/TMP_VALUABAL_CUST_AUM/*')
TMP_VALUABAL_CUST_AUM.registerTempTable("TMP_VALUABAL_CUST_AUM")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,COUNT(1)                       AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  INNER JOIN TMP_VALUABAL_CUST_AUM B                           --去年、上月、本月有价值客户临时表
     ON A.MGR_ID                = B.MGR_ID 
    AND A.ORG_ID                = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ETL_DATE              = DATE_ADD(TRUNC(V_DT, 'YY'),-1)
  WHERE A.ETL_DATE              = V_DT 
  GROUP BY A.ID
       ,A.AUM_MONTH
       ,A.AUM_YEAR
       ,A.LOAN_BALANCE
       ---,A.LAST_YEAR_VAL_CUST_NUM
       ,A.LAST_MONTH_VAL_CUST_NUM
       ,A.CURR_VAL_CUST_NUM
       ,A.NEW_YEAR_VAL_CUST_NUM
       ,A.NEW_MONTH_VAL_CUST_NUM
       ,A.LAST_MONTH_CREDIT_CARD_NUM
       ,A.CURR_CREDIT_CARD_NUM
       ,A.NEW_CREDIT_CARD_NUM
       ,A.LAST_YEAR_CUST_NUM
       ,A.LAST_MONTH_CUST_NUM
       ,A.CURR_CUST_NUM
       ,A.CUST_ADD_RATE_MONTH
       ,A.CUST_ADD_RATE_YEAR
       ,A.LAST_MONTH_CREDIT_CUST_NUM
       ,A.CURR_CREDIT_CUST_NUM
       ,A.NEW_CREDIT_CUST_NUM
       ,A.MGR_ID
       ,A.ORG_ID
       ,A.FR_ID
       ,A.ETL_DATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[12] 001-08::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,COUNT(1)                       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  INNER JOIN TMP_VALUABAL_CUST_AUM B                           --去年、上月、本月有价值客户临时表
     ON A.MGR_ID                = B.MGR_ID 
    AND A.ORG_ID                = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ETL_DATE              = DATE_ADD(TRUNC(V_DT, 'MM'),-1)
  WHERE A.ETL_DATE              = V_DT 
  GROUP BY A.ID
       ,A.AUM_MONTH
       ,A.AUM_YEAR
       ,A.LOAN_BALANCE
       ,A.LAST_YEAR_VAL_CUST_NUM
       --,A.LAST_MONTH_VAL_CUST_NUM
       ,A.CURR_VAL_CUST_NUM
       ,A.NEW_YEAR_VAL_CUST_NUM
       ,A.NEW_MONTH_VAL_CUST_NUM
       ,A.LAST_MONTH_CREDIT_CARD_NUM
       ,A.CURR_CREDIT_CARD_NUM
       ,A.NEW_CREDIT_CARD_NUM
       ,A.LAST_YEAR_CUST_NUM
       ,A.LAST_MONTH_CUST_NUM
       ,A.CURR_CUST_NUM
       ,A.CUST_ADD_RATE_MONTH
       ,A.CUST_ADD_RATE_YEAR
       ,A.LAST_MONTH_CREDIT_CUST_NUM
       ,A.CURR_CREDIT_CUST_NUM
       ,A.NEW_CREDIT_CUST_NUM
       ,A.MGR_ID
       ,A.ORG_ID
       ,A.FR_ID
       ,A.ETL_DATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[12] 001-09::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,COUNT(1)                       AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  INNER JOIN TMP_VALUABAL_CUST_AUM B                           --去年、上月、本月有价值客户临时表
     ON A.MGR_ID                = B.MGR_ID 
    AND A.ORG_ID                = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ETL_DATE              = V_DT 
  WHERE A.ETL_DATE              = V_DT 
  GROUP BY A.ID
       ,A.AUM_MONTH
       ,A.AUM_YEAR
       ,A.LOAN_BALANCE
       ,A.LAST_YEAR_VAL_CUST_NUM
       ,A.LAST_MONTH_VAL_CUST_NUM
       --,A.CURR_VAL_CUST_NUM
       ,A.NEW_YEAR_VAL_CUST_NUM
       ,A.NEW_MONTH_VAL_CUST_NUM
       ,A.LAST_MONTH_CREDIT_CARD_NUM
       ,A.CURR_CREDIT_CARD_NUM
       ,A.NEW_CREDIT_CARD_NUM
       ,A.LAST_YEAR_CUST_NUM
       ,A.LAST_MONTH_CUST_NUM
       ,A.CURR_CUST_NUM
       ,A.CUST_ADD_RATE_MONTH
       ,A.CUST_ADD_RATE_YEAR
       ,A.LAST_MONTH_CREDIT_CUST_NUM
       ,A.CURR_CREDIT_CUST_NUM
       ,A.NEW_CREDIT_CUST_NUM
       ,A.MGR_ID
       ,A.ORG_ID
       ,A.FR_ID
       ,A.ETL_DATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[12] 001-10::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,CAST((NVL(A.CURR_VAL_CUST_NUM, 0) - NVL(A.LAST_YEAR_VAL_CUST_NUM, 0)) AS DECIMAL(22))  AS NEW_YEAR_VAL_CUST_NUM 
       ,CAST((NVL(A.CURR_VAL_CUST_NUM, 0) - NVL(A.LAST_MONTH_VAL_CUST_NUM, 0))  AS DECIMAL(22))                     AS NEW_MONTH_VAL_CUST_NUM 
       ,CAST(NVL(A.CURR_CREDIT_CARD_NUM, 0) AS DECIMAL(22))                      AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  WHERE A.ETL_DATE              = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[12] 001-11::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,CAST(SUM(NVL(C.CARD_NUM, 0)) AS DECIMAL(22))                      AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  INNER JOIN 
 (SELECT A.CUST_ID,A.FR_ID,COUNT(CARD_NO) CARD_NUM  FROM OCRM_F_CI_CUSTLNAINFO A 
WHERE  PUB_DATE > add_months(V_DT,-1)
 AND PUB_DATE <= V_DT 
 GROUP BY A.CUST_ID,A.FR_ID) C                                          --
     ON  A.FR_ID  = C.FR_ID 
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --OCRM_F_CI_BELONG_CUSTMGR
     ON A.MGR_ID                = B.MGR_ID 
	AND C.CUST_ID               = B.CUST_ID
    AND A.ORG_ID                = B.INSTITUTION 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  WHERE A.ETL_DATE              = V_DT 
  GROUP BY A.ID
       ,A.AUM_MONTH
       ,A.AUM_YEAR
       ,A.LOAN_BALANCE
       ,A.LAST_YEAR_VAL_CUST_NUM
       ,A.LAST_MONTH_VAL_CUST_NUM
       ,A.CURR_VAL_CUST_NUM
       ,A.NEW_YEAR_VAL_CUST_NUM
       ,A.NEW_MONTH_VAL_CUST_NUM
       ,A.LAST_MONTH_CREDIT_CARD_NUM
       --,A.CURR_CREDIT_CARD_NUM
       ,A.NEW_CREDIT_CARD_NUM
       ,A.LAST_YEAR_CUST_NUM
       ,A.LAST_MONTH_CUST_NUM
       ,A.CURR_CUST_NUM
       ,A.CUST_ADD_RATE_MONTH
       ,A.CUST_ADD_RATE_YEAR
       ,A.LAST_MONTH_CREDIT_CUST_NUM
       ,A.CURR_CREDIT_CUST_NUM
       ,A.NEW_CREDIT_CUST_NUM
       ,A.MGR_ID
       ,A.ORG_ID
       ,A.FR_ID
       ,A.ETL_DATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[12] 001-12::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,CAST((NVL(CURR_CREDIT_CARD_NUM, 0) - NVL(LAST_MONTH_CREDIT_CARD_NUM, 0))  AS DECIMAL(22) ) AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  WHERE A.ETL_DATE              = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[21] 001-13::
#V_STEP = V_STEP + 1
#
#TMP_CUST_UP_RATE = sqlContext.read.parquet(hdfs+'/TMP_CUST_UP_RATE/*')
#TMP_CUST_UP_RATE.registerTempTable("TMP_CUST_UP_RATE")
#sql = """
# SELECT CUST_ID                 AS CUST_ID 
#       ,CUST_TYPE               AS CUST_TYPE 
#       ,OBJ_RATING              AS OBJ_RATING 
#       ,MGR_ID                  AS MGR_ID 
#       ,ORG_ID                  AS ORG_ID 
#       ,ETL_DATE                AS ETL_DATE 
#       ,FR_ID                   AS FR_ID 
#   FROM TMP_CUST_UP_RATE A                                     --
#  WHERE ETL_DATE = DATE_ADD(TRUNC(V_DT, 'YY'),-1) OR ETL_DATE = DATE_ADD(TRUNC(V_DT, 'MM'),-1) """
#
#sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
#TMP_CUST_UP_RATE_1 = sqlContext.sql(sql)
#TMP_CUST_UP_RATE_1.registerTempTable("TMP_CUST_UP_RATE_1")
#dfn="TMP_CUST_UP_RATE_1/"+V_DT+".parquet"
#TMP_CUST_UP_RATE_1.cache()
#nrows = TMP_CUST_UP_RATE_1.count()
#TMP_CUST_UP_RATE_1.write.save(path=hdfs + '/' + dfn, mode='overwrite')
#TMP_CUST_UP_RATE_1.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_UP_RATE_1/"+V_DT_LD+".parquet")
#et = datetime.now()
#print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_UP_RATE_1 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
#
##任务[21] 001-14::
#V_STEP = V_STEP + 1
#TMP_CUST_UP_RATE_1 = sqlContext.read.parquet(hdfs+'/TMP_CUST_UP_RATE_1/*')
#TMP_CUST_UP_RATE_1.registerTempTable("TMP_CUST_UP_RATE_1")
#sql = """
# SELECT CUST_ID                 AS CUST_ID 
#       ,CUST_TYPE               AS CUST_TYPE 
#       ,OBJ_RATING              AS OBJ_RATING 
#       ,MGR_ID                  AS MGR_ID 
#       ,ORG_ID                  AS ORG_ID 
#       ,ETL_DATE                AS ETL_DATE 
#       ,FR_ID                   AS FR_ID 
#   FROM TMP_CUST_UP_RATE_1 A                                   --
#"""
#
#sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
#TMP_CUST_UP_RATE = sqlContext.sql(sql)
#TMP_CUST_UP_RATE.registerTempTable("TMP_CUST_UP_RATE")
#dfn="TMP_CUST_UP_RATE/"+V_DT+".parquet"
#TMP_CUST_UP_RATE.cache()
#nrows = TMP_CUST_UP_RATE.count()
#TMP_CUST_UP_RATE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
#TMP_CUST_UP_RATE.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_CUST_UP_RATE/"+V_DT_LD+".parquet")
#et = datetime.now()
#print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_UP_RATE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-15::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_TYP              AS CUST_TYPE 
       ,A.OBJ_RATING            AS OBJ_RATING 
       ,B.MGR_ID                AS MGR_ID 
       ,B.INSTITUTION           AS ORG_ID 
       ,V_DT               AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --OCRM_F_CI_BELONG_CUSTMGR
     ON A.CUST_ID               = B.CUST_ID 
    AND B.MAIN_TYPE             = '1' 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_CUST_UP_RATE = sqlContext.sql(sql)
TMP_CUST_UP_RATE.registerTempTable("TMP_CUST_UP_RATE")
dfn="TMP_CUST_UP_RATE/"+V_DT+".parquet"
TMP_CUST_UP_RATE.cache()
nrows = TMP_CUST_UP_RATE.count()
TMP_CUST_UP_RATE.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_CUST_UP_RATE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_CUST_UP_RATE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-16::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
TMP_CUST_UP_RATE = sqlContext.read.parquet(hdfs+'/TMP_CUST_UP_RATE/*')
TMP_CUST_UP_RATE.registerTempTable("TMP_CUST_UP_RATE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,CAST(COUNT(1)  AS DECIMAL(22))                     AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  INNER JOIN TMP_CUST_UP_RATE B                                --
     ON A.MGR_ID                = B.MGR_ID 
    AND A.ORG_ID                = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ETL_DATE              = DATE_ADD(TRUNC(V_DT, 'MM'),-1)
  WHERE A.ETL_DATE              = DATE_ADD(TRUNC(V_DT, 'YY'),-1)
  GROUP BY A.ID
       ,A.AUM_MONTH
       ,A.AUM_YEAR
       ,A.LOAN_BALANCE
       ,A.LAST_YEAR_VAL_CUST_NUM
       ,A.LAST_MONTH_VAL_CUST_NUM
       ,A.CURR_VAL_CUST_NUM
       ,A.NEW_YEAR_VAL_CUST_NUM
       ,A.NEW_MONTH_VAL_CUST_NUM
       ,A.LAST_MONTH_CREDIT_CARD_NUM
       ,A.CURR_CREDIT_CARD_NUM
       ,A.NEW_CREDIT_CARD_NUM
       --,A.LAST_YEAR_CUST_NUM
       ,A.LAST_MONTH_CUST_NUM
       ,A.CURR_CUST_NUM
       ,A.CUST_ADD_RATE_MONTH
       ,A.CUST_ADD_RATE_YEAR
       ,A.LAST_MONTH_CREDIT_CUST_NUM
       ,A.CURR_CREDIT_CUST_NUM
       ,A.NEW_CREDIT_CUST_NUM
       ,A.MGR_ID
       ,A.ORG_ID
       ,A.FR_ID
       ,A.ETL_DATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[12] 001-17::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM                       AS LAST_YEAR_CUST_NUM 
       ,CAST(COUNT(1) AS DECIMAL(22))  AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  INNER JOIN TMP_CUST_UP_RATE B                                --
     ON A.MGR_ID                = B.MGR_ID 
    AND A.ORG_ID                = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ETL_DATE              = DATE_ADD(TRUNC(V_DT, 'MM'),-1) 
  WHERE A.ETL_DATE              = DATE_ADD(TRUNC(V_DT, 'MM'),-1)
  GROUP BY A.ID
       ,A.AUM_MONTH
       ,A.AUM_YEAR
       ,A.LOAN_BALANCE
       ,A.LAST_YEAR_VAL_CUST_NUM
       ,A.LAST_MONTH_VAL_CUST_NUM
       ,A.CURR_VAL_CUST_NUM
       ,A.NEW_YEAR_VAL_CUST_NUM
       ,A.NEW_MONTH_VAL_CUST_NUM
       ,A.LAST_MONTH_CREDIT_CARD_NUM
       ,A.CURR_CREDIT_CARD_NUM
       ,A.NEW_CREDIT_CARD_NUM
       ,A.LAST_YEAR_CUST_NUM
       --,A.LAST_MONTH_CUST_NUM
       ,A.CURR_CUST_NUM
       ,A.CUST_ADD_RATE_MONTH
       ,A.CUST_ADD_RATE_YEAR
       ,A.LAST_MONTH_CREDIT_CUST_NUM
       ,A.CURR_CREDIT_CUST_NUM
       ,A.NEW_CREDIT_CUST_NUM
       ,A.MGR_ID
       ,A.ORG_ID
       ,A.FR_ID
       ,A.ETL_DATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[12] 001-18::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,CAST((CASE WHEN NVL(A.LAST_YEAR_CUST_NUM, 0)  = 0 THEN 1 ELSE (NVL(SUM(CASE WHEN C.OBJ_RATING < D.OBJ_RATING THEN 1 ELSE 0 END), 0)) / A.LAST_YEAR_CUST_NUM END) AS DECIMAL(22,4))  AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --OCRM_F_CI_BELONG_CUSTMGR
     ON A.MGR_ID                = B.MGR_ID 
    AND A.ORG_ID                = B.INSTITUTION 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN OCRM_F_CI_CUST_DESC C                             --统一客户信息
     ON B.CUST_ID               = C.CUST_ID 
    AND B.FR_ID                 = C.FR_ID 
  INNER JOIN TMP_CUST_UP_RATE D                                --
     ON C.CUST_ID               = D.CUST_ID 
    AND C.FR_ID                 = D.FR_ID 
    AND D.ETL_DATE              = DATE_ADD(TRUNC(V_DT, 'YY'),-1) 
  WHERE A.ETL_DATE              = V_DT  
  GROUP BY A.ID
       ,A.AUM_MONTH
       ,A.AUM_YEAR
       ,A.LOAN_BALANCE
       ,A.LAST_YEAR_VAL_CUST_NUM
       ,A.LAST_MONTH_VAL_CUST_NUM
       ,A.CURR_VAL_CUST_NUM
       ,A.NEW_YEAR_VAL_CUST_NUM
       ,A.NEW_MONTH_VAL_CUST_NUM
       ,A.LAST_MONTH_CREDIT_CARD_NUM
       ,A.CURR_CREDIT_CARD_NUM
       ,A.NEW_CREDIT_CARD_NUM
       ,A.LAST_YEAR_CUST_NUM
       ,A.LAST_MONTH_CUST_NUM
       ,A.CURR_CUST_NUM
       ,A.CUST_ADD_RATE_MONTH
       --,A.CUST_ADD_RATE_YEAR
       ,A.LAST_MONTH_CREDIT_CUST_NUM
       ,A.CURR_CREDIT_CUST_NUM
       ,A.NEW_CREDIT_CUST_NUM
       ,A.MGR_ID
       ,A.ORG_ID
       ,A.FR_ID
       ,A.ETL_DATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[12] 001-19::
V_STEP = V_STEP + 1

OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,CAST(CASE WHEN NVL(A.LAST_YEAR_CUST_NUM, 0) = 0 THEN 1 ELSE (NVL(SUM(CASE WHEN C.OBJ_RATING < D.OBJ_RATING THEN 1 ELSE 0 END), 0)) / A.LAST_YEAR_CUST_NUM END   AS DECIMAL(22,4))   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR   AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --OCRM_F_CI_BELONG_CUSTMGR
     ON A.MGR_ID                = B.MGR_ID 
    AND A.ORG_ID                = B.INSTITUTION 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN OCRM_F_CI_CUST_DESC C                             --统一客户信息
     ON B.CUST_ID               = C.CUST_ID 
    AND B.FR_ID                 = C.FR_ID 
  INNER JOIN TMP_CUST_UP_RATE D                                --
     ON C.CUST_ID               = D.CUST_ID 
    AND C.FR_ID                 = D.FR_ID 
    AND D.ETL_DATE              = DATE_ADD(TRUNC(V_DT, 'MM'),-1)
  WHERE A.ETL_DATE              = V_DT  
  GROUP BY A.ID
       ,A.AUM_MONTH
       ,A.AUM_YEAR
       ,A.LOAN_BALANCE
       ,A.LAST_YEAR_VAL_CUST_NUM
       ,A.LAST_MONTH_VAL_CUST_NUM
       ,A.CURR_VAL_CUST_NUM
       ,A.NEW_YEAR_VAL_CUST_NUM
       ,A.NEW_MONTH_VAL_CUST_NUM
       ,A.LAST_MONTH_CREDIT_CARD_NUM
       ,A.CURR_CREDIT_CARD_NUM
       ,A.NEW_CREDIT_CARD_NUM
       ,A.LAST_YEAR_CUST_NUM
       ,A.LAST_MONTH_CUST_NUM
       ,A.CURR_CUST_NUM
       --,A.CUST_ADD_RATE_MONTH
       ,A.CUST_ADD_RATE_YEAR
       ,A.LAST_MONTH_CREDIT_CUST_NUM
       ,A.CURR_CREDIT_CUST_NUM
       ,A.NEW_CREDIT_CUST_NUM
       ,A.MGR_ID
       ,A.ORG_ID
       ,A.FR_ID
       ,A.ETL_DATE"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")

sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[12] 001-20::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,CAST(COUNT(1)  AS DECIMAL(22))                     AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  INNER JOIN TMP_VALUABAL_CUST_AUM B                           --去年、上月、本月有价值客户临时表
     ON A.MGR_ID                = B.MGR_ID 
    AND A.ORG_ID                = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.IS_CREDIT             = '1' 
    AND B.ETL_DATE              = DATE_ADD(TRUNC(V_DT, 'MM'),-1)
  WHERE A.ETL_DATE              = V_DT 
  GROUP BY A.ID
       ,A.AUM_MONTH
       ,A.AUM_YEAR
       ,A.LOAN_BALANCE
       ,A.LAST_YEAR_VAL_CUST_NUM
       ,A.LAST_MONTH_VAL_CUST_NUM
       ,A.CURR_VAL_CUST_NUM
       ,A.NEW_YEAR_VAL_CUST_NUM
       ,A.NEW_MONTH_VAL_CUST_NUM
       ,A.LAST_MONTH_CREDIT_CARD_NUM
       ,A.CURR_CREDIT_CARD_NUM
       ,A.NEW_CREDIT_CARD_NUM
       ,A.LAST_YEAR_CUST_NUM
       ,A.LAST_MONTH_CUST_NUM
       ,A.CURR_CUST_NUM
       ,A.CUST_ADD_RATE_MONTH
       ,A.CUST_ADD_RATE_YEAR
       --,A.LAST_MONTH_CREDIT_CUST_NUM
       ,A.CURR_CREDIT_CUST_NUM
       ,A.NEW_CREDIT_CUST_NUM
       ,A.MGR_ID
       ,A.ORG_ID
       ,A.FR_ID
       ,A.ETL_DATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[12] 001-21::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,CAST(COUNT(1) AS DECIMAL(22))                      AS CURR_CREDIT_CUST_NUM 
       ,A.NEW_CREDIT_CUST_NUM   AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
  INNER JOIN TMP_VALUABAL_CUST_AUM B                           --去年、上月、本月有价值客户临时表
     ON A.MGR_ID                = B.MGR_ID 
    AND A.ORG_ID                = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.IS_CREDIT             = '1' 
    AND B.ETL_DATE              = V_DT 
  WHERE A.ETL_DATE              = V_DT 
  GROUP BY A.ID
       ,A.AUM_MONTH
       ,A.AUM_YEAR
       ,A.LOAN_BALANCE
       ,A.LAST_YEAR_VAL_CUST_NUM
       ,A.LAST_MONTH_VAL_CUST_NUM
       ,A.CURR_VAL_CUST_NUM
       ,A.NEW_YEAR_VAL_CUST_NUM
       ,A.NEW_MONTH_VAL_CUST_NUM
       ,A.LAST_MONTH_CREDIT_CARD_NUM
       ,A.CURR_CREDIT_CARD_NUM
       ,A.NEW_CREDIT_CARD_NUM
       ,A.LAST_YEAR_CUST_NUM
       ,A.LAST_MONTH_CUST_NUM
       ,A.CURR_CUST_NUM
       ,A.CUST_ADD_RATE_MONTH
       ,A.CUST_ADD_RATE_YEAR
       ,A.LAST_MONTH_CREDIT_CUST_NUM
       ,A.NEW_CREDIT_CUST_NUM
       ,A.MGR_ID
       ,A.ORG_ID
       ,A.FR_ID
       ,A.ETL_DATE"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")

#任务[12] 001-22::
V_STEP = V_STEP + 1
OCRM_F_CI_MANAGER_PERFORMANCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_MANAGER_PERFORMANCE/*')
OCRM_F_CI_MANAGER_PERFORMANCE.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.AUM_MONTH             AS AUM_MONTH 
       ,A.AUM_YEAR              AS AUM_YEAR 
       ,A.LOAN_BALANCE          AS LOAN_BALANCE 
       ,A.LAST_YEAR_VAL_CUST_NUM        AS LAST_YEAR_VAL_CUST_NUM 
       ,A.LAST_MONTH_VAL_CUST_NUM       AS LAST_MONTH_VAL_CUST_NUM 
       ,A.CURR_VAL_CUST_NUM     AS CURR_VAL_CUST_NUM 
       ,A.NEW_YEAR_VAL_CUST_NUM AS NEW_YEAR_VAL_CUST_NUM 
       ,A.NEW_MONTH_VAL_CUST_NUM        AS NEW_MONTH_VAL_CUST_NUM 
       ,A.LAST_MONTH_CREDIT_CARD_NUM    AS LAST_MONTH_CREDIT_CARD_NUM 
       ,A.CURR_CREDIT_CARD_NUM  AS CURR_CREDIT_CARD_NUM 
       ,A.NEW_CREDIT_CARD_NUM   AS NEW_CREDIT_CARD_NUM 
       ,A.LAST_YEAR_CUST_NUM    AS LAST_YEAR_CUST_NUM 
       ,A.LAST_MONTH_CUST_NUM   AS LAST_MONTH_CUST_NUM 
       ,A.CURR_CUST_NUM         AS CURR_CUST_NUM 
       ,A.CUST_ADD_RATE_MONTH   AS CUST_ADD_RATE_MONTH 
       ,A.CUST_ADD_RATE_YEAR    AS CUST_ADD_RATE_YEAR 
       ,A.LAST_MONTH_CREDIT_CUST_NUM    AS LAST_MONTH_CREDIT_CUST_NUM 
       ,A.CURR_CREDIT_CUST_NUM  AS CURR_CREDIT_CUST_NUM 
       ,CAST((NVL(CURR_CREDIT_CUST_NUM, 0) - NVL(LAST_MONTH_CREDIT_CUST_NUM, 0))  AS DECIMAL(22))  AS NEW_CREDIT_CUST_NUM 
       ,A.MGR_ID                AS MGR_ID 
       ,A.ORG_ID                AS ORG_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.ETL_DATE              AS ETL_DATE 
   FROM OCRM_F_CI_MANAGER_PERFORMANCE A                        --OCRM_F_CI_MANAGER_PERFORMANCE
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.registerTempTable("OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.AUM_MONTH                                          --AUM月日均（万元）:src.AUM_MONTH
       ,DST.AUM_YEAR                                           --AUM年日均（万元）:src.AUM_YEAR
       ,DST.LOAN_BALANCE                                       --贷款余额（万元）:src.LOAN_BALANCE
       ,DST.LAST_YEAR_VAL_CUST_NUM                             --上年有价值客户数:src.LAST_YEAR_VAL_CUST_NUM
       ,DST.LAST_MONTH_VAL_CUST_NUM                            --上月有价值客户数:src.LAST_MONTH_VAL_CUST_NUM
       ,DST.CURR_VAL_CUST_NUM                                  --本月有价值客户数:src.CURR_VAL_CUST_NUM
       ,DST.NEW_YEAR_VAL_CUST_NUM                              --新增价值客户（户）（当年）:src.NEW_YEAR_VAL_CUST_NUM
       ,DST.NEW_MONTH_VAL_CUST_NUM                             --新增价值客户（户）（当月）:src.NEW_MONTH_VAL_CUST_NUM
       ,DST.LAST_MONTH_CREDIT_CARD_NUM                         --上月信用卡张数:src.LAST_MONTH_CREDIT_CARD_NUM
       ,DST.CURR_CREDIT_CARD_NUM                               --本月信用卡张数:src.CURR_CREDIT_CARD_NUM
       ,DST.NEW_CREDIT_CARD_NUM                                --信用卡拓展（张）:src.NEW_CREDIT_CARD_NUM
       ,DST.LAST_YEAR_CUST_NUM                                 --上年总客户数:src.LAST_YEAR_CUST_NUM
       ,DST.LAST_MONTH_CUST_NUM                                --上月总客户数:src.LAST_MONTH_CUST_NUM
       ,DST.CURR_CUST_NUM                                      --:src.CURR_CUST_NUM
       ,DST.CUST_ADD_RATE_MONTH                                --客户提升率（当月）:src.CUST_ADD_RATE_MONTH
       ,DST.CUST_ADD_RATE_YEAR                                 --客户提升率（当年）:src.CUST_ADD_RATE_YEAR
       ,DST.LAST_MONTH_CREDIT_CUST_NUM                         --上月授信客户数:src.LAST_MONTH_CREDIT_CUST_NUM
       ,DST.CURR_CREDIT_CUST_NUM                               --本月授信客户数:src.CURR_CREDIT_CUST_NUM
       ,DST.NEW_CREDIT_CUST_NUM                                --新增授信客户数（当月）:src.NEW_CREDIT_CUST_NUM
       ,DST.MGR_ID                                             --:src.MGR_ID
       ,DST.ORG_ID                                             --:src.ORG_ID
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.ETL_DATE                                           --:src.ETL_DATE
   FROM OCRM_F_CI_MANAGER_PERFORMANCE DST 
   LEFT JOIN OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1 SRC 
     ON SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.ORG_ID              = DST.ORG_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT+".parquet"
UNION=OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unionAll(OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1)
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.cache()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.count()
nrowsa = OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP1.unpersist()
OCRM_F_CI_MANAGER_PERFORMANCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_MANAGER_PERFORMANCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_MANAGER_PERFORMANCE_BK/")
