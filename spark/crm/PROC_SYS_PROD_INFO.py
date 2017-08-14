# coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_SYS_PROD_INFO').setMaster(sys.argv[2])
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
if len(sys.argv) > 5:
    if sys.argv[5] == "hive":
        sqlContext = HiveContext(sc)
else:
    sqlContext = SQLContext(sc)
hdfs = sys.argv[3]
dbname = sys.argv[4]

# 处理需要使用的日期
etl_date = sys.argv[1]
# etl日期
V_DT = etl_date
# 上一日日期
V_DT_LD = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8])) + timedelta(-1)).strftime("%Y%m%d")
# 月初日期
V_DT_FMD = date(int(etl_date[0:4]), int(etl_date[4:6]), 1).strftime("%Y%m%d")
# 上月末日期
V_DT_LMD = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1) + timedelta(-1)).strftime("%Y%m%d")
# 10位日期
V_DT10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8]))).strftime("%Y-%m-%d")
V_STEP = 0
# OCRM_F_PD_PROD_CATL 增量插入不存在本表的数据
ret = os.system("hdfs dfs -rm -r /" + dbname + "/OCRM_F_PD_PROD_CATL/*")
ret = os.system(
    "hdfs dfs -cp /" + dbname + "/OCRM_F_PD_PROD_CATL_BK/" + V_DT_LD + ".parquet /" + dbname + "/OCRM_F_PD_PROD_CATL/" + V_DT + ".parquet")

# OCRM_F_PD_PROD_INFO 增量插入，删除当天的文件
ret = os.system("hdfs dfs -rm -r /" + dbname + "/OCRM_F_PD_PROD_INFO/" + V_DT + ".parquet")

F_CM_FIN_PRODINFO = sqlContext.read.parquet(hdfs + '/F_CM_FIN_PRODINFO/*')
F_CM_FIN_PRODINFO.registerTempTable("F_CM_FIN_PRODINFO")
F_CI_CBOD_PDPDPPDP = sqlContext.read.parquet(hdfs + '/F_CI_CBOD_PDPDPPDP/*')
F_CI_CBOD_PDPDPPDP.registerTempTable("F_CI_CBOD_PDPDPPDP")
OCRM_F_FID = sqlContext.read.parquet(hdfs + '/OCRM_F_FID/*')
OCRM_F_FID.registerTempTable("OCRM_F_FID")
F_CM_XDXT_BUSINESS_TYPE = sqlContext.read.parquet(hdfs + '/F_CM_XDXT_BUSINESS_TYPE/*')
F_CM_XDXT_BUSINESS_TYPE.registerTempTable("F_CM_XDXT_BUSINESS_TYPE")
OCRM_F_PD_PROD_CATL = sqlContext.read.parquet(hdfs + '/OCRM_F_PD_PROD_CATL/*')
OCRM_F_PD_PROD_CATL.registerTempTable("OCRM_F_PD_PROD_CATL")
OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs + '/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")

# 任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.TYPENO                AS CATL_CODE 
       ,A.TYPENAME              AS CATL_NAME 
       ,CASE WHEN LENGTH(A.TYPENO) > 3 THEN SUBSTR(A.TYPENO, 1, LENGTH(A.TYPENO) - 3) ELSE 'B' END                     AS CATL_PARENT 
       ,CAST('' AS INTEGER)                   AS CATL_LEVEL 
       ,CAST(''    AS INTEGER)                AS CATL_ORDER 
       ,'B'                     AS VIEW_DETAIL 
       ,V_DT_8                       AS CATL_BUS_ID 
       ,''                    AS FR_ID 
   FROM F_CM_XDXT_BUSINESS_TYPE A                              --资产业务品种表
  LEFT JOIN OCRM_F_PD_PROD_CATL C ON C.CATL_CODE = A.TYPENO
   WHERE (A.NODETYPE = '010' OR A.CORPORATEORGID = '000')
            AND A.TYPENO <> '000000'
            AND LENGTH(A.TYPENO)=1
            AND C.CATL_CODE IS NULL
   """

sql = re.sub(r"\bV_DT\b", "'" + V_DT10 + "'", sql)
sql = re.sub(r"\bV_DT_8\b", "'" + V_DT + "'", sql)
OCRM_F_PD_PROD_CATL = sqlContext.sql(sql)
dfn = "OCRM_F_PD_PROD_CATL/" + V_DT + ".parquet"
OCRM_F_PD_PROD_CATL.cache()
nrows = OCRM_F_PD_PROD_CATL.count()
OCRM_F_PD_PROD_CATL.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_PD_PROD_CATL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_PD_PROD_CATL lines %d") % (
V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et - st).seconds, nrows)

ret = os.system("hdfs dfs -rm -r /" + dbname + "/OCRM_F_PD_PROD_CATL_BK/" + V_DT + ".parquet")
ret = os.system(
    "hdfs dfs -cp /" + dbname + "/OCRM_F_PD_PROD_CATL/" + V_DT + ".parquet /" + dbname + "/OCRM_F_PD_PROD_CATL_BK/")

# 任务[11] 001-02::
V_STEP = V_STEP + 1
OCRM_F_PD_PROD_CATL = sqlContext.read.parquet(hdfs + '/OCRM_F_PD_PROD_CATL/*')
OCRM_F_PD_PROD_CATL.registerTempTable("OCRM_F_PD_PROD_CATL")
sql = """
 SELECT CAST(monotonically_increasing_id() AS DECIMAL(27))    AS ID 
       ,A.PD_CODE               AS PRODUCT_ID 
       ,A.PD_DES                AS PROD_NAME 
       ,SUBSTR(A.PD_CODE, 4, 3)                       AS CATL_CODE 
       ,''                    AS TJKJ 
       ,CAST(''  AS DECIMAL(27))                  AS PROD_TYPE_ID 
       ,''                    AS PROD_DESC 
       ,''                    AS DISPLAY_FLAG 
       ,A.PD_START_DT                       AS PROD_START_DATE 
       ,A.PD_EXP_DT                       AS PROD_END_DATE 
       ,''                    AS PROD_STATE 
       ,'系统'                AS PROD_CREATOR 
       ,V_DT_8                    AS CREATE_DATE 
       ,''                    AS PROD_SHOW_URL 
       ,''                    AS PROD_QUERY_URL 
       ,''                    AS PROD_SEQ 
       ,''                    AS PROD_DEPT 
       ,''                    AS RATE 
       ,''                    AS COST_RATE 
       ,''                    AS LIMIT_TIME 
       ,''                    AS PROD_CHARACT 
       ,''                    AS OBJ_CUST_DISC 
       ,''                    AS DANGER_DISC 
       ,''                    AS CHANNEL_DISC 
       ,''                    AS ASSURE_DISC 
       ,''                    AS PROD_SWITCH 
       ,''                    AS RISK_LEVEL 
       ,''                    AS PROD_BUS_ID 
       ,D.FR_ID                 AS FR_ID 
       ,''                    AS TRANSCONDITION 
       ,''                    AS PROD_CASE 
       ,''                    AS IS_HOT 
       ,''                    AS IS_CHOICE 
   FROM F_CI_CBOD_PDPDPPDP A                                   --产品数据库
  INNER JOIN OCRM_F_PD_PROD_CATL B                             --OCRM_F_PD_PROD_CATL
     ON B.CATL_CODE = SUBSTR(A.PD_CODE, 4, 3) 
   LEFT JOIN OCRM_F_PD_PROD_INFO C                             --OCRM_F_PD_PROD_INFO
     ON A.FR_ID                 = C.FR_ID 
    AND C.PRODUCT_ID            = A.PD_CODE 
   LEFT JOIN OCRM_F_FID D                                      --法人表
     ON C.FR_ID                 = D.FR_ID 
    AND D.IF_USE                = '1' 
    AND D.FR_ID NOT IN('000', '900', '999') 
  WHERE (LENGTH(A.PD_CODE) > 6 AND SUBSTR(A.PD_CODE, 4, 3) IN('CR0', 'SA1', 'SA0', 'TD1', 'TD0', 'SH0')) 
    AND (A.PD_1LVL_BRH_ID        = '999' OR A.PD_1LVL_BRH_ID IS NULL) 
    AND C.PRODUCT_ID IS NULL """

# sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT_8\b", "'" + V_DT + "'", sql)
OCRM_F_PD_PROD_INFO = sqlContext.sql(sql)
dfn = "OCRM_F_PD_PROD_INFO/" + V_DT + ".parquet"
OCRM_F_PD_PROD_INFO.cache()
nrows = OCRM_F_PD_PROD_INFO.count()
OCRM_F_PD_PROD_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_PD_PROD_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_PD_PROD_INFO lines %d") % (
V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et - st).seconds, nrows)

# 任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id() AS DECIMAL(27))     AS ID 
       ,A.PD_CODE               AS PRODUCT_ID 
       ,A.PD_DES                AS PROD_NAME 
       ,SUBSTR(A.PD_CODE, 4, 3)                       AS CATL_CODE 
       ,''                    AS TJKJ 
       ,CAST(''  AS DECIMAL(27))                   AS PROD_TYPE_ID 
       ,''                    AS PROD_DESC 
       ,''                    AS DISPLAY_FLAG 
       ,A.PD_START_DT                       AS PROD_START_DATE 
       ,A.PD_EXP_DT                       AS PROD_END_DATE 
       ,''                    AS PROD_STATE 
       ,'系统'                AS PROD_CREATOR 
       ,V_DT_8                    AS CREATE_DATE 
       ,''                    AS PROD_SHOW_URL 
       ,''                    AS PROD_QUERY_URL 
       ,''                    AS PROD_SEQ 
       ,''                    AS PROD_DEPT 
       ,''                    AS RATE 
       ,''                    AS COST_RATE 
       ,''                    AS LIMIT_TIME 
       ,''                    AS PROD_CHARACT 
       ,''                    AS OBJ_CUST_DISC 
       ,''                    AS DANGER_DISC 
       ,''                    AS CHANNEL_DISC 
       ,''                    AS ASSURE_DISC 
       ,''                    AS PROD_SWITCH 
       ,''                    AS RISK_LEVEL 
       ,''                    AS PROD_BUS_ID 
       ,B.FR_ID                 AS FR_ID 
       ,''                    AS TRANSCONDITION 
       ,''                    AS PROD_CASE 
       ,''                    AS IS_HOT 
       ,''                    AS IS_CHOICE 
   FROM F_CI_CBOD_PDPDPPDP A                                   --产品数据库
  INNER JOIN OCRM_F_FID B                                      --法人表
     ON A.PD_1LVL_BRH_ID        = B.FR_ID 
    AND B.IF_USE                = '1' 
    AND B.FR_ID 
    NOT IN('000', '900', '999') 
  INNER JOIN OCRM_F_PD_PROD_CATL C                             --OCRM_F_PD_PROD_CATL
     ON C.CATL_CODE             = SUBSTR(A.PD_CODE, 4, 3) 
   LEFT JOIN OCRM_F_PD_PROD_INFO D                             --OCRM_F_PD_PROD_INFO
     ON D.FR_ID                 = B.FR_ID 
    AND D.PRODUCT_ID            = A.PD_CODE 
  WHERE LENGTH(A.PD_CODE) > 6 
    AND SUBSTR(A.PD_CODE, 4, 3) IN('CR0', 'SA1', 'SA0', 'TD1', 'TD0', 'SH0') 
    AND D.PRODUCT_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'" + V_DT10 + "'", sql)
sql = re.sub(r"\bV_DT_8\b", "'" + V_DT + "'", sql)
OCRM_F_PD_PROD_INFO = sqlContext.sql(sql)
# OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
dfn = "OCRM_F_PD_PROD_INFO/" + V_DT + ".parquet"
OCRM_F_PD_PROD_INFO.cache()
nrows = OCRM_F_PD_PROD_INFO.count()
OCRM_F_PD_PROD_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_PD_PROD_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_PD_PROD_INFO lines %d") % (
V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et - st).seconds, nrows)

# 任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id() AS DECIMAL(27))     AS ID 
       ,A.TYPENO                AS PRODUCT_ID 
       ,A.TYPENAME              AS PROD_NAME 
       ,A.PARENTTYPENO          AS CATL_CODE 
       ,''                    AS TJKJ 
       ,CAST(''     AS DECIMAL(27) )                 AS PROD_TYPE_ID 
       ,''                    AS PROD_DESC 
       ,''                    AS DISPLAY_FLAG 
       ,''                    AS PROD_START_DATE 
       ,''                    AS PROD_END_DATE 
       ,''                    AS PROD_STATE 
       ,'系统'                AS PROD_CREATOR 
       ,V_DT_8                    AS CREATE_DATE 
       ,''                    AS PROD_SHOW_URL 
       ,''                    AS PROD_QUERY_URL 
       ,''                    AS PROD_SEQ 
       ,''                    AS PROD_DEPT 
       ,''                    AS RATE 
       ,''                    AS COST_RATE 
       ,''                    AS LIMIT_TIME 
       ,''                    AS PROD_CHARACT 
       ,''                    AS OBJ_CUST_DISC 
       ,''                    AS DANGER_DISC 
       ,''                    AS CHANNEL_DISC 
       ,''                    AS ASSURE_DISC 
       ,''                    AS PROD_SWITCH 
       ,''                    AS RISK_LEVEL 
       ,''                    AS PROD_BUS_ID 
       ,A.CORPORATEORGID        AS FR_ID 
       ,''                    AS TRANSCONDITION 
       ,''                    AS PROD_CASE 
       ,''                    AS IS_HOT 
       ,''                    AS IS_CHOICE 
   FROM F_CM_XDXT_BUSINESS_TYPE A                              --资产业务品种表
  INNER JOIN OCRM_F_FID B                                      --法人表
     ON A.CORPORATEORGID        = B.FR_ID 
    AND B.IF_USE                = '1' 
    AND B.FR_ID 
    NOT IN('000', '900', '999') 
  INNER JOIN OCRM_F_PD_PROD_CATL C                             --OCRM_F_PD_PROD_CATL
     ON C.CATL_CODE             = A.PARENTTYPENO 
   LEFT JOIN OCRM_F_PD_PROD_INFO D                             --OCRM_F_PD_PROD_INFO
     ON D.FR_ID                 = A.CORPORATEORGID 
    AND D.PRODUCT_ID            = A.TYPENO 
  WHERE(A.NODETYPE <> '010' 
            AND A.CORPORATEORGID <> '000') 
    AND D.PRODUCT_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'" + V_DT10 + "'", sql)
sql = re.sub(r"\bV_DT_8\b", "'" + V_DT + "'", sql)
OCRM_F_PD_PROD_INFO = sqlContext.sql(sql)
# OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
dfn = "OCRM_F_PD_PROD_INFO/" + V_DT + ".parquet"
OCRM_F_PD_PROD_INFO.cache()
nrows = OCRM_F_PD_PROD_INFO.count()
OCRM_F_PD_PROD_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_PD_PROD_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_PD_PROD_INFO lines %d") % (
V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et - st).seconds, nrows)

# 任务[11] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id()  AS DECIMAL(27))    AS ID 
       ,A.PRODCODE              AS PRODUCT_ID 
       ,A.PRODNAME              AS PROD_NAME 
       ,'E'                     AS CATL_CODE 
       ,''                    AS TJKJ 
       ,CAST(''    AS DECIMAL(27))                 AS PROD_TYPE_ID 
       ,''                    AS PROD_DESC 
       ,''                    AS DISPLAY_FLAG 
       ,A.PRODSDATE                       AS PROD_START_DATE 
       ,A.PRODEDATE                       AS PROD_END_DATE 
       ,''                    AS PROD_STATE 
       ,'系统'                AS PROD_CREATOR 
       ,V_DT_8                    AS CREATE_DATE 
       ,''                    AS PROD_SHOW_URL 
       ,''                    AS PROD_QUERY_URL 
       ,''                    AS PROD_SEQ 
       ,''                    AS PROD_DEPT 
       ,''                    AS RATE 
       ,''                    AS COST_RATE 
       ,''                    AS LIMIT_TIME 
       ,''                    AS PROD_CHARACT 
       ,''                    AS OBJ_CUST_DISC 
       ,''                    AS DANGER_DISC 
       ,''                    AS CHANNEL_DISC 
       ,''                    AS ASSURE_DISC 
       ,''                    AS PROD_SWITCH 
       ,''                    AS RISK_LEVEL 
       ,''                    AS PROD_BUS_ID 
       ,A.PRODZONENO            AS FR_ID 
       ,''                    AS TRANSCONDITION 
       ,''                    AS PROD_CASE 
       ,''                    AS IS_HOT 
       ,''                    AS IS_CHOICE 
   FROM F_CM_FIN_PRODINFO A                                    --理财产品参数表
  INNER JOIN OCRM_F_FID B                                      --法人表
     ON A.PRODZONENO            = B.FR_ID 
    AND B.IF_USE                = '1' 
    AND B.FR_ID 
    NOT IN('000', '900', '999') 
   LEFT JOIN OCRM_F_PD_PROD_INFO C                             --OCRM_F_PD_PROD_INFO
     ON C.FR_ID                 = A.PRODZONENO 
    AND C.PRODUCT_ID            = A.PRODCODE 
  WHERE A.PRODZONENO <> '000' 
    AND C.PRODUCT_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'" + V_DT10 + "'", sql)
sql = re.sub(r"\bV_DT_8\b", "'" + V_DT + "'", sql)
OCRM_F_PD_PROD_INFO = sqlContext.sql(sql)
# OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
dfn = "OCRM_F_PD_PROD_INFO/" + V_DT + ".parquet"
OCRM_F_PD_PROD_INFO.cache()
nrows = OCRM_F_PD_PROD_INFO.count()
OCRM_F_PD_PROD_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_PD_PROD_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_PD_PROD_INFO lines %d") % (
V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et - st).seconds, nrows)
