#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_GROUP_INFO').setMaster(sys.argv[2])
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

F_CI_XDXT_ENT_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_ENT_INFO/*')
F_CI_XDXT_ENT_INFO.registerTempTable("F_CI_XDXT_ENT_INFO")
F_CI_XDXT_GROUP_RELATIVE = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_GROUP_RELATIVE/*')
F_CI_XDXT_GROUP_RELATIVE.registerTempTable("F_CI_XDXT_GROUP_RELATIVE")
OCRM_F_CI_GROUP_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_GROUP_INFO_BK/'+V_DT_LD+'.parquet/*')
OCRM_F_CI_GROUP_INFO.registerTempTable("OCRM_F_CI_GROUP_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(ID     AS BIGINT)                 AS ID 
       ,GROUP_TYPE              AS GROUP_TYPE 
       ,GROUP_NAME              AS GROUP_NAME 
       ,GROUP_STATUS            AS GROUP_STATUS 
       ,GROUP_ROOT_CUST_ID      AS GROUP_ROOT_CUST_ID 
       ,UPDATE_DATE             AS UPDATE_DATE 
       ,GROUP_MEMO              AS GROUP_MEMO 
       ,GROUP_NO                AS GROUP_NO 
       ,UPDATE_USER_ID          AS UPDATE_USER_ID 
       ,GROUP_HOST_ORG_NO       AS GROUP_HOST_ORG_NO 
       ,GROUP_ROOT_ADDRESS      AS GROUP_ROOT_ADDRESS 
       ,CREATA_DATE             AS CREATA_DATE 
       ,CREATE_USER_ID          AS CREATE_USER_ID 
       ,CREATE_USER_NAME        AS CREATE_USER_NAME 
       ,CREATE_USER_ORG_ID      AS CREATE_USER_ORG_ID 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
       ,FR_ID                   AS FR_ID 
       ,CUST_NAME               AS CUST_NAME 
   FROM OCRM_F_CI_GROUP_INFO A                                 --集团信息表
  WHERE ODS_ST_DATE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_GROUP_INFO = sqlContext.sql(sql)
TMP_OCRM_F_CI_GROUP_INFO.registerTempTable("TMP_OCRM_F_CI_GROUP_INFO")
dfn="TMP_OCRM_F_CI_GROUP_INFO/"+V_DT+".parquet"
TMP_OCRM_F_CI_GROUP_INFO.cache()
nrows = TMP_OCRM_F_CI_GROUP_INFO.count()
TMP_OCRM_F_CI_GROUP_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_GROUP_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_GROUP_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_GROUP_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(ID     AS BIGINT)                      AS ID 
       ,GROUP_TYPE              AS GROUP_TYPE 
       ,GROUP_NAME              AS GROUP_NAME 
       ,GROUP_STATUS            AS GROUP_STATUS 
       ,GROUP_ROOT_CUST_ID      AS GROUP_ROOT_CUST_ID 
       ,UPDATE_DATE             AS UPDATE_DATE 
       ,GROUP_MEMO              AS GROUP_MEMO 
       ,GROUP_NO                AS GROUP_NO 
       ,UPDATE_USER_ID          AS UPDATE_USER_ID 
       ,GROUP_HOST_ORG_NO       AS GROUP_HOST_ORG_NO 
       ,GROUP_ROOT_ADDRESS      AS GROUP_ROOT_ADDRESS 
       ,CREATA_DATE             AS CREATA_DATE 
       ,CREATE_USER_ID          AS CREATE_USER_ID 
       ,CREATE_USER_NAME        AS CREATE_USER_NAME 
       ,CREATE_USER_ORG_ID      AS CREATE_USER_ORG_ID 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
       ,FR_ID                   AS FR_ID 
       ,CUST_NAME               AS CUST_NAME 
   FROM TMP_OCRM_F_CI_GROUP_INFO A                             --集团信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_GROUP_INFO = sqlContext.sql(sql)
dfn="OCRM_F_CI_GROUP_INFO/"+V_DT+".parquet"
OCRM_F_CI_GROUP_INFO.cache()
nrows = OCRM_F_CI_GROUP_INFO.count()
OCRM_F_CI_GROUP_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_GROUP_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_GROUP_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_GROUP_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT DISTINCT CAST(monotonically_increasing_id() AS BIGINT)        AS ID 
       ,'1'                     AS GROUP_TYPE 
       ,A2.ENTERPRISENAME       AS GROUP_NAME 
       ,''                    AS GROUP_STATUS 
       ,A1.RELATIVEID           AS GROUP_ROOT_CUST_ID 
       ,''                    AS UPDATE_DATE 
       ,''                    AS GROUP_MEMO 
       ,CONCAT('A00',monotonically_increasing_id())  AS GROUP_NO 
       ,A2.UPDATEUSERID         AS UPDATE_USER_ID 
       ,''                    AS GROUP_HOST_ORG_NO 
       ,A2.REGISTERADD          AS GROUP_ROOT_ADDRESS 
       ,CASE WHEN A2.SETUPDATE IS NULL THEN '' ELSE CONCAT(SUBSTR(A2.SETUPDATE,1,4),'-',SUBSTR(A2.SETUPDATE,5,2),'-',SUBSTR(A2.SETUPDATE,7,2)) END AS CREATA_DATE 
       ,A1.INPUTUSERID          AS CREATE_USER_ID 
       ,''                    AS CREATE_USER_NAME 
       ,A1.INPUTORGID           AS CREATE_USER_ORG_ID 
       ,V_DT                  AS ODS_ST_DATE 
       ,NVL(A1.FR_ID, 'UNK')                       AS FR_ID 
       ,A2.ENTERPRISENAME       AS CUST_NAME 
   FROM F_CI_XDXT_GROUP_RELATIVE A1                            --集团客户成员关联表
  INNER JOIN F_CI_XDXT_ENT_INFO A2                             --企业基本信息
     ON A1.RELATIVEID           = A2.CUSTOMERID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_GROUP_INFO = sqlContext.sql(sql)
dfn="OCRM_F_CI_GROUP_INFO/"+V_DT+".parquet"
OCRM_F_CI_GROUP_INFO.cache()
nrows = OCRM_F_CI_GROUP_INFO.count()
OCRM_F_CI_GROUP_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_GROUP_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_GROUP_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_GROUP_INFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_GROUP_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_GROUP_INFO_BK/")
