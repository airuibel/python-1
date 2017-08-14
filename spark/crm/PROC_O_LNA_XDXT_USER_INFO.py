#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_USER_INFO').setMaster(sys.argv[2])
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

O_CM_XDXT_USER_INFO = sqlContext.read.parquet(hdfs+'/O_CM_XDXT_USER_INFO/*')
O_CM_XDXT_USER_INFO.registerTempTable("O_CM_XDXT_USER_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.USERID                AS USERID 
       ,A.LOGINID               AS LOGINID 
       ,A.USERNAME              AS USERNAME 
       ,A.PASSWORD              AS PASSWORD 
       ,A.BELONGORG             AS BELONGORG 
       ,A.ATTRIBUTE1            AS ATTRIBUTE1 
       ,A.ATTRIBUTE2            AS ATTRIBUTE2 
       ,A.ATTRIBUTE3            AS ATTRIBUTE3 
       ,A.ATTRIBUTE4            AS ATTRIBUTE4 
       ,A.ATTRIBUTE5            AS ATTRIBUTE5 
       ,A.ATTRIBUTE6            AS ATTRIBUTE6 
       ,A.ATTRIBUTE7            AS ATTRIBUTE7 
       ,A.ATTRIBUTE8            AS ATTRIBUTE8 
       ,A.ATTRIBUTE             AS ATTRIBUTE 
       ,A.DESCRIBE1             AS DESCRIBE1 
       ,A.DESCRIBE2             AS DESCRIBE2 
       ,A.DESCRIBE3             AS DESCRIBE3 
       ,A.DESCRIBE4             AS DESCRIBE4 
       ,A.STATUS                AS STATUS 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CERTID                AS CERTID 
       ,A.COMPANYTEL            AS COMPANYTEL 
       ,A.MOBILETEL             AS MOBILETEL 
       ,A.EMAIL                 AS EMAIL 
       ,A.ACCOUNTID             AS ACCOUNTID 
       ,A.ID1                   AS ID1 
       ,A.ID2                   AS ID2 
       ,A.SUM1                  AS SUM1 
       ,A.SUM2                  AS SUM2 
       ,A.INPUTORG              AS INPUTORG 
       ,A.INPUTUSER             AS INPUTUSER 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.INPUTTIME             AS INPUTTIME 
       ,A.UPDATEUSER            AS UPDATEUSER 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.REMARK                AS REMARK 
       ,A.BIRTHDAY              AS BIRTHDAY 
       ,A.GENDER                AS GENDER 
       ,A.FAMILYADD             AS FAMILYADD 
       ,A.EDUCATIONALBG         AS EDUCATIONALBG 
       ,A.AMLEVEL               AS AMLEVEL 
       ,A.TITLE                 AS TITLE 
       ,A.EDUCATIONEXP          AS EDUCATIONEXP 
       ,A.VOCATIONEXP           AS VOCATIONEXP 
       ,A.POSITION              AS POSITION 
       ,A.QUALIFICATION         AS QUALIFICATION 
       ,A.NTID                  AS NTID 
       ,A.BELONGTEAM            AS BELONGTEAM 
       ,A.LOB                   AS LOB 
       ,A.FAILCOUNT             AS FAILCOUNT 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CM_XDXT_USER_INFO A                                  --用户信息
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_XDXT_USER_INFO = sqlContext.sql(sql)
F_CM_XDXT_USER_INFO.registerTempTable("F_CM_XDXT_USER_INFO")
dfn="F_CM_XDXT_USER_INFO/"+V_DT+".parquet"
F_CM_XDXT_USER_INFO.cache()
nrows = F_CM_XDXT_USER_INFO.count()
F_CM_XDXT_USER_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_XDXT_USER_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_XDXT_USER_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_XDXT_USER_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
