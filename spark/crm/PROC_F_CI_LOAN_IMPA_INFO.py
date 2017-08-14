#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_LOAN_IMPA_INFO').setMaster(sys.argv[2])
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

F_LN_XDXT_GUARANTY_INFO = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_GUARANTY_INFO/*')
F_LN_XDXT_GUARANTY_INFO.registerTempTable("F_LN_XDXT_GUARANTY_INFO")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(''    AS BIGINT)       AS ID
              ,''                   AS GUARANTYID
              ,''                   AS GUARANTYTYPE
              ,''                   AS GUARANTYSTATUS
              ,''                   AS OWNERID
              ,''                   AS OWNERNAME
              ,''                   AS OWNERTYPE
              ,CAST(''              AS DECIMAL(24,6)) AS RATE
              ,''                   AS CUSTGUARANTYTYPE
              ,''                   AS SUBJECTNO
              ,''                   AS RELATIVEACCOUNT
              ,''                   AS GUARANTYRIGHTID
              ,''                   AS OTHERGUARANTYRIGHT
              ,''                   AS GUARANTYNAME
              ,''                   AS GUARANTYSUBTYPE
              ,''                   AS GUARANTYOWNWAY
              ,''                   AS GUARANTYUSING
              ,''                   AS GUARANTYLOCATION
              ,CAST(''                  AS DECIMAL(24,6)) AS GUARANTYAMOUNT
              ,CAST(''                  AS DECIMAL(24,6)) AS GUARANTYAMOUNT1
              ,CAST(''                  AS DECIMAL(24,6)) AS GUARANTYAMOUNT2
              ,''                   AS GUARANTYRESOUCE
              ,''                   AS GUARANTYDATE
              ,''                   AS BEGINDATE
              ,''                   AS OWNERTIME
              ,''                   AS GUARANTYDESCRIPT
              ,''                   AS ABOUTOTHERID1
              ,''                   AS ABOUTOTHERID2
              ,''                   AS ABOUTOTHERID3
              ,''                   AS ABOUTOTHERID4
              ,''                   AS PURPOSE
              ,CAST(''                  AS DECIMAL(24,6)) AS ABOUTSUM1
              ,CAST(''                  AS DECIMAL(24,6)) AS ABOUTSUM2
              ,CAST(       A.ABOUTRATE         AS DECIMAL(24,6)) AS ABOUTRATE
              ,       A.GUARANTYANA        AS GUARANTYANA
              ,CAST(       A.GUARANTYPRICE     AS DECIMAL(24,6)) AS GUARANTYPRICE
              ,       A.EVALMETHOD         AS EVALMETHOD
              ,       A.EVALORGID          AS EVALORGID
              ,       A.EVALORGNAME        AS EVALORGNAME
              ,       A.EVALDATE           AS EVALDATE
              ,CAST(       A.EVALNETVALUE      AS DECIMAL(24,6)) AS EVALNETVALUE
              ,CAST(       A.CONFIRMVALUE      AS DECIMAL(24,6)) AS CONFIRMVALUE
              ,CAST(       A.GUARANTYRATE      AS DECIMAL(24,6)) AS GUARANTYRATE
              ,       A.THIRDPARTY1        AS THIRDPARTY1
              ,       A.THIRDPARTY2        AS THIRDPARTY2
              ,       A.THIRDPARTY3        AS THIRDPARTY3
              ,       A.GUARANTYDESCRIBE1  AS GUARANTYDESCRIBE1
              ,       A.GUARANTYDESCRIBE2  AS GUARANTYDESCRIBE2
              ,       A.GUARANTYDESCRIBE3  AS GUARANTYDESCRIBE3
              ,       A.FLAG1              AS FLAG1
              ,       A.FLAG2              AS FLAG2
              ,       A.FLAG3              AS FLAG3
              ,       A.FLAG4              AS FLAG4
              ,       A.GUARANTYREGNO      AS GUARANTYREGNO
              ,       A.GUARANTYREGORG     AS GUARANTYREGORG
              ,       A.GUARANTYREGDATE    AS GUARANTYREGDATE
              ,       A.GUARANTYWODATE     AS GUARANTYWODATE
              ,       A.INSURECERTNO       AS INSURECERTNO
              ,       A.OTHERASSUMPSIT     AS OTHERASSUMPSIT
              ,       A.INPUTORGID         AS INPUTORGID
              ,       A.INPUTUSERID        AS INPUTUSERID
              ,       A.INPUTDATE          AS INPUTDATE
              ,       A.UPDATEUSERID       AS UPDATEUSERID
              ,''                   AS UPDATEDATE
              ,       A.REMARK             AS REMARK
              ,       A.ODS_ST_DATE        AS ODS_ST_DATE
   FROM F_LN_XDXT_GUARANTY_INFO A                              --押品基本信息
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.OWNERID               = B.SOURCE_CUST_ID 
    AND B.ODS_SYS_ID            = 'LNA' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_LOAN_IMPA_INFO = sqlContext.sql(sql)
ACRM_F_CI_LOAN_IMPA_INFO.registerTempTable("ACRM_F_CI_LOAN_IMPA_INFO")
dfn="ACRM_F_CI_LOAN_IMPA_INFO/"+V_DT+".parquet"
ACRM_F_CI_LOAN_IMPA_INFO.cache()
nrows = ACRM_F_CI_LOAN_IMPA_INFO.count()
ACRM_F_CI_LOAN_IMPA_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_LOAN_IMPA_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_LOAN_IMPA_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_LOAN_IMPA_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
