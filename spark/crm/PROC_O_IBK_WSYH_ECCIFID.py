#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_ECCIFID').setMaster(sys.argv[2])
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

O_CI_WSYH_ECCIFID = sqlContext.read.parquet(hdfs+'/O_CI_WSYH_ECCIFID/*')
O_CI_WSYH_ECCIFID.registerTempTable("O_CI_WSYH_ECCIFID")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CIFSEQ                AS CIFSEQ 
       ,A.IDTYPE                AS IDTYPE 
       ,A.IDNO                  AS IDNO 
       ,A.EFFDATE               AS EFFDATE 
       ,A.EXPDATE               AS EXPDATE 
       ,A.PRIMARYFLAG           AS PRIMARYFLAG 
       ,A.CREATEUSERSEQ         AS CREATEUSERSEQ 
       ,A.CREATEDEPTSEQ         AS CREATEDEPTSEQ 
       ,A.CREATETIME            AS CREATETIME 
       ,A.UPDATEUSERSEQ         AS UPDATEUSERSEQ 
       ,A.UPDATEDEPTSEQ         AS UPDATEDEPTSEQ 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'IBK'                   AS ODS_SYS_ID 
   FROM O_CI_WSYH_ECCIFID A                                    --客户证件表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECCIFID_INNTMP1 = sqlContext.sql(sql)
F_CI_WSYH_ECCIFID_INNTMP1.registerTempTable("F_CI_WSYH_ECCIFID_INNTMP1")

F_CI_WSYH_ECCIFID = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFID_BK/'+V_DT_LD+'.parquet/*')
F_CI_WSYH_ECCIFID.registerTempTable("F_CI_WSYH_ECCIFID")

sql = """
 SELECT DST.CIFSEQ                AS CIFSEQ 
       ,DST.IDTYPE                AS IDTYPE 
       ,DST.IDNO                  AS IDNO 
       ,DST.EFFDATE               AS EFFDATE 
       ,DST.EXPDATE               AS EXPDATE 
       ,DST.PRIMARYFLAG           AS PRIMARYFLAG 
       ,DST.CREATEUSERSEQ         AS CREATEUSERSEQ 
       ,DST.CREATEDEPTSEQ         AS CREATEDEPTSEQ 
       ,DST.CREATETIME            AS CREATETIME 
       ,DST.UPDATEUSERSEQ         AS UPDATEUSERSEQ 
       ,DST.UPDATEDEPTSEQ         AS UPDATEDEPTSEQ 
       ,DST.UPDATETIME            AS UPDATETIME 
       ,DST.FR_ID                 AS FR_ID 
       ,DST.ODS_ST_DATE                    AS ODS_ST_DATE 
       ,DST.ODS_SYS_ID                   AS ODS_SYS_ID 
   FROM F_CI_WSYH_ECCIFID DST 
   LEFT JOIN F_CI_WSYH_ECCIFID_INNTMP1 SRC 
     ON SRC.CIFSEQ          = DST.CIFSEQ 
	 AND  SRC.IDTYPE          = DST.IDTYPE 
	 AND  SRC.IDNO          = DST.IDNO 
  WHERE SRC.CIFSEQ IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECCIFID_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_WSYH_ECCIFID/"+V_DT+".parquet"
UNION=F_CI_WSYH_ECCIFID_INNTMP2.unionAll(F_CI_WSYH_ECCIFID_INNTMP1)
F_CI_WSYH_ECCIFID_INNTMP1.cache()
F_CI_WSYH_ECCIFID_INNTMP2.cache()
nrowsi = F_CI_WSYH_ECCIFID_INNTMP1.count()
nrowsa = F_CI_WSYH_ECCIFID_INNTMP2.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECCIFID/*.parquet ")
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_WSYH_ECCIFID_INNTMP1.unpersist()
F_CI_WSYH_ECCIFID_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_WSYH_ECCIFID lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)

#备份

ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECCIFID_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CI_WSYH_ECCIFID/"+V_DT+".parquet /"+dbname+"/F_CI_WSYH_ECCIFID_BK/")
