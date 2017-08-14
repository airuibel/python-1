#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_ECCIFPRODSET').setMaster(sys.argv[2])
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

O_CI_WSYH_ECCIFPRODSET = sqlContext.read.parquet(hdfs+'/O_CI_WSYH_ECCIFPRODSET/*')
O_CI_WSYH_ECCIFPRODSET.registerTempTable("O_CI_WSYH_ECCIFPRODSET")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CIFSEQ                AS CIFSEQ 
       ,A.PRDSETID              AS PRDSETID 
       ,A.MCHANNELID            AS MCHANNELID 
       ,A.CREATEUSERSEQ         AS CREATEUSERSEQ 
       ,A.CREATEDEPTSEQ         AS CREATEDEPTSEQ 
       ,A.CREATETIME            AS CREATETIME 
       ,A.UPDATEUSERSEQ         AS UPDATEUSERSEQ 
       ,A.UPDATEDEPTSEQ         AS UPDATEDEPTSEQ 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'IBK'                   AS ODS_SYS_ID 
   FROM O_CI_WSYH_ECCIFPRODSET A                               --F_CI_WSYH_ECCIFPRODSET
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECCIFPRODSET = sqlContext.sql(sql)
F_CI_WSYH_ECCIFPRODSET.registerTempTable("F_CI_WSYH_ECCIFPRODSET")
dfn="F_CI_WSYH_ECCIFPRODSET/"+V_DT+".parquet"
F_CI_WSYH_ECCIFPRODSET.cache()
nrows = F_CI_WSYH_ECCIFPRODSET.count()
F_CI_WSYH_ECCIFPRODSET.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_WSYH_ECCIFPRODSET.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECCIFPRODSET/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_WSYH_ECCIFPRODSET lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
