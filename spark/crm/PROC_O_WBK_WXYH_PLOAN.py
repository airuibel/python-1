#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_WBK_WXYH_PLOAN').setMaster(sys.argv[2])
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

O_LN_WXYH_PLOAN = sqlContext.read.parquet(hdfs+'/O_LN_WXYH_PLOAN/*')
O_LN_WXYH_PLOAN.registerTempTable("O_LN_WXYH_PLOAN")

#任务[12] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.APPLYSEQ              AS APPLYSEQ 
       ,A.APPLYTIME             AS APPLYTIME 
       ,A.CIFSEQ                AS CIFSEQ 
       ,A.LOANPRDID             AS LOANPRDID 
       ,A.CUSTNAME              AS CUSTNAME 
       ,A.ADDRESS               AS ADDRESS 
       ,A.PHONENO               AS PHONENO 
       ,A.EMAIL                 AS EMAIL 
       ,A.PURPOSE               AS PURPOSE 
       ,A.APPLYAMOUNT           AS APPLYAMOUNT 
       ,A.SEX                   AS SEX 
       ,A.CHANNELID             AS CHANNELID 
       ,A.DEPTID                AS DEPTID 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'WBK'                   AS ODS_SYS_ID 
   FROM O_LN_WXYH_PLOAN A                                      --对私贷款申请信息表
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_WXYH_PLOAN = sqlContext.sql(sql)
F_LN_WXYH_PLOAN.registerTempTable("F_LN_WXYH_PLOAN")
dfn="F_LN_WXYH_PLOAN/"+V_DT+".parquet"
F_LN_WXYH_PLOAN.cache()
nrows = F_LN_WXYH_PLOAN.count()

#删除当天数据，支持重跑
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_WXYH_PLOAN/"+V_DT+".parquet")
F_LN_WXYH_PLOAN.write.save(path=hdfs + '/' + dfn, mode='append')
F_LN_WXYH_PLOAN.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_WXYH_PLOAN/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_WXYH_PLOAN lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

