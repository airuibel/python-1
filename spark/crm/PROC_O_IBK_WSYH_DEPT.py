#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_DEPT').setMaster(sys.argv[2])
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
#----------来源表---------------
O_TX_WSYH_DEPT = sqlContext.read.parquet(hdfs+'/O_TX_WSYH_DEPT/*')
O_TX_WSYH_DEPT.registerTempTable("O_TX_WSYH_DEPT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.DEPTSEQ               AS DEPTSEQ 
       ,A.DEPTID                AS DEPTID 
       ,A.DEPTNAME              AS DEPTNAME 
       ,A.ADDR                  AS ADDR 
       ,A.ZIPCODE               AS ZIPCODE 
       ,A.PHONE                 AS PHONE 
       ,A.EMAIL                 AS EMAIL 
       ,A.DEPTSTATE             AS DEPTSTATE 
       ,A.DEPTTYPE              AS DEPTTYPE 
       ,A.DEPTLEVEL             AS DEPTLEVEL 
       ,A.EXTLEVEL              AS EXTLEVEL 
       ,A.DEPTCLASS             AS DEPTCLASS 
       ,A.PARENTDEPTSEQ         AS PARENTDEPTSEQ 
       ,A.CITYCODE              AS CITYCODE 
       ,A.PROVCD                AS PROVCD 
       ,A.DEPTAUTH              AS DEPTAUTH 
       ,A.LPNO                  AS LPNO 
       ,A.FR_ID                 AS FR_ID 
       ,'IBK'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
   FROM O_TX_WSYH_DEPT A                                       --网点表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_DEPT = sqlContext.sql(sql)
F_TX_WSYH_DEPT.registerTempTable("F_TX_WSYH_DEPT")
dfn="F_TX_WSYH_DEPT/"+V_DT+".parquet"
F_TX_WSYH_DEPT.cache()
nrows = F_TX_WSYH_DEPT.count()
F_TX_WSYH_DEPT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_TX_WSYH_DEPT.unpersist()
O_TX_WSYH_DEPT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_DEPT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_WSYH_DEPT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
