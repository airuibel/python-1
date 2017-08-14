#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_AFA_PXY_CRPINFO').setMaster(sys.argv[2])
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

O_CI_AFA_PXY_CRPINFO = sqlContext.read.parquet(hdfs+'/O_CI_AFA_PXY_CRPINFO/*')
O_CI_AFA_PXY_CRPINFO.registerTempTable("O_CI_AFA_PXY_CRPINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT SYSID                   AS SYSID 
       ,UNITNO                  AS UNITNO 
       ,CRPCOD                  AS CRPCOD 
       ,CRPNAM                  AS CRPNAM 
       ,ACCNO                   AS ACCNO 
       ,PROTOCOLNO              AS PROTOCOLNO 
       ,CRPADDR                 AS CRPADDR 
       ,CRPTELNO                AS CRPTELNO 
       ,NAME                    AS NAME 
       ,TELNO                   AS TELNO 
       ,STATUS                  AS STATUS 
       ,TELLERNO                AS TELLERNO 
       ,BRNO                    AS BRNO 
       ,WORKDATE                AS WORKDATE 
       ,DELTELLERNO             AS DELTELLERNO 
       ,DELDAT                  AS DELDAT 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'AFA'                   AS ODS_SYS_ID 
   FROM O_CI_AFA_PXY_CRPINFO A                                 --单位信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_AFA_PXY_CRPINFO = sqlContext.sql(sql)
F_CI_AFA_PXY_CRPINFO.registerTempTable("F_CI_AFA_PXY_CRPINFO")
dfn="F_CI_AFA_PXY_CRPINFO/"+V_DT+".parquet"
F_CI_AFA_PXY_CRPINFO.cache()
nrows = F_CI_AFA_PXY_CRPINFO.count()
F_CI_AFA_PXY_CRPINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_AFA_PXY_CRPINFO.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_AFA_PXY_CRPINFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_AFA_PXY_CRPINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
