#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_BUSINESS_TYPE').setMaster(sys.argv[2])
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

O_CM_XDXT_BUSINESS_TYPE = sqlContext.read.parquet(hdfs+'/O_CM_XDXT_BUSINESS_TYPE/*')
O_CM_XDXT_BUSINESS_TYPE.registerTempTable("O_CM_XDXT_BUSINESS_TYPE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.TYPENO                AS TYPENO 
       ,A.SORTNO                AS SORTNO 
       ,A.TYPENAME              AS TYPENAME 
       ,A.TYPESORTNO            AS TYPESORTNO 
       ,A.SUBTYPECODE           AS SUBTYPECODE 
       ,A.INFOSET               AS INFOSET 
       ,A.DISPLAYTEMPLET        AS DISPLAYTEMPLET 
       ,A.ATTRIBUTE1            AS ATTRIBUTE1 
       ,A.ATTRIBUTE2            AS ATTRIBUTE2 
       ,A.ATTRIBUTE3            AS ATTRIBUTE3 
       ,A.ATTRIBUTE4            AS ATTRIBUTE4 
       ,A.ATTRIBUTE5            AS ATTRIBUTE5 
       ,A.ATTRIBUTE6            AS ATTRIBUTE6 
       ,A.ATTRIBUTE7            AS ATTRIBUTE7 
       ,A.ATTRIBUTE8            AS ATTRIBUTE8 
       ,A.ATTRIBUTE9            AS ATTRIBUTE9 
       ,A.ATTRIBUTE10           AS ATTRIBUTE10 
       ,A.REMARK                AS REMARK 
       ,A.APPLYDETAILNO         AS APPLYDETAILNO 
       ,A.APPROVEDETAILNO       AS APPROVEDETAILNO 
       ,A.CONTRACTDETAILNO      AS CONTRACTDETAILNO 
       ,A.INPUTUSER             AS INPUTUSER 
       ,A.INPUTORG              AS INPUTORG 
       ,A.INPUTTIME             AS INPUTTIME 
       ,A.UPDATEUSER            AS UPDATEUSER 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.ATTRIBUTE11           AS ATTRIBUTE11 
       ,A.ATTRIBUTE12           AS ATTRIBUTE12 
       ,A.ATTRIBUTE13           AS ATTRIBUTE13 
       ,A.ATTRIBUTE14           AS ATTRIBUTE14 
       ,A.ATTRIBUTE15           AS ATTRIBUTE15 
       ,A.ATTRIBUTE16           AS ATTRIBUTE16 
       ,A.ATTRIBUTE17           AS ATTRIBUTE17 
       ,A.ATTRIBUTE18           AS ATTRIBUTE18 
       ,A.ATTRIBUTE19           AS ATTRIBUTE19 
       ,A.ATTRIBUTE20           AS ATTRIBUTE20 
       ,A.ATTRIBUTE21           AS ATTRIBUTE21 
       ,A.ATTRIBUTE22           AS ATTRIBUTE22 
       ,A.ATTRIBUTE23           AS ATTRIBUTE23 
       ,A.ATTRIBUTE24           AS ATTRIBUTE24 
       ,A.ATTRIBUTE25           AS ATTRIBUTE25 
       ,A.ISINUSE               AS ISINUSE 
       ,A.CORPORATEORGID        AS CORPORATEORGID 
       ,A.PARENTTYPENO          AS PARENTTYPENO 
       ,A.NODETYPE              AS NODETYPE 
       ,A.CORPORGID             AS CORPORGID 
       ,A.PUBLISHED             AS PUBLISHED 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
       ,A.OFFSHEETFLAG          AS OFFSHEETFLAG 
   FROM O_CM_XDXT_BUSINESS_TYPE A                              --资产业务品种表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_XDXT_BUSINESS_TYPE = sqlContext.sql(sql)
F_CM_XDXT_BUSINESS_TYPE.registerTempTable("F_CM_XDXT_BUSINESS_TYPE")
dfn="F_CM_XDXT_BUSINESS_TYPE/"+V_DT+".parquet"
F_CM_XDXT_BUSINESS_TYPE.cache()
nrows = F_CM_XDXT_BUSINESS_TYPE.count()
F_CM_XDXT_BUSINESS_TYPE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_XDXT_BUSINESS_TYPE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_XDXT_BUSINESS_TYPE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_XDXT_BUSINESS_TYPE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
