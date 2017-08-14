#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_DOC_LIBRARY').setMaster(sys.argv[2])
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

O_CI_XDXT_DOC_LIBRARY = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_DOC_LIBRARY/*')
O_CI_XDXT_DOC_LIBRARY.registerTempTable("O_CI_XDXT_DOC_LIBRARY")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.DOCNO                 AS DOCNO 
       ,A.DOCTYPE               AS DOCTYPE 
       ,A.DOCTITLE              AS DOCTITLE 
       ,A.DOCLENGTH             AS DOCLENGTH 
       ,A.DOCIMPORTANCE         AS DOCIMPORTANCE 
       ,A.DOCSECRET             AS DOCSECRET 
       ,A.DOCSTAGE              AS DOCSTAGE 
       ,A.DOCSOURCE             AS DOCSOURCE 
       ,A.DOCUNIT               AS DOCUNIT 
       ,A.DOCDATE               AS DOCDATE 
       ,A.DOCORGANIZER          AS DOCORGANIZER 
       ,A.DOCKEYWORD            AS DOCKEYWORD 
       ,A.DOCABSTRACT           AS DOCABSTRACT 
       ,A.DOCLOCATION           AS DOCLOCATION 
       ,A.DOCATTRIBUTE          AS DOCATTRIBUTE 
       ,A.USERID                AS USERID 
       ,A.USERNAME              AS USERNAME 
       ,A.ORGID                 AS ORGID 
       ,A.ORGNAME               AS ORGNAME 
       ,A.TRANSFORMTIME         AS TRANSFORMTIME 
       ,A.INPUTUSER             AS INPUTUSER 
       ,A.INPUTORG              AS INPUTORG 
       ,A.INPUTTIME             AS INPUTTIME 
       ,A.UPDATEUSER            AS UPDATEUSER 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.DOCFLAG               AS DOCFLAG 
       ,A.SORTNO                AS SORTNO 
       ,A.REMARK                AS REMARK 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_DOC_LIBRARY A                                --文档资料库表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_DOC_LIBRARY = sqlContext.sql(sql)
F_CI_XDXT_DOC_LIBRARY.registerTempTable("F_CI_XDXT_DOC_LIBRARY")
dfn="F_CI_XDXT_DOC_LIBRARY/"+V_DT+".parquet"
F_CI_XDXT_DOC_LIBRARY.cache()
nrows = F_CI_XDXT_DOC_LIBRARY.count()
F_CI_XDXT_DOC_LIBRARY.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_DOC_LIBRARY.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_DOC_LIBRARY/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_DOC_LIBRARY lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
