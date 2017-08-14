#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_AFA_BIS_BENEFITINFO').setMaster(sys.argv[2])
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

#清除数据，支持重跑
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_NI_AFA_BIS_BENEFITINFO/"+V_DT+".parquet")

O_NI_AFA_BIS_BENEFITINFO = sqlContext.read.parquet(hdfs+'/O_NI_AFA_BIS_BENEFITINFO/*')
O_NI_AFA_BIS_BENEFITINFO.registerTempTable("O_NI_AFA_BIS_BENEFITINFO")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT UNITNO                  AS UNITNO 
       ,POLICY                  AS POLICY 
       ,WORKDATE                AS WORKDATE 
       ,AGENTSERIALNO           AS AGENTSERIALNO 
       ,WINCOUNT                AS WINCOUNT 
       ,WINNAME                 AS WINNAME 
       ,WINSEX                  AS WINSEX 
       ,WINRELATION             AS WINRELATION 
       ,WINIDTYPE               AS WINIDTYPE 
       ,WINIDNO                 AS WINIDNO 
       ,WINBIRTHDAY             AS WINBIRTHDAY 
       ,WINSQU                  AS WINSQU 
       ,WINRATE                 AS WINRATE 
       ,WINADDR                 AS WINADDR 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'AFA'                   AS ODS_SYS_ID 
   FROM O_NI_AFA_BIS_BENEFITINFO A                             --受益人分配表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_NI_AFA_BIS_BENEFITINFO = sqlContext.sql(sql)
F_NI_AFA_BIS_BENEFITINFO.registerTempTable("F_NI_AFA_BIS_BENEFITINFO")
dfn="F_NI_AFA_BIS_BENEFITINFO/"+V_DT+".parquet"
F_NI_AFA_BIS_BENEFITINFO.cache()
nrows = F_NI_AFA_BIS_BENEFITINFO.count()
F_NI_AFA_BIS_BENEFITINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_NI_AFA_BIS_BENEFITINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_NI_AFA_BIS_BENEFITINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
