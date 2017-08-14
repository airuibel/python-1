#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CRE_INTRO').setMaster(sys.argv[2])
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

#删除当天的
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_INTRO/"+V_DT+".parquet")
#----------来源表---------------
O_CI_INTRO = sqlContext.read.parquet(hdfs+'/O_CI_INTRO/*')
O_CI_INTRO.registerTempTable("O_CI_INTRO")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT BANK                    AS BANK 
       ,INP_DATE                AS INP_DATE 
       ,INP_TIME                AS INP_TIME 
       ,CARD_NBR                AS CARD_NBR 
       ,IAPP_SEQ                AS IAPP_SEQ 
       ,AB_BRANCH               AS AB_BRANCH 
       ,APP_BATCH               AS APP_BATCH 
       ,INTR_NBR                AS INTR_NBR 
       ,INTR_NAME               AS INTR_NAME 
       ,INTR_CNBR               AS INTR_CNBR 
       ,INTR_RECOM              AS INTR_RECOM 
       ,INTR_RL                 AS INTR_RL 
       ,INP_EMP                 AS INP_EMP 
       ,CHANGE_EMP              AS CHANGE_EMP 
       ,INTR_QC                 AS INTR_QC 
       ,INTRO_TEXT              AS INTRO_TEXT 
       ,INTRO_CODE              AS INTRO_CODE 
       ,ACHK_NBR                AS ACHK_NBR 
       ,ADEC_NBR                AS ADEC_NBR 
       ,AVAL_NBR                AS AVAL_NBR 
       ,ORG_NO                  AS ORG_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CRE'                   AS ODS_SYS_ID 
   FROM O_CI_INTRO A                                           --推荐人信息修改
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_INTRO = sqlContext.sql(sql)
F_CI_INTRO.registerTempTable("F_CI_INTRO")
dfn="F_CI_INTRO/"+V_DT+".parquet"
F_CI_INTRO.cache()
nrows = F_CI_INTRO.count()
F_CI_INTRO.write.save(path=hdfs + '/' + dfn, mode='append')
F_CI_INTRO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_INTRO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
