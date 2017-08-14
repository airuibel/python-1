#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMMSCBST').setMaster(sys.argv[2])
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
O_CM_CBOD_CMMSCBST = sqlContext.read.parquet(hdfs+'/O_CM_CBOD_CMMSCBST/*')
O_CM_CBOD_CMMSCBST.registerTempTable("O_CM_CBOD_CMMSCBST")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_WORD_CLSFN           AS CM_WORD_CLSFN 
       ,CM_FILLER_BST           AS CM_FILLER_BST 
       ,CM_BUSN_TYP             AS CM_BUSN_TYP 
       ,CM_BUSN_TYP_DSCRP       AS CM_BUSN_TYP_DSCRP 
       ,CM_CM_BUSN_TYP_AUTH_CLS AS CM_CM_BUSN_TYP_AUTH_CLS 
       ,CM_TABLE_INT_FLG        AS CM_TABLE_INT_FLG 
       ,CM_LN_BSN_TYP           AS CM_LN_BSN_TYP 
       ,CM_TK_RISK_LN_FLAG      AS CM_TK_RISK_LN_FLAG 
       ,CM_OB_BUS_FLAG          AS CM_OB_BUS_FLAG 
       ,CM_ADVPMT_FLAG          AS CM_ADVPMT_FLAG 
       ,CM_DIS_TYP              AS CM_DIS_TYP 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CM_CBOD_CMMSCBST A                                   --业务别
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_CMMSCBST = sqlContext.sql(sql)
F_CM_CBOD_CMMSCBST.registerTempTable("F_CM_CBOD_CMMSCBST")
dfn="F_CM_CBOD_CMMSCBST/"+V_DT+".parquet"
F_CM_CBOD_CMMSCBST.cache()
nrows = F_CM_CBOD_CMMSCBST.count()
F_CM_CBOD_CMMSCBST.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_CBOD_CMMSCBST.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_CMMSCBST/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_CMMSCBST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
