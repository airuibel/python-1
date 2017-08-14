#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_BBSP_DISC_DATAPROVIDE_QUERY').setMaster(sys.argv[2])
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

O_TX_BBSP_DISC_DATAPROVIDE_QUERY = sqlContext.read.parquet(hdfs+'/O_TX_BBSP_DISC_DATAPROVIDE_QUERY/*')
O_TX_BBSP_DISC_DATAPROVIDE_QUERY.registerTempTable("O_TX_BBSP_DISC_DATAPROVIDE_QUERY")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,BANK_CODE               AS BANK_CODE 
       ,BILL_NO                 AS BILL_NO 
       ,PROD_NO                 AS PROD_NO 
       ,PROD_NAME               AS PROD_NAME 
       ,IN_FLG                  AS IN_FLG 
       ,START_DT                AS START_DT 
       ,END_DT                  AS END_DT 
       ,BILL_TYPE               AS BILL_TYPE 
       ,RATE                    AS RATE 
       ,RATE_TYPE               AS RATE_TYPE 
       ,STATUS                  AS STATUS 
       ,END_PROD_NO             AS END_PROD_NO 
       ,END_FLG                 AS END_FLG 
       ,BILL_MONEY              AS BILL_MONEY 
       ,REMA_MONEY              AS REMA_MONEY 
       ,BRANCH_NO               AS BRANCH_NO 
       ,CUST_NO                 AS CUST_NO 
       ,CUST_NAME               AS CUST_NAME 
       ,SUBJECT_NO              AS SUBJECT_NO 
       ,INTEREST                AS INTEREST 
       ,PROV_END_DT             AS PROV_END_DT 
       ,REAL_END_DT             AS REAL_END_DT 
       ,JITI_DT                 AS JITI_DT 
       ,REMA_INTEREST           AS REMA_INTEREST 
       ,ACPT_DT                 AS ACPT_DT 
       ,DUE_DT                  AS DUE_DT 
       ,BACK_END_DT             AS BACK_END_DT 
       ,PROM_NOTE_NO            AS PROM_NOTE_NO 
       ,MAGR_NO                 AS MAGR_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'BBSP'                  AS ODS_SYS_ID 
   FROM O_TX_BBSP_DISC_DATAPROVIDE_QUERY A                     --票据余额视图
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_BBSP_DISC_DATAPROVIDE_QUERY = sqlContext.sql(sql)
F_TX_BBSP_DISC_DATAPROVIDE_QUERY.registerTempTable("F_TX_BBSP_DISC_DATAPROVIDE_QUERY")
dfn="F_TX_BBSP_DISC_DATAPROVIDE_QUERY/"+V_DT+".parquet"
F_TX_BBSP_DISC_DATAPROVIDE_QUERY.cache()
nrows = F_TX_BBSP_DISC_DATAPROVIDE_QUERY.count()
F_TX_BBSP_DISC_DATAPROVIDE_QUERY.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_TX_BBSP_DISC_DATAPROVIDE_QUERY.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_BBSP_DISC_DATAPROVIDE_QUERY/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_BBSP_DISC_DATAPROVIDE_QUERY lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
