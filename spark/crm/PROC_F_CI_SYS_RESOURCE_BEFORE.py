#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_SYS_RESOURCE_BEFORE').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BEFORE/"+V_DT+".parquet")

F_DP_CBOD_TDACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_TDACNACN/*')
F_DP_CBOD_TDACNACN.registerTempTable("F_DP_CBOD_TDACNACN")
F_DP_CBOD_LNLNSLNS = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_LNLNSLNS/*')
F_DP_CBOD_LNLNSLNS.registerTempTable("F_DP_CBOD_LNLNSLNS")
F_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNACN/*')
F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")
OCRM_F_CI_SYS_RESOURCE_BEFORE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE_BEFORE_BK/'+V_DT_LD+'.parquet/*')
OCRM_F_CI_SYS_RESOURCE_BEFORE.registerTempTable("OCRM_F_CI_SYS_RESOURCE_BEFORE")

#任务[11] 001-01::
V_STEP = V_STEP + 1
#插入定存的账户信息
sql = """
 SELECT ''                    AS ID 
       ,TD_CUST_NO              AS ODS_CUST_ID 
       ,''                    AS ODS_CUST_NAME 
       ,''                    AS SOURCE_CUST_ID 
       ,''                    AS SOURCE_CUST_NAME 
       ,''                    AS CERT_TYPE 
       ,''                    AS CERT_NO 
       ,'CEN' AS ODS_SYS_ID
       ,V_DT                    AS ODS_ST_DATE 
       ,''                    AS ODS_CUST_TYPE 
       ,''                    AS CUST_STAT 
       ,''                    AS BEGIN_DATE 
       ,'2999-12-31'            AS END_DATE 
       ,'110000000000'          AS SYS_ID_FLAG 
       ,A.FR_ID                   AS FR_ID 
   FROM F_DP_CBOD_TDACNACN A                                   --定期存款主档
  WHERE A.ODS_ST_DATE           = V_DT 
    AND  NOT EXISTS ( SELECT 1 FROM OCRM_F_CI_SYS_RESOURCE_BEFORE C WHERE A.TD_CUST_NO=C.ODS_CUST_ID AND C.FR_ID=A.FR_ID )
  GROUP BY A.FR_ID 
       ,TD_CUST_NO """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_BEFORE = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_BEFORE.registerTempTable("OCRM_F_CI_SYS_RESOURCE_BEFORE")
dfn="OCRM_F_CI_SYS_RESOURCE_BEFORE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_BEFORE.cache()
nrows = OCRM_F_CI_SYS_RESOURCE_BEFORE.count()
OCRM_F_CI_SYS_RESOURCE_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_SYS_RESOURCE_BEFORE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

OCRM_F_CI_SYS_RESOURCE_BEFORE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE_BEFORE/*')
OCRM_F_CI_SYS_RESOURCE_BEFORE.registerTempTable("OCRM_F_CI_SYS_RESOURCE_BEFORE")

#任务[11] 001-02::
V_STEP = V_STEP + 1
#插入活存的账户信息
sql = """
 SELECT ''                    AS ID 
       ,SA_CUST_NO              AS ODS_CUST_ID 
       ,''                    AS ODS_CUST_NAME 
       ,''                    AS SOURCE_CUST_ID 
       ,''                    AS SOURCE_CUST_NAME 
       ,''                    AS CERT_TYPE 
       ,''                    AS CERT_NO 
       ,'CEN' AS ODS_SYS_ID
       ,V_DT                    AS ODS_ST_DATE 
       ,''                    AS ODS_CUST_TYPE 
       ,''                    AS CUST_STAT 
       ,''                    AS BEGIN_DATE 
       ,'2999-12-31'            AS END_DATE 
       ,'110000000000'          AS SYS_ID_FLAG 
       ,A.FR_ID                   AS FR_ID 
   FROM F_DP_CBOD_SAACNACN A                                   --活期存款主档
  WHERE A.ODS_ST_DATE           = V_DT 
    AND  NOT EXISTS ( SELECT 1 FROM OCRM_F_CI_SYS_RESOURCE_BEFORE C WHERE  A.SA_CUST_NO=C.ODS_CUST_ID AND C.FR_ID=A.FR_ID )    
  GROUP BY A.FR_ID 
       ,SA_CUST_NO """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_BEFORE = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_BEFORE.registerTempTable("OCRM_F_CI_SYS_RESOURCE_BEFORE")
dfn="OCRM_F_CI_SYS_RESOURCE_BEFORE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_BEFORE.cache()
nrows = OCRM_F_CI_SYS_RESOURCE_BEFORE.count()
OCRM_F_CI_SYS_RESOURCE_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_SYS_RESOURCE_BEFORE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

OCRM_F_CI_SYS_RESOURCE_BEFORE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE_BEFORE/*')
OCRM_F_CI_SYS_RESOURCE_BEFORE.registerTempTable("OCRM_F_CI_SYS_RESOURCE_BEFORE")
#任务[11] 001-03::
V_STEP = V_STEP + 1
#插入贷款的账户信息 
sql = """
 SELECT ''                    AS ID 
       ,LN_CUST_NO              AS ODS_CUST_ID 
       ,''                    AS ODS_CUST_NAME 
       ,''                    AS SOURCE_CUST_ID 
       ,''                    AS SOURCE_CUST_NAME 
       ,''                    AS CERT_TYPE 
       ,''                    AS CERT_NO 
       ,'CEN' AS ODS_SYS_ID
       ,V_DT                    AS ODS_ST_DATE 
       ,''                    AS ODS_CUST_TYPE 
       ,''                    AS CUST_STAT 
       ,''                    AS BEGIN_DATE 
       ,'2999-12-31'            AS END_DATE 
       ,'110000000000'          AS SYS_ID_FLAG 
       ,A.FR_ID                   AS FR_ID 
   FROM F_DP_CBOD_LNLNSLNS A                                   --放款主档
  WHERE A.ODS_ST_DATE           = V_DT 
    AND  NOT EXISTS ( SELECT 1 FROM OCRM_F_CI_SYS_RESOURCE_BEFORE C WHERE A.LN_CUST_NO=C.ODS_CUST_ID AND C.FR_ID=A.FR_ID )  
  GROUP BY A.FR_ID 
       ,LN_CUST_NO """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_BEFORE = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_BEFORE.registerTempTable("OCRM_F_CI_SYS_RESOURCE_BEFORE")
dfn="OCRM_F_CI_SYS_RESOURCE_BEFORE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_BEFORE.cache()
nrows = OCRM_F_CI_SYS_RESOURCE_BEFORE.count()
OCRM_F_CI_SYS_RESOURCE_BEFORE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_SYS_RESOURCE_BEFORE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE_BEFORE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
