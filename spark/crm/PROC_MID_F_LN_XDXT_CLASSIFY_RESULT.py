#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MID_F_LN_XDXT_CLASSIFY_RESULT').setMaster(sys.argv[2])
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

F_LN_XDXT_CLASSIFY_RESULT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_CLASSIFY_RESULT/*')
F_LN_XDXT_CLASSIFY_RESULT.registerTempTable("F_LN_XDXT_CLASSIFY_RESULT")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
SELECT CAST(B.OBJECTNO AS VARCHAR(32))                       AS OBJECTNO           
       ,CAST('' AS VARCHAR(10))                        AS ACCOUNTMONTH       
       ,CAST('' AS VARCHAR(20))                        AS CUSTOMERID         
       ,CAST('' AS VARCHAR(80))                        AS CUSTOMERNAME       
       ,CAST('' AS VARCHAR(6))                         AS CERTTYPE           
       ,CAST('' AS VARCHAR(10))                        AS CUSTOMERTYPE       
       ,CAST('' AS VARCHAR(18))                        AS BUSINESSTYPE       
       ,CAST('' AS DECIMAL(24))                        AS CURRENTBALANCE     
       ,CAST('' AS DECIMAL(24))                        AS BUSINESSBALANCE    
       ,CAST('' AS VARCHAR(6))                         AS FIRSTRESULT        
       ,CAST('' AS VARCHAR(6))                         AS FIRSTRESULTRESOURCE
       ,CAST('' AS VARCHAR(6))                         AS SECONDRESULT       
       ,CAST('' AS VARCHAR(6))                         AS MODELRESULT        
       ,CAST('' AS VARCHAR(6))                         AS LASTFIVERESULT     
       ,CAST('' AS VARCHAR(6))                         AS LASTTENRESULT      
       ,CAST(B.CURRENTFIVERESULT AS VARCHAR(6))        AS CURRENTFIVERESULT  
       ,CAST(B.CURRENTTENRESULT AS VARCHAR(6))         AS CURRENTTENRESULT   
       ,CAST('' AS VARCHAR(6))                         AS FINALLYRESULT      
       ,CAST('' AS VARCHAR(6))                         AS CONFIRMTYPE        
       ,CAST('' AS VARCHAR(6))                         AS CLASSIFYTYPE       
       ,CAST('' AS VARCHAR(20))                        AS USERID             
       ,CAST('' AS VARCHAR(20))                        AS ORGID              
       ,CAST('' AS VARCHAR(10))                        AS FINISHDATE         
       ,CAST('' AS VARCHAR(10))                        AS LASTRESULT         
       ,CAST('' AS VARCHAR(10))                        AS UPDATEDATE         
       ,CAST('' AS VARCHAR(10))                        AS ODS_ST_DATE        
       ,CAST('' AS VARCHAR(5))                         AS ODS_SYS_ID         
       ,CAST('' AS VARCHAR(5))                         AS FR_ID              
FROM  F_LN_XDXT_CLASSIFY_RESULT B
			JOIN (SELECT MAX(ACCOUNTMONTH) ACCOUNTMONTH,OBJECTNO
									 FROM F_LN_XDXT_CLASSIFY_RESULT
                   WHERE ODS_ST_DATE=V_DT
				GROUP BY OBJECTNO)C
		ON B.ACCOUNTMONTH = C.ACCOUNTMONTH AND B.OBJECTNO = C.OBJECTNO
GROUP BY B.OBJECTNO,B.CURRENTFIVERESULT,B.CURRENTTENRESULT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_F_LN_XDXT_CLASSIFY_RESULT = sqlContext.sql(sql)
MID_F_LN_XDXT_CLASSIFY_RESULT.registerTempTable("MID_F_LN_XDXT_CLASSIFY_RESULT")
dfn="MID_F_LN_XDXT_CLASSIFY_RESULT/"+V_DT+".parquet"
MID_F_LN_XDXT_CLASSIFY_RESULT.cache()
nrows = MID_F_LN_XDXT_CLASSIFY_RESULT.count()
MID_F_LN_XDXT_CLASSIFY_RESULT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_F_LN_XDXT_CLASSIFY_RESULT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_F_LN_XDXT_CLASSIFY_RESULT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_F_LN_XDXT_CLASSIFY_RESULT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
