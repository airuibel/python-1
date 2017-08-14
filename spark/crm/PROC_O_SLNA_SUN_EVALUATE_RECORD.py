#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_SLNA_SUN_EVALUATE_RECORD').setMaster(sys.argv[2])
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

O_LN_SUN_EVALUATE_RECORD = sqlContext.read.parquet(hdfs+'/O_LN_SUN_EVALUATE_RECORD/*')
O_LN_SUN_EVALUATE_RECORD.registerTempTable("O_LN_SUN_EVALUATE_RECORD")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.SERIALNO              AS SERIALNO 
       ,A.OBJECTTYPE            AS OBJECTTYPE 
       ,A.OBJECTNO              AS OBJECTNO 
       ,A.CUSTOMERNAME          AS CUSTOMERNAME 
       ,A.CUSTOMERID            AS CUSTOMERID 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CERTID                AS CERTID 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.UPDATEUSERID          AS UPDATEUSERID 
       ,A.UPDATEORGID           AS UPDATEORGID 
       ,A.UPDATEDATE            AS UPDATEDATE 
       ,A.ACCOUNTMONTH          AS ACCOUNTMONTH 
       ,A.MODELNO               AS MODELNO 
       ,A.EVALUATEDATE          AS EVALUATEDATE 
       ,A.EVALUATESCORE         AS EVALUATESCORE 
       ,A.EVALUATERESULT        AS EVALUATERESULT 
       ,A.ORGID                 AS ORGID 
       ,A.USERID                AS USERID 
       ,A.COGNDATE              AS COGNDATE 
       ,A.COGNSCORE             AS COGNSCORE 
       ,A.COGNRESULT            AS COGNRESULT 
       ,A.COGNORGID             AS COGNORGID 
       ,A.COGNUSERID            AS COGNUSERID 
       ,A.REMARK                AS REMARK 
       ,A.COGNREASON            AS COGNREASON 
       ,A.COGNRESULT2           AS COGNRESULT2 
       ,A.COGNUSERNAME2         AS COGNUSERNAME2 
       ,A.COGNREASON2           AS COGNREASON2 
       ,A.COGNRESULT3           AS COGNRESULT3 
       ,A.COGNUSERNAME3         AS COGNUSERNAME3 
       ,A.COGNREASON3           AS COGNREASON3 
       ,A.COGNUSERID3           AS COGNUSERID3 
       ,A.COGNUSERID2           AS COGNUSERID2 
       ,A.EVALUATELEVEL         AS EVALUATELEVEL 
       ,A.FINISHDATE2           AS FINISHDATE2 
       ,A.FINISHDATE3           AS FINISHDATE3 
       ,A.FINISHDATE            AS FINISHDATE 
       ,A.COGNRESULT4           AS COGNRESULT4 
       ,A.COGNUSERNAME4         AS COGNUSERNAME4 
       ,A.COGNREASON4           AS COGNREASON4 
       ,A.FINISHDATE4           AS FINISHDATE4 
       ,A.COGNUSERID4           AS COGNUSERID4 
       ,A.REPORTSCOPE           AS REPORTSCOPE 
       ,A.RECORDNO              AS RECORDNO 
       ,A.TEMPSAVEFLAG          AS TEMPSAVEFLAG 
       ,A.GRADESTATUS           AS GRADESTATUS 
       ,A.MODELNAME             AS MODELNAME 
       ,A.FAMILYID              AS FAMILYID 
       ,A.REGISTERFLAG          AS REGISTERFLAG 
       ,A.BUSINESSSUM           AS BUSINESSSUM 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'SLNA'                  AS ODS_SYS_ID 
   FROM O_LN_SUN_EVALUATE_RECORD A                             --评级信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_SUN_EVALUATE_RECORD = sqlContext.sql(sql)
F_LN_SUN_EVALUATE_RECORD.registerTempTable("F_LN_SUN_EVALUATE_RECORD")
dfn="F_LN_SUN_EVALUATE_RECORD/"+V_DT+".parquet"
F_LN_SUN_EVALUATE_RECORD.cache()
nrows = F_LN_SUN_EVALUATE_RECORD.count()
F_LN_SUN_EVALUATE_RECORD.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_LN_SUN_EVALUATE_RECORD.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_SUN_EVALUATE_RECORD/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_SUN_EVALUATE_RECORD lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
