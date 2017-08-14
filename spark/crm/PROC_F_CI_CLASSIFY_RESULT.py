#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CLASSIFY_RESULT').setMaster(sys.argv[2])
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
F_LN_XDXT_CLASSIFY_RESULT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_CLASSIFY_RESULT/*')
F_LN_XDXT_CLASSIFY_RESULT.registerTempTable("F_LN_XDXT_CLASSIFY_RESULT")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

#任务[12] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.OBJECTNO              AS OBJECTNO 
       ,A.ACCOUNTMONTH          AS ACCOUNTMONTH 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,A.CUSTOMERNAME          AS CUSTOMERNAME 
        ,A.CERTTYPE              AS CERTTYPE 
       ,A.CUSTOMERTYPE          AS CUSTOMERTYPE 
       ,A.BUSINESSTYPE          AS BUSINESSTYPE 
       ,CAST(A.CURRENTBALANCE  AS DECIMAL(24))      AS CURRENTBALANCE 
       ,CAST(A.BUSINESSBALANCE  AS DECIMAL(24))     AS BUSINESSBALANCE 
       ,A.FIRSTRESULT           AS FIRSTRESULT 
        ,A.FIRSTRESULTRESOURCE   AS FIRSTRESULTRESOURCE 
        ,A.SECONDRESULT          AS SECONDRESULT 
        ,A.MODELRESULT           AS MODELRESULT 
       ,A.LASTFIVERESULT        AS LASTFIVERESULT 
       ,A.LASTTENRESULT         AS LASTTENRESULT 
       ,A.CURRENTFIVERESULT     AS CURRENTFIVERESULT 
       ,A.CURRENTTENRESULT      AS CURRENTTENRESULT  
       ,A.FINALLYRESULT         AS FINALLYRESULT 
       ,A.CONFIRMTYPE           AS CONFIRMTYPE 
       ,A.CLASSIFYTYPE          AS CLASSIFYTYPE 
       ,A.USERID                AS USERID 
       ,A.ORGID                 AS ORGID  
       ,A.FINISHDATE            AS FINISHDATE  
       ,A.LASTRESULT            AS LASTRESULT  
       ,''                    AS UPDATEDATE 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,monotonically_increasing_id()   AS ID 
       ,A.FR_ID                 AS FR_ID 
   FROM F_LN_XDXT_CLASSIFY_RESULT A                            --资产风险分类结果
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.CUSTOMERID            = B.SOURCE_CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ODS_SYS_ID            = 'LNA' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CLASSIFY_RESULT_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CLASSIFY_RESULT_INNTMP1.registerTempTable("OCRM_F_CI_CLASSIFY_RESULT_INNTMP1")

OCRM_F_CI_CLASSIFY_RESULT = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CLASSIFY_RESULT_BK/'+V_DT_LD+'.parquet/*')
OCRM_F_CI_CLASSIFY_RESULT.registerTempTable("OCRM_F_CI_CLASSIFY_RESULT")
sql = """
 SELECT DST.OBJECTNO                                           --资产风险分类标号（合同编号）:src.OBJECTNO
       ,DST.ACCOUNTMONTH                                       --分类期次:src.ACCOUNTMONTH
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUSTOMERNAME                                       --客户名称:src.CUSTOMERNAME
       ,DST.CERTTYPE                                           --证件类型:src.CERTTYPE
       ,DST.CUSTOMERTYPE                                       --客户类型:src.CUSTOMERTYPE
       ,DST.BUSINESSTYPE                                       --业务品种:src.BUSINESSTYPE
       ,DST.CURRENTBALANCE                                     --当前余额:src.CURRENTBALANCE
       ,DST.BUSINESSBALANCE                                    --业务合同余额:src.BUSINESSBALANCE
       ,DST.FIRSTRESULT                                        --初分结果:src.FIRSTRESULT
       ,DST.FIRSTRESULTRESOURCE                                --初分结果来源:src.FIRSTRESULTRESOURCE
       ,DST.SECONDRESULT                                       --人工认定结果:src.SECONDRESULT
       ,DST.MODELRESULT                                        --（针对一般小企业）模型评定结果:src.MODELRESULT
       ,DST.LASTFIVERESULT                                     --上期五级分类结果:src.LASTFIVERESULT
       ,DST.LASTTENRESULT                                      --上期十级分类结果:src.LASTTENRESULT
       ,DST.CURRENTFIVERESULT                                  --当期五级分类结果:src.CURRENTFIVERESULT
       ,DST.CURRENTTENRESULT                                   --当期十级分类结果:src.CURRENTTENRESULT
       ,DST.FINALLYRESULT                                      --最终认定结果:src.FINALLYRESULT
       ,DST.CONFIRMTYPE                                        --认定模式，分流程和直接通过:src.CONFIRMTYPE
       ,DST.CLASSIFYTYPE                                       --分类产生类型,分批量产生和人工新增产生:src.CLASSIFYTYPE
       ,DST.USERID                                             --认定人:src.USERID
       ,DST.ORGID                                              --认定机构:src.ORGID
       ,DST.FINISHDATE                                         --认定完成日期:src.FINISHDATE
       ,DST.LASTRESULT                                         --上期结果:src.LASTRESULT
       ,DST.UPDATEDATE                                         --更新日期:src.UPDATEDATE
       ,DST.ODS_ST_DATE                                        --:src.ODS_ST_DATE
       ,DST.ID                                                 --:src.ID
       ,DST.FR_ID                                              --:src.FR_ID
   FROM OCRM_F_CI_CLASSIFY_RESULT DST 
   LEFT JOIN OCRM_F_CI_CLASSIFY_RESULT_INNTMP1 SRC 
     ON SRC.OBJECTNO            = DST.OBJECTNO 
    AND SRC.ACCOUNTMONTH        = DST.ACCOUNTMONTH 
  WHERE SRC.OBJECTNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CLASSIFY_RESULT_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CLASSIFY_RESULT/"+V_DT+".parquet"
UNION=OCRM_F_CI_CLASSIFY_RESULT_INNTMP2.unionAll(OCRM_F_CI_CLASSIFY_RESULT_INNTMP1)
OCRM_F_CI_CLASSIFY_RESULT_INNTMP1.cache()
OCRM_F_CI_CLASSIFY_RESULT_INNTMP2.cache()
nrowsi = OCRM_F_CI_CLASSIFY_RESULT_INNTMP1.count()
nrowsa = OCRM_F_CI_CLASSIFY_RESULT_INNTMP2.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CLASSIFY_RESULT/*")
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CLASSIFY_RESULT.unpersist()
OCRM_F_CI_CLASSIFY_RESULT_INNTMP1.unpersist()
OCRM_F_CI_CLASSIFY_RESULT_INNTMP2.unpersist()
F_LN_XDXT_CLASSIFY_RESULT.unpersist()
OCRM_F_CI_SYS_RESOURCE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CLASSIFY_RESULT lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CLASSIFY_RESULT_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_CLASSIFY_RESULT/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_CLASSIFY_RESULT_BK/")
