#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_CLASSIFY_RESULT').setMaster(sys.argv[2])
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

O_LN_XDXT_CLASSIFY_RESULT = sqlContext.read.parquet(hdfs+'/O_LN_XDXT_CLASSIFY_RESULT/*')
O_LN_XDXT_CLASSIFY_RESULT.registerTempTable("O_LN_XDXT_CLASSIFY_RESULT")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_CLASSIFY_RESULT/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_CLASSIFY_RESULT_BK/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_CLASSIFY_RESULT/"+V_DT+".parquet")


F_LN_XDXT_CLASSIFY_RESULT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_CLASSIFY_RESULT/*')
F_LN_XDXT_CLASSIFY_RESULT.registerTempTable("F_LN_XDXT_CLASSIFY_RESULT")

sql = """
 SELECT A.OBJECTNO              AS OBJECTNO 
       ,A.ACCOUNTMONTH          AS ACCOUNTMONTH 
       ,A.CUSTOMERID            AS CUSTOMERID 
       ,A.CUSTOMERNAME          AS CUSTOMERNAME 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CUSTOMERTYPE          AS CUSTOMERTYPE 
       ,A.BUSINESSTYPE          AS BUSINESSTYPE 
       ,A.CURRENTBALANCE        AS CURRENTBALANCE 
       ,A.BUSINESSBALANCE       AS BUSINESSBALANCE 
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
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_LN_XDXT_CLASSIFY_RESULT A                            --资产风险分类结果
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_CLASSIFY_RESULT_INNTMP1 = sqlContext.sql(sql)
F_LN_XDXT_CLASSIFY_RESULT_INNTMP1.registerTempTable("F_LN_XDXT_CLASSIFY_RESULT_INNTMP1")

#F_LN_XDXT_CLASSIFY_RESULT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_CLASSIFY_RESULT/*')
#F_LN_XDXT_CLASSIFY_RESULT.registerTempTable("F_LN_XDXT_CLASSIFY_RESULT")
sql = """
 SELECT DST.OBJECTNO                                            --资产风险分类标号:src.OBJECTNO
       ,DST.ACCOUNTMONTH                                       --分类期次:src.ACCOUNTMONTH
       ,DST.CUSTOMERID                                         --信贷客户编号:src.CUSTOMERID
       ,DST.CUSTOMERNAME                                       --客户名称:src.CUSTOMERNAME
       ,DST.CERTTYPE                                           --证件类型:src.CERTTYPE
       ,DST.CUSTOMERTYPE                                       --客户类型:src.CUSTOMERTYPE
       ,DST.BUSINESSTYPE                                       --业务类型:src.BUSINESSTYPE
       ,DST.CURRENTBALANCE                                     --当前余额:src.CURRENTBALANCE
       ,DST.BUSINESSBALANCE                                    --业务合同余额:src.BUSINESSBALANCE
       ,DST.FIRSTRESULT                                        --初分结果:src.FIRSTRESULT
       ,DST.FIRSTRESULTRESOURCE                                --初分结果来源:src.FIRSTRESULTRESOURCE
       ,DST.SECONDRESULT                                       --人工认定结果:src.SECONDRESULT
       ,DST.MODELRESULT                                        --模型评定结果:src.MODELRESULT
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
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
   FROM F_LN_XDXT_CLASSIFY_RESULT DST 
   LEFT JOIN F_LN_XDXT_CLASSIFY_RESULT_INNTMP1 SRC 
     ON SRC.OBJECTNO            = DST.OBJECTNO 
    AND SRC.ACCOUNTMONTH        = DST.ACCOUNTMONTH 
  WHERE SRC.OBJECTNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_CLASSIFY_RESULT_INNTMP2 = sqlContext.sql(sql)
dfn="F_LN_XDXT_CLASSIFY_RESULT/"+V_DT+".parquet"
F_LN_XDXT_CLASSIFY_RESULT_INNTMP2=F_LN_XDXT_CLASSIFY_RESULT_INNTMP2.unionAll(F_LN_XDXT_CLASSIFY_RESULT_INNTMP1)
F_LN_XDXT_CLASSIFY_RESULT_INNTMP1.cache()
F_LN_XDXT_CLASSIFY_RESULT_INNTMP2.cache()
nrowsi = F_LN_XDXT_CLASSIFY_RESULT_INNTMP1.count()
nrowsa = F_LN_XDXT_CLASSIFY_RESULT_INNTMP2.count()
F_LN_XDXT_CLASSIFY_RESULT_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_LN_XDXT_CLASSIFY_RESULT_INNTMP1.unpersist()
F_LN_XDXT_CLASSIFY_RESULT_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_XDXT_CLASSIFY_RESULT lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_LN_XDXT_CLASSIFY_RESULT/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_CLASSIFY_RESULT_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_CLASSIFY_RESULT_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_CLASSIFY_RESULT/"+V_DT+".parquet /"+dbname+"/F_LN_XDXT_CLASSIFY_RESULT_BK/"+V_DT+".parquet")
