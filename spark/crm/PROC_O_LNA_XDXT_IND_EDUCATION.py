#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_IND_EDUCATION').setMaster(sys.argv[2])
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

O_CI_XDXT_IND_EDUCATION = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_IND_EDUCATION/*')
O_CI_XDXT_IND_EDUCATION.registerTempTable("O_CI_XDXT_IND_EDUCATION")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_IND_EDUCATION/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_IND_EDUCATION_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_IND_EDUCATION/"+V_DT+".parquet")

F_CI_XDXT_IND_EDUCATION = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_EDUCATION/*')
F_CI_XDXT_IND_EDUCATION.registerTempTable("F_CI_XDXT_IND_EDUCATION")

sql = """
 SELECT A.CUSTOMERID            AS CUSTOMERID 
       ,A.SERIALNO              AS SERIALNO 
       ,A.BEGINDATE             AS BEGINDATE 
       ,A.ENDDATE               AS ENDDATE 
       ,A.SCHOOL                AS SCHOOL 
       ,A.DEPARTMENT            AS DEPARTMENT 
       ,A.SPECIALTY             AS SPECIALTY 
       ,A.DEGREE                AS DEGREE 
       ,A.EDUEXPERIENCE         AS EDUEXPERIENCE 
       ,A.SCHOOLLENGTH          AS SCHOOLLENGTH 
       ,A.DIPLOMANO             AS DIPLOMANO 
       ,A.DEGREENO              AS DEGREENO 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.REMARK                AS REMARK 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_IND_EDUCATION A                              --个人学业履历
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_IND_EDUCATION_INNTMP1 = sqlContext.sql(sql)
F_CI_XDXT_IND_EDUCATION_INNTMP1.registerTempTable("F_CI_XDXT_IND_EDUCATION_INNTMP1")

#F_CI_XDXT_IND_EDUCATION = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_EDUCATION/*')
#F_CI_XDXT_IND_EDUCATION.registerTempTable("F_CI_XDXT_IND_EDUCATION")
sql = """
 SELECT DST.CUSTOMERID                                          --客户编号:src.CUSTOMERID
       ,DST.SERIALNO                                           --流水号:src.SERIALNO
       ,DST.BEGINDATE                                          --开始日期:src.BEGINDATE
       ,DST.ENDDATE                                            --结束日期:src.ENDDATE
       ,DST.SCHOOL                                             --所在学校:src.SCHOOL
       ,DST.DEPARTMENT                                         --所在院系:src.DEPARTMENT
       ,DST.SPECIALTY                                          --专业:src.SPECIALTY
       ,DST.DEGREE                                             --最高学位:src.DEGREE
       ,DST.EDUEXPERIENCE                                      --最高学历:src.EDUEXPERIENCE
       ,DST.SCHOOLLENGTH                                       --学制:src.SCHOOLLENGTH
       ,DST.DIPLOMANO                                          --学历证书号:src.DIPLOMANO
       ,DST.DEGREENO                                           --学位证书号:src.DEGREENO
       ,DST.INPUTORGID                                         --登记单位:src.INPUTORGID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTDATE                                          --登记日期:src.INPUTDATE
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_CI_XDXT_IND_EDUCATION DST 
   LEFT JOIN F_CI_XDXT_IND_EDUCATION_INNTMP1 SRC 
     ON SRC.CUSTOMERID          = DST.CUSTOMERID 
    AND SRC.SERIALNO            = DST.SERIALNO 
  WHERE SRC.CUSTOMERID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_IND_EDUCATION_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_XDXT_IND_EDUCATION/"+V_DT+".parquet"
F_CI_XDXT_IND_EDUCATION_INNTMP2=F_CI_XDXT_IND_EDUCATION_INNTMP2.unionAll(F_CI_XDXT_IND_EDUCATION_INNTMP1)
F_CI_XDXT_IND_EDUCATION_INNTMP1.cache()
F_CI_XDXT_IND_EDUCATION_INNTMP2.cache()
nrowsi = F_CI_XDXT_IND_EDUCATION_INNTMP1.count()
nrowsa = F_CI_XDXT_IND_EDUCATION_INNTMP2.count()
F_CI_XDXT_IND_EDUCATION_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_IND_EDUCATION_INNTMP1.unpersist()
F_CI_XDXT_IND_EDUCATION_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_IND_EDUCATION lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_XDXT_IND_EDUCATION/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_IND_EDUCATION_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_IND_EDUCATION_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_IND_EDUCATION/"+V_DT+".parquet /"+dbname+"/F_CI_XDXT_IND_EDUCATION_BK/"+V_DT+".parquet")
