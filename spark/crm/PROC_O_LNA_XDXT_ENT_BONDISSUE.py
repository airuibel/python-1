#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_ENT_BONDISSUE').setMaster(sys.argv[2])
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

O_CI_XDXT_ENT_BONDISSUE = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_ENT_BONDISSUE/*')
O_CI_XDXT_ENT_BONDISSUE.registerTempTable("O_CI_XDXT_ENT_BONDISSUE")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_ENT_BONDISSUE/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_ENT_BONDISSUE_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_ENT_BONDISSUE/"+V_DT+".parquet")


F_CI_XDXT_ENT_BONDISSUE = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_ENT_BONDISSUE/*')
F_CI_XDXT_ENT_BONDISSUE.registerTempTable("F_CI_XDXT_ENT_BONDISSUE")

sql = """
 SELECT A.CUSTOMERID            AS CUSTOMERID 
       ,A.SERIALNO              AS SERIALNO 
       ,A.ISSUEDATE             AS ISSUEDATE 
       ,A.BONDTYPE              AS BONDTYPE 
       ,A.BONDGRADE             AS BONDGRADE 
       ,A.BONDNAME              AS BONDNAME 
       ,A.BONDCURRENCY          AS BONDCURRENCY 
       ,A.BONDSUM               AS BONDSUM 
       ,A.IRREGULATION          AS IRREGULATION 
       ,A.BONDSELLER            AS BONDSELLER 
       ,A.BONDWARRANTOR         AS BONDWARRANTOR 
       ,A.MARKETEDORNOT         AS MARKETEDORNOT 
       ,A.BOURSENAME            AS BOURSENAME 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.REMARK                AS REMARK 
       ,A.BONDTERM              AS BONDTERM 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_ENT_BONDISSUE A                              --企业发行债券信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_ENT_BONDISSUE_INNTMP1 = sqlContext.sql(sql)
F_CI_XDXT_ENT_BONDISSUE_INNTMP1.registerTempTable("F_CI_XDXT_ENT_BONDISSUE_INNTMP1")

#F_CI_XDXT_ENT_BONDISSUE = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_ENT_BONDISSUE/*')
#F_CI_XDXT_ENT_BONDISSUE.registerTempTable("F_CI_XDXT_ENT_BONDISSUE")
sql = """
 SELECT DST.CUSTOMERID                                          --客户编号:src.CUSTOMERID
       ,DST.SERIALNO                                           --流水号:src.SERIALNO
       ,DST.ISSUEDATE                                          --发行日期:src.ISSUEDATE
       ,DST.BONDTYPE                                           --债券类型:src.BONDTYPE
       ,DST.BONDGRADE                                          --债券级别:src.BONDGRADE
       ,DST.BONDNAME                                           --债券名称:src.BONDNAME
       ,DST.BONDCURRENCY                                       --币种:src.BONDCURRENCY
       ,DST.BONDSUM                                            --金额:src.BONDSUM
       ,DST.IRREGULATION                                       --利率规定:src.IRREGULATION
       ,DST.BONDSELLER                                         --债券承销商:src.BONDSELLER
       ,DST.BONDWARRANTOR                                      --债券主担保人:src.BONDWARRANTOR
       ,DST.MARKETEDORNOT                                      --是否上市:src.MARKETEDORNOT
       ,DST.BOURSENAME                                         --上市交易所名称:src.BOURSENAME
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTDATE                                          --登记日期:src.INPUTDATE
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.BONDTERM                                           --债券期限:src.BONDTERM
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
   FROM F_CI_XDXT_ENT_BONDISSUE DST 
   LEFT JOIN F_CI_XDXT_ENT_BONDISSUE_INNTMP1 SRC 
     ON SRC.CUSTOMERID          = DST.CUSTOMERID 
    AND SRC.SERIALNO            = DST.SERIALNO 
  WHERE SRC.CUSTOMERID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_ENT_BONDISSUE_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_XDXT_ENT_BONDISSUE/"+V_DT+".parquet"
F_CI_XDXT_ENT_BONDISSUE_INNTMP2=F_CI_XDXT_ENT_BONDISSUE_INNTMP2.unionAll(F_CI_XDXT_ENT_BONDISSUE_INNTMP1)
F_CI_XDXT_ENT_BONDISSUE_INNTMP1.cache()
F_CI_XDXT_ENT_BONDISSUE_INNTMP2.cache()
nrowsi = F_CI_XDXT_ENT_BONDISSUE_INNTMP1.count()
nrowsa = F_CI_XDXT_ENT_BONDISSUE_INNTMP2.count()
F_CI_XDXT_ENT_BONDISSUE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_ENT_BONDISSUE_INNTMP1.unpersist()
F_CI_XDXT_ENT_BONDISSUE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_ENT_BONDISSUE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_XDXT_ENT_BONDISSUE/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_ENT_BONDISSUE_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_ENT_BONDISSUE_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_ENT_BONDISSUE/"+V_DT+".parquet /"+dbname+"/F_CI_XDXT_ENT_BONDISSUE_BK/"+V_DT+".parquet")
