#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_ECACCTMCH').setMaster(sys.argv[2])
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
O_CI_WSYH_ECACCTMCH = sqlContext.read.parquet(hdfs+'/O_CI_WSYH_ECACCTMCH/*')
O_CI_WSYH_ECACCTMCH.registerTempTable("O_CI_WSYH_ECACCTMCH")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#----------目标表---------------
F_CI_WSYH_ECACCTMCH = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECACCTMCH_BK/'+V_DT_LD+'.parquet/*')
F_CI_WSYH_ECACCTMCH.registerTempTable("F_CI_WSYH_ECACCTMCH")

sql = """
 SELECT A.ACSEQ                 AS ACSEQ 
       ,A.MCHANNELID            AS MCHANNELID 
       ,A.CREATEUSERSEQ         AS CREATEUSERSEQ 
       ,A.CREATEDEPTSEQ         AS CREATEDEPTSEQ 
       ,A.CREATETIME            AS CREATETIME 
       ,A.UPDATEUSERSEQ         AS UPDATEUSERSEQ 
       ,A.UPDATEDEPTSEQ         AS UPDATEDEPTSEQ 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'IBK'                   AS ODS_SYS_ID 
   FROM O_CI_WSYH_ECACCTMCH A                                  --客户账号渠道开通关联表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECACCTMCH_INNTMP1 = sqlContext.sql(sql)
F_CI_WSYH_ECACCTMCH_INNTMP1.registerTempTable("F_CI_WSYH_ECACCTMCH_INNTMP1")

#F_CI_WSYH_ECACCTMCH = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECACCTMCH/*')
#F_CI_WSYH_ECACCTMCH.registerTempTable("F_CI_WSYH_ECACCTMCH")
sql = """
 SELECT DST.ACSEQ                                               --账号顺序号:src.ACSEQ
       ,DST.MCHANNELID                                         --模块渠道代号:src.MCHANNELID
       ,DST.CREATEUSERSEQ                                      --创建用户顺序号:src.CREATEUSERSEQ
       ,DST.CREATEDEPTSEQ                                      --创建机构顺序号:src.CREATEDEPTSEQ
       ,DST.CREATETIME                                         --创建时间:src.CREATETIME
       ,DST.UPDATEUSERSEQ                                      --更新用户顺序号:src.UPDATEUSERSEQ
       ,DST.UPDATEDEPTSEQ                                      --更新机构顺序号:src.UPDATEDEPTSEQ
       ,DST.UPDATETIME                                         --更新时间:src.UPDATETIME
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
   FROM F_CI_WSYH_ECACCTMCH DST 
   LEFT JOIN F_CI_WSYH_ECACCTMCH_INNTMP1 SRC 
     ON SRC.ACSEQ               = DST.ACSEQ 
    AND SRC.MCHANNELID          = DST.MCHANNELID 
  WHERE SRC.ACSEQ IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECACCTMCH_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_WSYH_ECACCTMCH/"+V_DT+".parquet"
UNION=F_CI_WSYH_ECACCTMCH_INNTMP2.unionAll(F_CI_WSYH_ECACCTMCH_INNTMP1)
F_CI_WSYH_ECACCTMCH_INNTMP1.cache()
F_CI_WSYH_ECACCTMCH_INNTMP2.cache()
nrowsi = F_CI_WSYH_ECACCTMCH_INNTMP1.count()
nrowsa = F_CI_WSYH_ECACCTMCH_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_WSYH_ECACCTMCH_INNTMP1.unpersist()
F_CI_WSYH_ECACCTMCH_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_WSYH_ECACCTMCH lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_WSYH_ECACCTMCH/"+V_DT_LD+".parquet /"+dbname+"/F_CI_WSYH_ECACCTMCH_BK/")
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECACCTMCH/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECACCTMCH_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CI_WSYH_ECACCTMCH/"+V_DT+".parquet /"+dbname+"/F_CI_WSYH_ECACCTMCH_BK/")