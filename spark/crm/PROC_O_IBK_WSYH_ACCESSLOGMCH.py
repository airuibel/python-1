#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_ACCESSLOGMCH').setMaster(sys.argv[2])
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
O_TX_WSYH_ACCESSLOGMCH = sqlContext.read.parquet(hdfs+'/O_TX_WSYH_ACCESSLOGMCH/*')
O_TX_WSYH_ACCESSLOGMCH.registerTempTable("O_TX_WSYH_ACCESSLOGMCH")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#----------目标表---------------
F_TX_WSYH_ACCESSLOGMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ACCESSLOGMCH_BK/'+V_DT_LD+'.parquet/*')
F_TX_WSYH_ACCESSLOGMCH.registerTempTable("F_TX_WSYH_ACCESSLOGMCH")

sql = """
 SELECT A.ACCESSJNLNO           AS ACCESSJNLNO 
       ,A.ACCESSSTATE           AS ACCESSSTATE 
       ,A.ACCESSDATE            AS ACCESSDATE 
       ,A.MACHINEID             AS MACHINEID 
       ,A.ACCESSIP              AS ACCESSIP 
       ,A.MODULEID              AS MODULEID 
       ,A.CHANNELID             AS CHANNELID 
       ,A.LOGINTYPE             AS LOGINTYPE 
       ,A.USERSEQ               AS USERSEQ 
       ,A.DEPTSEQ               AS DEPTSEQ 
       ,A.UNIQUEUSERID          AS UNIQUEUSERID 
       ,A.USERAGENT             AS USERAGENT 
       ,A.CHANNELJNLNO          AS CHANNELJNLNO 
       ,A.FR_ID                 AS FR_ID 
       ,'IBK'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
   FROM O_TX_WSYH_ACCESSLOGMCH A                               --访问日志表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_ACCESSLOGMCH_INNTMP1 = sqlContext.sql(sql)
F_TX_WSYH_ACCESSLOGMCH_INNTMP1.registerTempTable("F_TX_WSYH_ACCESSLOGMCH_INNTMP1")

#F_TX_WSYH_ACCESSLOGMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ACCESSLOGMCH/*')
#F_TX_WSYH_ACCESSLOGMCH.registerTempTable("F_TX_WSYH_ACCESSLOGMCH")
sql = """
 SELECT DST.ACCESSJNLNO                                         --ACCESSJNLNO:src.ACCESSJNLNO
       ,DST.ACCESSSTATE                                        --ACCESSSTATE:src.ACCESSSTATE
       ,DST.ACCESSDATE                                         --ACCESSDATE:src.ACCESSDATE
       ,DST.MACHINEID                                          --MACHINEID:src.MACHINEID
       ,DST.ACCESSIP                                           --ACCESSIP:src.ACCESSIP
       ,DST.MODULEID                                           --模块代号:src.MODULEID
       ,DST.CHANNELID                                          --渠道编号:src.CHANNELID
       ,DST.LOGINTYPE                                          --登陆类型:src.LOGINTYPE
       ,DST.USERSEQ                                            --用户顺序号:src.USERSEQ
       ,DST.DEPTSEQ                                            --DEPTSEQ:src.DEPTSEQ
       ,DST.UNIQUEUSERID                                       --唯一用户ID:src.UNIQUEUSERID
       ,DST.USERAGENT                                          --USERAGENT:src.USERAGENT
       ,DST.CHANNELJNLNO                                       --CHANNELJNLNO:src.CHANNELJNLNO
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
   FROM F_TX_WSYH_ACCESSLOGMCH DST 
   LEFT JOIN F_TX_WSYH_ACCESSLOGMCH_INNTMP1 SRC 
     ON SRC.ACCESSJNLNO         = DST.ACCESSJNLNO 
  WHERE SRC.ACCESSJNLNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_ACCESSLOGMCH_INNTMP2 = sqlContext.sql(sql)
dfn="F_TX_WSYH_ACCESSLOGMCH/"+V_DT+".parquet"
UNION=F_TX_WSYH_ACCESSLOGMCH_INNTMP2.unionAll(F_TX_WSYH_ACCESSLOGMCH_INNTMP1)
F_TX_WSYH_ACCESSLOGMCH_INNTMP1.cache()
F_TX_WSYH_ACCESSLOGMCH_INNTMP2.cache()
nrowsi = F_TX_WSYH_ACCESSLOGMCH_INNTMP1.count()
nrowsa = F_TX_WSYH_ACCESSLOGMCH_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_TX_WSYH_ACCESSLOGMCH_INNTMP1.unpersist()
F_TX_WSYH_ACCESSLOGMCH_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_WSYH_ACCESSLOGMCH lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_TX_WSYH_ACCESSLOGMCH/"+V_DT_LD+".parquet /"+dbname+"/F_TX_WSYH_ACCESSLOGMCH_BK/")

#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_ACCESSLOGMCH/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_ACCESSLOGMCH_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_TX_WSYH_ACCESSLOGMCH/"+V_DT+".parquet /"+dbname+"/F_TX_WSYH_ACCESSLOGMCH_BK/")