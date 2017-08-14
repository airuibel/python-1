#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_ECCIFADDR').setMaster(sys.argv[2])
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

O_CI_WSYH_ECCIFADDR = sqlContext.read.parquet(hdfs+'/O_CI_WSYH_ECCIFADDR/*')
O_CI_WSYH_ECCIFADDR.registerTempTable("O_CI_WSYH_ECCIFADDR")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECCIFADDR/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_WSYH_ECCIFADDR_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_WSYH_ECCIFADDR/"+V_DT+".parquet")

F_CI_WSYH_ECCIFADDR = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFADDR/*')
F_CI_WSYH_ECCIFADDR.registerTempTable("F_CI_WSYH_ECCIFADDR")

sql = """
 SELECT A.CIFSEQ                AS CIFSEQ 
       ,A.ADDRTYPE              AS ADDRTYPE 
       ,A.ADDRLINE2             AS ADDRLINE2 
       ,A.ADDRLINE3             AS ADDRLINE3 
       ,A.ADDRLINE4             AS ADDRLINE4 
       ,A.ADDRLINE5             AS ADDRLINE5 
       ,A.ADDR                  AS ADDR 
       ,A.CITYCODE              AS CITYCODE 
       ,A.CITYAREACODE          AS CITYAREACODE 
       ,A.STREET                AS STREET 
       ,A.BUILDING              AS BUILDING 
       ,A.ROOMNO                AS ROOMNO 
       ,A.POSTCODE              AS POSTCODE 
       ,A.CREATEUSERSEQ         AS CREATEUSERSEQ 
       ,A.CREATEDEPTSEQ         AS CREATEDEPTSEQ 
       ,A.CREATETIME            AS CREATETIME 
       ,A.UPDATEUSERSEQ         AS UPDATEUSERSEQ 
       ,A.UPDATEDEPTSEQ         AS UPDATEDEPTSEQ 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.FR_ID                 AS FR_ID 
       ,'IBK'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
   FROM O_CI_WSYH_ECCIFADDR A                                  --客户地址表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECCIFADDR_INNTMP1 = sqlContext.sql(sql)
F_CI_WSYH_ECCIFADDR_INNTMP1.registerTempTable("F_CI_WSYH_ECCIFADDR_INNTMP1")

#F_CI_WSYH_ECCIFADDR = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFADDR/*')
#F_CI_WSYH_ECCIFADDR.registerTempTable("F_CI_WSYH_ECCIFADDR")
sql = """
 SELECT DST.CIFSEQ                                              --客户顺序号:src.CIFSEQ
       ,DST.ADDRTYPE                                           --地址类型:src.ADDRTYPE
       ,DST.ADDRLINE2                                          --地址行二:src.ADDRLINE2
       ,DST.ADDRLINE3                                          --地址行三:src.ADDRLINE3
       ,DST.ADDRLINE4                                          --地址行四:src.ADDRLINE4
       ,DST.ADDRLINE5                                          --地址行五:src.ADDRLINE5
       ,DST.ADDR                                               --地址:src.ADDR
       ,DST.CITYCODE                                           --城市代码:src.CITYCODE
       ,DST.CITYAREACODE                                       --区县:src.CITYAREACODE
       ,DST.STREET                                             --街道:src.STREET
       ,DST.BUILDING                                           --大楼:src.BUILDING
       ,DST.ROOMNO                                             --房间编号:src.ROOMNO
       ,DST.POSTCODE                                           --邮编:src.POSTCODE
       ,DST.CREATEUSERSEQ                                      --创建用户顺序号:src.CREATEUSERSEQ
       ,DST.CREATEDEPTSEQ                                      --创建机构顺序号:src.CREATEDEPTSEQ
       ,DST.CREATETIME                                         --创建时间:src.CREATETIME
       ,DST.UPDATEUSERSEQ                                      --更新用户顺序号:src.UPDATEUSERSEQ
       ,DST.UPDATEDEPTSEQ                                      --更新机构顺序号:src.UPDATEDEPTSEQ
       ,DST.UPDATETIME                                         --更新时间:src.UPDATETIME
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
   FROM F_CI_WSYH_ECCIFADDR DST 
   LEFT JOIN F_CI_WSYH_ECCIFADDR_INNTMP1 SRC 
     ON SRC.CIFSEQ              = DST.CIFSEQ 
    AND SRC.ADDRTYPE            = DST.ADDRTYPE 
  WHERE SRC.CIFSEQ IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECCIFADDR_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_WSYH_ECCIFADDR/"+V_DT+".parquet"
F_CI_WSYH_ECCIFADDR_INNTMP2=F_CI_WSYH_ECCIFADDR_INNTMP2.unionAll(F_CI_WSYH_ECCIFADDR_INNTMP1)
F_CI_WSYH_ECCIFADDR_INNTMP1.cache()
F_CI_WSYH_ECCIFADDR_INNTMP2.cache()
nrowsi = F_CI_WSYH_ECCIFADDR_INNTMP1.count()
nrowsa = F_CI_WSYH_ECCIFADDR_INNTMP2.count()
F_CI_WSYH_ECCIFADDR_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_WSYH_ECCIFADDR_INNTMP1.unpersist()
F_CI_WSYH_ECCIFADDR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_WSYH_ECCIFADDR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_WSYH_ECCIFADDR/"+V_DT_LD+".parquet /"+dbname+"/F_CI_WSYH_ECCIFADDR_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECCIFADDR_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_WSYH_ECCIFADDR/"+V_DT+".parquet /"+dbname+"/F_CI_WSYH_ECCIFADDR_BK/"+V_DT+".parquet")
