#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_IND_REPORT_RECORD').setMaster(sys.argv[2])
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

O_CI_XDXT_IND_REPORT_RECORD = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_IND_REPORT_RECORD/*')
O_CI_XDXT_IND_REPORT_RECORD.registerTempTable("O_CI_XDXT_IND_REPORT_RECORD")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_IND_REPORT_RECORD/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_IND_REPORT_RECORD_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_IND_REPORT_RECORD/"+V_DT+".parquet")


F_CI_XDXT_IND_REPORT_RECORD = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_REPORT_RECORD/*')
F_CI_XDXT_IND_REPORT_RECORD.registerTempTable("F_CI_XDXT_IND_REPORT_RECORD")

sql = """
 SELECT A.REPORTNO              AS REPORTNO 
       ,A.OBJECTTYPE            AS OBJECTTYPE 
       ,A.OBJECTNO              AS OBJECTNO 
       ,A.MODELNO               AS MODELNO 
       ,A.REPORTNAME            AS REPORTNAME 
       ,A.INPUTTIME             AS INPUTTIME 
       ,A.ORGID                 AS ORGID 
       ,A.USERID                AS USERID 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.VALIDFLAG             AS VALIDFLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_IND_REPORT_RECORD A                          --个人财务报表数据记录表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_IND_REPORT_RECORD_INNTMP1 = sqlContext.sql(sql)
F_CI_XDXT_IND_REPORT_RECORD_INNTMP1.registerTempTable("F_CI_XDXT_IND_REPORT_RECORD_INNTMP1")

#F_CI_XDXT_IND_REPORT_RECORD = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_REPORT_RECORD/*')
#F_CI_XDXT_IND_REPORT_RECORD.registerTempTable("F_CI_XDXT_IND_REPORT_RECORD")
sql = """
 SELECT DST.REPORTNO                                            --报表编号:src.REPORTNO
       ,DST.OBJECTTYPE                                         --对象类型:src.OBJECTTYPE
       ,DST.OBJECTNO                                           --对象编号:src.OBJECTNO
       ,DST.MODELNO                                            --模型编号:src.MODELNO
       ,DST.REPORTNAME                                         --报表名称:src.REPORTNAME
       ,DST.INPUTTIME                                          --登记时间:src.INPUTTIME
       ,DST.ORGID                                              --创建机构:src.ORGID
       ,DST.USERID                                             --操作员:src.USERID
       ,DST.UPDATETIME                                         --更新时间:src.UPDATETIME
       ,DST.VALIDFLAG                                          --生效标志:src.VALIDFLAG
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
   FROM F_CI_XDXT_IND_REPORT_RECORD DST 
   LEFT JOIN F_CI_XDXT_IND_REPORT_RECORD_INNTMP1 SRC 
     ON SRC.REPORTNO            = DST.REPORTNO 
  WHERE SRC.REPORTNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_IND_REPORT_RECORD_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_XDXT_IND_REPORT_RECORD/"+V_DT+".parquet"
F_CI_XDXT_IND_REPORT_RECORD_INNTMP2=F_CI_XDXT_IND_REPORT_RECORD_INNTMP2.unionAll(F_CI_XDXT_IND_REPORT_RECORD_INNTMP1)
F_CI_XDXT_IND_REPORT_RECORD_INNTMP1.cache()
F_CI_XDXT_IND_REPORT_RECORD_INNTMP2.cache()
nrowsi = F_CI_XDXT_IND_REPORT_RECORD_INNTMP1.count()
nrowsa = F_CI_XDXT_IND_REPORT_RECORD_INNTMP2.count()
F_CI_XDXT_IND_REPORT_RECORD_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_IND_REPORT_RECORD_INNTMP1.unpersist()
F_CI_XDXT_IND_REPORT_RECORD_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_IND_REPORT_RECORD lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_XDXT_IND_REPORT_RECORD/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_IND_REPORT_RECORD_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_IND_REPORT_RECORD_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_IND_REPORT_RECORD/"+V_DT+".parquet /"+dbname+"/F_CI_XDXT_IND_REPORT_RECORD_BK/"+V_DT+".parquet")
