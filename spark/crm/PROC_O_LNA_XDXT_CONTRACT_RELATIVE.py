#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_CONTRACT_RELATIVE').setMaster(sys.argv[2])
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

O_LN_XDXT_CONTRACT_RELATIVE = sqlContext.read.parquet(hdfs+'/O_LN_XDXT_CONTRACT_RELATIVE/*')
O_LN_XDXT_CONTRACT_RELATIVE.registerTempTable("O_LN_XDXT_CONTRACT_RELATIVE")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_CONTRACT_RELATIVE/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_CONTRACT_RELATIVE_BK/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_CONTRACT_RELATIVE/"+V_DT+".parquet")


F_LN_XDXT_CONTRACT_RELATIVE = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_CONTRACT_RELATIVE/*')
F_LN_XDXT_CONTRACT_RELATIVE.registerTempTable("F_LN_XDXT_CONTRACT_RELATIVE")

sql = """
 SELECT A.SERIALNO              AS SERIALNO 
       ,A.OBJECTTYPE            AS OBJECTTYPE 
       ,A.OBJECTNO              AS OBJECTNO 
       ,A.RELATIVESUM           AS RELATIVESUM 
       ,A.RELATIONSTATUS        AS RELATIONSTATUS 
       ,A.RELATIVEBALANCESUM    AS RELATIVEBALANCESUM 
       ,A.USEFLAG               AS USEFLAG 
       ,A.NEWTYPE               AS NEWTYPE 
       ,A.ADDFLAG               AS ADDFLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_LN_XDXT_CONTRACT_RELATIVE A                          --合同关联表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_CONTRACT_RELATIVE_INNTMP1 = sqlContext.sql(sql)
F_LN_XDXT_CONTRACT_RELATIVE_INNTMP1.registerTempTable("F_LN_XDXT_CONTRACT_RELATIVE_INNTMP1")

#F_LN_XDXT_CONTRACT_RELATIVE = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_CONTRACT_RELATIVE/*')
#F_LN_XDXT_CONTRACT_RELATIVE.registerTempTable("F_LN_XDXT_CONTRACT_RELATIVE")
sql = """
 SELECT DST.SERIALNO                                            --合同流水号:src.SERIALNO
       ,DST.OBJECTTYPE                                         --对象类型:src.OBJECTTYPE
       ,DST.OBJECTNO                                           --对象编号:src.OBJECTNO
       ,DST.RELATIVESUM                                        --关联金额:src.RELATIVESUM
       ,DST.RELATIONSTATUS                                     --关联状态:src.RELATIONSTATUS
       ,DST.RELATIVEBALANCESUM                                 --参与本次最高额担保余额:src.RELATIVEBALANCESUM
       ,DST.USEFLAG                                            --使用金额标识:src.USEFLAG
       ,DST.NEWTYPE                                            --新加担保方式，用来区分新增还是引入:src.NEWTYPE
       ,DST.ADDFLAG                                            --合同阶段下新增担保:src.ADDFLAG
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
   FROM F_LN_XDXT_CONTRACT_RELATIVE DST 
   LEFT JOIN F_LN_XDXT_CONTRACT_RELATIVE_INNTMP1 SRC 
     ON SRC.SERIALNO            = DST.SERIALNO 
    AND SRC.OBJECTTYPE          = DST.OBJECTTYPE 
    AND SRC.OBJECTNO            = DST.OBJECTNO 
  WHERE SRC.SERIALNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_CONTRACT_RELATIVE_INNTMP2 = sqlContext.sql(sql)
dfn="F_LN_XDXT_CONTRACT_RELATIVE/"+V_DT+".parquet"
F_LN_XDXT_CONTRACT_RELATIVE_INNTMP2=F_LN_XDXT_CONTRACT_RELATIVE_INNTMP2.unionAll(F_LN_XDXT_CONTRACT_RELATIVE_INNTMP1)
F_LN_XDXT_CONTRACT_RELATIVE_INNTMP1.cache()
F_LN_XDXT_CONTRACT_RELATIVE_INNTMP2.cache()
nrowsi = F_LN_XDXT_CONTRACT_RELATIVE_INNTMP1.count()
nrowsa = F_LN_XDXT_CONTRACT_RELATIVE_INNTMP2.count()
F_LN_XDXT_CONTRACT_RELATIVE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_LN_XDXT_CONTRACT_RELATIVE_INNTMP1.unpersist()
F_LN_XDXT_CONTRACT_RELATIVE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_XDXT_CONTRACT_RELATIVE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_LN_XDXT_CONTRACT_RELATIVE/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_CONTRACT_RELATIVE_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_CONTRACT_RELATIVE_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_CONTRACT_RELATIVE/"+V_DT+".parquet /"+dbname+"/F_LN_XDXT_CONTRACT_RELATIVE_BK/"+V_DT+".parquet")
