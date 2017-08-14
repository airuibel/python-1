#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_FIN_FIN_PRODAREAINFO').setMaster(sys.argv[2])
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

O_CM_FIN_PRODAREAINFO = sqlContext.read.parquet(hdfs+'/O_CM_FIN_PRODAREAINFO/*')
O_CM_FIN_PRODAREAINFO.registerTempTable("O_CM_FIN_PRODAREAINFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CM_FIN_PRODAREAINFO = sqlContext.read.parquet(hdfs+'/F_CM_FIN_PRODAREAINFO_BK/'+V_DT_LD+'.parquet/*')
F_CM_FIN_PRODAREAINFO.registerTempTable("F_CM_FIN_PRODAREAINFO")

sql = """
 SELECT A.PRODCODE              AS PRODCODE 
       ,A.PRODZONENO            AS PRODZONENO 
       ,A.AGTBRNO               AS AGTBRNO 
       ,A.AGTZONENO             AS AGTZONENO 
       ,A.AGTZONENA             AS AGTZONENA 
       ,A.FEERATE               AS FEERATE 
       ,A.ZONENOMINAMT          AS ZONENOMINAMT 
       ,A.ZONENOMAXAMT          AS ZONENOMAXAMT 
       ,A.AGTAMT                AS AGTAMT 
       ,A.AGTSTATE              AS AGTSTATE 
       ,A.REGZONENO             AS REGZONENO 
       ,A.REGBRNO               AS REGBRNO 
       ,A.REGTELLERNO           AS REGTELLERNO 
       ,A.REGDATE               AS REGDATE 
       ,A.REGTIME               AS REGTIME 
       ,A.ENSZONENO             AS ENSZONENO 
       ,A.ENSBRNO               AS ENSBRNO 
       ,A.ENSTELLERNO           AS ENSTELLERNO 
       ,A.ENSDATE               AS ENSDATE 
       ,A.ENSTIME               AS ENSTIME 
       ,A.NOTE1                 AS NOTE1 
       ,A.NOTE2                 AS NOTE2 
       ,A.NOTE3                 AS NOTE3 
       ,A.NOTE4                 AS NOTE4 
       ,A.NOTE5                 AS NOTE5 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'FIN'                   AS ODS_SYS_ID 
   FROM O_CM_FIN_PRODAREAINFO A                                --销售法人范围表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_FIN_PRODAREAINFO_INNTMP1 = sqlContext.sql(sql)
F_CM_FIN_PRODAREAINFO_INNTMP1.registerTempTable("F_CM_FIN_PRODAREAINFO_INNTMP1")

#F_CM_FIN_PRODAREAINFO = sqlContext.read.parquet(hdfs+'/F_CM_FIN_PRODAREAINFO/*')
#F_CM_FIN_PRODAREAINFO.registerTempTable("F_CM_FIN_PRODAREAINFO")
sql = """
 SELECT DST.PRODCODE                                            --产品代码:src.PRODCODE
       ,DST.PRODZONENO                                         --产品归属法人:src.PRODZONENO
       ,DST.AGTBRNO                                            --代销法人机构码:src.AGTBRNO
       ,DST.AGTZONENO                                          --代销法人:src.AGTZONENO
       ,DST.AGTZONENA                                          --代销法人机构名称:src.AGTZONENA
       ,DST.FEERATE                                            --手续费比例:src.FEERATE
       ,DST.ZONENOMINAMT                                       --法人最低销售额度:src.ZONENOMINAMT
       ,DST.ZONENOMAXAMT                                       --法人最高销售额度:src.ZONENOMAXAMT
       ,DST.AGTAMT                                             --已销售额度:src.AGTAMT
       ,DST.AGTSTATE                                           --销售代理状态:src.AGTSTATE
       ,DST.REGZONENO                                          --登记机构所属法人:src.REGZONENO
       ,DST.REGBRNO                                            --登记机构:src.REGBRNO
       ,DST.REGTELLERNO                                        --登记柜员:src.REGTELLERNO
       ,DST.REGDATE                                            --登记日期:src.REGDATE
       ,DST.REGTIME                                            --登记时间:src.REGTIME
       ,DST.ENSZONENO                                          --确认机构所属法人:src.ENSZONENO
       ,DST.ENSBRNO                                            --确认机构:src.ENSBRNO
       ,DST.ENSTELLERNO                                        --确认柜员:src.ENSTELLERNO
       ,DST.ENSDATE                                            --确认日期:src.ENSDATE
       ,DST.ENSTIME                                            --确认时间:src.ENSTIME
       ,DST.NOTE1                                              --备用1:src.NOTE1
       ,DST.NOTE2                                              --备用2:src.NOTE2
       ,DST.NOTE3                                              --备用3:src.NOTE3
       ,DST.NOTE4                                              --备用4:src.NOTE4
       ,DST.NOTE5                                              --备用5:src.NOTE5
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_CM_FIN_PRODAREAINFO DST 
   LEFT JOIN F_CM_FIN_PRODAREAINFO_INNTMP1 SRC 
     ON SRC.PRODCODE            = DST.PRODCODE 
  WHERE SRC.PRODCODE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_FIN_PRODAREAINFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_CM_FIN_PRODAREAINFO/"+V_DT+".parquet"
UNION=F_CM_FIN_PRODAREAINFO_INNTMP2.unionAll(F_CM_FIN_PRODAREAINFO_INNTMP1)
F_CM_FIN_PRODAREAINFO_INNTMP1.cache()
F_CM_FIN_PRODAREAINFO_INNTMP2.cache()
nrowsi = F_CM_FIN_PRODAREAINFO_INNTMP1.count()
nrowsa = F_CM_FIN_PRODAREAINFO_INNTMP2.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_FIN_PRODAREAINFO/*.parquet")
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CM_FIN_PRODAREAINFO_INNTMP1.unpersist()
F_CM_FIN_PRODAREAINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_FIN_PRODAREAINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CM_FIN_PRODAREAINFO/"+V_DT_LD+".parquet /"+dbname+"/F_CM_FIN_PRODAREAINFO_BK/")

#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_FIN_PRODAREAINFO_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CM_FIN_PRODAREAINFO/"+V_DT+".parquet /"+dbname+"/F_CM_FIN_PRODAREAINFO_BK/"+V_DT+".parquet")
