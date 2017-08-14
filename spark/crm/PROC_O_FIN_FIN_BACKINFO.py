#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_FIN_FIN_BACKINFO').setMaster(sys.argv[2])
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
O_CM_FIN_BACKINFO = sqlContext.read.parquet(hdfs+'/O_CM_FIN_BACKINFO/*')
O_CM_FIN_BACKINFO.registerTempTable("O_CM_FIN_BACKINFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#----------目标表---------------
F_CM_FIN_BACKINFO = sqlContext.read.parquet(hdfs+'/F_CM_FIN_BACKINFO_BK/'+V_DT_LD+'.parquet/*')
F_CM_FIN_BACKINFO.registerTempTable("F_CM_FIN_BACKINFO")

sql = """
 SELECT PRODCODE                AS PRODCODE 
       ,SEQNO                   AS SEQNO 
       ,BACKSDATE               AS BACKSDATE 
       ,BACKEDATE               AS BACKEDATE 
       ,BACKTYPE                AS BACKTYPE 
       ,PERVALUE                AS PERVALUE 
       ,INTYRATE                AS INTYRATE 
       ,FINETYPE                AS FINETYPE 
       ,REGURATE                AS REGURATE 
       ,REGUAMT                 AS REGUAMT 
       ,REGZONENO               AS REGZONENO 
       ,REGBRNO                 AS REGBRNO 
       ,REGTELLERNO             AS REGTELLERNO 
       ,REGDATE                 AS REGDATE 
       ,REGTIME                 AS REGTIME 
       ,NOTE1                   AS NOTE1 
       ,NOTE2                   AS NOTE2 
       ,NOTE3                   AS NOTE3 
       ,NOTE4                   AS NOTE4 
       ,NOTE5                   AS NOTE5 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'FIN'                   AS ODS_SYS_ID 
   FROM O_CM_FIN_BACKINFO A                                    --赎回参数表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_FIN_BACKINFO_INNTMP1 = sqlContext.sql(sql)
F_CM_FIN_BACKINFO_INNTMP1.registerTempTable("F_CM_FIN_BACKINFO_INNTMP1")

#F_CM_FIN_BACKINFO = sqlContext.read.parquet(hdfs+'/F_CM_FIN_BACKINFO/*')
#F_CM_FIN_BACKINFO.registerTempTable("F_CM_FIN_BACKINFO")
sql = """
 SELECT DST.PRODCODE                                            --产品代码:src.PRODCODE
       ,DST.SEQNO                                              --序号:src.SEQNO
       ,DST.BACKSDATE                                          --赎回起始日期:src.BACKSDATE
       ,DST.BACKEDATE                                          --赎回截止日期:src.BACKEDATE
       ,DST.BACKTYPE                                           --赎回方式:src.BACKTYPE
       ,DST.PERVALUE                                           --每份净值:src.PERVALUE
       ,DST.INTYRATE                                           --计息年利率:src.INTYRATE
       ,DST.FINETYPE                                           --罚金方式:src.FINETYPE
       ,DST.REGURATE                                           --固定比率:src.REGURATE
       ,DST.REGUAMT                                            --固定金额:src.REGUAMT
       ,DST.REGZONENO                                          --登记机构所属法人:src.REGZONENO
       ,DST.REGBRNO                                            --登记机构:src.REGBRNO
       ,DST.REGTELLERNO                                        --登记柜员:src.REGTELLERNO
       ,DST.REGDATE                                            --登记日期:src.REGDATE
       ,DST.REGTIME                                            --登记时间:src.REGTIME
       ,DST.NOTE1                                              --备用1:src.NOTE1
       ,DST.NOTE2                                              --备用2:src.NOTE2
       ,DST.NOTE3                                              --备用3:src.NOTE3
       ,DST.NOTE4                                              --备用4:src.NOTE4
       ,DST.NOTE5                                              --备用5:src.NOTE5
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_CM_FIN_BACKINFO DST 
   LEFT JOIN F_CM_FIN_BACKINFO_INNTMP1 SRC 
     ON SRC.PRODCODE            = DST.PRODCODE 
    AND SRC.SEQNO               = DST.SEQNO 
  WHERE SRC.PRODCODE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_FIN_BACKINFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_CM_FIN_BACKINFO/"+V_DT+".parquet"
UNION=F_CM_FIN_BACKINFO_INNTMP2.unionAll(F_CM_FIN_BACKINFO_INNTMP1)
F_CM_FIN_BACKINFO_INNTMP1.cache()
F_CM_FIN_BACKINFO_INNTMP2.cache()
nrowsi = F_CM_FIN_BACKINFO_INNTMP1.count()
nrowsa = F_CM_FIN_BACKINFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CM_FIN_BACKINFO_INNTMP1.unpersist()
F_CM_FIN_BACKINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_FIN_BACKINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_CM_FIN_BACKINFO/"+V_DT_LD+".parquet /"+dbname+"/F_CM_FIN_BACKINFO_BK/")


#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_FIN_BACKINFO/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_FIN_BACKINFO_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CM_FIN_BACKINFO/"+V_DT+".parquet /"+dbname+"/F_CM_FIN_BACKINFO_BK/")
