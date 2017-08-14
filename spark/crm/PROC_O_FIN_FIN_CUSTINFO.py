#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_FIN_FIN_CUSTINFO').setMaster(sys.argv[2])
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
O_TX_FIN_CUSTINFO = sqlContext.read.parquet(hdfs+'/O_TX_FIN_CUSTINFO/*')
O_TX_FIN_CUSTINFO.registerTempTable("O_TX_FIN_CUSTINFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#----------目标表---------------
F_TX_FIN_CUSTINFO = sqlContext.read.parquet(hdfs+'/F_TX_FIN_CUSTINFO_BK/'+V_DT_LD+'.parquet/*')
F_TX_FIN_CUSTINFO.registerTempTable("F_TX_FIN_CUSTINFO")

sql = """
 SELECT ACCTNO                  AS ACCTNO 
       ,ZONENO                  AS ZONENO 
       ,CUSTTYPE                AS CUSTTYPE 
       ,IDTYPE                  AS IDTYPE 
       ,IDNO                    AS IDNO 
       ,ACCTNAME                AS ACCTNAME 
       ,PURCCHANNEL             AS PURCCHANNEL 
       ,CUSTMANAGE              AS CUSTMANAGE 
       ,ADDRESS                 AS ADDRESS 
       ,TELPHNO                 AS TELPHNO 
       ,PHONENO                 AS PHONENO 
       ,REGSERIALNO             AS REGSERIALNO 
       ,AGENTNAME               AS AGENTNAME 
       ,SIGNSTATE               AS SIGNSTATE 
       ,DATAORI                 AS DATAORI 
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
   FROM O_TX_FIN_CUSTINFO A                                    --客户签约表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_FIN_CUSTINFO_INNTMP1 = sqlContext.sql(sql)
F_TX_FIN_CUSTINFO_INNTMP1.registerTempTable("F_TX_FIN_CUSTINFO_INNTMP1")

#F_TX_FIN_CUSTINFO = sqlContext.read.parquet(hdfs+'/F_TX_FIN_CUSTINFO/*')
#F_TX_FIN_CUSTINFO.registerTempTable("F_TX_FIN_CUSTINFO")
sql = """
 SELECT DST.ACCTNO                                              --存折账号/卡/结算账户账号（单位）:src.ACCTNO
       ,DST.ZONENO                                             --法人号:src.ZONENO
       ,DST.CUSTTYPE                                           --客户类型:src.CUSTTYPE
       ,DST.IDTYPE                                             --证件种类:src.IDTYPE
       ,DST.IDNO                                               --证件号码:src.IDNO
       ,DST.ACCTNAME                                           --户名:src.ACCTNAME
       ,DST.PURCCHANNEL                                        --交易委托渠道:src.PURCCHANNEL
       ,DST.CUSTMANAGE                                         --客户经理代码:src.CUSTMANAGE
       ,DST.ADDRESS                                            --通讯地址:src.ADDRESS
       ,DST.TELPHNO                                            --联系电话（固定电话）:src.TELPHNO
       ,DST.PHONENO                                            --手机号码:src.PHONENO
       ,DST.REGSERIALNO                                        --登记流水:src.REGSERIALNO
       ,DST.AGENTNAME                                          --经办人名称:src.AGENTNAME
       ,DST.SIGNSTATE                                          --签约状态:src.SIGNSTATE
       ,DST.DATAORI                                            --数据来源:src.DATAORI
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
   FROM F_TX_FIN_CUSTINFO DST 
   LEFT JOIN F_TX_FIN_CUSTINFO_INNTMP1 SRC 
     ON SRC.ACCTNO              = DST.ACCTNO 
    AND SRC.ZONENO              = DST.ZONENO 
  WHERE SRC.ACCTNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_FIN_CUSTINFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_TX_FIN_CUSTINFO/"+V_DT+".parquet"
UNION=F_TX_FIN_CUSTINFO_INNTMP2.unionAll(F_TX_FIN_CUSTINFO_INNTMP1)
F_TX_FIN_CUSTINFO_INNTMP1.cache()
F_TX_FIN_CUSTINFO_INNTMP2.cache()
nrowsi = F_TX_FIN_CUSTINFO_INNTMP1.count()
nrowsa = F_TX_FIN_CUSTINFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_TX_FIN_CUSTINFO_INNTMP1.unpersist()
F_TX_FIN_CUSTINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_FIN_CUSTINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_TX_FIN_CUSTINFO/"+V_DT_LD+".parquet /"+dbname+"/F_TX_FIN_CUSTINFO_BK/")
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_FIN_CUSTINFO/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_FIN_CUSTINFO_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_TX_FIN_CUSTINFO/"+V_DT+".parquet /"+dbname+"/F_TX_FIN_CUSTINFO_BK/")