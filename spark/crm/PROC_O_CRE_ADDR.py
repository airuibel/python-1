#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CRE_ADDR').setMaster(sys.argv[2])
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

O_CI_ADDR = sqlContext.read.parquet(hdfs+'/O_CI_ADDR/*')
O_CI_ADDR.registerTempTable("O_CI_ADDR")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CI_ADDR = sqlContext.read.parquet(hdfs+'/F_CI_ADDR_BK/'+V_DT_LD+'.parquet/*')
F_CI_ADDR.registerTempTable("F_CI_ADDR")

sql = """
 SELECT BANK                    AS BANK 
       ,CARD_NBR                AS CARD_NBR 
       ,CARD_BIN                AS CARD_BIN 
       ,XACCOUNT                AS XACCOUNT 
       ,ADDR_TYPE               AS ADDR_TYPE 
       ,ADDR_LINE1              AS ADDR_LINE1 
       ,ADDR_LINE2              AS ADDR_LINE2 
       ,ADDR_LINE3              AS ADDR_LINE3 
       ,ADDR_LINE4              AS ADDR_LINE4 
       ,ADDR_LINE5              AS ADDR_LINE5 
       ,POST_CODE               AS POST_CODE 
       ,CREATE_DAY              AS CREATE_DAY 
       ,ORG_NO                  AS ORG_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CRE'                   AS ODS_SYS_ID 
   FROM O_CI_ADDR A                                            --客户账户信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_ADDR_INNTMP1 = sqlContext.sql(sql)
F_CI_ADDR_INNTMP1.registerTempTable("F_CI_ADDR_INNTMP1")

#F_CI_ADDR = sqlContext.read.parquet(hdfs+'/F_CI_ADDR/*')
#F_CI_ADDR.registerTempTable("F_CI_ADDR")
sql = """
 SELECT DST.BANK                                                --银行:src.BANK
       ,DST.CARD_NBR                                           --卡号:src.CARD_NBR
       ,DST.CARD_BIN                                           --BIN号:src.CARD_BIN
       ,DST.XACCOUNT                                           --账号:src.XACCOUNT
       ,DST.ADDR_TYPE                                          --地址类型:src.ADDR_TYPE
       ,DST.ADDR_LINE1                                         --地址栏1:src.ADDR_LINE1
       ,DST.ADDR_LINE2                                         --地址栏2:src.ADDR_LINE2
       ,DST.ADDR_LINE3                                         --地址栏3:src.ADDR_LINE3
       ,DST.ADDR_LINE4                                         --地址栏4:src.ADDR_LINE4
       ,DST.ADDR_LINE5                                         --地址栏5:src.ADDR_LINE5
       ,DST.POST_CODE                                          --邮编:src.POST_CODE
       ,DST.CREATE_DAY                                         --创建日期:src.CREATE_DAY
       ,DST.ORG_NO                                             --机构编号:src.ORG_NO
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_CI_ADDR DST 
   LEFT JOIN F_CI_ADDR_INNTMP1 SRC 
     ON SRC.BANK                = DST.BANK 
    AND SRC.CARD_BIN            = DST.CARD_BIN 
    AND SRC.XACCOUNT            = DST.XACCOUNT 
    AND SRC.ADDR_TYPE           = DST.ADDR_TYPE 
  WHERE SRC.BANK IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_ADDR_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_ADDR/"+V_DT+".parquet"
UNION=F_CI_ADDR_INNTMP2.unionAll(F_CI_ADDR_INNTMP1)
F_CI_ADDR_INNTMP1.cache()
F_CI_ADDR_INNTMP2.cache()
nrowsi = F_CI_ADDR_INNTMP1.count()
nrowsa = F_CI_ADDR_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_ADDR_INNTMP1.unpersist()
F_CI_ADDR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_ADDR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_ADDR/"+V_DT_LD+".parquet /"+dbname+"/F_CI_ADDR_BK/")

#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_ADDR/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_ADDR_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CI_ADDR/"+V_DT+".parquet /"+dbname+"/F_CI_ADDR_BK/")