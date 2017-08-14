#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CRE_ODUE2').setMaster(sys.argv[2])
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

O_CR_ODUE2 = sqlContext.read.parquet(hdfs+'/O_CR_ODUE2/*')
O_CR_ODUE2.registerTempTable("O_CR_ODUE2")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT BANK                    AS BANK 
       ,XACCOUNT                AS XACCOUNT 
       ,BRANCH                  AS BRANCH 
       ,CARD_BIN                AS CARD_BIN 
       ,PRODUCT                 AS PRODUCT 
       ,CURR_NUM                AS CURR_NUM 
       ,INP_DAY                 AS INP_DAY 
       ,DRAFT_DAY               AS DRAFT_DAY 
       ,MONTH_NBR               AS MONTH_NBR 
       ,ODUE_FLAG               AS ODUE_FLAG 
       ,INP_BAL                 AS INP_BAL 
       ,INP_BALORI              AS INP_BALORI 
       ,REM_BAL                 AS REM_BAL 
       ,REM_BALORI              AS REM_BALORI 
       ,LAST_PAYMT              AS LAST_PAYMT 
       ,INP_NOINT               AS INP_NOINT 
       ,REM_NOINT               AS REM_NOINT 
       ,INP_NINT01              AS INP_NINT01 
       ,INP_NINT02              AS INP_NINT02 
       ,INP_NINT03              AS INP_NINT03 
       ,INP_NINT04              AS INP_NINT04 
       ,INP_NINT05              AS INP_NINT05 
       ,INP_NINT06              AS INP_NINT06 
       ,INP_NINT07              AS INP_NINT07 
       ,INP_NINT08              AS INP_NINT08 
       ,INP_NINT09              AS INP_NINT09 
       ,INP_NINT10              AS INP_NINT10 
       ,REM_NINT01              AS REM_NINT01 
       ,REM_NINT02              AS REM_NINT02 
       ,REM_NINT03              AS REM_NINT03 
       ,REM_NINT04              AS REM_NINT04 
       ,REM_NINT05              AS REM_NINT05 
       ,REM_NINT06              AS REM_NINT06 
       ,REM_NINT07              AS REM_NINT07 
       ,REM_NINT08              AS REM_NINT08 
       ,REM_NINT09              AS REM_NINT09 
       ,REM_NINT10              AS REM_NINT10 
       ,ORG_NO                  AS ORG_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CRE'                   AS ODS_SYS_ID 
   FROM O_CR_ODUE2 A                                           --账户逾期记录表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CR_ODUE2 = sqlContext.sql(sql)
F_CR_ODUE2.registerTempTable("F_CR_ODUE2")
dfn="F_CR_ODUE2/"+V_DT+".parquet"
F_CR_ODUE2.cache()
nrows = F_CR_ODUE2.count()
F_CR_ODUE2.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CR_ODUE2.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CR_ODUE2/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CR_ODUE2 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
