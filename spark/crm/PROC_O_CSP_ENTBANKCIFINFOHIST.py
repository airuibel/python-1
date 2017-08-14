#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CSP_ENTBANKCIFINFOHIST').setMaster(sys.argv[2])
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

#删除当天的
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CSP_ENTBANKCIFINFOHIST/"+V_DT+".parquet")
#----------来源表---------------
O_CSP_ENTBANKCIFINFOHIST = sqlContext.read.parquet(hdfs+'/O_CSP_ENTBANKCIFINFOHIST/*')
O_CSP_ENTBANKCIFINFOHIST.registerTempTable("O_CSP_ENTBANKCIFINFOHIST")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT MAINTJNLNO              AS MAINTJNLNO 
       ,MAINTCODE               AS MAINTCODE 
       ,MAINTMCHANNELID         AS MAINTMCHANNELID 
       ,MAINTDATE               AS MAINTDATE 
       ,MAINTTIMESTAMP          AS MAINTTIMESTAMP 
       ,CIFSEQ                  AS CIFSEQ 
       ,CIFNO                   AS CIFNO 
       ,STATE                   AS STATE 
       ,DEPTID                  AS DEPTID 
       ,CORECIFNAME             AS CORECIFNAME 
       ,CORECIFENGNAME          AS CORECIFENGNAME 
       ,COREADDRESS             AS COREADDRESS 
       ,COREPOSTCODE            AS COREPOSTCODE 
       ,CORELEGALNAME           AS CORELEGALNAME 
       ,CORECONTACTTEL          AS CORECONTACTTEL 
       ,CORELEGALPAPERTYPE      AS CORELEGALPAPERTYPE 
       ,CORELEGALPAPERNO        AS CORELEGALPAPERNO 
       ,CORECIFLEVEL            AS CORECIFLEVEL 
       ,CIFNAME                 AS CIFNAME 
       ,INDUSTRYCODE            AS INDUSTRYCODE 
       ,ENTPROP                 AS ENTPROP 
       ,CIFLEVEL                AS CIFLEVEL 
       ,CIFTYPE                 AS CIFTYPE 
       ,MERTTYPE                AS MERTTYPE 
       ,RELATIONNAME            AS RELATIONNAME 
       ,RELATIONIDTYPE          AS RELATIONIDTYPE 
       ,RELATIONIDNO            AS RELATIONIDNO 
       ,RELATIONTELEPHONE       AS RELATIONTELEPHONE 
       ,RELATIONMOBILE          AS RELATIONMOBILE 
       ,RELATIONEMAIL           AS RELATIONEMAIL 
       ,TRANSFERTYPE            AS TRANSFERTYPE 
       ,CIFMANAGERSEQ           AS CIFMANAGERSEQ 
       ,CIFMANAGERNAME          AS CIFMANAGERNAME 
       ,CIFMANAGERTEL           AS CIFMANAGERTEL 
       ,CIFMANAGERDEPT          AS CIFMANAGERDEPT 
       ,REFEREETYPE             AS REFEREETYPE 
       ,REFEREENAME             AS REFEREENAME 
       ,REFEREEID               AS REFEREEID 
       ,REFEREECIFSEQ           AS REFEREECIFSEQ 
       ,ADDTELLERNO             AS ADDTELLERNO 
       ,CREATETIME              AS CREATETIME 
       ,UPDTELLERNO             AS UPDTELLERNO 
       ,UPDATETIME              AS UPDATETIME 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CSP'                   AS ODS_SYS_ID 
   FROM O_CSP_ENTBANKCIFINFOHIST A                             --企业电子银行客户信息历史表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CSP_ENTBANKCIFINFOHIST = sqlContext.sql(sql)
F_CSP_ENTBANKCIFINFOHIST.registerTempTable("F_CSP_ENTBANKCIFINFOHIST")
dfn="F_CSP_ENTBANKCIFINFOHIST/"+V_DT+".parquet"
F_CSP_ENTBANKCIFINFOHIST.cache()
nrows = F_CSP_ENTBANKCIFINFOHIST.count()
F_CSP_ENTBANKCIFINFOHIST.write.save(path=hdfs + '/' + dfn, mode='append')
F_CSP_ENTBANKCIFINFOHIST.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CSP_ENTBANKCIFINFOHIST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
