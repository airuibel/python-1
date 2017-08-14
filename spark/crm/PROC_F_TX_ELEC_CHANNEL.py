#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_TX_ELEC_CHANNEL').setMaster(sys.argv[2])
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

#清除当天数据，支持重跑
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_TX_ELEC_CHANNEL/"+V_DT+".parquet")


F_TX_ZFB_HT_TRANS = sqlContext.read.parquet(hdfs+'/F_TX_ZFB_HT_TRANS/*')
F_TX_ZFB_HT_TRANS.registerTempTable("F_TX_ZFB_HT_TRANS")
F_TX_WSYH_ACCESSLOGMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ACCESSLOGMCH/*')
F_TX_WSYH_ACCESSLOGMCH.registerTempTable("F_TX_WSYH_ACCESSLOGMCH")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_TX_WSYH_MCJNL = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_MCJNL/*')
F_TX_WSYH_MCJNL.registerTempTable("F_TX_WSYH_MCJNL")
F_CI_WSYH_ECCIFID = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFID/*')
F_CI_WSYH_ECCIFID.registerTempTable("F_CI_WSYH_ECCIFID")
F_TX_WSYH_ECCIF = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIF/*')
F_TX_WSYH_ECCIF.registerTempTable("F_TX_WSYH_ECCIF")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ''                    AS ID 
       ,E.ODS_CUST_ID           AS CUST_ID 
       ,A.ACCESSSTATE           AS ACCESSSTATE 
       ,'1' AS BANKSEQ
       ,A.ACCESSIP              AS ACCESSIP 
       ,A.CHANNELID             AS CHANNELID 
       ,A.LOGINTYPE             AS LOGINTYPE 
       ,A.USERSEQ               AS USERSEQ 
       ,CASE WHEN LENGTH(A.ACCESSDATE) = 8 
	     THEN CONCAT(SUBSTR(A.ACCESSDATE, 1, 4),'-',SUBSTR(A.ACCESSDATE, 5, 2),'-',SUBSTR(A.ACCESSDATE, 7, 2)) 
		 END AS ACCESSDATE 
       ,''                    AS TRANS_AMT 
       ,V_DT                    AS ODS_ST_DATE 
       ,'IBK'                   AS ODS_SYS_ID 
   FROM F_TX_WSYH_ACCESSLOGMCH A                               --访问日志表
  INNER JOIN F_TX_WSYH_MCJNL B                                 --电子银行日志表
     ON A.ACCESSJNLNO           = B.JNLNO 
    AND B.TRANSDATE             = V_8_DT 
  INNER JOIN F_TX_WSYH_ECCIF C                                 --电子银行参与方信息表
     ON A.USERSEQ               = C.CIFSEQ 
  INNER JOIN F_CI_WSYH_ECCIFID D                               --客户证件表
     ON C.CIFSEQ                = D.CIFSEQ 
    AND C.FR_ID                 = D.FR_ID 
  INNER JOIN OCRM_F_CI_SYS_RESOURCE E                          --系统来源中间表
     ON C.CIFNAME               = E.SOURCE_CUST_NAME 
    AND D.IDTYPE                = E.CERT_TYPE 
    AND D.IDNO                  = E.CERT_NO 
    AND E.ODS_SYS_ID IN('IBK', 'MBK') 
    AND E.ODS_CUST_TYPE         = '2' 
  WHERE A.CHANNELID IN('PIBS', 'EIBS', 'PMBS') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_8_DT\b", "'"+V_DT+"'", sql)

ACRM_F_TX_ELEC_CHANNEL = sqlContext.sql(sql)
ACRM_F_TX_ELEC_CHANNEL.registerTempTable("ACRM_F_TX_ELEC_CHANNEL")
dfn="ACRM_F_TX_ELEC_CHANNEL/"+V_DT+".parquet"
ACRM_F_TX_ELEC_CHANNEL.cache()
nrows = ACRM_F_TX_ELEC_CHANNEL.count()
ACRM_F_TX_ELEC_CHANNEL.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_TX_ELEC_CHANNEL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_TX_ELEC_CHANNEL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT ''                    AS ID 
       ,USER_CIFNO              AS CUST_ID 
       ,TRANS_STATUS            AS ACCESSSTATE 
       ,''                    AS BANKSEQ 
       ,''                    AS ACCESSIP 
       ,''                    AS CHANNELID 
       ,''                    AS LOGINTYPE 
       ,''                    AS USERSEQ 
       ,CONCAT(SUBSTR(A.TRANS_DATE, 1, 4),'-',SUBSTR(A.TRANS_DATE, 5, 2),'-',SUBSTR(A.TRANS_DATE, 7, 2)) AS ACCESSDATE
       ,CAST(TRANS_AMT    AS DECIMAL(15,2))           AS TRANS_AMT 
       ,V_DT                    AS ODS_ST_DATE 
       ,'APY'                   AS ODS_SYS_ID 
   FROM F_TX_ZFB_HT_TRANS A                                    --交易明细表(历史)
  WHERE TRANS_DATE              = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_TX_ELEC_CHANNEL = sqlContext.sql(sql)
ACRM_F_TX_ELEC_CHANNEL.registerTempTable("ACRM_F_TX_ELEC_CHANNEL")
dfn="ACRM_F_TX_ELEC_CHANNEL/"+V_DT+".parquet"
ACRM_F_TX_ELEC_CHANNEL.cache()
nrows = ACRM_F_TX_ELEC_CHANNEL.count()
ACRM_F_TX_ELEC_CHANNEL.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_TX_ELEC_CHANNEL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_TX_ELEC_CHANNEL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
