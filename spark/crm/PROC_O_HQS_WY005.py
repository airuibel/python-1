#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_HQS_WY005').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_HQS_WY005/"+V_DT+".parquet")
#----------来源表---------------
O_TX_HQS_WY005 = sqlContext.read.parquet(hdfs+'/O_TX_HQS_WY005/*')
O_TX_HQS_WY005.registerTempTable("O_TX_HQS_WY005")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.HANHAO                AS HANHAO 
       ,A.JAYIRQ                AS JAYIRQ 
       ,A.WENJZL                AS WENJZL 
       ,A.WENJBH                AS WENJBH 
       ,A.FABUHO                AS FABUHO 
       ,A.JIGUHO                AS JIGUHO 
       ,A.DLYWBM                AS DLYWBM 
       ,A.ZJZCZH                AS ZJZCZH 
       ,A.ZJZRZH                AS ZJZRZH 
       ,A.JAYIJE                AS JAYIJE 
       ,A.XTYYRQ                AS XTYYRQ 
       ,A.JAYLSH                AS JAYLSH 
       ,A.XIAYIM                AS XIAYIM 
       ,A.XIAYXX                AS XIAYXX 
       ,A.QZJYLS                AS QZJYLS 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'HQS'                   AS ODS_SYS_ID 
   FROM O_TX_HQS_WY005 A                                       --代发明细数据
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_HQS_WY005 = sqlContext.sql(sql)
F_TX_HQS_WY005.registerTempTable("F_TX_HQS_WY005")
dfn="F_TX_HQS_WY005/"+V_DT+".parquet"
F_TX_HQS_WY005.cache()
nrows = F_TX_HQS_WY005.count()
F_TX_HQS_WY005.write.save(path=hdfs + '/' + dfn, mode='append')
F_TX_HQS_WY005.unpersist()
O_TX_HQS_WY005.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_HQS_WY005 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
