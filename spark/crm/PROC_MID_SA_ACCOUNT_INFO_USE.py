#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MID_SA_ACCOUNT_INFO_USE').setMaster(sys.argv[2])
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

F_DP_CBOD_SAACNAMT = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNAMT/*')
F_DP_CBOD_SAACNAMT.registerTempTable("F_DP_CBOD_SAACNAMT")
#MID_SA_ACCOUNT_INFO = sqlContext.read.parquet(hdfs+'/MID_SA_ACCOUNT_INFO/*')
#MID_SA_ACCOUNT_INFO.registerTempTable("MID_SA_ACCOUNT_INFO")
F_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNACN/*')
F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT DISTINCT D.SA_CUST_NO            AS SA_CUST_NO 
       ,D.SA_CUST_NAME          AS SA_CUST_NAME 
       ,D.SA_ACCT_NO            AS SA_ACCT_NO 
       ,CONCAT(SUBSTR(D.SA_OPAC_DT, 1, 4), '-', SUBSTR(D.SA_OPAC_DT, 5, 2), '-', SUBSTR(D.SA_OPAC_DT, 7, 2))                       AS SA_OPAC_DT 
       ,D.SA_BELONG_INSTN_COD   AS SA_BELONG_INSTN_COD 
       ,D.FR_ID                 AS FR_ID 
   FROM F_DP_CBOD_SAACNACN D                                   --活存主档
  INNER JOIN F_DP_CBOD_SAACNAMT E                              --活存紫金档
     ON E.FK_SAACN_KEY          = D.SA_ACCT_NO 
    AND E.SA_DDP_ACCT_STS       = '01' 
    AND D.SA_ACCT_CHAR          = '1100' 
    AND D.SA_DEP_TYP <> '98' 
    AND D.FR_ID                 = E.FR_ID 
    AND D.SA_CUST_NO <> 'X9999999999999999999' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_SA_ACCOUNT_INFO = sqlContext.sql(sql)
MID_SA_ACCOUNT_INFO.registerTempTable("MID_SA_ACCOUNT_INFO")
dfn="MID_SA_ACCOUNT_INFO/"+V_DT+".parquet"
MID_SA_ACCOUNT_INFO.cache()
nrows = MID_SA_ACCOUNT_INFO.count() 
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_SA_ACCOUNT_INFO/*")
MID_SA_ACCOUNT_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_SA_ACCOUNT_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_SA_ACCOUNT_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.SA_CUST_NO            AS SA_CUST_NO 
       ,A.SA_CUST_NAME          AS SA_CUST_NAME 
       ,A.SA_ACCT_NO            AS SA_ACCT_NO 
       ,A.SA_OPAC_DT            AS SA_OPAC_DT 
       ,A.FR_ID                 AS FR_ID 
       ,A.SA_BELONG_INSTN_COD   AS ORG_ID 
   FROM MID_SA_ACCOUNT_INFO A                                  --客户基本户账户信息中间表
  INNER JOIN (SELECT FR_ID,SA_CUST_NO,SA_BELONG_INSTN_COD,MAX(SA_OPAC_DT) AS SA_OPAC_DT 
                FROM MID_SA_ACCOUNT_INFO 
               GROUP BY FR_ID,SA_CUST_NO,SA_BELONG_INSTN_COD ) B --客户基本户账户信息中间表
     ON A.SA_CUST_NO            = B.SA_CUST_NO 
    AND A.SA_OPAC_DT            = B.SA_OPAC_DT 
    AND A.SA_BELONG_INSTN_COD   = B.SA_BELONG_INSTN_COD 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_SA_ACCOUNT_INFO_USE = sqlContext.sql(sql)
MID_SA_ACCOUNT_INFO_USE.registerTempTable("MID_SA_ACCOUNT_INFO_USE")
dfn="MID_SA_ACCOUNT_INFO_USE/"+V_DT+".parquet"
MID_SA_ACCOUNT_INFO_USE.cache()
nrows = MID_SA_ACCOUNT_INFO_USE.count()  
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_SA_ACCOUNT_INFO_USE/*.parquet")
MID_SA_ACCOUNT_INFO_USE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_SA_ACCOUNT_INFO_USE.unpersist()

et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_SA_ACCOUNT_INFO_USE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
