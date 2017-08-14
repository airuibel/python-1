#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_DP_PAYROLL').setMaster(sys.argv[2])
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

F_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNACN/*')
F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")
F_TX_AFA_BATTXNINFO = sqlContext.read.parquet(hdfs+'/F_TX_AFA_BATTXNINFO/*')
F_TX_AFA_BATTXNINFO.registerTempTable("F_TX_AFA_BATTXNINFO")
F_TX_AFA_BATINFO = sqlContext.read.parquet(hdfs+'/F_TX_AFA_BATINFO/*')
F_TX_AFA_BATINFO.registerTempTable("F_TX_AFA_BATINFO")
F_DP_CBOD_SAACNTXN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNTXN/*')
F_DP_CBOD_SAACNTXN.registerTempTable("F_DP_CBOD_SAACNTXN")
ACRM_F_DP_PAYROLL = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_PAYROLL_BK/'+V_DT_LD+'.parquet/*')
ACRM_F_DP_PAYROLL.registerTempTable("ACRM_F_DP_PAYROLL")
F_TX_HQS_WY005 = sqlContext.read.parquet(hdfs+'/F_TX_HQS_WY005/*')
F_TX_HQS_WY005.registerTempTable("F_TX_HQS_WY005")
#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT 
         CUST_ID      AS CUST_ID     
        ,CURR_NO      AS CURR_NO     
        ,ACCT_NO      AS ACCT_NO     
        ,DEP_BAL      AS DEP_BAL     
        ,JYJE         AS JYJE        
        ,JYRQ         AS JYRQ        
        ,OP_CUST_ID   AS OP_CUST_ID  
        ,OP_CUST_NAME AS OP_CUST_NAME
        ,OP_ACCT_NO   AS OP_ACCT_NO  
        ,OP_ORG_ID    AS OP_ORG_ID   
        ,BELONG_ORG   AS BELONG_ORG  
        ,CRM_DT       AS CRM_DT      
        ,FR_ID        AS FR_ID 
   FROM ACRM_F_DP_PAYROLL A      
   WHERE CRM_DT > add_months(V_DT,-3)
   """
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_PAYROLL_TMP = sqlContext.sql(sql)
ACRM_F_DP_PAYROLL_TMP.registerTempTable("ACRM_F_DP_PAYROLL_TMP")
dfn="ACRM_F_DP_PAYROLL_TMP/"+V_DT+".parquet"
ACRM_F_DP_PAYROLL_TMP.cache()
nrows = ACRM_F_DP_PAYROLL_TMP.count()
ACRM_F_DP_PAYROLL_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_DP_PAYROLL_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_DP_PAYROLL_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_PAYROLL_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID      AS CUST_ID     
        ,CURR_NO      AS CURR_NO     
        ,ACCT_NO      AS ACCT_NO     
        ,DEP_BAL      AS DEP_BAL     
        ,JYJE         AS JYJE        
        ,JYRQ         AS JYRQ        
        ,OP_CUST_ID   AS OP_CUST_ID  
        ,OP_CUST_NAME AS OP_CUST_NAME
        ,OP_ACCT_NO   AS OP_ACCT_NO  
        ,OP_ORG_ID    AS OP_ORG_ID   
        ,BELONG_ORG   AS BELONG_ORG  
        ,CRM_DT       AS CRM_DT      
        ,FR_ID        AS FR_ID
   FROM ACRM_F_DP_PAYROLL_TMP A                                --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_PAYROLL = sqlContext.sql(sql)
dfn="ACRM_F_DP_PAYROLL/"+V_DT+".parquet"
ACRM_F_DP_PAYROLL.cache()
nrows = ACRM_F_DP_PAYROLL.count()
ACRM_F_DP_PAYROLL.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_DP_PAYROLL.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_DP_PAYROLL/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_PAYROLL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT D.SA_CUST_NO               AS CUST_ID
       ,C.SA_CURR_COD           AS CURR_NO 
       ,C.FK_SAACN_KEY          AS ACCT_NO 
       ,CAST(0 AS DECIMAL(15,2))       AS DEP_BAL 
       ,CAST(A.AMOUNT AS DECIMAL(15,2))      AS JYJE 
       ,A.WORKDATE        AS JYRQ 
       ,''                AS OP_CUST_ID 
       ,A.PAYEENAME       AS OP_CUST_NAME  
       ,A.PAYEEACC      AS OP_ACCT_NO 
       ,A.RECVBANK         AS OP_ORG_ID 
       ,''    AS BELONG_ORG 
       ,V_DT           AS CRM_DT 
       ,A.FR_ID                 AS FR_ID 
   FROM F_TX_AFA_BATTXNINFO A
   JOIN F_TX_AFA_BATINFO B ON B.FR_ID = A.FR_ID AND B.WORKDATE = V_8_DT 
                          AND A.BATNO = B.BATNO AND B.SYSID = '000019' AND B.BATTYP='1'
   JOIN F_DP_CBOD_SAACNTXN C ON C.FR_ID = A.FR_ID AND B.NOTE1 = C.SA_TX_LOG_NO AND C.ODS_ST_DATE = V_DT
         JOIN (SELECT DISTINCT FR_ID,SA_CUST_NO,SA_ACCT_NO 
                 FROM F_DP_CBOD_SAACNACN 
                WHERE SA_CUST_NO LIKE '2%' ) D ON C.FK_SAACN_KEY = D.SA_ACCT_NO AND A.FR_ID = D.FR_ID
        WHERE A.WORKDATE = V_8_DT  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_8_DT\b", "'"+V_DT+"'", sql)
ACRM_F_DP_PAYROLL = sqlContext.sql(sql)
ACRM_F_DP_PAYROLL.registerTempTable("ACRM_F_DP_PAYROLL")
dfn="ACRM_F_DP_PAYROLL/"+V_DT+".parquet"
ACRM_F_DP_PAYROLL.cache()
nrows = ACRM_F_DP_PAYROLL.count()
ACRM_F_DP_PAYROLL.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_DP_PAYROLL.unpersist()
F_TX_AFA_BATTXNINFO.unpersist()
F_TX_AFA_BATINFO.unpersist()
F_DP_CBOD_SAACNTXN.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_PAYROLL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT A.SA_CUST_NO                         AS CUST_ID
       ,'CNY'                                AS CURR_NO 
       ,A.SA_ACCT_NO                         AS ACCT_NO 
       ,CAST(0 AS DECIMAL(15,2))             AS DEP_BAL 
       ,CAST(B.JAYIJE AS DECIMAL(15,2))      AS JYJE 
       ,B.JAYIRQ                             AS JYRQ 
       ,''                                   AS OP_CUST_ID 
       ,''                                   AS OP_CUST_NAME  
       ,B.ZJZRZH                             AS OP_ACCT_NO 
       ,''                                   AS OP_ORG_ID 
       ,''                                   AS BELONG_ORG 
       ,V_DT                                 AS CRM_DT 
       ,A.FR_ID                              AS FR_ID 
   FROM F_DP_CBOD_SAACNACN A,F_TX_HQS_WY005 B
       WHERE A.FR_ID = B.FR_ID 
         AND B.JAYIRQ = V_8_DT 
         AND A.SA_ACCT_NO = B.ZJZCZH 
         AND XIAYXX = '交易成功！' 
         AND A.SA_CUST_NO LIKE '2%' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_8_DT\b", "'"+V_DT+"'", sql)
ACRM_F_DP_PAYROLL = sqlContext.sql(sql)
ACRM_F_DP_PAYROLL.registerTempTable("ACRM_F_DP_PAYROLL")
dfn="ACRM_F_DP_PAYROLL/"+V_DT+".parquet"
ACRM_F_DP_PAYROLL.cache()
nrows = ACRM_F_DP_PAYROLL.count()
ACRM_F_DP_PAYROLL.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_DP_PAYROLL.unpersist()
F_DP_CBOD_SAACNACN.unpersist()
F_TX_HQS_WY005.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_PAYROLL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_DP_PAYROLL_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_DP_PAYROLL/"+V_DT+".parquet /"+dbname+"/ACRM_F_DP_PAYROLL_BK/")