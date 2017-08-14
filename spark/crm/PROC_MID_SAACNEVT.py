#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MID_SAACNEVT').setMaster(sys.argv[2])
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

F_DP_CBOD_SAACNEVT = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNEVT/*')
F_DP_CBOD_SAACNEVT.registerTempTable("F_DP_CBOD_SAACNEVT")
#目标表
#MID_F_DP_CBOD_SAACNEVT：全量

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT C.ETLDT                   AS ETLDT 
       ,''                    AS SAEVT_LL 
       ,C.FK_SAACN_KEY          AS FK_SAACN_KEY 
       ,C.SA_ACCD_COD           AS SA_ACCD_COD 
       ,C.SA_ACCD_DT            AS SA_ACCD_DT 
       ,C.SA_EVT_SRL_NO_N       AS SA_EVT_SRL_NO_N 
       ,''                    AS SAEVT_DB_TIMESTAMP 
       ,C.SA_ACCD_TM              AS SA_ACCD_TM 
       ,C.SA_CURR_COD             AS SA_CURR_COD 
       ,C.SA_OPR_NO               AS SA_OPR_NO 
       ,C.SA_CURR_IDEN            AS SA_CURR_IDEN 
       ,C.SA_ACCD_RMRK            AS SA_ACCD_RMRK 
       ,C.SA_ACCD_INSTN_NO        AS SA_ACCD_INSTN_NO 
       ,C.SA_ACCD_AMT             AS SA_ACCD_AMT 
       ,C.SA_CERT_ID              AS SA_CERT_ID 
       ,C.SA_CERT_TYP             AS SA_CERT_TYP 
       ,C.SA_PROCESS_FLAG         AS SA_PROCESS_FLAG 
       ,C.SA_PRO_EVT_SRL_NO_N     AS SA_PRO_EVT_SRL_NO_N 
       ,C.SA_UNLCK_DT             AS SA_UNLCK_DT 
       ,C.SA_FRZ_TYPE_FLG         AS SA_FRZ_TYPE_FLG 
       ,C.SA_ACCD_AMT_1           AS SA_ACCD_AMT_1 
       ,C.SA_CURR_IDEN_1          AS SA_CURR_IDEN_1 
       ,C.SA_ACCT_DT_CLONE        AS SA_ACCT_DT_CLONE 
       ,C.SA_BRANCH_NAME          AS SA_BRANCH_NAME 
       ,C.SA_SHIFT_ADV_NO         AS SA_SHIFT_ADV_NO 
       ,C.SA_STAF_NAME            AS SA_STAF_NAME 
       ,C.SA_B_AUTH_PIC_NO        AS SA_B_AUTH_PIC_NO 
       ,C.SA_A_AUTH_PIC_NO        AS SA_A_AUTH_PIC_NO 
       ,C.SA_AGT_CERT_TYP         AS SA_AGT_CERT_TYP 
       ,C.SA_AGT_CERT_ID          AS SA_AGT_CERT_ID 
       ,C.SA_AGT_CUST_NAME        AS SA_AGT_CUST_NAME 
       ,C.SA_BELONG_INSTN_COD     AS SA_BELONG_INSTN_COD 
       ,C.SA_EVT_SRL_CLONE_N      AS SA_EVT_SRL_CLONE_N 
       ,C.SA_BRANCH_TYPE          AS SA_BRANCH_TYPE 
       ,C.SA_CLR_INTR             AS SA_CLR_INTR 
       ,C.SA_PDP_CODE             AS SA_PDP_CODE 
       ,C.SA_DB_PART_ID           AS SA_DB_PART_ID 
       ,C.FR_ID                   AS FR_ID 
       ,C.ODS_ST_DATE             AS ODS_ST_DATE 
       ,C.ODS_SYS_ID              AS ODS_SYS_ID 
   FROM F_DP_CBOD_SAACNEVT C                                   --
  INNER JOIN F_DP_CBOD_SAACNEVT A                              --
     ON A.FK_SAACN_KEY          = C.FK_SAACN_KEY 
    AND C.SA_EVT_SRL_NO_N       = A.SA_EVT_SRL_NO_N 
  WHERE C.SA_ACCD_COD IN('1112', '1122', '1121', '1123', '1221', '1222', '1223', '1212') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_F_DP_CBOD_SAACNEVT = sqlContext.sql(sql)
MID_F_DP_CBOD_SAACNEVT.registerTempTable("MID_F_DP_CBOD_SAACNEVT")
dfn="MID_F_DP_CBOD_SAACNEVT/"+V_DT+".parquet"
MID_F_DP_CBOD_SAACNEVT.cache()
nrows = MID_F_DP_CBOD_SAACNEVT.count()
MID_F_DP_CBOD_SAACNEVT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_F_DP_CBOD_SAACNEVT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_F_DP_CBOD_SAACNEVT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_F_DP_CBOD_SAACNEVT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
