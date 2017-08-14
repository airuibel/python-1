#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CUST_ELEC_PAYROLL').setMaster(sys.argv[2])
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
#---------------------------------------------------------------------------------------#
V_YEAR_MONTH =  etl_date[0:4]+"-" + etl_date[4:6]

OCRM_F_CUST_ELECTRICITY_COST = sqlContext.read.parquet(hdfs+'/OCRM_F_CUST_ELECTRICITY_COST/*')
OCRM_F_CUST_ELECTRICITY_COST.registerTempTable("OCRM_F_CUST_ELECTRICITY_COST")
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
ACRM_F_DP_PAYROLL = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_PAYROLL/*')
ACRM_F_DP_PAYROLL.registerTempTable("ACRM_F_DP_PAYROLL")
ACRM_A_CUST_ELEC_PAYROLL = sqlContext.read.parquet(hdfs+'/ACRM_A_CUST_ELEC_PAYROLL_BK/'+V_DT_LD+'.parquet/*')
ACRM_A_CUST_ELEC_PAYROLL.registerTempTable("ACRM_A_CUST_ELEC_PAYROLL")

#任务[21] 001-01::
V_STEP = V_STEP + 1
sql = """
	SELECT   	A.CUST_ID,
                A.ELEC_AMT,
                A.PAY_NUM,
                A.FR_ID,
                A.ETL_DATE              
          FROM  ACRM_A_CUST_ELEC_PAYROLL A
          WHERE A.ETL_DATE != V_DT AND A.ETL_DATE != add_months(V_DT,-12)
	 """
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
print(sql)
ACRM_A_CUST_ELEC_PAYROLL_OLD = sqlContext.sql(sql)

sql = """
	SELECT   	A.CUST_ID,
                COALESCE(B.SUM_PRICE,0) AS ELEC_AMT,
                COALESCE(C.NUM,0) AS PAY_NUM,
                A.FR_ID,
                V_DT              
          FROM  OCRM_F_CI_COM_CUST_INFO A
          LEFT JOIN  OCRM_F_CUST_ELECTRICITY_COST B ON  A.CUST_ID = B.CUST_ID AND B.FR_ID = A.FR_ID
          LEFT JOIN  ( 
                        SELECT M.CUST_ID,M.FR_ID,COUNT(1) as NUM
                             FROM  ACRM_F_DP_PAYROLL  M 
                            -- WHERE M.FR_ID = V_FR_ID
                            GROUP  BY M.CUST_ID,M.FR_ID 
                               ) C 
                ON   A.CUST_ID = C.CUST_ID AND C.FR_ID = A.FR_ID
	 """
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
print(sql)
ACRM_A_CUST_ELEC_PAYROLL_ADD = sqlContext.sql(sql)
ACRM_A_CUST_ELEC_PAYROLL = ACRM_A_CUST_ELEC_PAYROLL_OLD.unionAll(ACRM_A_CUST_ELEC_PAYROLL_ADD)
dfn="ACRM_A_CUST_ELEC_PAYROLL/"+V_DT+".parquet"
ACRM_A_CUST_ELEC_PAYROLL.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CUST_ELEC_PAYROLL/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CUST_ELEC_PAYROLL_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_A_CUST_ELEC_PAYROLL/"+V_DT+".parquet /"+dbname+"/ACRM_A_CUST_ELEC_PAYROLL_BK/")