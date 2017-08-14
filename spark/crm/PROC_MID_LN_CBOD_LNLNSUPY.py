#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MID_LN_CBOD_LNLNSUPY').setMaster(sys.argv[2])
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

F_LN_CBOD_LNLNSUPY = sqlContext.read.parquet(hdfs+'/F_LN_CBOD_LNLNSUPY/*')
F_LN_CBOD_LNLNSUPY.registerTempTable("F_LN_CBOD_LNLNSUPY")
#目标表
#MID_LN_CBOD_LNLNSUPY_2：全量
#MID_LN_CBOD_LNLNSUPY：全量

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,FK_LNLNS_KEY            AS FK_LNLNS_KEY 
       ,LN_INT_TYP              AS LN_INT_TYP 
       ,CAST(LN_INTC_STRT_DT_N       AS DECIMAL(8))                       AS LN_INTC_STRT_DT_N 
       ,CAST(''                      AS DECIMAL(15))                       AS LNUPY_DB_TIMESTAMP 
       ,CAST(LN_INTC_CUTDT_N         AS DECIMAL(8))                       AS LN_INTC_CUTDT_N 
       ,CAST(LN_CAC_INTC_PR          AS DECIMAL(15, 2))                       AS LN_CAC_INTC_PR 
       ,CAST(LN_INTC_DAYS            AS DECIMAL(3))                       AS LN_INTC_DAYS 
       ,CAST(LN_INTR                 AS DECIMAL(8, 5))                       AS LN_INTR 
       ,CAST(LN_INTRBL               AS DECIMAL(15, 2))                       AS LN_INTRBL 
       ,CAST(LN_ARFN_INT             AS DECIMAL(15, 2))                       AS LN_ARFN_INT 
       ,CAST(LN_ENTR_DT_N            AS DECIMAL(8))                       AS LN_ENTR_DT_N 
       ,CAST(LN_LST_TX_DT_N          AS DECIMAL(8))                       AS LN_LST_TX_DT_N 
       ,LN_BELONG_INSTN_COD     AS LN_BELONG_INSTN_COD 
       ,LN_ASS_OPUN_NO          AS LN_ASS_OPUN_NO 
       ,LN_FLST_OPUN_NO         AS LN_FLST_OPUN_NO 
       ,LN_RECON_NO             AS LN_RECON_NO 
       ,LN_DB_PART_ID           AS LN_DB_PART_ID 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
       ,ODS_SYS_ID              AS ODS_SYS_ID 
       ,CAST(NVL(LN_INTRBL, 0) - NVL(LN_ARFN_INT, 0)                       AS DECIMAL(24, 6))   AS SHOULD_INT 
   FROM F_LN_CBOD_LNLNSUPY A                                   --
  WHERE SUBSTR(ETLDT, 1, 4)                       = YEAR(V_DT) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_LN_CBOD_LNLNSUPY = sqlContext.sql(sql)
MID_LN_CBOD_LNLNSUPY.registerTempTable("MID_LN_CBOD_LNLNSUPY")
dfn="MID_LN_CBOD_LNLNSUPY/"+V_DT+".parquet"
MID_LN_CBOD_LNLNSUPY.cache()
nrows = MID_LN_CBOD_LNLNSUPY.count()
MID_LN_CBOD_LNLNSUPY.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_LN_CBOD_LNLNSUPY.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_LN_CBOD_LNLNSUPY/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_LN_CBOD_LNLNSUPY lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT FK_LNLNS_KEY            AS ACCT_NO 
       ,CAST(SUM(SHOULD_INT)                       AS DECIMAL(24, 6))                       AS SHOULD_INT 
   FROM MID_LN_CBOD_LNLNSUPY A                                 --
  GROUP BY FK_LNLNS_KEY """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_LN_CBOD_LNLNSUPY_2 = sqlContext.sql(sql)
MID_LN_CBOD_LNLNSUPY_2.registerTempTable("MID_LN_CBOD_LNLNSUPY_2")
dfn="MID_LN_CBOD_LNLNSUPY_2/"+V_DT+".parquet"
MID_LN_CBOD_LNLNSUPY_2.cache()
nrows = MID_LN_CBOD_LNLNSUPY_2.count()
MID_LN_CBOD_LNLNSUPY_2.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_LN_CBOD_LNLNSUPY_2.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_LN_CBOD_LNLNSUPY_2/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_LN_CBOD_LNLNSUPY_2 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
