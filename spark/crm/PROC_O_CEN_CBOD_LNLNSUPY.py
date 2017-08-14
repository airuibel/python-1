#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_LNLNSUPY').setMaster(sys.argv[2])
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

O_LN_CBOD_LNLNSUPY = sqlContext.read.parquet(hdfs+'/O_LN_CBOD_LNLNSUPY/*')
O_LN_CBOD_LNLNSUPY.registerTempTable("O_LN_CBOD_LNLNSUPY")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,FK_LNLNS_KEY            AS FK_LNLNS_KEY 
       ,LN_INT_TYP              AS LN_INT_TYP 
       ,LN_INTC_STRT_DT_N       AS LN_INTC_STRT_DT_N 
       ,LN_INTC_CUTDT_N         AS LN_INTC_CUTDT_N 
       ,LN_CAC_INTC_PR          AS LN_CAC_INTC_PR 
       ,LN_INTC_DAYS            AS LN_INTC_DAYS 
       ,LN_INTR                 AS LN_INTR 
       ,LN_INTRBL               AS LN_INTRBL 
       ,LN_ARFN_INT             AS LN_ARFN_INT 
       ,LN_ENTR_DT_N            AS LN_ENTR_DT_N 
       ,LN_LST_TX_DT_N          AS LN_LST_TX_DT_N 
       ,LN_BELONG_INSTN_COD     AS LN_BELONG_INSTN_COD 
       ,LN_ASS_OPUN_NO          AS LN_ASS_OPUN_NO 
       ,LN_FLST_OPUN_NO         AS LN_FLST_OPUN_NO 
       ,LN_RECON_NO             AS LN_RECON_NO 
       ,LN_DB_PART_ID           AS LN_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_LN_CBOD_LNLNSUPY A                                   --放款利息明细档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_CBOD_LNLNSUPY = sqlContext.sql(sql)
F_LN_CBOD_LNLNSUPY.registerTempTable("F_LN_CBOD_LNLNSUPY")
dfn="F_LN_CBOD_LNLNSUPY/"+V_DT+".parquet"
F_LN_CBOD_LNLNSUPY.cache()
nrows = F_LN_CBOD_LNLNSUPY.count()
F_LN_CBOD_LNLNSUPY.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_LN_CBOD_LNLNSUPY.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_CBOD_LNLNSUPY/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_CBOD_LNLNSUPY lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
