#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_LNLNSINT').setMaster(sys.argv[2])
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

ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_LNLNSINT/"+V_DT+".parquet")
O_CM_CBOD_LNLNSINT = sqlContext.read.parquet(hdfs+'/O_CM_CBOD_LNLNSINT/*')
O_CM_CBOD_LNLNSINT.registerTempTable("O_CM_CBOD_LNLNSINT")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,FK_LNLNS_KEY            AS FK_LNLNS_KEY 
       ,LN_INTR_EFF_STRT_DT_N   AS LN_INTR_EFF_STRT_DT_N 
       ,LN_INTR_EFF_CUTDT_N     AS LN_INTR_EFF_CUTDT_N 
       ,LN_INTR_ACOR_STY        AS LN_INTR_ACOR_STY 
       ,LN_INTR_TYP             AS LN_INTR_TYP 
       ,LN_INTR_NEGO_SYMB       AS LN_INTR_NEGO_SYMB 
       ,LN_INTR_NEGO_RATE       AS LN_INTR_NEGO_RATE 
       ,LN_DLAY_INTR_ACOR_STY   AS LN_DLAY_INTR_ACOR_STY 
       ,LN_DLAY_INTR_TYP        AS LN_DLAY_INTR_TYP 
       ,LN_DLAY_INTR_PLMN_SYMB  AS LN_DLAY_INTR_PLMN_SYMB 
       ,LN_DLAY_INTR_PLMN_COD   AS LN_DLAY_INTR_PLMN_COD 
       ,LN_BELONG_INSTN_COD     AS LN_BELONG_INSTN_COD 
       ,LN_ASS_OPUN_NO          AS LN_ASS_OPUN_NO 
       ,LN_FLST_OPUN_NO         AS LN_FLST_OPUN_NO 
       ,LN_DB_PART_ID           AS LN_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CM_CBOD_LNLNSINT A                                   --放款利率异动档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_LNLNSINT = sqlContext.sql(sql)
F_CM_CBOD_LNLNSINT.registerTempTable("F_CM_CBOD_LNLNSINT")
dfn="F_CM_CBOD_LNLNSINT/"+V_DT+".parquet"
F_CM_CBOD_LNLNSINT.cache()
nrows = F_CM_CBOD_LNLNSINT.count()
F_CM_CBOD_LNLNSINT.write.save(path=hdfs + '/' + dfn, mode='append')
F_CM_CBOD_LNLNSINT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_LNLNSINT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
