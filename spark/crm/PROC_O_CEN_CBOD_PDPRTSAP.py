#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_PDPRTSAP').setMaster(sys.argv[2])
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

O_CM_CBOD_PDPRTSAP = sqlContext.read.parquet(hdfs+'/O_CM_CBOD_PDPRTSAP/*')
O_CM_CBOD_PDPRTSAP.registerTempTable("O_CM_CBOD_PDPRTSAP")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,PD_WORD_BANK_COD        AS PD_WORD_BANK_COD 
       ,PD_WORD_CLSFN           AS PD_WORD_CLSFN 
       ,PD_WORD_PROV_COD        AS PD_WORD_PROV_COD 
       ,PD_PD_SEQ               AS PD_PD_SEQ 
       ,PD_CUR_COD              AS PD_CUR_COD 
       ,PD_CURR_IDEN            AS PD_CURR_IDEN 
       ,PD_INTC_TYP             AS PD_INTC_TYP 
       ,PD_INTC_TYPE            AS PD_INTC_TYPE 
       ,PD_CLSD_INTC_TYP        AS PD_CLSD_INTC_TYP 
       ,PD_CLSD_INTC_DT         AS PD_CLSD_INTC_DT 
       ,PD_INTR_TYP             AS PD_INTR_TYP 
       ,PD_SIGNED_MNG_ACCT_FLG  AS PD_SIGNED_MNG_ACCT_FLG 
       ,PD_NGO_INTR_TYP1        AS PD_NGO_INTR_TYP1 
       ,PD_NGO_FDEP_AMT1        AS PD_NGO_FDEP_AMT1 
       ,PD_NGO_INTR_TYP2        AS PD_NGO_INTR_TYP2 
       ,PD_NGO_FDEP_AMT2        AS PD_NGO_FDEP_AMT2 
       ,PD_NGO_INTR_TYP3        AS PD_NGO_INTR_TYP3 
       ,PD_NGO_FDEP_AMT3        AS PD_NGO_FDEP_AMT3 
       ,PD_NGO_INTR_TYP4        AS PD_NGO_INTR_TYP4 
       ,PD_NGO_FDEP_AMT4        AS PD_NGO_FDEP_AMT4 
       ,PD_NGO_INTR_TYP5        AS PD_NGO_INTR_TYP5 
       ,PD_NGO_FDEP_AMT5        AS PD_NGO_FDEP_AMT5 
       ,PD_CACCT_INTC_TYP       AS PD_CACCT_INTC_TYP 
       ,PD_INTR_COD             AS PD_INTR_COD 
       ,PD_INTR_FLOAT_MAX_LMT   AS PD_INTR_FLOAT_MAX_LMT 
       ,PD_INTR_FLOAT_MIN_LMT   AS PD_INTR_FLOAT_MIN_LMT 
       ,PD_CACL_INTR_CYCL       AS PD_CACL_INTR_CYCL 
       ,PD_CACL_INTR_DT         AS PD_CACL_INTR_DT 
       ,PD_CACL_INTR_FLG        AS PD_CACL_INTR_FLG 
       ,PD_NGO_FLG              AS PD_NGO_FLG 
       ,PD_DEP_TYP_DESC         AS PD_DEP_TYP_DESC 
       ,PD_RSV_INTR_TYP         AS PD_RSV_INTR_TYP 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CM_CBOD_PDPRTSAP A                                   --活期利息部件
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_PDPRTSAP = sqlContext.sql(sql)
F_CM_CBOD_PDPRTSAP.registerTempTable("F_CM_CBOD_PDPRTSAP")
dfn="F_CM_CBOD_PDPRTSAP/"+V_DT+".parquet"
F_CM_CBOD_PDPRTSAP.cache()
nrows = F_CM_CBOD_PDPRTSAP.count()
F_CM_CBOD_PDPRTSAP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_CBOD_PDPRTSAP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_PDPRTSAP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_PDPRTSAP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
