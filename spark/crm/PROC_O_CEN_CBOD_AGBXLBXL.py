#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_AGBXLBXL').setMaster(sys.argv[2])
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

#清除数据，支持重跑
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_NI_CBOD_AGBXLBXL/"+V_DT+".parquet")

O_NI_CBOD_AGBXLBXL = sqlContext.read.parquet(hdfs+'/O_NI_CBOD_AGBXLBXL/*')
O_NI_CBOD_AGBXLBXL.registerTempTable("O_NI_CBOD_AGBXLBXL")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,AG_ENT_OPUN_COD         AS AG_ENT_OPUN_COD 
       ,AG_BIZE_CODE            AS AG_BIZE_CODE 
       ,AG_BUSN_DT              AS AG_BUSN_DT 
       ,AG_HOST_LOGNO           AS AG_HOST_LOGNO 
       ,AG_PRE_BUSN_DT          AS AG_PRE_BUSN_DT 
       ,AG_PRE_LOGNO            AS AG_PRE_LOGNO 
       ,AG_EC_BUSN_DT           AS AG_EC_BUSN_DT 
       ,AG_EC_HOST_LOGNO        AS AG_EC_HOST_LOGNO 
       ,AG_TX_ID                AS AG_TX_ID 
       ,AG_TX_MODE              AS AG_TX_MODE 
       ,AG_TX_TYP               AS AG_TX_TYP 
       ,AG_TELLER_NO            AS AG_TELLER_NO 
       ,AG_ACC_TYP              AS AG_ACC_TYP 
       ,AG_CHANNEL_FLG          AS AG_CHANNEL_FLG 
       ,AG_TRIN_BANKNO          AS AG_TRIN_BANKNO 
       ,AG_TRIN_ACCNO           AS AG_TRIN_ACCNO 
       ,AG_TROT_BANKNO          AS AG_TROT_BANKNO 
       ,AG_TROT_ACCNO           AS AG_TROT_ACCNO 
       ,AG_COLLC_TYPE           AS AG_COLLC_TYPE 
       ,AG_CORR_BANK            AS AG_CORR_BANK 
       ,AG_TRANSITION_FLG       AS AG_TRANSITION_FLG 
       ,AG_CURR_IDEN            AS AG_CURR_IDEN 
       ,AG_CURR_COD             AS AG_CURR_COD 
       ,AG_AMT                  AS AG_AMT 
       ,AG_ACT_SETL_AMT         AS AG_ACT_SETL_AMT 
       ,AG_TOTL_AMT             AS AG_TOTL_AMT 
       ,AG_PZH_SORT             AS AG_PZH_SORT 
       ,AG_DOC_NO               AS AG_DOC_NO 
       ,AG_RTN_MSG              AS AG_RTN_MSG 
       ,AG_BATCH_NO             AS AG_BATCH_NO 
       ,AG_RMRK1                AS AG_RMRK1 
       ,AG_RMRK2                AS AG_RMRK2 
       ,AG_RMRK3                AS AG_RMRK3 
       ,AG_MUTI_AREA1           AS AG_MUTI_AREA1 
       ,AG_MUTI_AREA2           AS AG_MUTI_AREA2 
       ,AG_DB_PART_ID           AS AG_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_NI_CBOD_AGBXLBXL A                                   --中间业务流水档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_NI_CBOD_AGBXLBXL = sqlContext.sql(sql)
F_NI_CBOD_AGBXLBXL.registerTempTable("F_NI_CBOD_AGBXLBXL")
dfn="F_NI_CBOD_AGBXLBXL/"+V_DT+".parquet"
F_NI_CBOD_AGBXLBXL.cache()
nrows = F_NI_CBOD_AGBXLBXL.count()
F_NI_CBOD_AGBXLBXL.write.save(path=hdfs + '/' + dfn, mode='append')
F_NI_CBOD_AGBXLBXL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_NI_CBOD_AGBXLBXL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
