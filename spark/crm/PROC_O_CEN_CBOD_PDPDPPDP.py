#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_PDPDPPDP').setMaster(sys.argv[2])
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

O_CI_CBOD_PDPDPPDP = sqlContext.read.parquet(hdfs+'/O_CI_CBOD_PDPDPPDP/*')
O_CI_CBOD_PDPDPPDP.registerTempTable("O_CI_CBOD_PDPDPPDP")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,PD_CODE                 AS PD_CODE 
       ,PD_DES                  AS PD_DES 
       ,PD_SHORT_NAME           AS PD_SHORT_NAME 
       ,PD_START_DT             AS PD_START_DT 
       ,PD_EXP_DT               AS PD_EXP_DT 
       ,PD_STATUS               AS PD_STATUS 
       ,PD_USE_CTL_FLG          AS PD_USE_CTL_FLG 
       ,PD_1LVL_BRH_ID          AS PD_1LVL_BRH_ID 
       ,PD_OWNER                AS PD_OWNER 
       ,PD_CREATER              AS PD_CREATER 
       ,PD_CRTR_DT              AS PD_CRTR_DT 
       ,PD_CRTR_TM              AS PD_CRTR_TM 
       ,PD_APPROVER             AS PD_APPROVER 
       ,PD_APRV_DT              AS PD_APRV_DT 
       ,PD_APRV_TM              AS PD_APRV_TM 
       ,PD_LINK_PRT1            AS PD_LINK_PRT1 
       ,PD_LINK_PRT2            AS PD_LINK_PRT2 
       ,PD_LINK_PRT3            AS PD_LINK_PRT3 
       ,PD_LINK_PRT4            AS PD_LINK_PRT4 
       ,PD_LINK_PRT5            AS PD_LINK_PRT5 
       ,PD_LINK_PRT6            AS PD_LINK_PRT6 
       ,PD_LINK_PRT7            AS PD_LINK_PRT7 
       ,PD_LINK_PRT8            AS PD_LINK_PRT8 
       ,PD_LINK_PRT9            AS PD_LINK_PRT9 
       ,PD_LINK_PRT10           AS PD_LINK_PRT10 
       ,PD_LINK_PRT11           AS PD_LINK_PRT11 
       ,PD_LINK_PRT12           AS PD_LINK_PRT12 
       ,PD_LINK_PRT13           AS PD_LINK_PRT13 
       ,PD_LINK_PRT14           AS PD_LINK_PRT14 
       ,PD_LINK_PRT15           AS PD_LINK_PRT15 
       ,PD_LINK_PRT16           AS PD_LINK_PRT16 
       ,PD_CHAN_CTRL            AS PD_CHAN_CTRL 
       ,PD_LAST_MNTN_OPR_NO     AS PD_LAST_MNTN_OPR_NO 
       ,PD_VIP_LEVEL            AS PD_VIP_LEVEL 
       ,PD_RMRK                 AS PD_RMRK 
       ,PD_PDPKG_ATR            AS PD_PDPKG_ATR 
       ,PD_BSC_CODE             AS PD_BSC_CODE 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CI_CBOD_PDPDPPDP A                                   --产品数据库
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_PDPDPPDP = sqlContext.sql(sql)
F_CI_CBOD_PDPDPPDP.registerTempTable("F_CI_CBOD_PDPDPPDP")
dfn="F_CI_CBOD_PDPDPPDP/"+V_DT+".parquet"
F_CI_CBOD_PDPDPPDP.cache()
nrows = F_CI_CBOD_PDPDPPDP.count()
F_CI_CBOD_PDPDPPDP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_CBOD_PDPDPPDP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_PDPDPPDP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CBOD_PDPDPPDP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
