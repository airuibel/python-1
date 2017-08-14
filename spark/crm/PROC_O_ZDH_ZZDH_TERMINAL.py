#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_ZDH_ZZDH_TERMINAL').setMaster(sys.argv[2])
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

O_NI_ZDH_ZZDH_TERMINAL = sqlContext.read.parquet(hdfs+'/O_NI_ZDH_ZZDH_TERMINAL/*')
O_NI_ZDH_ZZDH_TERMINAL.registerTempTable("O_NI_ZDH_ZZDH_TERMINAL")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.SHOP_NO               AS SHOP_NO 
       ,A.POS_ID                AS POS_ID 
       ,A.PSAM_NO               AS PSAM_NO 
       ,A.TELEPHONE             AS TELEPHONE 
       ,A.TERM_MODULE           AS TERM_MODULE 
       ,A.PSAM_MODULE           AS PSAM_MODULE 
       ,A.APP_TYPE              AS APP_TYPE 
       ,A.DESCRIBE              AS DESCRIBE 
       ,A.COLLEC_LIMIT          AS COLLEC_LIMIT 
       ,A.COLLEC_TOTAL          AS COLLEC_TOTAL 
       ,A.OUT_LIMIT             AS OUT_LIMIT 
       ,A.OUT_TOTAL             AS OUT_TOTAL 
       ,A.OTHER_LIMIT           AS OTHER_LIMIT 
       ,A.OTHER_TOTAL           AS OTHER_TOTAL 
       ,A.POS_TYPE              AS POS_TYPE 
       ,A.ADDRESS               AS ADDRESS 
       ,A.PUT_DATE              AS PUT_DATE 
       ,A.CUR_TRACE             AS CUR_TRACE 
       ,A.IP                    AS IP 
       ,A.DOWN_MENU             AS DOWN_MENU 
       ,A.DOWN_TERM             AS DOWN_TERM 
       ,A.DOWN_PSAM             AS DOWN_PSAM 
       ,A.DOWN_PRINT            AS DOWN_PRINT 
       ,A.DOWN_OPERATE          AS DOWN_OPERATE 
       ,A.DOWN_FUNCTION         AS DOWN_FUNCTION 
       ,A.DOWN_ERROR            AS DOWN_ERROR 
       ,A.DOWN_ALL              AS DOWN_ALL 
       ,A.DOWN_PAYLIST          AS DOWN_PAYLIST 
       ,A.MENU_RECNO            AS MENU_RECNO 
       ,A.PRINT_RECNO           AS PRINT_RECNO 
       ,A.OPERATE_RECNO         AS OPERATE_RECNO 
       ,A.FUNCTION_RECNO        AS FUNCTION_RECNO 
       ,A.ERROR_RECNO           AS ERROR_RECNO 
       ,A.ALL_TRANSTYPE         AS ALL_TRANSTYPE 
       ,A.TERM_BITMAP           AS TERM_BITMAP 
       ,A.PSAM_BITMAP           AS PSAM_BITMAP 
       ,A.PRINT_BITMAP          AS PRINT_BITMAP 
       ,A.OPERATE_BITMAP        AS OPERATE_BITMAP 
       ,A.FUNCTION_BITMAP       AS FUNCTION_BITMAP 
       ,A.ERROR_BITMAP          AS ERROR_BITMAP 
       ,A.MSG_RECNUM            AS MSG_RECNUM 
       ,A.MSG_RECNO             AS MSG_RECNO 
       ,A.FIRST_PAGE            AS FIRST_PAGE 
       ,A.T_STATUS              AS T_STATUS 
       ,A.CUR_BATCH             AS CUR_BATCH 
       ,A.KEY_INDEX             AS KEY_INDEX 
       ,A.SHOP_NO_YL            AS SHOP_NO_YL 
       ,A.POS_ID_YL             AS POS_ID_YL 
       ,A.ADDRESS_YL            AS ADDRESS_YL 
       ,A.POS_TYPE_YL           AS POS_TYPE_YL 
       ,A.LOGIN_STAT            AS LOGIN_STAT 
       ,A.LOGIN_STAT_YL         AS LOGIN_STAT_YL 
       ,A.TYPE_YL_STAT          AS TYPE_YL_STAT 
       ,A.YL_TRANS_TYPE         AS YL_TRANS_TYPE 
       ,A.SHOP_CLASS            AS SHOP_CLASS 
       ,A.DATA_TYPE             AS DATA_TYPE 
       ,A.IS_EXPORT             AS IS_EXPORT 
       ,A.OTHER_CARD            AS OTHER_CARD 
       ,A.FIELD1                AS FIELD1 
       ,A.FIELD2                AS FIELD2 
       ,A.FIELD3                AS FIELD3 
       ,A.FIELD4                AS FIELD4 
       ,A.FIELD5                AS FIELD5 
       ,A.FIELD6                AS FIELD6 
       ,A.FIELD7                AS FIELD7 
       ,A.FIELD8                AS FIELD8 
       ,A.FIELD9                AS FIELD9 
       ,A.FIELD10               AS FIELD10 
       ,A.OLD_FLAG              AS OLD_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'ZDH'                   AS ODS_SYS_ID 
   FROM O_NI_ZDH_ZZDH_TERMINAL A                               --终端表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_NI_ZDH_ZZDH_TERMINAL = sqlContext.sql(sql)
F_NI_ZDH_ZZDH_TERMINAL.registerTempTable("F_NI_ZDH_ZZDH_TERMINAL")
dfn="F_NI_ZDH_ZZDH_TERMINAL/"+V_DT+".parquet"
F_NI_ZDH_ZZDH_TERMINAL.cache()
nrows = F_NI_ZDH_ZZDH_TERMINAL.count()
F_NI_ZDH_ZZDH_TERMINAL.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_NI_ZDH_ZZDH_TERMINAL.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_NI_ZDH_ZZDH_TERMINAL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
