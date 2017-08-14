#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_ZDH_ZZDH_SHOP').setMaster(sys.argv[2])
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

O_CI_ZDH_ZZDH_SHOP = sqlContext.read.parquet(hdfs+'/O_CI_ZDH_ZZDH_SHOP/*')
O_CI_ZDH_ZZDH_SHOP.registerTempTable("O_CI_ZDH_ZZDH_SHOP")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.SHOP_NO               AS SHOP_NO 
       ,A.SHOP_TYPE             AS SHOP_TYPE 
       ,A.SHOP_NAME             AS SHOP_NAME 
       ,A.ACQ_BANK              AS ACQ_BANK 
       ,A.CONTACTOR             AS CONTACTOR 
       ,A.TELEPHONE             AS TELEPHONE 
       ,A.ADDR                  AS ADDR 
       ,A.FEE                   AS FEE 
       ,A.COLLEC_LIMIT          AS COLLEC_LIMIT 
       ,A.COLLEC_TOTAL          AS COLLEC_TOTAL 
       ,A.OUT_LIMIT             AS OUT_LIMIT 
       ,A.OUT_TOTAL             AS OUT_TOTAL 
       ,A.OTHER_LIMIT           AS OTHER_LIMIT 
       ,A.OTHER_TOTAL           AS OTHER_TOTAL 
       ,A.FAX_NUM               AS FAX_NUM 
       ,A.SIGN_FLAG             AS SIGN_FLAG 
       ,A.SIGN_DATE             AS SIGN_DATE 
       ,A.UNSIGN_DATE           AS UNSIGN_DATE 
       ,A.DEPTNO                AS DEPTNO 
       ,A.CARD_KIND             AS CARD_KIND 
       ,A.ACQ_BANK_CODE         AS ACQ_BANK_CODE 
       ,A.PROCEDURE_FEE         AS PROCEDURE_FEE 
       ,A.BALANCE_DAY           AS BALANCE_DAY 
       ,A.PROCEDURE_DAY         AS PROCEDURE_DAY 
       ,A.SHOP_KIND             AS SHOP_KIND 
       ,A.LAWPER_NAME           AS LAWPER_NAME 
       ,A.LAWPER_CERTIFICATE    AS LAWPER_CERTIFICATE 
       ,A.REGISTER_CODE         AS REGISTER_CODE 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.FORM_CODE             AS FORM_CODE 
       ,A.TAX_CODE              AS TAX_CODE 
       ,A.ACQ_BANK_NAME         AS ACQ_BANK_NAME 
       ,A.IS_BLACK              AS IS_BLACK 
       ,A.ACQ_NAME              AS ACQ_NAME 
       ,A.PAN_TYPE              AS PAN_TYPE 
       ,A.ACQ_PLACE             AS ACQ_PLACE 
       ,A.ACQ_CITY              AS ACQ_CITY 
       ,A.ACQ_DEAFBANK_NAME     AS ACQ_DEAFBANK_NAME 
       ,A.ACQ_MANMOBILE         AS ACQ_MANMOBILE 
       ,A.ADD_AMOUNT            AS ADD_AMOUNT 
       ,A.ACCT_NO               AS ACCT_NO 
       ,A.ACCT_CHK_FLAG         AS ACCT_CHK_FLAG 
       ,A.EXPLOIT_NO            AS EXPLOIT_NO 
       ,A.EXPLOIT_NAME          AS EXPLOIT_NAME 
       ,A.DEPTDETAIL            AS DEPTDETAIL 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'ZDH'                   AS ODS_SYS_ID 
   FROM O_CI_ZDH_ZZDH_SHOP A                                   --金融进村2期商户信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_ZDH_ZZDH_SHOP = sqlContext.sql(sql)
F_CI_ZDH_ZZDH_SHOP.registerTempTable("F_CI_ZDH_ZZDH_SHOP")
dfn="F_CI_ZDH_ZZDH_SHOP/"+V_DT+".parquet"
F_CI_ZDH_ZZDH_SHOP.cache()
nrows = F_CI_ZDH_ZZDH_SHOP.count()
F_CI_ZDH_ZZDH_SHOP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_ZDH_ZZDH_SHOP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_ZDH_ZZDH_SHOP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_ZDH_ZZDH_SHOP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
