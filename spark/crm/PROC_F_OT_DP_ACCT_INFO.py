#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_OT_DP_ACCT_INFO').setMaster(sys.argv[2])
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

OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_CI_XDXT_ACCOUNT_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_ACCOUNT_INFO/*')
F_CI_XDXT_ACCOUNT_INFO.registerTempTable("F_CI_XDXT_ACCOUNT_INFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(''  AS BIGINT)                     AS ID 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,A.SERIALNO              AS SERIALNO 
       ,A.ACCOUNTKIND           AS ACCOUNTKIND 
       ,A.BANKNAME              AS BANKNAME 
       ,A.BANKACCOUNT           AS BANKACCOUNT 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,''                      AS UPDATEDATE 
       ,A.SBANK                 AS SBANK 
       ,A.ACCOUNTSTATE          AS ACCOUNTSTATE 
       ,CAST(A.ACCOUNTMONEY  AS DECIMAL(24))       AS ACCOUNTMONEY 
       ,CAST(A.ACCOUNTBLANCE  AS DECIMAL(24))       AS ACCOUNTBLANCE 
       ,A.UPTODATE              AS UPTODATE 
       ,CAST(A.BALANCE      AS DECIMAL(24))         AS BALANCE 
       ,A.CURRENCYTYPE          AS CURRENCYTYPE 
       ,A.OPENORGID             AS OPENORGID 
       ,CAST(A.DEPOSITNUMBER  AS DECIMAL(24))       AS DEPOSITNUMBER 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                  AS ODS_ST_DATE 
   FROM F_CI_XDXT_ACCOUNT_INFO A                               --客户账户信息存储表
  INNER JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.CUSTOMERID            = B.SOURCE_CUST_ID 
  WHERE A.CUSTOMERID IS NOT NULL 
    AND A.SERIALNO IS NOT NULL 
    AND B.ODS_SYS_ID            = 'LNA' 
    AND B.ODS_CUST_ID IS  NOT NULL 
    AND A.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_OT_DP_ACCT_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_OT_DP_ACCT_INFO_INNTMP1.registerTempTable("OCRM_F_OT_DP_ACCT_INFO_INNTMP1")

OCRM_F_OT_DP_ACCT_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_OT_DP_ACCT_INFO_BK/'+V_DT_LD+'.parquet/*')
OCRM_F_OT_DP_ACCT_INFO.registerTempTable("OCRM_F_OT_DP_ACCT_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.SERIALNO                                           --系统流水号:src.SERIALNO
       ,DST.ACCOUNTKIND                                        --账户编号:src.ACCOUNTKIND
       ,DST.BANKNAME                                           --开户行名称:src.BANKNAME
       ,DST.BANKACCOUNT                                        --开户账号:src.BANKACCOUNT
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTDATE                                          --登记日期:src.INPUTDATE
       ,DST.UPDATEDATE                                         --更新日期:src.UPDATEDATE
       ,DST.SBANK                                              --开户行:src.SBANK
       ,DST.ACCOUNTSTATE                                       --账户状态:src.ACCOUNTSTATE
       ,DST.ACCOUNTMONEY                                       --账户金额:src.ACCOUNTMONEY
       ,DST.ACCOUNTBLANCE                                      --账户资金（日均余额）:src.ACCOUNTBLANCE
       ,DST.UPTODATE                                           --截止统计日期:src.UPTODATE
       ,DST.BALANCE                                            --账户资金（余额）:src.BALANCE
       ,DST.CURRENCYTYPE                                       --账户币种:src.CURRENCYTYPE
       ,DST.OPENORGID                                          --管理机构:src.OPENORGID
       ,DST.DEPOSITNUMBER                                      --存款数量:src.DEPOSITNUMBER
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
   FROM OCRM_F_OT_DP_ACCT_INFO DST 
   LEFT JOIN OCRM_F_OT_DP_ACCT_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.SERIALNO            = DST.SERIALNO 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_OT_DP_ACCT_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_OT_DP_ACCT_INFO/"+V_DT+".parquet"
UNION=OCRM_F_OT_DP_ACCT_INFO_INNTMP2.unionAll(OCRM_F_OT_DP_ACCT_INFO_INNTMP1)
OCRM_F_OT_DP_ACCT_INFO_INNTMP1.cache()
OCRM_F_OT_DP_ACCT_INFO_INNTMP2.cache()
nrowsi = OCRM_F_OT_DP_ACCT_INFO_INNTMP1.count()
nrowsa = OCRM_F_OT_DP_ACCT_INFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_OT_DP_ACCT_INFO_INNTMP1.unpersist()
OCRM_F_OT_DP_ACCT_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_OT_DP_ACCT_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_OT_DP_ACCT_INFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_OT_DP_ACCT_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_OT_DP_ACCT_INFO_BK/")
