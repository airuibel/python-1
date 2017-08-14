#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CUST_ASSIGN_PRE').setMaster(sys.argv[2])
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

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
ACRM_F_AG_AGREEMENT = sqlContext.read.parquet(hdfs+'/ACRM_F_AG_AGREEMENT/*')
ACRM_F_AG_AGREEMENT.registerTempTable("ACRM_F_AG_AGREEMENT")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
ADMIN_AUTH_ACCOUNT_COMPARE = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ACCOUNT_COMPARE/*')
ADMIN_AUTH_ACCOUNT_COMPARE.registerTempTable("ADMIN_AUTH_ACCOUNT_COMPARE")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS ODS_CUSTOM_ID 
       ,A.CUST_ZH_NAME          AS CUST_NAME 
       ,A.CERT_TYPE             AS CERT_TYP 
       ,A.CERT_NUM              AS CERT_NO 
       ,A.CUST_STAT             AS CUST_STS 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN ACRM_F_AG_AGREEMENT C                             --客户协议表
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
   LEFT JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.CRM_DT                = V_DT 
    AND A.FR_ID IN('061', '103') 
    AND B.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
CUSTOMER_BELONG_TMP2 = sqlContext.sql(sql)
CUSTOMER_BELONG_TMP2.registerTempTable("CUSTOMER_BELONG_TMP2")
dfn="CUSTOMER_BELONG_TMP2/"+V_DT+".parquet"
CUSTOMER_BELONG_TMP2.cache()
nrows = CUSTOMER_BELONG_TMP2.count()
CUSTOMER_BELONG_TMP2.write.save(path=hdfs + '/' + dfn, mode='overwrite')
CUSTOMER_BELONG_TMP2.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/CUSTOMER_BELONG_TMP2/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert CUSTOMER_BELONG_TMP2 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS ODS_CUSTOM_ID 
       ,A.CUST_ZH_NAME          AS CUST_NAME 
       ,A.CERT_TYPE             AS CERT_TYP 
       ,A.CERT_NUM              AS CERT_NO 
       ,A.CUST_STAT             AS CUST_STS 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN ACRM_F_AG_AGREEMENT C                             --客户协议表
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
   LEFT JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  WHERE A.FR_ID 
    NOT IN('061', '103') 
    AND B.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
CUSTOMER_BELONG_TMP2 = sqlContext.sql(sql)
CUSTOMER_BELONG_TMP2.registerTempTable("CUSTOMER_BELONG_TMP2")
dfn="CUSTOMER_BELONG_TMP2/"+V_DT+".parquet"
CUSTOMER_BELONG_TMP2.cache()
nrows = CUSTOMER_BELONG_TMP2.count()
CUSTOMER_BELONG_TMP2.write.save(path=hdfs + '/' + dfn, mode='append')
CUSTOMER_BELONG_TMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert CUSTOMER_BELONG_TMP2 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS ODS_CUSTOM_ID 
       ,A.CUST_ZH_NAME          AS CUST_NAME 
       ,A.CERT_TYPE             AS CERT_TYP 
       ,A.CERT_NUM              AS CERT_NO 
       ,A.CUST_STAT             AS CUST_STS 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN ACRM_F_AG_AGREEMENT C                             --客户协议表
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.START_DATE            = V_DT 
  WHERE A.FR_ID 
    NOT IN('061', '103') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
CUSTOMER_BELONG_TMP2 = sqlContext.sql(sql)
CUSTOMER_BELONG_TMP2.registerTempTable("CUSTOMER_BELONG_TMP2")
dfn="CUSTOMER_BELONG_TMP2/"+V_DT+".parquet"
CUSTOMER_BELONG_TMP2.cache()
nrows = CUSTOMER_BELONG_TMP2.count()
CUSTOMER_BELONG_TMP2.write.save(path=hdfs + '/' + dfn, mode='append')
CUSTOMER_BELONG_TMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert CUSTOMER_BELONG_TMP2 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS ODS_CUSTOM_ID 
       ,A.CUST_ZH_NAME          AS CUST_NAME 
       ,A.CERT_TYPE             AS CERT_TYP 
       ,A.CERT_NUM              AS CERT_NO 
       ,A.CUST_STAT             AS CUST_STS 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
    AND B.EFF_DATE <= V_DT 
  WHERE A.FR_ID 
    NOT IN('061', '103') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
CUSTOMER_BELONG_TMP2 = sqlContext.sql(sql)
CUSTOMER_BELONG_TMP2.registerTempTable("CUSTOMER_BELONG_TMP2")
dfn="CUSTOMER_BELONG_TMP2/"+V_DT+".parquet"
CUSTOMER_BELONG_TMP2.cache()
nrows = CUSTOMER_BELONG_TMP2.count()
CUSTOMER_BELONG_TMP2.write.save(path=hdfs + '/' + dfn, mode='append')
CUSTOMER_BELONG_TMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert CUSTOMER_BELONG_TMP2 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS ODS_CUSTOM_ID 
       ,A.CUST_ZH_NAME          AS CUST_NAME 
       ,A.CERT_TYPE             AS CERT_TYP 
       ,A.CERT_NUM              AS CERT_NO 
       ,A.CUST_STAT             AS CUST_STS 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --归属经理表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
    AND B.EFF_DATE <= V_DT 
  WHERE A.FR_ID 
    NOT IN('061', '103') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
CUSTOMER_BELONG_TMP2 = sqlContext.sql(sql)
CUSTOMER_BELONG_TMP2.registerTempTable("CUSTOMER_BELONG_TMP2")
dfn="CUSTOMER_BELONG_TMP2/"+V_DT+".parquet"
CUSTOMER_BELONG_TMP2.cache()
nrows = CUSTOMER_BELONG_TMP2.count()
CUSTOMER_BELONG_TMP2.write.save(path=hdfs + '/' + dfn, mode='append')
CUSTOMER_BELONG_TMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert CUSTOMER_BELONG_TMP2 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-06::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS ODS_CUSTOM_ID 
       ,A.CUST_ZH_NAME          AS CUST_NAME 
       ,A.CERT_TYPE             AS CERT_TYP 
       ,A.CERT_NUM              AS CERT_NO 
       ,A.CUST_STAT             AS CUST_STS 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO C                      --资产协议表
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND(C.SUBJECTNO LIKE '1301%' 
             OR C.SUBJECTNO LIKE '1302%' 
             OR C.SUBJECTNO LIKE '1303%' 
             OR C.SUBJECTNO LIKE '1304%' 
             OR C.SUBJECTNO LIKE '1305%' 
             OR C.SUBJECTNO LIKE '1306%' 
             OR C.SUBJECTNO LIKE '1307%' 
             OR C.SUBJECTNO LIKE '1308%') 
    AND C.BAL > 0 
    AND C.LN_APCL_FLG           = 'N' 
  INNER JOIN ADMIN_AUTH_ACCOUNT_COMPARE D                      --源系统操作号对照表
     ON C.FR_ID                 = D.FR_ID 
    AND D.SOURCE_SYS_USER_ID    = C.MANAGE_USERID 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR E                        --归属经理表
     ON A.CUST_ID               = E.CUST_ID 
    AND A.FR_ID                 = E.FR_ID 
    AND E.MAIN_TYPE             = '1' 
  WHERE A.FR_ID 
    NOT IN('061', '103') 
    AND E.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
CUSTOMER_BELONG_TMP2 = sqlContext.sql(sql)
CUSTOMER_BELONG_TMP2.registerTempTable("CUSTOMER_BELONG_TMP2")
dfn="CUSTOMER_BELONG_TMP2/"+V_DT+".parquet"
CUSTOMER_BELONG_TMP2.cache()
nrows = CUSTOMER_BELONG_TMP2.count()
CUSTOMER_BELONG_TMP2.write.save(path=hdfs + '/' + dfn, mode='append')
CUSTOMER_BELONG_TMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert CUSTOMER_BELONG_TMP2 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-07::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS ODS_CUSTOM_ID 
       ,A.CUST_ZH_NAME          AS CUST_NAME 
       ,A.CERT_TYPE             AS CERT_TYP 
       ,A.CERT_NUM              AS CERT_NO 
       ,A.CUST_STAT             AS CUST_STS 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
  INNER JOIN ACRM_F_DP_SAVE_INFO C                             --资产协议表
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.ACCT_STATUS           = '01' 
  INNER JOIN ADMIN_AUTH_ACCOUNT_COMPARE D                      --源系统操作号对照表
     ON C.FR_ID                 = D.FR_ID 
    AND D.SOURCE_SYS_USER_ID    = C.CONNTR_NO 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR E                        --归属经理表
     ON A.CUST_ID               = E.CUST_ID 
    AND A.FR_ID                 = E.FR_ID 
    AND E.MAIN_TYPE             = '1' 
  WHERE A.FR_ID 
    NOT IN('061', '103') 
    AND E.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
CUSTOMER_BELONG_TMP2 = sqlContext.sql(sql)
CUSTOMER_BELONG_TMP2.registerTempTable("CUSTOMER_BELONG_TMP2")
dfn="CUSTOMER_BELONG_TMP2/"+V_DT+".parquet"
CUSTOMER_BELONG_TMP2.cache()
nrows = CUSTOMER_BELONG_TMP2.count()
CUSTOMER_BELONG_TMP2.write.save(path=hdfs + '/' + dfn, mode='append')
CUSTOMER_BELONG_TMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert CUSTOMER_BELONG_TMP2 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-08::
V_STEP = V_STEP + 1

sql = """
 SELECT ODS_CUSTOM_ID           AS ODS_CUSTOM_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,CERT_TYP                AS CERT_TYP 
       ,CERT_NO                 AS CERT_NO 
       ,CUST_STS                AS CUST_STS 
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
   FROM CUSTOMER_BELONG_TMP2 A                                 --待分配客户临时表2
  GROUP BY ODS_CUSTOM_ID 
       ,CUST_NAME 
       ,CERT_TYP 
       ,CERT_NO 
       ,CUST_STS 
       ,CUST_TYP 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
CUSTOMER_BELONG_TMP = sqlContext.sql(sql)
CUSTOMER_BELONG_TMP.registerTempTable("CUSTOMER_BELONG_TMP")
dfn="CUSTOMER_BELONG_TMP/"+V_DT+".parquet"
CUSTOMER_BELONG_TMP.cache()
nrows = CUSTOMER_BELONG_TMP.count()
CUSTOMER_BELONG_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
CUSTOMER_BELONG_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/CUSTOMER_BELONG_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert CUSTOMER_BELONG_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
