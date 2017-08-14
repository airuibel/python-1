#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CREDIT_REPORT_INFO_UPDATE').setMaster(sys.argv[2])
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

#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
#目标表：
#OCRM_F_CI_CREDIT_REPORT_INFO_TMP 临时表 全量表
#OCRM_F_CI_CREDIT_REPORT_INFO 全量表
OCRM_F_CI_CREDIT_REPORT_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CREDIT_REPORT_INFO/*')
OCRM_F_CI_CREDIT_REPORT_INFO.registerTempTable("OCRM_F_CI_CREDIT_REPORT_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,CUST_NAME               AS CUST_NAME 
       ,REGISTERED_ADDR         AS REGISTERED_ADDR 
       ,REGISTERED_TYPE         AS REGISTERED_TYPE 
       ,REGISTERED_NO           AS REGISTERED_NO 
       ,REGISTERED_DATE         AS REGISTERED_DATE 
       ,EXPIRATION_DATE         AS EXPIRATION_DATE 
       ,ORGANIZATION_CODE       AS ORGANIZATION_CODE 
       ,LOANCARD_CODE           AS LOANCARD_CODE 
       ,NATIONALTAX_NO          AS NATIONALTAX_NO 
       ,LANDTAX_NO              AS LANDTAX_NO 
       ,CERT_TYPE               AS CERT_TYPE 
       ,CERT_NO                 AS CERT_NO 
       ,QUERY_OPERATOR          AS QUERY_OPERATOR 
       ,QUERY_CAUSE             AS QUERY_CAUSE 
       ,CUST_ID                 AS CUST_ID 
       ,FR_ID                   AS FR_ID 
       ,UPDATE_DATE             AS UPDATE_DATE 
   FROM OCRM_F_CI_CREDIT_REPORT_INFO A                         --
  WHERE UPDATE_DATE > ADD_MONTHS(V_DT,-12) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CREDIT_REPORT_INFO_TMP = sqlContext.sql(sql)
OCRM_F_CI_CREDIT_REPORT_INFO_TMP.registerTempTable("OCRM_F_CI_CREDIT_REPORT_INFO_TMP")
dfn="OCRM_F_CI_CREDIT_REPORT_INFO_TMP/"+V_DT+".parquet"
OCRM_F_CI_CREDIT_REPORT_INFO_TMP.cache()
nrows = OCRM_F_CI_CREDIT_REPORT_INFO_TMP.count()
OCRM_F_CI_CREDIT_REPORT_INFO_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CREDIT_REPORT_INFO_TMP.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CREDIT_REPORT_INFO_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CREDIT_REPORT_INFO_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

OCRM_F_CI_CREDIT_REPORT_INFO_TMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CREDIT_REPORT_INFO_TMP/*')
OCRM_F_CI_CREDIT_REPORT_INFO_TMP.registerTempTable("OCRM_F_CI_CREDIT_REPORT_INFO_TMP")

sql = """
 SELECT ID                      AS ID 
       ,CUST_NAME               AS CUST_NAME 
       ,REGISTERED_ADDR         AS REGISTERED_ADDR 
       ,REGISTERED_TYPE         AS REGISTERED_TYPE 
       ,REGISTERED_NO           AS REGISTERED_NO 
       ,REGISTERED_DATE         AS REGISTERED_DATE 
       ,EXPIRATION_DATE         AS EXPIRATION_DATE 
       ,ORGANIZATION_CODE       AS ORGANIZATION_CODE 
       ,LOANCARD_CODE           AS LOANCARD_CODE 
       ,NATIONALTAX_NO          AS NATIONALTAX_NO 
       ,LANDTAX_NO              AS LANDTAX_NO 
       ,CERT_TYPE               AS CERT_TYPE 
       ,CERT_NO                 AS CERT_NO 
       ,QUERY_OPERATOR          AS QUERY_OPERATOR 
       ,QUERY_CAUSE             AS QUERY_CAUSE 
       ,CUST_ID                 AS CUST_ID 
       ,FR_ID                   AS FR_ID 
       ,UPDATE_DATE             AS UPDATE_DATE 
   FROM OCRM_F_CI_CREDIT_REPORT_INFO_TMP A                     --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CREDIT_REPORT_INFO = sqlContext.sql(sql)
OCRM_F_CI_CREDIT_REPORT_INFO.registerTempTable("OCRM_F_CI_CREDIT_REPORT_INFO")
dfn="OCRM_F_CI_CREDIT_REPORT_INFO/"+V_DT+".parquet"
OCRM_F_CI_CREDIT_REPORT_INFO.cache()
nrows = OCRM_F_CI_CREDIT_REPORT_INFO.count()
OCRM_F_CI_CREDIT_REPORT_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CREDIT_REPORT_INFO.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CREDIT_REPORT_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CREDIT_REPORT_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-03::
V_STEP = V_STEP + 1
OCRM_F_CI_CREDIT_REPORT_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CREDIT_REPORT_INFO/*')
OCRM_F_CI_CREDIT_REPORT_INFO.registerTempTable("OCRM_F_CI_CREDIT_REPORT_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.REGISTERED_ADDR       AS REGISTERED_ADDR 
       ,A.REGISTERED_TYPE       AS REGISTERED_TYPE 
       ,A.REGISTERED_NO         AS REGISTERED_NO 
       ,A.REGISTERED_DATE       AS REGISTERED_DATE 
       ,A.EXPIRATION_DATE       AS EXPIRATION_DATE 
       ,A.ORGANIZATION_CODE     AS ORGANIZATION_CODE 
       ,A.LOANCARD_CODE         AS LOANCARD_CODE 
       ,A.NATIONALTAX_NO        AS NATIONALTAX_NO 
       ,A.LANDTAX_NO            AS LANDTAX_NO 
       ,A.CERT_TYPE             AS CERT_TYPE 
       ,A.CERT_NO               AS CERT_NO 
       ,A.QUERY_OPERATOR        AS QUERY_OPERATOR 
       ,A.QUERY_CAUSE           AS QUERY_CAUSE 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.UPDATE_DATE           AS UPDATE_DATE 
   FROM OCRM_F_CI_CREDIT_REPORT_INFO A                         --
  INNER JOIN (SELECT  DISTINCT ODS_CUST_ID ,ODS_CUST_NAME,CERT_TYPE ,CERT_NO,FR_ID
   FROM   OCRM_F_CI_SYS_RESOURCE
   WHERE ODS_SYS_ID = 'CEN') B                          --
     ON A.CUST_NAME             = B.ODS_CUST_NAME 
    AND A.CERT_TYPE             = B.CERT_TYPE 
    AND A.CERT_NO               = B.CERT_NO 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.UPDATE_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP1.registerTempTable("OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP1")

sql = """
 SELECT DST.ID                                                  --信用报告ID:src.ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.REGISTERED_ADDR                                    --注册地址:src.REGISTERED_ADDR
       ,DST.REGISTERED_TYPE                                    --登记注册类型:src.REGISTERED_TYPE
       ,DST.REGISTERED_NO                                      --登记注册号:src.REGISTERED_NO
       ,DST.REGISTERED_DATE                                    --登记注册日期:src.REGISTERED_DATE
       ,DST.EXPIRATION_DATE                                    --有效截止日期:src.EXPIRATION_DATE
       ,DST.ORGANIZATION_CODE                                  --组织机构代码:src.ORGANIZATION_CODE
       ,DST.LOANCARD_CODE                                      --中征码:src.LOANCARD_CODE
       ,DST.NATIONALTAX_NO                                     --国税登记号:src.NATIONALTAX_NO
       ,DST.LANDTAX_NO                                         --地税登记号:src.LANDTAX_NO
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.QUERY_OPERATOR                                     --查询操作员:src.QUERY_OPERATOR
       ,DST.QUERY_CAUSE                                        --查询原因:src.QUERY_CAUSE
       ,DST.CUST_ID                                            --客户ID:src.CUST_ID
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.UPDATE_DATE                                        --更新日期:src.UPDATE_DATE
   FROM OCRM_F_CI_CREDIT_REPORT_INFO DST 
   LEFT JOIN OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP1 SRC 
     ON SRC.CUST_NAME           = DST.CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CREDIT_REPORT_INFO/"+V_DT+".parquet"
OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP2=OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP2.unionAll(OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP1)
OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP1.cache()
OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP2.count()
OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP1.unpersist()
OCRM_F_CI_CREDIT_REPORT_INFO_INNTMP2.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CREDIT_REPORT_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CREDIT_REPORT_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)

