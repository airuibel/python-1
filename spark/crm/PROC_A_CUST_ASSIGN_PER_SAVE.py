#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CUST_ASSIGN_PER_SAVE').setMaster(sys.argv[2])
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

OCRM_CUST_AUM_VALUE1 = sqlContext.read.parquet(hdfs+'/OCRM_CUST_AUM_VALUE1/*')
OCRM_CUST_AUM_VALUE1.registerTempTable("OCRM_CUST_AUM_VALUE1")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
CUSTOMER_BELONG_TMP = sqlContext.read.parquet(hdfs+'/CUSTOMER_BELONG_TMP/*')
CUSTOMER_BELONG_TMP.registerTempTable("CUSTOMER_BELONG_TMP")
OCRM_CUST_LOAN_AUM = sqlContext.read.parquet(hdfs+'/OCRM_CUST_LOAN_AUM/*')
OCRM_CUST_LOAN_AUM.registerTempTable("OCRM_CUST_LOAN_AUM")
OCRM_CUST_AUM_VALUE = sqlContext.read.parquet(hdfs+'/OCRM_CUST_AUM_VALUE/*')
OCRM_CUST_AUM_VALUE.registerTempTable("OCRM_CUST_AUM_VALUE")
TEMP_A = sqlContext.read.parquet(hdfs+'/TEMP_A/*')
TEMP_A.registerTempTable("TEMP_A")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,''                    AS ORG_ID 
       ,'1'                     AS ORG_TYP 
       ,''                    AS CUST_MNG 
       ,'1'                     AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM CUSTOMER_BELONG_TMP A                                  --待分配客户临时表
   LEFT JOIN TEMP_A B                                          --对私分配中间表A
     ON A.ODS_CUSTOM_ID         = B.ODS_CUSTOM_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.CUST_STS              = '1' 
    AND A.CUST_TYP              = '1' 
    AND B.FR_ID IS NULL 
    AND A.FR_ID <> '095' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_B = sqlContext.sql(sql)
TEMP_B.registerTempTable("TEMP_B")
dfn="TEMP_B/"+V_DT+".parquet"
TEMP_B.cache()
nrows = TEMP_B.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_B/*.parquet")
TEMP_B.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TEMP_B.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_B lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,B.ORG_ID                AS ORG_ID 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.CUST_MNG              AS CUST_MNG 
       ,A.MNG_TYP               AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_B A                                               --对私分配中间表B
  INNER JOIN OCRM_CUST_AUM_VALUE1 B                            --客户-经理存款AUM
     ON A.ODS_CUSTOM_ID         = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.RANK                  = '1' 
  WHERE A.FR_ID <> '095' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_B_INNTMP1 = sqlContext.sql(sql)
TEMP_B_INNTMP1.registerTempTable("TEMP_B_INNTMP1")

TEMP_B = sqlContext.read.parquet(hdfs+'/TEMP_B/*')
TEMP_B.registerTempTable("TEMP_B")
sql = """
 SELECT DST.ODS_CUSTOM_ID                                       --客户号:src.ODS_CUSTOM_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_TYP                                            --机构主协办:src.ORG_TYP
       ,DST.CUST_MNG                                           --客户经理:src.CUST_MNG
       ,DST.MNG_TYP                                            --经理主协办:src.MNG_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM TEMP_B DST 
   LEFT JOIN TEMP_B_INNTMP1 SRC 
     ON SRC.ODS_CUSTOM_ID       = DST.ODS_CUSTOM_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.ODS_CUSTOM_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_B_INNTMP2 = sqlContext.sql(sql)
dfn="TEMP_B/"+V_DT+".parquet"
TEMP_B_INNTMP2=TEMP_B_INNTMP2.unionAll(TEMP_B_INNTMP1)
TEMP_B_INNTMP1.cache()
TEMP_B_INNTMP2.cache()
nrowsi = TEMP_B_INNTMP1.count()
nrowsa = TEMP_B_INNTMP2.count()
TEMP_B_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
TEMP_B_INNTMP1.unpersist()
TEMP_B_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_B lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/TEMP_B/"+V_DT_LD+".parquet /"+dbname+"/TEMP_B_BK/")

#任务[11] 001-03::
V_STEP = V_STEP + 1

TEMP_B = sqlContext.read.parquet(hdfs+'/TEMP_B/*')
TEMP_B.registerTempTable("TEMP_B")

sql = """
 SELECT DISTINCT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,B.ORG_ID                AS ORG_ID 
       ,CASE WHEN B.ORG_ID                = A.ORG_ID THEN '1' ELSE '2' END                     AS ORG_TYP 
       ,CASE WHEN A.CUST_MNG IS 
    NOT NULL 
    AND TRIM(A.CUST_MNG) <> '' THEN B.CONNTR_NO ELSE NULL END                     AS CUST_MNG 
       ,'2'                     AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_B A                                               --对私分配中间表B
  INNER JOIN ACRM_F_DP_SAVE_INFO B                             --负债协议
     ON A.ODS_CUSTOM_ID         = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.BAL_RMB > 0 
    AND B.ACCT_STATUS           = '01' 
  WHERE A.ORG_ID IS 
    NOT NULL 
    AND A.CUST_MNG IS 
    NOT NULL 
    AND A.ORG_TYP               = '1' 
    AND A.MNG_TYP               = '1' 
    AND A.FR_ID <> '095'  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_B = sqlContext.sql(sql)
TEMP_B.registerTempTable("TEMP_B")
dfn="TEMP_B/"+V_DT+".parquet"
TEMP_B.cache()
nrows = TEMP_B.count()
TEMP_B.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_B.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_B lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-04::
V_STEP = V_STEP + 1

TEMP_B = sqlContext.read.parquet(hdfs+'/TEMP_B/*')
TEMP_B.registerTempTable("TEMP_B")

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,B.ORG_ID                AS ORG_ID 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.CUST_MNG              AS CUST_MNG 
       ,'1'                     AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_B A                                               --对私分配中间表B
  INNER JOIN OCRM_CUST_AUM_VALUE B                             --客户-机构存款AUM
     ON A.ODS_CUSTOM_ID         = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.RANK                  = '1' 
  WHERE A.FR_ID <> '095' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_B_INNTMP1 = sqlContext.sql(sql)
TEMP_B_INNTMP1.registerTempTable("TEMP_B_INNTMP1")

TEMP_B = sqlContext.read.parquet(hdfs+'/TEMP_B/*')
TEMP_B.registerTempTable("TEMP_B")
sql = """
 SELECT DST.ODS_CUSTOM_ID                                       --客户号:src.ODS_CUSTOM_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_TYP                                            --机构主协办:src.ORG_TYP
       ,DST.CUST_MNG                                           --客户经理:src.CUST_MNG
       ,DST.MNG_TYP                                            --经理主协办:src.MNG_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM TEMP_B DST 
   LEFT JOIN TEMP_B_INNTMP1 SRC 
     ON SRC.ODS_CUSTOM_ID       = DST.ODS_CUSTOM_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_MNG            = DST.CUST_MNG 
    AND SRC.MNG_TYP             = DST.MNG_TYP 
  WHERE SRC.ODS_CUSTOM_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_B_INNTMP2 = sqlContext.sql(sql)
dfn="TEMP_B/"+V_DT+".parquet"
TEMP_B_INNTMP2=TEMP_B_INNTMP2.unionAll(TEMP_B_INNTMP1)
TEMP_B_INNTMP1.cache()
TEMP_B_INNTMP2.cache()
nrowsi = TEMP_B_INNTMP1.count()
nrowsa = TEMP_B_INNTMP2.count()
TEMP_B_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
TEMP_B_INNTMP1.unpersist()
TEMP_B_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_B lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/TEMP_B/"+V_DT_LD+".parquet /"+dbname+"/TEMP_B_BK/")

#任务[11] 001-05::
V_STEP = V_STEP + 1

TEMP_B = sqlContext.read.parquet(hdfs+'/TEMP_B/*')
TEMP_B.registerTempTable("TEMP_B")

sql = """
 SELECT DISTINCT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,B.ORG_ID                AS ORG_ID 
       ,'2' AS ORG_TYP
		,A.CUST_MNG AS CUST_MNG
		,A.MNG_TYP AS MNG_TYP
		,A.FR_ID AS FR_ID 
from TEMP_B A    --对私分配中间表B
		INNER JOIN
		OCRM_CUST_AUM_VALUE B    --客户-机构存款AUM
		on A.ODS_CUSTOM_ID = B.CUST_ID AND A.FR_ID = B.FR_ID AND A.ORG_ID <> B.ORG_ID  
where A.ORG_ID IS NOT NULL AND A.CUST_MNG IS NULL AND A.ORG_TYP = ' 1 ' AND A.MNG_TYP = '1' AND A.FR_ID <>'095' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_B = sqlContext.sql(sql)
TEMP_B.registerTempTable("TEMP_B")
dfn="TEMP_B/"+V_DT+".parquet"
TEMP_B.cache()
nrows = TEMP_B.count()
TEMP_B.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_B.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_B lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-06::
V_STEP = V_STEP + 1

TEMP_B = sqlContext.read.parquet(hdfs+'/TEMP_B/*')
TEMP_B.registerTempTable("TEMP_B")

sql = """
 SELECT ODS_CUSTOM_ID           AS ODS_CUSTOM_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,CERT_TYP                AS CERT_TYP 
       ,CERT_NO                 AS CERT_NO 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_TYP                 AS ORG_TYP 
       ,CUST_MNG                AS CUST_MNG 
       ,MNG_TYP                 AS MNG_TYP 
       ,FR_ID                   AS FR_ID 
   FROM TEMP_B A                                               --对私分配中间表B
  WHERE A.ORG_ID IS 
    NOT NULL 
     OR A.CUST_MNG IS 
    NOT NULL 
    AND A.FR_ID <> '095' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_TEMP_B_01 = sqlContext.sql(sql)
TMP_TEMP_B_01.registerTempTable("TMP_TEMP_B_01")
dfn="TMP_TEMP_B_01/"+V_DT+".parquet"
TMP_TEMP_B_01.cache()
nrows = TMP_TEMP_B_01.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_TEMP_B_01/*.parquet")
TMP_TEMP_B_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_TEMP_B_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_TEMP_B_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-07::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUSTOM_ID           AS ODS_CUSTOM_ID 
       ,A.CUST_NAME               AS CUST_NAME 
       ,A.CERT_TYP                AS CERT_TYP 
       ,A.CERT_NO                 AS CERT_NO 
       ,B.ORG_ID                  AS ORG_ID 
       ,A.ORG_TYP                 AS ORG_TYP 
       ,A.CUST_MNG                AS CUST_MNG 
       ,A.MNG_TYP                 AS MNG_TYP 
       ,A.FR_ID                   AS FR_ID 
   FROM TEMP_B A                                               --对私分配中间表B
  INNER JOIN(
         SELECT DISTINCT ORG_ID 
               ,FR_ID 
           FROM ADMIN_AUTH_ORG 
          WHERE UP_ORG_ID               = '320000000') B       --机构表
     ON A.FR_ID                 = B.FR_ID 
  WHERE A.ORG_ID IS NULL 
    AND A.CUST_MNG IS NULL 
    AND A.FR_ID <> '095' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_TEMP_B_01 = sqlContext.sql(sql)
TMP_TEMP_B_01.registerTempTable("TMP_TEMP_B_01")
dfn="TMP_TEMP_B_01/"+V_DT+".parquet"
TMP_TEMP_B_01.cache()
nrows = TMP_TEMP_B_01.count()
TMP_TEMP_B_01.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_TEMP_B_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_TEMP_B_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-08::
V_STEP = V_STEP + 1

TMP_TEMP_B_01 = sqlContext.read.parquet(hdfs+'/TMP_TEMP_B_01/*')
TMP_TEMP_B_01.registerTempTable("TMP_TEMP_B_01")

sql = """
 SELECT ODS_CUSTOM_ID           AS ODS_CUSTOM_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,CERT_TYP                AS CERT_TYP 
       ,CERT_NO                 AS CERT_NO 
       ,ORG_ID                  AS ORG_ID 
       ,ORG_TYP                 AS ORG_TYP 
       ,CUST_MNG                AS CUST_MNG 
       ,MNG_TYP                 AS MNG_TYP 
       ,FR_ID                   AS FR_ID 
   FROM TMP_TEMP_B_01 A                                        --对私分配中间表B临时表01
  WHERE A.FR_ID <> '095' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_B = sqlContext.sql(sql)
TEMP_B.registerTempTable("TEMP_B")
dfn="TEMP_B/"+V_DT+".parquet"
TEMP_B.cache()
nrows = TEMP_B.count()
TEMP_B.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_B.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_B/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_B lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-09::
V_STEP = V_STEP + 1

TEMP_B = sqlContext.read.parquet(hdfs+'/TEMP_B/*')
TEMP_B.registerTempTable("TEMP_B")

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.CUST_MNG              AS CUST_MNG 
       ,A.MNG_TYP               AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_B A                                               --对私分配中间表B
  WHERE ORG_ID IS 
    NOT NULL 
    AND A.FR_ID <> '095' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_A = sqlContext.sql(sql)
TEMP_A.registerTempTable("TEMP_A")
dfn="TEMP_A/"+V_DT+"_02.parquet"
TEMP_A.cache()
nrows = TEMP_A.count()

#清除历史数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_A/"+V_DT+"_02.parquet")
#写入
TEMP_A.write.save(path=hdfs + '/' + dfn, mode='overwrite')

TEMP_A.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_A/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_A lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-10::
V_STEP = V_STEP + 1

sql = """
 SELECT DISTINCT CUST_ID AS CUST_ID 
       ,ORG_ID                  AS INSTITUTION_CODE 
       ,''                    AS INSTITUTION_NAME 
       ,CASE WHEN RN = 1 THEN '1' ELSE '2' END  AS MAIN_TYPE 
       ,''                      AS ASSIGN_USER 
       ,''                      AS ASSIGN_USERNAME 
       ,V_DT                    AS ASSIGN_DATE 
       ,''                    AS ETL_DATE 
       ,''                    AS EFF_DATE 
       ,FR_ID                   AS FR_ID 
   FROM OCRM_CUST_LOAN_AUM A                                   --客户-机构贷款AUM"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")
dfn="OCRM_F_CI_BELONG_ORG_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP.cache()
nrows = OCRM_F_CI_BELONG_ORG_TEMP.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP/*.parquet")
OCRM_F_CI_BELONG_ORG_TEMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_TEMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-11::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP/*')
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")

sql = """
 SELECT DISTINCT A.CUST_ID               AS CUST_ID 
       ,A.ORG_ID                AS INSTITUTION_CODE 
       ,B.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,CASE WHEN A.RANK = 1 THEN '1' ELSE '2' END AS MAIN_TYPE 
       ,''                      AS ASSIGN_USER 
       ,'' AS ASSIGN_USERNAME
		,V_DT AS ASSIGN_DATE
		,B.ETL_DATE AS ETL_DATE
		,B.EFF_DATE AS EFF_DATE
		,'095' AS FR_ID 
from 
OCRM_CUST_AUM_VALUE A    --客户-机构存款AUM
		LEFT JOIN
		OCRM_F_CI_BELONG_ORG_TEMP B    --射阳对私分配临时表
		on
		A.CUST_ID = B.CUST_ID AND A.FR_ID = B.FR_ID AND B.MAIN_TYPE = '1' AND B.FR_ID = '095'  
where A.FR_ID = '095'
    AND B.FR_ID IS NULL  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")
dfn="OCRM_F_CI_BELONG_ORG_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP.cache()
nrows = OCRM_F_CI_BELONG_ORG_TEMP.count()
OCRM_F_CI_BELONG_ORG_TEMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_ORG_TEMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-12::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP/*')
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")

sql = """
 SELECT DISTINCT A.CUST_ID               AS CUST_ID 
       ,A.ORG_ID                AS INSTITUTION_CODE 
       ,B.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,CASE WHEN A.RANK = 1 THEN '1' ELSE '2' END AS MAIN_TYPE 
       ,''                      AS ASSIGN_USER 
       ,'' AS ASSIGN_USERNAME
		,V_DT AS ASSIGN_DATE
		,B.ETL_DATE AS ETL_DATE
		,B.EFF_DATE AS EFF_DATE
		,'095' AS FR_ID 
from 
OCRM_CUST_AUM_VALUE A    --客户-机构存款AUM
		LEFT JOIN
		OCRM_F_CI_BELONG_ORG_TEMP B    --射阳对私分配临时表
		on
		A.CUST_ID = B.CUST_ID AND A.FR_ID = B.FR_ID AND A.ORG_ID = B.INSTITUTION_CODE AND B.FR_ID = '095'  
where A.FR_ID = '095'
    AND B.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")
dfn="OCRM_F_CI_BELONG_ORG_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP.cache()
nrows = OCRM_F_CI_BELONG_ORG_TEMP.count()
OCRM_F_CI_BELONG_ORG_TEMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_ORG_TEMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-13::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP/*')
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,B.ORG_ID                AS INSTITUTION_CODE 
       ,B.ORG_NAME              AS INSTITUTION_NAME 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG_TEMP A                            --射阳对私分配临时表
  INNER JOIN(
         SELECT DISTINCT ORG_ID,FR_ID 
               ,ORG_NAME 
           FROM ADMIN_AUTH_ORG) B                              --机构表
     ON A.FR_ID                 = B.FR_ID 
    AND A.INSTITUTION_CODE      = B.ORG_ID 
  WHERE A.FR_ID                 = '095' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1")

OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP/*')
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.INSTITUTION_CODE                                   --归属机构:src.INSTITUTION_CODE
       ,DST.INSTITUTION_NAME                                   --归属机构名称:src.INSTITUTION_NAME
       ,DST.MAIN_TYPE                                          --主协办:src.MAIN_TYPE
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.ETL_DATE                                           --加工日期:src.ETL_DATE
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BELONG_ORG_TEMP DST 
   LEFT JOIN OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.INSTITUTION_CODE    = DST.INSTITUTION_CODE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_ORG_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2=OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2.unionAll(OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1)
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1.cache()
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2.count()
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1.unpersist()
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP_BK/")

#任务[12] 001-14::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP/*')
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,'2'                     AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG_TEMP A                            --射阳对私分配临时表
  INNER JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND B.FR_ID                 = '095' 
  WHERE A.FR_ID                 = '095' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1")

OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP/*')
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.INSTITUTION_CODE                                   --归属机构:src.INSTITUTION_CODE
       ,DST.INSTITUTION_NAME                                   --归属机构名称:src.INSTITUTION_NAME
       ,DST.MAIN_TYPE                                          --主协办:src.MAIN_TYPE
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.ETL_DATE                                           --加工日期:src.ETL_DATE
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BELONG_ORG_TEMP DST 
   LEFT JOIN OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_ORG_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2=OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2.unionAll(OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1)
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1.cache()
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2.count()
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP1.unpersist()
OCRM_F_CI_BELONG_ORG_TEMP_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP_BK/")

#任务[21] 001-15::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP/*')
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG_TEMP A                            --射阳对私分配临时表
   LEFT JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.INSTITUTION_CODE      = B.INSTITUTION_CODE 
  WHERE B.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_01 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP_01.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_01")
dfn="OCRM_F_CI_BELONG_ORG_TEMP_01/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP_01.cache()
nrows = OCRM_F_CI_BELONG_ORG_TEMP_01.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP_01/*.parquet")
OCRM_F_CI_BELONG_ORG_TEMP_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_TEMP_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-16::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG_TEMP_01 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP_01/*')
OCRM_F_CI_BELONG_ORG_TEMP_01.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_01")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG_TEMP_01 A                         --射阳对私分配临时表01
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")
dfn="OCRM_F_CI_BELONG_ORG_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP.cache()
nrows = OCRM_F_CI_BELONG_ORG_TEMP.count()
OCRM_F_CI_BELONG_ORG_TEMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_ORG_TEMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
