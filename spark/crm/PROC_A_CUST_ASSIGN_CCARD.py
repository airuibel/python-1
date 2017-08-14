#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CUST_ASSIGN_CCARD').setMaster(sys.argv[2])
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

ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
OCRM_F_CI_CUSTLNAINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUSTLNAINFO/*')
OCRM_F_CI_CUSTLNAINFO.registerTempTable("OCRM_F_CI_CUSTLNAINFO")
OCRM_F_CI_CCARD = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CCARD/*')
OCRM_F_CI_CCARD.registerTempTable("OCRM_F_CI_CCARD")
OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
TEMP_A = sqlContext.read.parquet(hdfs+'/TEMP_A/*')
TEMP_A.registerTempTable("TEMP_A")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT DISTINCT A.CUST_ID               AS ODS_CUSTOM_ID 
       ,A.CUSTOM_NAME           AS CUST_NAME 
       ,''                    AS CERT_TYP 
       ,''                    AS CERT_NO 
       ,''                    AS ORG_ID 
       ,'1'                     AS ORG_TYP 
       ,''                    AS CUST_MNG 
       ,'1'                     AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUSTLNAINFO A                                --客户信用信息
   LEFT JOIN TEMP_A B                                          --对私分配中间表A
     ON A.CUST_ID               = B.ODS_CUSTOM_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE B.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C = sqlContext.sql(sql)
TEMP_C.registerTempTable("TEMP_C")
dfn="TEMP_C/"+V_DT+".parquet"
TEMP_C.cache()
nrows = TEMP_C.count()
TEMP_C.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TEMP_C.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_C/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

TEMP_C = sqlContext.read.parquet(hdfs+'/TEMP_C/*')
TEMP_C.registerTempTable("TEMP_C")

sql = """
 SELECT DISTINCT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.CUST_MNG              AS CUST_MNG 
       ,A.MNG_TYP               AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_C A                                               --对私分配中间表C
   LEFT JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.ODS_CUSTOM_ID         = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE B.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C_BK = sqlContext.sql(sql)
TEMP_C_BK.registerTempTable("TEMP_C_BK")
dfn="TEMP_C_BK/"+V_DT+".parquet"
TEMP_C_BK.cache()
nrows = TEMP_C_BK.count()
TEMP_C_BK.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TEMP_C_BK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_C_BK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C_BK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

TEMP_C_BK = sqlContext.read.parquet(hdfs+'/TEMP_C_BK/*')
TEMP_C_BK.registerTempTable("TEMP_C_BK")

sql = """
 SELECT DISTINCT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.CUST_MNG              AS CUST_MNG 
       ,A.MNG_TYP               AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_C_BK A                                            --对私分配中间表C(删数使用)
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C = sqlContext.sql(sql)
TEMP_C.registerTempTable("TEMP_C")
dfn="TEMP_C/"+V_DT+".parquet"
TEMP_C.cache()
nrows = TEMP_C.count()
TEMP_C.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_C.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_C/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT DISTINCT B.FR_ID                 AS FR_ID 
       ,B.CUST_ID               AS CUST_ID 
       ,ROW_NUMBER() OVER(
      PARTITION BY B.FR_ID 
               ,B.CUST_ID 
          ORDER BY C.DAY_OPENED)                       AS ROWNUM 
       ,C.INTR_NBR              AS USER_ID 
   FROM OCRM_F_CI_CUSTLNAINFO B                                --客户信用信息
  INNER JOIN OCRM_F_CI_CCARD C                                 --贷记卡卡信息
     ON B.CARD_NO               = C.CARD_NBR """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_TEMP_C_02 = sqlContext.sql(sql)
TMP_TEMP_C_02.registerTempTable("TMP_TEMP_C_02")
dfn="TMP_TEMP_C_02/"+V_DT+".parquet"
TMP_TEMP_C_02.cache()
nrows = TMP_TEMP_C_02.count()
TMP_TEMP_C_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_TEMP_C_02.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_TEMP_C_02/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_TEMP_C_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-05::
V_STEP = V_STEP + 1

TMP_TEMP_C_02 = sqlContext.read.parquet(hdfs+'/TMP_TEMP_C_02/*')
TMP_TEMP_C_02.registerTempTable("TMP_TEMP_C_02")

sql = """
 SELECT DISTINCT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_TYP               AS ORG_TYP 
       ,B.USER_ID               AS CUST_MNG 
       ,A.MNG_TYP               AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_C A                                               --对私分配中间表C
  INNER JOIN TMP_TEMP_C_02 B                                   --对私分配中间表C临时表02(贷记卡)
     ON A.ODS_CUSTOM_ID         = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ROWNUM                = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C_INNTMP1 = sqlContext.sql(sql)
TEMP_C_INNTMP1.registerTempTable("TEMP_C_INNTMP1")

TEMP_C = sqlContext.read.parquet(hdfs+'/TEMP_C/*')
TEMP_C.registerTempTable("TEMP_C")
sql = """
 SELECT DISTINCT DST.ODS_CUSTOM_ID                                       --客户号:src.ODS_CUSTOM_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_TYP                                            --机构主协办:src.ORG_TYP
       ,DST.CUST_MNG                                           --客户经理:src.CUST_MNG
       ,DST.MNG_TYP                                            --经理主协办:src.MNG_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM TEMP_C DST 
   LEFT JOIN TEMP_C_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ODS_CUSTOM_ID       = DST.ODS_CUSTOM_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C_INNTMP2 = sqlContext.sql(sql)
dfn="TEMP_C/"+V_DT+".parquet"
TEMP_C_INNTMP2=TEMP_C_INNTMP2.unionAll(TEMP_C_INNTMP1)
TEMP_C_INNTMP1.cache()
TEMP_C_INNTMP2.cache()
nrowsi = TEMP_C_INNTMP1.count()
nrowsa = TEMP_C_INNTMP2.count()
TEMP_C_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
TEMP_C_INNTMP1.unpersist()
TEMP_C_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/TEMP_C/"+V_DT_LD+".parquet /"+dbname+"/TEMP_C_BK/")

#任务[21] 001-06::
V_STEP = V_STEP + 1

TEMP_C = sqlContext.read.parquet(hdfs+'/TEMP_C/*')
TEMP_C.registerTempTable("TEMP_C")

sql = """
 SELECT DISTINCT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.CUST_MNG              AS CUST_MNG 
       ,A.MNG_TYP               AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_C A                                               --对私分配中间表C
  WHERE ORG_ID IS 
    NOT NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C_BK = sqlContext.sql(sql)
TEMP_C_BK.registerTempTable("TEMP_C_BK")
dfn="TEMP_C_BK/"+V_DT+".parquet"
TEMP_C_BK.cache()
nrows = TEMP_C_BK.count()
TEMP_C_BK.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_C_BK.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_C_BK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C_BK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-07::
V_STEP = V_STEP + 1

TEMP_C = sqlContext.read.parquet(hdfs+'/TEMP_C/*')
TEMP_C.registerTempTable("TEMP_C")

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
   FROM TEMP_C A                                               --对私分配中间表C
  INNER JOIN(
         SELECT DISTINCT ORG_ID,FR_ID 
               ,ORG_NAME 
           FROM ADMIN_AUTH_ORG) B                              --机构表
     ON A.FR_ID                 = B.FR_ID 
  WHERE A.ORG_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C_BK = sqlContext.sql(sql)
TEMP_C_BK.registerTempTable("TEMP_C_BK")
dfn="TEMP_C_BK/"+V_DT+".parquet"
TEMP_C_BK.cache()
nrows = TEMP_C_BK.count()
TEMP_C_BK.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_C_BK.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C_BK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-08::
V_STEP = V_STEP + 1

TEMP_C_BK = sqlContext.read.parquet(hdfs+'/TEMP_C_BK/*')
TEMP_C_BK.registerTempTable("TEMP_C_BK")

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
   FROM TEMP_C_BK A                                            --对私分配中间表C(删数使用)
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C = sqlContext.sql(sql)
TEMP_C.registerTempTable("TEMP_C")
dfn="TEMP_C/"+V_DT+".parquet"
TEMP_C.cache()
nrows = TEMP_C.count()
TEMP_C.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_C.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_C/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-09::
V_STEP = V_STEP + 1

TEMP_C = sqlContext.read.parquet(hdfs+'/TEMP_C/*')
TEMP_C.registerTempTable("TEMP_C")

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
   FROM TEMP_C A                                               --对私分配中间表C
  WHERE ORG_ID IS 
    NOT NULL 
    AND ODS_CUSTOM_ID IS 
    NOT NULL 
    AND ODS_CUSTOM_ID <> '' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_A = sqlContext.sql(sql)
TEMP_A.registerTempTable("TEMP_A")
dfn="TEMP_A/"+V_DT+".parquet"
TEMP_A.cache()
nrows = TEMP_A.count()
TEMP_A.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_A.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_A lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
