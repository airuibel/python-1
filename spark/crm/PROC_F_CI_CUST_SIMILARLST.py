#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUST_SIMILARLST').setMaster(sys.argv[2])
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

#ACRM_F_CI_CUST_SIMILARLST_TMP01
#ACRM_F_CI_CUST_SIMILARLST_TMP02
#ACRM_F_CI_CUST_SIMILARLST：增改
#MID_SIMILARLST：增量
#SIMILAR_CUST_GROUP_TMP01
#SIMILAR_CUST_GROUP：增改
#TEMP_SIMILAR_CUST：先全后增后增改


#任务[21] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST/"+V_DT+".parquet")
ACRM_F_CI_CUST_SIMILARLST = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST/*')
ACRM_F_CI_CUST_SIMILARLST.registerTempTable("ACRM_F_CI_CUST_SIMILARLST")
sql = """
 SELECT CUSTOM_ID_MAIN          AS CUST_ID 
       ,ODS_SYS_ID              AS ODS_SYS_ID 
       ,FR_ID                   AS FR_ID 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
   FROM ACRM_F_CI_CUST_SIMILARLST A                            --客户相似标识清单历史表
  WHERE IS_SHOW                 = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_SIMILARLST = sqlContext.sql(sql)
MID_SIMILARLST.registerTempTable("MID_SIMILARLST")
dfn="MID_SIMILARLST/"+V_DT+".parquet"
MID_SIMILARLST.cache()
nrows = MID_SIMILARLST.count()
MID_SIMILARLST.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_SIMILARLST.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_SIMILARLST/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_SIMILARLST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
ret = os.system("hdfs dfs -rm -r/"+dbname+"/TEMP_SIMILAR_CUST/*")
 
MID_SIMILARLST = sqlContext.read.parquet(hdfs+'/MID_SIMILARLST/*')
MID_SIMILARLST.registerTempTable("MID_SIMILARLST")

sql = """
 SELECT DISTINCT '01'                    AS GROUP_TYP 
       ,A.CERT_TYPE             AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,''                      AS CUST_NAME 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(0 AS BIGINT)                      AS GROUP_ID 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
  INNER JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON B.ODS_ST_DATE           = V_DT 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CERT_NO               = A.CERT_NO 
    AND(B.CERT_FLAG             = 'Y' 
             OR B.CERT_FLAG IS NULL) 
    AND B.CUST_STAT             = '1' 
    AND B.CERT_TYPE             = A.CERT_TYPE 
    AND B.SOURCE_CUST_NAME <> A.SOURCE_CUST_NAME 
   LEFT JOIN MID_SIMILARLST C                                  --无需合并客户表
     ON A.ODS_CUST_ID           = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
  WHERE(A.CERT_FLAG             = 'Y' 
             OR A.CERT_FLAG IS NULL) 
    AND A.CUST_STAT             = '1' 
    AND C.CUST_ID IS NULL   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_SIMILAR_CUST = sqlContext.sql(sql)
TEMP_SIMILAR_CUST.registerTempTable("TEMP_SIMILAR_CUST")
dfn="TEMP_SIMILAR_CUST/"+V_DT+".parquet"
TEMP_SIMILAR_CUST.cache()
nrows = TEMP_SIMILAR_CUST.count()
TEMP_SIMILAR_CUST.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TEMP_SIMILAR_CUST.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_SIMILAR_CUST/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_SIMILAR_CUST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

TEMP_SIMILAR_CUST = sqlContext.read.parquet(hdfs+'/TEMP_SIMILAR_CUST/*')
TEMP_SIMILAR_CUST.registerTempTable("TEMP_SIMILAR_CUST")

sql = """
 SELECT DISTINCT '02'                    AS GROUP_TYP 
       ,''                      AS CERT_TYP 
       ,''                      AS CERT_NO 
       ,A.ODS_CUST_NAME         AS CUST_NAME 
       ,A.FR_ID                 AS FR_ID 
       ,CAST(0 AS BIGINT)                      AS GROUP_ID 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
  INNER JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON B.ODS_ST_DATE           = V_DT 
    AND A.FR_ID                 = B.FR_ID 
    AND(B.CERT_NO <> A.CERT_NO 
             OR B.CERT_TYPE <> A.CERT_TYPE) 
    AND(B.CERT_FLAG             = 'Y' 
             OR B.CERT_FLAG IS NULL) 
    AND B.CUST_STAT             = '1' 
    AND B.SOURCE_CUST_NAME      = A.SOURCE_CUST_NAME 
    AND B.ODS_CUST_TYPE         = '2' 
   LEFT JOIN MID_SIMILARLST C                                  --无需合并客户表
     ON A.ODS_CUST_ID           = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
  WHERE(A.CERT_FLAG             = 'Y' 
             OR A.CERT_FLAG IS NULL) 
    AND A.CUST_STAT             = '1' 
    AND A.ODS_CUST_TYPE         = '2' 
    AND C.CUST_ID IS NULL   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_SIMILAR_CUST = sqlContext.sql(sql)
TEMP_SIMILAR_CUST.registerTempTable("TEMP_SIMILAR_CUST")
dfn="TEMP_SIMILAR_CUST/"+V_DT+".parquet"
TEMP_SIMILAR_CUST.cache()
nrows = TEMP_SIMILAR_CUST.count()
TEMP_SIMILAR_CUST.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_SIMILAR_CUST.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_SIMILAR_CUST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-04::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/SIMILAR_CUST_GROUP/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/SIMILAR_CUST_GROUP_BK/"+V_DT_LD+".parquet /"+dbname+"/SIMILAR_CUST_GROUP/"+V_DT+".parquet")
SIMILAR_CUST_GROUP = sqlContext.read.parquet(hdfs+'/SIMILAR_CUST_GROUP/*')
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")
TEMP_SIMILAR_CUST = sqlContext.read.parquet(hdfs+'/TEMP_SIMILAR_CUST/*')
TEMP_SIMILAR_CUST.registerTempTable("TEMP_SIMILAR_CUST")

sql = """
 SELECT DISTINCT B.GROUP_ID              AS GROUP_ID 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,B.CUST_NAME             AS CUST_NAME 
       ,'01'                    AS SIMILAR_TYP 
       ,V_DT                    AS REGISTER_TIME 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_SIMILAR_CUST  A --当天相似客户组表
  INNER JOIN SIMILAR_CUST_GROUP B                              --相似组信息
     ON A.FR_ID                 = B.FR_ID 
    AND A.CERT_TYP              = B.CERT_TYP 
    AND A.CERT_NO               = B.CERT_NO 
  WHERE A.GROUP_TYP = '01'
    """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
SIMILAR_CUST_GROUP_INNTMP1 = sqlContext.sql(sql)
SIMILAR_CUST_GROUP_INNTMP1.registerTempTable("SIMILAR_CUST_GROUP_INNTMP1")

#SIMILAR_CUST_GROUP = sqlContext.read.parquet(hdfs+'/SIMILAR_CUST_GROUP/*')
#SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")
sql = """
 SELECT DISTINCT DST.GROUP_ID                                            --组号:src.GROUP_ID
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.SIMILAR_TYP                                        --相似类型:src.SIMILAR_TYP
       ,DST.REGISTER_TIME                                      --登记日期:src.REGISTER_TIME
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM SIMILAR_CUST_GROUP DST 
   LEFT JOIN SIMILAR_CUST_GROUP_INNTMP1 SRC 
     ON SRC.CERT_TYP            = DST.CERT_TYP 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CERT_TYP IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
SIMILAR_CUST_GROUP_INNTMP2 = sqlContext.sql(sql)
dfn="SIMILAR_CUST_GROUP/"+V_DT+".parquet"
SIMILAR_CUST_GROUP_INNTMP2 = SIMILAR_CUST_GROUP_INNTMP2.unionAll(SIMILAR_CUST_GROUP_INNTMP1)
SIMILAR_CUST_GROUP_INNTMP1.cache()
SIMILAR_CUST_GROUP_INNTMP2.cache()
nrowsi = SIMILAR_CUST_GROUP_INNTMP1.count()
nrowsa = SIMILAR_CUST_GROUP_INNTMP2.count()
SIMILAR_CUST_GROUP_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
SIMILAR_CUST_GROUP_INNTMP1.unpersist()
SIMILAR_CUST_GROUP_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert SIMILAR_CUST_GROUP lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/SIMILAR_CUST_GROUP/"+V_DT_LD+".parquet /"+dbname+"/SIMILAR_CUST_GROUP_BK/")

#任务[12] 001-05::
V_STEP = V_STEP + 1

SIMILAR_CUST_GROUP = sqlContext.read.parquet(hdfs+'/SIMILAR_CUST_GROUP/*')
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")

sql = """
 SELECT DISTINCT B.GROUP_ID              AS GROUP_ID 
       ,B.CERT_TYP              AS CERT_TYP 
       ,B.CERT_NO               AS CERT_NO 
       ,A.CUST_NAME             AS CUST_NAME 
       ,'02'                    AS SIMILAR_TYP 
       ,V_DT                    AS REGISTER_TIME 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_SIMILAR_CUST A                                    --当天相似客户组表
  INNER JOIN SIMILAR_CUST_GROUP B                              --相似组信息
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_NAME             = B.CUST_NAME
    WHERE A.GROUP_TYP = '02'   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
SIMILAR_CUST_GROUP_INNTMP1 = sqlContext.sql(sql)
SIMILAR_CUST_GROUP_INNTMP1.registerTempTable("SIMILAR_CUST_GROUP_INNTMP1")

SIMILAR_CUST_GROUP = sqlContext.read.parquet(hdfs+'/SIMILAR_CUST_GROUP/*')
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")
sql = """
 SELECT DISTINCT DST.GROUP_ID                                            --组号:src.GROUP_ID
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.SIMILAR_TYP                                        --相似类型:src.SIMILAR_TYP
       ,DST.REGISTER_TIME                                      --登记日期:src.REGISTER_TIME
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM SIMILAR_CUST_GROUP DST 
   LEFT JOIN SIMILAR_CUST_GROUP_INNTMP1 SRC 
     ON SRC.CUST_NAME           = DST.CUST_NAME 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
SIMILAR_CUST_GROUP_INNTMP2 = sqlContext.sql(sql)
dfn="SIMILAR_CUST_GROUP/"+V_DT+".parquet"
SIMILAR_CUST_GROUP_INNTMP2 = SIMILAR_CUST_GROUP_INNTMP2.unionAll(SIMILAR_CUST_GROUP_INNTMP1)
SIMILAR_CUST_GROUP_INNTMP1.cache()
SIMILAR_CUST_GROUP_INNTMP2.cache()
nrowsi = SIMILAR_CUST_GROUP_INNTMP1.count()
nrowsa = SIMILAR_CUST_GROUP_INNTMP2.count()
SIMILAR_CUST_GROUP_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
SIMILAR_CUST_GROUP_INNTMP1.unpersist()
SIMILAR_CUST_GROUP_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert SIMILAR_CUST_GROUP lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/SIMILAR_CUST_GROUP/"+V_DT_LD+".parquet /"+dbname+"/SIMILAR_CUST_GROUP_BK/")

#任务[12] 001-06::
V_STEP = V_STEP + 1

SIMILAR_CUST_GROUP = sqlContext.read.parquet(hdfs+'/SIMILAR_CUST_GROUP/*')
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")

sql = """
 SELECT DISTINCT '01'                    AS GROUP_TYP 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,B.CUST_NAME             AS CUST_NAME 
       ,A.FR_ID                 AS FR_ID 
       ,A.GROUP_ID              AS GROUP_ID 
   FROM SIMILAR_CUST_GROUP A                                   --相似组信息
  INNER JOIN TEMP_SIMILAR_CUST B                               --当天相似客户组表
     ON B.GROUP_TYP             = '01' 
    AND A.FR_ID                 = B.FR_ID 
    AND A.CERT_TYP              = B.CERT_TYP 
    AND A.CERT_NO               = B.CERT_NO  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_SIMILAR_CUST_INNTMP1 = sqlContext.sql(sql)
TEMP_SIMILAR_CUST_INNTMP1.registerTempTable("TEMP_SIMILAR_CUST_INNTMP1")

TEMP_SIMILAR_CUST = sqlContext.read.parquet(hdfs+'/TEMP_SIMILAR_CUST/*')
TEMP_SIMILAR_CUST.registerTempTable("TEMP_SIMILAR_CUST")
sql = """
 SELECT DST.GROUP_TYP                                           --组类型:src.GROUP_TYP
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.GROUP_ID                                           --已存在的组号:src.GROUP_ID
   FROM TEMP_SIMILAR_CUST DST 
   LEFT JOIN TEMP_SIMILAR_CUST_INNTMP1 SRC 
     ON SRC.GROUP_TYP           = DST.GROUP_TYP 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.CERT_TYP            = DST.CERT_TYP 
    AND SRC.CERT_NO             = DST.CERT_NO 
  WHERE SRC.GROUP_TYP IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_SIMILAR_CUST_INNTMP2 = sqlContext.sql(sql)
dfn="TEMP_SIMILAR_CUST/"+V_DT+".parquet"
TEMP_SIMILAR_CUST_INNTMP2 = TEMP_SIMILAR_CUST_INNTMP2.unionAll(TEMP_SIMILAR_CUST_INNTMP1)
TEMP_SIMILAR_CUST_INNTMP1.cache()
TEMP_SIMILAR_CUST_INNTMP2.cache()
nrowsi = TEMP_SIMILAR_CUST_INNTMP1.count()
nrowsa = TEMP_SIMILAR_CUST_INNTMP2.count()
TEMP_SIMILAR_CUST_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
TEMP_SIMILAR_CUST_INNTMP1.unpersist()
TEMP_SIMILAR_CUST_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_SIMILAR_CUST lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/TEMP_SIMILAR_CUST/"+V_DT_LD+".parquet /"+dbname+"/TEMP_SIMILAR_CUST_BK/")

#任务[12] 001-07::
V_STEP = V_STEP + 1

TEMP_SIMILAR_CUST = sqlContext.read.parquet(hdfs+'/TEMP_SIMILAR_CUST/*')
TEMP_SIMILAR_CUST.registerTempTable("TEMP_SIMILAR_CUST")

sql = """
 SELECT DISTINCT '02'                    AS GROUP_TYP 
       ,B.CERT_TYP              AS CERT_TYP 
       ,B.CERT_NO               AS CERT_NO 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.FR_ID                 AS FR_ID 
       ,A.GROUP_ID              AS GROUP_ID 
   FROM SIMILAR_CUST_GROUP A                                   --相似组信息
  INNER JOIN TEMP_SIMILAR_CUST B                               --当天相似客户组表
     ON B.GROUP_TYP             = '02' 
    AND A.FR_ID                 = B.FR_ID 
    AND A.CUST_NAME             = B.CUST_NAME   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_SIMILAR_CUST_INNTMP1 = sqlContext.sql(sql)
TEMP_SIMILAR_CUST_INNTMP1.registerTempTable("TEMP_SIMILAR_CUST_INNTMP1")

TEMP_SIMILAR_CUST = sqlContext.read.parquet(hdfs+'/TEMP_SIMILAR_CUST/*')
TEMP_SIMILAR_CUST.registerTempTable("TEMP_SIMILAR_CUST")
sql = """
 SELECT DST.GROUP_TYP                                           --组类型:src.GROUP_TYP
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.GROUP_ID                                           --已存在的组号:src.GROUP_ID
   FROM TEMP_SIMILAR_CUST DST 
   LEFT JOIN TEMP_SIMILAR_CUST_INNTMP1 SRC 
     ON SRC.GROUP_TYP           = DST.GROUP_TYP 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_NAME           = DST.CUST_NAME 
  WHERE SRC.GROUP_TYP IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_SIMILAR_CUST_INNTMP2 = sqlContext.sql(sql)
dfn="TEMP_SIMILAR_CUST/"+V_DT+".parquet"
TEMP_SIMILAR_CUST_INNTMP2 = TEMP_SIMILAR_CUST_INNTMP2.unionAll(TEMP_SIMILAR_CUST_INNTMP1)
TEMP_SIMILAR_CUST_INNTMP1.cache()
TEMP_SIMILAR_CUST_INNTMP2.cache()
nrowsi = TEMP_SIMILAR_CUST_INNTMP1.count()
nrowsa = TEMP_SIMILAR_CUST_INNTMP2.count()
TEMP_SIMILAR_CUST_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
TEMP_SIMILAR_CUST_INNTMP1.unpersist()
TEMP_SIMILAR_CUST_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_SIMILAR_CUST lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/TEMP_SIMILAR_CUST/"+V_DT_LD+".parquet /"+dbname+"/TEMP_SIMILAR_CUST_BK/")

#任务[21] 001-08::
V_STEP = V_STEP + 1

TEMP_SIMILAR_CUST = sqlContext.read.parquet(hdfs+'/TEMP_SIMILAR_CUST/*')
TEMP_SIMILAR_CUST.registerTempTable("TEMP_SIMILAR_CUST")

sql = """
 SELECT DISTINCT A.CUSTOM_ID_MAIN          AS CUSTOM_ID_MAIN 
       ,CAST(A.TYP_ID AS INT)                  AS TYP_ID 
       ,A.GROUP_ID                AS GROUP_ID 
       ,A.SOUR_CUSTOM_ID_M        AS SOUR_CUSTOM_ID_M 
       ,A.CERT_TYP_M              AS CERT_TYP_M 
       ,A.CERT_NO_M               AS CERT_NO_M 
       ,A.CUST_NAME_M             AS CUST_NAME_M 
       ,A.ODS_LOAD_DT             AS ODS_LOAD_DT 
       ,A.ODS_SYS_ID              AS ODS_SYS_ID 
       ,A.ODS_ST_DATE             AS ODS_ST_DATE 
       ,A.IS_SHOW                 AS IS_SHOW 
       ,A.FR_ID                   AS FR_ID 
   FROM ACRM_F_CI_CUST_SIMILARLST A                            --客户相似标识清单历史表
  LEFT JOIN SIMILAR_CUST_GROUP B                              --相似组信息
     ON B.GROUP_ID              = A.TYP_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.REGISTER_TIME         = V_DT 
  WHERE IS_SHOW <> '2' AND B.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_SIMILARLST_TMP01 = sqlContext.sql(sql)
ACRM_F_CI_CUST_SIMILARLST_TMP01.registerTempTable("ACRM_F_CI_CUST_SIMILARLST_TMP01")
dfn="ACRM_F_CI_CUST_SIMILARLST_TMP01/"+V_DT+".parquet"
ACRM_F_CI_CUST_SIMILARLST_TMP01.cache()
nrows = ACRM_F_CI_CUST_SIMILARLST_TMP01.count()
ACRM_F_CI_CUST_SIMILARLST_TMP01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_SIMILARLST_TMP01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST_TMP01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_SIMILARLST_TMP01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-09::
V_STEP = V_STEP + 1

ACRM_F_CI_CUST_SIMILARLST_TMP01 = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST_TMP01/*')
ACRM_F_CI_CUST_SIMILARLST_TMP01.registerTempTable("ACRM_F_CI_CUST_SIMILARLST_TMP01")

sql = """
 SELECT A.CUSTOM_ID_MAIN        AS CUSTOM_ID_MAIN 
       ,A.TYP_ID                AS TYP_ID 
       ,A.GROUP_ID              AS GROUP_ID 
       ,A.SOUR_CUSTOM_ID_M      AS SOUR_CUSTOM_ID_M 
       ,A.CERT_TYP_M            AS CERT_TYP_M 
       ,A.CERT_NO_M             AS CERT_NO_M 
       ,A.CUST_NAME_M           AS CUST_NAME_M 
       ,A.ODS_LOAD_DT           AS ODS_LOAD_DT 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.IS_SHOW               AS IS_SHOW 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_CUST_SIMILARLST_TMP01 A                      --客户相似标识清单历史表临时表01
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_SIMILARLST = sqlContext.sql(sql)
ACRM_F_CI_CUST_SIMILARLST.registerTempTable("ACRM_F_CI_CUST_SIMILARLST")
dfn="ACRM_F_CI_CUST_SIMILARLST/"+V_DT+".parquet"
ACRM_F_CI_CUST_SIMILARLST.cache()
nrows = ACRM_F_CI_CUST_SIMILARLST.count()
ACRM_F_CI_CUST_SIMILARLST.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_SIMILARLST.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_SIMILARLST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-10::
V_STEP = V_STEP + 1

#ACRM_F_CI_CUST_SIMILARLST = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST/*')
#ACRM_F_CI_CUST_SIMILARLST.registerTempTable("ACRM_F_CI_CUST_SIMILARLST")

sql = """
 SELECT GROUP_ID                AS GROUP_ID 
       ,CERT_TYP                AS CERT_TYP 
       ,CERT_NO                 AS CERT_NO 
       ,CUST_NAME               AS CUST_NAME 
       ,SIMILAR_TYP             AS SIMILAR_TYP 
       ,REGISTER_TIME           AS REGISTER_TIME 
       ,FR_ID                   AS FR_ID 
   FROM SIMILAR_CUST_GROUP A                                   --相似组信息
  WHERE REGISTER_TIME <> V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
SIMILAR_CUST_GROUP_TMP01 = sqlContext.sql(sql)
SIMILAR_CUST_GROUP_TMP01.registerTempTable("SIMILAR_CUST_GROUP_TMP01")
dfn="SIMILAR_CUST_GROUP_TMP01/"+V_DT+".parquet"
SIMILAR_CUST_GROUP_TMP01.cache()
nrows = SIMILAR_CUST_GROUP_TMP01.count()
SIMILAR_CUST_GROUP_TMP01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
SIMILAR_CUST_GROUP_TMP01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/SIMILAR_CUST_GROUP_TMP01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert SIMILAR_CUST_GROUP_TMP01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-11::
V_STEP = V_STEP + 1

SIMILAR_CUST_GROUP_TMP01 = sqlContext.read.parquet(hdfs+'/SIMILAR_CUST_GROUP_TMP01/*')
SIMILAR_CUST_GROUP_TMP01.registerTempTable("SIMILAR_CUST_GROUP_TMP01")

sql = """
 SELECT GROUP_ID                AS GROUP_ID 
       ,CERT_TYP                AS CERT_TYP 
       ,CERT_NO                 AS CERT_NO 
       ,CUST_NAME               AS CUST_NAME 
       ,SIMILAR_TYP             AS SIMILAR_TYP 
       ,REGISTER_TIME           AS REGISTER_TIME 
       ,FR_ID                   AS FR_ID 
   FROM SIMILAR_CUST_GROUP_TMP01 A                             --相似组信息临时表01
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
SIMILAR_CUST_GROUP = sqlContext.sql(sql)
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")
dfn="SIMILAR_CUST_GROUP/"+V_DT+".parquet"
SIMILAR_CUST_GROUP.cache()
nrows = SIMILAR_CUST_GROUP.count()
SIMILAR_CUST_GROUP.write.save(path=hdfs + '/' + dfn, mode='append')
SIMILAR_CUST_GROUP.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/SIMILAR_CUST_GROUP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert SIMILAR_CUST_GROUP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-12::
V_STEP = V_STEP + 1

SIMILAR_CUST_GROUP = sqlContext.read.parquet(hdfs+'/SIMILAR_CUST_GROUP/*')
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")

sql = """
 SELECT GROUP_ID                AS GROUP_ID 
       ,CERT_TYP                AS CERT_TYP 
       ,CERT_NO                 AS CERT_NO 
       ,''                      AS CUST_NAME 
       ,'01'                    AS SIMILAR_TYP 
       ,V_DT                    AS REGISTER_TIME 
       ,FR_ID                   AS FR_ID 
   FROM TEMP_SIMILAR_CUST A                                    --当天相似客户组表
  WHERE GROUP_TYP               = '01' 
    AND GROUP_ID IS 
    NOT NULL 
  GROUP BY GROUP_ID 
       ,CERT_TYP 
       ,CERT_NO 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
SIMILAR_CUST_GROUP = sqlContext.sql(sql)
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")
dfn="SIMILAR_CUST_GROUP/"+V_DT+".parquet"
SIMILAR_CUST_GROUP.cache()
nrows = SIMILAR_CUST_GROUP.count()
SIMILAR_CUST_GROUP.write.save(path=hdfs + '/' + dfn, mode='append')
SIMILAR_CUST_GROUP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert SIMILAR_CUST_GROUP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-13::
V_STEP = V_STEP + 1

SIMILAR_CUST_GROUP = sqlContext.read.parquet(hdfs+'/SIMILAR_CUST_GROUP/*')
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")

sql = """
 SELECT GROUP_ID                AS GROUP_ID 
       ,''                      AS CERT_TYP 
       ,''                      AS CERT_NO 
       ,CUST_NAME               AS CUST_NAME 
       ,'02'                    AS SIMILAR_TYP 
       ,V_DT                    AS REGISTER_TIME 
       ,FR_ID                   AS FR_ID 
   FROM TEMP_SIMILAR_CUST A                                    --当天相似客户组表
  WHERE GROUP_TYP               = '02' 
    AND GROUP_ID IS 
    NOT NULL 
  GROUP BY GROUP_ID 
       ,CUST_NAME 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
SIMILAR_CUST_GROUP = sqlContext.sql(sql)
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")
dfn="SIMILAR_CUST_GROUP/"+V_DT+".parquet"
SIMILAR_CUST_GROUP.cache()
nrows = SIMILAR_CUST_GROUP.count()
SIMILAR_CUST_GROUP.write.save(path=hdfs + '/' + dfn, mode='append')
SIMILAR_CUST_GROUP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert SIMILAR_CUST_GROUP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-14::
V_STEP = V_STEP + 1

SIMILAR_CUST_GROUP = sqlContext.read.parquet(hdfs+'/SIMILAR_CUST_GROUP/*')
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")

sql = """
 SELECT RANK() OVER(ORDER BY CERT_TYP,CERT_NO,A.FR_ID) + MAX_GROUP_ID            AS GROUP_ID 
       ,CERT_TYP                AS CERT_TYP 
       ,CERT_NO                 AS CERT_NO 
       ,''                      AS CUST_NAME 
       ,'01'                    AS SIMILAR_TYP 
       ,V_DT                    AS REGISTER_TIME 
       ,A.FR_ID                   AS FR_ID 
   FROM TEMP_SIMILAR_CUST A                                    --当天相似客户组表
  INNER JOIN(
         SELECT FR_ID,COALESCE(MAX(GROUP_ID), 0)                       AS MAX_GROUP_ID 
           FROM SIMILAR_CUST_GROUP
           GROUP BY FR_ID) B                          --最大组号
     ON A.GROUP_TYP = '01' AND A.FR_ID = B.FR_ID
    AND GROUP_ID IS NULL  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
SIMILAR_CUST_GROUP = sqlContext.sql(sql)
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")
dfn="SIMILAR_CUST_GROUP/"+V_DT+".parquet"
SIMILAR_CUST_GROUP.cache()
nrows = SIMILAR_CUST_GROUP.count()
SIMILAR_CUST_GROUP.write.save(path=hdfs + '/' + dfn, mode='append')
SIMILAR_CUST_GROUP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert SIMILAR_CUST_GROUP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-15::
V_STEP = V_STEP + 1

SIMILAR_CUST_GROUP = sqlContext.read.parquet(hdfs+'/SIMILAR_CUST_GROUP/*')
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")

sql = """
 SELECT RANK() OVER(
          ORDER BY CUST_NAME 
               ,A.FR_ID) + MAX_GROUP_ID            AS GROUP_ID 
       ,''                      AS CERT_TYP 
       ,''                      AS CERT_NO 
       ,CUST_NAME               AS CUST_NAME 
       ,'02'                    AS SIMILAR_TYP 
       ,V_DT                    AS REGISTER_TIME 
       ,A.FR_ID                   AS FR_ID 
   FROM TEMP_SIMILAR_CUST A                                    --当天相似客户组表
  INNER JOIN(
         SELECT FR_ID,COALESCE(MAX(GROUP_ID), 0)                       AS MAX_GROUP_ID 
           FROM SIMILAR_CUST_GROUP
          GROUP BY FR_ID) B                          --最大组号
     ON A.GROUP_TYP = '02' AND A.FR_ID =B.FR_ID 
    AND GROUP_ID IS NULL   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
SIMILAR_CUST_GROUP = sqlContext.sql(sql)
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")
dfn="SIMILAR_CUST_GROUP/"+V_DT+".parquet"
SIMILAR_CUST_GROUP.cache()
nrows = SIMILAR_CUST_GROUP.count()
SIMILAR_CUST_GROUP.write.save(path=hdfs + '/' + dfn, mode='append')

#备份
ret = os.system("hdfs dfs -cp -f /"+dbname+"/SIMILAR_CUST_GROUP/"+V_DT+".parquet /"+dbname+"/SIMILAR_CUST_GROUP_BK/"+V_DT+".parquet")
SIMILAR_CUST_GROUP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert SIMILAR_CUST_GROUP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-16::
V_STEP = V_STEP + 1

SIMILAR_CUST_GROUP = sqlContext.read.parquet(hdfs+'/SIMILAR_CUST_GROUP/*')
SIMILAR_CUST_GROUP.registerTempTable("SIMILAR_CUST_GROUP")

sql = """
 SELECT COALESCE(A.FR_ID, 'UNK')                       AS FR_ID 
       ,CAST(A.GROUP_ID AS INTEGER)              AS TYP_ID 
       ,B.ODS_CUST_ID           AS CUSTOM_ID_MAIN 
       ,B.SOURCE_CUST_ID        AS SOUR_CUSTOM_ID_M 
       ,A.SIMILAR_TYP           AS GROUP_ID 
       ,A.CERT_TYP              AS CERT_TYP_M 
       ,B.SOURCE_CUST_NAME      AS CUST_NAME_M 
       ,A.CERT_NO               AS CERT_NO_M 
       ,B.ODS_ST_DATE           AS ODS_ST_DATE 
       ,B.ODS_SYS_ID            AS ODS_SYS_ID 
       ,CAST(CASE WHEN B.ODS_SYS_ID = 'CEN' THEN 1 
                  WHEN B.ODS_SYS_ID = 'LNA' THEN 2 
                  WHEN B.ODS_SYS_ID = 'INT' THEN 3 
                  WHEN B.ODS_SYS_ID = 'CRE' THEN 4 
                  WHEN B.ODS_SYS_ID = 'IBK' THEN 5 
                  ELSE 6 END AS VARCHAR(10)) AS ODS_SYS_ID_NO 
   FROM SIMILAR_CUST_GROUP A                                   --相似组信息
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.CERT_TYP              = B.CERT_TYPE 
    AND A.CERT_NO               = B.CERT_NO 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_STAT             = '1' 
  WHERE A.SIMILAR_TYP           = '01' 
    AND A.REGISTER_TIME         = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_SIMILARLST_TMP02 = sqlContext.sql(sql)
ACRM_F_CI_CUST_SIMILARLST_TMP02.registerTempTable("ACRM_F_CI_CUST_SIMILARLST_TMP02")
dfn="ACRM_F_CI_CUST_SIMILARLST_TMP02/"+V_DT+".parquet"
ACRM_F_CI_CUST_SIMILARLST_TMP02.cache()
nrows = ACRM_F_CI_CUST_SIMILARLST_TMP02.count()
ACRM_F_CI_CUST_SIMILARLST_TMP02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_SIMILARLST_TMP02.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST_TMP02/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_SIMILARLST_TMP02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-17::
V_STEP = V_STEP + 1

ACRM_F_CI_CUST_SIMILARLST_TMP02 = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST_TMP02/*')
ACRM_F_CI_CUST_SIMILARLST_TMP02.registerTempTable("ACRM_F_CI_CUST_SIMILARLST_TMP02")

sql = """
 SELECT COALESCE(A.FR_ID, 'UNK')                       AS FR_ID 
       ,CAST(A.GROUP_ID AS INTEGER)             AS TYP_ID 
       ,B.ODS_CUST_ID           AS CUSTOM_ID_MAIN 
       ,B.SOURCE_CUST_ID        AS SOUR_CUSTOM_ID_M 
       ,A.SIMILAR_TYP           AS GROUP_ID 
       ,A.CERT_TYP              AS CERT_TYP_M 
       ,B.SOURCE_CUST_NAME      AS CUST_NAME_M 
       ,A.CERT_NO               AS CERT_NO_M 
       ,B.ODS_ST_DATE           AS ODS_ST_DATE 
       ,B.ODS_SYS_ID            AS ODS_SYS_ID 
       ,CAST(CASE WHEN B.ODS_SYS_ID = 'CEN' THEN 1 
             WHEN B.ODS_SYS_ID = 'LNA' THEN 2 
             WHEN B.ODS_SYS_ID = 'INT' THEN 3 
             WHEN B.ODS_SYS_ID = 'CRE' THEN 4 
             WHEN B.ODS_SYS_ID = 'IBK' THEN 5 
             ELSE 6 END AS VARCHAR(10)) AS ODS_SYS_ID_NO 
   FROM SIMILAR_CUST_GROUP A                                   --相似组信息
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.CERT_TYP              = B.CERT_TYPE 
    AND A.CERT_NO               = B.CERT_NO 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_STAT             = '1' 
    AND B.ODS_CUST_TYPE         = '2' 
  WHERE A.SIMILAR_TYP           = '02' 
    AND A.REGISTER_TIME         = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_SIMILARLST_TMP02 = sqlContext.sql(sql)
ACRM_F_CI_CUST_SIMILARLST_TMP02.registerTempTable("ACRM_F_CI_CUST_SIMILARLST_TMP02")
dfn="ACRM_F_CI_CUST_SIMILARLST_TMP02/"+V_DT+".parquet"
ACRM_F_CI_CUST_SIMILARLST_TMP02.cache()
nrows = ACRM_F_CI_CUST_SIMILARLST_TMP02.count()
ACRM_F_CI_CUST_SIMILARLST_TMP02.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_SIMILARLST_TMP02.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_SIMILARLST_TMP02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-18::
V_STEP = V_STEP + 1

ACRM_F_CI_CUST_SIMILARLST_TMP02 = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST_TMP02/*')
ACRM_F_CI_CUST_SIMILARLST_TMP02.registerTempTable("ACRM_F_CI_CUST_SIMILARLST_TMP02")

sql = """
 SELECT CUSTOM_ID_MAIN          AS CUSTOM_ID_MAIN 
       ,CAST(TYP_ID AS INTEGER)                 AS TYP_ID 
       ,GROUP_ID                AS GROUP_ID 
       ,SOUR_CUSTOM_ID_M        AS SOUR_CUSTOM_ID_M 
       ,CERT_TYP_M              AS CERT_TYP_M 
       ,CERT_NO_M               AS CERT_NO_M 
       ,CUST_NAME_M             AS CUST_NAME_M 
       ,V_DT                    AS ODS_LOAD_DT 
       ,ODS_SYS_ID              AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,''                      AS IS_SHOW 
       ,FR_ID                   AS FR_ID 
   FROM (SELECT  T.*,
                 ROW_NUMBER () OVER (PARTITION BY T.GROUP_ID,CUSTOM_ID_MAIN,FR_ID 
                                     ORDER BY T.ODS_SYS_ID_NO ASC,ODS_ST_DATE DESC) AS N 
           FROM ACRM_F_CI_CUST_SIMILARLST_TMP02 T ) A                                                   --当日客户合并表
  WHERE N = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_SIMILARLST_TMP01 = sqlContext.sql(sql)
ACRM_F_CI_CUST_SIMILARLST_TMP01.registerTempTable("ACRM_F_CI_CUST_SIMILARLST_TMP01")
dfn="ACRM_F_CI_CUST_SIMILARLST_TMP01/"+V_DT+".parquet"
ACRM_F_CI_CUST_SIMILARLST_TMP01.cache()
nrows = ACRM_F_CI_CUST_SIMILARLST_TMP01.count()
ACRM_F_CI_CUST_SIMILARLST_TMP01.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_SIMILARLST_TMP01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST_TMP01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_SIMILARLST_TMP01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-19::
V_STEP = V_STEP + 1

ACRM_F_CI_CUST_SIMILARLST_TMP01 = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST_TMP01/*')
ACRM_F_CI_CUST_SIMILARLST_TMP01.registerTempTable("ACRM_F_CI_CUST_SIMILARLST_TMP01")

sql = """
 SELECT A.CUSTOM_ID_MAIN        AS CUSTOM_ID_MAIN 
       ,CAST(A.TYP_ID AS INTEGER)               AS TYP_ID 
       ,A.GROUP_ID              AS GROUP_ID 
       ,A.SOUR_CUSTOM_ID_M      AS SOUR_CUSTOM_ID_M 
       ,A.CERT_TYP_M            AS CERT_TYP_M 
       ,A.CERT_NO_M             AS CERT_NO_M 
       ,A.CUST_NAME_M           AS CUST_NAME_M 
       ,A.ODS_LOAD_DT           AS ODS_LOAD_DT 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.IS_SHOW               AS IS_SHOW 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_CUST_SIMILARLST_TMP01 A                      --客户相似标识清单历史表临时表01
   LEFT JOIN MID_SIMILARLST B                                  --无需合并客户表
     ON A.CUSTOM_ID_MAIN        = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.ODS_ST_DATE             = V_DT 
  WHERE B.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_SIMILARLST = sqlContext.sql(sql)
ACRM_F_CI_CUST_SIMILARLST.registerTempTable("ACRM_F_CI_CUST_SIMILARLST")
dfn="ACRM_F_CI_CUST_SIMILARLST/"+V_DT+".parquet"
ACRM_F_CI_CUST_SIMILARLST.cache()
nrows = ACRM_F_CI_CUST_SIMILARLST.count()
ACRM_F_CI_CUST_SIMILARLST.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_SIMILARLST.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_SIMILARLST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST/"+V_DT+".parquet /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST_BK/"+V_DT+".parquet")
