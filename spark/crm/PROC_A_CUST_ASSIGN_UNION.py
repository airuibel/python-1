#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CUST_ASSIGN_UNION').setMaster(sys.argv[2])
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

#清除数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/*.parquet")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_ORG/*.parquet")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/*.parquet")
#恢复数据到今日数据文件
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_BK/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet")


ADMIN_AUTH_ACCOUNT = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ACCOUNT/*')
ADMIN_AUTH_ACCOUNT.registerTempTable("ADMIN_AUTH_ACCOUNT")
OCRM_F_CI_BELONG_APPLY = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_APPLY/*')
OCRM_F_CI_BELONG_APPLY.registerTempTable("OCRM_F_CI_BELONG_APPLY")
OCRM_F_CI_SUN_BASEINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_BASEINFO/*')
OCRM_F_CI_SUN_BASEINFO.registerTempTable("OCRM_F_CI_SUN_BASEINFO")
OCRM_F_CI_LATENT_APPLY_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_LATENT_APPLY_INFO/*')
OCRM_F_CI_LATENT_APPLY_INFO.registerTempTable("OCRM_F_CI_LATENT_APPLY_INFO")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
OCRM_F_CI_TRANS_APPLY = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_TRANS_APPLY/*')
OCRM_F_CI_TRANS_APPLY.registerTempTable("OCRM_F_CI_TRANS_APPLY")
SYS_UNITS = sqlContext.read.parquet(hdfs+'/SYS_UNITS/*')
SYS_UNITS.registerTempTable("SYS_UNITS")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
ACRM_F_CI_CERT_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CERT_INFO/*')
ACRM_F_CI_CERT_INFO.registerTempTable("ACRM_F_CI_CERT_INFO")
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")


#任务[12] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.MGR_ID                AS MGR_ID 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.MAINTAIN_RIGHT        AS MAINTAIN_RIGHT 
       ,A.CHECK_RIGHT           AS CHECK_RIGHT 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.INSTITUTION           AS INSTITUTION 
       ,COALESCE(B.ORG_NAME, A.INSTITUTION_NAME)                       AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR A                             --归属经理表
   LEFT JOIN ADMIN_AUTH_ORG B                                  --机构表
     ON A.INSTITUTION           = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_INNTMP1")

OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.MGR_ID                                             --归属客户经理编号:src.MGR_ID
       ,DST.MAIN_TYPE                                          --主协办类型:src.MAIN_TYPE
       ,DST.MAINTAIN_RIGHT                                     --是否有维护权:src.MAINTAIN_RIGHT
       ,DST.CHECK_RIGHT                                        --是否有查看权:src.CHECK_RIGHT
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.INSTITUTION                                        --所属机构:src.INSTITUTION
       ,DST.INSTITUTION_NAME                                   --所属机构名称:src.INSTITUTION_NAME
       ,DST.MGR_NAME                                           --归属客户经理名称:src.MGR_NAME
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.ETL_DATE                                           --数据加工日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BELONG_CUSTMGR DST 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.MGR_ID              = DST.MGR_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2=OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.unionAll(OCRM_F_CI_BELONG_CUSTMGR_INNTMP1)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.cache()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.count()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.unpersist()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_BK/")

#任务[21] 001-02::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,MAIN_TYPE               AS MAIN_TYPE 
       ,INSTITUTION_NAME        AS INSTITUTION_NAME 
       ,INSTITUTION             AS INSTITUTION 
       ,FR_ID                   AS FR_ID 
       ,ROW_NUMBER() OVER(
      PARTITION BY CUST_ID 
               ,FR_ID 
               ,INSTITUTION 
               ,INSTITUTION_NAME 
          ORDER BY MAIN_TYPE)                       AS RANK 
   FROM OCRM_F_CI_BELONG_CUSTMGR A                             --归属经理表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_BELONG_CUSTMGR_01 = sqlContext.sql(sql)
TMP_OCRM_F_CI_BELONG_CUSTMGR_01.registerTempTable("TMP_OCRM_F_CI_BELONG_CUSTMGR_01")
dfn="TMP_OCRM_F_CI_BELONG_CUSTMGR_01/"+V_DT+".parquet"
TMP_OCRM_F_CI_BELONG_CUSTMGR_01.cache()
nrows = TMP_OCRM_F_CI_BELONG_CUSTMGR_01.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_BELONG_CUSTMGR_01/*.parquet")
TMP_OCRM_F_CI_BELONG_CUSTMGR_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_BELONG_CUSTMGR_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_BELONG_CUSTMGR_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-03::
V_STEP = V_STEP + 1

TMP_OCRM_F_CI_BELONG_CUSTMGR_01 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_BELONG_CUSTMGR_01/*')
TMP_OCRM_F_CI_BELONG_CUSTMGR_01.registerTempTable("TMP_OCRM_F_CI_BELONG_CUSTMGR_01")

sql = """
 SELECT CAST(monotonically_increasing_id() AS BIGINT)       AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION           AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,''                      AS ASSIGN_USER 
       ,'系统'                AS ASSIGN_USERNAME 
       ,V_DT                    AS ASSIGN_DATE 
       ,V_DT                    AS ETL_DATE 
       ,'9999-12-31'            AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TMP_OCRM_F_CI_BELONG_CUSTMGR_01 A                      --归属经理表临时表01
   LEFT JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID 
    AND B.INSTITUTION_CODE      = A.INSTITUTION 
  WHERE A.RANK                  = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_ORG_INNTMP1")

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.INSTITUTION_CODE                                   --归属机构代码:src.INSTITUTION_CODE
       ,DST.INSTITUTION_NAME                                   --归属机构名称:src.INSTITUTION_NAME
       ,DST.MAIN_TYPE                                          --主协办类型:src.MAIN_TYPE
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.ETL_DATE                                           --ETL日期:src.ETL_DATE
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.FR_ID                                              --0:src.FR_ID
   FROM OCRM_F_CI_BELONG_ORG DST 
   LEFT JOIN OCRM_F_CI_BELONG_ORG_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.MAIN_TYPE           = DST.MAIN_TYPE 
    AND SRC.INSTITUTION_CODE    = DST.INSTITUTION_CODE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_INNTMP2=OCRM_F_CI_BELONG_ORG_INNTMP2.unionAll(OCRM_F_CI_BELONG_ORG_INNTMP1)
OCRM_F_CI_BELONG_ORG_INNTMP1.cache()
OCRM_F_CI_BELONG_ORG_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_ORG_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_ORG_INNTMP2.count()
OCRM_F_CI_BELONG_ORG_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_INNTMP1.unpersist()
OCRM_F_CI_BELONG_ORG_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/")

#任务[21] 001-04::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")

sql = """
 SELECT DISTINCT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG A                                 --归属机构表
   LEFT JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.MAIN_TYPE             = B.MAIN_TYPE 
    AND A.INSTITUTION_CODE      = B.INSTITUTION_CODE 
    AND A.ID < B.ID 
   LEFT JOIN OCRM_F_CI_BELONG_ORG C                            --归属机构表
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND A.MAIN_TYPE             = '1' 
    AND C.MAIN_TYPE             = '1' 
    AND A.INSTITUTION_CODE <> C.INSTITUTION_CODE 
    AND A.ID < C.ID 
  WHERE B.CUST_ID IS NULL 
    AND C.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_BELONG_ORG_02 = sqlContext.sql(sql)
TMP_OCRM_F_CI_BELONG_ORG_02.registerTempTable("TMP_OCRM_F_CI_BELONG_ORG_02")
dfn="TMP_OCRM_F_CI_BELONG_ORG_02/"+V_DT+".parquet"
TMP_OCRM_F_CI_BELONG_ORG_02.cache()
nrows = TMP_OCRM_F_CI_BELONG_ORG_02.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_BELONG_ORG_02/*.parquet")
TMP_OCRM_F_CI_BELONG_ORG_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_BELONG_ORG_02.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_BELONG_ORG_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-05::
V_STEP = V_STEP + 1

TMP_OCRM_F_CI_BELONG_ORG_02 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_BELONG_ORG_02/*')
TMP_OCRM_F_CI_BELONG_ORG_02.registerTempTable("TMP_OCRM_F_CI_BELONG_ORG_02")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TMP_OCRM_F_CI_BELONG_ORG_02 A                          --客户归属机构表临时表02
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
dfn="OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG.cache()
nrows = OCRM_F_CI_BELONG_ORG.count()
OCRM_F_CI_BELONG_ORG.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-06::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")

sql = """
 SELECT A.ID                    AS ID 
       ,B.CUST_ID               AS CUST_ID 
       ,A.CERT_NO               AS CERT_NO 
       ,A.MGR_ID                AS MGR_ID 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_NAME              AS ORG_NAME 
       ,A.REMARKS               AS REMARKS 
       ,A.CREATE_USER           AS CREATE_USER 
       ,A.CREATE_DATE           AS CREATE_DATE 
       ,A.STATUS                AS STATUS 
       ,A.FR_ID                 AS FR_ID 
       ,A.CRM_DT                AS CRM_DT 
   FROM OCRM_F_CI_INPUT_BELONG_DATA A                          --分配数据导入表
  INNER JOIN(
         SELECT FR_ID 
               ,CERT_NO 
               ,MIN(CUST_ID)                       AS CUST_ID 
           FROM ACRM_F_CI_CERT_INFO 
          GROUP BY FR_ID,CERT_NO) B                                  --证件表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CERT_NO               = B.CERT_NO 
  WHERE A.CREATE_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1")

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.CERT_NO                                            --证件号:src.CERT_NO
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MGR_NAME                                           --经理名称:src.MGR_NAME
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.REMARKS                                            --处理结果：证件匹配客户失败;客户号重复;客户经理不存在:src.REMARKS
       ,DST.CREATE_USER                                        --创建人:src.CREATE_USER
       ,DST.CREATE_DATE                                        --创建日期:src.CREATE_DATE
       ,DST.STATUS                                             --成功标志(Y/N):src.STATUS
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --分配日期:src.CRM_DT
   FROM OCRM_F_CI_INPUT_BELONG_DATA DST 
   LEFT JOIN OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ID                  = DST.ID 
    AND SRC.CERT_NO             = DST.CERT_NO 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet"
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2=OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unionAll(OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.cache()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.cache()
nrowsi = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.count()
nrowsa = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.count()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.unpersist()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INPUT_BELONG_DATA lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/")

#任务[12] 001-07::
V_STEP = V_STEP + 1

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CERT_NO               AS CERT_NO 
       ,A.MGR_ID                AS MGR_ID 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_NAME              AS ORG_NAME 
       ,CASE WHEN CUST_ID IS NULL THEN '匹配客户失败' END                     AS REMARKS 
       ,A.CREATE_USER           AS CREATE_USER 
       ,A.CREATE_DATE           AS CREATE_DATE 
       ,CASE WHEN CUST_ID IS NULL THEN 'N' ELSE 'Y1' END                     AS STATUS 
       ,A.FR_ID                 AS FR_ID 
       ,A.CRM_DT                AS CRM_DT 
   FROM OCRM_F_CI_INPUT_BELONG_DATA A                          --分配数据导入表
  WHERE CREATE_DATE             = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1")

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.CERT_NO                                            --证件号:src.CERT_NO
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MGR_NAME                                           --经理名称:src.MGR_NAME
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.REMARKS                                            --处理结果：证件匹配客户失败;客户号重复;客户经理不存在:src.REMARKS
       ,DST.CREATE_USER                                        --创建人:src.CREATE_USER
       ,DST.CREATE_DATE                                        --创建日期:src.CREATE_DATE
       ,DST.STATUS                                             --成功标志(Y/N):src.STATUS
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --分配日期:src.CRM_DT
   FROM OCRM_F_CI_INPUT_BELONG_DATA DST 
   LEFT JOIN OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ID                  = DST.ID 
    AND SRC.CREATE_DATE         = DST.CREATE_DATE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet"
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2=OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unionAll(OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.cache()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.cache()
nrowsi = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.count()
nrowsa = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.count()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.unpersist()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INPUT_BELONG_DATA lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/")

#任务[12] 001-08::
V_STEP = V_STEP + 1

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CERT_NO               AS CERT_NO 
       ,A.MGR_ID                AS MGR_ID 
       ,B.USER_NAME             AS MGR_NAME 
       ,B.ORG_ID                AS ORG_ID 
       ,C.ORG_NAME              AS ORG_NAME 
       ,A.REMARKS               AS REMARKS 
       ,A.CREATE_USER           AS CREATE_USER 
       ,A.CREATE_DATE           AS CREATE_DATE 
       ,A.STATUS                AS STATUS 
       ,A.FR_ID                 AS FR_ID 
       ,A.CRM_DT                AS CRM_DT 
   FROM OCRM_F_CI_INPUT_BELONG_DATA A                          --分配数据导入表
  INNER JOIN ADMIN_AUTH_ACCOUNT B                              --账户表
     ON A.MGR_ID                = B.ACCOUNT_NAME 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN ADMIN_AUTH_ORG C                                  --机构表
     ON B.ORG_ID                = C.ORG_ID 
  WHERE A.CREATE_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1")

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.CERT_NO                                            --证件号:src.CERT_NO
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MGR_NAME                                           --经理名称:src.MGR_NAME
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.REMARKS                                            --处理结果：证件匹配客户失败;客户号重复;客户经理不存在:src.REMARKS
       ,DST.CREATE_USER                                        --创建人:src.CREATE_USER
       ,DST.CREATE_DATE                                        --创建日期:src.CREATE_DATE
       ,DST.STATUS                                             --成功标志(Y/N):src.STATUS
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --分配日期:src.CRM_DT
   FROM OCRM_F_CI_INPUT_BELONG_DATA DST 
   LEFT JOIN OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ID                  = DST.ID 
    AND SRC.CERT_NO             = DST.CERT_NO 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet"
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2=OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unionAll(OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.cache()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.cache()
nrowsi = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.count()
nrowsa = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.count()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.unpersist()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INPUT_BELONG_DATA lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/")

#任务[12] 001-09::
V_STEP = V_STEP + 1

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")

sql = """
 SELECT M.ID                    AS ID 
       ,M.CUST_ID               AS CUST_ID 
       ,M.CERT_NO               AS CERT_NO 
       ,M.MGR_ID                AS MGR_ID 
       ,M.MGR_NAME              AS MGR_NAME 
       ,M.ORG_ID                AS ORG_ID 
       ,M.ORG_NAME              AS ORG_NAME 
       ,'客户号重复'       AS REMARKS 
       ,M.CREATE_USER           AS CREATE_USER 
       ,M.CREATE_DATE           AS CREATE_DATE 
       ,'N'                     AS STATUS 
       ,M.FR_ID                 AS FR_ID 
       ,M.CRM_DT                AS CRM_DT 
   FROM OCRM_F_CI_INPUT_BELONG_DATA M                          --分配数据导入表
  INNER JOIN OCRM_F_CI_INPUT_BELONG_DATA N                     --分配数据导入表
     ON N.CREATE_DATE           = V_DT 
    AND N.STATUS                = 'Y1' 
    AND M.CUST_ID               = N.CUST_ID 
    AND M.ID < N.ID 
  WHERE M.CREATE_DATE           = V_DT 
    AND M.STATUS                = 'Y1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1")

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.CERT_NO                                            --证件号:src.CERT_NO
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MGR_NAME                                           --经理名称:src.MGR_NAME
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.REMARKS                                            --处理结果：证件匹配客户失败;客户号重复;客户经理不存在:src.REMARKS
       ,DST.CREATE_USER                                        --创建人:src.CREATE_USER
       ,DST.CREATE_DATE                                        --创建日期:src.CREATE_DATE
       ,DST.STATUS                                             --成功标志(Y/N):src.STATUS
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --分配日期:src.CRM_DT
   FROM OCRM_F_CI_INPUT_BELONG_DATA DST 
   LEFT JOIN OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ID                  = DST.ID 
    AND SRC.CREATE_DATE         = DST.CREATE_DATE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet"
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2=OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unionAll(OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.cache()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.cache()
nrowsi = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.count()
nrowsa = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.count()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.unpersist()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INPUT_BELONG_DATA lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/")

#任务[12] 001-10::
V_STEP = V_STEP + 1

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")

sql = """
 SELECT M.ID                    AS ID 
       ,M.CUST_ID               AS CUST_ID 
       ,M.CERT_NO               AS CERT_NO 
       ,M.MGR_ID                AS MGR_ID 
       ,M.MGR_NAME              AS MGR_NAME 
       ,M.ORG_ID                AS ORG_ID 
       ,M.ORG_NAME              AS ORG_NAME 
       ,'匹配客户经理失败'      AS REMARKS 
       ,M.CREATE_USER           AS CREATE_USER 
       ,M.CREATE_DATE           AS CREATE_DATE 
       ,'N'                     AS STATUS 
       ,M.FR_ID                 AS FR_ID 
       ,M.CRM_DT                AS CRM_DT 
   FROM OCRM_F_CI_INPUT_BELONG_DATA M                          --分配数据导入表
  WHERE M.CREATE_DATE           = V_DT 
    AND M.STATUS                = 'Y1' 
    AND M.MGR_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1")

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.CERT_NO                                            --证件号:src.CERT_NO
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MGR_NAME                                           --经理名称:src.MGR_NAME
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.REMARKS                                            --处理结果：证件匹配客户失败;客户号重复;客户经理不存在:src.REMARKS
       ,DST.CREATE_USER                                        --创建人:src.CREATE_USER
       ,DST.CREATE_DATE                                        --创建日期:src.CREATE_DATE
       ,DST.STATUS                                             --成功标志(Y/N):src.STATUS
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --分配日期:src.CRM_DT
   FROM OCRM_F_CI_INPUT_BELONG_DATA DST 
   LEFT JOIN OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ID                  = DST.ID 
    AND SRC.CREATE_DATE         = DST.CREATE_DATE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet"
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2=OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unionAll(OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.cache()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.cache()
nrowsi = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.count()
nrowsa = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.count()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.unpersist()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INPUT_BELONG_DATA lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/")

#任务[12] 001-11::
V_STEP = V_STEP + 1

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")

sql = """
 SELECT M.ID                    AS ID 
       ,M.CUST_ID               AS CUST_ID 
       ,M.CERT_NO               AS CERT_NO 
       ,M.MGR_ID                AS MGR_ID 
       ,M.MGR_NAME              AS MGR_NAME 
       ,M.ORG_ID                AS ORG_ID 
       ,M.ORG_NAME              AS ORG_NAME 
       ,'正在分配流程中' AS REMARKS 
       ,M.CREATE_USER           AS CREATE_USER 
       ,M.CREATE_DATE           AS CREATE_DATE 
       ,'N'                     AS STATUS 
       ,M.FR_ID                 AS FR_ID 
       ,M.CRM_DT                AS CRM_DT 
   FROM OCRM_F_CI_INPUT_BELONG_DATA M                          --分配数据导入表
  INNER JOIN OCRM_F_CI_BELONG_APPLY N                          --分配申请表
     ON M.FR_ID                 = N.FR_ID 
    AND M.CUST_ID               = N.CUST_ID 
    AND N.STATUS                = '1' 
  WHERE M.CREATE_DATE           = V_DT 
    AND M.STATUS                = 'Y1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1")

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.CERT_NO                                            --证件号:src.CERT_NO
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MGR_NAME                                           --经理名称:src.MGR_NAME
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.REMARKS                                            --处理结果：证件匹配客户失败;客户号重复;客户经理不存在:src.REMARKS
       ,DST.CREATE_USER                                        --创建人:src.CREATE_USER
       ,DST.CREATE_DATE                                        --创建日期:src.CREATE_DATE
       ,DST.STATUS                                             --成功标志(Y/N):src.STATUS
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --分配日期:src.CRM_DT
   FROM OCRM_F_CI_INPUT_BELONG_DATA DST 
   LEFT JOIN OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ID                  = DST.ID 
    AND SRC.CREATE_DATE         = DST.CREATE_DATE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet"
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2=OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unionAll(OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.cache()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.cache()
nrowsi = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.count()
nrowsa = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.count()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.unpersist()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INPUT_BELONG_DATA lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/")

#任务[12] 001-12::
V_STEP = V_STEP + 1

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")

sql = """
 SELECT M.ID                    AS ID 
       ,M.CUST_ID               AS CUST_ID 
       ,M.CERT_NO               AS CERT_NO 
       ,M.MGR_ID                AS MGR_ID 
       ,M.MGR_NAME              AS MGR_NAME 
       ,M.ORG_ID                AS ORG_ID 
       ,M.ORG_NAME              AS ORG_NAME 
       ,'正在移交流程中' AS REMARKS 
       ,M.CREATE_USER           AS CREATE_USER 
       ,M.CREATE_DATE           AS CREATE_DATE 
       ,'N'                     AS STATUS 
       ,M.FR_ID                 AS FR_ID 
       ,M.CRM_DT                AS CRM_DT 
   FROM OCRM_F_CI_INPUT_BELONG_DATA M                          --分配数据导入表
  INNER JOIN OCRM_F_CI_TRANS_APPLY N                           --移交申请表
     ON M.FR_ID                 = N.FR_ID 
    AND M.CUST_ID               = N.CUST_ID 
    AND N.APPROVE_STAT          = '1' 
  WHERE M.CREATE_DATE           = V_DT 
    AND M.STATUS                = 'Y1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1")

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.CERT_NO                                            --证件号:src.CERT_NO
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MGR_NAME                                           --经理名称:src.MGR_NAME
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.REMARKS                                            --处理结果：证件匹配客户失败;客户号重复;客户经理不存在:src.REMARKS
       ,DST.CREATE_USER                                        --创建人:src.CREATE_USER
       ,DST.CREATE_DATE                                        --创建日期:src.CREATE_DATE
       ,DST.STATUS                                             --成功标志(Y/N):src.STATUS
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --分配日期:src.CRM_DT
   FROM OCRM_F_CI_INPUT_BELONG_DATA DST 
   LEFT JOIN OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ID                  = DST.ID 
    AND SRC.CREATE_DATE         = DST.CREATE_DATE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet"
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2=OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unionAll(OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.cache()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.cache()
nrowsi = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.count()
nrowsa = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.count()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.unpersist()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INPUT_BELONG_DATA lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/")

#任务[12] 001-13::
V_STEP = V_STEP + 1

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")

sql = """
 SELECT M.ID                    AS ID 
       ,M.CUST_ID               AS CUST_ID 
       ,M.CERT_NO               AS CERT_NO 
       ,M.MGR_ID                AS MGR_ID 
       ,M.MGR_NAME              AS MGR_NAME 
       ,M.ORG_ID                AS ORG_ID 
       ,M.ORG_NAME              AS ORG_NAME 
       ,'正在认领流程中' AS REMARKS 
       ,M.CREATE_USER           AS CREATE_USER 
       ,M.CREATE_DATE           AS CREATE_DATE 
       ,'N'                     AS STATUS 
       ,M.FR_ID                 AS FR_ID 
       ,M.CRM_DT                AS CRM_DT 
   FROM OCRM_F_CI_INPUT_BELONG_DATA M                          --分配数据导入表
  INNER JOIN OCRM_F_CI_LATENT_APPLY_INFO N                     --认领申请表
     ON M.FR_ID                 = N.FR_ID 
    AND M.CUST_ID               = N.CUST_ID 
    AND N.APPROVEL_STATUS       = '1' 
  WHERE M.CREATE_DATE           = V_DT 
    AND M.STATUS                = 'Y1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1")

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.CERT_NO                                            --证件号:src.CERT_NO
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MGR_NAME                                           --经理名称:src.MGR_NAME
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.REMARKS                                            --处理结果：证件匹配客户失败;客户号重复;客户经理不存在:src.REMARKS
       ,DST.CREATE_USER                                        --创建人:src.CREATE_USER
       ,DST.CREATE_DATE                                        --创建日期:src.CREATE_DATE
       ,DST.STATUS                                             --成功标志(Y/N):src.STATUS
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --分配日期:src.CRM_DT
   FROM OCRM_F_CI_INPUT_BELONG_DATA DST 
   LEFT JOIN OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ID                  = DST.ID 
    AND SRC.CREATE_DATE         = DST.CREATE_DATE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet"
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2=OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unionAll(OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.cache()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.cache()
nrowsi = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.count()
nrowsa = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.count()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.unpersist()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INPUT_BELONG_DATA lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/")

#任务[12] 001-14::
V_STEP = V_STEP + 1

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CERT_NO               AS CERT_NO 
       ,A.MGR_ID                AS MGR_ID 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_NAME              AS ORG_NAME 
       ,A.REMARKS               AS REMARKS 
       ,A.CREATE_USER           AS CREATE_USER 
       ,A.CREATE_DATE           AS CREATE_DATE 
       ,'Y'                     AS STATUS 
       ,A.FR_ID                 AS FR_ID 
       ,A.CRM_DT                AS CRM_DT 
   FROM OCRM_F_CI_INPUT_BELONG_DATA A                          --分配数据导入表
  INNER JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID 
  INNER JOIN SYS_UNITS C                                       --机构层级表
     ON B.INSTITUTION_CODE      = C.UNITID 
    AND LOCATE(A.ORG_ID, C.UNITSEQ) > 0 
    AND B.FR_ID                 = C.FR_ID 
  WHERE A.CREATE_DATE           = V_DT 
    AND A.STATUS                = 'Y1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1")

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.CERT_NO                                            --证件号:src.CERT_NO
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MGR_NAME                                           --经理名称:src.MGR_NAME
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.REMARKS                                            --处理结果：证件匹配客户失败;客户号重复;客户经理不存在:src.REMARKS
       ,DST.CREATE_USER                                        --创建人:src.CREATE_USER
       ,DST.CREATE_DATE                                        --创建日期:src.CREATE_DATE
       ,DST.STATUS                                             --成功标志(Y/N):src.STATUS
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --分配日期:src.CRM_DT
   FROM OCRM_F_CI_INPUT_BELONG_DATA DST 
   LEFT JOIN OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ID                  = DST.ID 
    AND SRC.CREATE_DATE         = DST.CREATE_DATE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet"
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2=OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unionAll(OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.cache()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.cache()
nrowsi = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.count()
nrowsa = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.count()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.unpersist()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INPUT_BELONG_DATA lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/")

#任务[12] 001-15::
V_STEP = V_STEP + 1

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")

sql = """
 SELECT M.ID                    AS ID 
       ,M.CUST_ID               AS CUST_ID 
       ,M.CERT_NO               AS CERT_NO 
       ,M.MGR_ID                AS MGR_ID 
       ,M.MGR_NAME              AS MGR_NAME 
       ,M.ORG_ID                AS ORG_ID 
       ,M.ORG_NAME              AS ORG_NAME 
       ,'客户不属于经理管辖机构'     AS REMARKS 
       ,M.CREATE_USER           AS CREATE_USER 
       ,M.CREATE_DATE           AS CREATE_DATE 
       ,'N'                     AS STATUS 
       ,M.FR_ID                 AS FR_ID 
       ,M.CRM_DT                AS CRM_DT 
   FROM OCRM_F_CI_INPUT_BELONG_DATA M                          --分配数据导入表
  WHERE M.CREATE_DATE           = V_DT 
    AND M.STATUS                = 'Y1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1")

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.CERT_NO                                            --证件号:src.CERT_NO
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MGR_NAME                                           --经理名称:src.MGR_NAME
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.REMARKS                                            --处理结果：证件匹配客户失败;客户号重复;客户经理不存在:src.REMARKS
       ,DST.CREATE_USER                                        --创建人:src.CREATE_USER
       ,DST.CREATE_DATE                                        --创建日期:src.CREATE_DATE
       ,DST.STATUS                                             --成功标志(Y/N):src.STATUS
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --分配日期:src.CRM_DT
   FROM OCRM_F_CI_INPUT_BELONG_DATA DST 
   LEFT JOIN OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ID                  = DST.ID 
    AND SRC.CREATE_DATE         = DST.CREATE_DATE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet"
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2=OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unionAll(OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.cache()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.cache()
nrowsi = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.count()
nrowsa = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.count()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.unpersist()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INPUT_BELONG_DATA lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/")

#任务[12] 001-16::
V_STEP = V_STEP + 1

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")

sql = """
 SELECT M.ID                    AS ID 
       ,M.CUST_ID               AS CUST_ID 
       ,M.MGR_ID                AS MGR_ID 
       ,'2'                     AS MAIN_TYPE 
       ,M.MAINTAIN_RIGHT        AS MAINTAIN_RIGHT 
       ,M.CHECK_RIGHT           AS CHECK_RIGHT 
       ,''                      AS ASSIGN_USER 
       ,'批量分配覆盖'    AS ASSIGN_USERNAME 
       ,M.ASSIGN_DATE           AS ASSIGN_DATE 
       ,M.INSTITUTION           AS INSTITUTION 
       ,M.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,M.MGR_NAME              AS MGR_NAME 
       ,M.EFF_DATE              AS EFF_DATE 
       ,M.ETL_DATE              AS ETL_DATE 
       ,M.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR M                             --归属经理表
  INNER JOIN OCRM_F_CI_INPUT_BELONG_DATA N                     --分配数据导入表
     ON M.FR_ID                 = N.FR_ID 
    AND M.CUST_ID               = N.CUST_ID 
    AND M.MGR_ID <> N.MGR_ID 
    AND N.CREATE_DATE           = V_DT 
    AND N.STATUS                = 'Y' 
  WHERE M.MAIN_TYPE             = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_INNTMP1")

OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.MGR_ID                                             --归属客户经理编号:src.MGR_ID
       ,DST.MAIN_TYPE                                          --主协办类型:src.MAIN_TYPE
       ,DST.MAINTAIN_RIGHT                                     --是否有维护权:src.MAINTAIN_RIGHT
       ,DST.CHECK_RIGHT                                        --是否有查看权:src.CHECK_RIGHT
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.INSTITUTION                                        --所属机构:src.INSTITUTION
       ,DST.INSTITUTION_NAME                                   --所属机构名称:src.INSTITUTION_NAME
       ,DST.MGR_NAME                                           --归属客户经理名称:src.MGR_NAME
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.ETL_DATE                                           --数据加工日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BELONG_CUSTMGR DST 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.MGR_ID              = DST.MGR_ID 
    AND SRC.MAIN_TYPE           = DST.MAIN_TYPE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2=OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.unionAll(OCRM_F_CI_BELONG_CUSTMGR_INNTMP1)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.cache()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.count()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.unpersist()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_BK/")

#任务[12] 001-17::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")

sql = """
 SELECT COALESCE(M.ID, CAST(monotonically_increasing_id() AS INTEGER))                       AS ID 
       ,COALESCE(M.CUST_ID, N.CUST_ID)                       AS CUST_ID 
       ,COALESCE(M.MGR_ID, N.MGR_ID)                       AS MGR_ID 
       ,'1'                     AS MAIN_TYPE 
       ,M.MAINTAIN_RIGHT        AS MAINTAIN_RIGHT 
       ,M.CHECK_RIGHT           AS CHECK_RIGHT 
       ,''                      AS ASSIGN_USER 
       ,'批量分配'          AS ASSIGN_USERNAME 
       ,V_DT                    AS ASSIGN_DATE 
       ,COALESCE(M.INSTITUTION, N.ORG_ID)                       AS INSTITUTION 
       ,COALESCE(M.INSTITUTION_NAME, N.ORG_NAME)                       AS INSTITUTION_NAME 
       ,COALESCE(M.MGR_NAME, N.MGR_NAME)                       AS MGR_NAME 
       ,M.EFF_DATE              AS EFF_DATE 
       ,V_DT                    AS ETL_DATE 
       ,N.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_INPUT_BELONG_DATA N                          --分配数据导入表
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR M                        --归属经理表
     ON M.FR_ID                 = N.FR_ID 
    AND M.CUST_ID               = N.CUST_ID 
    AND M.MGR_ID                = N.MGR_ID 
    AND N.CREATE_DATE           = V_DT 
    AND N.STATUS                = 'Y' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_INNTMP1")

OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.MGR_ID                                             --归属客户经理编号:src.MGR_ID
       ,DST.MAIN_TYPE                                          --主协办类型:src.MAIN_TYPE
       ,DST.MAINTAIN_RIGHT                                     --是否有维护权:src.MAINTAIN_RIGHT
       ,DST.CHECK_RIGHT                                        --是否有查看权:src.CHECK_RIGHT
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.INSTITUTION                                        --所属机构:src.INSTITUTION
       ,DST.INSTITUTION_NAME                                   --所属机构名称:src.INSTITUTION_NAME
       ,DST.MGR_NAME                                           --归属客户经理名称:src.MGR_NAME
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.ETL_DATE                                           --数据加工日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BELONG_CUSTMGR DST 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.MGR_ID              = DST.MGR_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2=OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.unionAll(OCRM_F_CI_BELONG_CUSTMGR_INNTMP1)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.cache()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.count()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.unpersist()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_BK/")

#任务[12] 001-18::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")

sql = """
 SELECT M.ID                    AS ID 
       ,M.CUST_ID               AS CUST_ID 
       ,M.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,M.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,'2'                     AS MAIN_TYPE 
       ,''                      AS ASSIGN_USER 
       ,'批量分配覆盖'    AS ASSIGN_USERNAME 
       ,M.ASSIGN_DATE           AS ASSIGN_DATE 
       ,M.ETL_DATE              AS ETL_DATE 
       ,M.EFF_DATE              AS EFF_DATE 
       ,M.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG M                                 --归属机构表
  INNER JOIN OCRM_F_CI_INPUT_BELONG_DATA N                     --分配数据导入表
     ON M.FR_ID                 = N.FR_ID 
    AND M.CUST_ID               = N.CUST_ID 
    AND M.INSTITUTION_CODE <> N.ORG_ID 
    AND N.CREATE_DATE           = V_DT 
    AND N.STATUS                = 'Y' 
  WHERE M.MAIN_TYPE             = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_ORG_INNTMP1")

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.INSTITUTION_CODE                                   --归属机构代码:src.INSTITUTION_CODE
       ,DST.INSTITUTION_NAME                                   --归属机构名称:src.INSTITUTION_NAME
       ,DST.MAIN_TYPE                                          --主协办类型:src.MAIN_TYPE
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.ETL_DATE                                           --ETL日期:src.ETL_DATE
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.FR_ID                                              --0:src.FR_ID
   FROM OCRM_F_CI_BELONG_ORG DST 
   LEFT JOIN OCRM_F_CI_BELONG_ORG_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.INSTITUTION_CODE    = DST.INSTITUTION_CODE 
    AND SRC.MAIN_TYPE           = DST.MAIN_TYPE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_INNTMP2=OCRM_F_CI_BELONG_ORG_INNTMP2.unionAll(OCRM_F_CI_BELONG_ORG_INNTMP1)
OCRM_F_CI_BELONG_ORG_INNTMP1.cache()
OCRM_F_CI_BELONG_ORG_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_ORG_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_ORG_INNTMP2.count()
OCRM_F_CI_BELONG_ORG_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_INNTMP1.unpersist()
OCRM_F_CI_BELONG_ORG_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/")

#任务[12] 001-19::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")

sql = """
 SELECT COALESCE(M.ID, CAST(monotonically_increasing_id() AS INTEGER))                       AS ID 
       ,COALESCE(M.CUST_ID, N.CUST_ID)                       AS CUST_ID 
       ,COALESCE(M.INSTITUTION_CODE, N.ORG_ID)                       AS INSTITUTION_CODE 
       ,COALESCE(M.INSTITUTION_NAME, N.ORG_NAME)                       AS INSTITUTION_NAME 
       ,'1'                     AS MAIN_TYPE 
       ,''                      AS ASSIGN_USER 
       ,'批量分配'          AS ASSIGN_USERNAME 
       ,V_DT                    AS ASSIGN_DATE 
       ,V_DT                    AS ETL_DATE 
       ,M.EFF_DATE              AS EFF_DATE 
       ,N.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_INPUT_BELONG_DATA N                          --分配数据导入表
  INNER JOIN OCRM_F_CI_BELONG_ORG M                            --归属机构表
     ON M.FR_ID                 = N.FR_ID 
    AND M.CUST_ID               = N.CUST_ID 
    AND M.INSTITUTION_CODE      = N.ORG_ID 
    AND N.CREATE_DATE           = V_DT 
    AND N.STATUS                = 'Y' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_ORG_INNTMP1")

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.INSTITUTION_CODE                                   --归属机构代码:src.INSTITUTION_CODE
       ,DST.INSTITUTION_NAME                                   --归属机构名称:src.INSTITUTION_NAME
       ,DST.MAIN_TYPE                                          --主协办类型:src.MAIN_TYPE
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.ETL_DATE                                           --ETL日期:src.ETL_DATE
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.FR_ID                                              --0:src.FR_ID
   FROM OCRM_F_CI_BELONG_ORG DST 
   LEFT JOIN OCRM_F_CI_BELONG_ORG_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.INSTITUTION_CODE    = DST.INSTITUTION_CODE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_INNTMP2=OCRM_F_CI_BELONG_ORG_INNTMP2.unionAll(OCRM_F_CI_BELONG_ORG_INNTMP1)
OCRM_F_CI_BELONG_ORG_INNTMP1.cache()
OCRM_F_CI_BELONG_ORG_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_ORG_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_ORG_INNTMP2.count()
OCRM_F_CI_BELONG_ORG_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_INNTMP1.unpersist()
OCRM_F_CI_BELONG_ORG_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/")

#任务[12] 001-20::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")

sql = """
 SELECT M.ID                    AS ID 
       ,M.CUST_ID               AS CUST_ID 
       ,M.CERT_NO               AS CERT_NO 
       ,M.MGR_ID                AS MGR_ID 
       ,M.MGR_NAME              AS MGR_NAME 
       ,M.ORG_ID                AS ORG_ID 
       ,M.ORG_NAME              AS ORG_NAME 
       ,M.REMARKS               AS REMARKS 
       ,M.CREATE_USER           AS CREATE_USER 
       ,M.CREATE_DATE           AS CREATE_DATE 
       ,M.STATUS                AS STATUS 
       ,M.FR_ID                 AS FR_ID 
       ,V_DT                    AS CRM_DT 
   FROM OCRM_F_CI_INPUT_BELONG_DATA M                          --分配数据导入表
  WHERE CREATE_DATE             = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1")

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.CERT_NO                                            --证件号:src.CERT_NO
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MGR_NAME                                           --经理名称:src.MGR_NAME
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.REMARKS                                            --处理结果：证件匹配客户失败;客户号重复;客户经理不存在:src.REMARKS
       ,DST.CREATE_USER                                        --创建人:src.CREATE_USER
       ,DST.CREATE_DATE                                        --创建日期:src.CREATE_DATE
       ,DST.STATUS                                             --成功标志(Y/N):src.STATUS
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --分配日期:src.CRM_DT
   FROM OCRM_F_CI_INPUT_BELONG_DATA DST 
   LEFT JOIN OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.ID                  = DST.ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet"
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2=OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unionAll(OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1)
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.cache()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.cache()
nrowsi = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.count()
nrowsa = OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.count()
#装载数据
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/"+V_DT+".parquet")


OCRM_F_CI_INPUT_BELONG_DATA_INNTMP1.unpersist()
OCRM_F_CI_INPUT_BELONG_DATA_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_INPUT_BELONG_DATA lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_INPUT_BELONG_DATA_BK/")

#任务[21] 001-21::
V_STEP = V_STEP + 1

OCRM_F_CI_INPUT_BELONG_DATA = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_INPUT_BELONG_DATA/*')
OCRM_F_CI_INPUT_BELONG_DATA.registerTempTable("OCRM_F_CI_INPUT_BELONG_DATA")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_NAME 
       ,A.CUST_TYP              AS CUST_TYP 
       ,B.INSTITUTION_CODE      AS ORG_ID 
       ,B.INSTITUTION_NAME      AS ORG_NAME 
       ,B.MAIN_TYPE             AS O_MAIN_TYPE 
       ,C.MGR_ID                AS MGR_ID 
       ,C.MGR_NAME              AS MGR_NAME 
       ,C.MAIN_TYPE             AS M_MAIN_TYPE 
       ,V_DT                    AS ETL_DATE 
       ,A.OBJ_DATE              AS OBJ_DATE 
       ,A.OBJ_NAME              AS OBJ_NAME 
       ,A.OBJ_RATING            AS OBJ_RATING 
       ,A.FR_ID                 AS FR_ID 
       ,''                      AS OLD_OBJ_NAME 
       ,''                      AS OLD_OBJ_RATING 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
   LEFT JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND B.MAIN_TYPE             = '1' 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR C                        --归属经理表
     ON A.CUST_ID               = C.CUST_ID 
    AND C.MAIN_TYPE             = '1' 
    AND C.FR_ID                 = A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CUST_ORG_MGR = sqlContext.sql(sql)
OCRM_F_CUST_ORG_MGR.registerTempTable("OCRM_F_CUST_ORG_MGR")
dfn="OCRM_F_CUST_ORG_MGR/"+V_DT+".parquet"
OCRM_F_CUST_ORG_MGR.cache()
nrows = OCRM_F_CUST_ORG_MGR.count()
OCRM_F_CUST_ORG_MGR.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CUST_ORG_MGR.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CUST_ORG_MGR/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CUST_ORG_MGR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-22::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,B.ACCOUNT_NAME          AS MGR_ID 
       ,B.USER_NAME             AS MGR_NAME 
       ,B.ORG_ID                AS ORG_ID 
       ,C.ORG_NAME              AS ORG_NAME 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_SUN_BASEINFO A                               --阳光信贷农户表
  INNER JOIN ADMIN_AUTH_ACCOUNT B                              --账户表
     ON A.MANAGEUSERID          = B.ACCOUNT_NAME 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN ADMIN_AUTH_ORG C                                  --机构表
     ON B.ORG_ID                = C.ORG_ID 
  WHERE A.CRM_DT                = V_DT 
    AND A.CUST_ID LIKE '1%' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_SUN_BASEINFO_01 = sqlContext.sql(sql)
TMP_OCRM_F_CI_SUN_BASEINFO_01.registerTempTable("TMP_OCRM_F_CI_SUN_BASEINFO_01")
dfn="TMP_OCRM_F_CI_SUN_BASEINFO_01/"+V_DT+".parquet"
TMP_OCRM_F_CI_SUN_BASEINFO_01.cache()
nrows = TMP_OCRM_F_CI_SUN_BASEINFO_01.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_SUN_BASEINFO_01/*.parquet")
TMP_OCRM_F_CI_SUN_BASEINFO_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_SUN_BASEINFO_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_SUN_BASEINFO_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-23::
V_STEP = V_STEP + 1

TMP_OCRM_F_CI_SUN_BASEINFO_01 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_SUN_BASEINFO_01/*')
TMP_OCRM_F_CI_SUN_BASEINFO_01.registerTempTable("TMP_OCRM_F_CI_SUN_BASEINFO_01")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,'2'                     AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG A                                 --归属机构表
  INNER JOIN TMP_OCRM_F_CI_SUN_BASEINFO_01 B                   --阳光信贷导入客户分配
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.MAIN_TYPE             = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_ORG_INNTMP1")

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.INSTITUTION_CODE                                   --归属机构代码:src.INSTITUTION_CODE
       ,DST.INSTITUTION_NAME                                   --归属机构名称:src.INSTITUTION_NAME
       ,DST.MAIN_TYPE                                          --主协办类型:src.MAIN_TYPE
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.ETL_DATE                                           --ETL日期:src.ETL_DATE
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BELONG_ORG DST 
   LEFT JOIN OCRM_F_CI_BELONG_ORG_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_INNTMP2=OCRM_F_CI_BELONG_ORG_INNTMP2.unionAll(OCRM_F_CI_BELONG_ORG_INNTMP1)
OCRM_F_CI_BELONG_ORG_INNTMP1.cache()
OCRM_F_CI_BELONG_ORG_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_ORG_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_ORG_INNTMP2.count()
OCRM_F_CI_BELONG_ORG_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_INNTMP1.unpersist()
OCRM_F_CI_BELONG_ORG_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/")

#任务[12] 001-24::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")

sql = """
 SELECT COALESCE(B.ID, CAST(monotonically_increasing_id() AS INTEGER))                       AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.ORG_ID                AS INSTITUTION_CODE 
       ,A.ORG_NAME              AS INSTITUTION_NAME 
       ,'1'                     AS MAIN_TYPE 
       ,''                      AS ASSIGN_USER 
       ,'系统'                AS ASSIGN_USERNAME 
       ,COALESCE(B.ASSIGN_DATE, V_DT)                       AS ASSIGN_DATE 
       ,V_DT                    AS ETL_DATE 
       ,'9999-12-31'            AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TMP_OCRM_F_CI_SUN_BASEINFO_01 A                        --阳光信贷导入客户分配
   LEFT JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND B.INSTITUTION_CODE      = A.ORG_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_ORG_INNTMP1")

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.INSTITUTION_CODE                                   --归属机构代码:src.INSTITUTION_CODE
       ,DST.INSTITUTION_NAME                                   --归属机构名称:src.INSTITUTION_NAME
       ,DST.MAIN_TYPE                                          --主协办类型:src.MAIN_TYPE
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.ETL_DATE                                           --ETL日期:src.ETL_DATE
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BELONG_ORG DST 
   LEFT JOIN OCRM_F_CI_BELONG_ORG_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.INSTITUTION_CODE    = DST.INSTITUTION_CODE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_INNTMP2=OCRM_F_CI_BELONG_ORG_INNTMP2.unionAll(OCRM_F_CI_BELONG_ORG_INNTMP1)
OCRM_F_CI_BELONG_ORG_INNTMP1.cache()
OCRM_F_CI_BELONG_ORG_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_ORG_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_ORG_INNTMP2.count()
#装载数据
OCRM_F_CI_BELONG_ORG_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/"+V_DT+".parquet")


OCRM_F_CI_BELONG_ORG_INNTMP1.unpersist()
OCRM_F_CI_BELONG_ORG_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/")

#任务[12] 001-25::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.MGR_ID                AS MGR_ID 
       ,'2'                     AS MAIN_TYPE 
       ,A.MAINTAIN_RIGHT        AS MAINTAIN_RIGHT 
       ,A.CHECK_RIGHT           AS CHECK_RIGHT 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.INSTITUTION           AS INSTITUTION 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR A                             --归属经理表
  INNER JOIN TMP_OCRM_F_CI_SUN_BASEINFO_01 B                   --阳光信贷导入客户分配
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.MAIN_TYPE             = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_INNTMP1")

OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.MGR_ID                                             --归属客户经理编号:src.MGR_ID
       ,DST.MAIN_TYPE                                          --主协办类型:src.MAIN_TYPE
       ,DST.MAINTAIN_RIGHT                                     --是否有维护权:src.MAINTAIN_RIGHT
       ,DST.CHECK_RIGHT                                        --是否有查看权:src.CHECK_RIGHT
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.INSTITUTION                                        --所属机构:src.INSTITUTION
       ,DST.INSTITUTION_NAME                                   --所属机构名称:src.INSTITUTION_NAME
       ,DST.MGR_NAME                                           --归属客户经理名称:src.MGR_NAME
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.ETL_DATE                                           --数据加工日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BELONG_CUSTMGR DST 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2=OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.unionAll(OCRM_F_CI_BELONG_CUSTMGR_INNTMP1)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.cache()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.count()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.unpersist()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_BK/")

#任务[12] 001-26::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")

sql = """
 SELECT COALESCE(B.ID, CAST(monotonically_increasing_id() AS INTEGER))                       AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.MGR_ID                AS MGR_ID 
       ,'1'                     AS MAIN_TYPE 
       ,B.MAINTAIN_RIGHT        AS MAINTAIN_RIGHT 
       ,B.CHECK_RIGHT           AS CHECK_RIGHT 
       ,''                      AS ASSIGN_USER 
       ,'系统'                AS ASSIGN_USERNAME 
       ,V_DT                    AS ASSIGN_DATE 
       ,A.ORG_ID                AS INSTITUTION 
       ,A.ORG_NAME              AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,'9999-12-31'            AS EFF_DATE 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TMP_OCRM_F_CI_SUN_BASEINFO_01 A                        --阳光信贷导入客户分配
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --归属经理表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.MGR_ID                = B.MGR_ID 
  WHERE B.MAIN_TYPE             = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_INNTMP1")

OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.MGR_ID                                             --归属客户经理编号:src.MGR_ID
       ,DST.MAIN_TYPE                                          --主协办类型:src.MAIN_TYPE
       ,DST.MAINTAIN_RIGHT                                     --是否有维护权:src.MAINTAIN_RIGHT
       ,DST.CHECK_RIGHT                                        --是否有查看权:src.CHECK_RIGHT
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.INSTITUTION                                        --所属机构:src.INSTITUTION
       ,DST.INSTITUTION_NAME                                   --所属机构名称:src.INSTITUTION_NAME
       ,DST.MGR_NAME                                           --归属客户经理名称:src.MGR_NAME
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.ETL_DATE                                           --数据加工日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BELONG_CUSTMGR DST 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.MGR_ID              = DST.MGR_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2=OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.unionAll(OCRM_F_CI_BELONG_CUSTMGR_INNTMP1)
OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.cache()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.count()

#装载数据
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_BK/"+V_DT+".parquet")

OCRM_F_CI_BELONG_CUSTMGR_INNTMP1.unpersist()
OCRM_F_CI_BELONG_CUSTMGR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
