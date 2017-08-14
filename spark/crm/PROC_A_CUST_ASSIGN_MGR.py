#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CUST_ASSIGN_MGR').setMaster(sys.argv[2])
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
#恢复数据到今日数据文件
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet")

ADMIN_AUTH_ACCOUNT = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ACCOUNT/*')
ADMIN_AUTH_ACCOUNT.registerTempTable("ADMIN_AUTH_ACCOUNT")
ADMIN_AUTH_ACCOUNT_SORT = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ACCOUNT_SORT/*')
ADMIN_AUTH_ACCOUNT_SORT.registerTempTable("ADMIN_AUTH_ACCOUNT_SORT")
OCRM_F_CI_BELONG_APPLY = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_APPLY/*')
OCRM_F_CI_BELONG_APPLY.registerTempTable("OCRM_F_CI_BELONG_APPLY")
OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP/*')
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")
OCRM_F_CI_TRANS_APPLY = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_TRANS_APPLY/*')
OCRM_F_CI_TRANS_APPLY.registerTempTable("OCRM_F_CI_TRANS_APPLY")
UNIT_CUST_B = sqlContext.read.parquet(hdfs+'/UNIT_CUST_B/*')
UNIT_CUST_B.registerTempTable("UNIT_CUST_B")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
TEMP_A = sqlContext.read.parquet(hdfs+'/TEMP_A/*')
TEMP_A.registerTempTable("TEMP_A")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(0 AS DECIMAL(27))                    AS ID 
       ,A.ODS_CUSTOM_ID         AS CUST_ID 
       ,A.CUST_MNG              AS MGR_ID 
       ,A.MNG_TYP               AS MAIN_TYPE 
       ,''                    AS MAINTAIN_RIGHT 
       ,''                    AS CHECK_RIGHT 
       ,''                      AS ASSIGN_USER 
       ,'系统'                AS ASSIGN_USERNAME 
       ,V_DT                    AS ASSIGN_DATE 
       ,A.ORG_ID                AS INSTITUTION 
       ,''                    AS INSTITUTION_NAME 
       ,''                    AS MGR_NAME 
       ,''                    AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_A A                                               --对私分配中间表A
  WHERE A.CUST_MNG IS 
    NOT NULL 
    AND TRIM(A.CUST_MNG) <> '' 
  GROUP BY A.FR_ID 
       ,A.ODS_CUSTOM_ID 
       ,A.CUST_MNG 
       ,A.MNG_TYP 
       ,A.ORG_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")
dfn="OCRM_F_CI_BELONG_CUSTMGR_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_TEMP.cache()
nrows = OCRM_F_CI_BELONG_CUSTMGR_TEMP.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_TEMP/*.parquet")
OCRM_F_CI_BELONG_CUSTMGR_TEMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(0 AS DECIMAL(27))                    AS ID 
       ,A.ODS_CUST_ID           AS CUST_ID 
       ,A.MANGER_NO             AS MGR_ID 
       ,A.TYPE2                 AS MAIN_TYPE 
       ,''                    AS MAINTAIN_RIGHT 
       ,''                    AS CHECK_RIGHT 
       ,''                      AS ASSIGN_USER 
       ,'系统'                AS ASSIGN_USERNAME 
       ,V_DT                    AS ASSIGN_DATE 
       ,A.BRANCH                AS INSTITUTION 
       ,''                    AS INSTITUTION_NAME 
       ,''                    AS MGR_NAME 
       ,''                    AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM UNIT_CUST_B A                                          --对公分配中间表B
  WHERE A.MANGER_NO IS 
    NOT NULL 
    AND TRIM(A.MANGER_NO) <> '' 
  GROUP BY A.FR_ID 
       ,A.ODS_CUST_ID 
       ,A.MANGER_NO 
       ,A.TYPE2 
       ,A.BRANCH """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")
dfn="OCRM_F_CI_BELONG_CUSTMGR_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_TEMP.cache()
nrows = OCRM_F_CI_BELONG_CUSTMGR_TEMP.count()
OCRM_F_CI_BELONG_CUSTMGR_TEMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-03::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR_TEMP/*')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,COALESCE(B.ACCOUNT_NAME, A.MGR_ID)                       AS MGR_ID 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.MAINTAIN_RIGHT        AS MAINTAIN_RIGHT 
       ,A.CHECK_RIGHT           AS CHECK_RIGHT 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,COALESCE(B.ORG_ID, A.INSTITUTION)                       AS INSTITUTION 
       ,COALESCE(B.ORG_NAME, A.INSTITUTION_NAME)                       AS INSTITUTION_NAME 
       ,COALESCE(B.USER_NAME, A.MGR_NAME)                       AS MGR_NAME 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR_TEMP A                        --归属经理临时表
  INNER JOIN ADMIN_AUTH_ACCOUNT_SORT B                         --源系统操作号对照表
     ON A.MGR_ID                = B.SOURCE_SYS_USER_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP1")

OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR_TEMP/*')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.MGR_ID                                             --经理号:src.MGR_ID
       ,DST.MAIN_TYPE                                          --主协办:src.MAIN_TYPE
       ,DST.MAINTAIN_RIGHT                                     --是否有维护权:src.MAINTAIN_RIGHT
       ,DST.CHECK_RIGHT                                        --检查状态:src.CHECK_RIGHT
       ,DST.ASSIGN_USER                                        --分配人:src.ASSIGN_USER
       ,DST.ASSIGN_USERNAME                                    --分配人名称:src.ASSIGN_USERNAME
       ,DST.ASSIGN_DATE                                        --分配日期:src.ASSIGN_DATE
       ,DST.INSTITUTION                                        --机构号:src.INSTITUTION
       ,DST.INSTITUTION_NAME                                   --机构名称:src.INSTITUTION_NAME
       ,DST.MGR_NAME                                           --经理号:src.MGR_NAME
       ,DST.EFF_DATE                                           --有效日期:src.EFF_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_BELONG_CUSTMGR_TEMP DST 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP1 SRC 
     ON DST.MGR_ID = SRC.MGR_ID AND DST.FR_ID = SRC.FR_ID AND DST.CUST_ID = SRC.CUST_ID
  WHERE SRC.MGR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_CUSTMGR_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP2=OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP2.unionAll(OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP1)
OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP1.cache()
OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP2.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_TEMP/*.parquet")
OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP1.unpersist()
OCRM_F_CI_BELONG_CUSTMGR_TEMP_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR_TEMP lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_TEMP/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_TEMP_BK/")

#任务[11] 001-04::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR_TEMP/*')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")

sql = """
 SELECT CAST(0 AS DECIMAL(27))                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.MGR_ID                AS MGR_ID 
       ,'2'                     AS MAIN_TYPE 
       ,''                    AS MAINTAIN_RIGHT 
       ,''                    AS CHECK_RIGHT 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.INSTITUTION           AS INSTITUTION 
       ,''                    AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,''                    AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR A                             --归属经理表
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR_TEMP B                   --归属经理临时表
     ON A.CUST_ID               = B.CUST_ID 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR_TEMP C                   --归属经理临时表
     ON A.CUST_ID               = C.CUST_ID 
    AND A.MGR_ID                = C.MGR_ID 
    AND C.FR_ID                 = A.FR_ID 
  WHERE C.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")
dfn="OCRM_F_CI_BELONG_CUSTMGR_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_TEMP.cache()
nrows = OCRM_F_CI_BELONG_CUSTMGR_TEMP.count()
OCRM_F_CI_BELONG_CUSTMGR_TEMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-05::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR_TEMP/*')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")

sql = """
 SELECT A.ID                      AS ID 
       ,A.CUST_ID                 AS CUST_ID 
       ,A.MGR_ID                  AS MGR_ID 
       ,'2'                     AS MAIN_TYPE 
       ,A.MAINTAIN_RIGHT          AS MAINTAIN_RIGHT 
       ,A.CHECK_RIGHT             AS CHECK_RIGHT 
       ,A.ASSIGN_USER             AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME         AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE             AS ASSIGN_DATE 
       ,A.INSTITUTION             AS INSTITUTION 
       ,A.INSTITUTION_NAME        AS INSTITUTION_NAME 
       ,A.MGR_NAME                AS MGR_NAME 
       ,A.EFF_DATE                AS EFF_DATE 
       ,A.FR_ID                   AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR_TEMP A                        --归属经理临时表
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --归属经理表
     ON A.CUST_ID               = B.CUST_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.MAIN_TYPE             = '1' 
    AND B.ASSIGN_USERNAME <> '系统' 
    AND B.EFF_DATE IS 
    NOT NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")
dfn="OCRM_F_CI_BELONG_CUSTMGR_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_TEMP.cache()
nrows = OCRM_F_CI_BELONG_CUSTMGR_TEMP.count()
OCRM_F_CI_BELONG_CUSTMGR_TEMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-06::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.MGR_ID                AS MGR_ID 
       ,'1'                     AS MAIN_TYPE 
       ,A.MAINTAIN_RIGHT        AS MAINTAIN_RIGHT 
       ,A.CHECK_RIGHT           AS CHECK_RIGHT 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,B.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.INSTITUTION           AS INSTITUTION 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,B.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR_TEMP A                        --归属经理临时表
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --归属经理表
     ON A.CUST_ID               = B.CUST_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.MAIN_TYPE             = '1' 
    AND B.ASSIGN_USERNAME <> '系统' 
    AND A.MGR_ID                = B.MGR_ID 
    AND B.EFF_DATE > V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")
dfn="OCRM_F_CI_BELONG_CUSTMGR_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_TEMP.cache()
nrows = OCRM_F_CI_BELONG_CUSTMGR_TEMP.count()
OCRM_F_CI_BELONG_CUSTMGR_TEMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-07::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR_TEMP/*')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")

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
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR A                             --归属经理表
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR_TEMP B                   --归属经理中间表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE B.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_BELONG_CUSTMGR_02 = sqlContext.sql(sql)
TMP_OCRM_F_CI_BELONG_CUSTMGR_02.registerTempTable("TMP_OCRM_F_CI_BELONG_CUSTMGR_02")
dfn="TMP_OCRM_F_CI_BELONG_CUSTMGR_02/"+V_DT+".parquet"
TMP_OCRM_F_CI_BELONG_CUSTMGR_02.cache()
nrows = TMP_OCRM_F_CI_BELONG_CUSTMGR_02.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_BELONG_CUSTMGR_02/*.parquet")
TMP_OCRM_F_CI_BELONG_CUSTMGR_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_BELONG_CUSTMGR_02.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_BELONG_CUSTMGR_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-08::
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
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TMP_OCRM_F_CI_BELONG_CUSTMGR_02 A                      --归属经理表临时表02（删数中间表)
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
dfn="OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR.cache()
nrows = OCRM_F_CI_BELONG_CUSTMGR.count()
OCRM_F_CI_BELONG_CUSTMGR.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_CUSTMGR.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-09::
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
       ,B.ORG_NAME              AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR_TEMP A                        --归属经理中间表
  INNER JOIN ADMIN_AUTH_ORG B                                  --机构表
     ON A.INSTITUTION           = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")
dfn="OCRM_F_CI_BELONG_CUSTMGR_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_TEMP.cache()
nrows = OCRM_F_CI_BELONG_CUSTMGR_TEMP.count()
OCRM_F_CI_BELONG_CUSTMGR_TEMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-10::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR_TEMP/*')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")

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
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR_TEMP A                        --归属经理临时表
   LEFT JOIN OCRM_F_CI_BELONG_APPLY B                          --分配申请表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.MGR_ID                = B.MGR_ID 
    AND B.STATUS                = '2' 
    AND B.APPLY_TYPE            = '3' 
    AND A.FR_ID                 = B.FR_ID 
  WHERE(A.ASSIGN_USERNAME       = '系统' 
            AND B.CUST_ID IS NULL) 
     OR A.ASSIGN_USERNAME <> '系统' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01 = sqlContext.sql(sql)
TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01.registerTempTable("TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01")
dfn="TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01/"+V_DT+".parquet"
TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01.cache()
nrows = TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01/*.parquet")
TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-11::
V_STEP = V_STEP + 1

TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01/*')
TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01.registerTempTable("TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01")

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
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TMP_OCRM_F_CI_BELONG_CUSTMGR_TEMP_01 A                 --归属经理临时表01(删数中间表)
   LEFT JOIN OCRM_F_CI_TRANS_APPLY B                           --移交中间表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID 
    AND B.APPROVE_STAT          = '2' 
    AND A.MGR_ID                = B.BEFORE_MGR_ID 
  WHERE(A.ASSIGN_USERNAME       = '系统' 
            AND B.CUST_ID IS NULL) 
     OR A.ASSIGN_USERNAME <> '系统' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")
dfn="OCRM_F_CI_BELONG_CUSTMGR_TEMP/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR_TEMP.cache()
nrows = OCRM_F_CI_BELONG_CUSTMGR_TEMP.count()
OCRM_F_CI_BELONG_CUSTMGR_TEMP.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_TEMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-12::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_CUSTMGR_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR_TEMP/*')
OCRM_F_CI_BELONG_CUSTMGR_TEMP.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR_TEMP")

sql = """
 SELECT CAST(monotonically_increasing_id() AS DECIMAL(27))       AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.MGR_ID                AS MGR_ID 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,''                    AS MAINTAIN_RIGHT 
       ,''                    AS CHECK_RIGHT 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.INSTITUTION           AS ASSIGN_DATE 
       ,A.INSTITUTION_NAME      AS INSTITUTION 
       ,A.ASSIGN_DATE           AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,'9999-12-31'                       AS EFF_DATE 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR_TEMP A                        --分配经理中间表
  WHERE A.MGR_ID IS 
    NOT NULL 
    AND TRIM(A.MGR_ID) <> '' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
dfn="OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR.cache()
nrows = OCRM_F_CI_BELONG_CUSTMGR.count()
OCRM_F_CI_BELONG_CUSTMGR.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_CUSTMGR.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-13::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")

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
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_CUSTMGR A                             --归属经理表
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR B                        --归属经理表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.MAIN_TYPE             = '1' 
    AND B.MAIN_TYPE             = '1' 
    AND A.ID > B.ID 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR C                        --归属经理表
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND A.MAIN_TYPE             = '2' 
    AND C.MAIN_TYPE             = '2' 
    AND A.MGR_ID                = C.MGR_ID 
    AND A.ID > C.ID 
   LEFT JOIN OCRM_F_CI_BELONG_CUSTMGR D                        --归属经理表
     ON A.CUST_ID               = D.CUST_ID 
    AND A.FR_ID                 = D.FR_ID 
    AND A.MAIN_TYPE             = '2' 
    AND D.MAIN_TYPE             = '1' 
    AND A.MGR_ID                = D.MGR_ID 
  WHERE B.FR_ID IS NULL 
    AND C.FR_ID IS NULL 
    AND C.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_BELONG_CUSTMGR_02 = sqlContext.sql(sql)
TMP_OCRM_F_CI_BELONG_CUSTMGR_02.registerTempTable("TMP_OCRM_F_CI_BELONG_CUSTMGR_02")
dfn="TMP_OCRM_F_CI_BELONG_CUSTMGR_02/"+V_DT+".parquet"
TMP_OCRM_F_CI_BELONG_CUSTMGR_02.cache()
nrows = TMP_OCRM_F_CI_BELONG_CUSTMGR_02.count()
TMP_OCRM_F_CI_BELONG_CUSTMGR_02.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_OCRM_F_CI_BELONG_CUSTMGR_02.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_BELONG_CUSTMGR_02/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_BELONG_CUSTMGR_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-14::
V_STEP = V_STEP + 1

TMP_OCRM_F_CI_BELONG_CUSTMGR_02 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_BELONG_CUSTMGR_02/*')
TMP_OCRM_F_CI_BELONG_CUSTMGR_02.registerTempTable("TMP_OCRM_F_CI_BELONG_CUSTMGR_02")

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
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MGR_NAME              AS MGR_NAME 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TMP_OCRM_F_CI_BELONG_CUSTMGR_02 A                      --归属经理表临时表02（删数中间表)
   LEFT JOIN ADMIN_AUTH_ACCOUNT B                              --账户表
     ON A.MGR_ID                = B.ACCOUNT_NAME 
   LEFT JOIN ADMIN_AUTH_ORG C                                  --机构表
     ON B.ORG_ID                = C.ORG_ID 
    AND B.FR_ID                 = C.FR_ID 
  WHERE B.FR_ID IS 
    NOT NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
dfn="OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR.cache()
nrows = OCRM_F_CI_BELONG_CUSTMGR.count()
OCRM_F_CI_BELONG_CUSTMGR.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_CUSTMGR.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-15::
V_STEP = V_STEP + 1
sql = """
 SELECT CAST(monotonically_increasing_id() AS DECIMAL(27))       AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,C.ACCOUNT_NAME          AS MGR_ID 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,''                    AS MAINTAIN_RIGHT 
       ,''                    AS CHECK_RIGHT 
       ,''                      AS ASSIGN_USER 
       ,'系统'                AS ASSIGN_USERNAME 
       ,V_DT                    AS ASSIGN_DATE 
       ,A.INSTITUTION_CODE      AS INSTITUTION 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,C.USER_NAME             AS MGR_NAME 
       ,'9999-12-31'                       AS EFF_DATE 
       ,V_DT                    AS ETL_DATE 
       ,'095'                   AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG_TEMP A                            --分配经理中间表
  INNER JOIN ADMIN_AUTH_ORG B                                  --机构表
     ON A.INSTITUTION_CODE      = B.ORG_ID 
    AND B.FR_ID                 = '095' 
  INNER JOIN ADMIN_AUTH_ACCOUNT C                              --账户表
     ON C.ACCOUNT_NAME LIKE '%XN%' 
    AND B.ORG_ID                = C.ORG_ID 
  WHERE A.MAIN_TYPE             = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.sql(sql)
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
dfn="OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet"
OCRM_F_CI_BELONG_CUSTMGR.cache()
nrows = OCRM_F_CI_BELONG_CUSTMGR.count()

#装载数据
OCRM_F_CI_BELONG_CUSTMGR.write.save(path=hdfs + '/' + dfn, mode='append')
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_BK/"+V_DT+".parquet")

OCRM_F_CI_BELONG_CUSTMGR.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_CUSTMGR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
