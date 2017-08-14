#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CUST_ASSIGN_ORG').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_ORG/*.parquet")
#恢复数据到今日数据文件
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet")

ADMIN_AUTH_ACCOUNT_SORT = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ACCOUNT_SORT/*')
ADMIN_AUTH_ACCOUNT_SORT.registerTempTable("ADMIN_AUTH_ACCOUNT_SORT")
OCRM_F_CI_BELONG_APPLY = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_APPLY/*')
OCRM_F_CI_BELONG_APPLY.registerTempTable("OCRM_F_CI_BELONG_APPLY")
OCRM_F_CI_TRANS_APPLY = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_TRANS_APPLY/*')
OCRM_F_CI_TRANS_APPLY.registerTempTable("OCRM_F_CI_TRANS_APPLY")
UNIT_CUST_B = sqlContext.read.parquet(hdfs+'/UNIT_CUST_B/*')
UNIT_CUST_B.registerTempTable("UNIT_CUST_B")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
OCRM_F_CI_BELONG_ORG_TEMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP/*')
OCRM_F_CI_BELONG_ORG_TEMP.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP")
TEMP_A = sqlContext.read.parquet(hdfs+'/TEMP_A/*')
TEMP_A.registerTempTable("TEMP_A")

#目标表
OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUSTOM_ID         AS CUST_ID 
       ,B.ORG_ID                AS INSTITUTION_CODE 
       ,''                    AS INSTITUTION_NAME 
       ,A.ORG_TYP               AS MAIN_TYPE 
       ,''                      AS ASSIGN_USER 
       ,'系统'                AS ASSIGN_USERNAME 
       ,V_DT                    AS ASSIGN_DATE 
       ,''                    AS ETL_DATE 
       ,''                    AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_A A                                               --对私分配中间表A
   LEFT JOIN ADMIN_AUTH_ACCOUNT_SORT B                         --源系统操作号对照表2
     ON A.CUST_MNG              = B.SOURCE_SYS_USER_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.ODS_CUSTOM_ID IS 
    NOT NULL 
    AND A.FR_ID <> '095' 
  GROUP BY A.ODS_CUSTOM_ID 
       ,A.FR_ID 
       ,B.ORG_ID 
       ,A.ORG_TYP """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_02 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP_02.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_02")
dfn="OCRM_F_CI_BELONG_ORG_TEMP_02/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP_02.cache()
nrows = OCRM_F_CI_BELONG_ORG_TEMP_02.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP_02/*.parquet")
OCRM_F_CI_BELONG_ORG_TEMP_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_TEMP_02.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUST_ID           AS CUST_ID 
       ,B.ORG_ID                AS INSTITUTION_CODE 
       ,''                    AS INSTITUTION_NAME 
       ,A.TYPE1                 AS MAIN_TYPE 
       ,''                      AS ASSIGN_USER 
       ,'系统'                AS ASSIGN_USERNAME 
       ,V_DT                    AS ASSIGN_DATE 
       ,''                    AS ETL_DATE 
       ,''                    AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM UNIT_CUST_B A                                          --对公分配中间表B
   LEFT JOIN ADMIN_AUTH_ACCOUNT_SORT B                         --源系统操作号对照表2
     ON A.MANGER_NO             = B.SOURCE_SYS_USER_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.ODS_CUST_ID IS 
    NOT NULL 
    AND A.FR_ID <> '095' 
  GROUP BY A.ODS_CUST_ID 
       ,A.FR_ID 
       ,B.ORG_ID 
       ,A.TYPE1 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_02 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP_02.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_02")
dfn="OCRM_F_CI_BELONG_ORG_TEMP_02/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP_02.cache()
nrows = OCRM_F_CI_BELONG_ORG_TEMP_02.count()
OCRM_F_CI_BELONG_ORG_TEMP_02.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_ORG_TEMP_02.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG_TEMP_02 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP_02/*')
OCRM_F_CI_BELONG_ORG_TEMP_02.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_02")

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
   FROM OCRM_F_CI_BELONG_ORG_TEMP_02 A                         --对私分配临时表02(分配归属机构使用)
   LEFT JOIN OCRM_F_CI_BELONG_ORG_TEMP_02 B                    --对私分配临时表02(分配归属机构使用)
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.INSTITUTION_CODE      = B.INSTITUTION_CODE 
    AND B.MAIN_TYPE             = '1' 
  WHERE A.MAIN_TYPE             = '1' 
     OR(A.MAIN_TYPE             = '2' 
            AND B.FR_ID IS NULL) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_03 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP_03.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03")
dfn="OCRM_F_CI_BELONG_ORG_TEMP_03/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP_03.cache()
nrows = OCRM_F_CI_BELONG_ORG_TEMP_03.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP_03/*.parquet")
OCRM_F_CI_BELONG_ORG_TEMP_03.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_TEMP_03.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP_03 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG_TEMP_03 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP_03/*')
OCRM_F_CI_BELONG_ORG_TEMP_03.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,''                    AS INSTITUTION_NAME 
       ,'2'                     AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE             AS ASSIGN_DATE 
       ,''                    AS ETL_DATE 
       ,''                    AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG A                                 --归属机构表
  INNER JOIN OCRM_F_CI_BELONG_ORG_TEMP_03 B                    --对私分配临时表03(分配归属机构使用)
     ON A.CUST_ID               = B.CUST_ID 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN OCRM_F_CI_BELONG_ORG_TEMP_03 C                    --对私分配临时表03(分配归属机构使用)
     ON A.CUST_ID               = C.CUST_ID 
    AND C.FR_ID                 = A.FR_ID 
    AND A.INSTITUTION_CODE      = C.INSTITUTION_CODE 
  WHERE A.EFF_DATE > V_DT 
    AND A.FR_ID <> '095' 
    AND C.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_03 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP_03.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03")
dfn="OCRM_F_CI_BELONG_ORG_TEMP_03/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP_03.cache()
nrows = OCRM_F_CI_BELONG_ORG_TEMP_03.count()
OCRM_F_CI_BELONG_ORG_TEMP_03.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_ORG_TEMP_03.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP_03 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-05::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG_TEMP_03 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP_03/*')
OCRM_F_CI_BELONG_ORG_TEMP_03.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03")

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
   FROM OCRM_F_CI_BELONG_ORG_TEMP_03 A                         --对私分配临时表03(分配归属机构使用)
  INNER JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
    AND B.EFF_DATE > V_DT 
    AND B.ASSIGN_USERNAME <> '系统' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1")

OCRM_F_CI_BELONG_ORG_TEMP_03 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP_03/*')
OCRM_F_CI_BELONG_ORG_TEMP_03.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03")
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
   FROM OCRM_F_CI_BELONG_ORG_TEMP_03 DST 
   LEFT JOIN OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_ORG_TEMP_03/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2=OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.unionAll(OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.cache()
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.count()
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.unpersist()
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP_03 lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP_03/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP_03_BK/")

#任务[12] 001-06::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG_TEMP_03 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP_03/*')
OCRM_F_CI_BELONG_ORG_TEMP_03.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03")

sql = """
 SELECT DISTINCT A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,'1'                     AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,B.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,B.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG_TEMP_03 A                         --对私分配临时表03(分配归属机构使用)
  INNER JOIN OCRM_F_CI_BELONG_ORG B                            --归属机构表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MAIN_TYPE             = '1' 
    AND B.EFF_DATE IS 
    NOT NULL 
    AND B.EFF_DATE > V_DT 
    AND B.ASSIGN_USERNAME <> '系统' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1")

OCRM_F_CI_BELONG_ORG_TEMP_03 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP_03/*')
OCRM_F_CI_BELONG_ORG_TEMP_03.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03")
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
   FROM OCRM_F_CI_BELONG_ORG_TEMP_03 DST 
   LEFT JOIN OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.INSTITUTION_CODE    = DST.INSTITUTION_CODE 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_ORG_TEMP_03/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2=OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.unionAll(OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.cache()
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.count()
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.unpersist()
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP_03 lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP_03/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP_03_BK/")

#任务[12] 001-07::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG_TEMP_03 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP_03/*')
OCRM_F_CI_BELONG_ORG_TEMP_03.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,B.ORG_NAME              AS INSTITUTION_NAME 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG_TEMP_03 A                         --对私分配临时表03(分配归属机构使用)
  INNER JOIN ADMIN_AUTH_ORG B                                  --归属机构表
     ON A.INSTITUTION_CODE      = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1")

OCRM_F_CI_BELONG_ORG_TEMP_03 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP_03/*')
OCRM_F_CI_BELONG_ORG_TEMP_03.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03")
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
   FROM OCRM_F_CI_BELONG_ORG_TEMP_03 DST 
   LEFT JOIN OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.INSTITUTION_CODE    = DST.INSTITUTION_CODE 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_BELONG_ORG_TEMP_03/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2=OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.unionAll(OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1)
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.cache()
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.cache()
nrowsi = OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.count()
nrowsa = OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.count()
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP1.unpersist()
OCRM_F_CI_BELONG_ORG_TEMP_03_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG_TEMP_03 lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP_03/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_TEMP_03_BK/")

OCRM_F_CI_BELONG_ORG_TEMP_03 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_TEMP_03/*')
OCRM_F_CI_BELONG_ORG_TEMP_03.registerTempTable("OCRM_F_CI_BELONG_ORG_TEMP_03")

#任务[12] 001-08::
V_STEP = V_STEP + 1

sql = """
 SELECT DISTINCT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,'客户退回'          AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG A                                 --归属机构表
  INNER JOIN OCRM_F_CI_BELONG_APPLY B                          --客户分配申请表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.MAIN_TYPE             = B.MAIN_TYPE 
    AND B.APPLY_TYPE            = '3' 
    AND B.STATUS                = '2' 
  WHERE A.FR_ID <> '095' 
    AND A.ASSIGN_USERNAME       = '系统' """

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

#任务[12] 001-09::
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
       ,'客户移交'          AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG A                                 --归属机构表
  INNER JOIN OCRM_F_CI_TRANS_APPLY B                           --客户移交申请表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.INSTITUTION_CODE      = B.AFTER_INST_CODE 
    AND A.MAIN_TYPE             = B.AFTER_MAIN_TYPE 
    AND B.APPROVE_STAT          = '2' 
  WHERE A.FR_ID <> '095' 
    AND A.ASSIGN_USERNAME       = '系统' """

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

#任务[21] 001-10::
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
   LEFT JOIN OCRM_F_CI_BELONG_ORG_TEMP_03 B                    --对私分配临时表03(分配归属机构使用)
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE B.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_BELONG_ORG_01 = sqlContext.sql(sql)
TMP_OCRM_F_CI_BELONG_ORG_01.registerTempTable("TMP_OCRM_F_CI_BELONG_ORG_01")
dfn="TMP_OCRM_F_CI_BELONG_ORG_01/"+V_DT+".parquet"
TMP_OCRM_F_CI_BELONG_ORG_01.cache()
nrows = TMP_OCRM_F_CI_BELONG_ORG_01.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_BELONG_ORG_01/*.parquet")
TMP_OCRM_F_CI_BELONG_ORG_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_BELONG_ORG_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_BELONG_ORG_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-11::
V_STEP = V_STEP + 1

TMP_OCRM_F_CI_BELONG_ORG_01 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_BELONG_ORG_01/*')
TMP_OCRM_F_CI_BELONG_ORG_01.registerTempTable("TMP_OCRM_F_CI_BELONG_ORG_01")

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
   FROM TMP_OCRM_F_CI_BELONG_ORG_01 A                          --客户归属机构表临时表01
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
dfn="OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG.cache()
nrows = OCRM_F_CI_BELONG_ORG.count()
OCRM_F_CI_BELONG_ORG.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_ORG.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-12::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id()       AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,B.ORG_NAME              AS INSTITUTION_NAME 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,V_DT                    AS ETL_DATE 
       ,COALESCE(A.EFF_DATE, '9999-12-31')                       AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG_TEMP_03 A                         --对私分配临时表03(分配归属机构使用)
   LEFT JOIN ADMIN_AUTH_ORG B                                  --机构表
     ON A.INSTITUTION_CODE      = B.ORG_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
dfn="OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG.cache()
nrows = OCRM_F_CI_BELONG_ORG.count()
OCRM_F_CI_BELONG_ORG.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_ORG.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-13::
V_STEP = V_STEP + 1

sql = """
 SELECT distinct CAST(monotonically_increasing_id() AS BIGINT)       AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,A.INSTITUTION_NAME      AS INSTITUTION_NAME 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,V_DT                    AS ETL_DATE 
       ,'9999-12-31'                       AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG_TEMP A                            --对私分配临时表
  WHERE FR_ID                   = '095' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BELONG_ORG = sqlContext.sql(sql)
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
dfn="OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet"
OCRM_F_CI_BELONG_ORG.cache()
nrows = OCRM_F_CI_BELONG_ORG.count()
OCRM_F_CI_BELONG_ORG.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_BELONG_ORG.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-14::
V_STEP = V_STEP + 1

OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")

sql = """
 SELECT DISTINCT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.INSTITUTION_CODE      AS INSTITUTION_CODE 
       ,B.ORG_NAME              AS INSTITUTION_NAME 
       ,A.MAIN_TYPE             AS MAIN_TYPE 
       ,A.ASSIGN_USER           AS ASSIGN_USER 
       ,A.ASSIGN_USERNAME       AS ASSIGN_USERNAME 
       ,A.ASSIGN_DATE           AS ASSIGN_DATE 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_BELONG_ORG A                                 --归属机构表
  INNER JOIN ADMIN_AUTH_ORG B                                  --归属机构表
     ON A.INSTITUTION_CODE      = B.ORG_ID 
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
    AND SRC.INSTITUTION_CODE    = DST.INSTITUTION_CODE 
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

#装载数据
OCRM_F_CI_BELONG_ORG_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/"+V_DT+".parquet")

OCRM_F_CI_BELONG_ORG_INNTMP1.unpersist()
OCRM_F_CI_BELONG_ORG_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BELONG_ORG lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)

