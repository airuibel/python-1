#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUST_AUTO_MAPPING').setMaster(sys.argv[2])
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

ACRM_F_CI_CUST_SIMILARLST = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST/*')
ACRM_F_CI_CUST_SIMILARLST.registerTempTable("ACRM_F_CI_CUST_SIMILARLST")
ACRM_F_CI_CUST_SIMILARLST_HIS = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST_HIS/*')
ACRM_F_CI_CUST_SIMILARLST_HIS.registerTempTable("ACRM_F_CI_CUST_SIMILARLST_HIS")
F_CI_CUST_MERGERLST = sqlContext.read.parquet(hdfs+'/F_CI_CUST_MERGERLST/*')
F_CI_CUST_MERGERLST.registerTempTable("F_CI_CUST_MERGERLST")
#目标表
#AUTO_SIMILARLST_TMP_01：全量
#AUTO_SIMILARLST_TMP：先全量后增量
#OCRM_F_CI_HHB_MAPPING_TMP：全量
#OCRM_F_CI_HHB_MAPPING:先增改（删除自身一部分数据）后增


#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT T.CUSTOM_ID_MAIN        AS CUSTOM_ID_MAIN 
       ,A.TARGET_CUST_ID        AS TARGET_CUST_ID 
       ,T.TYP_ID                AS TYP_ID 
       ,T.FR_ID                 AS FR_ID 
       ,T.CERT_NO_M             AS CERT_NO_M 
       ,T.CERT_TYP_M            AS CERT_TYP_M 
       ,T.CUST_NAME_M           AS CUST_NAME_M 
   FROM ACRM_F_CI_CUST_SIMILARLST T                            --
  INNER JOIN ACRM_F_CI_CUST_SIMILARLST_HIS A                   --
     ON A.FR_ID                 = T.FR_ID 
    AND A.CERT_NO_M             = T.CERT_NO_M 
    AND A.CERT_TYP_M            = T.CERT_TYP_M 
    AND A.ODS_SYS_ID            = T.ODS_SYS_ID 
    AND A.CUST_NAME_M           = T.CUST_NAME_M 
  WHERE 
    NOT EXISTS(
         SELECT 1 
           FROM F_CI_CUST_MERGERLST B 
          WHERE B.FR_ID                 = T.FR_ID 
            AND T.CUSTOM_ID_MAIN        = B.CUSTOM_ID_BEFORE) 
  GROUP BY T.CUSTOM_ID_MAIN 
       ,A.TARGET_CUST_ID 
       ,T.TYP_ID 
       ,T.FR_ID 
       ,T.CERT_NO_M 
       ,T.CERT_TYP_M 
       ,T.CUST_NAME_M """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
AUTO_SIMILARLST_TMP = sqlContext.sql(sql)
AUTO_SIMILARLST_TMP.registerTempTable("AUTO_SIMILARLST_TMP")
dfn="AUTO_SIMILARLST_TMP/"+V_DT+".parquet"
AUTO_SIMILARLST_TMP.cache()
nrows = AUTO_SIMILARLST_TMP.count()
AUTO_SIMILARLST_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
AUTO_SIMILARLST_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/AUTO_SIMILARLST_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert AUTO_SIMILARLST_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
OCRM_F_CI_HHB_MAPPING = sqlContext.read.parquet(hdfs+'/AUTO_SIMILARLST_TMP/*')
OCRM_F_CI_HHB_MAPPING.registerTempTable("AUTO_SIMILARLST_TMP")
sql = """
 SELECT CUSTOM_ID_MAIN          AS CUSTOM_ID_MAIN 
       ,TARGET_CUST_ID          AS TARGET_CUST_ID 
       ,TYP_ID                  AS TYP_ID 
       ,FR_ID                   AS FR_ID 
       ,CERT_NO_M               AS CERT_NO_M 
       ,CERT_TYP_M              AS CERT_TYP_M 
       ,CUST_NAME_M             AS CUST_NAME_M 
   FROM AUTO_SIMILARLST_TMP A                                  --
  WHERE 
    NOT EXISTS(
         SELECT 1 
           FROM(
                 SELECT CERT_NO_M 
                       ,CERT_TYP_M 
                       ,CUST_NAME_M 
                       ,TARGET_CUST_ID
                       ,FR_ID
                   FROM AUTO_SIMILARLST_TMP 
                  GROUP BY CERT_NO_M 
                       ,CERT_TYP_M 
                       ,CUST_NAME_M
                       ,TARGET_CUST_ID
                       ,FR_ID HAVING COUNT(1) > 1) C 
          WHERE A.CERT_NO_M             = C.CERT_NO_M 
            AND A.CERT_TYP_M            = C.CERT_TYP_M 
            AND A.CUST_NAME_M           = C.CUST_NAME_M
            AND A.FR_ID = C.FR_ID) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
AUTO_SIMILARLST_TMP_01 = sqlContext.sql(sql)
AUTO_SIMILARLST_TMP_01.registerTempTable("AUTO_SIMILARLST_TMP_01")
dfn="AUTO_SIMILARLST_TMP_01/"+V_DT+".parquet"
AUTO_SIMILARLST_TMP_01.cache()
nrows = AUTO_SIMILARLST_TMP_01.count()
AUTO_SIMILARLST_TMP_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
AUTO_SIMILARLST_TMP_01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/AUTO_SIMILARLST_TMP_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert AUTO_SIMILARLST_TMP_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1
AUTO_SIMILARLST_TMP_01 = sqlContext.read.parquet(hdfs+'/AUTO_SIMILARLST_TMP_01/*')
AUTO_SIMILARLST_TMP_01.registerTempTable("AUTO_SIMILARLST_TMP_01")
sql = """
 SELECT CUSTOM_ID_MAIN          AS CUSTOM_ID_MAIN 
       ,TARGET_CUST_ID          AS TARGET_CUST_ID 
       ,TYP_ID                  AS TYP_ID 
       ,FR_ID                   AS FR_ID 
       ,CERT_NO_M               AS CERT_NO_M 
       ,CERT_TYP_M              AS CERT_TYP_M 
       ,CUST_NAME_M             AS CUST_NAME_M 
   FROM AUTO_SIMILARLST_TMP_01 A                               --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
AUTO_SIMILARLST_TMP = sqlContext.sql(sql)
AUTO_SIMILARLST_TMP.registerTempTable("AUTO_SIMILARLST_TMP")
dfn="AUTO_SIMILARLST_TMP/"+V_DT+".parquet"
AUTO_SIMILARLST_TMP.cache()
nrows = AUTO_SIMILARLST_TMP.count()
AUTO_SIMILARLST_TMP.write.save(path=hdfs + '/' + dfn, mode='append')
AUTO_SIMILARLST_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/AUTO_SIMILARLST_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert AUTO_SIMILARLST_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_HHB_MAPPING/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_HHB_MAPPING_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_HHB_MAPPING/"+V_DT+".parquet")
OCRM_F_CI_HHB_MAPPING = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_HHB_MAPPING/*')
OCRM_F_CI_HHB_MAPPING.registerTempTable("OCRM_F_CI_HHB_MAPPING")
sql = """
 SELECT CAST(ID  AS BIGINT)                    AS ID 
       ,SOURCE_CUST_ID          AS SOURCE_CUST_ID 
       ,TARGET_CUST_ID          AS TARGET_CUST_ID 
       ,CAST(GROUP_ID    AS INTEGER)            AS GROUP_ID 
       ,APPLY_TELLER            AS APPLY_TELLER 
       ,HHB_DT                  AS HHB_DT 
       ,APPLY_STS               AS APPLY_STS 
       ,APPLY_DATE              AS APPLY_DATE 
       ,UNION_FLAG              AS UNION_FLAG 
       ,SUCCESS_DATE            AS SUCCESS_DATE 
       ,DESC                    AS DESC 
       ,SOURCE_CUST_NAME        AS SOURCE_CUST_NAME 
       ,TARGET_CUST_NAME        AS TARGET_CUST_NAME 
       ,FR_ID                   AS FR_ID 
       ,BATCH_NO                AS BATCH_NO 
   FROM OCRM_F_CI_HHB_MAPPING A                                --
  WHERE A.APPLY_TELLER <> 'auto' 
    OR A.HHB_DT <> V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_HHB_MAPPING_TMP = sqlContext.sql(sql)
OCRM_F_CI_HHB_MAPPING_TMP.registerTempTable("OCRM_F_CI_HHB_MAPPING_TMP")
dfn="OCRM_F_CI_HHB_MAPPING_TMP/"+V_DT+".parquet"
OCRM_F_CI_HHB_MAPPING_TMP.cache()
nrows = OCRM_F_CI_HHB_MAPPING_TMP.count()
OCRM_F_CI_HHB_MAPPING_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_HHB_MAPPING_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_HHB_MAPPING_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_HHB_MAPPING_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-05::
V_STEP = V_STEP + 1
OCRM_F_CI_HHB_MAPPING_TMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_HHB_MAPPING_TMP/*')
OCRM_F_CI_HHB_MAPPING_TMP.registerTempTable("OCRM_F_CI_HHB_MAPPING_TMP")
sql = """
 SELECT CAST(ID  AS BIGINT)                       AS ID 
       ,SOURCE_CUST_ID          AS SOURCE_CUST_ID 
       ,TARGET_CUST_ID          AS TARGET_CUST_ID 
       ,CAST(GROUP_ID    AS INTEGER)                 AS GROUP_ID 
       ,APPLY_TELLER            AS APPLY_TELLER 
       ,HHB_DT                  AS HHB_DT 
       ,APPLY_STS               AS APPLY_STS 
       ,APPLY_DATE              AS APPLY_DATE 
       ,UNION_FLAG              AS UNION_FLAG 
       ,SUCCESS_DATE            AS SUCCESS_DATE 
       ,DESC                    AS DESC 
       ,SOURCE_CUST_NAME        AS SOURCE_CUST_NAME 
       ,TARGET_CUST_NAME        AS TARGET_CUST_NAME 
       ,FR_ID                   AS FR_ID 
       ,BATCH_NO                AS BATCH_NO 
   FROM OCRM_F_CI_HHB_MAPPING_TMP A                            --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_HHB_MAPPING = sqlContext.sql(sql)
dfn="OCRM_F_CI_HHB_MAPPING/"+V_DT+".parquet"
OCRM_F_CI_HHB_MAPPING.cache()
nrows = OCRM_F_CI_HHB_MAPPING.count()
OCRM_F_CI_HHB_MAPPING.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_HHB_MAPPING.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_HHB_MAPPING/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_HHB_MAPPING lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-06::
V_STEP = V_STEP + 1
AUTO_SIMILARLST_TMP = sqlContext.read.parquet(hdfs+'/AUTO_SIMILARLST_TMP/*')
AUTO_SIMILARLST_TMP.registerTempTable("AUTO_SIMILARLST_TMP")
sql = """
 SELECT CAST(''  AS BIGINT)     AS ID 
       ,T.CUSTOM_ID_MAIN        AS SOURCE_CUST_ID 
       ,T.TARGET_CUST_ID        AS TARGET_CUST_ID 
       ,CAST(T.TYP_ID  AS INTEGER)              AS GROUP_ID 
       ,'系统'                AS APPLY_TELLER 
       ,V_DT               AS HHB_DT 
       ,'1'                     AS APPLY_STS 
       ,V_DT               AS APPLY_DATE 
       ,'0'                     AS UNION_FLAG 
       ,''                    AS SUCCESS_DATE 
       ,'系统自动合并'    AS DESC 
       ,''                    AS SOURCE_CUST_NAME 
       ,''                    AS TARGET_CUST_NAME 
       ,T.FR_ID                 AS FR_ID 
       ,''                    AS BATCH_NO 
   FROM AUTO_SIMILARLST_TMP T                                  --
  GROUP BY T.CUSTOM_ID_MAIN 
       ,T.TARGET_CUST_ID 
       ,T.TYP_ID 
       ,T.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_HHB_MAPPING = sqlContext.sql(sql)
dfn="OCRM_F_CI_HHB_MAPPING/"+V_DT+".parquet"
OCRM_F_CI_HHB_MAPPING.cache()
nrows = OCRM_F_CI_HHB_MAPPING.count()
OCRM_F_CI_HHB_MAPPING.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_HHB_MAPPING.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_HHB_MAPPING lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_HHB_MAPPING_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_HHB_MAPPING/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_HHB_MAPPING_BK/"+V_DT+".parquet")
