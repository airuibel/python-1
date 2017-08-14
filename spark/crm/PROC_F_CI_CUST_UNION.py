#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUST_UNION').setMaster(sys.argv[2])
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

F_CI_CUST_MERGERLST = sqlContext.read.parquet(hdfs+'/F_CI_CUST_MERGERLST/*')
F_CI_CUST_MERGERLST.registerTempTable("F_CI_CUST_MERGERLST")

#任务[12] 001-01::更新合并历史表合并成功标志
V_STEP = V_STEP + 1

OCRM_F_CI_HHB_MAPPING = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_HHB_MAPPING_BK/'+V_DT_LD+'.parquet/*')
OCRM_F_CI_HHB_MAPPING.registerTempTable("OCRM_F_CI_HHB_MAPPING")

sql = """
 SELECT A.ID                    AS ID 
       ,A.SOURCE_CUST_ID        AS SOURCE_CUST_ID 
       ,A.TARGET_CUST_ID        AS TARGET_CUST_ID 
       ,A.GROUP_ID              AS GROUP_ID 
       ,A.APPLY_TELLER          AS APPLY_TELLER 
       ,A.HHB_DT                AS HHB_DT 
       ,A.APPLY_STS             AS APPLY_STS 
       ,A.APPLY_DATE            AS APPLY_DATE 
       ,B.UNION_FLAG            AS UNION_FLAG 
       ,V_DT                    AS SUCCESS_DATE 
       ,A.DESC                  AS DESC 
       ,A.SOURCE_CUST_NAME      AS SOURCE_CUST_NAME 
       ,A.TARGET_CUST_NAME      AS TARGET_CUST_NAME 
       ,A.FR_ID                 AS FR_ID 
       ,A.BATCH_NO              AS BATCH_NO 
   FROM OCRM_F_CI_HHB_MAPPING A                                --
  INNER JOIN(
         SELECT DISTINCT UNION_FLAG 
               ,CUSTOM_ID 
               ,CUSTOM_ID_BEFORE 
               ,FR_ID 
           FROM F_CI_CUST_MERGERLST 
          WHERE ODS_ST_DATE             = V_DT) B              --
     ON A.TARGET_CUST_ID        = B.CUSTOM_ID 
    AND A.SOURCE_CUST_ID        = B.CUSTOM_ID_BEFORE 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.APPLY_STS             = '1' 
    AND A.UNION_FLAG            = '0' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_HHB_MAPPING_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_HHB_MAPPING_INNTMP1.registerTempTable("OCRM_F_CI_HHB_MAPPING_INNTMP1")

#OCRM_F_CI_HHB_MAPPING = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_HHB_MAPPING/*')
#OCRM_F_CI_HHB_MAPPING.registerTempTable("OCRM_F_CI_HHB_MAPPING")
sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.SOURCE_CUST_ID                                     --:src.SOURCE_CUST_ID
       ,DST.TARGET_CUST_ID                                     --:src.TARGET_CUST_ID
       ,DST.GROUP_ID                                           --:src.GROUP_ID
       ,DST.APPLY_TELLER                                       --:src.APPLY_TELLER
       ,DST.HHB_DT                                             --:src.HHB_DT
       ,DST.APPLY_STS                                          --:src.APPLY_STS
       ,DST.APPLY_DATE                                         --:src.APPLY_DATE
       ,DST.UNION_FLAG                                         --:src.UNION_FLAG
       ,DST.SUCCESS_DATE                                       --:src.SUCCESS_DATE
       ,DST.DESC                                               --:src.DESC
       ,DST.SOURCE_CUST_NAME                                   --:src.SOURCE_CUST_NAME
       ,DST.TARGET_CUST_NAME                                   --:src.TARGET_CUST_NAME
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.BATCH_NO                                           --:src.BATCH_NO
   FROM OCRM_F_CI_HHB_MAPPING DST 
   LEFT JOIN OCRM_F_CI_HHB_MAPPING_INNTMP1 SRC 
     ON SRC.TARGET_CUST_ID      = DST.TARGET_CUST_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.TARGET_CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_HHB_MAPPING_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_HHB_MAPPING/"+V_DT+".parquet"
OCRM_F_CI_HHB_MAPPING_INNTMP2=OCRM_F_CI_HHB_MAPPING_INNTMP2.unionAll(OCRM_F_CI_HHB_MAPPING_INNTMP1)
OCRM_F_CI_HHB_MAPPING_INNTMP1.cache()
OCRM_F_CI_HHB_MAPPING_INNTMP2.cache()
nrowsi = OCRM_F_CI_HHB_MAPPING_INNTMP1.count()
nrowsa = OCRM_F_CI_HHB_MAPPING_INNTMP2.count()
OCRM_F_CI_HHB_MAPPING_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_HHB_MAPPING_INNTMP1.unpersist()
OCRM_F_CI_HHB_MAPPING_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_HHB_MAPPING lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_HHB_MAPPING_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_HHB_MAPPING/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_HHB_MAPPING_BK/")
#任务[12] 001-02::更新相似标识清单表中相似组展现标志
V_STEP = V_STEP + 1

ACRM_F_CI_CUST_SIMILARLST = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST_BK/'+V_DT_LD+'.parquet/*')
ACRM_F_CI_CUST_SIMILARLST.registerTempTable("ACRM_F_CI_CUST_SIMILARLST")

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
       ,'1'                     AS IS_SHOW 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_CUST_SIMILARLST A                            --
  INNER JOIN F_CI_CUST_MERGERLST B                             --
     ON A.TYP_ID                = B.GROUP_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.IS_SHOW               = '2' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_SIMILARLST_INNTMP1 = sqlContext.sql(sql)
ACRM_F_CI_CUST_SIMILARLST_INNTMP1.registerTempTable("ACRM_F_CI_CUST_SIMILARLST_INNTMP1")

#ACRM_F_CI_CUST_SIMILARLST = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST/*')
#ACRM_F_CI_CUST_SIMILARLST.registerTempTable("ACRM_F_CI_CUST_SIMILARLST")
sql = """
 SELECT DST.CUSTOM_ID_MAIN                                      --ODS平台客户号:src.CUSTOM_ID_MAIN
       ,DST.TYP_ID                                             --相似组ID:src.TYP_ID
       ,DST.GROUP_ID                                           --相似类型ID:src.GROUP_ID
       ,DST.SOUR_CUSTOM_ID_M                                   --源系统客户号:src.SOUR_CUSTOM_ID_M
       ,DST.CERT_TYP_M                                         --客户证件类型:src.CERT_TYP_M
       ,DST.CERT_NO_M                                          --客户证件号:src.CERT_NO_M
       ,DST.CUST_NAME_M                                        --客户名称:src.CUST_NAME_M
       ,DST.ODS_LOAD_DT                                        --ODS加载时间戳:src.ODS_LOAD_DT
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --源系统平台数据日期:src.ODS_ST_DATE
       ,DST.IS_SHOW                                            --是否展示:src.IS_SHOW
       ,DST.FR_ID                                              --:src.FR_ID
   FROM ACRM_F_CI_CUST_SIMILARLST DST 
   LEFT JOIN ACRM_F_CI_CUST_SIMILARLST_INNTMP1 SRC 
     ON SRC.TYP_ID              = DST.TYP_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.TYP_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_SIMILARLST_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_CI_CUST_SIMILARLST/"+V_DT+".parquet"
ACRM_F_CI_CUST_SIMILARLST_INNTMP2=ACRM_F_CI_CUST_SIMILARLST_INNTMP2.unionAll(ACRM_F_CI_CUST_SIMILARLST_INNTMP1)
ACRM_F_CI_CUST_SIMILARLST_INNTMP1.cache()
ACRM_F_CI_CUST_SIMILARLST_INNTMP2.cache()
nrowsi = ACRM_F_CI_CUST_SIMILARLST_INNTMP1.count()
nrowsa = ACRM_F_CI_CUST_SIMILARLST_INNTMP2.count()
ACRM_F_CI_CUST_SIMILARLST_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_SIMILARLST_INNTMP1.unpersist()
ACRM_F_CI_CUST_SIMILARLST_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_SIMILARLST lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)

#ACRM_F_CI_CUST_SIMILARLST_HIS 增量 删除当天跑批文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST_HIS/"+V_DT+".parquet")

#任务[11] 001-03::备份要删除的客户信息
V_STEP = V_STEP + 1
ACRM_F_CI_CUST_SIMILARLST = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST/*')
ACRM_F_CI_CUST_SIMILARLST.registerTempTable("ACRM_F_CI_CUST_SIMILARLST")
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
       ,B.CUSTOM_ID             AS TARGET_CUST_ID 
   FROM ACRM_F_CI_CUST_SIMILARLST A                            --
   LEFT JOIN F_CI_CUST_MERGERLST B                             --
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUSTOM_ID_MAIN        = B.CUSTOM_ID_BEFORE 
  WHERE B.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_SIMILARLST_HIS = sqlContext.sql(sql)
ACRM_F_CI_CUST_SIMILARLST_HIS.registerTempTable("ACRM_F_CI_CUST_SIMILARLST_HIS")
dfn="ACRM_F_CI_CUST_SIMILARLST_HIS/"+V_DT+".parquet"
ACRM_F_CI_CUST_SIMILARLST_HIS.cache()
nrows = ACRM_F_CI_CUST_SIMILARLST_HIS.count()
ACRM_F_CI_CUST_SIMILARLST_HIS.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_SIMILARLST_HIS.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_SIMILARLST_HIS lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::删除相似标识清单中被合并客户信息
V_STEP = V_STEP + 1

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
   FROM ACRM_F_CI_CUST_SIMILARLST A                            --
   WHERE NOT EXISTS(SELECT 1 FROM F_CI_CUST_MERGERLST B 
	WHERE A.CUSTOM_ID_MAIN = B.CUSTOM_ID_BEFORE 
	AND B.UNION_FLAG = '1' AND B.FR_ID=A.FR_ID) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_SIMILARLST = sqlContext.sql(sql)
ACRM_F_CI_CUST_SIMILARLST.registerTempTable("ACRM_F_CI_CUST_SIMILARLST")
dfn="ACRM_F_CI_CUST_SIMILARLST/"+V_DT+".parquet"
ACRM_F_CI_CUST_SIMILARLST.cache()
nrows = ACRM_F_CI_CUST_SIMILARLST.count()
ACRM_F_CI_CUST_SIMILARLST.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_SIMILARLST.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_SIMILARLST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-05::删除相似标识清单中被合并客户信息
V_STEP = V_STEP + 1
ACRM_F_CI_CUST_SIMILARLST = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_SIMILARLST/*')
ACRM_F_CI_CUST_SIMILARLST.registerTempTable("ACRM_F_CI_CUST_SIMILARLST")
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
   FROM ACRM_F_CI_CUST_SIMILARLST A                            --
   WHERE NOT EXISTS (SELECT 1 FROM (SELECT TYP_ID FROM ACRM_F_CI_CUST_SIMILARLST B WHERE A.FR_ID=B.FR_ID
	 GROUP BY B.TYP_ID HAVING COUNT(DISTINCT B.CUSTOM_ID_MAIN) = 1) B WHERE A.TYP_ID=B.TYP_ID)
          """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_SIMILARLST = sqlContext.sql(sql)
ACRM_F_CI_CUST_SIMILARLST.registerTempTable("ACRM_F_CI_CUST_SIMILARLST")
dfn="ACRM_F_CI_CUST_SIMILARLST/"+V_DT+".parquet"
ACRM_F_CI_CUST_SIMILARLST.cache()
nrows = ACRM_F_CI_CUST_SIMILARLST.count()
ACRM_F_CI_CUST_SIMILARLST.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_SIMILARLST.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_SIMILARLST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
#ACRM_F_CI_CUST_SIMILARLST备份当天跑批全量数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST/"+V_DT+".parquet /"+dbname+"/ACRM_F_CI_CUST_SIMILARLST_BK/")

#任务[21] 001-06::删除系统来源中间表
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE_BK/'+V_DT+'.parquet/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
sql = """
 SELECT A.ID                    AS ID 
       ,A.ODS_CUST_ID           AS ODS_CUST_ID 
       ,A.ODS_CUST_NAME         AS ODS_CUST_NAME 
       ,A.SOURCE_CUST_ID        AS SOURCE_CUST_ID 
       ,A.SOURCE_CUST_NAME      AS SOURCE_CUST_NAME 
       ,A.CERT_TYPE             AS CERT_TYPE 
       ,A.CERT_NO               AS CERT_NO 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.ODS_CUST_TYPE         AS ODS_CUST_TYPE 
       ,A.CUST_STAT             AS CUST_STAT 
       ,A.BEGIN_DATE            AS BEGIN_DATE 
       ,A.END_DATE              AS END_DATE 
       ,A.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,A.CERT_FLAG             AS CERT_FLAG 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --
   WHERE NOT EXISTS(SELECT 1 FROM F_CI_CUST_MERGERLST B WHERE A.ODS_CUST_ID = B.CUSTOM_ID_BEFORE AND UNION_FLAG = '1' AND B.FR_ID=A.FR_ID
                   AND B.ODS_ST_DATE = V_DT )
           """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE.cache()
nrows = OCRM_F_CI_SYS_RESOURCE.count()
OCRM_F_CI_SYS_RESOURCE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/")


#任务[21] 001-07::更新统一客户信息、对公客户信息表、零售客户信息表
#删除统一客户信息、对公客户基本信息、个人客户基本信息、客户编号映射关系、关注客户中被合并客户信息
V_STEP = V_STEP + 1
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC_BK/'+V_DT+'.parquet/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_ABBR             AS CUST_ABBR 
       ,A.CUST_STAT             AS CUST_STAT 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.CERT_TYPE             AS CERT_TYPE 
       ,A.CERT_NUM              AS CERT_NUM 
       ,A.CUST_CRELVL           AS CUST_CRELVL 
       ,A.CUST_EVALUATEDATE     AS CUST_EVALUATEDATE 
       ,A.CUST_EVALUATEOVERDATE AS CUST_EVALUATEOVERDATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.CRM_DT                AS CRM_DT 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_STRATEGY           AS IF_STRATEGY 
       ,A.OBJ_RATING            AS OBJ_RATING 
       ,A.OBJ_DATE              AS OBJ_DATE 
       ,A.OLD_OBJ_RATING        AS OLD_OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,A.SUB_DATE              AS SUB_DATE 
       ,A.OLD_SUB_RATING        AS OLD_SUB_RATING 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUST_LEV              AS CUST_LEV 
       ,A.HB_FLAG               AS HB_FLAG 
       ,A.BILL_REPLACE_FLAG     AS BILL_REPLACE_FLAG 
       ,A.OBLIGATION_FLAG       AS OBLIGATION_FLAG 
       ,A.WAGE_ARREAR_FALG      AS WAGE_ARREAR_FALG 
       ,A.TAXES_FLAG            AS TAXES_FLAG 
       ,A.USURY_FLAG            AS USURY_FLAG 
       ,A.OPERATION_SAFE_FALG   AS OPERATION_SAFE_FALG 
       ,A.OBJ_NAME              AS OBJ_NAME 
       ,A.OLD_OBJ_NAME          AS OLD_OBJ_NAME 
       ,A.GRADE_HAND_NAME       AS GRADE_HAND_NAME 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.IS_CRE                AS IS_CRE 
       ,A.FR_ID                 AS FR_ID 
       ,A.UN_NULL_RATE          AS UN_NULL_RATE 
       ,A.HOLD_PRO_FLAG         AS HOLD_PRO_FLAG 
       ,A.GPS                   AS GPS 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.LINK_MAN              AS LINK_MAN 
       ,A.CUST_ADDR             AS CUST_ADDR 
       ,A.VALID_FLG             AS VALID_FLG 
   FROM OCRM_F_CI_CUST_DESC A                                  --
   WHERE NOT EXISTS(SELECT 1 FROM F_CI_CUST_MERGERLST B WHERE A.CUST_ID = B.CUSTOM_ID_BEFORE AND B.UNION_FLAG = '1' AND B.FR_ID=A.FR_ID)
        """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
dfn="OCRM_F_CI_CUST_DESC/"+V_DT+".parquet"
OCRM_F_CI_CUST_DESC.cache()
nrows = OCRM_F_CI_CUST_DESC.count()
OCRM_F_CI_CUST_DESC.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_DESC.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/")

#任务[21] 001-08::
V_STEP = V_STEP + 1
#读取当天备份文件
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO_BK/'+V_DT+'.parquet/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.COM_BELONG            AS COM_BELONG 
       ,A.HOLDING_TYP           AS HOLDING_TYP 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.OBJECT_RATE           AS OBJECT_RATE 
       ,A.SUBJECT_RATE          AS SUBJECT_RATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.RATE_DATE             AS RATE_DATE 
       ,A.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,A.IF_SMALL              AS IF_SMALL 
       ,A.IF_IBK                AS IF_IBK 
       ,A.PLICY_TYP             AS PLICY_TYP 
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,A.AREA_ID               AS AREA_ID 
       ,A.UNION_FLAG            AS UNION_FLAG 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.AUTH_ORG              AS AUTH_ORG 
       ,A.OPEN_ORG1             AS OPEN_ORG1 
       ,A.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,A.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LOAN_CARDNO           AS LOAN_CARDNO 
       ,A.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,A.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,A.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,A.LOAN_RATE             AS LOAN_RATE 
       ,A.DEP_RATE              AS DEP_RATE 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.SETTLE_RATIO          AS SETTLE_RATIO 
       ,A.BASIC_ACCT            AS BASIC_ACCT 
       ,A.LEGAL_NAME            AS LEGAL_NAME 
       ,A.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,A.LINK_MOBILE           AS LINK_MOBILE 
       ,A.FAX_NO                AS FAX_NO 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.REGISTER_ZIP          AS REGISTER_ZIP 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.E_MAIL                AS E_MAIL 
       ,A.WEB_ADDRESS           AS WEB_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,A.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.OPEN_DATE             AS OPEN_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.LAST_DATE             AS LAST_DATE 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_OWNER             AS BUS_OWNER 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,A.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,A.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,A.MERGE_ORG             AS MERGE_ORG 
       ,A.MERGE_DATE            AS MERGE_DATE 
       ,A.MERGE_OFFICER         AS MERGE_OFFICER 
       ,A.REMARK1               AS REMARK1 
       ,A.BIRTH_DATE            AS BIRTH_DATE 
       ,A.KEY_CERT_NO           AS KEY_CERT_NO 
       ,A.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,A.KEY_CUST_ID           AS KEY_CUST_ID 
       ,A.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,A.EDU_LEVEL             AS EDU_LEVEL 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,A.HOUSE_ZIP             AS HOUSE_ZIP 
       ,A.DUTY_TIME             AS DUTY_TIME 
       ,A.SHARE_HOLDING         AS SHARE_HOLDING 
       ,A.DUTY                  AS DUTY 
       ,A.REMARK2               AS REMARK2 
       ,A.SEX                   AS SEX 
       ,A.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.GREEN_FLAG            AS GREEN_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --
   WHERE NOT EXISTS(SELECT 1 FROM F_CI_CUST_MERGERLST B WHERE A.CUST_ID = B.CUSTOM_ID_BEFORE AND B.UNION_FLAG = '1'AND B.FR_ID=A.FR_ID) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO.cache()
nrows = OCRM_F_CI_COM_CUST_INFO.count()
OCRM_F_CI_COM_CUST_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/")


#任务[21] 001-09::
V_STEP = V_STEP + 1
OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO_BK/'+V_DT+'.parquet/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ENAME            AS CUST_ENAME 
       ,A.CUST_ENAME1           AS CUST_ENAME1 
       ,A.CUST_BIR              AS CUST_BIR 
       ,A.CUST_RESCNTY          AS CUST_RESCNTY 
       ,A.CUST_MRG              AS CUST_MRG 
       ,A.CUST_SEX              AS CUST_SEX 
       ,A.CUST_NATION           AS CUST_NATION 
       ,A.CUST_REGISTER         AS CUST_REGISTER 
       ,A.CUST_REGTYP           AS CUST_REGTYP 
       ,A.CUST_REGADDR          AS CUST_REGADDR 
       ,A.CUST_CITISHIP         AS CUST_CITISHIP 
       ,A.CUST_USED_NAME        AS CUST_USED_NAME 
       ,A.CUST_TTL              AS CUST_TTL 
       ,A.CUST_GROUP            AS CUST_GROUP 
       ,A.CUST_EVADATE          AS CUST_EVADATE 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUS_TYPE              AS CUS_TYPE 
       ,A.IS_GOODMAN            AS IS_GOODMAN 
       ,A.IS_TECH               AS IS_TECH 
       ,A.IS_GUDONG             AS IS_GUDONG 
       ,A.IS_SY                 AS IS_SY 
       ,A.IS_BUSSMA             AS IS_BUSSMA 
       ,A.CUST_FAMSTATUS        AS CUST_FAMSTATUS 
       ,A.CUST_HEALTH           AS CUST_HEALTH 
       ,A.CUST_LANGUAGE         AS CUST_LANGUAGE 
       ,A.CUST_RELIGION         AS CUST_RELIGION 
       ,A.CUST_POLIFACE         AS CUST_POLIFACE 
       ,A.CUST_RES_CNTY         AS CUST_RES_CNTY 
       ,A.CITY_COD              AS CITY_COD 
       ,A.CUST_LOCALYEAR        AS CUST_LOCALYEAR 
       ,A.ID_BLACKLIST          AS ID_BLACKLIST 
       ,A.CUST_ATT_FMDAT        AS CUST_ATT_FMDAT 
       ,A.CUST_ATT_DESC         AS CUST_ATT_DESC 
       ,A.FIN_RISK_ASS          AS FIN_RISK_ASS 
       ,A.RISK_APPETITE         AS RISK_APPETITE 
       ,A.EVA_DATE              AS EVA_DATE 
       ,A.FIN_WARN              AS FIN_WARN 
       ,A.CUST_CRT_SCT          AS CUST_CRT_SCT 
       ,A.CUST_HEIGHT           AS CUST_HEIGHT 
       ,A.CUST_WEIGHT           AS CUST_WEIGHT 
       ,A.CUST_BLOTYP           AS CUST_BLOTYP 
       ,A.TEMP_RESIDENCE        AS TEMP_RESIDENCE 
       ,A.BK_RELASHIP           AS BK_RELASHIP 
       ,A.CUST_EDU_LVL_COD      AS CUST_EDU_LVL_COD 
       ,A.GIHHEST_DEGREE        AS GIHHEST_DEGREE 
       ,A.GRADUATION            AS GRADUATION 
       ,A.GRADUATE_NAME         AS GRADUATE_NAME 
       ,A.CUST_OCCUP_COD        AS CUST_OCCUP_COD 
       ,A.CUST_OCCUP_COD1       AS CUST_OCCUP_COD1 
       ,A.OCCP_STATE            AS OCCP_STATE 
       ,A.WORK_STATE            AS WORK_STATE 
       ,A.CUST_POSN             AS CUST_POSN 
       ,A.CUST_TITL             AS CUST_TITL 
       ,A.TERM_OF_CON           AS TERM_OF_CON 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.CUST_WORK_YEAR        AS CUST_WORK_YEAR 
       ,A.CUST_WORK_UNIT_NAME   AS CUST_WORK_UNIT_NAME 
       ,A.CUST_UTELL            AS CUST_UTELL 
       ,A.CUST_UTYP             AS CUST_UTYP 
       ,A.DEPT                  AS DEPT 
       ,A.IS_CONTROLLER         AS IS_CONTROLLER 
       ,A.SOCIAL_DUTY           AS SOCIAL_DUTY 
       ,A.PER_DESCRIB           AS PER_DESCRIB 
       ,A.PER_RESUME            AS PER_RESUME 
       ,A.IS_FARMER_FLG         AS IS_FARMER_FLG 
       ,A.HOUHOLD_CLASS         AS HOUHOLD_CLASS 
       ,A.CUST_TEAMNAME         AS CUST_TEAMNAME 
       ,A.VILLAGE_NAME          AS VILLAGE_NAME 
       ,A.IS_VILLAGECADRE       AS IS_VILLAGECADRE 
       ,A.IS_MEDICARE           AS IS_MEDICARE 
       ,A.IS_POORISNO           AS IS_POORISNO 
       ,A.MAKUP                 AS MAKUP 
       ,A.INDUSTRYTYPE          AS INDUSTRYTYPE 
       ,A.MOSTBUSINESS          AS MOSTBUSINESS 
       ,A.BUSINESSADD           AS BUSINESSADD 
       ,A.LICENSENO             AS LICENSENO 
       ,A.LICENSEDATE           AS LICENSEDATE 
       ,A.TAXNO                 AS TAXNO 
       ,A.TAXNO1                AS TAXNO1 
       ,A.MAINPROORINCOME       AS MAINPROORINCOME 
       ,A.ADMIN_LVL             AS ADMIN_LVL 
       ,A.WORK_PERMIT           AS WORK_PERMIT 
       ,A.LINKMAN               AS LINKMAN 
       ,A.OFF_ADDR              AS OFF_ADDR 
       ,A.OFF_ZIP               AS OFF_ZIP 
       ,A.MICRO_BLOG            AS MICRO_BLOG 
       ,A.FAX                   AS FAX 
       ,A.MSN                   AS MSN 
       ,A.OTHER_CONTACT         AS OTHER_CONTACT 
       ,A.CUST_REG_ADDR2        AS CUST_REG_ADDR2 
       ,A.CI_ADDR               AS CI_ADDR 
       ,A.CUST_POSTCOD          AS CUST_POSTCOD 
       ,A.CUST_YEL_NO           AS CUST_YEL_NO 
       ,A.CUST_AREA_COD         AS CUST_AREA_COD 
       ,A.CUST_MBTELNO          AS CUST_MBTELNO 
       ,A.CUST_EMAIL            AS CUST_EMAIL 
       ,A.CUST_SUB_TEL          AS CUST_SUB_TEL 
       ,A.CUST_WEBSITE          AS CUST_WEBSITE 
       ,A.CUST_WORKADDR         AS CUST_WORKADDR 
       ,A.CUST_COMMADD          AS CUST_COMMADD 
       ,A.CUST_COMZIP           AS CUST_COMZIP 
       ,A.CUST_WORKZIP          AS CUST_WORKZIP 
       ,A.CUST_RELIGNLISM       AS CUST_RELIGNLISM 
       ,A.CUST_EFFSTATUS        AS CUST_EFFSTATUS 
       ,A.CUST_ARREA            AS CUST_ARREA 
       ,A.CUST_FAMADDR          AS CUST_FAMADDR 
       ,A.CUST_VILLAGENO        AS CUST_VILLAGENO 
       ,A.NET_ADDR              AS NET_ADDR 
       ,A.CUST_CRE_TYP          AS CUST_CRE_TYP 
       ,A.CUST_CER_NO           AS CUST_CER_NO 
       ,A.CUST_EXPD_DT          AS CUST_EXPD_DT 
       ,A.CUST_CHK_FLG          AS CUST_CHK_FLG 
       ,A.CUST_CER_STS          AS CUST_CER_STS 
       ,A.CUST_SONO             AS CUST_SONO 
       ,A.CUST_PECON_RESUR      AS CUST_PECON_RESUR 
       ,A.CUST_MN_INCO          AS CUST_MN_INCO 
       ,A.CUST_ANNUAL_INCOME    AS CUST_ANNUAL_INCOME 
       ,A.PER_INCURR_YCODE      AS PER_INCURR_YCODE 
       ,A.PER_IN_ANOUNT         AS PER_IN_ANOUNT 
       ,A.PER_INCURR_MCODE      AS PER_INCURR_MCODE 
       ,A.PER_INCURR_FAMCODE    AS PER_INCURR_FAMCODE 
       ,A.FAM_INCOMEACC         AS FAM_INCOMEACC 
       ,A.OTH_FAMINCOME         AS OTH_FAMINCOME 
       ,A.CUST_TOT_ASS          AS CUST_TOT_ASS 
       ,A.CUST_TOT_DEBT         AS CUST_TOT_DEBT 
       ,A.CUST_FAM_NUM          AS CUST_FAM_NUM 
       ,A.CUST_DEPEND_NO        AS CUST_DEPEND_NO 
       ,A.CUST_OT_INCO          AS CUST_OT_INCO 
       ,A.CUST_HOUSE_TYP        AS CUST_HOUSE_TYP 
       ,A.WAGES_ACCOUNT         AS WAGES_ACCOUNT 
       ,A.OPEN_BANK             AS OPEN_BANK 
       ,A.CRE_RECORD            AS CRE_RECORD 
       ,A.HAVE_YDTCARD          AS HAVE_YDTCARD 
       ,A.IS_LIFSUR             AS IS_LIFSUR 
       ,A.IS_ILLSUR             AS IS_ILLSUR 
       ,A.IS_ENDOSUR            AS IS_ENDOSUR 
       ,A.HAVE_CAR              AS HAVE_CAR 
       ,A.AVG_ASS               AS AVG_ASS 
       ,A.REMARK                AS REMARK 
       ,A.REC_UPSYS             AS REC_UPSYS 
       ,A.REC_UPMEC             AS REC_UPMEC 
       ,A.REC_UODATE            AS REC_UODATE 
       ,A.REC_UPMAN             AS REC_UPMAN 
       ,A.SOUR_SYS              AS SOUR_SYS 
       ,A.PLAT_DATE             AS PLAT_DATE 
       ,A.COMBIN_FLG            AS COMBIN_FLG 
       ,A.DATE_SOU              AS DATE_SOU 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.CUST_FANID            AS CUST_FANID 
       ,A.HZ_CERTYPE            AS HZ_CERTYPE 
       ,A.HZ_CERTID             AS HZ_CERTID 
       ,A.CUST_TEAMNO           AS CUST_TEAMNO 
       ,A.HZ_NAME               AS HZ_NAME 
       ,A.CUST_FAMNUM           AS CUST_FAMNUM 
       ,A.CUST_WEANAME          AS CUST_WEANAME 
       ,A.CUST_WEAVALUE         AS CUST_WEAVALUE 
       ,A.CONLANDAREA           AS CONLANDAREA 
       ,A.CONPOOLAREA           AS CONPOOLAREA 
       ,A.CUST_CHILDREN         AS CUST_CHILDREN 
       ,A.NUM_OF_CHILD          AS NUM_OF_CHILD 
       ,A.TOTINCO_OF_CH         AS TOTINCO_OF_CH 
       ,A.CUST_HOUSE            AS CUST_HOUSE 
       ,A.CUST_HOUSECOUNT       AS CUST_HOUSECOUNT 
       ,A.CUST_HOUSEAREA        AS CUST_HOUSEAREA 
       ,A.CUST_PRIVATECAR       AS CUST_PRIVATECAR 
       ,A.CAR_NUM_DESC          AS CAR_NUM_DESC 
       ,A.CUST_FAMILYSORT       AS CUST_FAMILYSORT 
       ,A.CUST_OTHMEG           AS CUST_OTHMEG 
       ,A.CUST_SUMJF            AS CUST_SUMJF 
       ,A.CUST_ZCJF             AS CUST_ZCJF 
       ,A.CUST_FZJF             AS CUST_FZJF 
       ,A.CUST_CONSUMEJF        AS CUST_CONSUMEJF 
       ,A.CUST_CHANNELJF        AS CUST_CHANNELJF 
       ,A.CUST_MIDBUSJF         AS CUST_MIDBUSJF 
       ,A.CRECARD_POINTS        AS CRECARD_POINTS 
       ,A.QQ                    AS QQ 
       ,A.MICRO_MSG             AS MICRO_MSG 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.FAMILY_SAFE           AS FAMILY_SAFE 
       ,A.BAD_HABIT_FLAG        AS BAD_HABIT_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_PER_CUST_INFO A                              --
   WHERE NOT EXISTS(SELECT 1 FROM F_CI_CUST_MERGERLST B WHERE A.CUST_ID = B.CUSTOM_ID_BEFORE AND B.UNION_FLAG = '1' AND B.FR_ID=A.FR_ID) """
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_PER_CUST_INFO = sqlContext.sql(sql)
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
dfn="OCRM_F_CI_PER_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_PER_CUST_INFO.cache()
nrows = OCRM_F_CI_PER_CUST_INFO.count()
OCRM_F_CI_PER_CUST_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_PER_CUST_INFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_PER_CUST_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_PER_CUST_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_PER_CUST_INFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_PER_CUST_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_PER_CUST_INFO_BK/")


#任务[21] 001-10::获取核心合并客户记录并删除无用数据
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUSTOM_ID             AS CUSTOM_ID 
       ,A.CUSTOM_ID_BEFORE      AS CUSTOM_ID_BEFORE 
       ,A.RN                    AS RN 
       ,A.FR_ID                 AS FR_ID 
 FROM (
     SELECT A.CUSTOM_ID             AS CUSTOM_ID 
           ,A.CUSTOM_ID_BEFORE      AS CUSTOM_ID_BEFORE 
           ,ROW_NUMBER() OVER(PARTITION BY A.CUSTOM_ID_BEFORE ORDER BY A.ODS_ST_DATE DESC) AS RN 
           ,A.FR_ID                 AS FR_ID 
      FROM F_CI_CUST_MERGERLST A 
      WHERE A.CUSTOM_ID_BEFORE NOT LIKE '1111%' ) A
 WHERE A.RN = '1'"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_MERGERLST = sqlContext.sql(sql)
TMP_MERGERLST.registerTempTable("TMP_MERGERLST")
dfn="TMP_MERGERLST/"+V_DT+".parquet"
TMP_MERGERLST.cache()
nrows = TMP_MERGERLST.count()
TMP_MERGERLST.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_MERGERLST.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_MERGERLST/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_MERGERLST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-11::对于A->B,B->C 的客户更新为 A->C
V_STEP = V_STEP + 1

sql = """
 SELECT DISTINCT
        CASE WHEN B.CUSTOM_ID IS NULL THEN A.CUSTOM_ID ELSE B.CUSTOM_ID END AS CUSTOM_ID
       ,A.CUSTOM_ID_BEFORE      AS CUSTOM_ID_BEFORE 
       ,A.RN                    AS RN 
       ,A.FR_ID                 AS FR_ID 
 FROM TMP_MERGERLST  A
 LEFT JOIN TMP_MERGERLST B
 ON A.FR_ID= B.FR_ID AND A.CUSTOM_ID = B.CUSTOM_ID_BEFORE"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_MERGERLST = sqlContext.sql(sql)
TMP_MERGERLST.registerTempTable("TMP_MERGERLST")
dfn="TMP_MERGERLST/"+V_DT+".parquet"
TMP_MERGERLST.cache()
nrows = TMP_MERGERLST.count()
TMP_MERGERLST.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_MERGERLST.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_MERGERLST/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_MERGERLST lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-12::更新负债协议
V_STEP = V_STEP + 1

ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO_BK/'+V_DT+'.parquet/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

sql = """
 SELECT distinct 
        A.AGREEMENT_ID          AS AGREEMENT_ID 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,A.ODS_ACCT_NO           AS ODS_ACCT_NO 
       ,B.CUSTOM_ID             AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CYNO                  AS CYNO 
       ,A.ORG_ID                AS ORG_ID 
       ,A.PERD                  AS PERD 
       ,A.INRT                  AS INRT 
       ,A.BSKD                  AS BSKD 
       ,A.AMAL                  AS AMAL 
       ,A.DETP                  AS DETP 
       ,A.OPEN_DT               AS OPEN_DT 
       ,A.OPEN_TL               AS OPEN_TL 
       ,A.CLOSE_DT              AS CLOSE_DT 
       ,A.CLOSE_TL              AS CLOSE_TL 
       ,A.S_CUST_NO             AS S_CUST_NO 
       ,A.TD_MU_DT              AS TD_MU_DT 
       ,A.UP_BAL                AS UP_BAL 
       ,A.UP_BAL_RMB            AS UP_BAL_RMB 
       ,A.BAL_RMB               AS BAL_RMB 
       ,A.BAL_US                AS BAL_US 
       ,A.TD_IR_TP              AS TD_IR_TP 
       ,A.AGREENMENT_RATE       AS AGREENMENT_RATE 
       ,A.ACCT_STATUS           AS ACCT_STATUS 
       ,A.TD_VL_DT              AS TD_VL_DT 
       ,A.MS_AC_BAL             AS MS_AC_BAL 
       ,A.LTDT                  AS LTDT 
       ,A.MSFG                  AS MSFG 
       ,A.FSFG                  AS FSFG 
       ,A.FZAM                  AS FZAM 
       ,A.SMFG                  AS SMFG 
       ,A.LKBL                  AS LKBL 
       ,A.LKFG                  AS LKFG 
       ,A.STCD                  AS STCD 
       ,A.ACCONT_TYPE           AS ACCONT_TYPE 
       ,A.MONTH_AVG             AS MONTH_AVG 
       ,A.QUARTER_DAILY         AS QUARTER_DAILY 
       ,A.YEAR_AVG              AS YEAR_AVG 
       ,A.MVAL_RMB              AS MVAL_RMB 
       ,A.SYS_NO                AS SYS_NO 
       ,A.PERD_UNIT             AS PERD_UNIT 
       ,A.INTE_BEAR_TERM        AS INTE_BEAR_TERM 
       ,A.INTE_BEAR_MODE        AS INTE_BEAR_MODE 
       ,A.INTEREST_SETTLEMENT   AS INTEREST_SETTLEMENT 
       ,A.DEPOSIT_RESE_REQ      AS DEPOSIT_RESE_REQ 
       ,A.CRM_DT                AS CRM_DT 
       ,A.ITEM                  AS ITEM 
       ,A.SBIT                  AS SBIT 
       ,A.SSIT                  AS SSIT 
       ,A.CONNTR_NO             AS CONNTR_NO 
       ,A.MONTH_RMB             AS MONTH_RMB 
       ,A.QUARTER_RMB           AS QUARTER_RMB 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.IS_CARD_TERM          AS IS_CARD_TERM 
       ,A.CARD_NO               AS CARD_NO 
       ,A.FR_ID                 AS FR_ID 
       ,A.CURR_IDEN             AS CURR_IDEN 
       ,A.CARD_TYPE             AS CARD_TYPE 
   FROM ACRM_F_DP_SAVE_INFO A                                  --
  INNER JOIN TMP_MERGERLST B                                   --
     ON A.FR_ID                 = B.FR_ID 
    AND (A.CUST_ID = B.CUSTOM_ID_BEFORE OR A.CUST_ID = B.CUSTOM_ID)"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_DP_SAVE_INFO_INNTMP1.registerTempTable("ACRM_F_DP_SAVE_INFO_INNTMP1")

#ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
#ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
sql = """
 SELECT DST.AGREEMENT_ID                                        --协议标识:src.AGREEMENT_ID
       ,DST.PRODUCT_ID                                         --产品标识:src.PRODUCT_ID
       ,DST.ODS_ACCT_NO                                        --账户:src.ODS_ACCT_NO
       ,DST.CUST_ID                                            --客户标识:src.CUST_ID
       ,DST.CUST_NAME                                          --:src.CUST_NAME
       ,DST.CYNO                                               --币种:src.CYNO
       ,DST.ORG_ID                                             --开户机构代码:src.ORG_ID
       ,DST.PERD                                               --期限:src.PERD
       ,DST.INRT                                               --利率:src.INRT
       ,DST.BSKD                                               --业务种类:src.BSKD
       ,DST.AMAL                                               --年积数:src.AMAL
       ,DST.DETP                                               --储种:src.DETP
       ,DST.OPEN_DT                                            --开户日期:src.OPEN_DT
       ,DST.OPEN_TL                                            --开户柜员:src.OPEN_TL
       ,DST.CLOSE_DT                                           --销户日期:src.CLOSE_DT
       ,DST.CLOSE_TL                                           --销户柜员:src.CLOSE_TL
       ,DST.S_CUST_NO                                          --原系统客户号:src.S_CUST_NO
       ,DST.TD_MU_DT                                           --到期日:src.TD_MU_DT
       ,DST.UP_BAL                                             --上日余额:src.UP_BAL
       ,DST.UP_BAL_RMB                                         --折人民币上日余额:src.UP_BAL_RMB
       ,DST.BAL_RMB                                            --折人民币余额:src.BAL_RMB
       ,DST.BAL_US                                             --折美元余额:src.BAL_US
       ,DST.TD_IR_TP                                           --利率种类:src.TD_IR_TP
       ,DST.AGREENMENT_RATE                                    --协议利率:src.AGREENMENT_RATE
       ,DST.ACCT_STATUS                                        --账户状态:src.ACCT_STATUS
       ,DST.TD_VL_DT                                           --起息日期:src.TD_VL_DT
       ,DST.MS_AC_BAL                                          --帐户余额:src.MS_AC_BAL
       ,DST.LTDT                                               --上次交易日:src.LTDT
       ,DST.MSFG                                               --挂失标志:src.MSFG
       ,DST.FSFG                                               --冻结暂禁标志:src.FSFG
       ,DST.FZAM                                               --冻结金额:src.FZAM
       ,DST.SMFG                                               --印鉴挂失标志:src.SMFG
       ,DST.LKBL                                               --看管余额:src.LKBL
       ,DST.LKFG                                               --抵押看管标志:src.LKFG
       ,DST.STCD                                               --记录状态:src.STCD
       ,DST.ACCONT_TYPE                                        --账户类型:src.ACCONT_TYPE
       ,DST.MONTH_AVG                                          --月日均:src.MONTH_AVG
       ,DST.QUARTER_DAILY                                      --季日均:src.QUARTER_DAILY
       ,DST.YEAR_AVG                                           --年日均:src.YEAR_AVG
       ,DST.MVAL_RMB                                           --折人民币年日均:src.MVAL_RMB
       ,DST.SYS_NO                                             --业务系统:src.SYS_NO
       ,DST.PERD_UNIT                                          --存期单位:src.PERD_UNIT
       ,DST.INTE_BEAR_TERM                                     --计息期限:src.INTE_BEAR_TERM
       ,DST.INTE_BEAR_MODE                                     --计息方式:src.INTE_BEAR_MODE
       ,DST.INTEREST_SETTLEMENT                                --结息方式:src.INTEREST_SETTLEMENT
       ,DST.DEPOSIT_RESE_REQ                                   --缴存存款准备金方式:src.DEPOSIT_RESE_REQ
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.ITEM                                               --科目号:src.ITEM
       ,DST.SBIT                                               --子目:src.SBIT
       ,DST.SSIT                                               --细目:src.SSIT
       ,DST.CONNTR_NO                                          --:src.CONNTR_NO
       ,DST.MONTH_RMB                                          --:src.MONTH_RMB
       ,DST.QUARTER_RMB                                        --:src.QUARTER_RMB
       ,DST.CUST_TYP                                           --:src.CUST_TYP
       ,DST.IS_CARD_TERM                                       --:src.IS_CARD_TERM
       ,DST.CARD_NO                                            --:src.CARD_NO
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.CARD_TYPE                                          --卡种:src.CARD_TYPE
   FROM ACRM_F_DP_SAVE_INFO DST 
   LEFT JOIN ACRM_F_DP_SAVE_INFO_INNTMP1 SRC 
     ON DST.FR_ID               = SRC.FR_ID 
    AND DST.CUST_ID = SRC.CUST_ID
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_SAVE_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_DP_SAVE_INFO/"+V_DT+".parquet"
ACRM_F_DP_SAVE_INFO_INNTMP2=ACRM_F_DP_SAVE_INFO_INNTMP2.unionAll(ACRM_F_DP_SAVE_INFO_INNTMP1)
ACRM_F_DP_SAVE_INFO_INNTMP1.cache()
ACRM_F_DP_SAVE_INFO_INNTMP2.cache()
nrowsi = ACRM_F_DP_SAVE_INFO_INNTMP1.count()
nrowsa = ACRM_F_DP_SAVE_INFO_INNTMP2.count()
ACRM_F_DP_SAVE_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_DP_SAVE_INFO_INNTMP1.unpersist()
ACRM_F_DP_SAVE_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_SAVE_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_DP_SAVE_INFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_DP_SAVE_INFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_DP_SAVE_INFO_BK/")


#任务[12] 001-13::更新负债协议
V_STEP = V_STEP + 1

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO_BK/'+V_DT+'.parquet/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")

sql = """
 SELECT distinct
        A.AGREEMENT_ID          AS AGREEMENT_ID 
       ,B.CUSTOM_ID             AS CUST_ID 
       ,A.CONT_NO               AS CONT_NO 
       ,A.CN_CONT_NO            AS CN_CONT_NO 
       ,A.ACCT_NO               AS ACCT_NO 
       ,A.CORE_CUST_NO          AS CORE_CUST_NO 
       ,A.CORE_CUST_NAME        AS CORE_CUST_NAME 
       ,A.LOAN_TYP              AS LOAN_TYP 
       ,A.LOAN_QUAL             AS LOAN_QUAL 
       ,A.ENT_LOAN_TYP          AS ENT_LOAN_TYP 
       ,A.LOAN_POL_PRO_CLASS    AS LOAN_POL_PRO_CLASS 
       ,A.SPEC_LOAN_TYE         AS SPEC_LOAN_TYE 
       ,A.PRODUCT_ID            AS PRODUCT_ID 
       ,A.GRNT_TYP              AS GRNT_TYP 
       ,A.GRNT_TYP2             AS GRNT_TYP2 
       ,A.GRNT_TYP3             AS GRNT_TYP3 
       ,A.LOAN_INVEST           AS LOAN_INVEST 
       ,A.FOREXCH_RATE          AS FOREXCH_RATE 
       ,A.CONT_AMT              AS CONT_AMT 
       ,A.GIVE_OUT_AMT          AS GIVE_OUT_AMT 
       ,A.RECOVER_AMT           AS RECOVER_AMT 
       ,A.BM_MOT                AS BM_MOT 
       ,A.FLOAT_THAN            AS FLOAT_THAN 
       ,A.MON_INTE_RATE         AS MON_INTE_RATE 
       ,A.LATE_MOT              AS LATE_MOT 
       ,A.DEFUT_MOT             AS DEFUT_MOT 
       ,A.FLT_CYCL              AS FLT_CYCL 
       ,A.ACTU_WITHDR           AS ACTU_WITHDR 
       ,A.AGA_PRIC              AS AGA_PRIC 
       ,A.AGA_PRIC_RAT          AS AGA_PRIC_RAT 
       ,A.REPAY_MODE            AS REPAY_MODE 
       ,A.REPAY_CYCL            AS REPAY_CYCL 
       ,A.INTC_CYCL             AS INTC_CYCL 
       ,A.INTR_CORR_MODE        AS INTR_CORR_MODE 
       ,A.YN_DISC               AS YN_DISC 
       ,A.LOAN_USE              AS LOAN_USE 
       ,A.REPAY_ORG             AS REPAY_ORG 
       ,A.TAKE_CGT_LINE         AS TAKE_CGT_LINE 
       ,A.TAKE_MAX_GRNT_AMT     AS TAKE_MAX_GRNT_AMT 
       ,A.CGT_LINE_FLG          AS CGT_LINE_FLG 
       ,A.MAX_GRNT_FLG          AS MAX_GRNT_FLG 
       ,A.SYN_LOAN_SIGN         AS SYN_LOAN_SIGN 
       ,A.BEFO_REPORT_FLG       AS BEFO_REPORT_FLG 
       ,A.MORT_PRO_NUM          AS MORT_PRO_NUM 
       ,A.SYN_PRO_NUM           AS SYN_PRO_NUM 
       ,A.CONT_SIGN_DT          AS CONT_SIGN_DT 
       ,A.CONT_STS              AS CONT_STS 
       ,A.SIGN_BANK             AS SIGN_BANK 
       ,A.AGENCY_BRAN           AS AGENCY_BRAN 
       ,A.LENDER                AS LENDER 
       ,A.MANAGE_BRAN           AS MANAGE_BRAN 
       ,A.RECORD_DATE           AS RECORD_DATE 
       ,A.REGISTRANT            AS REGISTRANT 
       ,A.FINAL_UPDATE_DATE     AS FINAL_UPDATE_DATE 
       ,A.STATI_BUSIN_VAR       AS STATI_BUSIN_VAR 
       ,A.SUPP_LOAN_TYP         AS SUPP_LOAN_TYP 
       ,A.MODE_PAYMENT          AS MODE_PAYMENT 
       ,A.YN_FIX_INTR           AS YN_FIX_INTR 
       ,A.FIX_FLOAT_POINTS      AS FIX_FLOAT_POINTS 
       ,A.BILL_QTY              AS BILL_QTY 
       ,A.SDPT_ACCT_NO          AS SDPT_ACCT_NO 
       ,A.FEE_RATIO             AS FEE_RATIO 
       ,A.RSK_EXPO_RATIO        AS RSK_EXPO_RATIO 
       ,A.SDPT_PATIO            AS SDPT_PATIO 
       ,A.SDPT_AMT              AS SDPT_AMT 
       ,A.SDPT_BAL              AS SDPT_BAL 
       ,A.DPTRPT_GRNT_PATIO     AS DPTRPT_GRNT_PATIO 
       ,A.DPTRPT_FACE_AMT       AS DPTRPT_FACE_AMT 
       ,A.DPTRPT_GRNT_AMT       AS DPTRPT_GRNT_AMT 
       ,A.DPTRPT_GRNT_BAL       AS DPTRPT_GRNT_BAL 
       ,A.BANNOTE_GRNT_PATIO    AS BANNOTE_GRNT_PATIO 
       ,A.BANNOTE_GRNT_AMT      AS BANNOTE_GRNT_AMT 
       ,A.BANNOTE_GRNT_BAL      AS BANNOTE_GRNT_BAL 
       ,A.WO_AMT                AS WO_AMT 
       ,A.SIGN_FLG              AS SIGN_FLG 
       ,A.BILL_TYP              AS BILL_TYP 
       ,A.ACCEPTED_A_NUMBER     AS ACCEPTED_A_NUMBER 
       ,A.BANNOTE_FACE_AMT      AS BANNOTE_FACE_AMT 
       ,A.FACTOR_TYP            AS FACTOR_TYP 
       ,A.BBK_DT                AS BBK_DT 
       ,A.WEI_AVER_INTR         AS WEI_AVER_INTR 
       ,A.WEI_AVER_DAY          AS WEI_AVER_DAY 
       ,A.LOGRT_KIND            AS LOGRT_KIND 
       ,A.LOGRT_TYP             AS LOGRT_TYP 
       ,A.CURR                  AS CURR 
       ,A.BAL                   AS BAL 
       ,A.BEGIN_DATE            AS BEGIN_DATE 
       ,A.END_DATE              AS END_DATE 
       ,A.ASSURER               AS ASSURER 
       ,A.BENF                  AS BENF 
       ,A.ASSET_SYS             AS ASSET_SYS 
       ,A.MONTH_AVG             AS MONTH_AVG 
       ,A.QUARTER_DAILY         AS QUARTER_DAILY 
       ,A.YEAR_AVG              AS YEAR_AVG 
       ,A.CHANGE_AMT            AS CHANGE_AMT 
       ,A.CRM_DT                AS CRM_DT 
       ,A.FACT_INT              AS FACT_INT 
       ,A.SHOULD_INT            AS SHOULD_INT 
       ,A.TERMYEAR              AS TERMYEAR 
       ,A.TERMMONTH             AS TERMMONTH 
       ,A.TERMDAY               AS TERMDAY 
       ,A.POUNDAGE_CNCY         AS POUNDAGE_CNCY 
       ,A.POUNDAGE_MONEY        AS POUNDAGE_MONEY 
       ,A.TRANS_AMT_USD         AS TRANS_AMT_USD 
       ,A.FEE_AMT               AS FEE_AMT 
       ,A.LN_APCL_FLG           AS LN_APCL_FLG 
       ,A.IS_EXTEND             AS IS_EXTEND 
       ,A.IS_OVERDUE            AS IS_OVERDUE 
       ,A.IS_ADVAN              AS IS_ADVAN 
       ,A.IS_BORROW_NEW         AS IS_BORROW_NEW 
       ,A.IS_FLAW               AS IS_FLAW 
       ,A.SUBJECTNO             AS SUBJECTNO 
       ,A.CLASSIFYRESULT        AS CLASSIFYRESULT 
       ,A.MANAGE_USERID         AS MANAGE_USERID 
       ,A.IS_INT_IN             AS IS_INT_IN 
       ,A.IS_INT_OUT            AS IS_INT_OUT 
       ,A.CURRENTTENRESULT      AS CURRENTTENRESULT 
       ,A.MVAL_RMB              AS MVAL_RMB 
       ,A.MONTH_RMB             AS MONTH_RMB 
       ,A.QUARTER_RMB           AS QUARTER_RMB 
       ,A.BAL_RMB               AS BAL_RMB 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A 
  INNER JOIN(
         SELECT DISTINCT 
                CUSTOM_ID 
               ,CUSTOM_ID_BEFORE 
               ,FR_ID 
           FROM F_CI_CUST_MERGERLST) B 
     ON A.CUST_ID = B.CUSTOM_ID_BEFORE
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP1.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP1")

#ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
#ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
sql = """
 SELECT DST.AGREEMENT_ID                                        --协议标识:src.AGREEMENT_ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CONT_NO                                            --合同编号:src.CONT_NO
       ,DST.CN_CONT_NO                                         --中文合同编号:src.CN_CONT_NO
       ,DST.ACCT_NO                                            --账号:src.ACCT_NO
       ,DST.CORE_CUST_NO                                       --核心客户码:src.CORE_CUST_NO
       ,DST.CORE_CUST_NAME                                     --核心客户名称:src.CORE_CUST_NAME
       ,DST.LOAN_TYP                                           --贷款形式:src.LOAN_TYP
       ,DST.LOAN_QUAL                                          --贷款性质:src.LOAN_QUAL
       ,DST.ENT_LOAN_TYP                                       --委托贷款种类:src.ENT_LOAN_TYP
       ,DST.LOAN_POL_PRO_CLASS                                 --贷款政策性质分类:src.LOAN_POL_PRO_CLASS
       ,DST.SPEC_LOAN_TYE                                      --特殊贷款类型:src.SPEC_LOAN_TYE
       ,DST.PRODUCT_ID                                         --产品标识:src.PRODUCT_ID
       ,DST.GRNT_TYP                                           --担保方式:src.GRNT_TYP
       ,DST.GRNT_TYP2                                          --担保方式2:src.GRNT_TYP2
       ,DST.GRNT_TYP3                                          --担保方式3:src.GRNT_TYP3
       ,DST.LOAN_INVEST                                        --贷款投向:src.LOAN_INVEST
       ,DST.FOREXCH_RATE                                       --汇率:src.FOREXCH_RATE
       ,DST.CONT_AMT                                           --合同金额(元):src.CONT_AMT
       ,DST.GIVE_OUT_AMT                                       --发放金额:src.GIVE_OUT_AMT
       ,DST.RECOVER_AMT                                        --回收金额:src.RECOVER_AMT
       ,DST.BM_MOT                                             --基准月利率(‰):src.BM_MOT
       ,DST.FLOAT_THAN                                         --浮动比:src.FLOAT_THAN
       ,DST.MON_INTE_RATE                                      --月利率:src.MON_INTE_RATE
       ,DST.LATE_MOT                                           --逾期月利率:src.LATE_MOT
       ,DST.DEFUT_MOT                                          --违约月利率:src.DEFUT_MOT
       ,DST.FLT_CYCL                                           --浮动周期:src.FLT_CYCL
       ,DST.ACTU_WITHDR                                        --实际提款日:src.ACTU_WITHDR
       ,DST.AGA_PRIC                                           --重新定价日:src.AGA_PRIC
       ,DST.AGA_PRIC_RAT                                       --重新定价利率:src.AGA_PRIC_RAT
       ,DST.REPAY_MODE                                         --还款方式:src.REPAY_MODE
       ,DST.REPAY_CYCL                                         --还款周期:src.REPAY_CYCL
       ,DST.INTC_CYCL                                          --计息周期:src.INTC_CYCL
       ,DST.INTR_CORR_MODE                                     --利率调整方式:src.INTR_CORR_MODE
       ,DST.YN_DISC                                            --是否贴息:src.YN_DISC
       ,DST.LOAN_USE                                           --借款用途:src.LOAN_USE
       ,DST.REPAY_ORG                                          --还款来源:src.REPAY_ORG
       ,DST.TAKE_CGT_LINE                                      --占用标准授信额度(RMB):src.TAKE_CGT_LINE
       ,DST.TAKE_MAX_GRNT_AMT                                  --占用最高额担保金额:src.TAKE_MAX_GRNT_AMT
       ,DST.CGT_LINE_FLG                                       --授信额度使用标志:src.CGT_LINE_FLG
       ,DST.MAX_GRNT_FLG                                       --最高额担保使用标志:src.MAX_GRNT_FLG
       ,DST.SYN_LOAN_SIGN                                      --银团贷款标志:src.SYN_LOAN_SIGN
       ,DST.BEFO_REPORT_FLG                                    --贷前调查报告标识:src.BEFO_REPORT_FLG
       ,DST.MORT_PRO_NUM                                       --按揭项目编号:src.MORT_PRO_NUM
       ,DST.SYN_PRO_NUM                                        --银团项目编号:src.SYN_PRO_NUM
       ,DST.CONT_SIGN_DT                                       --合同签订日期:src.CONT_SIGN_DT
       ,DST.CONT_STS                                           --合同状态:src.CONT_STS
       ,DST.SIGN_BANK                                          --签发行:src.SIGN_BANK
       ,DST.AGENCY_BRAN                                        --经办机构:src.AGENCY_BRAN
       ,DST.LENDER                                             --贷款机构:src.LENDER
       ,DST.MANAGE_BRAN                                        --管理机构:src.MANAGE_BRAN
       ,DST.RECORD_DATE                                        --登记日期:src.RECORD_DATE
       ,DST.REGISTRANT                                         --登记人:src.REGISTRANT
       ,DST.FINAL_UPDATE_DATE                                  --最后修改日期:src.FINAL_UPDATE_DATE
       ,DST.STATI_BUSIN_VAR                                    --统计业务品种:src.STATI_BUSIN_VAR
       ,DST.SUPP_LOAN_TYP                                      --支农贷款类型:src.SUPP_LOAN_TYP
       ,DST.MODE_PAYMENT                                       --支付方式:src.MODE_PAYMENT
       ,DST.YN_FIX_INTR                                        --浮动类型:src.YN_FIX_INTR
       ,DST.FIX_FLOAT_POINTS                                   --固定浮动基本点:src.FIX_FLOAT_POINTS
       ,DST.BILL_QTY                                           --汇票数量:src.BILL_QTY
       ,DST.SDPT_ACCT_NO                                       --保证金账号:src.SDPT_ACCT_NO
       ,DST.FEE_RATIO                                          --手续费率(万分号):src.FEE_RATIO
       ,DST.RSK_EXPO_RATIO                                     --风险敞口比例:src.RSK_EXPO_RATIO
       ,DST.SDPT_PATIO                                         --保证金比例:src.SDPT_PATIO
       ,DST.SDPT_AMT                                           --保证金金额(元):src.SDPT_AMT
       ,DST.SDPT_BAL                                           --保证金余额:src.SDPT_BAL
       ,DST.DPTRPT_GRNT_PATIO                                  --存单担保比例:src.DPTRPT_GRNT_PATIO
       ,DST.DPTRPT_FACE_AMT                                    --存单票面金额(RMB):src.DPTRPT_FACE_AMT
       ,DST.DPTRPT_GRNT_AMT                                    --存单担保金额:src.DPTRPT_GRNT_AMT
       ,DST.DPTRPT_GRNT_BAL                                    --存单担保余额:src.DPTRPT_GRNT_BAL
       ,DST.BANNOTE_GRNT_PATIO                                 --银票担保比例:src.BANNOTE_GRNT_PATIO
       ,DST.BANNOTE_GRNT_AMT                                   --银票担保金额:src.BANNOTE_GRNT_AMT
       ,DST.BANNOTE_GRNT_BAL                                   --银票担保余额:src.BANNOTE_GRNT_BAL
       ,DST.WO_AMT                                             --核销金额:src.WO_AMT
       ,DST.SIGN_FLG                                           --代签标志:src.SIGN_FLG
       ,DST.BILL_TYP                                           --票据类型:src.BILL_TYP
       ,DST.ACCEPTED_A_NUMBER                                  --承兑人行号:src.ACCEPTED_A_NUMBER
       ,DST.BANNOTE_FACE_AMT                                   --银票票面金额:src.BANNOTE_FACE_AMT
       ,DST.FACTOR_TYP                                         --保理形式:src.FACTOR_TYP
       ,DST.BBK_DT                                             --回购日期:src.BBK_DT
       ,DST.WEI_AVER_INTR                                      --加权平均利率:src.WEI_AVER_INTR
       ,DST.WEI_AVER_DAY                                       --加权平均天数:src.WEI_AVER_DAY
       ,DST.LOGRT_KIND                                         --保函种类:src.LOGRT_KIND
       ,DST.LOGRT_TYP                                          --保函类型:src.LOGRT_TYP
       ,DST.CURR                                               --币种:src.CURR
       ,DST.BAL                                                --余额:src.BAL
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.ASSURER                                            --保证人:src.ASSURER
       ,DST.BENF                                               --受益人:src.BENF
       ,DST.ASSET_SYS                                          --业务系统:src.ASSET_SYS
       ,DST.MONTH_AVG                                          --月日均:src.MONTH_AVG
       ,DST.QUARTER_DAILY                                      --季日均:src.QUARTER_DAILY
       ,DST.YEAR_AVG                                           --年日均:src.YEAR_AVG
       ,DST.CHANGE_AMT                                         --置换金额:src.CHANGE_AMT
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.FACT_INT                                           --本年实收利息:src.FACT_INT
       ,DST.SHOULD_INT                                         --本年应收利息:src.SHOULD_INT
       ,DST.TERMYEAR                                           --期限年:src.TERMYEAR
       ,DST.TERMMONTH                                          --期限月:src.TERMMONTH
       ,DST.TERMDAY                                            --期限日:src.TERMDAY
       ,DST.POUNDAGE_CNCY                                      --手续费币种:src.POUNDAGE_CNCY
       ,DST.POUNDAGE_MONEY                                     --手续费:src.POUNDAGE_MONEY
       ,DST.TRANS_AMT_USD                                      --金额折美元:src.TRANS_AMT_USD
       ,DST.FEE_AMT                                            --手续费(折人民币):src.FEE_AMT
       ,DST.LN_APCL_FLG                                        --呆帐核销标识:src.LN_APCL_FLG
       ,DST.IS_EXTEND                                          --是否展期:src.IS_EXTEND
       ,DST.IS_OVERDUE                                         --是否逾期:src.IS_OVERDUE
       ,DST.IS_ADVAN                                           --是否垫款:src.IS_ADVAN
       ,DST.IS_BORROW_NEW                                      --是否借新还旧:src.IS_BORROW_NEW
       ,DST.IS_FLAW                                            --是否瑕疵贷款:src.IS_FLAW
       ,DST.SUBJECTNO                                          --会计科目:src.SUBJECTNO
       ,DST.CLASSIFYRESULT                                     --五级分类:src.CLASSIFYRESULT
       ,DST.MANAGE_USERID                                      --当前管户人:src.MANAGE_USERID
       ,DST.IS_INT_IN                                          --:src.IS_INT_IN
       ,DST.IS_INT_OUT                                         --:src.IS_INT_OUT
       ,DST.CURRENTTENRESULT                                   --:src.CURRENTTENRESULT
       ,DST.MVAL_RMB                                           --:src.MVAL_RMB
       ,DST.MONTH_RMB                                          --:src.MONTH_RMB
       ,DST.QUARTER_RMB                                        --:src.QUARTER_RMB
       ,DST.BAL_RMB                                            --余额折人民币:src.BAL_RMB
       ,DST.CUST_TYP                                           --:src.CUST_TYP
       ,DST.FR_ID                                              --:src.FR_ID
   FROM ACRM_F_CI_ASSET_BUSI_PROTO DST 
   LEFT JOIN ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP1 SRC
   ON DST.CUST_ID = SRC.CUST_ID 
    AND DST.FR_ID                 = SRC.FR_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_CI_ASSET_BUSI_PROTO/"+V_DT+".parquet"
ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP2=ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP2.unionAll(ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP1)
ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP1.cache()
ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP2.cache()
nrowsi = ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP1.count()
nrowsa = ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP2.count()
ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP1.unpersist()
ACRM_F_CI_ASSET_BUSI_PROTO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_ASSET_BUSI_PROTO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_ASSET_BUSI_PROTO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_CI_ASSET_BUSI_PROTO/"+V_DT+".parquet /"+dbname+"/ACRM_F_CI_ASSET_BUSI_PROTO_BK/")



#任务[12] 001-14::存款积数表
V_STEP = V_STEP + 1

ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO_BK/'+V_DT+'.parquet/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")

sql = """
 SELECT distinct
        B.CUSTOM_ID             AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.TYPE                  AS TYPE 
       ,A.CURR                  AS CURR 
       ,A.CURR_IDEN             AS CURR_IDEN 
       ,A.ACCT_NO               AS ACCT_NO 
       ,A.OPEN_BRC              AS OPEN_BRC 
       ,A.PRDT_CODE             AS PRDT_CODE 
       ,A.AMOUNT                AS AMOUNT 
       ,A.LAST_AMOUNT           AS LAST_AMOUNT 
       ,A.YEAR                  AS YEAR 
       ,A.OLD_YEAR_BAL_SUM      AS OLD_YEAR_BAL_SUM 
       ,A.OLD_YEAR_BAL          AS OLD_YEAR_BAL 
       ,A.OLD_YEAR_DAYS         AS OLD_YEAR_DAYS 
       ,A.MONTH_BAL_SUM_1       AS MONTH_BAL_SUM_1 
       ,A.MONTH_DAYS_1          AS MONTH_DAYS_1 
       ,A.MONTH_BAL_SUM_2       AS MONTH_BAL_SUM_2 
       ,A.MONTH_DAYS_2          AS MONTH_DAYS_2 
       ,A.MONTH_BAL_SUM_3       AS MONTH_BAL_SUM_3 
       ,A.MONTH_DAYS_3          AS MONTH_DAYS_3 
       ,A.MONTH_BAL_SUM_4       AS MONTH_BAL_SUM_4 
       ,A.MONTH_DAYS_4          AS MONTH_DAYS_4 
       ,A.MONTH_BAL_SUM_5       AS MONTH_BAL_SUM_5 
       ,A.MONTH_DAYS_5          AS MONTH_DAYS_5 
       ,A.MONTH_BAL_SUM_6       AS MONTH_BAL_SUM_6 
       ,A.MONTH_DAYS_6          AS MONTH_DAYS_6 
       ,A.MONTH_BAL_SUM_7       AS MONTH_BAL_SUM_7 
       ,A.MONTH_DAYS_7          AS MONTH_DAYS_7 
       ,A.MONTH_BAL_SUM_8       AS MONTH_BAL_SUM_8 
       ,A.MONTH_DAYS_8          AS MONTH_DAYS_8 
       ,A.MONTH_BAL_SUM_9       AS MONTH_BAL_SUM_9 
       ,A.MONTH_DAYS_9          AS MONTH_DAYS_9 
       ,A.MONTH_BAL_SUM_10      AS MONTH_BAL_SUM_10 
       ,A.MONTH_DAYS_10         AS MONTH_DAYS_10 
       ,A.MONTH_BAL_SUM_11      AS MONTH_BAL_SUM_11 
       ,A.MONTH_DAYS_11         AS MONTH_DAYS_11 
       ,A.MONTH_BAL_SUM_12      AS MONTH_BAL_SUM_12 
       ,A.MONTH_DAYS_12         AS MONTH_DAYS_12 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.MONTH_BAL_1           AS MONTH_BAL_1 
       ,A.MONTH_BAL_2           AS MONTH_BAL_2 
       ,A.MONTH_BAL_3           AS MONTH_BAL_3 
       ,A.MONTH_BAL_4           AS MONTH_BAL_4 
       ,A.MONTH_BAL_5           AS MONTH_BAL_5 
       ,A.MONTH_BAL_6           AS MONTH_BAL_6 
       ,A.MONTH_BAL_7           AS MONTH_BAL_7 
       ,A.MONTH_BAL_8           AS MONTH_BAL_8 
       ,A.MONTH_BAL_9           AS MONTH_BAL_9 
       ,A.MONTH_BAL_10          AS MONTH_BAL_10 
       ,A.MONTH_BAL_11          AS MONTH_BAL_11 
       ,A.MONTH_BAL_12          AS MONTH_BAL_12 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
       ,A.YEAR_BAL_SUM          AS YEAR_BAL_SUM 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --
  INNER JOIN TMP_MERGERLST B                                   --
     ON A.FR_ID                 = B.FR_ID 
    AND (A.CUST_ID = B.CUSTOM_ID_BEFORE OR A.CUST_ID = B.CUSTOM_ID)"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO_INNTMP1")

#ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
#ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.TYPE                                               --存款类型:src.TYPE
       ,DST.CURR                                               --币种:src.CURR
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.ACCT_NO                                            --帐号:src.ACCT_NO
       ,DST.OPEN_BRC                                           --开户机构:src.OPEN_BRC
       ,DST.PRDT_CODE                                          --产品:src.PRDT_CODE
       ,DST.AMOUNT                                             --当日余额:src.AMOUNT
       ,DST.LAST_AMOUNT                                        --昨日余额:src.LAST_AMOUNT
       ,DST.YEAR                                               --年度:src.YEAR
       ,DST.OLD_YEAR_BAL_SUM                                   --去年12月余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --去年年末余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --去年12月天数:src.OLD_YEAR_DAYS
       ,DST.MONTH_BAL_SUM_1                                    --一月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --一月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --二月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --二月天数:src.MONTH_DAYS_2
       ,DST.MONTH_BAL_SUM_3                                    --3月余额积数:src.MONTH_BAL_SUM_3
       ,DST.MONTH_DAYS_3                                       --3月天数:src.MONTH_DAYS_3
       ,DST.MONTH_BAL_SUM_4                                    --4月余额积数:src.MONTH_BAL_SUM_4
       ,DST.MONTH_DAYS_4                                       --4月天数:src.MONTH_DAYS_4
       ,DST.MONTH_BAL_SUM_5                                    --5月余额积数:src.MONTH_BAL_SUM_5
       ,DST.MONTH_DAYS_5                                       --5月天数:src.MONTH_DAYS_5
       ,DST.MONTH_BAL_SUM_6                                    --6月余额积数:src.MONTH_BAL_SUM_6
       ,DST.MONTH_DAYS_6                                       --6月天数:src.MONTH_DAYS_6
       ,DST.MONTH_BAL_SUM_7                                    --7月余额积数:src.MONTH_BAL_SUM_7
       ,DST.MONTH_DAYS_7                                       --7月天数:src.MONTH_DAYS_7
       ,DST.MONTH_BAL_SUM_8                                    --8月余额积数:src.MONTH_BAL_SUM_8
       ,DST.MONTH_DAYS_8                                       --8月天数:src.MONTH_DAYS_8
       ,DST.MONTH_BAL_SUM_9                                    --9月余额积数:src.MONTH_BAL_SUM_9
       ,DST.MONTH_DAYS_9                                       --9月天数:src.MONTH_DAYS_9
       ,DST.MONTH_BAL_SUM_10                                   --10月余额积数:src.MONTH_BAL_SUM_10
       ,DST.MONTH_DAYS_10                                      --10月天数:src.MONTH_DAYS_10
       ,DST.MONTH_BAL_SUM_11                                   --11月余额积数:src.MONTH_BAL_SUM_11
       ,DST.MONTH_DAYS_11                                      --11月天数:src.MONTH_DAYS_11
       ,DST.MONTH_BAL_SUM_12                                   --12月余额积数:src.MONTH_BAL_SUM_12
       ,DST.MONTH_DAYS_12                                      --12月天数:src.MONTH_DAYS_12
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.MONTH_BAL_1                                        --1月底余额:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月底余额:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月底余额:src.MONTH_BAL_3
       ,DST.MONTH_BAL_4                                        --4月底余额:src.MONTH_BAL_4
       ,DST.MONTH_BAL_5                                        --5月底余额:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月底余额:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月底余额:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月底余额:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月底余额:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月底余额:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月底余额:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月底余额:src.MONTH_BAL_12
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.YEAR_BAL_SUM                                       --年余额积数:src.YEAR_BAL_SUM
   FROM ACRM_F_RE_SAVESUMAVGINFO DST 
   LEFT JOIN ACRM_F_RE_SAVESUMAVGINFO_INNTMP1 SRC            
     ON DST.FR_ID               = SRC.FR_ID 
    AND DST.CUST_ID = SRC.CUST_ID
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_SAVESUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2=ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_SAVESUMAVGINFO_INNTMP1)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.count()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_SAVESUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO_BK/")


#任务[12] 001-15::贷款积数表
V_STEP = V_STEP + 1

ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO_BK/'+V_DT+'.parquet/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")

sql = """
 SELECT distinct
        B.CUSTOM_ID             AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.ACCOUNT               AS ACCOUNT 
       ,A.OPEN_BRC              AS OPEN_BRC 
       ,A.PRDT_CODE             AS PRDT_CODE 
       ,A.MONEY_TYPE            AS MONEY_TYPE 
       ,A.AMOUNT                AS AMOUNT 
       ,A.LAST_AMOUNT           AS LAST_AMOUNT 
       ,A.YEAR                  AS YEAR 
       ,A.MONTH_BAL_SUM_1       AS MONTH_BAL_SUM_1 
       ,A.MONTH_DAYS_1          AS MONTH_DAYS_1 
       ,A.MONTH_BAL_SUM_2       AS MONTH_BAL_SUM_2 
       ,A.MONTH_DAYS_2          AS MONTH_DAYS_2 
       ,A.MONTH_BAL_SUM_3       AS MONTH_BAL_SUM_3 
       ,A.MONTH_DAYS_3          AS MONTH_DAYS_3 
       ,A.MONTH_BAL_SUM_4       AS MONTH_BAL_SUM_4 
       ,A.MONTH_DAYS_4          AS MONTH_DAYS_4 
       ,A.MONTH_BAL_SUM_5       AS MONTH_BAL_SUM_5 
       ,A.MONTH_DAYS_5          AS MONTH_DAYS_5 
       ,A.MONTH_BAL_SUM_6       AS MONTH_BAL_SUM_6 
       ,A.MONTH_DAYS_6          AS MONTH_DAYS_6 
       ,A.MONTH_BAL_SUM_7       AS MONTH_BAL_SUM_7 
       ,A.MONTH_DAYS_7          AS MONTH_DAYS_7 
       ,A.MONTH_BAL_SUM_8       AS MONTH_BAL_SUM_8 
       ,A.MONTH_DAYS_8          AS MONTH_DAYS_8 
       ,A.MONTH_BAL_SUM_9       AS MONTH_BAL_SUM_9 
       ,A.MONTH_DAYS_9          AS MONTH_DAYS_9 
       ,A.MONTH_BAL_SUM_10      AS MONTH_BAL_SUM_10 
       ,A.MONTH_DAYS_10         AS MONTH_DAYS_10 
       ,A.MONTH_BAL_SUM_11      AS MONTH_BAL_SUM_11 
       ,A.MONTH_DAYS_11         AS MONTH_DAYS_11 
       ,A.MONTH_BAL_SUM_12      AS MONTH_BAL_SUM_12 
       ,A.MONTH_DAYS_12         AS MONTH_DAYS_12 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.OLD_YEAR_BAL_SUM      AS OLD_YEAR_BAL_SUM 
       ,A.OLD_YEAR_BAL          AS OLD_YEAR_BAL 
       ,A.OLD_YEAR_DAYS         AS OLD_YEAR_DAYS 
       ,A.MONTH_BAL_1           AS MONTH_BAL_1 
       ,A.MONTH_BAL_2           AS MONTH_BAL_2 
       ,A.MONTH_BAL_3           AS MONTH_BAL_3 
       ,A.MONTH_BAL_4           AS MONTH_BAL_4 
       ,A.MONTH_BAL_5           AS MONTH_BAL_5 
       ,A.MONTH_BAL_6           AS MONTH_BAL_6 
       ,A.MONTH_BAL_7           AS MONTH_BAL_7 
       ,A.MONTH_BAL_8           AS MONTH_BAL_8 
       ,A.MONTH_BAL_9           AS MONTH_BAL_9 
       ,A.MONTH_BAL_10          AS MONTH_BAL_10 
       ,A.MONTH_BAL_11          AS MONTH_BAL_11 
       ,A.MONTH_BAL_12          AS MONTH_BAL_12 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_RE_LENDSUMAVGINFO A                             --
  INNER JOIN(
         SELECT DISTINCT FR_ID 
               ,CUSTOM_ID 
               ,CUSTOM_ID_BEFORE 
           FROM F_CI_CUST_MERGERLST 
          WHERE ODS_ST_DATE             = V_DT) B              --
     ON A.CUST_ID = B.CUSTOM_ID_BEFORE 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_LENDSUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_LENDSUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO_INNTMP1")

#ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
#ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.ACCOUNT                                            --账号:src.ACCOUNT
       ,DST.OPEN_BRC                                           --机构号(账号):src.OPEN_BRC
       ,DST.PRDT_CODE                                          --产品代码:src.PRDT_CODE
       ,DST.MONEY_TYPE                                         --币种:src.MONEY_TYPE
       ,DST.AMOUNT                                             --本日余额(原币种):src.AMOUNT
       ,DST.LAST_AMOUNT                                        --昨日余额(原币种):src.LAST_AMOUNT
       ,DST.YEAR                                               --积数年份:src.YEAR
       ,DST.MONTH_BAL_SUM_1                                    --1月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --1月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --2月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --2月天数:src.MONTH_DAYS_2
       ,DST.MONTH_BAL_SUM_3                                    --3月余额积数:src.MONTH_BAL_SUM_3
       ,DST.MONTH_DAYS_3                                       --3月天数:src.MONTH_DAYS_3
       ,DST.MONTH_BAL_SUM_4                                    --4月余额积数:src.MONTH_BAL_SUM_4
       ,DST.MONTH_DAYS_4                                       --4月天数:src.MONTH_DAYS_4
       ,DST.MONTH_BAL_SUM_5                                    --5月余额积数:src.MONTH_BAL_SUM_5
       ,DST.MONTH_DAYS_5                                       --5月天数:src.MONTH_DAYS_5
       ,DST.MONTH_BAL_SUM_6                                    --6月余额积数:src.MONTH_BAL_SUM_6
       ,DST.MONTH_DAYS_6                                       --6月天数:src.MONTH_DAYS_6
       ,DST.MONTH_BAL_SUM_7                                    --7月余额积数:src.MONTH_BAL_SUM_7
       ,DST.MONTH_DAYS_7                                       --7月天数:src.MONTH_DAYS_7
       ,DST.MONTH_BAL_SUM_8                                    --8月余额积数:src.MONTH_BAL_SUM_8
       ,DST.MONTH_DAYS_8                                       --8月天数:src.MONTH_DAYS_8
       ,DST.MONTH_BAL_SUM_9                                    --9月余额积数:src.MONTH_BAL_SUM_9
       ,DST.MONTH_DAYS_9                                       --9月天数:src.MONTH_DAYS_9
       ,DST.MONTH_BAL_SUM_10                                   --10月余额积数:src.MONTH_BAL_SUM_10
       ,DST.MONTH_DAYS_10                                      --10月天数:src.MONTH_DAYS_10
       ,DST.MONTH_BAL_SUM_11                                   --11月余额积数:src.MONTH_BAL_SUM_11
       ,DST.MONTH_DAYS_11                                      --11月天数:src.MONTH_DAYS_11
       ,DST.MONTH_BAL_SUM_12                                   --12月余额积数:src.MONTH_BAL_SUM_12
       ,DST.MONTH_DAYS_12                                      --12月天数:src.MONTH_DAYS_12
       ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
       ,DST.OLD_YEAR_BAL_SUM                                   --年初余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --年初余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --上年天数合计:src.OLD_YEAR_DAYS
       ,DST.MONTH_BAL_1                                        --1月余额积数:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月余额积数:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月余额积数:src.MONTH_BAL_3
       ,DST.MONTH_BAL_4                                        --4月余额积数:src.MONTH_BAL_4
       ,DST.MONTH_BAL_5                                        --5月余额积数:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月余额积数:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月余额积数:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月余额积数:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月余额积数:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月余额积数:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月余额积数:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月余额积数:src.MONTH_BAL_12
       ,DST.CUST_TYP                                           --:src.CUST_TYP
       ,DST.FR_ID                                              --:src.FR_ID
   FROM ACRM_F_RE_LENDSUMAVGINFO DST 
   LEFT JOIN ACRM_F_RE_LENDSUMAVGINFO_INNTMP1 SRC
     ON DST.CUST_ID = SRC.CUST_ID 
    AND DST.FR_ID                 = SRC.FR_ID
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_LENDSUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_LENDSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_LENDSUMAVGINFO_INNTMP2=ACRM_F_RE_LENDSUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_LENDSUMAVGINFO_INNTMP1)
ACRM_F_RE_LENDSUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_LENDSUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_LENDSUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_LENDSUMAVGINFO_INNTMP2.count()
ACRM_F_RE_LENDSUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_LENDSUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_LENDSUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_LENDSUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_LENDSUMAVGINFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_RE_LENDSUMAVGINFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_RE_LENDSUMAVGINFO_BK/")


#任务[12] 001-16::理财积数
V_STEP = V_STEP + 1

ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO_BK/'+V_DT+'.parquet/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")

sql = """
 SELECT distinct
        B.CUSTOM_ID             AS CUST_ID 
       ,A.ACCT_NAME             AS ACCT_NAME 
       ,A.ACCT_NO               AS ACCT_NO 
       ,A.CURR                  AS CURR 
       ,A.PRDT_CODE             AS PRDT_CODE 
       ,A.AMT                   AS AMT 
       ,A.LAST_AMOUNT           AS LAST_AMOUNT 
       ,A.YEAR                  AS YEAR 
       ,A.MONTH_BAL_SUM_1       AS MONTH_BAL_SUM_1 
       ,A.MONTH_DAYS_1          AS MONTH_DAYS_1 
       ,A.MONTH_BAL_SUM_2       AS MONTH_BAL_SUM_2 
       ,A.MONTH_DAYS_2          AS MONTH_DAYS_2 
       ,A.MONTH_BAL_SUM_3       AS MONTH_BAL_SUM_3 
       ,A.MONTH_DAYS_3          AS MONTH_DAYS_3 
       ,A.MONTH_BAL_SUM_4       AS MONTH_BAL_SUM_4 
       ,A.MONTH_DAYS_4          AS MONTH_DAYS_4 
       ,A.MONTH_BAL_SUM_5       AS MONTH_BAL_SUM_5 
       ,A.MONTH_DAYS_5          AS MONTH_DAYS_5 
       ,A.MONTH_BAL_SUM_6       AS MONTH_BAL_SUM_6 
       ,A.MONTH_DAYS_6          AS MONTH_DAYS_6 
       ,A.MONTH_BAL_SUM_7       AS MONTH_BAL_SUM_7 
       ,A.MONTH_DAYS_7          AS MONTH_DAYS_7 
       ,A.MONTH_BAL_SUM_8       AS MONTH_BAL_SUM_8 
       ,A.MONTH_DAYS_8          AS MONTH_DAYS_8 
       ,A.MONTH_BAL_SUM_9       AS MONTH_BAL_SUM_9 
       ,A.MONTH_DAYS_9          AS MONTH_DAYS_9 
       ,A.MONTH_BAL_SUM_10      AS MONTH_BAL_SUM_10 
       ,A.MONTH_DAYS_10         AS MONTH_DAYS_10 
       ,A.MONTH_BAL_SUM_11      AS MONTH_BAL_SUM_11 
       ,A.MONTH_DAYS_11         AS MONTH_DAYS_11 
       ,A.MONTH_BAL_SUM_12      AS MONTH_BAL_SUM_12 
       ,A.MONTH_DAYS_12         AS MONTH_DAYS_12 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.OLD_YEAR_BAL_SUM      AS OLD_YEAR_BAL_SUM 
       ,A.OLD_YEAR_BAL          AS OLD_YEAR_BAL 
       ,A.OLD_YEAR_DAYS         AS OLD_YEAR_DAYS 
       ,A.ORG_ID                AS ORG_ID 
       ,A.MONTH_BAL_1           AS MONTH_BAL_1 
       ,A.MONTH_BAL_2           AS MONTH_BAL_2 
       ,A.MONTH_BAL_3           AS MONTH_BAL_3 
       ,A.MONTH_BAL_4           AS MONTH_BAL_4 
       ,A.MONTH_BAL_5           AS MONTH_BAL_5 
       ,A.MONTH_BAL_6           AS MONTH_BAL_6 
       ,A.MONTH_BAL_7           AS MONTH_BAL_7 
       ,A.MONTH_BAL_8           AS MONTH_BAL_8 
       ,A.MONTH_BAL_9           AS MONTH_BAL_9 
       ,A.MONTH_BAL_10          AS MONTH_BAL_10 
       ,A.MONTH_BAL_11          AS MONTH_BAL_11 
       ,A.MONTH_BAL_12          AS MONTH_BAL_12 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_RE_ACCSUMAVGINFO A                              --
  INNER JOIN(
         SELECT DISTINCT FR_ID 
               ,CUSTOM_ID 
               ,CUSTOM_ID_BEFORE 
           FROM F_CI_CUST_MERGERLST 
          WHERE ODS_ST_DATE           = V_DT) B              --
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID = B.CUSTOM_ID_BEFORE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO_INNTMP1")

#ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
#ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.ACCT_NAME                                          --账户名称:src.ACCT_NAME
       ,DST.ACCT_NO                                            --帐号:src.ACCT_NO
       ,DST.CURR                                               --币种:src.CURR
       ,DST.PRDT_CODE                                          --产品号:src.PRDT_CODE
       ,DST.AMT                                                --余额:src.AMT
       ,DST.LAST_AMOUNT                                        --昨日余额:src.LAST_AMOUNT
       ,DST.YEAR                                               --年份:src.YEAR
       ,DST.MONTH_BAL_SUM_1                                    --1月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --1月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --2月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --2月天数:src.MONTH_DAYS_2
       ,DST.MONTH_BAL_SUM_3                                    --3月余额积数:src.MONTH_BAL_SUM_3
       ,DST.MONTH_DAYS_3                                       --3月天数:src.MONTH_DAYS_3
       ,DST.MONTH_BAL_SUM_4                                    --4月余额积数:src.MONTH_BAL_SUM_4
       ,DST.MONTH_DAYS_4                                       --4月天数:src.MONTH_DAYS_4
       ,DST.MONTH_BAL_SUM_5                                    --5月余额积数:src.MONTH_BAL_SUM_5
       ,DST.MONTH_DAYS_5                                       --5月天数:src.MONTH_DAYS_5
       ,DST.MONTH_BAL_SUM_6                                    --6月余额积数:src.MONTH_BAL_SUM_6
       ,DST.MONTH_DAYS_6                                       --6月天数:src.MONTH_DAYS_6
       ,DST.MONTH_BAL_SUM_7                                    --7月余额积数:src.MONTH_BAL_SUM_7
       ,DST.MONTH_DAYS_7                                       --7月天数:src.MONTH_DAYS_7
       ,DST.MONTH_BAL_SUM_8                                    --8月余额积数:src.MONTH_BAL_SUM_8
       ,DST.MONTH_DAYS_8                                       --8月天数:src.MONTH_DAYS_8
       ,DST.MONTH_BAL_SUM_9                                    --9月余额积数:src.MONTH_BAL_SUM_9
       ,DST.MONTH_DAYS_9                                       --9月天数:src.MONTH_DAYS_9
       ,DST.MONTH_BAL_SUM_10                                   --10月余额积数:src.MONTH_BAL_SUM_10
       ,DST.MONTH_DAYS_10                                      --10月天数:src.MONTH_DAYS_10
       ,DST.MONTH_BAL_SUM_11                                   --11月余额积数:src.MONTH_BAL_SUM_11
       ,DST.MONTH_DAYS_11                                      --11月天数:src.MONTH_DAYS_11
       ,DST.MONTH_BAL_SUM_12                                   --12月余额积数:src.MONTH_BAL_SUM_12
       ,DST.MONTH_DAYS_12                                      --12月天数:src.MONTH_DAYS_12
       ,DST.ODS_ST_DATE                                        --加工日期:src.ODS_ST_DATE
       ,DST.OLD_YEAR_BAL_SUM                                   --去年余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --年初余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --去年年天数:src.OLD_YEAR_DAYS
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.MONTH_BAL_1                                        --1月月末剩余金额（原币种）:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月月末剩余金额（原币种）:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月月末剩余金额（原币种）:src.MONTH_BAL_3
       ,DST.MONTH_BAL_4                                        --4月月末剩余金额（原币种）:src.MONTH_BAL_4
       ,DST.MONTH_BAL_5                                        --5月月末剩余金额（原币种）:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月月末剩余金额（原币种）:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月月末剩余金额（原币种）:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月月末剩余金额（原币种）:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月月末剩余金额（原币种）:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月月末剩余金额（原币种）:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月月末剩余金额（原币种）:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月月末剩余金额（原币种）:src.MONTH_BAL_12
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_F_RE_ACCSUMAVGINFO DST 
   LEFT JOIN ACRM_F_RE_ACCSUMAVGINFO_INNTMP1 SRC         
     ON DST.FR_ID                 = SRC.FR_ID 
    AND DST.CUST_ID = SRC.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_ACCSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2=ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_ACCSUMAVGINFO_INNTMP1)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.count()
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_ACCSUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO_BK/")


#任务[12] 001-17::保险累计
V_STEP = V_STEP + 1

ACRM_F_RE_INSUSUMINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_INSUSUMINFO_BK/'+V_DT+'.parquet/*')
ACRM_F_RE_INSUSUMINFO.registerTempTable("ACRM_F_RE_INSUSUMINFO")

sql = """
 SELECT distinct
        B.CUSTOM_ID             AS CUST_ID 
       ,A.CUST_NAME_CN          AS CUST_NAME_CN 
       ,A.ACCT_NO               AS ACCT_NO 
       ,A.PRDT_CODE             AS PRDT_CODE 
       ,A.ORG_NO                AS ORG_NO 
       ,A.LAST_AMOUNT           AS LAST_AMOUNT 
       ,A.YEAR                  AS YEAR 
       ,A.MONTH_BAL_SUM_1       AS MONTH_BAL_SUM_1 
       ,A.MONTH_COUNT_1         AS MONTH_COUNT_1 
       ,A.MONTH_BAL_SUM_2       AS MONTH_BAL_SUM_2 
       ,A.MONTH_COUNT_2         AS MONTH_COUNT_2 
       ,A.MONTH_BAL_SUM_3       AS MONTH_BAL_SUM_3 
       ,A.MONTH_COUNT_3         AS MONTH_COUNT_3 
       ,A.MONTH_BAL_SUM_4       AS MONTH_BAL_SUM_4 
       ,A.MONTH_COUNT_4         AS MONTH_COUNT_4 
       ,A.MONTH_BAL_SUM_5       AS MONTH_BAL_SUM_5 
       ,A.MONTH_COUNT_5         AS MONTH_COUNT_5 
       ,A.MONTH_BAL_SUM_6       AS MONTH_BAL_SUM_6 
       ,A.MONTH_COUNT_6         AS MONTH_COUNT_6 
       ,A.MONTH_BAL_SUM_7       AS MONTH_BAL_SUM_7 
       ,A.MONTH_COUNT_7         AS MONTH_COUNT_7 
       ,A.MONTH_BAL_SUM_8       AS MONTH_BAL_SUM_8 
       ,A.MONTH_COUNT_8         AS MONTH_COUNT_8 
       ,A.MONTH_BAL_SUM_9       AS MONTH_BAL_SUM_9 
       ,A.MONTH_COUNT_9         AS MONTH_COUNT_9 
       ,A.MONTH_BAL_SUM_10      AS MONTH_BAL_SUM_10 
       ,A.MONTH_COUNT_10        AS MONTH_COUNT_10 
       ,A.MONTH_BAL_SUM_11      AS MONTH_BAL_SUM_11 
       ,A.MONTH_COUNT_11        AS MONTH_COUNT_11 
       ,A.MONTH_BAL_SUM_12      AS MONTH_BAL_SUM_12 
       ,A.MONTH_COUNT_12        AS MONTH_COUNT_12 
       ,A.ALL_MONEY             AS ALL_MONEY 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.ALL_COUNT             AS ALL_COUNT 
       ,A.OLD_YEAR_BAL_SUM      AS OLD_YEAR_BAL_SUM 
       ,A.OLD_YEAR_BAL          AS OLD_YEAR_BAL 
       ,A.AMOUNT                AS AMOUNT 
       ,A.COUNT                 AS COUNT 
       ,A.LAST_COUNT            AS LAST_COUNT 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_RE_INSUSUMINFO A                                --
  INNER JOIN(
         SELECT DISTINCT FR_ID 
               ,CUSTOM_ID 
               ,CUSTOM_ID_BEFORE 
           FROM F_CI_CUST_MERGERLST 
          WHERE ODS_ST_DATE             = V_DT) B              --
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID = B.CUSTOM_ID_BEFORE"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_INSUSUMINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_INSUSUMINFO_INNTMP1.registerTempTable("ACRM_F_RE_INSUSUMINFO_INNTMP1")

#ACRM_F_RE_INSUSUMINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_INSUSUMINFO/*')
#ACRM_F_RE_INSUSUMINFO.registerTempTable("ACRM_F_RE_INSUSUMINFO")
sql = """
 SELECT DST.CUST_ID                                             --客户编号:src.CUST_ID
       ,DST.CUST_NAME_CN                                       --客户名称:src.CUST_NAME_CN
       ,DST.ACCT_NO                                            --账户:src.ACCT_NO
       ,DST.PRDT_CODE                                          --产品编号:src.PRDT_CODE
       ,DST.ORG_NO                                             --机构代码:src.ORG_NO
       ,DST.LAST_AMOUNT                                        --去年累计金额:src.LAST_AMOUNT
       ,DST.YEAR                                               --积数年份:src.YEAR
       ,DST.MONTH_BAL_SUM_1                                    --1月购买金额:src.MONTH_BAL_SUM_1
       ,DST.MONTH_COUNT_1                                      --1月购买笔数:src.MONTH_COUNT_1
       ,DST.MONTH_BAL_SUM_2                                    --2月购买金额:src.MONTH_BAL_SUM_2
       ,DST.MONTH_COUNT_2                                      --2月购买笔数:src.MONTH_COUNT_2
       ,DST.MONTH_BAL_SUM_3                                    --3月购买金额:src.MONTH_BAL_SUM_3
       ,DST.MONTH_COUNT_3                                      --3月购买笔数:src.MONTH_COUNT_3
       ,DST.MONTH_BAL_SUM_4                                    --4月购买金额:src.MONTH_BAL_SUM_4
       ,DST.MONTH_COUNT_4                                      --4月购买笔数:src.MONTH_COUNT_4
       ,DST.MONTH_BAL_SUM_5                                    --5月购买金额:src.MONTH_BAL_SUM_5
       ,DST.MONTH_COUNT_5                                      --5月购买笔数:src.MONTH_COUNT_5
       ,DST.MONTH_BAL_SUM_6                                    --6月购买金额:src.MONTH_BAL_SUM_6
       ,DST.MONTH_COUNT_6                                      --6月购买笔数:src.MONTH_COUNT_6
       ,DST.MONTH_BAL_SUM_7                                    --7月购买金额:src.MONTH_BAL_SUM_7
       ,DST.MONTH_COUNT_7                                      --7月购买笔数:src.MONTH_COUNT_7
       ,DST.MONTH_BAL_SUM_8                                    --8月购买金额:src.MONTH_BAL_SUM_8
       ,DST.MONTH_COUNT_8                                      --8月购买笔数:src.MONTH_COUNT_8
       ,DST.MONTH_BAL_SUM_9                                    --9月购买金额:src.MONTH_BAL_SUM_9
       ,DST.MONTH_COUNT_9                                      --9月购买笔数:src.MONTH_COUNT_9
       ,DST.MONTH_BAL_SUM_10                                   --10月购买金额:src.MONTH_BAL_SUM_10
       ,DST.MONTH_COUNT_10                                     --10月购买笔数:src.MONTH_COUNT_10
       ,DST.MONTH_BAL_SUM_11                                   --11月购买金额:src.MONTH_BAL_SUM_11
       ,DST.MONTH_COUNT_11                                     --11月购买笔数:src.MONTH_COUNT_11
       ,DST.MONTH_BAL_SUM_12                                   --12月购买金额:src.MONTH_BAL_SUM_12
       ,DST.MONTH_COUNT_12                                     --12月购买笔数:src.MONTH_COUNT_12
       ,DST.ALL_MONEY                                          --保费累计:src.ALL_MONEY
       ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
       ,DST.ALL_COUNT                                          --累计购买笔数:src.ALL_COUNT
       ,DST.OLD_YEAR_BAL_SUM                                   --年初累计购买金额:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --年初累计购买笔数:src.OLD_YEAR_BAL
       ,DST.AMOUNT                                             --:src.AMOUNT
       ,DST.COUNT                                              --:src.COUNT
       ,DST.LAST_COUNT                                         --:src.LAST_COUNT
       ,DST.FR_ID                                              --:src.FR_ID
   FROM ACRM_F_RE_INSUSUMINFO DST 
   LEFT JOIN ACRM_F_RE_INSUSUMINFO_INNTMP1 SRC
     ON DST.FR_ID                 = SRC.FR_ID 
    AND DST.CUST_ID = SRC.CUST_ID
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_INSUSUMINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_INSUSUMINFO/"+V_DT+".parquet"
ACRM_F_RE_INSUSUMINFO_INNTMP2=ACRM_F_RE_INSUSUMINFO_INNTMP2.unionAll(ACRM_F_RE_INSUSUMINFO_INNTMP1)
ACRM_F_RE_INSUSUMINFO_INNTMP1.cache()
ACRM_F_RE_INSUSUMINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_INSUSUMINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_INSUSUMINFO_INNTMP2.count()
ACRM_F_RE_INSUSUMINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_INSUSUMINFO_INNTMP1.unpersist()
ACRM_F_RE_INSUSUMINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_INSUSUMINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_INSUSUMINFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_RE_INSUSUMINFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_RE_INSUSUMINFO_BK/")


#任务[21] 001-18::客户归属机构
V_STEP = V_STEP + 1
OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG_BK/'+V_DT+'.parquet/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
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
   FROM OCRM_F_CI_BELONG_ORG A                                 --
   WHERE NOT EXISTS (SELECT 1 FROM F_CI_CUST_MERGERLST C 
	WHERE C.FR_ID= A.FR_ID 
		AND A.CUST_ID = C.CUSTOM_ID_BEFORE 
		AND C.ODS_ST_DATE = V_DT ) """

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

#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_BELONG_ORG/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_BELONG_ORG_BK/")


#任务[21] 001-19::客户归属客户经理
V_STEP = V_STEP + 1
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR_BK/'+V_DT+'.parquet/*')
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
   FROM OCRM_F_CI_BELONG_CUSTMGR A                             --
   WHERE NOT EXISTS (SELECT 1 FROM F_CI_CUST_MERGERLST C WHERE C.FR_ID= A.FR_ID AND A.CUST_ID = C.CUSTOM_ID_BEFORE )"""

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
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_BELONG_CUSTMGR_BK/")

