#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_GROUP_DEP_CRE').setMaster(sys.argv[2])
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

ACRM_F_GROUP_DEP_CRE = sqlContext.read.parquet(hdfs+'/ACRM_F_GROUP_DEP_CRE_BK/'+V_DT_LD+'.parquet')
ACRM_F_GROUP_DEP_CRE.registerTempTable("ACRM_F_GROUP_DEP_CRE")
OCRM_F_CI_BASE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE/*')
OCRM_F_CI_BASE.registerTempTable("OCRM_F_CI_BASE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(GROUP_ID  AS DECIMAL(27))              AS GROUP_ID 
       ,GROUP_NAME              AS GROUP_NAME 
       ,CAST(DEP_BAL   AS DECIMAL(20,2))              AS DEP_BAL 
       ,CAST(LN_BAL     AS DECIMAL(20,2))             AS LN_BAL 
       ,FR_ID                   AS FR_ID 
       ,CRM_DT                  AS CRM_DT 
   FROM ACRM_F_GROUP_DEP_CRE A                                 --
  WHERE CRM_DT > DATE_ADD(V_DT,-30) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_GROUP_DEP_CRE_TMP = sqlContext.sql(sql)
ACRM_F_GROUP_DEP_CRE_TMP.registerTempTable("ACRM_F_GROUP_DEP_CRE_TMP")
dfn="ACRM_F_GROUP_DEP_CRE_TMP/"+V_DT+".parquet"
ACRM_F_GROUP_DEP_CRE_TMP.cache()
nrows = ACRM_F_GROUP_DEP_CRE_TMP.count()

ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_GROUP_DEP_CRE_TMP/*.parquet")
ACRM_F_GROUP_DEP_CRE_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_GROUP_DEP_CRE_TMP.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_GROUP_DEP_CRE_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
ACRM_F_GROUP_DEP_CRE_TMP = sqlContext.read.parquet(hdfs+'/ACRM_F_GROUP_DEP_CRE_TMP/*')
ACRM_F_GROUP_DEP_CRE_TMP.registerTempTable("ACRM_F_GROUP_DEP_CRE_TMP")
sql = """
 SELECT CAST(GROUP_ID  AS DECIMAL(27))                  AS GROUP_ID 
       ,GROUP_NAME              AS GROUP_NAME 
       ,CAST(DEP_BAL   AS DECIMAL(20,2))                 AS DEP_BAL 
       ,CAST(LN_BAL     AS DECIMAL(20,2))                  AS LN_BAL 
       ,FR_ID                   AS FR_ID 
       ,CRM_DT                  AS CRM_DT 
   FROM ACRM_F_GROUP_DEP_CRE_TMP A                             --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_GROUP_DEP_CRE = sqlContext.sql(sql)
ACRM_F_GROUP_DEP_CRE.registerTempTable("ACRM_F_GROUP_DEP_CRE")
dfn="ACRM_F_GROUP_DEP_CRE/"+V_DT+".parquet"
ACRM_F_GROUP_DEP_CRE.cache()
nrows = ACRM_F_GROUP_DEP_CRE.count()
ACRM_F_GROUP_DEP_CRE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_GROUP_DEP_CRE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_GROUP_DEP_CRE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_GROUP_DEP_CRE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1
OCRM_F_CI_CUSTGROUP_GRAPH_VERT = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUSTGROUP_GRAPH_VERT/*')
OCRM_F_CI_CUSTGROUP_GRAPH_VERT.registerTempTable("OCRM_F_CI_CUSTGROUP_GRAPH_VERT")
sql = """
 SELECT ID                      AS ID 
       ,CUSTGROUP_ID            AS CUSTGROUP_ID 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
   FROM OCRM_F_CI_CUSTGROUP_GRAPH_VERT A                       --
  WHERE CUSTGROUP_ID NOT IN
		(
         SELECT ID 
           FROM OCRM_F_CI_BASE B 
         WHERE ADD_MONTHS(B.CUST_BASE_CREATE_DATE , B.EFFECT_MONTHS) < V_DT 
        ) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP = sqlContext.sql(sql)
OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP.registerTempTable("OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP")
dfn="OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP/"+V_DT+".parquet"
OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP.cache()
nrows = OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP.count()
OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::
V_STEP = V_STEP + 1
OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP/*')
OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP.registerTempTable("OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP")
sql = """
 SELECT ID                      AS ID 
       ,CUSTGROUP_ID            AS CUSTGROUP_ID 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
   FROM OCRM_F_CI_CUSTGROUP_GRAPH_VERT_TMP A                   --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUSTGROUP_GRAPH_VERT = sqlContext.sql(sql)
OCRM_F_CI_CUSTGROUP_GRAPH_VERT.registerTempTable("OCRM_F_CI_CUSTGROUP_GRAPH_VERT")
dfn="OCRM_F_CI_CUSTGROUP_GRAPH_VERT/"+V_DT+".parquet"
OCRM_F_CI_CUSTGROUP_GRAPH_VERT.cache()
nrows = OCRM_F_CI_CUSTGROUP_GRAPH_VERT.count()
OCRM_F_CI_CUSTGROUP_GRAPH_VERT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUSTGROUP_GRAPH_VERT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUSTGROUP_GRAPH_VERT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUSTGROUP_GRAPH_VERT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-05::
V_STEP = V_STEP + 1
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUSTGROUP_GRAPH_EDGE/*')
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE.registerTempTable("OCRM_F_CI_CUSTGROUP_GRAPH_EDGE")
sql = """
 SELECT ID                      AS ID 
       ,CUSTGROUP_ID            AS CUSTGROUP_ID 
       ,FROM_VERTEX             AS FROM_VERTEX 
       ,TO_VERTEX               AS TO_VERTEX 
       ,RELATION_CODE           AS RELATION_CODE 
       ,RELATION_NAME           AS RELATION_NAME 
   FROM OCRM_F_CI_CUSTGROUP_GRAPH_EDGE A                       --
  WHERE 
    NOT EXISTS(
         SELECT 1 
           FROM OCRM_F_CI_BASE B 
          WHERE ADD_MONTHS(B.CUST_BASE_CREATE_DATE , B.EFFECT_MONTHS) < V_DT 
            AND A.CUSTGROUP_ID          = B.ID) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP = sqlContext.sql(sql)
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP.registerTempTable("OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP")
dfn="OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP/"+V_DT+".parquet"
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP.cache()
nrows = OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP.count()
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-06::
V_STEP = V_STEP + 1
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP/*')
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP.registerTempTable("OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP")
sql = """
 SELECT ID                      AS ID 
       ,CUSTGROUP_ID            AS CUSTGROUP_ID 
       ,FROM_VERTEX             AS FROM_VERTEX 
       ,TO_VERTEX               AS TO_VERTEX 
       ,RELATION_CODE           AS RELATION_CODE 
       ,RELATION_NAME           AS RELATION_NAME 
   FROM OCRM_F_CI_CUSTGROUP_GRAPH_EDGE_TMP A                   --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE = sqlContext.sql(sql)
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE.registerTempTable("OCRM_F_CI_CUSTGROUP_GRAPH_EDGE")
dfn="OCRM_F_CI_CUSTGROUP_GRAPH_EDGE/"+V_DT+".parquet"
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE.cache()
nrows = OCRM_F_CI_CUSTGROUP_GRAPH_EDGE.count()
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUSTGROUP_GRAPH_EDGE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUSTGROUP_GRAPH_EDGE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUSTGROUP_GRAPH_EDGE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-07::
V_STEP = V_STEP + 1
OCRM_F_A_SS_COL = sqlContext.read.parquet(hdfs+'/OCRM_F_A_SS_COL/*')
OCRM_F_A_SS_COL.registerTempTable("OCRM_F_A_SS_COL")
OCRM_F_CI_BASE_SEARCHSOLUTION = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_SEARCHSOLUTION/*')
OCRM_F_CI_BASE_SEARCHSOLUTION.registerTempTable("OCRM_F_CI_BASE_SEARCHSOLUTION")
sql = """
 SELECT ID                      AS ID 
       ,SS_ID                   AS SS_ID 
       ,SS_COL_ORDER            AS SS_COL_ORDER 
       ,SS_COL_LEFT             AS SS_COL_LEFT 
       ,SS_COL_ITEM             AS SS_COL_ITEM 
       ,SS_COL_OP               AS SS_COL_OP 
       ,SS_COL_VALUE            AS SS_COL_VALUE 
       ,SS_COL_RIGHT            AS SS_COL_RIGHT 
       ,SS_COL_JOIN             AS SS_COL_JOIN 
   FROM OCRM_F_A_SS_COL T                                      --
  WHERE T.SS_ID 
    NOT IN(
         SELECT ID 
           FROM OCRM_F_CI_BASE_SEARCHSOLUTION 
          WHERE GROUP_ID IN(
                 SELECT ID 
                   FROM OCRM_F_CI_BASE 
                  WHERE ADD_MONTHS(CUST_BASE_CREATE_DATE, EFFECT_MONTHS ) < V_DT)) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_A_SS_COL_TMP = sqlContext.sql(sql)
OCRM_F_A_SS_COL_TMP.registerTempTable("OCRM_F_A_SS_COL_TMP")
dfn="OCRM_F_A_SS_COL_TMP/"+V_DT+".parquet"
OCRM_F_A_SS_COL_TMP.cache()
nrows = OCRM_F_A_SS_COL_TMP.count()
OCRM_F_A_SS_COL_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_A_SS_COL_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_A_SS_COL_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_A_SS_COL_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-08::
V_STEP = V_STEP + 1
OCRM_F_A_SS_COL_TMP = sqlContext.read.parquet(hdfs+'/OCRM_F_A_SS_COL_TMP/*')
OCRM_F_A_SS_COL_TMP.registerTempTable("OCRM_F_A_SS_COL_TMP")
sql = """
 SELECT ID                      AS ID 
       ,SS_ID                   AS SS_ID 
       ,SS_COL_ORDER            AS SS_COL_ORDER 
       ,SS_COL_LEFT             AS SS_COL_LEFT 
       ,SS_COL_ITEM             AS SS_COL_ITEM 
       ,SS_COL_OP               AS SS_COL_OP 
       ,SS_COL_VALUE            AS SS_COL_VALUE 
       ,SS_COL_RIGHT            AS SS_COL_RIGHT 
       ,SS_COL_JOIN             AS SS_COL_JOIN 
   FROM OCRM_F_A_SS_COL_TMP A                                  --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_A_SS_COL = sqlContext.sql(sql)
OCRM_F_A_SS_COL.registerTempTable("OCRM_F_A_SS_COL")
dfn="OCRM_F_A_SS_COL/"+V_DT+".parquet"
OCRM_F_A_SS_COL.cache()
nrows = OCRM_F_A_SS_COL.count()
OCRM_F_A_SS_COL.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_A_SS_COL.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_A_SS_COL/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_A_SS_COL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-09::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,GROUP_NAME              AS GROUP_NAME 
       ,GROUP_ID                AS GROUP_ID 
       ,SS_TYPE                 AS SS_TYPE 
       ,SS_RESULT               AS SS_RESULT 
       ,SS_SORT                 AS SS_SORT 
       ,SS_USER                 AS SS_USER 
       ,SS_DATE                 AS SS_DATE 
   FROM OCRM_F_CI_BASE_SEARCHSOLUTION T                        --
  WHERE T.GROUP_ID 
    NOT IN(
         SELECT ID 
           FROM OCRM_F_CI_BASE 
          WHERE ADD_MONTHS(CUST_BASE_CREATE_DATE ,EFFECT_MONTHS ) < V_DT) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_SEARCHSOLUTION_TMP = sqlContext.sql(sql)
OCRM_F_CI_BASE_SEARCHSOLUTION_TMP.registerTempTable("OCRM_F_CI_BASE_SEARCHSOLUTION_TMP")
dfn="OCRM_F_CI_BASE_SEARCHSOLUTION_TMP/"+V_DT+".parquet"
OCRM_F_CI_BASE_SEARCHSOLUTION_TMP.cache()
nrows = OCRM_F_CI_BASE_SEARCHSOLUTION_TMP.count()
OCRM_F_CI_BASE_SEARCHSOLUTION_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_SEARCHSOLUTION_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BASE_SEARCHSOLUTION_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_SEARCHSOLUTION_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-10::
V_STEP = V_STEP + 1
OCRM_F_CI_BASE_SEARCHSOLUTION_TMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_SEARCHSOLUTION_TMP/*')
OCRM_F_CI_BASE_SEARCHSOLUTION_TMP.registerTempTable("OCRM_F_CI_BASE_SEARCHSOLUTION_TMP")
sql = """
 SELECT ID                      AS ID 
       ,GROUP_NAME              AS GROUP_NAME 
       ,GROUP_ID                AS GROUP_ID 
       ,SS_TYPE                 AS SS_TYPE 
       ,SS_RESULT               AS SS_RESULT 
       ,SS_SORT                 AS SS_SORT 
       ,SS_USER                 AS SS_USER 
       ,SS_DATE                 AS SS_DATE 
   FROM OCRM_F_CI_BASE_SEARCHSOLUTION_TMP A                    --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_SEARCHSOLUTION = sqlContext.sql(sql)
OCRM_F_CI_BASE_SEARCHSOLUTION.registerTempTable("OCRM_F_CI_BASE_SEARCHSOLUTION")
dfn="OCRM_F_CI_BASE_SEARCHSOLUTION/"+V_DT+".parquet"
OCRM_F_CI_BASE_SEARCHSOLUTION.cache()
nrows = OCRM_F_CI_BASE_SEARCHSOLUTION.count()
OCRM_F_CI_BASE_SEARCHSOLUTION.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_SEARCHSOLUTION.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BASE_SEARCHSOLUTION/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_SEARCHSOLUTION lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-11::
V_STEP = V_STEP + 1

sql = """
 SELECT ID                      AS ID 
       ,CUST_BASE_NAME          AS CUST_BASE_NAME 
       ,CUST_BASE_CREATE_DATE   AS CUST_BASE_CREATE_DATE 
       ,CUST_BASE_MEMBER_NUM    AS CUST_BASE_MEMBER_NUM 
       ,CUST_BASE_NUMBER        AS CUST_BASE_NUMBER 
       ,CUST_BASE_DESC          AS CUST_BASE_DESC 
       ,CUST_BASE_CREATE_NAME   AS CUST_BASE_CREATE_NAME 
       ,SHARE_FLAG              AS SHARE_FLAG 
       ,CUST_BASE_CREATE_ORG    AS CUST_BASE_CREATE_ORG 
       ,CUST_FROM               AS CUST_FROM 
       ,CUST_FROM_NAME          AS CUST_FROM_NAME 
       ,RECENT_UPDATE_USER      AS RECENT_UPDATE_USER 
       ,RECENT_UPDATE_ORG       AS RECENT_UPDATE_ORG 
       ,RECENT_UPDATE_DATE      AS RECENT_UPDATE_DATE 
       ,GROUP_MEMBER_TYPE       AS GROUP_MEMBER_TYPE 
       ,GROUP_TYPE              AS GROUP_TYPE 
       ,FR_ID                   AS FR_ID 
       ,EFFECT_MONTHS           AS EFFECT_MONTHS 
   FROM OCRM_F_CI_BASE A                                       --
  WHERE ADD_MONTHS(CUST_BASE_CREATE_DATE ,EFFECT_MONTHS) >= V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE_TMP = sqlContext.sql(sql)
OCRM_F_CI_BASE_TMP.registerTempTable("OCRM_F_CI_BASE_TMP")
dfn="OCRM_F_CI_BASE_TMP/"+V_DT+".parquet"
OCRM_F_CI_BASE_TMP.cache()
nrows = OCRM_F_CI_BASE_TMP.count()
OCRM_F_CI_BASE_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BASE_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-12::
V_STEP = V_STEP + 1
OCRM_F_CI_BASE_TMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE_TMP/*')
OCRM_F_CI_BASE_TMP.registerTempTable("OCRM_F_CI_BASE_TMP")
sql = """
 SELECT ID                      AS ID 
       ,CUST_BASE_NAME          AS CUST_BASE_NAME 
       ,CUST_BASE_CREATE_DATE   AS CUST_BASE_CREATE_DATE 
       ,CUST_BASE_MEMBER_NUM    AS CUST_BASE_MEMBER_NUM 
       ,CUST_BASE_NUMBER        AS CUST_BASE_NUMBER 
       ,CUST_BASE_DESC          AS CUST_BASE_DESC 
       ,CUST_BASE_CREATE_NAME   AS CUST_BASE_CREATE_NAME 
       ,SHARE_FLAG              AS SHARE_FLAG 
       ,CUST_BASE_CREATE_ORG    AS CUST_BASE_CREATE_ORG 
       ,CUST_FROM               AS CUST_FROM 
       ,CUST_FROM_NAME          AS CUST_FROM_NAME 
       ,RECENT_UPDATE_USER      AS RECENT_UPDATE_USER 
       ,RECENT_UPDATE_ORG       AS RECENT_UPDATE_ORG 
       ,RECENT_UPDATE_DATE      AS RECENT_UPDATE_DATE 
       ,GROUP_MEMBER_TYPE       AS GROUP_MEMBER_TYPE 
       ,GROUP_TYPE              AS GROUP_TYPE 
       ,FR_ID                   AS FR_ID 
       ,EFFECT_MONTHS           AS EFFECT_MONTHS 
   FROM OCRM_F_CI_BASE_TMP A                                   --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_BASE = sqlContext.sql(sql)
OCRM_F_CI_BASE.registerTempTable("OCRM_F_CI_BASE")
dfn="OCRM_F_CI_BASE/"+V_DT+".parquet"
OCRM_F_CI_BASE.cache()
nrows = OCRM_F_CI_BASE.count()
OCRM_F_CI_BASE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_BASE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_BASE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_BASE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-13::
V_STEP = V_STEP + 1
OCRM_F_CI_BASE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BASE/*')
OCRM_F_CI_BASE.registerTempTable("OCRM_F_CI_BASE")
OCRM_F_CI_RELATE_CUST_BASE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_RELATE_CUST_BASE/*')
OCRM_F_CI_RELATE_CUST_BASE.registerTempTable("OCRM_F_CI_RELATE_CUST_BASE")
sql = """
 SELECT ID                      AS ID 
       ,CRATE_DATE              AS CRATE_DATE 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_BASE_ID            AS CUST_BASE_ID 
       ,CREATE_USER             AS CREATE_USER 
       ,CUST_ZH_NAME            AS CUST_ZH_NAME 
       ,CREATE_ORG              AS CREATE_ORG 
       ,BELONG_ORG_ID           AS BELONG_ORG_ID 
       ,BELONG_ORG_NAME         AS BELONG_ORG_NAME 
       ,BELONG_CUST_MGR_ID      AS BELONG_CUST_MGR_ID 
       ,BELONG_CUST_MGR_NAME    AS BELONG_CUST_MGR_NAME 
       ,FR_ID                   AS FR_ID 
   FROM OCRM_F_CI_RELATE_CUST_BASE T                           --
  WHERE T.CUST_BASE_ID 
    NOT IN(
         SELECT ID 
           FROM OCRM_F_CI_BASE 
          WHERE ADD_MONTHS(CUST_BASE_CREATE_DATE,EFFECT_MONTHS) < V_DT) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_RELATE_CUST_BASE_TMP = sqlContext.sql(sql)
OCRM_F_CI_RELATE_CUST_BASE_TMP.registerTempTable("OCRM_F_CI_RELATE_CUST_BASE_TMP")
dfn="OCRM_F_CI_RELATE_CUST_BASE_TMP/"+V_DT+".parquet"
OCRM_F_CI_RELATE_CUST_BASE_TMP.cache()
nrows = OCRM_F_CI_RELATE_CUST_BASE_TMP.count()
OCRM_F_CI_RELATE_CUST_BASE_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_RELATE_CUST_BASE_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_RELATE_CUST_BASE_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_RELATE_CUST_BASE_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-14::
V_STEP = V_STEP + 1
OCRM_F_CI_RELATE_CUST_BASE_TMP = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_RELATE_CUST_BASE_TMP/*')
OCRM_F_CI_RELATE_CUST_BASE_TMP.registerTempTable("OCRM_F_CI_RELATE_CUST_BASE_TMP")
sql = """
 SELECT ID                      AS ID 
       ,CRATE_DATE              AS CRATE_DATE 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_BASE_ID            AS CUST_BASE_ID 
       ,CREATE_USER             AS CREATE_USER 
       ,CUST_ZH_NAME            AS CUST_ZH_NAME 
       ,CREATE_ORG              AS CREATE_ORG 
       ,BELONG_ORG_ID           AS BELONG_ORG_ID 
       ,BELONG_ORG_NAME         AS BELONG_ORG_NAME 
       ,BELONG_CUST_MGR_ID      AS BELONG_CUST_MGR_ID 
       ,BELONG_CUST_MGR_NAME    AS BELONG_CUST_MGR_NAME 
       ,FR_ID                   AS FR_ID 
   FROM OCRM_F_CI_RELATE_CUST_BASE_TMP T                       --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_RELATE_CUST_BASE = sqlContext.sql(sql)
OCRM_F_CI_RELATE_CUST_BASE.registerTempTable("OCRM_F_CI_RELATE_CUST_BASE")
dfn="OCRM_F_CI_RELATE_CUST_BASE/"+V_DT+".parquet"
OCRM_F_CI_RELATE_CUST_BASE.cache()
nrows = OCRM_F_CI_RELATE_CUST_BASE.count()
OCRM_F_CI_RELATE_CUST_BASE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_RELATE_CUST_BASE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_RELATE_CUST_BASE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_RELATE_CUST_BASE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-15::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(ID   AS DECIMAL(27))                   AS GROUP_ID 
       ,CUST_BASE_NAME          AS GROUP_NAME 
       ,CAST(''     AS DECIMAL(20,2))               AS DEP_BAL 
       ,CAST(''     AS DECIMAL(20,2))               AS LN_BAL 
       ,FR_ID                   AS FR_ID 
       ,V_DT               AS CRM_DT 
   FROM OCRM_F_CI_BASE A                                       --
  WHERE FR_ID IS NOT NULL 
    AND TRIM(FR_ID) <> '' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_GROUP_DEP_CRE = sqlContext.sql(sql)
ACRM_F_GROUP_DEP_CRE.registerTempTable("ACRM_F_GROUP_DEP_CRE")
dfn="ACRM_F_GROUP_DEP_CRE/"+V_DT+".parquet"
ACRM_F_GROUP_DEP_CRE.cache()
nrows = ACRM_F_GROUP_DEP_CRE.count()
ACRM_F_GROUP_DEP_CRE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_GROUP_DEP_CRE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_GROUP_DEP_CRE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-16::
V_STEP = V_STEP + 1
ACRM_F_GROUP_DEP_CRE = sqlContext.read.parquet(hdfs+'/ACRM_F_GROUP_DEP_CRE/*')
ACRM_F_GROUP_DEP_CRE.registerTempTable("ACRM_F_GROUP_DEP_CRE")
OCRM_F_CI_RELATE_CUST_BASE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_RELATE_CUST_BASE/*')
OCRM_F_CI_RELATE_CUST_BASE.registerTempTable("OCRM_F_CI_RELATE_CUST_BASE")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
sql = """
 SELECT T.GROUP_ID              AS GROUP_ID 
       ,T.GROUP_NAME            AS GROUP_NAME 
       ,T.DEP_BAL               AS DEP_BAL 
       ,CAST(SUM(B.BAL)  AS DECIMAL(20,2))                     AS LN_BAL 
       ,T.FR_ID                 AS FR_ID 
       ,T.CRM_DT                AS CRM_DT 
   FROM ACRM_F_GROUP_DEP_CRE T                                 --
  INNER JOIN OCRM_F_CI_RELATE_CUST_BASE A                      --
     ON T.GROUP_ID              = A.CUST_BASE_ID 
    AND T.FR_ID                 = A.FR_ID 
  INNER JOIN 
 (SELECT CUST_ID,FR_ID,
                              SUM(BAL_RMB) AS BAL
                         FROM ACRM_F_CI_ASSET_BUSI_PROTO
                        WHERE BAL_RMB > 0
                        GROUP BY CUST_ID,FR_ID
) B                                                 --
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE T.CRM_DT                = V_DT 
  GROUP BY T.GROUP_ID 
       ,T.FR_ID
	   ,T.GROUP_NAME
		,T.DEP_BAL
		,T.CRM_DT"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_GROUP_DEP_CRE_INNTMP1 = sqlContext.sql(sql)
ACRM_F_GROUP_DEP_CRE_INNTMP1.registerTempTable("ACRM_F_GROUP_DEP_CRE_INNTMP1")


sql = """
 SELECT DST.GROUP_ID                                            --客户群编号:src.GROUP_ID
       ,DST.GROUP_NAME                                         --客户群名称:src.GROUP_NAME
       ,DST.DEP_BAL                                            --存款余额:src.DEP_BAL
       ,DST.LN_BAL                                             --贷款余额:src.LN_BAL
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --跑批日期:src.CRM_DT
   FROM ACRM_F_GROUP_DEP_CRE DST 
   LEFT JOIN ACRM_F_GROUP_DEP_CRE_INNTMP1 SRC 
     ON SRC.GROUP_ID            = DST.GROUP_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_GROUP_DEP_CRE_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_GROUP_DEP_CRE/"+V_DT+".parquet"
UNION=ACRM_F_GROUP_DEP_CRE_INNTMP2.unionAll(ACRM_F_GROUP_DEP_CRE_INNTMP1)
ACRM_F_GROUP_DEP_CRE_INNTMP1.cache()
ACRM_F_GROUP_DEP_CRE_INNTMP2.cache()
nrowsi = ACRM_F_GROUP_DEP_CRE_INNTMP1.count()
nrowsa = ACRM_F_GROUP_DEP_CRE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_GROUP_DEP_CRE_INNTMP1.unpersist()
ACRM_F_GROUP_DEP_CRE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_GROUP_DEP_CRE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_GROUP_DEP_CRE/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_GROUP_DEP_CRE_BK/")

#任务[12] 001-17::
V_STEP = V_STEP + 1
ACRM_F_GROUP_DEP_CRE = sqlContext.read.parquet(hdfs+'/ACRM_F_GROUP_DEP_CRE/*')
ACRM_F_GROUP_DEP_CRE.registerTempTable("ACRM_F_GROUP_DEP_CRE")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
sql = """
 SELECT CAST(T.GROUP_ID   AS DECIMAL(27))           AS GROUP_ID 
       ,T.GROUP_NAME            AS GROUP_NAME 
       ,CAST(SUM(B.BAL)   AS DECIMAL(20,2))                    AS DEP_BAL 
       ,CAST(T.LN_BAL    AS DECIMAL(20,2))            AS LN_BAL 
       ,T.FR_ID                 AS FR_ID 
       ,T.CRM_DT                AS CRM_DT 
   FROM ACRM_F_GROUP_DEP_CRE T                                 --
  INNER JOIN OCRM_F_CI_RELATE_CUST_BASE A                      --
     ON T.GROUP_ID              = A.CUST_BASE_ID 
    AND T.FR_ID                 = A.FR_ID 
  INNER JOIN 
 (SELECT CUST_ID,FR_ID,
                             SUM(BAL_RMB) AS BAL
                            FROM ACRM_F_DP_SAVE_INFO
                           WHERE ACCT_STATUS = '01'  
                             AND DETP<>'98' 
                             AND DETP<>'1A'
                           GROUP BY CUST_ID,FR_ID
) B                                                 --
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE T.CRM_DT                = V_DT 
  GROUP BY T.GROUP_ID  
       ,T.GROUP_NAME
       ,T.LN_BAL 
       ,T.FR_ID 
       ,T.CRM_DT   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_GROUP_DEP_CRE_INNTMP1 = sqlContext.sql(sql)
ACRM_F_GROUP_DEP_CRE_INNTMP1.registerTempTable("ACRM_F_GROUP_DEP_CRE_INNTMP1")

sql = """
 SELECT DST.GROUP_ID                                            --客户群编号:src.GROUP_ID
       ,DST.GROUP_NAME                                         --客户群名称:src.GROUP_NAME
       ,DST.DEP_BAL                                            --存款余额:src.DEP_BAL
       ,DST.LN_BAL                                             --贷款余额:src.LN_BAL
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --跑批日期:src.CRM_DT
   FROM ACRM_F_GROUP_DEP_CRE DST 
   LEFT JOIN ACRM_F_GROUP_DEP_CRE_INNTMP1 SRC 
     ON SRC.GROUP_ID            = DST.GROUP_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GROUP_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_GROUP_DEP_CRE_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_GROUP_DEP_CRE/"+V_DT+".parquet"
ACRM_F_GROUP_DEP_CRE_INNTMP2=ACRM_F_GROUP_DEP_CRE_INNTMP2.unionAll(ACRM_F_GROUP_DEP_CRE_INNTMP1)
ACRM_F_GROUP_DEP_CRE_INNTMP1.cache()
ACRM_F_GROUP_DEP_CRE_INNTMP2.cache()
nrowsi = ACRM_F_GROUP_DEP_CRE_INNTMP1.count()
nrowsa = ACRM_F_GROUP_DEP_CRE_INNTMP2.count()

ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_GROUP_DEP_CRE/*.parquet")
ACRM_F_GROUP_DEP_CRE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_GROUP_DEP_CRE_INNTMP1.unpersist()
ACRM_F_GROUP_DEP_CRE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_GROUP_DEP_CRE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)

#备份当天生成数据
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_GROUP_DEP_CRE/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_GROUP_DEP_CRE_BK/")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_GROUP_DEP_CRE/"+V_DT+".parquet /"+dbname+"/ACRM_F_GROUP_DEP_CRE_BK/")
