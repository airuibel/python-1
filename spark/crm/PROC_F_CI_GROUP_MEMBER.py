#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_GROUP_MEMBER').setMaster(sys.argv[2])
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
OCRM_F_CI_GROUP_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_GROUP_INFO/*')
OCRM_F_CI_GROUP_INFO.registerTempTable("OCRM_F_CI_GROUP_INFO")
F_CI_XDXT_GROUP_RELATIVE = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_GROUP_RELATIVE/*')
F_CI_XDXT_GROUP_RELATIVE.registerTempTable("F_CI_XDXT_GROUP_RELATIVE")
OCRM_F_CI_GROUP_MEMBER = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_GROUP_MEMBER_BK/'+V_DT_LD+'.parquet/*')
OCRM_F_CI_GROUP_MEMBER.registerTempTable("OCRM_F_CI_GROUP_MEMBER")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(ID     AS BIGINT)                      AS ID 
       ,CUST_ID                 AS CUST_ID 
       ,APPROVAL_STA            AS APPROVAL_STA 
       ,STAKE                   AS STAKE 
       ,REMARK                  AS REMARK 
       ,SUB_DATE                AS SUB_DATE 
       ,RELATIONSHIP            AS RELATIONSHIP 
       ,UP_DATE                 AS UP_DATE 
       ,RELATIVEID              AS RELATIVEID 
       ,INPUT_ID                AS INPUT_ID 
       ,PROC_TIME               AS PROC_TIME 
       ,REALATION_ID            AS REALATION_ID 
       ,COMMENTS                AS COMMENTS 
       ,HEAD_COMMEN             AS HEAD_COMMEN 
       ,ENTERPRISENAME          AS ENTERPRISENAME 
       ,PNODE_ID                AS PNODE_ID 
       ,APPROVER                AS APPROVER 
       ,APPROVER_NAME           AS APPROVER_NAME 
       ,APP_AFF                 AS APP_AFF 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
       ,FR_ID                   AS FR_ID 
       ,CUST_NAME               AS CUST_NAME 
   FROM OCRM_F_CI_GROUP_MEMBER A                               --集团成员信息表
  WHERE ODS_ST_DATE IS NULL 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_GROUP_MEMBER = sqlContext.sql(sql)
TMP_OCRM_F_CI_GROUP_MEMBER.registerTempTable("TMP_OCRM_F_CI_GROUP_MEMBER")
dfn="TMP_OCRM_F_CI_GROUP_MEMBER/"+V_DT+".parquet"
TMP_OCRM_F_CI_GROUP_MEMBER.cache()
nrows = TMP_OCRM_F_CI_GROUP_MEMBER.count()
TMP_OCRM_F_CI_GROUP_MEMBER.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_GROUP_MEMBER.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_GROUP_MEMBER/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_GROUP_MEMBER lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
TMP_OCRM_F_CI_GROUP_MEMBER = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_GROUP_MEMBER/*')
TMP_OCRM_F_CI_GROUP_MEMBER.registerTempTable("TMP_OCRM_F_CI_GROUP_MEMBER")
sql = """
 SELECT CAST(ID     AS BIGINT)                      AS ID 
       ,CUST_ID                 AS CUST_ID 
       ,APPROVAL_STA            AS APPROVAL_STA 
       ,STAKE                   AS STAKE 
       ,REMARK                  AS REMARK 
       ,SUB_DATE                AS SUB_DATE 
       ,RELATIONSHIP            AS RELATIONSHIP 
       ,UP_DATE                 AS UP_DATE 
       ,RELATIVEID              AS RELATIVEID 
       ,INPUT_ID                AS INPUT_ID 
       ,PROC_TIME               AS PROC_TIME 
       ,REALATION_ID            AS REALATION_ID 
       ,COMMENTS                AS COMMENTS 
       ,HEAD_COMMEN             AS HEAD_COMMEN 
       ,ENTERPRISENAME          AS ENTERPRISENAME 
       ,PNODE_ID                AS PNODE_ID 
       ,APPROVER                AS APPROVER 
       ,APPROVER_NAME           AS APPROVER_NAME 
       ,APP_AFF                 AS APP_AFF 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
       ,FR_ID                   AS FR_ID 
       ,CUST_NAME               AS CUST_NAME 
   FROM TMP_OCRM_F_CI_GROUP_MEMBER A                           --集团成员信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_GROUP_MEMBER = sqlContext.sql(sql)
dfn="OCRM_F_CI_GROUP_MEMBER/"+V_DT+".parquet"
OCRM_F_CI_GROUP_MEMBER.cache()
nrows = OCRM_F_CI_GROUP_MEMBER.count()
OCRM_F_CI_GROUP_MEMBER.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_GROUP_MEMBER.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_GROUP_MEMBER/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_GROUP_MEMBER lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id() AS BIGINT)      AS ID 
       ,A3.ODS_CUST_ID          AS CUST_ID 
       ,''                    AS APPROVAL_STA 
       ,''                    AS STAKE 
       ,''                    AS REMARK 
       ,''                    AS SUB_DATE 
       ,'1'                     AS RELATIONSHIP 
       ,''                    AS UP_DATE 
       ,A2.GROUP_NO             AS RELATIVEID 
       ,''                    AS INPUT_ID 
       ,''                    AS PROC_TIME 
       ,''                    AS REALATION_ID 
       ,''                    AS COMMENTS 
       ,''                    AS HEAD_COMMEN 
       ,''                    AS ENTERPRISENAME 
       ,CAST(A2.ID AS VARCHAR(100))                   AS PNODE_ID 
       ,''                    AS APPROVER 
       ,''                    AS APPROVER_NAME 
       ,''                    AS APP_AFF 
       ,V_DT                  AS ODS_ST_DATE 
       ,A1.FR_ID                AS FR_ID 
       ,A3.ODS_CUST_NAME        AS CUST_NAME 
   FROM F_CI_XDXT_GROUP_RELATIVE A1                            --集团客户成员关联表
  INNER JOIN OCRM_F_CI_GROUP_INFO A2                           --集团信息表
     ON A1.RELATIVEID           = A2.GROUP_ROOT_CUST_ID 
    AND A2.ODS_ST_DATE IS NOT NULL 
  INNER JOIN OCRM_F_CI_SYS_RESOURCE A3                         --系统来源中间表
     ON A1.CUSTOMERID           = A3.SOURCE_CUST_ID 
    AND A3.ODS_SYS_ID           = 'LNA' 
    AND LENGTH(A3.ODS_CUST_NAME) <= 50 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_GROUP_MEMBER = sqlContext.sql(sql)
dfn="OCRM_F_CI_GROUP_MEMBER/"+V_DT+".parquet"
OCRM_F_CI_GROUP_MEMBER.cache()
nrows = OCRM_F_CI_GROUP_MEMBER.count()
OCRM_F_CI_GROUP_MEMBER.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_GROUP_MEMBER.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_GROUP_MEMBER lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id() AS BIGINT)      AS ID 
       ,A3.ODS_CUST_ID          AS CUST_ID 
       ,''                    AS APPROVAL_STA 
       ,''                    AS STAKE 
       ,''                    AS REMARK 
       ,''                    AS SUB_DATE 
       ,'1'                     AS RELATIONSHIP 
       ,''                    AS UP_DATE 
       ,A2.GROUP_NO             AS RELATIVEID 
       ,''                    AS INPUT_ID 
       ,''                    AS PROC_TIME 
       ,''                    AS REALATION_ID 
       ,''                    AS COMMENTS 
       ,''                    AS HEAD_COMMEN 
       ,''                    AS ENTERPRISENAME 
       ,CAST(A2.ID AS VARCHAR(100))                  AS PNODE_ID 
       ,''                    AS APPROVER 
       ,''                    AS APPROVER_NAME 
       ,''                    AS APP_AFF 
       ,V_DT                  AS ODS_ST_DATE 
       ,A1.FR_ID                AS FR_ID 
       ,SUBSTR(A3.ODS_CUST_NAME, 1, 50)                       AS CUST_NAME 
   FROM F_CI_XDXT_GROUP_RELATIVE A1                            --集团客户成员关联表
  INNER JOIN OCRM_F_CI_GROUP_INFO A2                           --集团信息表
     ON A1.RELATIVEID           = A2.GROUP_ROOT_CUST_ID 
    AND A2.ODS_ST_DATE IS NOT NULL 
  INNER JOIN OCRM_F_CI_SYS_RESOURCE A3                         --系统来源中间表
     ON A1.CUSTOMERID           = A3.SOURCE_CUST_ID 
    AND A3.ODS_SYS_ID           = 'LNA' 
    AND LENGTH(A3.ODS_CUST_NAME) > 50 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_GROUP_MEMBER = sqlContext.sql(sql)
dfn="OCRM_F_CI_GROUP_MEMBER/"+V_DT+".parquet"
OCRM_F_CI_GROUP_MEMBER.cache()
nrows = OCRM_F_CI_GROUP_MEMBER.count()
OCRM_F_CI_GROUP_MEMBER.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_GROUP_MEMBER.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_GROUP_MEMBER lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id() AS BIGINT)      AS ID 
       ,GROUP_ROOT_CUST_ID      AS CUST_ID 
       ,''                    AS APPROVAL_STA 
       ,''                    AS STAKE 
       ,''                    AS REMARK 
       ,''                    AS SUB_DATE 
       ,'1'                     AS RELATIONSHIP 
       ,''                    AS UP_DATE 
       ,A1.GROUP_NO             AS RELATIVEID 
       ,''                    AS INPUT_ID 
       ,''                    AS PROC_TIME 
       ,''                    AS REALATION_ID 
       ,''                    AS COMMENTS 
       ,''                    AS HEAD_COMMEN 
       ,''                    AS ENTERPRISENAME 
       ,'0'                     AS PNODE_ID 
       ,''                    AS APPROVER 
       ,''                    AS APPROVER_NAME 
       ,''                    AS APP_AFF 
       ,V_DT                  AS ODS_ST_DATE 
       ,A1.FR_ID                AS FR_ID 
       ,CUST_NAME               AS CUST_NAME 
   FROM OCRM_F_CI_GROUP_INFO A1                                --集团信息表
  WHERE LENGTH(CUST_NAME) <= 50 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_GROUP_MEMBER = sqlContext.sql(sql)
dfn="OCRM_F_CI_GROUP_MEMBER/"+V_DT+".parquet"
OCRM_F_CI_GROUP_MEMBER.cache()
nrows = OCRM_F_CI_GROUP_MEMBER.count()
OCRM_F_CI_GROUP_MEMBER.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_GROUP_MEMBER.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_GROUP_MEMBER lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-06::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(monotonically_increasing_id() AS BIGINT)     AS ID 
       ,GROUP_ROOT_CUST_ID      AS CUST_ID 
       ,''                    AS APPROVAL_STA 
       ,''                    AS STAKE 
       ,''                    AS REMARK 
       ,''                    AS SUB_DATE 
       ,'1'                     AS RELATIONSHIP 
       ,''                    AS UP_DATE 
       ,A1.GROUP_NO             AS RELATIVEID 
       ,''                    AS INPUT_ID 
       ,''                    AS PROC_TIME 
       ,''                    AS REALATION_ID 
       ,''                    AS COMMENTS 
       ,''                    AS HEAD_COMMEN 
       ,''                    AS ENTERPRISENAME 
       ,'0'                     AS PNODE_ID 
       ,''                    AS APPROVER 
       ,''                    AS APPROVER_NAME 
       ,''                    AS APP_AFF 
       ,V_DT                  AS ODS_ST_DATE 
       ,A1.FR_ID                AS FR_ID 
       ,SUBSTR(CUST_NAME, 1, 50)                       AS CUST_NAME 
   FROM OCRM_F_CI_GROUP_INFO A1                                --集团信息表
  WHERE LENGTH(CUST_NAME) > 50 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_GROUP_MEMBER = sqlContext.sql(sql)
dfn="OCRM_F_CI_GROUP_MEMBER/"+V_DT+".parquet"
OCRM_F_CI_GROUP_MEMBER.cache()
nrows = OCRM_F_CI_GROUP_MEMBER.count()
OCRM_F_CI_GROUP_MEMBER.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_GROUP_MEMBER.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_GROUP_MEMBER lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-07::
V_STEP = V_STEP + 1

OCRM_F_CI_GROUP_MEMBER = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_GROUP_MEMBER/*')
OCRM_F_CI_GROUP_MEMBER.registerTempTable("OCRM_F_CI_GROUP_MEMBER")
sql = """
 SELECT CAST(A.ID     AS BIGINT)                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.APPROVAL_STA          AS APPROVAL_STA 
       ,A.STAKE                 AS STAKE 
       ,A.REMARK                AS REMARK 
       ,A.SUB_DATE              AS SUB_DATE 
       ,A.RELATIONSHIP          AS RELATIONSHIP 
       ,A.UP_DATE               AS UP_DATE 
       ,A.RELATIVEID            AS RELATIVEID 
       ,A.INPUT_ID              AS INPUT_ID 
       ,A.PROC_TIME             AS PROC_TIME 
       ,A.REALATION_ID          AS REALATION_ID 
       ,A.COMMENTS              AS COMMENTS 
       ,A.HEAD_COMMEN           AS HEAD_COMMEN 
       ,A.ENTERPRISENAME        AS ENTERPRISENAME 
       ,CAST(B.ID AS VARCHAR(100))                   AS PNODE_ID 
       ,A.APPROVER              AS APPROVER 
       ,A.APPROVER_NAME         AS APPROVER_NAME 
       ,A.APP_AFF               AS APP_AFF 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.FR_ID                 AS FR_ID 
       ,A.CUST_NAME             AS CUST_NAME 
   FROM OCRM_F_CI_GROUP_MEMBER A                               --集团成员信息表
  INNER JOIN (SELECT MIN(ID) ID ,RELATIVEID 
        FROM OCRM_F_CI_GROUP_MEMBER  
        WHERE PNODE_ID = '0' GROUP BY RELATIVEID) B ON A.RELATIVEID            = B.RELATIVEID 
  WHERE A.PNODE_ID <> '0'  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_GROUP_MEMBER_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_GROUP_MEMBER_INNTMP1.registerTempTable("OCRM_F_CI_GROUP_MEMBER_INNTMP1")

sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_ID                                            --:src.CUST_ID
       ,DST.APPROVAL_STA                                       --:src.APPROVAL_STA
       ,DST.STAKE                                              --:src.STAKE
       ,DST.REMARK                                             --:src.REMARK
       ,DST.SUB_DATE                                           --:src.SUB_DATE
       ,DST.RELATIONSHIP                                       --:src.RELATIONSHIP
       ,DST.UP_DATE                                            --:src.UP_DATE
       ,DST.RELATIVEID                                         --:src.RELATIVEID
       ,DST.INPUT_ID                                           --:src.INPUT_ID
       ,DST.PROC_TIME                                          --:src.PROC_TIME
       ,DST.REALATION_ID                                       --:src.REALATION_ID
       ,DST.COMMENTS                                           --:src.COMMENTS
       ,DST.HEAD_COMMEN                                        --:src.HEAD_COMMEN
       ,DST.ENTERPRISENAME                                     --:src.ENTERPRISENAME
       ,DST.PNODE_ID                                           --:src.PNODE_ID
       ,DST.APPROVER                                           --:src.APPROVER
       ,DST.APPROVER_NAME                                      --:src.APPROVER_NAME
       ,DST.APP_AFF                                            --:src.APP_AFF
       ,DST.ODS_ST_DATE                                        --:src.ODS_ST_DATE
       ,DST.FR_ID                                              --:src.FR_ID
       ,DST.CUST_NAME                                          --:src.CUST_NAME
   FROM OCRM_F_CI_GROUP_MEMBER DST 
   LEFT JOIN OCRM_F_CI_GROUP_MEMBER_INNTMP1 SRC 
     ON SRC.ID                  = DST.ID 
  WHERE SRC.ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_GROUP_MEMBER_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_GROUP_MEMBER/"+V_DT+".parquet"
UNION=OCRM_F_CI_GROUP_MEMBER_INNTMP2.unionAll(OCRM_F_CI_GROUP_MEMBER_INNTMP1)
OCRM_F_CI_GROUP_MEMBER_INNTMP1.cache()
OCRM_F_CI_GROUP_MEMBER_INNTMP2.cache()
nrowsi = OCRM_F_CI_GROUP_MEMBER_INNTMP1.count()
nrowsa = OCRM_F_CI_GROUP_MEMBER_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_GROUP_MEMBER_INNTMP1.unpersist()
OCRM_F_CI_GROUP_MEMBER_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_GROUP_MEMBER lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_GROUP_MEMBER_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_GROUP_MEMBER/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_GROUP_MEMBER_BK/")
