#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUST_RALE').setMaster(sys.argv[2])
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

F_CI_XDXT_CUSTOMER_RELATIVE = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_RELATIVE/*')
F_CI_XDXT_CUSTOMER_RELATIVE.registerTempTable("F_CI_XDXT_CUSTOMER_RELATIVE")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_LN_XDXT_GUARANTY_CONTRACT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_GUARANTY_CONTRACT/*')
F_LN_XDXT_GUARANTY_CONTRACT.registerTempTable("F_LN_XDXT_GUARANTY_CONTRACT")
F_CI_XDXT_CUSTOMER_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_INFO/*')
F_CI_XDXT_CUSTOMER_INFO.registerTempTable("F_CI_XDXT_CUSTOMER_INFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS BIGINT)                   AS ID 
       ,B.CUSTOMERNAME          AS CUST_NAME 
       ,A.CUSTOMERID            AS CUST_ID 
       ,A.GUARANTORID           AS RELA_CUST_ID 
       ,A.GUARANTORNAME         AS RELA_CUST_NAME 
       ,''                    AS RELATION_TYPE 
       ,'7701'                  AS RELATION_NAME 
       ,''                    AS ASSESS_FLAG 
       ,''                    AS DIRECT 
       ,CAST('' AS DECIMAL(20,2))                  AS AMT 
       ,CAST('' AS  DECIMAL(5,2))                 AS KG_RATE 
       ,''                    AS DEMO 
       ,A.INPUTUSERID           AS CREATE_USER 
       ,A.INPUTORGID            AS CREATE_ORG 
       ,CASE WHEN A.INPUTDATE = '' THEN '' ELSE CONCAT(SUBSTR(A.INPUTDATE,1,4),'-',SUBSTR(A.INPUTDATE,5,2),'-',SUBSTR(A.INPUTDATE,7,2))  END AS CREATE_DATE 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                    AS APP_STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM F_LN_XDXT_GUARANTY_CONTRACT A                          --担保合同信息
  INNER JOIN F_CI_XDXT_CUSTOMER_INFO B                         --客户基本信息
     ON A.CUSTOMERID            = B.CUSTOMERID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_ACRM_F_CI_CUST_RALE = sqlContext.sql(sql)
TEMP_ACRM_F_CI_CUST_RALE.registerTempTable("TEMP_ACRM_F_CI_CUST_RALE")
dfn="TEMP_ACRM_F_CI_CUST_RALE/"+V_DT+".parquet"
TEMP_ACRM_F_CI_CUST_RALE.cache()
nrows = TEMP_ACRM_F_CI_CUST_RALE.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_ACRM_F_CI_CUST_RALE/*.parquet")
TEMP_ACRM_F_CI_CUST_RALE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TEMP_ACRM_F_CI_CUST_RALE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_ACRM_F_CI_CUST_RALE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
TEMP_ACRM_F_CI_CUST_RALE = sqlContext.read.parquet(hdfs+'/TEMP_ACRM_F_CI_CUST_RALE/*')
TEMP_ACRM_F_CI_CUST_RALE.registerTempTable("TEMP_ACRM_F_CI_CUST_RALE")
sql = """
 SELECT CAST(''  AS BIGINT)                  AS ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ID               AS CUST_ID 
       ,A.RELA_CUST_ID          AS RELA_CUST_ID 
       ,A.RELA_CUST_NAME        AS RELA_CUST_NAME 
       ,'1'                     AS RELATION_TYPE 
       ,'7703'                  AS RELATION_NAME 
       ,A.ASSESS_FLAG           AS ASSESS_FLAG 
       ,A.DIRECT                AS DIRECT 
       ,CAST(A.AMT  AS  DECIMAL(20,2))               AS AMT 
       ,CAST(A.KG_RATE   AS DECIMAL(5,2))            AS KG_RATE 
       ,A.DEMO                  AS DEMO 
       ,A.CREATE_USER           AS CREATE_USER 
       ,A.CREATE_ORG            AS CREATE_ORG 
       ,A.CREATE_DATE           AS CREATE_DATE 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                    AS APP_STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_ACRM_F_CI_CUST_RALE A                             --关联客户表
  INNER JOIN TEMP_ACRM_F_CI_CUST_RALE B                        --关联客户表
     ON A.CUST_ID               = B.RELA_CUST_ID 
    AND A.RELA_CUST_ID          = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.CUST_ID <> A.RELA_CUST_ID 
  GROUP BY A.CUST_NAME      
,A.CUST_ID       
,A.RELA_CUST_ID  
,A.RELA_CUST_NAME         
,A.ASSESS_FLAG   
,A.DIRECT        
,A.AMT           
,A.KG_RATE       
,A.DEMO          
,A.CREATE_USER   
,A.CREATE_ORG    
,A.CREATE_DATE   
,A.ODS_ST_DATE                 
,A.FR_ID         
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_RALE = sqlContext.sql(sql)
ACRM_F_CI_CUST_RALE.registerTempTable("ACRM_F_CI_CUST_RALE")
dfn="ACRM_F_CI_CUST_RALE/"+V_DT+".parquet"
ACRM_F_CI_CUST_RALE.cache()
nrows = ACRM_F_CI_CUST_RALE.count()
ACRM_F_CI_CUST_RALE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_RALE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_CUST_RALE/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_RALE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(''     AS BIGINT)              AS ID 
       ,A.RELA_CUST_NAME        AS CUST_NAME 
       ,A.RELA_CUST_ID          AS CUST_ID 
       ,A.CUST_ID               AS RELA_CUST_ID 
       ,A.CUST_NAME             AS RELA_CUST_NAME 
       ,'1'                     AS RELATION_TYPE 
       ,'7702'                  AS RELATION_NAME 
       ,A.ASSESS_FLAG           AS ASSESS_FLAG 
       ,A.DIRECT                AS DIRECT 
       ,CAST(A.AMT  AS  DECIMAL(20,2))               AS AMT 
       ,CAST(A.KG_RATE   AS DECIMAL(5,2))            AS KG_RATE  
       ,A.DEMO                  AS DEMO 
       ,A.CREATE_USER           AS CREATE_USER 
       ,A.CREATE_ORG            AS CREATE_ORG 
       ,A.CREATE_DATE           AS CREATE_DATE 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                    AS APP_STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_ACRM_F_CI_CUST_RALE A                             --关联客户表
   LEFT JOIN TEMP_ACRM_F_CI_CUST_RALE B                        --关联客户表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.RELA_CUST_ID          = B.RELA_CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE B.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_RALE = sqlContext.sql(sql)
dfn="ACRM_F_CI_CUST_RALE/"+V_DT+".parquet"
ACRM_F_CI_CUST_RALE.cache()
nrows = ACRM_F_CI_CUST_RALE.count()
ACRM_F_CI_CUST_RALE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_RALE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_RALE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1
ACRM_F_CI_CUST_RALE = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_RALE/*')
ACRM_F_CI_CUST_RALE.registerTempTable("ACRM_F_CI_CUST_RALE")
sql = """
 SELECT CAST(''  AS BIGINT)                  AS ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ID               AS CUST_ID 
       ,A.RELA_CUST_ID          AS RELA_CUST_ID 
       ,A.RELA_CUST_NAME        AS RELA_CUST_NAME 
       ,'1'                     AS RELATION_TYPE 
       ,'7701'                  AS RELATION_NAME 
       ,A.ASSESS_FLAG           AS ASSESS_FLAG 
       ,A.DIRECT                AS DIRECT 
       ,CAST(A.AMT  AS  DECIMAL(20,2))               AS AMT 
       ,CAST(A.KG_RATE   AS DECIMAL(5,2))            AS KG_RATE 
       ,A.DEMO                  AS DEMO 
       ,A.CREATE_USER           AS CREATE_USER 
       ,A.CREATE_ORG            AS CREATE_ORG 
       ,A.CREATE_DATE           AS CREATE_DATE 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                    AS APP_STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_ACRM_F_CI_CUST_RALE A                             --关联客户表
  INNER JOIN (SELECT MAX(CREATE_DATE)  CREATE_DATE,RELA_CUST_ID,CUST_ID,FR_ID FROM TEMP_ACRM_F_CI_CUST_RALE B GROUP BY RELA_CUST_ID,CUST_ID,FR_ID
) B                        --关联客户表
     ON A.RELA_CUST_ID          = B.RELA_CUST_ID 
    AND A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.CREATE_DATE           = B.CREATE_DATE 
   LEFT JOIN ACRM_F_CI_CUST_RALE C                             --
     ON A.CUST_ID               = C.CUST_ID 
    AND A.RELA_CUST_ID          = C.RELA_CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.RELATION_NAME         = '7703' 
  WHERE C.CUST_ID IS NULL 
  GROUP BY A.CUST_NAME                                           
       ,A.CUST_ID                                      
       ,A.RELA_CUST_ID                                 
       ,A.RELA_CUST_NAME                                         
       ,A.ASSESS_FLAG                                  
       ,A.DIRECT                                       
       ,A.AMT                                          
       ,A.KG_RATE                                      
       ,A.DEMO                                         
       ,A.CREATE_USER                                  
       ,A.CREATE_ORG                                   
       ,A.CREATE_DATE                                  
       ,A.ODS_ST_DATE                                       
       ,A.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_RALE = sqlContext.sql(sql)
dfn="ACRM_F_CI_CUST_RALE/"+V_DT+".parquet"
ACRM_F_CI_CUST_RALE.cache()
nrows = ACRM_F_CI_CUST_RALE.count()
ACRM_F_CI_CUST_RALE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_RALE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_RALE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST(''   AS BIGINT)                 AS ID 
       ,A.CUSTOMERNAME          AS CUST_NAME 
       ,A.RELATIVEID            AS CUST_ID 
       ,B.RELATIVEID            AS RELA_CUST_ID 
       ,B.CUSTOMERNAME          AS RELA_CUST_NAME 
       ,'2'                     AS RELATION_TYPE 
       ,'0701'                  AS RELATION_NAME 
       ,''                    AS ASSESS_FLAG 
       ,''                    AS DIRECT 
       ,CAST(''  AS DECIMAL(20,2))               AS AMT 
       ,CAST(''   AS DECIMAL(5,2))            AS KG_RATE 
       ,'同一股东占股50%以上(实际控制人)'  AS DEMO 
       ,''                    AS CREATE_USER 
       ,''                    AS CREATE_ORG 
       ,''                    AS CREATE_DATE 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                    AS APP_STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM F_CI_XDXT_CUSTOMER_RELATIVE A                          --客户关联信息
  INNER JOIN F_CI_XDXT_CUSTOMER_RELATIVE B                     --客户关联信息
     ON A.CUSTOMERID            = B.CUSTOMERID 
    AND A.RELATIONSHIP          = B.RELATIONSHIP 
    AND A.FR_ID                 = B.FR_ID 
    AND A.RELATIVEID <> B.RELATIVEID 
    AND B.INVESTMENTPROP > 50 
  WHERE A.RELATIONSHIP          = '5109' 
    AND A.INVESTMENTPROP > 50 
  GROUP BY A.CUSTOMERNAME 
       ,A.RELATIVEID 
       ,B.RELATIVEID 
       ,B.CUSTOMERNAME 
       ,A.ODS_ST_DATE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_RALE = sqlContext.sql(sql)
dfn="ACRM_F_CI_CUST_RALE/"+V_DT+".parquet"
ACRM_F_CI_CUST_RALE.cache()
nrows = ACRM_F_CI_CUST_RALE.count()
ACRM_F_CI_CUST_RALE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_RALE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_RALE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-06::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS BIGINT)                    AS ID 
       ,A.CUSTOMERNAME          AS CUST_NAME 
       ,A.RELATIVEID            AS CUST_ID 
       ,B.RELATIVEID            AS RELA_CUST_ID 
       ,B.CUSTOMERNAME          AS RELA_CUST_NAME 
       ,'2'                     AS RELATION_TYPE 
       ,'0702'                  AS RELATION_NAME 
       ,''                    AS ASSESS_FLAG 
       ,''                    AS DIRECT 
       ,CAST('' AS    DECIMAL(20,2))               AS AMT 
       ,CAST(''   AS DECIMAL(5,2))            AS KG_RATE  
       ,'同一法定代表人公司相互关联'       AS DEMO 
       ,''                    AS CREATE_USER 
       ,''                    AS CREATE_ORG 
       ,''                    AS CREATE_DATE 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                    AS APP_STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM F_CI_XDXT_CUSTOMER_RELATIVE A                          --客户关联信息
  INNER JOIN F_CI_XDXT_CUSTOMER_RELATIVE B                     --客户关联信息
     ON A.CUSTOMERID            = B.CUSTOMERID 
    AND A.RELATIONSHIP          = B.RELATIONSHIP 
    AND A.FR_ID                 = B.FR_ID 
    AND A.RELATIVEID <> B.RELATIVEID 
  WHERE A.RELATIONSHIP IN('100', '5100') 
  GROUP BY A.CUSTOMERNAME 
       ,A.RELATIVEID 
       ,B.RELATIVEID 
       ,B.CUSTOMERNAME 
       ,A.ODS_ST_DATE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_RALE = sqlContext.sql(sql)
dfn="ACRM_F_CI_CUST_RALE/"+V_DT+".parquet"
ACRM_F_CI_CUST_RALE.cache()
nrows = ACRM_F_CI_CUST_RALE.count()
ACRM_F_CI_CUST_RALE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_RALE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_RALE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-07::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS BIGINT)                    AS ID 
       ,B.CUSTOMERNAME          AS CUST_NAME 
       ,B.RELATIVEID            AS CUST_ID 
       ,C.RELATIVEID            AS RELA_CUST_ID 
       ,C.CUSTOMERNAME          AS RELA_CUST_NAME 
       ,'1'                     AS RELATION_TYPE 
       ,'0703'                  AS RELATION_NAME 
       ,''                    AS ASSESS_FLAG 
       ,''                    AS DIRECT 
       ,CAST('' AS    DECIMAL(20,2))               AS AMT 
       ,CAST(''   AS DECIMAL(5,2))            AS KG_RATE  
       ,'法人为家庭成员相互关联'     AS DEMO 
       ,''                    AS CREATE_USER 
       ,''                    AS CREATE_ORG 
       ,''                    AS CREATE_DATE 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                    AS APP_STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM F_CI_XDXT_CUSTOMER_RELATIVE A                          --客户关联信息
  INNER JOIN F_CI_XDXT_CUSTOMER_RELATIVE B                     --客户关联信息
     ON A.CUSTOMERID            = B.CUSTOMERID 
    AND B.RELATIONSHIP IN('100', '5100') 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN F_CI_XDXT_CUSTOMER_RELATIVE C                     --客户关联信息
     ON A.RELATIVEID            = C.CUSTOMERID 
    AND C.RELATIONSHIP IN('100', '5100') 
    AND B.RELATIVEID <> C.RELATIVEID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.RELATIONSHIP IN('031', '24', '0303', '20', '21') 
  GROUP BY B.CUSTOMERNAME 
       ,B.RELATIVEID 
       ,C.RELATIVEID 
       ,C.CUSTOMERNAME 
       ,A.ODS_ST_DATE 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_RALE = sqlContext.sql(sql)
dfn="ACRM_F_CI_CUST_RALE/"+V_DT+".parquet"
ACRM_F_CI_CUST_RALE.cache()
nrows = ACRM_F_CI_CUST_RALE.count()
ACRM_F_CI_CUST_RALE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_RALE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_RALE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-08::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS BIGINT)                    AS ID 
       ,A.CUSTOMERID            AS CUST_NAME 
       ,A.CUSTOMERID            AS CUST_ID 
       ,A.RELATIVEID            AS RELA_CUST_ID 
       ,A.CUSTOMERNAME          AS RELA_CUST_NAME 
       ,''                    AS RELATION_TYPE 
       ,A.RELATIONSHIP          AS RELATION_NAME 
       ,''                    AS ASSESS_FLAG 
       ,''                    AS DIRECT 
       ,CAST(A.INVESTMENTSUM  AS DECIMAL(20,2))       AS AMT 
       ,CAST(A.INVESTMENTPROP   AS DECIMAL(5,2))     AS KG_RATE 
       ,A.DESCRIBE              AS DEMO 
       ,A.INPUTUSERID           AS CREATE_USER 
       ,A.INPUTORGID            AS CREATE_ORG 
       ,CASE WHEN A.INPUTDATE             = '' THEN '' ELSE CONCAT(SUBSTR(A.INPUTDATE,1,4),'-',SUBSTR(A.INPUTDATE,5,2),'-',SUBSTR(A.INPUTDATE,7,2))  END AS CREATE_DATE 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,''                    AS APP_STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM F_CI_XDXT_CUSTOMER_RELATIVE A                          --客户关联信息
  WHERE A.RELATIVEID IS NOT NULL 
    AND TRIM(A.RELATIVEID) <> '' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_RALE = sqlContext.sql(sql)
dfn="ACRM_F_CI_CUST_RALE/"+V_DT+".parquet"
ACRM_F_CI_CUST_RALE.cache()
nrows = ACRM_F_CI_CUST_RALE.count()
ACRM_F_CI_CUST_RALE.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_CI_CUST_RALE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_RALE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


#任务[12] 001-09::
V_STEP = V_STEP + 1
ACRM_F_CI_CUST_RALE = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_RALE/*')
ACRM_F_CI_CUST_RALE.registerTempTable("ACRM_F_CI_CUST_RALE")
sql = """
 SELECT CAST(A.ID   AS BIGINT)                 AS ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ID               AS CUST_ID 
       ,B.ODS_CUST_ID           AS RELA_CUST_ID 
       ,B.ODS_CUST_NAME         AS RELA_CUST_NAME 
       ,A.RELATION_TYPE         AS RELATION_TYPE 
       ,A.RELATION_NAME         AS RELATION_NAME 
       ,A.ASSESS_FLAG           AS ASSESS_FLAG 
       ,A.DIRECT                AS DIRECT 
       ,CAST(A.AMT    AS DECIMAL(20,2))               AS AMT 
       ,CAST(A.KG_RATE  AS DECIMAL(5,2))             AS KG_RATE 
       ,A.DEMO                  AS DEMO 
       ,A.CREATE_USER           AS CREATE_USER 
       ,A.CREATE_ORG            AS CREATE_ORG 
       ,A.CREATE_DATE           AS CREATE_DATE 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.APP_STATE             AS APP_STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_CUST_RALE A                                  --关联客户表
  INNER JOIN (SELECT DISTINCT  
               ODS_CUST_ID,
               ODS_CUST_NAME,
               SOURCE_CUST_ID,
               FR_ID
      FROM OCRM_F_CI_SYS_RESOURCE 
     WHERE ODS_SYS_ID='LNA'
) B                          --系统来源中间表
     ON A.FR_ID                 = B.FR_ID 
    AND A.RELA_CUST_ID          = B.SOURCE_CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_RALE_INNTMP1 = sqlContext.sql(sql)
ACRM_F_CI_CUST_RALE_INNTMP1.registerTempTable("ACRM_F_CI_CUST_RALE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_NAME                                          --:src.CUST_NAME
       ,DST.CUST_ID                                            --:src.CUST_ID
       ,DST.RELA_CUST_ID                                       --:src.RELA_CUST_ID
       ,DST.RELA_CUST_NAME                                     --:src.RELA_CUST_NAME
       ,DST.RELATION_TYPE                                      --:src.RELATION_TYPE
       ,DST.RELATION_NAME                                      --:src.RELATION_NAME
       ,DST.ASSESS_FLAG                                        --:src.ASSESS_FLAG
       ,DST.DIRECT                                             --:src.DIRECT
       ,DST.AMT                                                --:src.AMT
       ,DST.KG_RATE                                            --:src.KG_RATE
       ,DST.DEMO                                               --:src.DEMO
       ,DST.CREATE_USER                                        --:src.CREATE_USER
       ,DST.CREATE_ORG                                         --:src.CREATE_ORG
       ,DST.CREATE_DATE                                        --:src.CREATE_DATE
       ,DST.ODS_ST_DATE                                        --:src.ODS_ST_DATE
       ,DST.APP_STATE                                          --0不参与考核1待审批2审批通过3审批未通过:src.APP_STATE
       ,DST.FR_ID                                              --:src.FR_ID
   FROM ACRM_F_CI_CUST_RALE DST 
   LEFT JOIN ACRM_F_CI_CUST_RALE_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_RALE_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_CI_CUST_RALE/"+V_DT+".parquet"
UNION=ACRM_F_CI_CUST_RALE_INNTMP2.unionAll(ACRM_F_CI_CUST_RALE_INNTMP1)
ACRM_F_CI_CUST_RALE_INNTMP1.cache()
ACRM_F_CI_CUST_RALE_INNTMP2.cache()
nrowsi = ACRM_F_CI_CUST_RALE_INNTMP1.count()
nrowsa = ACRM_F_CI_CUST_RALE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_RALE_INNTMP1.unpersist()
ACRM_F_CI_CUST_RALE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_RALE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_CI_CUST_RALE/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_CI_CUST_RALE_BK/")

#任务[12] 001-10::
V_STEP = V_STEP + 1
ACRM_F_CI_CUST_RALE = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_RALE/*')
ACRM_F_CI_CUST_RALE.registerTempTable("ACRM_F_CI_CUST_RALE")
sql = """
 SELECT CAST(A.ID AS BIGINT)                   AS ID 
       ,B.ODS_CUST_NAME         AS CUST_NAME 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,A.RELA_CUST_ID          AS RELA_CUST_ID 
       ,A.RELA_CUST_NAME        AS RELA_CUST_NAME 
       ,A.RELATION_TYPE         AS RELATION_TYPE 
       ,A.RELATION_NAME         AS RELATION_NAME 
       ,A.ASSESS_FLAG           AS ASSESS_FLAG 
       ,A.DIRECT                AS DIRECT 
       ,CAST(A.AMT    AS DECIMAL(20,2))               AS AMT 
       ,CAST(A.KG_RATE  AS DECIMAL(5,2))             AS KG_RATE 
       ,A.DEMO                  AS DEMO 
       ,A.CREATE_USER           AS CREATE_USER 
       ,A.CREATE_ORG            AS CREATE_ORG 
       ,A.CREATE_DATE           AS CREATE_DATE 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.APP_STATE             AS APP_STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_CUST_RALE A                                  --关联客户表
  INNER JOIN (SELECT DISTINCT  
               ODS_CUST_ID,
               ODS_CUST_NAME,
               SOURCE_CUST_ID,
               FR_ID
      FROM OCRM_F_CI_SYS_RESOURCE 
     WHERE ODS_SYS_ID='LNA'
) B                          --系统来源中间表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.SOURCE_CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_RALE_INNTMP1 = sqlContext.sql(sql)
ACRM_F_CI_CUST_RALE_INNTMP1.registerTempTable("ACRM_F_CI_CUST_RALE_INNTMP1")

sql = """
 SELECT DST.ID                                                  --:src.ID
       ,DST.CUST_NAME                                          --:src.CUST_NAME
       ,DST.CUST_ID                                            --:src.CUST_ID
       ,DST.RELA_CUST_ID                                       --:src.RELA_CUST_ID
       ,DST.RELA_CUST_NAME                                     --:src.RELA_CUST_NAME
       ,DST.RELATION_TYPE                                      --:src.RELATION_TYPE
       ,DST.RELATION_NAME                                      --:src.RELATION_NAME
       ,DST.ASSESS_FLAG                                        --:src.ASSESS_FLAG
       ,DST.DIRECT                                             --:src.DIRECT
       ,DST.AMT                                                --:src.AMT
       ,DST.KG_RATE                                            --:src.KG_RATE
       ,DST.DEMO                                               --:src.DEMO
       ,DST.CREATE_USER                                        --:src.CREATE_USER
       ,DST.CREATE_ORG                                         --:src.CREATE_ORG
       ,DST.CREATE_DATE                                        --:src.CREATE_DATE
       ,DST.ODS_ST_DATE                                        --:src.ODS_ST_DATE
       ,DST.APP_STATE                                          --0不参与考核1待审批2审批通过3审批未通过:src.APP_STATE
       ,DST.FR_ID                                              --:src.FR_ID
   FROM ACRM_F_CI_CUST_RALE DST 
   LEFT JOIN ACRM_F_CI_CUST_RALE_INNTMP1 SRC 
     ON SRC.RELA_CUST_ID        = DST.RELA_CUST_ID 
  WHERE SRC.RELA_CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_CUST_RALE_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_CI_CUST_RALE/"+V_DT+".parquet"
UNION=ACRM_F_CI_CUST_RALE_INNTMP2.unionAll(ACRM_F_CI_CUST_RALE_INNTMP1)
ACRM_F_CI_CUST_RALE_INNTMP1.cache()
ACRM_F_CI_CUST_RALE_INNTMP2.cache()
nrowsi = ACRM_F_CI_CUST_RALE_INNTMP1.count()
nrowsa = ACRM_F_CI_CUST_RALE_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_CI_CUST_RALE_INNTMP1.unpersist()
ACRM_F_CI_CUST_RALE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_CUST_RALE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_CI_CUST_RALE/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_CI_CUST_RALE_BK/")
