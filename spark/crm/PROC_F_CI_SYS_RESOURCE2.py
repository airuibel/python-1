#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_SYS_RESOURCE2').setMaster(sys.argv[2])
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

F_CI_CBOD_CICIEUNN = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIEUNN/*')
F_CI_CBOD_CICIEUNN.registerTempTable("F_CI_CBOD_CICIEUNN")
F_CI_CBOD_CICIFUNN = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIFUNN/*')
F_CI_CBOD_CICIFUNN.registerTempTable("F_CI_CBOD_CICIFUNN")
F_CI_DXPT_CUSTOMER = sqlContext.read.parquet(hdfs+'/F_CI_DXPT_CUSTOMER/*')
F_CI_DXPT_CUSTOMER.registerTempTable("F_CI_DXPT_CUSTOMER")
F_CI_SUN_IND_INFO = sqlContext.read.parquet(hdfs+'/F_CI_SUN_IND_INFO/*')
F_CI_SUN_IND_INFO.registerTempTable("F_CI_SUN_IND_INFO")
F_CI_WSYH_ECCIFID = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFID/*')
F_CI_WSYH_ECCIFID.registerTempTable("F_CI_WSYH_ECCIFID")
F_CI_XDXT_CUSTOMER_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_INFO/*')
F_CI_XDXT_CUSTOMER_INFO.registerTempTable("F_CI_XDXT_CUSTOMER_INFO")
F_CI_XDXT_CUSTOMER_RELATIVE = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_RELATIVE/*')
F_CI_XDXT_CUSTOMER_RELATIVE.registerTempTable("F_CI_XDXT_CUSTOMER_RELATIVE")
F_CI_XDXT_ENT_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_ENT_INFO/*')
F_CI_XDXT_ENT_INFO.registerTempTable("F_CI_XDXT_ENT_INFO")
F_CI_XDXT_IND_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_INFO/*')
F_CI_XDXT_IND_INFO.registerTempTable("F_CI_XDXT_IND_INFO")
F_LN_XDXT_GUARANTY_CONTRACT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_GUARANTY_CONTRACT/*')
F_LN_XDXT_GUARANTY_CONTRACT.registerTempTable("F_LN_XDXT_GUARANTY_CONTRACT")
F_TX_WSYH_ECCIF = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIF/*')
F_TX_WSYH_ECCIF.registerTempTable("F_TX_WSYH_ECCIF")
F_TX_WSYH_ECCIFMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIFMCH/*')
F_TX_WSYH_ECCIFMCH.registerTempTable("F_TX_WSYH_ECCIFMCH")


#任务[21] 001-01::
V_STEP = V_STEP + 1
#核心客户合并汇总(暂未使用)
sql = """
 SELECT FK_CICIF_KEY            AS CUST_ID 
   FROM (SELECT DISTINCT FK_CICIF_KEY FROM F_CI_CBOD_CICIEUNN WHERE ODS_ST_DATE=V_DT
UNION SELECT DISTINCT FK_CICIF_KEY FROM F_CI_CBOD_CICIFUNN WHERE ODS_ST_DATE=V_DT ) A                                                   --
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_SYS_RESOURCE_01 = sqlContext.sql(sql)

TMP_OCRM_F_CI_SYS_RESOURCE_01.registerTempTable("TMP_OCRM_F_CI_SYS_RESOURCE_01")
dfn="TMP_OCRM_F_CI_SYS_RESOURCE_01/"+V_DT+".parquet"
TMP_OCRM_F_CI_SYS_RESOURCE_01.cache()
nrows = TMP_OCRM_F_CI_SYS_RESOURCE_01.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_SYS_RESOURCE_01/*.parquet")
TMP_OCRM_F_CI_SYS_RESOURCE_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_SYS_RESOURCE_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_SYS_RESOURCE_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1
#三证有重复的核心客户三证信息-三证
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/*.parquet")
#从当天备表复制一份全量过来
#ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet")
#直接从备份表中读取当天最新数据
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE_BK/'+V_DT+'.parquet')
#OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")


#阳光信贷
sql = """
 SELECT CAST('' AS BIGINT)         AS ID 
       ,B.ODS_CUST_ID           AS ODS_CUST_ID 
       ,COALESCE(B.ODS_CUST_NAME, A.FULLNAME)                       AS ODS_CUST_NAME 
       ,A.CUSTOMERID            AS SOURCE_CUST_ID 
       ,A.FULLNAME              AS SOURCE_CUST_NAME 
       ,A.CERTTYPE              AS CERT_TYPE 
       ,A.CERTID                AS CERT_NO 
       ,'SLNA'                  AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'1'                     AS ODS_CUST_TYPE 
       ,'1'                     AS CUST_STAT 
       ,INPUTDATE                     AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,'100000100000'          AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,''                    AS CERT_FLAG 
   FROM F_CI_SUN_IND_INFO A                                    --阳光信贷农户表
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.FR_ID                 = B.FR_ID 
    AND A.FULLNAME              = B.SOURCE_CUST_NAME 
    AND A.CERTTYPE              = B.CERT_TYPE 
    AND A.CERTID                = B.CERT_NO 
    AND B.ODS_SYS_ID            = 'SLNA' 
    AND B.SOURCE_CUST_ID        = A.CUSTOMERID 
   where A.ODS_ST_DATE           = V_DT 
  GROUP BY ODS_CUST_ID 
       ,A.CUSTOMERID 
       ,ODS_CUST_NAME 
       ,A.FULLNAME 
       ,A.CERTTYPE 
       ,A.CERTID 
       ,END_DATE 
       ,A.FR_ID 
       ,A.INPUTDATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")

#OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
#OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
sql = """
 SELECT DST.ID                  	AS ID                 	  --ID:src.ID                             
       ,DST.ODS_CUST_ID         	AS ODS_CUST_ID        	  --核心客户号:src.ODS_CUST_ID                      
       ,DST.ODS_CUST_NAME       	AS ODS_CUST_NAME      	  --核心客户名称:src.ODS_CUST_NAME                  
       ,DST.SOURCE_CUST_ID      	AS SOURCE_CUST_ID     	  --源系统客户号:src.SOURCE_CUST_ID                 
       ,DST.SOURCE_CUST_NAME    	AS SOURCE_CUST_NAME   	  --院系统客户名称:src.SOURCE_CUST_NAME             
       ,DST.CERT_TYPE           	AS CERT_TYPE          	  --证件类型:src.CERT_TYPE                          
       ,DST.CERT_NO             	AS CERT_NO            	  --证件号码:src.CERT_NO                            
       ,DST.ODS_SYS_ID          	AS ODS_SYS_ID         	  --系统标志:src.ODS_SYS_ID                         
       ,DST.ODS_ST_DATE         	AS ODS_ST_DATE        	  --系统日期:src.ODS_ST_DATE                        
       ,DST.ODS_CUST_TYPE       	AS ODS_CUST_TYPE      	  --客户类型:src.ODS_CUST_TYPE                      
       ,DST.CUST_STAT           	AS CUST_STAT          	  --客户状态，1正式客户，2潜在客户:src.CUST_STAT    
       ,DST.BEGIN_DATE          	AS BEGIN_DATE         	  --开始日期:src.BEGIN_DATE                         
       ,DST.END_DATE            	AS END_DATE           	  --结束日期:src.END_DATE                           
       ,DST.SYS_ID_FLAG         	AS SYS_ID_FLAG        	  --系统标志位:src.SYS_ID_FLAG                      
       ,DST.FR_ID               	AS FR_ID              	  --法人号:src.FR_ID                                
       ,DST.CERT_FLAG           	AS CERT_FLAG          	  --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.SOURCE_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-03::
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
#信贷对公客户
sql = """
 SELECT CAST('' AS BIGINT)            AS ID 
       ,C.ODS_CUST_ID           AS ODS_CUST_ID 
       ,COALESCE(C.ODS_CUST_NAME, A.ENTERPRISENAME)                       AS ODS_CUST_NAME 
       ,A.CUSTOMERID            AS SOURCE_CUST_ID 
       ,A.ENTERPRISENAME        AS SOURCE_CUST_NAME 
       ,B.CERTTYPE              AS CERT_TYPE 
       ,B.CERTID                AS CERT_NO 
       ,'LNA'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'2'                     AS ODS_CUST_TYPE 
       ,'1'                     AS CUST_STAT 
       ,A.INPUTDATE                       AS BEGIN_DATE 
       ,'2999-12-31'            AS END_DATE 
       ,'100100000000'          AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,NULL                    AS CERT_FLAG 
   FROM F_CI_XDXT_ENT_INFO A                                   --企业基本信息
   LEFT JOIN F_CI_XDXT_CUSTOMER_INFO B                         --客户基本信息
     ON A.CUSTOMERID            = B.CUSTOMERID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE C                          --系统来源中间表
     ON A.FR_ID                 = C.FR_ID 
    AND C.ODS_SYS_ID            = 'LNA' 
    AND C.SOURCE_CUST_ID        = A.CUSTOMERID 
  WHERE A.ODS_ST_DATE           = V_DT 
  GROUP BY C.ODS_CUST_ID 
       ,A.CUSTOMERID 
       ,C.ODS_CUST_NAME 
       ,A.ENTERPRISENAME 
       ,B.CERTTYPE 
       ,B.CERTID 
       ,A.FR_ID 
       ,A.INPUTDATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")

sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.ODS_SYS_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-04::
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
#信贷对私客户
sql = """
 SELECT CAST('' AS BIGINT)                    AS ID 
       ,C.ODS_CUST_ID           AS ODS_CUST_ID 
       ,COALESCE(C.ODS_CUST_NAME, A.FULLNAME)                       AS ODS_CUST_NAME 
       ,A.CUSTOMERID            AS SOURCE_CUST_ID 
       ,A.FULLNAME              AS SOURCE_CUST_NAME 
       ,A.CERTTYPE              AS CERT_TYPE 
       ,A.CERTID                AS CERT_NO 
       ,'LNA'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'1'                     AS ODS_CUST_TYPE 
       ,'1'                     AS CUST_STAT 
       ,A.INPUTDATE                      AS BEGIN_DATE 
       ,'2999-12-31'            AS END_DATE 
       ,'100100000000'          AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,''                      AS CERT_FLAG 
   FROM F_CI_XDXT_IND_INFO A                                   --个人基本信息
   LEFT JOIN F_CI_XDXT_CUSTOMER_INFO B                         --客户基本信息
     ON A.CUSTOMERID            = B.CUSTOMERID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE C                          --系统来源中间表
     ON A.FR_ID                 = C.FR_ID 
    AND C.ODS_SYS_ID            = 'LNA' 
    AND C.SOURCE_CUST_ID        = A.CUSTOMERID 
  WHERE A.ODS_ST_DATE           = V_DT 
  GROUP BY C.ODS_CUST_ID 
       ,A.CUSTOMERID 
       ,C.ODS_CUST_NAME 
       ,A.FULLNAME 
       ,A.CERTTYPE 
       ,A.CERTID 
       ,A.FR_ID 
       ,A.INPUTDATE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")

sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.ODS_SYS_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-05::
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
#网银客户+手机银行（个人）
sql = """
 SELECT CAST('' AS BIGINT)                    AS ID 
       ,D.ODS_CUST_ID           AS ODS_CUST_ID 
       ,COALESCE(D.ODS_CUST_NAME, A.CIFNAME)                       AS ODS_CUST_NAME 
       ,A.CIFSEQ                AS SOURCE_CUST_ID 
       ,A.CIFNAME               AS SOURCE_CUST_NAME 
       ,CASE WHEN B.IDTYPE                = 'P00' THEN '0' ELSE B.IDTYPE END                     AS CERT_TYPE 
       ,B.IDNO                  AS CERT_NO 
       ,CASE C.MCHANNELID WHEN 'PIBS' THEN 'IBK' ELSE 'MBK' END                     AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'1'                     AS ODS_CUST_TYPE 
       ,'1'                     AS CUST_STAT 
       ,''            AS BEGIN_DATE 
       ,''              AS END_DATE 
       ,CASE C.MCHANNELID WHEN 'PIBS' THEN '101000000000' ELSE '100000000010' END                     AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,''             AS CERT_FLAG 
   FROM F_TX_WSYH_ECCIF A                                      --电子银行参与方信息表
  INNER JOIN F_CI_WSYH_ECCIFID B                               --客户证件表
     ON A.CIFSEQ                = B.CIFSEQ 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN F_TX_WSYH_ECCIFMCH C                              --客户渠道表
     ON A.CIFSEQ                = C.CIFSEQ 
    AND A.FR_ID                 = C.FR_ID 
    AND C.MCHANNELID IN('PIBS', 'PMBS') 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE D                          --系统来源中间表
     ON A.FR_ID                 = D.FR_ID 
    AND A.CIFNAME               = D.SOURCE_CUST_NAME 
    AND(CASE WHEN B.IDTYPE                = 'P00' THEN '0' ELSE B.IDTYPE END)                       = D.CERT_TYPE 
    AND B.IDNO                  = D.CERT_NO 
    AND D.ODS_SYS_ID            =(CASE C.MCHANNELID WHEN 'PIBS' THEN 'IBK' ELSE 'MBK' END) 
    AND D.SOURCE_CUST_ID        = A.CIFSEQ 
  WHERE A.ODS_ST_DATE           = V_DT 
  GROUP BY D.ODS_CUST_ID,A.CIFSEQ,D.ODS_CUST_NAME,A.CIFNAME,B.IDTYPE,B.IDNO,C.MCHANNELID,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.SOURCE_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-06::
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
#短信平台客户
sql = """
 SELECT CAST('' AS BIGINT)                    AS ID 
       ,CASE WHEN LENGTH(A.CUST_NO) > 12 THEN B.ODS_CUST_ID ELSE A.CUST_NO END                     AS ODS_CUST_ID 
       ,COALESCE(B.ODS_CUST_NAME, A.CUST_NAME)                       AS ODS_CUST_NAME 
       ,A.CUST_ID               AS SOURCE_CUST_ID 
       ,A.CUST_NAME             AS SOURCE_CUST_NAME 
       ,A.ID_TYPE               AS CERT_TYPE 
       ,A.ID_NO                 AS CERT_NO 
       ,'MSG'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.CUST_TYPE             AS ODS_CUST_TYPE 
       ,'1'                     AS CUST_STAT 
       ,COALESCE(B.BEGIN_DATE, A.CUST_DATE)                       AS BEGIN_DATE 
       ,COALESCE(B.END_DATE, A.END_DATE)                       AS END_DATE 
       ,'100000001000'          AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,''                    AS CERT_FLAG 
   FROM F_CI_DXPT_CUSTOMER A                                   --客户信息表
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_NAME             = B.SOURCE_CUST_NAME 
    AND A.ID_TYPE               = B.CERT_TYPE 
    AND A.ID_NO                 = B.CERT_NO 
    AND B.ODS_SYS_ID            = 'MSG' 
    AND B.SOURCE_CUST_ID        = A.CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.SOURCE_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[11] 001-07::
V_STEP = V_STEP + 1
#信贷关联关系(潜在客户)
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
sql = """
 SELECT CAST('' AS BIGINT)                    AS ID 
       ,''                    AS ODS_CUST_ID 
       ,A.CUSTOMERNAME          AS ODS_CUST_NAME 
       ,A.RELATIVEID            AS SOURCE_CUST_ID 
       ,A.CUSTOMERNAME          AS SOURCE_CUST_NAME 
       ,A.CERTTYPE              AS CERT_TYPE 
       ,A.CERTID                AS CERT_NO 
       ,''                    AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,CASE WHEN LENGTH(A.CERTID) >= 15 THEN '1' ELSE '2' END                     AS ODS_CUST_TYPE 
       ,'2'                     AS CUST_STAT 
       ,''                    AS BEGIN_DATE 
       ,''                    AS END_DATE 
       ,''                    AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,''                    AS CERT_FLAG 
   FROM F_CI_XDXT_CUSTOMER_RELATIVE A                          --客户关联信息
  WHERE A.ODS_ST_DATE           = V_DT 
    AND NOT EXISTS(SELECT 1 FROM OCRM_F_CI_SYS_RESOURCE B
	                WHERE A.CERTTYPE              = B.CERT_TYPE 
                      AND A.CERTID                = B.CERT_NO 
                      AND A.CUSTOMERNAME          = B.ODS_CUST_NAME 
                      AND A.FR_ID                 = B.FR_ID)
  GROUP BY A.CUSTOMERNAME 
       ,A.RELATIVEID 
       ,A.CERTTYPE 
       ,A.CERTID 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE.cache()
nrows = OCRM_F_CI_SYS_RESOURCE.count()
OCRM_F_CI_SYS_RESOURCE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_SYS_RESOURCE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-8::
V_STEP = V_STEP + 1
#信贷担保信息(潜在客户)
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

sql = """
 SELECT DISTINCT CAST('' AS BIGINT)                    AS ID 
       ,''                    AS ODS_CUST_ID 
       ,A.GUARANTORNAME         AS ODS_CUST_NAME 
       ,A.GUARANTORID           AS SOURCE_CUST_ID 
       ,A.GUARANTORNAME         AS SOURCE_CUST_NAME 
       ,A.CERTTYPE              AS CERT_TYPE 
       ,A.CERTID                AS CERT_NO 
       ,''                    AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,CASE WHEN LENGTH(A.CERTID) >= 15 THEN '1' ELSE '2' END                     AS ODS_CUST_TYPE 
       ,'2'                     AS CUST_STAT 
       ,''                    AS BEGIN_DATE 
       ,''                    AS END_DATE 
       ,''                    AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,''                    AS CERT_FLAG 
   FROM F_LN_XDXT_GUARANTY_CONTRACT A                          --担保合同信息
  WHERE A.ODS_ST_DATE           = V_DT
    AND NOT EXISTS(SELECT 1 FROM OCRM_F_CI_SYS_RESOURCE B
	                WHERE A.CERTTYPE              = B.CERT_TYPE 
                      AND A.CERTID                = B.CERT_NO 
                      AND A.GUARANTORNAME         = B.ODS_CUST_NAME 
                      AND A.FR_ID                 = B.FR_ID)
       """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE.cache()
nrows = OCRM_F_CI_SYS_RESOURCE.count()
OCRM_F_CI_SYS_RESOURCE.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_SYS_RESOURCE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


#任务[21] 001-09::
V_STEP = V_STEP + 1
#统计待删除客户信息(三证有重复的核心客户三证信息-三证信息)
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
sql = """
 SELECT FR_ID                   AS FR_ID 
       ,CERT_NO                 AS CERT_NO 
       ,CERT_TYPE               AS CERT_TYPE 
       ,SOURCE_CUST_NAME        AS SOURCE_CUST_NAME 
       ,'CEN'                   AS ODS_SYS_ID 
       ,''                      AS ODS_CUST_ID
   FROM (SELECT FR_ID,CERT_NO,CERT_TYPE,SOURCE_CUST_NAME FROM (
			SELECT DISTINCT FR_ID,ODS_CUST_ID,CERT_NO,CERT_TYPE,SOURCE_CUST_NAME 
			FROM OCRM_F_CI_SYS_RESOURCE WHERE ODS_SYS_ID = 'CEN') A
	GROUP BY FR_ID,CERT_NO,CERT_TYPE,SOURCE_CUST_NAME HAVING COUNT(1) > 1) A                                                   --系统来源中间表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_SYS_RESOURCE_03 = sqlContext.sql(sql)
TMP_OCRM_F_CI_SYS_RESOURCE_03.registerTempTable("TMP_OCRM_F_CI_SYS_RESOURCE_03")
dfn="TMP_OCRM_F_CI_SYS_RESOURCE_03/"+V_DT+".parquet"
TMP_OCRM_F_CI_SYS_RESOURCE_03.cache()
nrows = TMP_OCRM_F_CI_SYS_RESOURCE_03.count()
TMP_OCRM_F_CI_SYS_RESOURCE_03.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_SYS_RESOURCE_03.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_SYS_RESOURCE_03/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_SYS_RESOURCE_03 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-10::
V_STEP = V_STEP + 1
#统计待删除客户信息(核心三证一样客户号不一样的客户-客户号)
sql = """
 SELECT FR_ID                   AS FR_ID 
       ,''                 AS CERT_NO 
       ,''               AS CERT_TYPE 
       ,''        AS SOURCE_CUST_NAME 
       ,'CEN'                   AS ODS_SYS_ID 
       ,ODS_CUST_ID             AS ODS_CUST_ID 
   FROM (SELECT FR_ID,ODS_CUST_ID FROM (
			SELECT DISTINCT FR_ID,ODS_CUST_ID,CERT_NO,CERT_TYPE,SOURCE_CUST_NAME,CERT_FLAG 
				FROM OCRM_F_CI_SYS_RESOURCE WHERE ODS_SYS_ID = 'CEN'  AND CERT_FLAG='Y') A 
		GROUP BY FR_ID,ODS_CUST_ID HAVING COUNT(1) > 1) A              --系统来源中间表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_SYS_RESOURCE_03 = sqlContext.sql(sql)
TMP_OCRM_F_CI_SYS_RESOURCE_03.registerTempTable("TMP_OCRM_F_CI_SYS_RESOURCE_03")
dfn="TMP_OCRM_F_CI_SYS_RESOURCE_03/"+V_DT+".parquet"
TMP_OCRM_F_CI_SYS_RESOURCE_03.cache()
nrows = TMP_OCRM_F_CI_SYS_RESOURCE_03.count()
TMP_OCRM_F_CI_SYS_RESOURCE_03.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_OCRM_F_CI_SYS_RESOURCE_03.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_SYS_RESOURCE_03/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_SYS_RESOURCE_03 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


#任务[21] 001-11::
V_STEP = V_STEP + 1
#去除03临时表中的重复客户
TMP_OCRM_F_CI_SYS_RESOURCE_03 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_SYS_RESOURCE_03/*')
TMP_OCRM_F_CI_SYS_RESOURCE_03.registerTempTable("TMP_OCRM_F_CI_SYS_RESOURCE_03")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
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
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
   LEFT JOIN TMP_OCRM_F_CI_SYS_RESOURCE_03 B                   --系统来源中间表临时表03(去除核心重复数据)
     ON A.FR_ID                 = B.FR_ID 
    AND A.ODS_SYS_ID            = 'CEN' 
    AND(A.ODS_CUST_ID           = B.ODS_CUST_ID 
             OR(A.CERT_NO               = B.CERT_NO 
                    AND A.CERT_TYPE             = B.CERT_TYPE 
                    AND A.SOURCE_CUST_NAME      = B.SOURCE_CUST_NAME)) 
  WHERE B.FR_ID IS NULL """


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


#任务[12] 001-12::
V_STEP = V_STEP + 1
#用核心客户号更新新增无核心客户号、原有潜在客户号的数据
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

sql = """
 SELECT B.ID                    AS ID 
       ,A.ODS_CUST_ID           AS ODS_CUST_ID 
       ,A.ODS_CUST_NAME         AS ODS_CUST_NAME 
       ,B.SOURCE_CUST_ID        AS SOURCE_CUST_ID 
       ,B.SOURCE_CUST_NAME      AS SOURCE_CUST_NAME 
       ,A.CERT_TYPE             AS CERT_TYPE 
       ,A.CERT_NO               AS CERT_NO 
       ,B.ODS_SYS_ID            AS ODS_SYS_ID 
       ,B.ODS_ST_DATE           AS ODS_ST_DATE 
       ,B.ODS_CUST_TYPE         AS ODS_CUST_TYPE 
       ,'1'                     AS CUST_STAT 
       ,B.BEGIN_DATE            AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,B.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,B.CERT_FLAG             AS CERT_FLAG 
   FROM (SELECT DISTINCT FR_ID,ODS_CUST_ID,ODS_CUST_NAME,CERT_TYPE,CERT_NO FROM OCRM_F_CI_SYS_RESOURCE WHERE ODS_SYS_ID = 'CEN') A                                                   --系统来源中间表-核心客户
  INNER JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.FR_ID                 = B.FR_ID 
    AND A.ODS_CUST_NAME         = B.ODS_CUST_NAME 
    AND A.CERT_TYPE             = B.CERT_TYPE 
    AND A.CERT_NO               = B.CERT_NO """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")

OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.ODS_CUST_NAME       = DST.ODS_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.ODS_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[21] 001-13::
V_STEP = V_STEP + 1
#待删除客户(取三证一致客户号不一致的客户)
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
sql = """
 SELECT ODS_CUST_NAME           AS ODS_CUST_NAME 
       ,CERT_TYPE               AS CERT_TYPE 
       ,CERT_NO                 AS CERT_NO 
       ,FR_ID                   AS FR_ID 
   FROM (SELECT FR_ID,ODS_CUST_NAME,CERT_TYPE,CERT_NO 
           FROM (SELECT DISTINCT FR_ID,ODS_CUST_ID,ODS_CUST_NAME,CERT_TYPE,CERT_NO 
                   FROM OCRM_F_CI_SYS_RESOURCE WHERE ODS_CUST_ID IS NOT NULL AND LENGTH(TRIM(ODS_CUST_ID)) > 0) 
                  GROUP BY FR_ID,ODS_CUST_NAME,CERT_TYPE,CERT_NO HAVING COUNT(1)>1) A    --系统来源中间表
  GROUP BY A.ODS_CUST_NAME 
       ,A.CERT_TYPE 
       ,A.CERT_NO 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_SYS_RESOURCE_04 = sqlContext.sql(sql)
TMP_OCRM_F_CI_SYS_RESOURCE_04.registerTempTable("TMP_OCRM_F_CI_SYS_RESOURCE_04")
dfn="TMP_OCRM_F_CI_SYS_RESOURCE_04/"+V_DT+".parquet"
TMP_OCRM_F_CI_SYS_RESOURCE_04.cache()
nrows = TMP_OCRM_F_CI_SYS_RESOURCE_04.count()
TMP_OCRM_F_CI_SYS_RESOURCE_04.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_SYS_RESOURCE_04.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_SYS_RESOURCE_04/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_SYS_RESOURCE_04 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-14::
V_STEP = V_STEP + 1
#去除04临时表中(三证一致客户号不一致的客户)
TMP_OCRM_F_CI_SYS_RESOURCE_04 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_SYS_RESOURCE_04/*')
TMP_OCRM_F_CI_SYS_RESOURCE_04.registerTempTable("TMP_OCRM_F_CI_SYS_RESOURCE_04")

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
       ,CASE WHEN LENGTH(A.BEGIN_DATE) = 10 AND SUBSTR(A.BEGIN_DATE, 3, 1)='/'
               THEN CONCAT(SUBSTR(BEGIN_DATE, 7, 4),'-',SUBSTR(BEGIN_DATE, 1, 2),'-',SUBSTR(BEGIN_DATE, 4, 2))
             WHEN LENGTH(A.BEGIN_DATE) <> 10 OR A.BEGIN_DATE LIKE '%/%'
               THEN ''
             ELSE A.BEGIN_DATE 
         END AS BEGIN_DATE 
       ,A.END_DATE              AS END_DATE 
       ,A.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,A.CERT_FLAG             AS CERT_FLAG 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
   LEFT JOIN TMP_OCRM_F_CI_SYS_RESOURCE_04 B                   --系统来源中间表临时表04(去除三证一致客户号不一致的)
     ON A.FR_ID                 = B.FR_ID 
    AND A.ODS_CUST_NAME         = B.ODS_CUST_NAME 
    AND A.CERT_TYPE             = B.CERT_TYPE 
    AND A.CERT_NO               = B.CERT_NO 
  WHERE B.FR_ID IS NULL """


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

#任务[12] 001-15::
V_STEP = V_STEP + 1
#通过三证更新没有核心客户号的客户
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

sql = """
 SELECT B.ID                 AS ID               
       ,A.ODS_CUST_ID        AS ODS_CUST_ID      
       ,B.ODS_CUST_NAME      AS ODS_CUST_NAME    
       ,B.SOURCE_CUST_ID     AS SOURCE_CUST_ID   
       ,B.SOURCE_CUST_NAME   AS SOURCE_CUST_NAME 
       ,B.CERT_TYPE          AS CERT_TYPE        
       ,B.CERT_NO            AS CERT_NO          
       ,B.ODS_SYS_ID         AS ODS_SYS_ID       
       ,B.ODS_ST_DATE        AS ODS_ST_DATE      
       ,B.ODS_CUST_TYPE      AS ODS_CUST_TYPE    
       ,B.CUST_STAT          AS CUST_STAT        
       ,B.BEGIN_DATE         AS BEGIN_DATE       
       ,B.END_DATE           AS END_DATE         
       ,B.SYS_ID_FLAG        AS SYS_ID_FLAG      
       ,B.FR_ID              AS FR_ID            
       ,B.CERT_FLAG          AS CERT_FLAG        
   FROM (SELECT DISTINCT ODS_CUST_ID,ODS_CUST_NAME,CERT_TYPE,CERT_NO,FR_ID
           FROM OCRM_F_CI_SYS_RESOURCE 
          WHERE ODS_CUST_ID IS NOT NULL 
           AND LENGTH(TRIM(ODS_CUST_ID)) > 0) A 
   INNER JOIN OCRM_F_CI_SYS_RESOURCE B ON A.FR_ID=B.FR_ID AND A.ODS_CUST_NAME=B.ODS_CUST_NAME 
                                      AND A.CERT_TYPE=B.CERT_TYPE AND A.CERT_NO=B.CERT_NO
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")

OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.ODS_CUST_NAME       = DST.ODS_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID
  WHERE SRC.ODS_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[21] 001-16::
V_STEP = V_STEP + 1
#通过三证生成CRM客户号(1111开头的客户号)
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

#计算1111开头的最大客户号，实际数据中可能111开头
MAX_ODS_CUST_ID = OCRM_F_CI_SYS_RESOURCE.where('ODS_CUST_ID like "111%"').agg({"ODS_CUST_ID": "max"}).collect()[0][0]
if MAX_ODS_CUST_ID == None:
	MAX_ODS_CUST_ID='111100000000000'
sql = """
 SELECT CAST("""+str(MAX_ODS_CUST_ID)+"""+monotonically_increasing_id()+1 AS VARCHAR(20))     AS ODS_CUST_ID 
       ,ODS_CUST_NAME           AS ODS_CUST_NAME 
       ,CERT_NO                 AS CERT_NO 
       ,CERT_TYPE               AS CERT_TYPE 
       ,FR_ID                   AS FR_ID 
   FROM (SELECT FR_ID,CERT_TYPE,CERT_NO,ODS_CUST_NAME FROM OCRM_F_CI_SYS_RESOURCE 
          WHERE ODS_CUST_ID IS NULL OR ODS_CUST_ID = ''
          GROUP BY FR_ID,CERT_NO,CERT_TYPE,ODS_CUST_NAME) A --中间表(待生成客户号表)
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
print("-----------------sql---------------")
print(sql)
print("--------------------------------")
TMP_OCRM_F_CI_SYS_RESOURCE_05 = sqlContext.sql(sql)
TMP_OCRM_F_CI_SYS_RESOURCE_05.registerTempTable("TMP_OCRM_F_CI_SYS_RESOURCE_05")
dfn="TMP_OCRM_F_CI_SYS_RESOURCE_05/"+V_DT+".parquet"
TMP_OCRM_F_CI_SYS_RESOURCE_05.cache()
nrows = TMP_OCRM_F_CI_SYS_RESOURCE_05.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_SYS_RESOURCE_05/*.parquet")
TMP_OCRM_F_CI_SYS_RESOURCE_05.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_SYS_RESOURCE_05.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_SYS_RESOURCE_05 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-17::
V_STEP = V_STEP + 1
#更新生成的客户号
TMP_OCRM_F_CI_SYS_RESOURCE_05 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_SYS_RESOURCE_05/*')
TMP_OCRM_F_CI_SYS_RESOURCE_05.registerTempTable("TMP_OCRM_F_CI_SYS_RESOURCE_05")

sql = """
 SELECT B.ID                    AS ID 
       ,A.ODS_CUST_ID           AS ODS_CUST_ID 
       ,A.ODS_CUST_NAME         AS ODS_CUST_NAME 
       ,B.SOURCE_CUST_ID        AS SOURCE_CUST_ID 
       ,B.SOURCE_CUST_NAME      AS SOURCE_CUST_NAME 
       ,A.CERT_TYPE             AS CERT_TYPE 
       ,A.CERT_NO               AS CERT_NO 
       ,B.ODS_SYS_ID            AS ODS_SYS_ID 
       ,B.ODS_ST_DATE           AS ODS_ST_DATE 
       ,B.ODS_CUST_TYPE         AS ODS_CUST_TYPE 
       ,B.CUST_STAT             AS CUST_STAT 
       ,B.BEGIN_DATE            AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,B.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,B.CERT_FLAG             AS CERT_FLAG 
   FROM TMP_OCRM_F_CI_SYS_RESOURCE_05 A                        --系统来源中间表临时表05(生成客户号)
  INNER JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.FR_ID                 = B.FR_ID 
    AND A.ODS_CUST_NAME         = B.ODS_CUST_NAME 
    AND A.CERT_TYPE             = B.CERT_TYPE 
    AND A.CERT_NO               = B.CERT_NO 
    AND (B.ODS_CUST_ID IS NULL OR B.ODS_CUST_ID = '') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")

#dfn="OCRM_F_CI_SYS_RESOURCE_INNTMP1/"+V_DT+".parquet"
#OCRM_F_CI_SYS_RESOURCE_INNTMP1.write.save(path=hdfs + '/' + dfn, mode='overwrite')


OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.ODS_CUST_NAME       = DST.ODS_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.ODS_CUST_NAME IS NULL 
	AND (DST.ODS_CUST_ID IS NOT NULL AND DST.ODS_CUST_ID !='')
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
#dfn="OCRM_F_CI_SYS_RESOURCE_INNTMP2/"+V_DT+".parquet"
#OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path=hdfs + '/' + dfn, mode='overwrite')

dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/"+V_DT+".parquet")
