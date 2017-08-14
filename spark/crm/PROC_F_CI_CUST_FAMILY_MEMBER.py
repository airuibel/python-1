#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUST_FAMILY_MEMBER').setMaster(sys.argv[2])
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

ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_FAMILY_MEMBER/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_CUST_FAMILY_MEMBER_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_FAMILY_MEMBER/"+V_DT+".parquet")


#----------来源表---------------
F_CI_XDXT_CUSTOMER_RELATIVE = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_RELATIVE/*')
F_CI_XDXT_CUSTOMER_RELATIVE.registerTempTable("F_CI_XDXT_CUSTOMER_RELATIVE")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_CI_XDXT_IND_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_INFO/*')
F_CI_XDXT_IND_INFO.registerTempTable("F_CI_XDXT_IND_INFO")
OCRM_F_CI_CUST_FAMILY_MEMBER = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_FAMILY_MEMBER/*')
OCRM_F_CI_CUST_FAMILY_MEMBER.registerTempTable("OCRM_F_CI_CUST_FAMILY_MEMBER")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT monotonically_increasing_id() AS ID 
       ,A.ODS_CUST_ID           AS CUST_ID 
       ,''                    AS MEMBER_CUST_ID 
       ,B.CUSTOMERNAME          AS MEMBER_NAME 
       ,C.SEX                   AS MEMBER_SEX 
       ,B.CERTTYPE              AS MEMBER_CERT_TYP 
       ,B.CERTID                AS MEMBER_CERT_NO 
       ,B.TELEPHONE             AS MENBER_LINK_PHONE 
       ,B.RELATIONSHIP          AS MEMBER_RELATIVE 
       ,'2'                     AS MEMBER_CUST_STAT 
       ,V_DT                  AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
  INNER JOIN F_CI_XDXT_CUSTOMER_RELATIVE B                     --客户关联信息
     ON A.SOURCE_CUST_ID        = B.CUSTOMERID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.RELATIONSHIP IN('0301', '0302', '0303', '0304', '0305') 
   LEFT JOIN F_CI_XDXT_IND_INFO C                              --个人基本信息
     ON B.RELATIVEID            = C.CUSTOMERID 
    AND C.FR_ID                 = A.FR_ID 
   LEFT JOIN OCRM_F_CI_CUST_FAMILY_MEMBER D                    --客户家庭成员信息表
     ON A.ODS_CUST_ID           = D.CUST_ID 
    AND B.CERTTYPE              = D.MEMBER_CERT_TYP 
    AND B.CERTID                = D.MEMBER_CERT_NO 
    AND B.CUSTOMERNAME          = D.MEMBER_NAME 
    AND D.FR_ID                 = A.FR_ID 
  WHERE A.ODS_SYS_ID            = 'LNA' 
    AND D.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_FAMILY_MEMBER = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_FAMILY_MEMBER/"+V_DT+".parquet"
OCRM_F_CI_CUST_FAMILY_MEMBER.cache()
nrows = OCRM_F_CI_CUST_FAMILY_MEMBER.count()
OCRM_F_CI_CUST_FAMILY_MEMBER.write.save(path=hdfs + '/' + dfn, mode='append')
F_CI_XDXT_CUSTOMER_RELATIVE.unpersist()
F_CI_XDXT_IND_INFO.unpersist()
OCRM_F_CI_CUST_FAMILY_MEMBER.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_FAMILY_MEMBER lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_FAMILY_MEMBER = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_FAMILY_MEMBER/*')
OCRM_F_CI_CUST_FAMILY_MEMBER.registerTempTable("OCRM_F_CI_CUST_FAMILY_MEMBER")
sql = """
 SELECT CAST(A.ID    AS BIGINT)                AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,B.ODS_CUST_ID           AS MEMBER_CUST_ID 
       ,A.MEMBER_NAME           AS MEMBER_NAME 
       ,A.MEMBER_SEX            AS MEMBER_SEX 
       ,A.MEMBER_CERT_TYP       AS MEMBER_CERT_TYP 
       ,A.MEMBER_CERT_NO        AS MEMBER_CERT_NO 
       ,A.MENBER_LINK_PHONE     AS MENBER_LINK_PHONE 
       ,A.MEMBER_RELATIVE       AS MEMBER_RELATIVE 
       ,B.CUST_STAT             AS MEMBER_CUST_STAT 
       ,A.ETL_DATE              AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_CUST_FAMILY_MEMBER A                         --
  INNER JOIN (SELECT DISTINCT ODS_CUST_ID, CUST_STAT,ODS_CUST_NAME,CERT_TYPE,CERT_NO,FR_ID
          FROM OCRM_F_CI_SYS_RESOURCE WHERE ODS_SYS_ID = 'LNA'  ) B                          --
     ON A.MEMBER_NAME           = B.ODS_CUST_NAME 
    AND A.MEMBER_CERT_TYP      = B.CERT_TYPE 
    AND A.MEMBER_CERT_NO        = B.CERT_NO 
    AND A.FR_ID                 = B.FR_ID 
     """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP1.registerTempTable("OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP1")


sql = """
 SELECT  DST.ID                                                  --:src.ID
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.MEMBER_CUST_ID                                     --家庭成员客户号:src.MEMBER_CUST_ID
       ,DST.MEMBER_NAME                                        --成员名称:src.MEMBER_NAME
       ,DST.MEMBER_SEX                                         --成员性别:src.MEMBER_SEX
       ,DST.MEMBER_CERT_TYP                                    --成员证件类型:src.MEMBER_CERT_TYP
       ,DST.MEMBER_CERT_NO                                     --成员证件号码:src.MEMBER_CERT_NO
       ,DST.MENBER_LINK_PHONE                                  --成员联系方式:src.MENBER_LINK_PHONE
       ,DST.MEMBER_RELATIVE                                    --与当前客户关系:src.MEMBER_RELATIVE
       ,DST.MEMBER_CUST_STAT                                   --成员客户状态:src.MEMBER_CUST_STAT
       ,DST.ETL_DATE                                           --跑批日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_CUST_FAMILY_MEMBER DST 
   LEFT JOIN OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP1 SRC 
     ON SRC.MEMBER_NAME         = DST.MEMBER_NAME 
    AND SRC.MEMBER_CERT_TYP     = DST.MEMBER_CERT_TYP 
    AND SRC.MEMBER_CERT_NO      = DST.MEMBER_CERT_NO 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.MEMBER_NAME IS NULL 
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_FAMILY_MEMBER/"+V_DT+".parquet"
UNION=OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP2.unionAll(OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP1)
OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP1.cache()
OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP1.unpersist()
OCRM_F_CI_CUST_FAMILY_MEMBER_INNTMP2.unpersist()
OCRM_F_CI_SYS_RESOURCE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_FAMILY_MEMBER lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_FAMILY_MEMBER_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_CUST_FAMILY_MEMBER/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_CUST_FAMILY_MEMBER_BK/")