#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_PER_CUST_INFO').setMaster(sys.argv[2])
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
F_CI_CBOD_ECCSIATT = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCSIATT/*')
F_CI_CBOD_ECCSIATT.registerTempTable("F_CI_CBOD_ECCSIATT")
F_CI_WSYH_ECUSR = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECUSR/*')
F_CI_WSYH_ECUSR.registerTempTable("F_CI_WSYH_ECUSR")
F_CI_XDXT_CUSTOMER_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_INFO/*')
F_CI_XDXT_CUSTOMER_INFO.registerTempTable("F_CI_XDXT_CUSTOMER_INFO")
F_CI_WSYH_ECCIFID = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFID/*')
F_CI_WSYH_ECCIFID.registerTempTable("F_CI_WSYH_ECCIFID")
F_CI_WSYH_ECCIFTEL = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFTEL/*')
F_CI_WSYH_ECCIFTEL.registerTempTable("F_CI_WSYH_ECCIFTEL")
F_CI_CBOD_ECCIFICS = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCIFICS/*')
F_CI_CBOD_ECCIFICS.registerTempTable("F_CI_CBOD_ECCIFICS")
F_CI_XDXT_IND_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_INFO/*')
F_CI_XDXT_IND_INFO.registerTempTable("F_CI_XDXT_IND_INFO")
F_CI_CBOD_CICIFCIF = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIFCIF/*')
F_CI_CBOD_CICIFCIF.registerTempTable("F_CI_CBOD_CICIFCIF")
F_CI_XDXT_IND_ENT_ADDITIONAL_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_ENT_ADDITIONAL_INFO/*')
F_CI_XDXT_IND_ENT_ADDITIONAL_INFO.registerTempTable("F_CI_XDXT_IND_ENT_ADDITIONAL_INFO")
F_CI_XDXT_CUSTOMER_SPECIAL = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_SPECIAL/*')
F_CI_XDXT_CUSTOMER_SPECIAL.registerTempTable("F_CI_XDXT_CUSTOMER_SPECIAL")
F_CI_CBOD_CICIFADR = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIFADR/*')
F_CI_CBOD_CICIFADR.registerTempTable("F_CI_CBOD_CICIFADR")
F_CI_XDXT_IND_EDUCATION = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_EDUCATION/*')
F_CI_XDXT_IND_EDUCATION.registerTempTable("F_CI_XDXT_IND_EDUCATION")
F_CI_WSYH_ECCIFADDR = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFADDR/*')
F_CI_WSYH_ECCIFADDR.registerTempTable("F_CI_WSYH_ECCIFADDR")
F_CM_CBOD_CMEMPEMP = sqlContext.read.parquet(hdfs+'/F_CM_CBOD_CMEMPEMP/*')
F_CM_CBOD_CMEMPEMP.registerTempTable("F_CM_CBOD_CMEMPEMP")
F_CI_SUN_IND_INFO_TMP = sqlContext.read.parquet(hdfs+'/F_CI_SUN_IND_INFO_TMP/*')
F_CI_SUN_IND_INFO_TMP.registerTempTable("F_CI_SUN_IND_INFO_TMP")
F_TX_WSYH_ECCIFMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIFMCH/*')
F_TX_WSYH_ECCIFMCH.registerTempTable("F_TX_WSYH_ECCIFMCH")
F_TX_WSYH_ECCIF = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIF/*')
F_TX_WSYH_ECCIF.registerTempTable("F_TX_WSYH_ECCIF")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.EC_CUST_NO            AS EC_CUST_NO 
       ,A.EC_ATT_TYP            AS EC_ATT_TYP 
       ,A.EC_ATT_FMDAT_N        AS EC_ATT_FMDAT_N 
       ,A.EC_ATT_DESC           AS EC_ATT_DESC 
       ,A.EC_BNK_NO             AS FR_ID 
   FROM F_CI_CBOD_ECCSIATT A                                   --关注客户清单档
  INNER JOIN(
         SELECT FR_ID 
               ,EC_CUST_NO 
               ,MAX(EC_SEQ_NO)                       AS EC_SEQ_NO 
           FROM F_CI_CBOD_ECCSIATT 
          GROUP BY EC_CUST_NO 
               ,FR_ID) B                                       --关注客户清单档
     ON A.EC_CUST_NO            = B.EC_CUST_NO 
    AND A.EC_SEQ_NO             = B.EC_SEQ_NO 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_PER_CUST_INFO_01 = sqlContext.sql(sql)
TMP_OCRM_F_CI_PER_CUST_INFO_01.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_01")
dfn="TMP_OCRM_F_CI_PER_CUST_INFO_01/"+V_DT+".parquet"
TMP_OCRM_F_CI_PER_CUST_INFO_01.cache()
nrows = TMP_OCRM_F_CI_PER_CUST_INFO_01.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_PER_CUST_INFO_01/*.parquet")
TMP_OCRM_F_CI_PER_CUST_INFO_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_PER_CUST_INFO_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_PER_CUST_INFO_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

TMP_OCRM_F_CI_PER_CUST_INFO_01 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_PER_CUST_INFO_01/*')
TMP_OCRM_F_CI_PER_CUST_INFO_01.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_01")

sql = """
 SELECT FR_ID                   AS FR_ID 
       ,ODS_CUST_ID             AS ODS_CUST_ID 
       ,MAX(SOURCE_CUST_ID)                       AS SOURCE_CUST_ID 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
  WHERE ODS_SYS_ID              = 'LNA' 
  GROUP BY FR_ID 
       ,ODS_CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_PER_CUST_INFO_02 = sqlContext.sql(sql)
TMP_OCRM_F_CI_PER_CUST_INFO_02.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_02")
dfn="TMP_OCRM_F_CI_PER_CUST_INFO_02/"+V_DT+".parquet"
TMP_OCRM_F_CI_PER_CUST_INFO_02.cache()
nrows = TMP_OCRM_F_CI_PER_CUST_INFO_02.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_PER_CUST_INFO_02/*.parquet")
TMP_OCRM_F_CI_PER_CUST_INFO_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_PER_CUST_INFO_02.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_PER_CUST_INFO_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

TMP_OCRM_F_CI_PER_CUST_INFO_02 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_PER_CUST_INFO_02/*')
TMP_OCRM_F_CI_PER_CUST_INFO_02.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_02")

sql = """
 SELECT C.CUSTOMERID              AS CUSTOMERID 
       ,C.FR_ID                   AS FR_ID 
       ,INPUTDATE               AS INPUTDATE 
       ,INLISTREASON            AS INLISTREASON 
   FROM F_CI_XDXT_CUSTOMER_SPECIAL C                           --特定客户名单表
  INNER JOIN(
         SELECT FR_ID 
               ,MAX(SERIALNO) SERIALNO 
               ,CUSTOMERID 
           FROM F_CI_XDXT_CUSTOMER_SPECIAL 
          WHERE BLACKSHEETORNOT         = '1' 
          GROUP BY CUSTOMERID 
               ,FR_ID) D                                       --特定客户名单表
     ON C.CUSTOMERID            = D.CUSTOMERID 
    AND C.SERIALNO              = D.SERIALNO 
    AND C.FR_ID                 = D.FR_ID 
    AND C.BLACKSHEETORNOT       = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_PER_CUST_INFO_03 = sqlContext.sql(sql)
TMP_OCRM_F_CI_PER_CUST_INFO_03.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_03")
dfn="TMP_OCRM_F_CI_PER_CUST_INFO_03/"+V_DT+".parquet"
TMP_OCRM_F_CI_PER_CUST_INFO_03.cache()
nrows = TMP_OCRM_F_CI_PER_CUST_INFO_03.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_PER_CUST_INFO_03/*.parquet")
TMP_OCRM_F_CI_PER_CUST_INFO_03.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_PER_CUST_INFO_03.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_PER_CUST_INFO_03 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::
V_STEP = V_STEP + 1

TMP_OCRM_F_CI_PER_CUST_INFO_03 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_PER_CUST_INFO_03/*')
TMP_OCRM_F_CI_PER_CUST_INFO_03.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_03")

sql = """
 SELECT A.FR_ID                   AS FR_ID 
       ,A.SERIALNO                AS SERIALNO 
       ,ENDDATE                 AS ENDDATE 
       ,A.CUSTOMERID              AS CUSTOMERID 
       ,SCHOOL                  AS SCHOOL 
   FROM F_CI_XDXT_IND_EDUCATION A                              --个人学历履历
  INNER JOIN(
         SELECT MAX(SERIALNO) SERIALNO 
               ,CUSTOMERID 
               ,FR_ID 
           FROM F_CI_XDXT_IND_EDUCATION 
          GROUP BY FR_ID 
               ,CUSTOMERID) B                                  --个人学历履历
     ON A.CUSTOMERID            = B.CUSTOMERID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.SERIALNO              = B.SERIALNO """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_PER_CUST_INFO_04 = sqlContext.sql(sql)
TMP_OCRM_F_CI_PER_CUST_INFO_04.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_04")
dfn="TMP_OCRM_F_CI_PER_CUST_INFO_04/"+V_DT+".parquet"
TMP_OCRM_F_CI_PER_CUST_INFO_04.cache()
nrows = TMP_OCRM_F_CI_PER_CUST_INFO_04.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_PER_CUST_INFO_04/*.parquet")
TMP_OCRM_F_CI_PER_CUST_INFO_04.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_PER_CUST_INFO_04.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_PER_CUST_INFO_04 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-05::
V_STEP = V_STEP + 1

TMP_OCRM_F_CI_PER_CUST_INFO_04 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_PER_CUST_INFO_04/*')
TMP_OCRM_F_CI_PER_CUST_INFO_04.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_04")

sql = """
 SELECT E.ODS_CUST_ID           AS ODS_CUST_ID 
       ,F.FAMILYADD             AS FAMILYADD 
       ,F.VILLAGENO             AS VILLAGENO 
       ,F.FARMILYID             AS FARMILYID 
       ,F.CERTTYPE              AS CERTTYPE 
       ,F.CERTID                AS CERTID 
       ,F.FULLNAME              AS FULLNAME 
       ,F.HOUSEAREANO           AS HOUSEAREANO 
       ,F.NATIVEPLACE           AS NATIVEPLACE 
       ,F.REMARK                AS REMARK 
       ,F.CORPORATEORGID                 AS FR_ID 
       ,ROW_NUMBER() OVER( PARTITION BY E.ODS_CUST_ID ORDER BY E.ODS_CUST_ID)                       AS RN 
   FROM F_CI_SUN_IND_INFO_TMP F                                --阳光信贷农户表
  INNER JOIN OCRM_F_CI_SYS_RESOURCE E                          --
     ON E.SOURCE_CUST_ID        = F.CUSTOMERID 
    AND E.ODS_SYS_ID            = 'SLNA' 
    AND F.CORPORATEORGID                 = E.FR_ID 
  WHERE F.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_PER_CUST_INFO_05 = sqlContext.sql(sql)
TMP_OCRM_F_CI_PER_CUST_INFO_05.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_05")
dfn="TMP_OCRM_F_CI_PER_CUST_INFO_05/"+V_DT+".parquet"
TMP_OCRM_F_CI_PER_CUST_INFO_05.cache()
nrows = TMP_OCRM_F_CI_PER_CUST_INFO_05.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_PER_CUST_INFO_05/*.parquet")
TMP_OCRM_F_CI_PER_CUST_INFO_05.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_PER_CUST_INFO_05.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_PER_CUST_INFO_05 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-06::
V_STEP = V_STEP + 1
TMP_OCRM_F_CI_PER_CUST_INFO_05 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_PER_CUST_INFO_05/*')
TMP_OCRM_F_CI_PER_CUST_INFO_05.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_05")

sql = """
  SELECT A.CIFNAME               AS CIFNAME 
       ,E.IDTYPE                AS IDTYPE 
       ,E.IDNO                  AS IDNO 
       ,CASE WHEN F.GENDER = 'X' THEN NULL ELSE F.GENDER END                       AS SEX 
       ,C.ADDR                  AS ADDR 
       ,C.POSTCODE              AS ZIPCODE 
       ,D.TELNO                 AS PHONE 
       ,F.MOBILEPHONE           AS MOBILEPHONE 
       ,F.EMAIL                 AS EMAIL 
       ,A.FR_ID                 AS FR_ID 
       ,ROW_NUMBER() OVER( PARTITION BY A.CIFNAME,E.IDTYPE ,E.IDNO ORDER BY A.CIFNAME) AS RN 
   FROM F_TX_WSYH_ECCIF A                                      --电子银行参与方信息表
  INNER JOIN F_TX_WSYH_ECCIFMCH B                              --客户渠道表
     ON A.CIFSEQ                = B.CIFSEQ 
    AND A.FR_ID                 = B.FR_ID 
    AND B.MCHANNELID            = 'PIBS' 
  INNER JOIN F_CI_WSYH_ECCIFADDR C                             --客户地址表
     ON B.CIFSEQ                = C.CIFSEQ 
    AND A.FR_ID                 = C.FR_ID 
    AND C.ODS_ST_DATE           = V_DT 
  INNER JOIN F_CI_WSYH_ECCIFTEL D                              --客户电话表
     ON C.CIFSEQ                = D.CIFSEQ 
    AND A.FR_ID                 = D.FR_ID 
    AND D.ODS_ST_DATE           = V_DT 
  INNER JOIN F_CI_WSYH_ECCIFID E                               --客户证件表
     ON D.CIFSEQ                = E.CIFSEQ 
    AND A.FR_ID                 = E.FR_ID 
  INNER JOIN F_CI_WSYH_ECUSR F                                 --电子银行用户表
     ON E.CIFSEQ                = F.CIFSEQ 
    AND A.FR_ID                 = F.FR_ID"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_PER_CUST_INFO_06 = sqlContext.sql(sql)
TMP_OCRM_F_CI_PER_CUST_INFO_06.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_06")
dfn="TMP_OCRM_F_CI_PER_CUST_INFO_06/"+V_DT+".parquet"
TMP_OCRM_F_CI_PER_CUST_INFO_06.cache()
nrows = TMP_OCRM_F_CI_PER_CUST_INFO_06.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_PER_CUST_INFO_06/*.parquet")
TMP_OCRM_F_CI_PER_CUST_INFO_06.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_PER_CUST_INFO_06.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_PER_CUST_INFO_06 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-07::
V_STEP = V_STEP + 1
TMP_OCRM_F_CI_PER_CUST_INFO_06 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_PER_CUST_INFO_06/*')
TMP_OCRM_F_CI_PER_CUST_INFO_06.registerTempTable("TMP_OCRM_F_CI_PER_CUST_INFO_06")
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_PER_CUST_INFO/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_PER_CUST_INFO_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_PER_CUST_INFO/"+V_DT+".parquet")

OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
sql = """
 SELECT CAST(monotonically_increasing_id() AS BIGINT)                    AS ID ,
        A.*
      FROM (SELECT DISTINCT TRIM(A.ODS_CUST_ID)                       AS CUST_ID 
       ,A.SOURCE_CUST_NAME      AS CUST_NAME 
       ,''                    AS CUST_ENAME 
       ,''                    AS CUST_ENAME1 
       ,''                    AS CUST_BIR 
       ,''                    AS CUST_RESCNTY 
       ,''                    AS CUST_MRG 
       ,''                    AS CUST_SEX 
       ,''                    AS CUST_NATION 
       ,''                    AS CUST_REGISTER 
       ,''                    AS CUST_REGTYP 
       ,''                    AS CUST_REGADDR 
       ,''                    AS CUST_CITISHIP 
       ,''                    AS CUST_USED_NAME 
       ,''                    AS CUST_TTL 
       ,''                    AS CUST_GROUP 
       ,''                    AS CUST_EVADATE 
       ,'N'                     AS IS_STAFF 
       ,''                    AS CUS_TYPE 
       ,''                    AS IS_GOODMAN 
       ,''                    AS IS_TECH 
       ,''                    AS IS_GUDONG 
       ,''                    AS IS_SY 
       ,''                    AS IS_BUSSMA 
       ,''                    AS CUST_FAMSTATUS 
       ,''                    AS CUST_HEALTH 
       ,''                    AS CUST_LANGUAGE 
       ,''                    AS CUST_RELIGION 
       ,''                    AS CUST_POLIFACE 
       ,''                    AS CUST_RES_CNTY 
       ,''                    AS CITY_COD 
       ,''                    AS CUST_LOCALYEAR 
       ,''                    AS ID_BLACKLIST 
       ,''                    AS CUST_ATT_FMDAT 
       ,''                    AS CUST_ATT_DESC 
       ,''                    AS FIN_RISK_ASS 
       ,''                    AS RISK_APPETITE 
       ,''                    AS EVA_DATE 
       ,''                    AS FIN_WARN 
       ,''                    AS CUST_CRT_SCT 
       ,''                    AS CUST_HEIGHT 
       ,''                    AS CUST_WEIGHT 
       ,''                    AS CUST_BLOTYP 
       ,''                    AS TEMP_RESIDENCE 
       ,''                    AS BK_RELASHIP 
       ,''                    AS CUST_EDU_LVL_COD 
       ,''                    AS GIHHEST_DEGREE 
       ,''                    AS GRADUATION 
       ,''                    AS GRADUATE_NAME 
       ,''                    AS CUST_OCCUP_COD 
       ,''                    AS CUST_OCCUP_COD1 
       ,''                    AS OCCP_STATE 
       ,''                    AS WORK_STATE 
       ,''                    AS CUST_POSN 
       ,''                    AS CUST_TITL 
       ,''                    AS TERM_OF_CON 
       ,''                    AS WORK_YEAR 
       ,''                    AS CUST_WORK_YEAR 
       ,''                    AS CUST_WORK_UNIT_NAME 
       ,''                    AS CUST_UTELL 
       ,''                    AS CUST_UTYP 
       ,''                    AS DEPT 
       ,''                    AS IS_CONTROLLER 
       ,''                    AS SOCIAL_DUTY 
       ,''                    AS PER_DESCRIB 
       ,''                    AS PER_RESUME 
       ,''                    AS IS_FARMER_FLG 
       ,''                    AS HOUHOLD_CLASS 
       ,''                    AS CUST_TEAMNAME 
       ,''                    AS VILLAGE_NAME 
       ,''                    AS IS_VILLAGECADRE 
       ,''                    AS IS_MEDICARE 
       ,''                    AS IS_POORISNO 
       ,''                    AS MAKUP 
       ,''                    AS INDUSTRYTYPE 
       ,''                    AS MOSTBUSINESS 
       ,''                    AS BUSINESSADD 
       ,''                    AS LICENSENO 
       ,''                    AS LICENSEDATE 
       ,''                    AS TAXNO 
       ,''                    AS TAXNO1 
       ,''                    AS MAINPROORINCOME 
       ,''                    AS ADMIN_LVL 
       ,''                    AS WORK_PERMIT 
       ,''                    AS LINKMAN 
       ,''                    AS OFF_ADDR 
       ,''                    AS OFF_ZIP 
       ,''                    AS MICRO_BLOG 
       ,''                    AS FAX 
       ,''                    AS MSN 
       ,''                    AS OTHER_CONTACT 
       ,''                    AS CUST_REG_ADDR2 
       ,''                    AS CI_ADDR 
       ,''                    AS CUST_POSTCOD 
       ,''                    AS CUST_YEL_NO 
       ,''                    AS CUST_AREA_COD 
       ,''                    AS CUST_MBTELNO 
       ,''                    AS CUST_EMAIL 
       ,''                    AS CUST_SUB_TEL 
       ,''                    AS CUST_WEBSITE 
       ,''                    AS CUST_WORKADDR 
       ,''                    AS CUST_COMMADD 
       ,''                    AS CUST_COMZIP 
       ,''                    AS CUST_WORKZIP 
       ,''                    AS CUST_RELIGNLISM 
       ,''                    AS CUST_EFFSTATUS 
       ,''                    AS CUST_ARREA 
       ,''                    AS CUST_FAMADDR 
       ,''                    AS CUST_VILLAGENO 
       ,''                    AS NET_ADDR 
       ,A.CERT_TYPE             AS CUST_CRE_TYP 
       ,A.CERT_NO               AS CUST_CER_NO 
       ,CAST(0 AS DECIMAL(8))                    AS CUST_EXPD_DT 
       ,''                    AS CUST_CHK_FLG 
       ,''                    AS CUST_CER_STS 
       ,''                    AS CUST_SONO 
       ,''                    AS CUST_PECON_RESUR 
       ,''                    AS CUST_MN_INCO 
       ,CAST(0 AS DECIMAL(24))                    AS CUST_ANNUAL_INCOME 
       ,''                    AS PER_INCURR_YCODE 
       ,CAST(0 AS DECIMAL(15,2))                    AS PER_IN_ANOUNT 
       ,''                    AS PER_INCURR_MCODE 
       ,''                    AS PER_INCURR_FAMCODE 
       ,CAST(0 AS DECIMAL(24))                    AS FAM_INCOMEACC 
       ,CAST(0 AS DECIMAL(24))                    AS OTH_FAMINCOME 
       ,CAST(0 AS DECIMAL(15,2))                    AS CUST_TOT_ASS 
       ,CAST(0 AS DECIMAL(15,2))                    AS CUST_TOT_DEBT 
       ,''                    AS CUST_FAM_NUM 
       ,CAST(0 AS DECIMAL(2))                    AS CUST_DEPEND_NO 
       ,''                    AS CUST_OT_INCO 
       ,''                    AS CUST_HOUSE_TYP 
       ,''                    AS WAGES_ACCOUNT 
       ,''                    AS OPEN_BANK 
       ,''                    AS CRE_RECORD 
       ,''                    AS HAVE_YDTCARD 
       ,'N'                     AS IS_LIFSUR 
       ,'N'                     AS IS_ILLSUR 
       ,'N'                     AS IS_ENDOSUR 
       ,'N'                     AS HAVE_CAR 
       ,CAST(0 AS DECIMAL(15,2))                    AS AVG_ASS 
       ,''                    AS REMARK 
       ,''                    AS REC_UPSYS 
       ,''                    AS REC_UPMEC 
       ,''                    AS REC_UODATE 
       ,''                    AS REC_UPMAN 
       ,''                    AS SOUR_SYS 
       ,''                    AS PLAT_DATE 
       ,''                    AS COMBIN_FLG 
       ,''                    AS DATE_SOU 
       ,''                    AS INPUTDATE 
       ,''                    AS CUST_FANID 
       ,''                    AS HZ_CERTYPE 
       ,''                    AS HZ_CERTID 
       ,''                    AS CUST_TEAMNO 
       ,''                    AS HZ_NAME 
       ,CAST(0 AS DECIMAL(18))                    AS CUST_FAMNUM 
       ,''                    AS CUST_WEANAME 
       ,CAST(0 AS DECIMAL(18))                    AS CUST_WEAVALUE 
       ,''                    AS CONLANDAREA 
       ,CAST(0 AS DECIMAL(24))                    AS CONPOOLAREA 
       ,''                    AS CUST_CHILDREN 
       ,CAST(0 AS DECIMAL(18))                    AS NUM_OF_CHILD 
       ,CAST(0 AS DECIMAL(18))                    AS TOTINCO_OF_CH 
       ,''                    AS CUST_HOUSE 
       ,CAST(0 AS DECIMAL(18))                    AS CUST_HOUSECOUNT 
       ,CAST(0 AS DECIMAL(18))                    AS CUST_HOUSEAREA 
       ,''                    AS CUST_PRIVATECAR 
       ,''                    AS CAR_NUM_DESC 
       ,''                    AS CUST_FAMILYSORT 
       ,''                    AS CUST_OTHMEG 
       ,''                    AS CUST_SUMJF 
       ,CAST(0 AS DECIMAL(20))                    AS CUST_ZCJF 
       ,CAST(0 AS DECIMAL(20))                    AS CUST_FZJF 
       ,CAST(0 AS DECIMAL(20))                    AS CUST_CONSUMEJF 
       ,CAST(0 AS DECIMAL(20))                    AS CUST_CHANNELJF 
       ,CAST(0 AS DECIMAL(20))                    AS CUST_MIDBUSJF 
       ,CAST(0 AS DECIMAL(20))                    AS CRECARD_POINTS 
       ,''                    AS QQ 
       ,''                    AS MICRO_MSG 
       ,V_DT                    AS ODS_ST_DATE 
       ,''                    AS FAMILY_SAFE 
       ,''                    AS BAD_HABIT_FLAG 
       ,''                    AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,''                    AS LONGITUDE 
       ,''                    AS LATITUDE 
       ,''                    AS GPS 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
   LEFT JOIN OCRM_F_CI_PER_CUST_INFO B                         --对私客户信息表
     ON A.ODS_CUST_ID           = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.ODS_CUST_TYPE         = '1' 
    AND A.CUST_STAT             = '1' 
    AND A.ODS_CUST_ID IS 
    NOT NULL 
    AND A.ODS_SYS_ID            = 'CEN' 
    AND A.CERT_FLAG             = 'Y' 
    AND A.ODS_ST_DATE           = V_DT 
    AND B.CUST_ID IS NULL 
  GROUP BY A.FR_ID 
       ,A.ODS_CUST_ID 
       ,A.CERT_TYPE 
       ,A.CERT_NO 
       ,A.SOURCE_CUST_NAME) A """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_PER_CUST_INFO = sqlContext.sql(sql)
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
dfn="OCRM_F_CI_PER_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_PER_CUST_INFO.cache()
nrows = OCRM_F_CI_PER_CUST_INFO.count()
OCRM_F_CI_PER_CUST_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_PER_CUST_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_PER_CUST_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-08::
V_STEP = V_STEP + 1
OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
sql = """
 SELECT CAST(monotonically_increasing_id() AS BIGINT)                    AS ID 
       ,A.*
     FROM (SELECT DISTINCT TRIM(A.ODS_CUST_ID)                       AS CUST_ID 
       ,A.SOURCE_CUST_NAME      AS CUST_NAME 
       ,''                    AS CUST_ENAME 
       ,''                    AS CUST_ENAME1 
       ,''                    AS CUST_BIR 
       ,''                    AS CUST_RESCNTY 
       ,''                    AS CUST_MRG 
       ,''                    AS CUST_SEX 
       ,''                    AS CUST_NATION 
       ,''                    AS CUST_REGISTER 
       ,''                    AS CUST_REGTYP 
       ,''                    AS CUST_REGADDR 
       ,''                    AS CUST_CITISHIP 
       ,''                    AS CUST_USED_NAME 
       ,''                    AS CUST_TTL 
       ,''                    AS CUST_GROUP 
       ,''                    AS CUST_EVADATE 
       ,'N'                     AS IS_STAFF 
       ,''                    AS CUS_TYPE 
       ,''                    AS IS_GOODMAN 
       ,''                    AS IS_TECH 
       ,''                    AS IS_GUDONG 
       ,''                    AS IS_SY 
       ,''                    AS IS_BUSSMA 
       ,''                    AS CUST_FAMSTATUS 
       ,''                    AS CUST_HEALTH 
       ,''                    AS CUST_LANGUAGE 
       ,''                    AS CUST_RELIGION 
       ,''                    AS CUST_POLIFACE 
       ,''                    AS CUST_RES_CNTY 
       ,''                    AS CITY_COD 
       ,''                    AS CUST_LOCALYEAR 
       ,''                    AS ID_BLACKLIST 
       ,''                    AS CUST_ATT_FMDAT 
       ,''                    AS CUST_ATT_DESC 
       ,''                    AS FIN_RISK_ASS 
       ,''                    AS RISK_APPETITE 
       ,''                    AS EVA_DATE 
       ,''                    AS FIN_WARN 
       ,''                    AS CUST_CRT_SCT 
       ,''                    AS CUST_HEIGHT 
       ,''                    AS CUST_WEIGHT 
       ,''                    AS CUST_BLOTYP 
       ,''                    AS TEMP_RESIDENCE 
       ,''                    AS BK_RELASHIP 
       ,''                    AS CUST_EDU_LVL_COD 
       ,''                    AS GIHHEST_DEGREE 
       ,''                    AS GRADUATION 
       ,''                    AS GRADUATE_NAME 
       ,''                    AS CUST_OCCUP_COD 
       ,''                    AS CUST_OCCUP_COD1 
       ,''                    AS OCCP_STATE 
       ,''                    AS WORK_STATE 
       ,''                    AS CUST_POSN 
       ,''                    AS CUST_TITL 
       ,''                    AS TERM_OF_CON 
       ,''                    AS WORK_YEAR 
       ,''                    AS CUST_WORK_YEAR 
       ,''                    AS CUST_WORK_UNIT_NAME 
       ,''                    AS CUST_UTELL 
       ,''                    AS CUST_UTYP 
       ,''                    AS DEPT 
       ,''                    AS IS_CONTROLLER 
       ,''                    AS SOCIAL_DUTY 
       ,''                    AS PER_DESCRIB 
       ,''                    AS PER_RESUME 
       ,''                    AS IS_FARMER_FLG 
       ,''                    AS HOUHOLD_CLASS 
       ,''                    AS CUST_TEAMNAME 
       ,''                    AS VILLAGE_NAME 
       ,''                    AS IS_VILLAGECADRE 
       ,''                    AS IS_MEDICARE 
       ,''                    AS IS_POORISNO 
       ,''                    AS MAKUP 
       ,''                    AS INDUSTRYTYPE 
       ,''                    AS MOSTBUSINESS 
       ,''                    AS BUSINESSADD 
       ,''                    AS LICENSENO 
       ,''                    AS LICENSEDATE 
       ,''                    AS TAXNO 
       ,''                    AS TAXNO1 
       ,''                    AS MAINPROORINCOME 
       ,''                    AS ADMIN_LVL 
       ,''                    AS WORK_PERMIT 
       ,''                    AS LINKMAN 
       ,''                    AS OFF_ADDR 
       ,''                    AS OFF_ZIP 
       ,''                    AS MICRO_BLOG 
       ,''                    AS FAX 
       ,''                    AS MSN 
       ,''                    AS OTHER_CONTACT 
       ,''                    AS CUST_REG_ADDR2 
       ,''                    AS CI_ADDR 
       ,''                    AS CUST_POSTCOD 
       ,''                    AS CUST_YEL_NO 
       ,''                    AS CUST_AREA_COD 
       ,''                    AS CUST_MBTELNO 
       ,''                    AS CUST_EMAIL 
       ,''                    AS CUST_SUB_TEL 
       ,''                    AS CUST_WEBSITE 
       ,''                    AS CUST_WORKADDR 
       ,''                    AS CUST_COMMADD 
       ,''                    AS CUST_COMZIP 
       ,''                    AS CUST_WORKZIP 
       ,''                    AS CUST_RELIGNLISM 
       ,''                    AS CUST_EFFSTATUS 
       ,''                    AS CUST_ARREA 
       ,''                    AS CUST_FAMADDR 
       ,''                    AS CUST_VILLAGENO 
       ,''                    AS NET_ADDR 
       ,''                    AS CUST_CRE_TYP 
       ,''                    AS CUST_CER_NO 
       ,CAST(0 AS DECIMAL(8))                    AS CUST_EXPD_DT 
       ,''                    AS CUST_CHK_FLG 
       ,''                    AS CUST_CER_STS 
       ,''                    AS CUST_SONO 
       ,''                    AS CUST_PECON_RESUR 
       ,''                    AS CUST_MN_INCO 
       ,CAST(0 AS DECIMAL(24))                    AS CUST_ANNUAL_INCOME 
       ,''                    AS PER_INCURR_YCODE 
       ,CAST(0 AS DECIMAL(15,2))                    AS PER_IN_ANOUNT 
       ,''                    AS PER_INCURR_MCODE 
       ,''                    AS PER_INCURR_FAMCODE 
       ,CAST(0 AS DECIMAL(24))                    AS FAM_INCOMEACC 
       ,CAST(0 AS DECIMAL(24))                    AS OTH_FAMINCOME 
       ,CAST(0 AS DECIMAL(15,2))                    AS CUST_TOT_ASS 
       ,CAST(0 AS DECIMAL(15,2))                    AS CUST_TOT_DEBT 
       ,''                    AS CUST_FAM_NUM 
       ,CAST(0 AS DECIMAL(2))                    AS CUST_DEPEND_NO 
       ,''                    AS CUST_OT_INCO 
       ,''                    AS CUST_HOUSE_TYP 
       ,''                    AS WAGES_ACCOUNT 
       ,''                    AS OPEN_BANK 
       ,''                    AS CRE_RECORD 
       ,''                    AS HAVE_YDTCARD 
       ,'N'                     AS IS_LIFSUR 
       ,'N'                     AS IS_ILLSUR 
       ,'N'                     AS IS_ENDOSUR 
       ,'N'                     AS HAVE_CAR 
       ,CAST(0 AS DECIMAL(15,2))                    AS AVG_ASS 
       ,''                    AS REMARK 
       ,''                    AS REC_UPSYS 
       ,''                    AS REC_UPMEC 
       ,''                    AS REC_UODATE 
       ,''                    AS REC_UPMAN 
       ,''                    AS SOUR_SYS 
       ,''                    AS PLAT_DATE 
       ,''                    AS COMBIN_FLG 
       ,''                    AS DATE_SOU 
       ,''                    AS INPUTDATE 
       ,''                    AS CUST_FANID 
       ,''                    AS HZ_CERTYPE 
       ,''                    AS HZ_CERTID 
       ,''                    AS CUST_TEAMNO 
       ,''                    AS HZ_NAME 
       ,CAST(0 AS DECIMAL(18))                    AS CUST_FAMNUM 
       ,''                    AS CUST_WEANAME 
       ,CAST(0 AS DECIMAL(18))                    AS CUST_WEAVALUE 
       ,''                    AS CONLANDAREA 
       ,CAST(0 AS DECIMAL(24))                    AS CONPOOLAREA 
       ,''                    AS CUST_CHILDREN 
       ,CAST(0 AS DECIMAL(18))                    AS NUM_OF_CHILD 
       ,CAST(0 AS DECIMAL(18))                    AS TOTINCO_OF_CH 
       ,''                    AS CUST_HOUSE 
       ,CAST(0 AS DECIMAL(18))                    AS CUST_HOUSECOUNT 
       ,CAST(0 AS DECIMAL(18))                    AS CUST_HOUSEAREA 
       ,''                    AS CUST_PRIVATECAR 
       ,''                    AS CAR_NUM_DESC 
       ,''                    AS CUST_FAMILYSORT 
       ,''                    AS CUST_OTHMEG 
       ,''                    AS CUST_SUMJF 
       ,CAST(0 AS DECIMAL(20))                    AS CUST_ZCJF 
       ,CAST(0 AS DECIMAL(20))                    AS CUST_FZJF 
       ,CAST(0 AS DECIMAL(20))                    AS CUST_CONSUMEJF 
       ,CAST(0 AS DECIMAL(20))                    AS CUST_CHANNELJF 
       ,CAST(0 AS DECIMAL(20))                    AS CUST_MIDBUSJF 
       ,CAST(0 AS DECIMAL(20))                    AS CRECARD_POINTS 
       ,''                    AS QQ 
       ,''                    AS MICRO_MSG 
       ,V_DT                    AS ODS_ST_DATE 
       ,''                    AS FAMILY_SAFE 
       ,''                    AS BAD_HABIT_FLAG 
       ,''                    AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,''                    AS LONGITUDE 
       ,''                    AS LATITUDE 
       ,''                    AS GPS 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
   LEFT JOIN OCRM_F_CI_PER_CUST_INFO B                         --对私客户信息表
     ON A.ODS_CUST_ID           = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.ODS_CUST_TYPE         = '1' 
    AND A.CUST_STAT             = '1' 
    AND A.ODS_CUST_ID IS 
    NOT NULL 
    AND A.ODS_SYS_ID <> 'CEN' 
    AND A.ODS_ST_DATE           = V_DT 
    AND B.CUST_ID IS NULL 
  GROUP BY A.FR_ID 
       ,A.ODS_CUST_ID 
       ,A.SOURCE_CUST_NAME) A """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_PER_CUST_INFO = sqlContext.sql(sql)
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
dfn="OCRM_F_CI_PER_CUST_INFO/"+V_DT+".parquet" 
OCRM_F_CI_PER_CUST_INFO.cache()
nrows = OCRM_F_CI_PER_CUST_INFO.count()
OCRM_F_CI_PER_CUST_INFO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_F_CI_PER_CUST_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_PER_CUST_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-09::
V_STEP = V_STEP + 1

OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")

sql = """
 SELECT B.ID                    AS ID 
       ,B.CUST_ID               AS CUST_ID 
       ,COALESCE(A.CI_CHN_NAME, B.CUST_NAME)                       AS CUST_NAME 
       ,COALESCE(A.CI_ENG_SPL_NAME_1, B.CUST_ENAME)                       AS CUST_ENAME 
       ,COALESCE(A.CI_ENG_SPL_NAME_2, B.CUST_ENAME1)                       AS CUST_ENAME1 
       ,COALESCE((CASE WHEN LENGTH(A.CI_REG_BIRDY_N)=8 THEN CONCAT(SUBSTR(A.CI_REG_BIRDY_N,1,4),'-',SUBSTR(A.CI_REG_BIRDY_N,5,2),'-',SUBSTR(A.CI_REG_BIRDY_N,7,2)) ELSE NULL END), B.CUST_BIR) AS CUST_BIR 
       ,COALESCE(CASE WHEN A.CI_RESCNTY='X' THEN '' ELSE A.CI_RESCNTY END, B.CUST_RESCNTY)                       AS CUST_RESCNTY 
       ,COALESCE(CASE WHEN A.CI_MRG_COND='X' THEN '' ELSE CI_MRG_COND END, B.CUST_MRG)                       AS CUST_MRG 
       ,COALESCE(CASE WHEN A.CI_SEX='X' THEN '' ELSE CI_SEX END, B.CUST_SEX)                       AS CUST_SEX 
       ,COALESCE(CASE WHEN A.CI_PPL_COD='X' THEN '' ELSE CI_PPL_COD END, B.CUST_NATION)                       AS CUST_NATION 
       ,COALESCE(CASE WHEN A.CI_REGISTER='X' THEN '' ELSE CI_REGISTER END, B.CUST_REGISTER)                       AS CUST_REGISTER 
       ,CUST_REGTYP             AS CUST_REGTYP 
       ,COALESCE(B.CUST_REGADDR, C.EC_REG_ADDR)                       AS CUST_REGADDR 
       ,COALESCE(CASE WHEN A.CI_RES_CNTY='X' THEN '' ELSE A.CI_RES_CNTY END, B.CUST_CITISHIP)                       AS CUST_CITISHIP 
       ,CUST_USED_NAME          AS CUST_USED_NAME 
       ,COALESCE(CASE WHEN A.CI_CUST_TTL='X' THEN '' ELSE CI_CUST_TTL END, B.CUST_TTL)                       AS CUST_TTL 
       ,CUST_GROUP              AS CUST_GROUP 
       ,CUST_EVADATE            AS CUST_EVADATE 
       ,COALESCE(D.IS_STAFF,'N') AS IS_STAFF
       ,CUS_TYPE                AS CUS_TYPE 
       ,IS_GOODMAN              AS IS_GOODMAN 
       ,IS_TECH                 AS IS_TECH 
       ,IS_GUDONG               AS IS_GUDONG 
       ,IS_SY                   AS IS_SY 
       ,IS_BUSSMA               AS IS_BUSSMA 
       ,COALESCE(CASE WHEN A.CI_HOUSE_TYPE='X' THEN '' ELSE CI_HOUSE_TYPE END, B.CUST_FAMSTATUS)                       AS CUST_FAMSTATUS 
       ,CUST_HEALTH             AS CUST_HEALTH 
       ,COALESCE(B.CUST_LANGUAGE, C.EC_LANGUAGE_PRE)                       AS CUST_LANGUAGE 
       ,COALESCE(B.CUST_RELIGION, C.EC_RELIGION)                       AS CUST_RELIGION 
       ,CUST_POLIFACE           AS CUST_POLIFACE 
       ,CUST_RES_CNTY           AS CUST_RES_CNTY 
       ,CITY_COD                AS CITY_COD 
       ,CUST_LOCALYEAR          AS CUST_LOCALYEAR 
       ,ID_BLACKLIST            AS ID_BLACKLIST 
       ,CUST_ATT_FMDAT          AS CUST_ATT_FMDAT 
       ,CUST_ATT_DESC           AS CUST_ATT_DESC 
       ,FIN_RISK_ASS            AS FIN_RISK_ASS 
       ,RISK_APPETITE           AS RISK_APPETITE 
       ,EVA_DATE                AS EVA_DATE 
       ,FIN_WARN                AS FIN_WARN 
       ,COALESCE(A.CI_CRT_SCT_N, B.CUST_CRT_SCT)                       AS CUST_CRT_SCT 
       ,CUST_HEIGHT             AS CUST_HEIGHT 
       ,CUST_WEIGHT             AS CUST_WEIGHT 
       ,CUST_BLOTYP             AS CUST_BLOTYP 
       ,TEMP_RESIDENCE          AS TEMP_RESIDENCE 
       ,BK_RELASHIP             AS BK_RELASHIP 
       ,COALESCE(CASE WHEN A.CI_MAX_EDUC_LVL_COD='X' THEN '' ELSE CI_MAX_EDUC_LVL_COD END, B.CUST_EDU_LVL_COD)                       AS CUST_EDU_LVL_COD 
       ,GIHHEST_DEGREE          AS GIHHEST_DEGREE 
       ,GRADUATION              AS GRADUATION 
       ,GRADUATE_NAME           AS GRADUATE_NAME 
       ,COALESCE(CASE WHEN A.CI_OCCUP_COD='X' THEN '' ELSE CI_OCCUP_COD END, B.CUST_OCCUP_COD)                       AS CUST_OCCUP_COD 
       ,COALESCE(CASE WHEN A.CI_OCCUP_COD='X' THEN '' ELSE CI_OCCUP_COD END, B.CUST_OCCUP_COD1)                       AS CUST_OCCUP_COD1 
       ,OCCP_STATE              AS OCCP_STATE 
       ,WORK_STATE              AS WORK_STATE 
       ,COALESCE(CASE WHEN A.CI_POSN='X' THEN '' ELSE CI_POSN END, B.CUST_POSN)                       AS CUST_POSN 
       ,COALESCE(CASE WHEN A.CI_TITL='X' THEN '' ELSE CI_TITL END, B.CUST_TITL)                       AS CUST_TITL 
       ,TERM_OF_CON             AS TERM_OF_CON 
       ,WORK_YEAR               AS WORK_YEAR 
       ,COALESCE(A.CI_HAVE_WORK_YEAR, B.CUST_WORK_YEAR)                       AS CUST_WORK_YEAR 
       ,COALESCE(A.CI_WOKG_UNIT_NAME, B.CUST_WORK_UNIT_NAME)                       AS CUST_WORK_UNIT_NAME 
       ,COALESCE(A.CI_UNIT_CHN_INIL, B.CUST_UTELL)                       AS CUST_UTELL 
       ,COALESCE(CASE WHEN A.CI_CF_UNIT_TYP='X' THEN '' ELSE CI_CF_UNIT_TYP END, B.CUST_UTYP)                       AS CUST_UTYP 
       ,DEPT                    AS DEPT 
       ,IS_CONTROLLER           AS IS_CONTROLLER 
       ,SOCIAL_DUTY             AS SOCIAL_DUTY 
       ,PER_DESCRIB             AS PER_DESCRIB 
       ,PER_RESUME              AS PER_RESUME 
       ,IS_FARMER_FLG           AS IS_FARMER_FLG 
       ,HOUHOLD_CLASS           AS HOUHOLD_CLASS 
       ,CUST_TEAMNAME           AS CUST_TEAMNAME 
       ,VILLAGE_NAME            AS VILLAGE_NAME 
       ,IS_VILLAGECADRE         AS IS_VILLAGECADRE 
       ,IS_MEDICARE             AS IS_MEDICARE 
       ,IS_POORISNO             AS IS_POORISNO 
       ,MAKUP                   AS MAKUP 
       ,INDUSTRYTYPE            AS INDUSTRYTYPE 
       ,MOSTBUSINESS            AS MOSTBUSINESS 
       ,BUSINESSADD             AS BUSINESSADD 
       ,LICENSENO               AS LICENSENO 
       ,LICENSEDATE             AS LICENSEDATE 
       ,TAXNO                   AS TAXNO 
       ,TAXNO1                  AS TAXNO1 
       ,MAINPROORINCOME         AS MAINPROORINCOME 
       ,ADMIN_LVL               AS ADMIN_LVL 
       ,WORK_PERMIT             AS WORK_PERMIT 
       ,LINKMAN                 AS LINKMAN 
       ,OFF_ADDR                AS OFF_ADDR 
       ,OFF_ZIP                 AS OFF_ZIP 
       ,MICRO_BLOG              AS MICRO_BLOG 
       ,FAX                     AS FAX 
       ,MSN                     AS MSN 
       ,OTHER_CONTACT           AS OTHER_CONTACT 
       ,CUST_REG_ADDR2          AS CUST_REG_ADDR2 
       ,CI_ADDR                 AS CI_ADDR 
       ,CUST_POSTCOD            AS CUST_POSTCOD 
       ,CUST_YEL_NO             AS CUST_YEL_NO 
       ,CUST_AREA_COD           AS CUST_AREA_COD 
       ,CUST_MBTELNO            AS CUST_MBTELNO 
       ,CUST_EMAIL              AS CUST_EMAIL 
       ,CUST_SUB_TEL            AS CUST_SUB_TEL 
       ,CUST_WEBSITE            AS CUST_WEBSITE 
       ,CUST_WORKADDR           AS CUST_WORKADDR 
       ,CUST_COMMADD            AS CUST_COMMADD 
       ,CUST_COMZIP             AS CUST_COMZIP 
       ,CUST_WORKZIP            AS CUST_WORKZIP 
       ,CUST_RELIGNLISM         AS CUST_RELIGNLISM 
       ,CUST_EFFSTATUS          AS CUST_EFFSTATUS 
       ,CUST_ARREA              AS CUST_ARREA 
       ,CUST_FAMADDR            AS CUST_FAMADDR 
       ,CUST_VILLAGENO          AS CUST_VILLAGENO 
       ,NET_ADDR                AS NET_ADDR 
       ,CUST_CRE_TYP            AS CUST_CRE_TYP 
       ,CUST_CER_NO             AS CUST_CER_NO 
       ,CUST_EXPD_DT            AS CUST_EXPD_DT 
       ,CUST_CHK_FLG            AS CUST_CHK_FLG 
       ,CUST_CER_STS            AS CUST_CER_STS 
       ,CUST_SONO               AS CUST_SONO 
       ,COALESCE(A.CI_PECON_RESUR, B.CUST_PECON_RESUR)                       AS CUST_PECON_RESUR 
       ,COALESCE(CASE WHEN A.CI_MN_INCOM='X' THEN '' ELSE A.CI_MN_INCOM END, B.CUST_MN_INCO)                       AS CUST_MN_INCO 
       ,COALESCE(B.CUST_ANNUAL_INCOME, C.EC_ANNUAL_INCOME)                       AS CUST_ANNUAL_INCOME 
       ,PER_INCURR_YCODE        AS PER_INCURR_YCODE 
       ,PER_IN_ANOUNT           AS PER_IN_ANOUNT 
       ,PER_INCURR_MCODE        AS PER_INCURR_MCODE 
       ,PER_INCURR_FAMCODE      AS PER_INCURR_FAMCODE 
       ,FAM_INCOMEACC           AS FAM_INCOMEACC 
       ,OTH_FAMINCOME           AS OTH_FAMINCOME 
       ,COALESCE(B.CUST_TOT_ASS, C.EC_TOT_ASSETS)                       AS CUST_TOT_ASS 
       ,CAST(COALESCE(COALESCE(A.CI_MANG_DEPT, B.CUST_TOT_DEBT),C.EC_TOT_DEBT) AS DECIMAL(15,2))                       AS CUST_TOT_DEBT 
       ,COALESCE(B.CUST_FAM_NUM, C.EC_FAMILY_NUM_N)                       AS CUST_FAM_NUM 
       ,COALESCE(B.CUST_DEPEND_NO, C.EC_FAMILY_NUM_N)                       AS CUST_DEPEND_NO 
       ,COALESCE(A.CI_OTH_ECON_RSUR, B.CUST_OT_INCO)                       AS CUST_OT_INCO 
       ,COALESCE(CASE WHEN A.CI_HOUSE_TYPE='X' THEN '' ELSE CI_HOUSE_TYPE END, B.CUST_HOUSE_TYP)                       AS CUST_HOUSE_TYP 
       ,WAGES_ACCOUNT           AS WAGES_ACCOUNT 
       ,OPEN_BANK               AS OPEN_BANK 
       ,CRE_RECORD              AS CRE_RECORD 
       ,HAVE_YDTCARD            AS HAVE_YDTCARD 
       ,IS_LIFSUR               AS IS_LIFSUR 
       ,IS_ILLSUR               AS IS_ILLSUR 
       ,IS_ENDOSUR              AS IS_ENDOSUR 
       ,HAVE_CAR                AS HAVE_CAR 
       ,AVG_ASS                 AS AVG_ASS 
       ,REMARK                  AS REMARK 
       ,REC_UPSYS               AS REC_UPSYS 
       ,REC_UPMEC               AS REC_UPMEC 
       ,REC_UODATE              AS REC_UODATE 
       ,REC_UPMAN               AS REC_UPMAN 
       ,SOUR_SYS                AS SOUR_SYS 
       ,PLAT_DATE               AS PLAT_DATE 
       ,COMBIN_FLG              AS COMBIN_FLG 
       ,DATE_SOU                AS DATE_SOU 
       ,INPUTDATE               AS INPUTDATE 
       ,CUST_FANID              AS CUST_FANID 
       ,HZ_CERTYPE              AS HZ_CERTYPE 
       ,HZ_CERTID               AS HZ_CERTID 
       ,CUST_TEAMNO             AS CUST_TEAMNO 
       ,HZ_NAME                 AS HZ_NAME 
       ,CUST_FAMNUM             AS CUST_FAMNUM 
       ,CUST_WEANAME            AS CUST_WEANAME 
       ,CUST_WEAVALUE           AS CUST_WEAVALUE 
       ,CONLANDAREA             AS CONLANDAREA 
       ,CONPOOLAREA             AS CONPOOLAREA 
       ,CUST_CHILDREN           AS CUST_CHILDREN 
       ,NUM_OF_CHILD            AS NUM_OF_CHILD 
       ,TOTINCO_OF_CH           AS TOTINCO_OF_CH 
       ,CUST_HOUSE              AS CUST_HOUSE 
       ,CUST_HOUSECOUNT         AS CUST_HOUSECOUNT 
       ,CUST_HOUSEAREA          AS CUST_HOUSEAREA 
       ,CUST_PRIVATECAR         AS CUST_PRIVATECAR 
       ,CAR_NUM_DESC            AS CAR_NUM_DESC 
       ,CUST_FAMILYSORT         AS CUST_FAMILYSORT 
       ,CUST_OTHMEG             AS CUST_OTHMEG 
       ,CUST_SUMJF              AS CUST_SUMJF 
       ,CUST_ZCJF               AS CUST_ZCJF 
       ,CUST_FZJF               AS CUST_FZJF 
       ,CUST_CONSUMEJF          AS CUST_CONSUMEJF 
       ,CUST_CHANNELJF          AS CUST_CHANNELJF 
       ,CUST_MIDBUSJF           AS CUST_MIDBUSJF 
       ,CRECARD_POINTS          AS CRECARD_POINTS 
       ,QQ                      AS QQ 
       ,MICRO_MSG               AS MICRO_MSG 
       ,V_DT                    AS ODS_ST_DATE 
       ,FAMILY_SAFE             AS FAMILY_SAFE 
       ,BAD_HABIT_FLAG          AS BAD_HABIT_FLAG 
       ,IS_MODIFY               AS IS_MODIFY 
       ,B.FR_ID                 AS FR_ID 
       ,LONGITUDE               AS LONGITUDE 
       ,LATITUDE                AS LATITUDE 
       ,GPS                     AS GPS 
   FROM OCRM_F_CI_PER_CUST_INFO B  --对私客户信息表
   LEFT JOIN F_CI_CBOD_CICIFCIF A  --对私客户信息主档                     
     ON A.CI_CUST_NO = B.CUST_ID AND A.ODS_ST_DATE = V_DT
   LEFT JOIN F_CI_CBOD_ECCIFICS C                         --对私客户补充信息
     ON C.EC_CUST_NO = B.CUST_ID AND C.ODS_ST_DATE = V_DT 
   LEFT JOIN (SELECT DISTINCT D.FR_ID,D.ODS_CUST_ID,'Y' AS IS_STAFF
             FROM F_CM_CBOD_CMEMPEMP C ,OCRM_F_CI_SYS_RESOURCE D 
                           WHERE C.CM_ID_NO=D.CERT_NO AND D.CERT_TYPE='0'
                             AND D.ODS_SYS_ID='CEN' AND D.ODS_CUST_TYPE='1' 
                             AND C.FR_ID=D.FR_ID
         ) D ON B.FR_ID = D.FR_ID AND B.CUST_ID=D.ODS_CUST_ID """ 

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



#任务[12] 001-10::
V_STEP = V_STEP + 1

OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
sql = """
 SELECT DISTINCT A.ID                    AS ID 
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
       ,COALESCE(D.CI_ADDR, COALESCE(B.CI_ADDR, A.CUST_REGADDR))                       AS CUST_REGADDR 
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
       ,(CASE WHEN C.EC_ATT_TYP            = '04' THEN 1 ELSE 2 END)                       AS ID_BLACKLIST 
       ,CAST(C.EC_ATT_FMDAT_N AS VARCHAR(10))                       AS CUST_ATT_FMDAT 
       ,C.EC_ATT_DESC           AS CUST_ATT_DESC 
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
       ,COALESCE(D.CI_ADDR,COALESCE(B.CI_ADDR, A.CI_ADDR))                       AS CI_ADDR 
       ,COALESCE(D.CI_POSTCOD,COALESCE(B.CI_POSTCOD, A.CUST_POSTCOD))                       AS CUST_POSTCOD 
       ,COALESCE(D.CI_TEL_NO,COALESCE(B.CI_TEL_NO, A.CUST_YEL_NO))                       AS CUST_YEL_NO 
       ,COALESCE(D.CI_AREA_COD,COALESCE(B.CI_AREA_COD, A.CUST_AREA_COD))                       AS CUST_AREA_COD 
       ,COALESCE(D.CI_MOBILE_PHONE,COALESCE(B.CI_MOBILE_PHONE, A.CUST_MBTELNO))                       AS CUST_MBTELNO 
       ,COALESCE(D.CI_EMAIL,COALESCE(B.CI_EMAIL, A.CUST_EMAIL))                       AS CUST_EMAIL 
       ,COALESCE(D.CI_SUB_TEL,COALESCE(B.CI_SUB_TEL, A.CUST_SUB_TEL))                       AS CUST_SUB_TEL 
       ,COALESCE(D.CI_WEBSITE,COALESCE(B.CI_WEBSITE, A.CUST_WEBSITE))                       AS CUST_WEBSITE 
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
       ,V_DT                    AS ODS_ST_DATE 
       ,A.FAMILY_SAFE           AS FAMILY_SAFE 
       ,A.BAD_HABIT_FLAG        AS BAD_HABIT_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_PER_CUST_INFO A                              --对私客户信息表
  LEFT JOIN F_CI_CBOD_CICIFADR B                              --对私客户地址信息表
     ON A.CUST_ID               = B.FK_CICIF_KEY 
    AND A.ODS_ST_DATE           = V_DT 
    AND B.CI_ADDR_COD           = 'HOM' 
   LEFT JOIN TMP_OCRM_F_CI_PER_CUST_INFO_01 C                  --对私客户信息表临时表01
     ON A.FR_ID                 = C.FR_ID 
    AND A.CUST_ID               = C.EC_CUST_NO 
  LEFT JOIN F_CI_CBOD_CICIFADR D                              --对私客户地址信息表
     ON A.CUST_ID               = D.FK_CICIF_KEY 
    AND A.ODS_ST_DATE           = V_DT 
    AND D.CI_ADDR_COD           = 'MOB'"""


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

#任务[12] 001-11::
V_STEP = V_STEP + 1

OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
sql = """
 SELECT DISTINCT H.ID                    AS ID 
       ,H.CUST_ID               AS CUST_ID 
       ,COALESCE(A.FULLNAME, H.CUST_NAME)                       AS CUST_NAME 
       ,H.CUST_ENAME            AS CUST_ENAME 
       ,H.CUST_ENAME1           AS CUST_ENAME1 
       ,COALESCE((CASE WHEN A.BIRTHDAY IS NULL 
                     OR A.BIRTHDAY LIKE '%/%' THEN NULL 
                 ELSE concat(SUBSTR(A.BIRTHDAY,1,4),'-',SUBSTR(A.BIRTHDAY,5,2),'-',SUBSTR(A.BIRTHDAY,7,2)) END), H.CUST_BIR)                       AS CUST_BIR 
       ,H.CUST_RESCNTY          AS CUST_RESCNTY 
       ,COALESCE(A.MARRIAGE, H.CUST_MRG)                       AS CUST_MRG 
       ,COALESCE(A.SEX, H.CUST_SEX)                       AS CUST_SEX 
       ,COALESCE(A.NATIONALITY, H.CUST_NATION)                       AS CUST_NATION 
       ,H.CUST_REGISTER         AS CUST_REGISTER 
       ,COALESCE((CASE WHEN B.CUSTOMERTYPE          = '0310' THEN '非农户' WHEN B.CUSTOMERTYPE          = '0315' THEN '农户' END), H.CUST_REGTYP)                       AS CUST_REGTYP 
       ,COALESCE(A.FAMILYADD, H.CUST_REGADDR)                       AS CUST_REGADDR 
       ,H.CUST_CITISHIP         AS CUST_CITISHIP 
       ,H.CUST_USED_NAME        AS CUST_USED_NAME 
       ,H.CUST_TTL              AS CUST_TTL 
       ,H.CUST_GROUP            AS CUST_GROUP 
       ,H.CUST_EVADATE          AS CUST_EVADATE 
       ,H.IS_STAFF              AS IS_STAFF 
       ,H.CUS_TYPE              AS CUS_TYPE 
       ,H.IS_GOODMAN            AS IS_GOODMAN 
       ,H.IS_TECH               AS IS_TECH 
       ,COALESCE(A.MYBANKDORM, H.IS_GUDONG)                       AS IS_GUDONG 
       ,COALESCE(A.ASSOCIATOR, H.IS_SY)                       AS IS_SY 
       ,COALESCE(A.ISBUSINESSMAN, H.IS_BUSSMA)                       AS IS_BUSSMA 
       ,COALESCE(A.FAMILYSTATUS, H.CUST_FAMSTATUS)                       AS CUST_FAMSTATUS 
       ,COALESCE(A.HEALTH, H.CUST_HEALTH)                       AS CUST_HEALTH 
       ,H.CUST_LANGUAGE         AS CUST_LANGUAGE 
       ,H.CUST_RELIGION         AS CUST_RELIGION 
       ,COALESCE(A.POLITICALFACE, H.CUST_POLIFACE)                       AS CUST_POLIFACE 
       ,H.CUST_RES_CNTY         AS CUST_RES_CNTY 
       ,H.CITY_COD              AS CITY_COD 
       ,COALESCE(A.LOCALYEAR, H.CUST_LOCALYEAR)                       AS CUST_LOCALYEAR 
       ,COALESCE(CASE WHEN B.BLACKSHEETORNOT= 'X' THEN NULL ELSE B.BLACKSHEETORNOT END, H.ID_BLACKLIST)                       AS ID_BLACKLIST 
       ,COALESCE(C.INPUTDATE, H.CUST_ATT_FMDAT)                       AS CUST_ATT_FMDAT 
       ,COALESCE(C.INLISTREASON, H.CUST_ATT_DESC)                       AS CUST_ATT_DESC 
       ,H.FIN_RISK_ASS          AS FIN_RISK_ASS 
       ,H.RISK_APPETITE         AS RISK_APPETITE 
       ,H.EVA_DATE              AS EVA_DATE 
       ,H.FIN_WARN              AS FIN_WARN 
       ,H.CUST_CRT_SCT          AS CUST_CRT_SCT 
       ,H.CUST_HEIGHT           AS CUST_HEIGHT 
       ,H.CUST_WEIGHT           AS CUST_WEIGHT 
       ,H.CUST_BLOTYP           AS CUST_BLOTYP 
       ,H.TEMP_RESIDENCE        AS TEMP_RESIDENCE 
       ,H.BK_RELASHIP           AS BK_RELASHIP 
       ,COALESCE(CASE WHEN A.EDUEXPERIENCE = '-' THEN NULL ELSE A.EDUEXPERIENCE END, H.CUST_EDU_LVL_COD)                       AS CUST_EDU_LVL_COD 
       ,COALESCE(A.EDUDEGREE, H.GIHHEST_DEGREE)                       AS GIHHEST_DEGREE 
       ,H.GRADUATION            AS GRADUATION 
       ,H.GRADUATE_NAME         AS GRADUATE_NAME 
       ,COALESCE(A.OCCUPATION, H.CUST_OCCUP_COD)                       AS CUST_OCCUP_COD 
       ,COALESCE(A.OCCUPATION1, H.CUST_OCCUP_COD1)                       AS CUST_OCCUP_COD1 
       ,H.OCCP_STATE            AS OCCP_STATE 
       ,H.WORK_STATE            AS WORK_STATE 
       ,COALESCE(A.HEADSHIP, H.CUST_POSN)                       AS CUST_POSN 
       ,COALESCE(A.POSITION, H.CUST_TITL)                       AS CUST_TITL 
       ,H.TERM_OF_CON           AS TERM_OF_CON 
       ,H.WORK_YEAR             AS WORK_YEAR 
       ,H.CUST_WORK_YEAR        AS CUST_WORK_YEAR 
       ,COALESCE(A.WORKCORP, H.CUST_WORK_UNIT_NAME)                       AS CUST_WORK_UNIT_NAME 
       ,H.CUST_UTELL            AS CUST_UTELL 
       ,H.CUST_UTYP             AS CUST_UTYP 
       ,H.DEPT                  AS DEPT 
       ,H.IS_CONTROLLER         AS IS_CONTROLLER 
       ,H.SOCIAL_DUTY           AS SOCIAL_DUTY 
       ,H.PER_DESCRIB           AS PER_DESCRIB 
       ,H.PER_RESUME            AS PER_RESUME 
       ,H.IS_FARMER_FLG         AS IS_FARMER_FLG 
       ,H.HOUHOLD_CLASS         AS HOUHOLD_CLASS 
       ,COALESCE(A.TEAMNAME, H.CUST_TEAMNAME)                       AS CUST_TEAMNAME 
       ,COALESCE(A.REMARK3, H.VILLAGE_NAME)                       AS VILLAGE_NAME 
       ,COALESCE(A.VILLAGECADRE, H.IS_VILLAGECADRE)                       AS IS_VILLAGECADRE 
       ,COALESCE(A.MEDICARE, H.IS_MEDICARE)                       AS IS_MEDICARE 
       ,COALESCE(A.POORISNO, H.IS_POORISNO)                       AS IS_POORISNO 
       ,COALESCE(A.MARKUP, H.MAKUP)                       AS MAKUP 
       ,COALESCE(H.INDUSTRYTYPE, A.MANAGETYPE)                       AS INDUSTRYTYPE 
       ,H.MOSTBUSINESS          AS MOSTBUSINESS 
       ,H.BUSINESSADD           AS BUSINESSADD 
       ,H.LICENSENO             AS LICENSENO 
       ,H.LICENSEDATE           AS LICENSEDATE 
       ,H.TAXNO                 AS TAXNO 
       ,H.TAXNO1                AS TAXNO1 
       ,COALESCE(A.MAINPROORINCOME, H.MAINPROORINCOME)                       AS MAINPROORINCOME 
       ,H.ADMIN_LVL             AS ADMIN_LVL 
       ,H.WORK_PERMIT           AS WORK_PERMIT 
       ,H.LINKMAN               AS LINKMAN 
       ,H.OFF_ADDR              AS OFF_ADDR 
       ,H.OFF_ZIP               AS OFF_ZIP 
       ,H.MICRO_BLOG            AS MICRO_BLOG 
       ,H.FAX                   AS FAX 
       ,H.MSN                   AS MSN 
       ,H.OTHER_CONTACT         AS OTHER_CONTACT 
       ,H.CUST_REG_ADDR2        AS CUST_REG_ADDR2 
       ,H.CI_ADDR               AS CI_ADDR 
       ,COALESCE(A.FAMILYZIP, H.CUST_POSTCOD)                       AS CUST_POSTCOD 
       ,H.CUST_YEL_NO           AS CUST_YEL_NO 
       ,H.CUST_AREA_COD         AS CUST_AREA_COD 
       ,COALESCE(A.MOBILETELEPHONE, H.CUST_MBTELNO)                       AS CUST_MBTELNO 
       ,COALESCE(A.EMAILADD, H.CUST_EMAIL)                       AS CUST_EMAIL 
       ,H.CUST_SUB_TEL          AS CUST_SUB_TEL 
       ,H.CUST_WEBSITE          AS CUST_WEBSITE 
       ,COALESCE(A.WORKADD, H.CUST_WORKADDR)                       AS CUST_WORKADDR 
       ,COALESCE(A.FAMILYADD, H.CUST_COMMADD)                       AS CUST_COMMADD 
       ,COALESCE(A.COMMZIP, H.CUST_COMZIP)                       AS CUST_COMZIP 
       ,COALESCE(A.WORKZIP, H.CUST_WORKZIP)                       AS CUST_WORKZIP 
       ,COALESCE(A.REGIONALISM, H.CUST_RELIGNLISM)                       AS CUST_RELIGNLISM 
       ,H.CUST_EFFSTATUS        AS CUST_EFFSTATUS 
       ,H.CUST_ARREA            AS CUST_ARREA 
       ,COALESCE(A.FAMILYADD, H.CUST_FAMADDR)                       AS CUST_FAMADDR 
       ,H.CUST_VILLAGENO        AS CUST_VILLAGENO 
       ,H.NET_ADDR              AS NET_ADDR 
       ,COALESCE(A.CERTTYPE, H.CUST_CRE_TYP)                       AS CUST_CRE_TYP 
       ,COALESCE(A.CERTID, H.CUST_CER_NO)                       AS CUST_CER_NO 
       ,H.CUST_EXPD_DT          AS CUST_EXPD_DT 
       ,H.CUST_CHK_FLG          AS CUST_CHK_FLG 
       ,H.CUST_CER_STS          AS CUST_CER_STS 
       ,COALESCE(A.SINO, H.CUST_SONO)                       AS CUST_SONO 
       ,COALESCE(A.INCOMESOURCE, H.CUST_PECON_RESUR)                       AS CUST_PECON_RESUR 
       ,COALESCE(TRIM(CAST(A.SELFMONTHINCOME AS VARCHAR(31))), H.CUST_MN_INCO)                       AS CUST_MN_INCO 
       ,COALESCE(A.YEARINCOME, H.CUST_ANNUAL_INCOME)                       AS CUST_ANNUAL_INCOME 
       ,H.PER_INCURR_YCODE      AS PER_INCURR_YCODE 
       ,H.PER_IN_ANOUNT         AS PER_IN_ANOUNT 
       ,H.PER_INCURR_MCODE      AS PER_INCURR_MCODE 
       ,H.PER_INCURR_FAMCODE    AS PER_INCURR_FAMCODE 
       ,CAST(COALESCE(A.FAMILYYEARIN, H.FAM_INCOMEACC) AS DECIMAL(24,0))                       AS FAM_INCOMEACC 
       ,H.OTH_FAMINCOME         AS OTH_FAMINCOME 
       ,H.CUST_TOT_ASS          AS CUST_TOT_ASS 
       ,H.CUST_TOT_DEBT         AS CUST_TOT_DEBT 
       ,COALESCE(A.FAMILYAMOUNT, H.CUST_FAM_NUM)                       AS CUST_FAM_NUM 
       ,H.CUST_DEPEND_NO        AS CUST_DEPEND_NO 
       ,H.CUST_OT_INCO          AS CUST_OT_INCO 
       ,COALESCE(A.FAMILYSTATUS, H.CUST_HOUSE_TYP)                       AS CUST_HOUSE_TYP 
       ,COALESCE(A.PAYACCOUNT, H.WAGES_ACCOUNT)                       AS WAGES_ACCOUNT 
       ,COALESCE(A.PAYACCOUNTBANK, H.OPEN_BANK)                       AS OPEN_BANK 
       ,COALESCE(CRE_RECORD, A.CREDITRECORD)                       AS CRE_RECORD 
       ,COALESCE(A.FARMERCARD, H.HAVE_YDTCARD)                       AS HAVE_YDTCARD 
       ,H.IS_LIFSUR             AS IS_LIFSUR 
       ,H.IS_ILLSUR             AS IS_ILLSUR 
       ,H.IS_ENDOSUR            AS IS_ENDOSUR 
       ,H.HAVE_CAR              AS HAVE_CAR 
       ,H.AVG_ASS               AS AVG_ASS 
       ,H.REMARK                AS REMARK 
       ,H.REC_UPSYS             AS REC_UPSYS 
       ,H.REC_UPMEC             AS REC_UPMEC 
       ,H.REC_UODATE            AS REC_UODATE 
       ,H.REC_UPMAN             AS REC_UPMAN 
       ,H.SOUR_SYS              AS SOUR_SYS 
       ,H.PLAT_DATE             AS PLAT_DATE 
       ,H.COMBIN_FLG            AS COMBIN_FLG 
       ,H.DATE_SOU              AS DATE_SOU 
       ,H.INPUTDATE             AS INPUTDATE 
       ,COALESCE(A.CENSUSBOOKID, H.CUST_FANID)                       AS CUST_FANID 
       ,COALESCE(A.HOUSEMASTERCERTTYPE, H.HZ_CERTYPE)                       AS HZ_CERTYPE 
       ,COALESCE(A.HOUSEMASTERCERTID, H.HZ_CERTID)                       AS HZ_CERTID 
       ,COALESCE(A.TEAMNO, H.CUST_TEAMNO)                       AS CUST_TEAMNO 
       ,COALESCE(A.HOUSEMASTERNAME, H.HZ_NAME)                       AS HZ_NAME 
       ,COALESCE(A.FAMILYNUM, H.CUST_FAMNUM)                       AS CUST_FAMNUM 
       ,COALESCE(A.WEALTHNAME, H.CUST_WEANAME)                       AS CUST_WEANAME 
       ,CAST(COALESCE(A.WEALTHVALUE, H.CUST_WEAVALUE) AS DECIMAL(18))                      AS CUST_WEAVALUE 
       ,COALESCE(A.CONLANDAREA, H.CONLANDAREA)                       AS CONLANDAREA 
       ,CAST(COALESCE(A.CONPOOLAREA, H.CONPOOLAREA) AS DECIMAL(24,0))                       AS CONPOOLAREA 
       ,H.CUST_CHILDREN         AS CUST_CHILDREN 
       ,H.NUM_OF_CHILD          AS NUM_OF_CHILD 
       ,H.TOTINCO_OF_CH         AS TOTINCO_OF_CH 
       ,H.CUST_HOUSE            AS CUST_HOUSE 
       ,H.CUST_HOUSECOUNT       AS CUST_HOUSECOUNT 
       ,H.CUST_HOUSEAREA        AS CUST_HOUSEAREA 
       ,H.CUST_PRIVATECAR       AS CUST_PRIVATECAR 
       ,H.CAR_NUM_DESC          AS CAR_NUM_DESC 
       ,H.CUST_FAMILYSORT       AS CUST_FAMILYSORT 
       ,H.CUST_OTHMEG           AS CUST_OTHMEG 
       ,H.CUST_SUMJF            AS CUST_SUMJF 
       ,H.CUST_ZCJF             AS CUST_ZCJF 
       ,H.CUST_FZJF             AS CUST_FZJF 
       ,H.CUST_CONSUMEJF        AS CUST_CONSUMEJF 
       ,H.CUST_CHANNELJF        AS CUST_CHANNELJF 
       ,H.CUST_MIDBUSJF         AS CUST_MIDBUSJF 
       ,H.CRECARD_POINTS        AS CRECARD_POINTS 
       ,H.QQ                    AS QQ 
       ,H.MICRO_MSG             AS MICRO_MSG 
       ,V_DT                    AS ODS_ST_DATE 
       ,H.FAMILY_SAFE           AS FAMILY_SAFE 
       ,H.BAD_HABIT_FLAG        AS BAD_HABIT_FLAG 
       ,H.IS_MODIFY             AS IS_MODIFY 
       ,H.FR_ID                 AS FR_ID 
       ,H.LONGITUDE             AS LONGITUDE 
       ,H.LATITUDE              AS LATITUDE 
       ,H.GPS                   AS GPS 
   FROM OCRM_F_CI_PER_CUST_INFO H                              --对私客户信息表
  LEFT JOIN TMP_OCRM_F_CI_PER_CUST_INFO_02 D                  --对私客户信息表临时表02
     ON H.FR_ID                 = D.FR_ID 
    AND D.ODS_CUST_ID           = H.CUST_ID 
  LEFT JOIN F_CI_XDXT_IND_INFO A                              --个人基本信息
     ON A.CUSTOMERID            = D.SOURCE_CUST_ID 
    AND A.FR_ID                 = H.FR_ID 
    AND A.ODS_ST_DATE           = V_DT 
   LEFT JOIN F_CI_XDXT_CUSTOMER_INFO B                         --客户基本信息
     ON A.CUSTOMERID            = B.CUSTOMERID 
    AND B.FR_ID                 = H.FR_ID 
   LEFT JOIN TMP_OCRM_F_CI_PER_CUST_INFO_03 C                  --对私客户信息表临时表03
     ON A.CUSTOMERID            = C.CUSTOMERID 
    AND C.FR_ID                 = H.FR_ID """


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

#任务[12] 001-12::
V_STEP = V_STEP + 1

OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
sql = """
 SELECT DISTINCT B.ID                    AS ID 
       ,B.CUST_ID               AS CUST_ID 
       ,B.CUST_NAME             AS CUST_NAME 
       ,B.CUST_ENAME            AS CUST_ENAME 
       ,B.CUST_ENAME1           AS CUST_ENAME1 
       ,B.CUST_BIR              AS CUST_BIR 
       ,B.CUST_RESCNTY          AS CUST_RESCNTY 
       ,B.CUST_MRG              AS CUST_MRG 
       ,B.CUST_SEX              AS CUST_SEX 
       ,B.CUST_NATION           AS CUST_NATION 
       ,B.CUST_REGISTER         AS CUST_REGISTER 
       ,B.CUST_REGTYP           AS CUST_REGTYP 
       ,B.CUST_REGADDR          AS CUST_REGADDR 
       ,B.CUST_CITISHIP         AS CUST_CITISHIP 
       ,B.CUST_USED_NAME        AS CUST_USED_NAME 
       ,B.CUST_TTL              AS CUST_TTL 
       ,B.CUST_GROUP            AS CUST_GROUP 
       ,B.CUST_EVADATE          AS CUST_EVADATE 
       ,B.IS_STAFF              AS IS_STAFF 
       ,B.CUS_TYPE              AS CUS_TYPE 
       ,B.IS_GOODMAN            AS IS_GOODMAN 
       ,B.IS_TECH               AS IS_TECH 
       ,B.IS_GUDONG             AS IS_GUDONG 
       ,B.IS_SY                 AS IS_SY 
       ,B.IS_BUSSMA             AS IS_BUSSMA 
       ,B.CUST_FAMSTATUS        AS CUST_FAMSTATUS 
       ,B.CUST_HEALTH           AS CUST_HEALTH 
       ,B.CUST_LANGUAGE         AS CUST_LANGUAGE 
       ,B.CUST_RELIGION         AS CUST_RELIGION 
       ,B.CUST_POLIFACE         AS CUST_POLIFACE 
       ,B.CUST_RES_CNTY         AS CUST_RES_CNTY 
       ,B.CITY_COD              AS CITY_COD 
       ,B.CUST_LOCALYEAR        AS CUST_LOCALYEAR 
       ,B.ID_BLACKLIST          AS ID_BLACKLIST 
       ,B.CUST_ATT_FMDAT        AS CUST_ATT_FMDAT 
       ,B.CUST_ATT_DESC         AS CUST_ATT_DESC 
       ,B.FIN_RISK_ASS          AS FIN_RISK_ASS 
       ,B.RISK_APPETITE         AS RISK_APPETITE 
       ,B.EVA_DATE              AS EVA_DATE 
       ,B.FIN_WARN              AS FIN_WARN 
       ,B.CUST_CRT_SCT          AS CUST_CRT_SCT 
       ,B.CUST_HEIGHT           AS CUST_HEIGHT 
       ,B.CUST_WEIGHT           AS CUST_WEIGHT 
       ,B.CUST_BLOTYP           AS CUST_BLOTYP 
       ,B.TEMP_RESIDENCE        AS TEMP_RESIDENCE 
       ,B.BK_RELASHIP           AS BK_RELASHIP 
       ,B.CUST_EDU_LVL_COD      AS CUST_EDU_LVL_COD 
       ,B.GIHHEST_DEGREE        AS GIHHEST_DEGREE 
       ,COALESCE(D.ENDDATE, B.GRADUATION)                       AS GRADUATION 
       ,COALESCE(D.SCHOOL, B.GRADUATE_NAME)                       AS GRADUATE_NAME 
       ,B.CUST_OCCUP_COD        AS CUST_OCCUP_COD 
       ,B.CUST_OCCUP_COD1       AS CUST_OCCUP_COD1 
       ,B.OCCP_STATE            AS OCCP_STATE 
       ,B.WORK_STATE            AS WORK_STATE 
       ,B.CUST_POSN             AS CUST_POSN 
       ,B.CUST_TITL             AS CUST_TITL 
       ,B.TERM_OF_CON           AS TERM_OF_CON 
       ,B.WORK_YEAR             AS WORK_YEAR 
       ,B.CUST_WORK_YEAR        AS CUST_WORK_YEAR 
       ,B.CUST_WORK_UNIT_NAME   AS CUST_WORK_UNIT_NAME 
       ,B.CUST_UTELL            AS CUST_UTELL 
       ,B.CUST_UTYP             AS CUST_UTYP 
       ,B.DEPT                  AS DEPT 
       ,B.IS_CONTROLLER         AS IS_CONTROLLER 
       ,B.SOCIAL_DUTY           AS SOCIAL_DUTY 
       ,B.PER_DESCRIB           AS PER_DESCRIB 
       ,B.PER_RESUME            AS PER_RESUME 
       ,B.IS_FARMER_FLG         AS IS_FARMER_FLG 
       ,B.HOUHOLD_CLASS         AS HOUHOLD_CLASS 
       ,B.CUST_TEAMNAME         AS CUST_TEAMNAME 
       ,B.VILLAGE_NAME          AS VILLAGE_NAME 
       ,B.IS_VILLAGECADRE       AS IS_VILLAGECADRE 
       ,B.IS_MEDICARE           AS IS_MEDICARE 
       ,B.IS_POORISNO           AS IS_POORISNO 
       ,B.MAKUP                 AS MAKUP 
       ,B.INDUSTRYTYPE          AS INDUSTRYTYPE 
       ,COALESCE(E.MOSTBUSINESS, B.MOSTBUSINESS)                       AS MOSTBUSINESS 
       ,COALESCE(E.BUSINESSADD, B.BUSINESSADD)                       AS BUSINESSADD 
       ,COALESCE(E.LICENSENO, B.LICENSENO)                       AS LICENSENO 
       ,COALESCE(E.LICENSEDATE, B.LICENSEDATE)                       AS LICENSEDATE 
       ,COALESCE(E.TAXNO, B.TAXNO)                       AS TAXNO 
       ,COALESCE(E.TAXNO1, B.TAXNO1)                       AS TAXNO1 
       ,B.MAINPROORINCOME       AS MAINPROORINCOME 
       ,B.ADMIN_LVL             AS ADMIN_LVL 
       ,B.WORK_PERMIT           AS WORK_PERMIT 
       ,B.LINKMAN               AS LINKMAN 
       ,B.OFF_ADDR              AS OFF_ADDR 
       ,B.OFF_ZIP               AS OFF_ZIP 
       ,B.MICRO_BLOG            AS MICRO_BLOG 
       ,B.FAX                   AS FAX 
       ,B.MSN                   AS MSN 
       ,B.OTHER_CONTACT         AS OTHER_CONTACT 
       ,B.CUST_REG_ADDR2        AS CUST_REG_ADDR2 
       ,B.CI_ADDR               AS CI_ADDR 
       ,B.CUST_POSTCOD          AS CUST_POSTCOD 
       ,B.CUST_YEL_NO           AS CUST_YEL_NO 
       ,B.CUST_AREA_COD         AS CUST_AREA_COD 
       ,B.CUST_MBTELNO          AS CUST_MBTELNO 
       ,B.CUST_EMAIL            AS CUST_EMAIL 
       ,B.CUST_SUB_TEL          AS CUST_SUB_TEL 
       ,B.CUST_WEBSITE          AS CUST_WEBSITE 
       ,B.CUST_WORKADDR         AS CUST_WORKADDR 
       ,B.CUST_COMMADD          AS CUST_COMMADD 
       ,B.CUST_COMZIP           AS CUST_COMZIP 
       ,B.CUST_WORKZIP          AS CUST_WORKZIP 
       ,B.CUST_RELIGNLISM       AS CUST_RELIGNLISM 
       ,B.CUST_EFFSTATUS        AS CUST_EFFSTATUS 
       ,B.CUST_ARREA            AS CUST_ARREA 
       ,B.CUST_FAMADDR          AS CUST_FAMADDR 
       ,B.CUST_VILLAGENO        AS CUST_VILLAGENO 
       ,B.NET_ADDR              AS NET_ADDR 
       ,B.CUST_CRE_TYP          AS CUST_CRE_TYP 
       ,B.CUST_CER_NO           AS CUST_CER_NO 
       ,B.CUST_EXPD_DT          AS CUST_EXPD_DT 
       ,B.CUST_CHK_FLG          AS CUST_CHK_FLG 
       ,B.CUST_CER_STS          AS CUST_CER_STS 
       ,B.CUST_SONO             AS CUST_SONO 
       ,B.CUST_PECON_RESUR      AS CUST_PECON_RESUR 
       ,B.CUST_MN_INCO          AS CUST_MN_INCO 
       ,B.CUST_ANNUAL_INCOME    AS CUST_ANNUAL_INCOME 
       ,B.PER_INCURR_YCODE      AS PER_INCURR_YCODE 
       ,B.PER_IN_ANOUNT         AS PER_IN_ANOUNT 
       ,B.PER_INCURR_MCODE      AS PER_INCURR_MCODE 
       ,B.PER_INCURR_FAMCODE    AS PER_INCURR_FAMCODE 
       ,B.FAM_INCOMEACC         AS FAM_INCOMEACC 
       ,B.OTH_FAMINCOME         AS OTH_FAMINCOME 
       ,B.CUST_TOT_ASS          AS CUST_TOT_ASS 
       ,B.CUST_TOT_DEBT         AS CUST_TOT_DEBT 
       ,B.CUST_FAM_NUM          AS CUST_FAM_NUM 
       ,B.CUST_DEPEND_NO        AS CUST_DEPEND_NO 
       ,B.CUST_OT_INCO          AS CUST_OT_INCO 
       ,B.CUST_HOUSE_TYP        AS CUST_HOUSE_TYP 
       ,B.WAGES_ACCOUNT         AS WAGES_ACCOUNT 
       ,B.OPEN_BANK             AS OPEN_BANK 
       ,B.CRE_RECORD            AS CRE_RECORD 
       ,B.HAVE_YDTCARD          AS HAVE_YDTCARD 
       ,B.IS_LIFSUR             AS IS_LIFSUR 
       ,B.IS_ILLSUR             AS IS_ILLSUR 
       ,B.IS_ENDOSUR            AS IS_ENDOSUR 
       ,B.HAVE_CAR              AS HAVE_CAR 
       ,B.AVG_ASS               AS AVG_ASS 
       ,B.REMARK                AS REMARK 
       ,B.REC_UPSYS             AS REC_UPSYS 
       ,B.REC_UPMEC             AS REC_UPMEC 
       ,B.REC_UODATE            AS REC_UODATE 
       ,B.REC_UPMAN             AS REC_UPMAN 
       ,B.SOUR_SYS              AS SOUR_SYS 
       ,B.PLAT_DATE             AS PLAT_DATE 
       ,B.COMBIN_FLG            AS COMBIN_FLG 
       ,B.DATE_SOU              AS DATE_SOU 
       ,B.INPUTDATE             AS INPUTDATE 
       ,B.CUST_FANID            AS CUST_FANID 
       ,B.HZ_CERTYPE            AS HZ_CERTYPE 
       ,B.HZ_CERTID             AS HZ_CERTID 
       ,B.CUST_TEAMNO           AS CUST_TEAMNO 
       ,B.HZ_NAME               AS HZ_NAME 
       ,B.CUST_FAMNUM           AS CUST_FAMNUM 
       ,B.CUST_WEANAME          AS CUST_WEANAME 
       ,B.CUST_WEAVALUE         AS CUST_WEAVALUE 
       ,B.CONLANDAREA           AS CONLANDAREA 
       ,B.CONPOOLAREA           AS CONPOOLAREA 
       ,B.CUST_CHILDREN         AS CUST_CHILDREN 
       ,B.NUM_OF_CHILD          AS NUM_OF_CHILD 
       ,B.TOTINCO_OF_CH         AS TOTINCO_OF_CH 
       ,B.CUST_HOUSE            AS CUST_HOUSE 
       ,B.CUST_HOUSECOUNT       AS CUST_HOUSECOUNT 
       ,B.CUST_HOUSEAREA        AS CUST_HOUSEAREA 
       ,B.CUST_PRIVATECAR       AS CUST_PRIVATECAR 
       ,B.CAR_NUM_DESC          AS CAR_NUM_DESC 
       ,B.CUST_FAMILYSORT       AS CUST_FAMILYSORT 
       ,B.CUST_OTHMEG           AS CUST_OTHMEG 
       ,B.CUST_SUMJF            AS CUST_SUMJF 
       ,B.CUST_ZCJF             AS CUST_ZCJF 
       ,B.CUST_FZJF             AS CUST_FZJF 
       ,B.CUST_CONSUMEJF        AS CUST_CONSUMEJF 
       ,B.CUST_CHANNELJF        AS CUST_CHANNELJF 
       ,B.CUST_MIDBUSJF         AS CUST_MIDBUSJF 
       ,B.CRECARD_POINTS        AS CRECARD_POINTS 
       ,B.QQ                    AS QQ 
       ,B.MICRO_MSG             AS MICRO_MSG 
       ,V_DT                    AS ODS_ST_DATE 
       ,B.FAMILY_SAFE           AS FAMILY_SAFE 
       ,B.BAD_HABIT_FLAG        AS BAD_HABIT_FLAG 
       ,B.IS_MODIFY             AS IS_MODIFY 
       ,B.FR_ID                 AS FR_ID 
       ,B.LONGITUDE             AS LONGITUDE 
       ,B.LATITUDE              AS LATITUDE 
       ,B.GPS                   AS GPS 
   FROM OCRM_F_CI_PER_CUST_INFO B                              --对私客户信息表
  LEFT JOIN TMP_OCRM_F_CI_PER_CUST_INFO_02 H                  --临时表02存储信贷核心客户号关联关系
     ON H.FR_ID                 = B.FR_ID 
    AND B.CUST_ID               = H.ODS_CUST_ID 
  LEFT JOIN F_CI_XDXT_CUSTOMER_INFO A                         --客户信息表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUSTOMERID            = H.SOURCE_CUST_ID 
    AND A.ODS_ST_DATE           = V_DT 
  LEFT JOIN TMP_OCRM_F_CI_PER_CUST_INFO_04 D                  --临时表04存储学历履历表
     ON D.FR_ID                 = B.FR_ID 
    AND D.CUSTOMERID            = A.CUSTOMERID 
   LEFT JOIN F_CI_XDXT_IND_ENT_ADDITIONAL_INFO E               --个人工商户附加信息表
     ON E.CUSTOMERID            = A.CUSTOMERID 
    AND E.FR_ID                 = B.FR_ID """


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


#任务[12] 001-13::
V_STEP = V_STEP + 1

OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
sql = """
 SELECT DISTINCT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,COALESCE(B.CIFNAME, A.CUST_NAME)                       AS CUST_NAME 
       ,A.CUST_ENAME            AS CUST_ENAME 
       ,A.CUST_ENAME1           AS CUST_ENAME1 
       ,A.CUST_BIR              AS CUST_BIR 
       ,A.CUST_RESCNTY          AS CUST_RESCNTY 
       ,A.CUST_MRG              AS CUST_MRG 
       ,COALESCE(B.SEX, A.CUST_SEX)                       AS CUST_SEX 
       ,A.CUST_NATION           AS CUST_NATION 
       ,A.CUST_REGISTER         AS CUST_REGISTER 
       ,A.CUST_REGTYP           AS CUST_REGTYP 
       ,COALESCE(N.NATIVEPLACE, A.CUST_REGADDR)                       AS CUST_REGADDR 
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
       ,COALESCE(B.ADDR, A.CI_ADDR)                       AS CI_ADDR 
       ,COALESCE(B.ZIPCODE, A.CUST_POSTCOD)                       AS CUST_POSTCOD 
       ,COALESCE(B.PHONE, A.CUST_YEL_NO)                       AS CUST_YEL_NO 
       ,A.CUST_AREA_COD         AS CUST_AREA_COD 
       ,COALESCE(B.MOBILEPHONE, A.CUST_MBTELNO)                       AS CUST_MBTELNO 
       ,COALESCE(B.EMAIL, A.CUST_EMAIL)                       AS CUST_EMAIL 
       ,A.CUST_SUB_TEL          AS CUST_SUB_TEL 
       ,A.CUST_WEBSITE          AS CUST_WEBSITE 
       ,A.CUST_WORKADDR         AS CUST_WORKADDR 
       ,A.CUST_COMMADD          AS CUST_COMMADD 
       ,A.CUST_COMZIP           AS CUST_COMZIP 
       ,A.CUST_WORKZIP          AS CUST_WORKZIP 
       ,A.CUST_RELIGNLISM       AS CUST_RELIGNLISM 
       ,A.CUST_EFFSTATUS        AS CUST_EFFSTATUS 
       ,A.CUST_ARREA            AS CUST_ARREA 
       ,COALESCE(N.FAMILYADD, A.CUST_FAMADDR)                       AS CUST_FAMADDR 
       ,COALESCE(N.VILLAGENO, A.CUST_VILLAGENO)                       AS CUST_VILLAGENO 
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
       ,COALESCE(N.FARMILYID, A.CUST_FANID)                       AS CUST_FANID 
       ,COALESCE(N.CERTTYPE, A.HZ_CERTYPE)                       AS HZ_CERTYPE 
       ,COALESCE(N.CERTID, A.HZ_CERTID)                       AS HZ_CERTID 
       ,A.CUST_TEAMNO           AS CUST_TEAMNO 
       ,COALESCE(N.FULLNAME, A.HZ_NAME)                       AS HZ_NAME 
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
       ,COALESCE(N.REMARK, A.CUST_OTHMEG)                       AS CUST_OTHMEG 
       ,A.CUST_SUMJF            AS CUST_SUMJF 
       ,A.CUST_ZCJF             AS CUST_ZCJF 
       ,A.CUST_FZJF             AS CUST_FZJF 
       ,A.CUST_CONSUMEJF        AS CUST_CONSUMEJF 
       ,A.CUST_CHANNELJF        AS CUST_CHANNELJF 
       ,A.CUST_MIDBUSJF         AS CUST_MIDBUSJF 
       ,A.CRECARD_POINTS        AS CRECARD_POINTS 
       ,A.QQ                    AS QQ 
       ,A.MICRO_MSG             AS MICRO_MSG 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.FAMILY_SAFE           AS FAMILY_SAFE 
       ,A.BAD_HABIT_FLAG        AS BAD_HABIT_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_PER_CUST_INFO A                              --对私客户信息表
  LEFT JOIN OCRM_F_CI_SYS_RESOURCE C                          --系统来源中间表
     ON A.CUST_ID               = C.ODS_CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.ODS_SYS_ID            = 'IBK' 
    AND C.CERT_FLAG             = 'Y' 
  LEFT JOIN TMP_OCRM_F_CI_PER_CUST_INFO_06 B                  --对私客户信息表临时表05(网银系统)
     ON C.CERT_TYPE             = B.IDTYPE 
    AND C.CERT_NO               = B.IDNO 
    AND C.SOURCE_CUST_NAME      = B.CIFNAME 
    AND B.FR_ID                 = A.FR_ID
    AND B.RN = '1'
  LEFT JOIN TMP_OCRM_F_CI_PER_CUST_INFO_05 N                  --对私客户信息表临时表05(阳光信贷)
     ON A.FR_ID                 = N.FR_ID 
    AND A.CUST_ID               = N.ODS_CUST_ID 
    AND N.RN                    = '1' 
  WHERE COALESCE(B.CIFNAME, A.CUST_NAME) IS NOT NULL """


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

#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_PER_CUST_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_PER_CUST_INFO_BK/"+V_DT+".parquet")

