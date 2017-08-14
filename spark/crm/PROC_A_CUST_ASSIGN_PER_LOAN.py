#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CUST_ASSIGN_PER_LOAN').setMaster(sys.argv[2])
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

F_LN_XDXT_BUSINESS_CONTRACT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BUSINESS_CONTRACT/*')
F_LN_XDXT_BUSINESS_CONTRACT.registerTempTable("F_LN_XDXT_BUSINESS_CONTRACT")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
F_CI_XDXT_CUSTOMER_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_INFO/*')
F_CI_XDXT_CUSTOMER_INFO.registerTempTable("F_CI_XDXT_CUSTOMER_INFO")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
CUSTOMER_BELONG_TMP = sqlContext.read.parquet(hdfs+'/CUSTOMER_BELONG_TMP/*')
CUSTOMER_BELONG_TMP.registerTempTable("CUSTOMER_BELONG_TMP")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS ODS_CUST_ID 
       ,'1'                     AS CUST_TYPE 
       ,C.MANAGERUSERID         AS SOURCE_MGR_ID 
       ,''                    AS MGR_ID 
       ,''                    AS MGR_NAME 
       ,C.MANAGERORGID          AS ORG_ID 
       ,''                    AS ORG_NAME 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN F_LN_XDXT_BUSINESS_CONTRACT B                     --贷款合同信息表
     ON A.CONT_NO               = B.SERIALNO 
    AND B.FR_ID                 = A.FR_ID 
  INNER JOIN F_CI_XDXT_CUSTOMER_INFO C                         --信贷客户信息表
     ON B.CUSTOMERID            = C.CUSTOMERID 
    AND C.FR_ID                 = A.FR_ID 
  WHERE A.FR_ID                 = '104' 
    AND A.LN_APCL_FLG           = 'N' 
    AND A.BAL_RMB > 0 
    AND A.CUST_TYP              = '1' 
    AND(A.SUBJECTNO LIKE '1301%' 
             OR A.SUBJECTNO LIKE '1302%' 
             OR A.SUBJECTNO LIKE '1303%' 
             OR A.SUBJECTNO LIKE '1304%' 
             OR A.SUBJECTNO LIKE '1305%' 
             OR A.SUBJECTNO LIKE '1306%' 
             OR A.SUBJECTNO LIKE '1307%' 
             OR A.SUBJECTNO LIKE '1308%') 
  GROUP BY A.CUST_ID 
       ,C.MANAGERUSERID 
       ,C.MANAGERORGID 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
PER_CUST_CREDIT_104 = sqlContext.sql(sql)
PER_CUST_CREDIT_104.registerTempTable("PER_CUST_CREDIT_104")
dfn="PER_CUST_CREDIT_104/"+V_DT+".parquet"
PER_CUST_CREDIT_104.cache()
nrows = PER_CUST_CREDIT_104.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/PER_CUST_CREDIT_104/*.parquet")
PER_CUST_CREDIT_104.write.save(path=hdfs + '/' + dfn, mode='overwrite')
PER_CUST_CREDIT_104.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert PER_CUST_CREDIT_104 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

PER_CUST_CREDIT_104 = sqlContext.read.parquet(hdfs+'/PER_CUST_CREDIT_104/*')
PER_CUST_CREDIT_104.registerTempTable("PER_CUST_CREDIT_104")

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,B.ORG_ID                AS ORG_ID 
       ,'1' AS ORG_TYP
		,B.SOURCE_MGR_ID AS CUST_MNG
		,'1'     AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM CUSTOMER_BELONG_TMP A                                  --待分配客户临时表
  INNER JOIN PER_CUST_CREDIT_104 B                                 --贷款分配信息(104法人)
     ON A.ODS_CUSTOM_ID         = B.ODS_CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.FR_ID                 = '104' 
    AND A.CUST_STS              = '1' 
    AND A.CUST_TYP              = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C = sqlContext.sql(sql)
TEMP_C.registerTempTable("TEMP_C")
dfn="TEMP_C/"+V_DT+".parquet"
TEMP_C.cache()
nrows = TEMP_C.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_C/*.parquet")
TEMP_C.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TEMP_C.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,''                     AS ORG_ID 
       ,'1' AS ORG_TYP
		   ,'' AS CUST_MNG
		   ,'1'  AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM CUSTOMER_BELONG_TMP A 
    WHERE A.CUST_STS = '1' 
      AND A.CUST_TYP = '1' 
      AND EXISTS(SELECT 1 FROM ACRM_F_CI_ASSET_BUSI_PROTO B 
                  WHERE A.ODS_CUSTOM_ID = B.CUST_ID 
                    AND A.FR_ID=B.FR_ID
                    AND (B.SUBJECTNO LIKE '1301%'
	                   OR  B.SUBJECTNO LIKE '1302%'
		                 OR  B.SUBJECTNO LIKE '1303%'
                     OR  B.SUBJECTNO LIKE '1304%'
                     OR  B.SUBJECTNO LIKE '1305%'
                     OR  B.SUBJECTNO LIKE '1306%'
                     OR  B.SUBJECTNO LIKE '1307%'
		                 OR  B.SUBJECTNO LIKE '1308%')
                    AND B.BAL>0
                    AND B.LN_APCL_FLG = 'N'
                   )"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C = sqlContext.sql(sql)
TEMP_C.registerTempTable("TEMP_C")
dfn="TEMP_C/"+V_DT+".parquet"
TEMP_C.cache()
nrows = TEMP_C.count()
TEMP_C.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_C.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_C/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT A.FR_ID                 AS FR_ID 
       ,A.CUST_ID               AS CUST_ID 
       ,ROW_NUMBER () OVER (PARTITION BY A.FR_ID,A.CUST_ID ORDER BY A.CONT_AMT DESC, A.BEGIN_DATE ASC) AS RANK
       ,MANAGE_BRAN AS MANAGE_BRAN
       ,MANAGE_USERID AS MANAGE_USERID
  FROM ACRM_F_CI_ASSET_BUSI_PROTO A  --资产协议
 WHERE BAL > 0  
   AND LN_APCL_FLG = 'N' 
   AND (A.SUBJECTNO LIKE '1301%' 
     OR A.SUBJECTNO LIKE '1302%' 
     OR A.SUBJECTNO LIKE '1303%' 
     OR A.SUBJECTNO LIKE '1304%' 
     OR A.SUBJECTNO LIKE '1305%' 
     OR A.SUBJECTNO LIKE '1306%' 
     OR A.SUBJECTNO LIKE '1307%' 
     OR A.SUBJECTNO LIKE '1308%'
     )
        """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_TEMP_C_01 = sqlContext.sql(sql)
TMP_TEMP_C_01.registerTempTable("TMP_TEMP_C_01")
dfn="TMP_TEMP_C_01/"+V_DT+".parquet"
TMP_TEMP_C_01.cache()
nrows = TMP_TEMP_C_01.count()
TMP_TEMP_C_01.write.save(path=hdfs + '/' + dfn, mode='append')
TMP_TEMP_C_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_TEMP_C_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,B.MANAGE_BRAN           AS ORG_ID 
       ,A.ORG_TYP               AS ORG_TYP 
       ,B.MANAGE_USERID         AS CUST_MNG 
       ,A.MNG_TYP               AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_C A                                               --对私分配中间表C
  INNER JOIN TMP_TEMP_C_01 B                                   --对私分配中间表C临时表01
     ON A.ODS_CUSTOM_ID         = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.RANK                  = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C_INNTMP1 = sqlContext.sql(sql)
TEMP_C_INNTMP1.registerTempTable("TEMP_C_INNTMP1")

TEMP_C = sqlContext.read.parquet(hdfs+'/TEMP_C/*')
TEMP_C.registerTempTable("TEMP_C")
sql = """
 SELECT DST.ODS_CUSTOM_ID                                       --客户号:src.ODS_CUSTOM_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_TYP                                            --机构主协办:src.ORG_TYP
       ,DST.CUST_MNG                                           --客户经理:src.CUST_MNG
       ,DST.MNG_TYP                                            --经理主协办:src.MNG_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM TEMP_C DST 
   LEFT JOIN TEMP_C_INNTMP1 SRC 
     ON SRC.ODS_CUSTOM_ID       = DST.ODS_CUSTOM_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.ODS_CUSTOM_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C_INNTMP2 = sqlContext.sql(sql)
dfn="TEMP_C/"+V_DT+".parquet"
TEMP_C_INNTMP2=TEMP_C_INNTMP2=TEMP_C_INNTMP2.unionAll(TEMP_C_INNTMP1)
TEMP_C_INNTMP1.cache()
TEMP_C_INNTMP2.cache()
nrowsi = TEMP_C_INNTMP1.count()
nrowsa = TEMP_C_INNTMP2.count()
TEMP_C_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
TEMP_C_INNTMP1.unpersist()
TEMP_C_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/TEMP_C/"+V_DT_LD+".parquet /"+dbname+"/TEMP_C_BK/")

#任务[11] 001-06::
V_STEP = V_STEP + 1

TEMP_C = sqlContext.read.parquet(hdfs+'/TEMP_C/*')
TEMP_C.registerTempTable("TEMP_C")

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,B.MANAGE_BRAN           AS ORG_ID 
       ,CASE WHEN B.MANAGE_BRAN = A.ORG_ID THEN '1' ELSE '2' END AS ORG_TYP 
       ,CASE WHEN A.CUST_MNG IS NOT NULL AND TRIM(A.CUST_MNG) <> '' THEN B.MANAGE_USERID ELSE NULL END AS CUST_MNG 
       ,'2'                     AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_C A                                               --对私分配中间表C
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO B                      --资产协议
     ON A.ODS_CUSTOM_ID         = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.BAL > 0 
    AND B.LN_APCL_FLG           = 'N' 
    AND B.MANAGE_USERID <> A.CUST_MNG 
    AND(B.SUBJECTNO LIKE '1301%' 
             OR B.SUBJECTNO LIKE '1302%' 
             OR B.SUBJECTNO LIKE '1303%' 
             OR B.SUBJECTNO LIKE '1304%' 
             OR B.SUBJECTNO LIKE '1305%' 
             OR B.SUBJECTNO LIKE '1306%' 
             OR B.SUBJECTNO LIKE '1307%' 
             OR B.SUBJECTNO LIKE '1308%') 
  WHERE A.ORG_ID IS NOT NULL 
    AND A.CUST_MNG IS NOT NULL 
    AND A.ORG_TYP               = '1' 
    AND A.MNG_TYP               = '1' 
  GROUP BY A.FR_ID 
       ,A.ODS_CUSTOM_ID 
       ,A.CUST_NAME 
       ,A.CERT_TYP 
       ,A.CERT_NO 
       ,B.MANAGE_BRAN 
       ,A.ORG_ID 
       ,A.CUST_MNG
       ,B.MANAGE_USERID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C = sqlContext.sql(sql)
TEMP_C.registerTempTable("TEMP_C")
dfn="TEMP_C/"+V_DT+".parquet"
TEMP_C.cache()
nrows = TEMP_C.count()
TEMP_C.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_C.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-07::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,B.ORG_ID                AS ORG_ID 
       ,CASE WHEN B.ORG_ID = A.ORG_ID THEN '1' ELSE '2' END                     AS ORG_TYP 
       ,B.CONNTR_NO             AS CUST_MNG 
       ,'2'                     AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_C A                                               --对私分配中间表C
  INNER JOIN ACRM_F_DP_SAVE_INFO B                             --负债协议
     ON A.ODS_CUSTOM_ID         = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.BAL_RMB > 0 
    AND B.ACCT_STATUS           = '01' 
    AND B.CONNTR_NO <> A.CUST_MNG 
  WHERE A.ORG_ID IS NOT NULL 
    AND A.CUST_MNG IS NOT NULL 
    AND A.ORG_TYP               = '1' 
    AND A.MNG_TYP               = '1' 
  GROUP BY A.FR_ID 
       ,A.ODS_CUSTOM_ID 
       ,A.CUST_NAME 
       ,A.CERT_TYP 
       ,A.CERT_NO 
       ,B.ORG_ID 
       ,A.ORG_ID 
       ,B.CONNTR_NO """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C = sqlContext.sql(sql)
TEMP_C.registerTempTable("TEMP_C")
dfn="TEMP_C/"+V_DT+".parquet"
TEMP_C.cache()
nrows = TEMP_C.count()
TEMP_C.write.save(path=hdfs + '/' + dfn, mode='append')
TEMP_C.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-08::
V_STEP = V_STEP + 1

TEMP_C = sqlContext.read.parquet(hdfs+'/TEMP_C/*')
TEMP_C.registerTempTable("TEMP_C")

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,B.ORG_ID                AS ORG_ID 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.CUST_MNG              AS CUST_MNG 
       ,A.MNG_TYP               AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_C A                                               --对私分配中间表C
  INNER JOIN(
         SELECT DISTINCT ORG_ID 
               ,FR_ID 
           FROM ADMIN_AUTH_ORG 
          WHERE UP_ORG_ID               = '320000000') B       --机构表
     ON A.FR_ID                 = B.FR_ID 
  WHERE A.ORG_ID IS NULL 
    AND A.CUST_MNG IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C_INNTMP1 = sqlContext.sql(sql)
TEMP_C_INNTMP1.registerTempTable("TEMP_C_INNTMP1")

TEMP_C = sqlContext.read.parquet(hdfs+'/TEMP_C/*')
TEMP_C.registerTempTable("TEMP_C")
sql = """
 SELECT DST.ODS_CUSTOM_ID                                       --客户号:src.ODS_CUSTOM_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.ORG_TYP                                            --机构主协办:src.ORG_TYP
       ,DST.CUST_MNG                                           --客户经理:src.CUST_MNG
       ,DST.MNG_TYP                                            --经理主协办:src.MNG_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM TEMP_C DST 
   LEFT JOIN TEMP_C_INNTMP1 SRC 
     ON SRC.ODS_CUSTOM_ID       = DST.ODS_CUSTOM_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.ORG_TYP             = DST.ORG_TYP 
    AND SRC.MNG_TYP             = DST.MNG_TYP 
    AND SRC.CUST_MNG            = DST.CUST_MNG 
  WHERE SRC.ODS_CUSTOM_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_C_INNTMP2 = sqlContext.sql(sql)
dfn="TEMP_C/"+V_DT+".parquet"
TEMP_C_INNTMP2=TEMP_C_INNTMP2.unionAll(TEMP_C_INNTMP1)
TEMP_C_INNTMP1.cache()
TEMP_C_INNTMP2.cache()
nrowsi = TEMP_C_INNTMP1.count()
nrowsa = TEMP_C_INNTMP2.count()
TEMP_C_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
TEMP_C_INNTMP1.unpersist()
TEMP_C_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_C lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/TEMP_C/"+V_DT_LD+".parquet /"+dbname+"/TEMP_C_BK/")

#任务[21] 001-09::
V_STEP = V_STEP + 1

TEMP_C = sqlContext.read.parquet(hdfs+'/TEMP_C/*')
TEMP_C.registerTempTable("TEMP_C")

sql = """
 SELECT DISTINCT A.ODS_CUSTOM_ID         AS ODS_CUSTOM_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.CUST_MNG              AS CUST_MNG 
       ,A.MNG_TYP               AS MNG_TYP 
       ,A.FR_ID                 AS FR_ID 
   FROM TEMP_C A                                               --对私分配中间表C
  WHERE A.ORG_ID IS NOT NULL  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TEMP_A = sqlContext.sql(sql)
TEMP_A.registerTempTable("TEMP_A")
dfn="TEMP_A/"+V_DT+"_01.parquet"
TEMP_A.cache()
nrows = TEMP_A.count()

#清除历史数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TEMP_A/*.parquet")
#写入
TEMP_A.write.save(path=hdfs + '/' + dfn, mode='overwrite')

TEMP_A.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TEMP_A lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
