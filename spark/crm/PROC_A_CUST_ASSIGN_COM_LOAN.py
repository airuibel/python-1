#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CUST_ASSIGN_COM_LOAN').setMaster(sys.argv[2])
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
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")
CUSTOMER_BELONG_TMP = sqlContext.read.parquet(hdfs+'/CUSTOMER_BELONG_TMP/*')
CUSTOMER_BELONG_TMP.registerTempTable("CUSTOMER_BELONG_TMP")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS ODS_CUST_ID 
       ,'2'                     AS CUST_TYPE 
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
    AND A.CUST_TYP              = '2' 
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
COM_CUST_CREDIT_104 = sqlContext.sql(sql)
COM_CUST_CREDIT_104.registerTempTable("COM_CUST_CREDIT_104")
dfn="COM_CUST_CREDIT_104/"+V_DT+".parquet"
COM_CUST_CREDIT_104.cache()
nrows = COM_CUST_CREDIT_104.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/COM_CUST_CREDIT_104/*.parquet")
COM_CUST_CREDIT_104.write.save(path=hdfs + '/' + dfn, mode='overwrite')
COM_CUST_CREDIT_104.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert COM_CUST_CREDIT_104 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

COM_CUST_CREDIT_104 = sqlContext.read.parquet(hdfs+'/COM_CUST_CREDIT_104/*')
COM_CUST_CREDIT_104.registerTempTable("COM_CUST_CREDIT_104")

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,''                      AS CUST_STS 
       ,B.ORG_ID                AS BRANCH 
       ,'1'                     AS TYPE1 
       ,B.SOURCE_MGR_ID         AS MANGER_NO 
       ,'1'                     AS TYPE2 
       ,A.FR_ID                 AS FR_ID 
   FROM CUSTOMER_BELONG_TMP A                                  --待分配客户临时表
  INNER JOIN COM_CUST_CREDIT_104 B                                 --贷款分配信息(104法人)
     ON A.ODS_CUSTOM_ID         = B.ODS_CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.CUST_STS              = '1' 
    AND A.CUST_TYP              = '2' 
    AND A.FR_ID                 = '104' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
UNIT_CUST_A = sqlContext.sql(sql)
UNIT_CUST_A.registerTempTable("UNIT_CUST_A")
dfn="UNIT_CUST_A/"+V_DT+".parquet"
UNIT_CUST_A.cache()
nrows = UNIT_CUST_A.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/UNIT_CUST_A/*.parquet")
UNIT_CUST_A.write.save(path=hdfs + '/' + dfn, mode='overwrite')
UNIT_CUST_A.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert UNIT_CUST_A lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUSTOM_ID         AS ODS_CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,''                    AS CUST_STS 
       ,''                    AS BRANCH 
       ,'1'                     AS TYPE1 
       ,''                    AS MANGER_NO 
       ,'1'                     AS TYPE2 
       ,A.FR_ID                 AS FR_ID 
   FROM CUSTOMER_BELONG_TMP A                                  --待分配客户临时表
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO B                      --资产协议
     ON A.ODS_CUSTOM_ID         = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.BAL > 0 
    AND B.LN_APCL_FLG           = 'N' 
    AND(B.SUBJECTNO LIKE '1301%' 
             OR B.SUBJECTNO LIKE '1302%' 
             OR B.SUBJECTNO LIKE '1303%' 
             OR B.SUBJECTNO LIKE '1304%' 
             OR B.SUBJECTNO LIKE '1305%' 
             OR B.SUBJECTNO LIKE '1306%' 
             OR B.SUBJECTNO LIKE '1307%' 
             OR B.SUBJECTNO LIKE '1308%') 
  WHERE A.CUST_TYP              = '2' 
    AND A.CUST_STS              = '1' 
    AND A.FR_ID <> '104' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
UNIT_CUST_A = sqlContext.sql(sql)
UNIT_CUST_A.registerTempTable("UNIT_CUST_A")
dfn="UNIT_CUST_A/"+V_DT+".parquet"
UNIT_CUST_A.cache()
nrows = UNIT_CUST_A.count()
UNIT_CUST_A.write.save(path=hdfs + '/' + dfn, mode='append')
UNIT_CUST_A.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert UNIT_CUST_A lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUST_ID           AS ODS_CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.CUST_STS              AS CUST_STS 
       ,B.MANAGE_BRAN           AS BRANCH 
       ,A.TYPE1                 AS TYPE1 
       ,B.MANAGE_USERID         AS MANGER_NO 
       ,A.TYPE2                 AS TYPE2 
       ,A.FR_ID                 AS FR_ID 
   FROM UNIT_CUST_A A                                          --对公分配中间表A
  INNER JOIN (SELECT FR_ID,CUST_ID,MANAGE_BRAN,MANAGE_USERID,
                     ROW_NUMBER() OVER(PARTITION BY CUST_ID,FR_ID,MANAGE_BRAN,MANAGE_USERID ORDER BY CONT_AMT DESC,BEGIN_DATE ASC) RANK
                FROM ACRM_F_CI_ASSET_BUSI_PROTO
               WHERE BAL>0
                 AND LN_APCL_FLG = 'N'
                 AND (SUBJECTNO LIKE '1301%'
	                OR  SUBJECTNO LIKE '1302%'
		              OR  SUBJECTNO LIKE '1303%'
                  OR  SUBJECTNO LIKE '1304%'
                  OR  SUBJECTNO LIKE '1305%'
                  OR  SUBJECTNO LIKE '1306%'
                  OR  SUBJECTNO LIKE '1307%'
		              OR  SUBJECTNO LIKE '1308%')) B
     ON A.ODS_CUST_ID         = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.RANK                  = '1' 
  WHERE A.MANGER_NO IS NULL  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
UNIT_CUST_A_INNTMP1 = sqlContext.sql(sql)
UNIT_CUST_A_INNTMP1.registerTempTable("UNIT_CUST_A_INNTMP1")

UNIT_CUST_A = sqlContext.read.parquet(hdfs+'/UNIT_CUST_A/*')
UNIT_CUST_A.registerTempTable("UNIT_CUST_A")
sql = """
 SELECT DST.ODS_CUST_ID                                         --客户号:src.ODS_CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.CUST_STS                                           --客户状态:src.CUST_STS
       ,DST.BRANCH                                             --机构:src.BRANCH
       ,DST.TYPE1                                              --类型1:src.TYPE1
       ,DST.MANGER_NO                                          --经理号:src.MANGER_NO
       ,DST.TYPE2                                              --类型2:src.TYPE2
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM UNIT_CUST_A DST 
   LEFT JOIN UNIT_CUST_A_INNTMP1 SRC 
     ON SRC.ODS_CUST_ID         = DST.ODS_CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.ODS_CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
UNIT_CUST_A_INNTMP2 = sqlContext.sql(sql)
dfn="UNIT_CUST_A/"+V_DT+".parquet"
UNIT_CUST_A_INNTMP2=UNIT_CUST_A_INNTMP2.unionAll(UNIT_CUST_A_INNTMP1)
UNIT_CUST_A_INNTMP1.cache()
UNIT_CUST_A_INNTMP2.cache()
nrowsi = UNIT_CUST_A_INNTMP1.count()
nrowsa = UNIT_CUST_A_INNTMP2.count()
UNIT_CUST_A_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
UNIT_CUST_A_INNTMP1.unpersist()
UNIT_CUST_A_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert UNIT_CUST_A lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/UNIT_CUST_A/"+V_DT_LD+".parquet /"+dbname+"/UNIT_CUST_A_BK/")

#任务[12] 001-05::
V_STEP = V_STEP + 1

UNIT_CUST_A = sqlContext.read.parquet(hdfs+'/UNIT_CUST_A/*')
UNIT_CUST_A.registerTempTable("UNIT_CUST_A")

sql = """
 SELECT C.ODS_CUST_ID           AS ODS_CUST_ID 
       ,C.CUST_NAME             AS CUST_NAME 
       ,C.CERT_TYP              AS CERT_TYP 
       ,C.CERT_NO               AS CERT_NO 
       ,C.CUST_STS              AS CUST_STS 
       ,B.ORG_ID                AS BRANCH 
       ,C.TYPE1                 AS TYPE1 
       ,B.CONNTR_NO             AS MANGER_NO 
       ,C.TYPE2                 AS TYPE2 
       ,C.FR_ID                 AS FR_ID 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户信息表
  INNER JOIN ACRM_F_DP_SAVE_INFO B                             --负债协议
     ON A.BASIC_ACCT            = B.ODS_ACCT_NO 
    AND A.FR_ID                 = B.FR_ID 
    AND B.BAL_RMB > 0 
    AND B.ACCT_STATUS           = '01' 
  INNER JOIN UNIT_CUST_A C                                     --对公分配中间表A
     ON A.CUST_ID               = C.ODS_CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.BRANCH IS NULL 
  WHERE A.IF_BASIC              = 'Y' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
UNIT_CUST_A_INNTMP1 = sqlContext.sql(sql)
UNIT_CUST_A_INNTMP1.registerTempTable("UNIT_CUST_A_INNTMP1")

UNIT_CUST_A = sqlContext.read.parquet(hdfs+'/UNIT_CUST_A/*')
UNIT_CUST_A.registerTempTable("UNIT_CUST_A")
sql = """
 SELECT DST.ODS_CUST_ID                                         --客户号:src.ODS_CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.CUST_STS                                           --客户状态:src.CUST_STS
       ,DST.BRANCH                                             --机构:src.BRANCH
       ,DST.TYPE1                                              --类型1:src.TYPE1
       ,DST.MANGER_NO                                          --经理号:src.MANGER_NO
       ,DST.TYPE2                                              --类型2:src.TYPE2
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM UNIT_CUST_A DST 
   LEFT JOIN UNIT_CUST_A_INNTMP1 SRC 
     ON SRC.ODS_CUST_ID         = DST.ODS_CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.ODS_CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
UNIT_CUST_A_INNTMP2 = sqlContext.sql(sql)
dfn="UNIT_CUST_A/"+V_DT+".parquet"
UNIT_CUST_A_INNTMP2=UNIT_CUST_A_INNTMP2.unionAll(UNIT_CUST_A_INNTMP1)
UNIT_CUST_A_INNTMP1.cache()
UNIT_CUST_A_INNTMP2.cache()
nrowsi = UNIT_CUST_A_INNTMP1.count()
nrowsa = UNIT_CUST_A_INNTMP2.count()
UNIT_CUST_A_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
UNIT_CUST_A_INNTMP1.unpersist()
UNIT_CUST_A_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert UNIT_CUST_A lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/UNIT_CUST_A/"+V_DT_LD+".parquet /"+dbname+"/UNIT_CUST_A_BK/")

#任务[11] 001-06::
V_STEP = V_STEP + 1
UNIT_CUST_A = sqlContext.read.parquet(hdfs+'/UNIT_CUST_A/*')
UNIT_CUST_A.registerTempTable("UNIT_CUST_A")

sql = """
 SELECT A.ODS_CUST_ID           AS ODS_CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.CUST_STS              AS CUST_STS 
       ,B.MANAGE_BRAN           AS BRANCH 
       ,CASE WHEN B.MANAGE_BRAN           = A.BRANCH THEN '1' ELSE '2' END                     AS TYPE1 
       ,CASE WHEN A.MANGER_NO IS  NOT NULL AND TRIM(A.MANGER_NO) <> '' THEN B.MANAGE_USERID ELSE NULL END AS MANGER_NO 
       ,'2'                     AS TYPE2 
       ,A.FR_ID                 AS FR_ID 
   FROM UNIT_CUST_A A                                          --对公分配中间表A
  INNER JOIN ACRM_F_CI_ASSET_BUSI_PROTO B                      --资产协议
     ON A.ODS_CUST_ID           = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.BAL > 0 
    AND B.LN_APCL_FLG           = 'N' 
    AND B.MANAGE_USERID <> A.MANGER_NO 
    AND(B.SUBJECTNO LIKE '1301%' 
             OR B.SUBJECTNO LIKE '1302%' 
             OR B.SUBJECTNO LIKE '1303%' 
             OR B.SUBJECTNO LIKE '1304%' 
             OR B.SUBJECTNO LIKE '1305%' 
             OR B.SUBJECTNO LIKE '1306%' 
             OR B.SUBJECTNO LIKE '1307%' 
             OR B.SUBJECTNO LIKE '1308%') 
  WHERE A.BRANCH IS 
    NOT NULL 
    AND A.MANGER_NO IS 
    NOT NULL 
    AND A.TYPE1                 = '1' 
    AND A.TYPE2                 = '1' 
  GROUP BY A.ODS_CUST_ID 
       ,A.FR_ID 
       ,A.CUST_NAME 
       ,A.CERT_TYP 
       ,A.CERT_NO 
       ,B.MANAGE_BRAN 
       ,B.MANAGE_USERID
       ,A.BRANCH 
       ,A.MANGER_NO
       ,A.CUST_STS """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
UNIT_CUST_A = sqlContext.sql(sql)
UNIT_CUST_A.registerTempTable("UNIT_CUST_A")
dfn="UNIT_CUST_A/"+V_DT+".parquet"
UNIT_CUST_A.cache()
nrows = UNIT_CUST_A.count()
UNIT_CUST_A.write.save(path=hdfs + '/' + dfn, mode='append')
UNIT_CUST_A.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert UNIT_CUST_A lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-07::
V_STEP = V_STEP + 1

UNIT_CUST_A = sqlContext.read.parquet(hdfs+'/UNIT_CUST_A/*')
UNIT_CUST_A.registerTempTable("UNIT_CUST_A")

sql = """
 SELECT A.ODS_CUST_ID           AS ODS_CUST_ID 
       ,B.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.CUST_STS              AS CUST_STS 
       ,B.ORG_ID                AS BRANCH 
       ,CASE WHEN B.ORG_ID                = A.BRANCH THEN '1' ELSE '2' END                     AS TYPE1 
       ,B.CONNTR_NO             AS MANGER_NO 
       ,'2' AS TYPE2
		,A.FR_ID AS FR_ID 
from UNIT_CUST_A A    --对公分配中间表A
		INNER JOIN
		ACRM_F_DP_SAVE_INFO B    --负债协议
		on A.ODS_CUST_ID = B.CUST_ID AND A.FR_ID = B.FR_ID AND B.BAL_RMB>0 AND B.ACCT_STATUS='01' AND B.CONNTR_NO<> A.MANGER_NO  
where A.BRANCH IS NOT NULL AND A.MANGER_NO IS NOT NULL AND A.TYPE1 = '1' AND A.TYPE2 = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
UNIT_CUST_A = sqlContext.sql(sql)
UNIT_CUST_A.registerTempTable("UNIT_CUST_A")
dfn="UNIT_CUST_A/"+V_DT+".parquet"
UNIT_CUST_A.cache()
nrows = UNIT_CUST_A.count()
UNIT_CUST_A.write.save(path=hdfs + '/' + dfn, mode='append')
UNIT_CUST_A.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert UNIT_CUST_A lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-08::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ODS_CUST_ID           AS ODS_CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.CUST_STS              AS CUST_STS 
       ,B.ORG_ID                AS BRANCH 
       ,A.TYPE1                 AS TYPE1 
       ,A.MANGER_NO             AS MANGER_NO 
       ,A.TYPE2                 AS TYPE2 
       ,A.FR_ID                 AS FR_ID 
   FROM UNIT_CUST_A A                                          --对公分配中间表A
  INNER JOIN(
         SELECT DISTINCT ORG_ID 
               ,FR_ID 
           FROM ADMIN_AUTH_ORG 
          WHERE UP_ORG_ID               = '320000000') B       --机构表
     ON A.FR_ID                 = B.FR_ID 
  WHERE A.BRANCH IS NULL 
    AND A.MANGER_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
UNIT_CUST_A_INNTMP1 = sqlContext.sql(sql)
UNIT_CUST_A_INNTMP1.registerTempTable("UNIT_CUST_A_INNTMP1")

UNIT_CUST_A = sqlContext.read.parquet(hdfs+'/UNIT_CUST_A/*')
UNIT_CUST_A.registerTempTable("UNIT_CUST_A")
sql = """
 SELECT DST.ODS_CUST_ID                                         --客户号:src.ODS_CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.CUST_STS                                           --客户状态:src.CUST_STS
       ,DST.BRANCH                                             --机构:src.BRANCH
       ,DST.TYPE1                                              --类型1:src.TYPE1
       ,DST.MANGER_NO                                          --经理号:src.MANGER_NO
       ,DST.TYPE2                                              --类型2:src.TYPE2
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM UNIT_CUST_A DST 
   LEFT JOIN UNIT_CUST_A_INNTMP1 SRC 
     ON SRC.ODS_CUST_ID         = DST.ODS_CUST_ID 
    AND SRC.TYPE1               = DST.TYPE1 
    AND SRC.TYPE2               = DST.TYPE2 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.ODS_CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
UNIT_CUST_A_INNTMP2 = sqlContext.sql(sql)
dfn="UNIT_CUST_A/"+V_DT+".parquet"
UNIT_CUST_A_INNTMP2=UNIT_CUST_A_INNTMP2.unionAll(UNIT_CUST_A_INNTMP1)
UNIT_CUST_A_INNTMP1.cache()
UNIT_CUST_A_INNTMP2.cache()
nrowsi = UNIT_CUST_A_INNTMP1.count()
nrowsa = UNIT_CUST_A_INNTMP2.count()
UNIT_CUST_A_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
UNIT_CUST_A_INNTMP1.unpersist()
UNIT_CUST_A_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert UNIT_CUST_A lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/UNIT_CUST_A/"+V_DT_LD+".parquet /"+dbname+"/UNIT_CUST_A_BK/")

#任务[21] 001-09::
V_STEP = V_STEP + 1

UNIT_CUST_A = sqlContext.read.parquet(hdfs+'/UNIT_CUST_A/*')
UNIT_CUST_A.registerTempTable("UNIT_CUST_A")

sql = """
 SELECT DISTINCT ODS_CUST_ID             AS ODS_CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,CERT_TYP                AS CERT_TYP 
       ,CERT_NO                 AS CERT_NO 
       ,A.CUST_STS              AS CUST_STS 
       ,BRANCH                  AS BRANCH 
       ,TYPE1                   AS TYPE1 
       ,CASE WHEN A.MANGER_NO IS NOT NULL THEN A.TYPE2 ELSE NULL END                     AS MANGER_NO 
       ,''                      AS TYPE2 
       ,FR_ID                   AS FR_ID 
   FROM UNIT_CUST_A A                                          --对公分配中间表A
  WHERE A.BRANCH IS NOT NULL  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
UNIT_CUST_B = sqlContext.sql(sql)
UNIT_CUST_B.registerTempTable("UNIT_CUST_B")
dfn="UNIT_CUST_B/"+V_DT+"_01.parquet"
UNIT_CUST_B.cache()
nrows = UNIT_CUST_B.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/UNIT_CUST_B/*.parquet")
UNIT_CUST_B.write.save(path=hdfs + '/' + dfn, mode='overwrite')
UNIT_CUST_B.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert UNIT_CUST_B lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
