#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUST_SIGN').setMaster(sys.argv[2])
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

#清除数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_SIGN/*.parquet")
#恢复数据到今日数据文件
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_CUST_SIGN_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet")

F_CSP_WIRESIGNINFOHIST = sqlContext.read.parquet(hdfs+'/F_CSP_WIRESIGNINFOHIST/*')
F_CSP_WIRESIGNINFOHIST.registerTempTable("F_CSP_WIRESIGNINFOHIST")
F_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNACN/*')
F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")
F_CSP_PERNETBANKCIFINFOHIST = sqlContext.read.parquet(hdfs+'/F_CSP_PERNETBANKCIFINFOHIST/*')
F_CSP_PERNETBANKCIFINFOHIST.registerTempTable("F_CSP_PERNETBANKCIFINFOHIST")
F_CI_AFA_CUSTINFO = sqlContext.read.parquet(hdfs+'/F_CI_AFA_CUSTINFO/*')
F_CI_AFA_CUSTINFO.registerTempTable("F_CI_AFA_CUSTINFO")
F_CSP_SMSSIGNINFOHIST = sqlContext.read.parquet(hdfs+'/F_CSP_SMSSIGNINFOHIST/*')
F_CSP_SMSSIGNINFOHIST.registerTempTable("F_CSP_SMSSIGNINFOHIST")
F_NI_AFA_ELEC_DKGX = sqlContext.read.parquet(hdfs+'/F_NI_AFA_ELEC_DKGX/*')
F_NI_AFA_ELEC_DKGX.registerTempTable("F_NI_AFA_ELEC_DKGX")
OCRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_DP_CARD_INFO/*')
OCRM_F_DP_CARD_INFO.registerTempTable("OCRM_F_DP_CARD_INFO")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
F_CSP_MOBILEBANKCIFINFOHIST = sqlContext.read.parquet(hdfs+'/F_CSP_MOBILEBANKCIFINFOHIST/*')
F_CSP_MOBILEBANKCIFINFOHIST.registerTempTable("F_CSP_MOBILEBANKCIFINFOHIST")
F_CSP_ENTBANKCIFINFOHIST = sqlContext.read.parquet(hdfs+'/F_CSP_ENTBANKCIFINFOHIST/*')
F_CSP_ENTBANKCIFINFOHIST.registerTempTable("F_CSP_ENTBANKCIFINFOHIST")
F_CSP_TVSIGNINFOHIST = sqlContext.read.parquet(hdfs+'/F_CSP_TVSIGNINFOHIST/*')
F_CSP_TVSIGNINFOHIST.registerTempTable("F_CSP_TVSIGNINFOHIST")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT COALESCE(B.SA_CUST_NO, C.CR_CUST_NO)                       AS CUST_ID 
       ,CASE WHEN A.ZT='0' THEN '1' ELSE '0' END AS STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM F_NI_AFA_ELEC_DKGX A                                   --省级电费代扣关系表
   LEFT JOIN F_DP_CBOD_SAACNACN B                              --活存主档
     ON A.YHZH                  = B.SA_ACCT_NO 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN OCRM_F_DP_CARD_INFO C                             --卡档
     ON A.YHZH                  = C.CR_CRD_NO 
    AND C.FR_ID                 = A.FR_ID 
  WHERE A.SYSID                 = '800012' 
    AND A.ODS_ST_DATE           = V_DT 
  GROUP BY B.SA_CUST_NO 
       ,C.CR_CUST_NO 
       ,A.ZT 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_CUST_SIGN_01 = sqlContext.sql(sql)
TMP_OCRM_F_CI_CUST_SIGN_01.registerTempTable("TMP_OCRM_F_CI_CUST_SIGN_01")
dfn="TMP_OCRM_F_CI_CUST_SIGN_01/"+V_DT+".parquet"
TMP_OCRM_F_CI_CUST_SIGN_01.cache()
nrows = TMP_OCRM_F_CI_CUST_SIGN_01.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_CUST_SIGN_01/*.parquet")
TMP_OCRM_F_CI_CUST_SIGN_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_CUST_SIGN_01.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_CUST_SIGN_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT DISTINCT COALESCE(B.SA_CUST_NO, C.CR_CUST_NO)                       AS CUST_ID 
       ,CASE WHEN SYSID = '800235' THEN 'Water' 
             WHEN SYSID = '800037' THEN 'Gas'
             END AS TYPE 
       ,CASE WHEN A.SIGNSTATE = '0' THEN '1' ELSE '0' END          AS STATE 
       ,A.FR_ID                 AS FR_ID 
   FROM F_CI_AFA_CUSTINFO A                                    --代理单位客户签约信息表
   LEFT JOIN F_DP_CBOD_SAACNACN B                              --活存主档
     ON A.ACCOUNT               = B.SA_ACCT_NO 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN OCRM_F_DP_CARD_INFO C                             --卡档
     ON A.ACCOUNT               = C.CR_CRD_NO 
    AND C.FR_ID                 = A.FR_ID 
  WHERE A.SYSID IN('800235', '800037') 
    AND A.ODS_ST_DATE           = V_DT 
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_OCRM_F_CI_CUST_SIGN_02 = sqlContext.sql(sql)
TMP_OCRM_F_CI_CUST_SIGN_02.registerTempTable("TMP_OCRM_F_CI_CUST_SIGN_02")
dfn="TMP_OCRM_F_CI_CUST_SIGN_02/"+V_DT+".parquet"
TMP_OCRM_F_CI_CUST_SIGN_02.cache()
nrows = TMP_OCRM_F_CI_CUST_SIGN_02.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_OCRM_F_CI_CUST_SIGN_02/*.parquet")
TMP_OCRM_F_CI_CUST_SIGN_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_OCRM_F_CI_CUST_SIGN_02.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_OCRM_F_CI_CUST_SIGN_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


#任务[12] 001-03::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")
TMP_OCRM_F_CI_CUST_SIGN_01 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_CUST_SIGN_01/*')
TMP_OCRM_F_CI_CUST_SIGN_01.registerTempTable("TMP_OCRM_F_CI_CUST_SIGN_01")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.STATE                 AS IF_ELEC 
       ,B.IF_WATER              AS IF_WATER 
       ,B.IF_TV                 AS IF_TV 
       ,B.IF_MOBILE             AS IF_MOBILE 
       ,B.IF_WY                 AS IF_WY 
       ,B.IF_MSG                AS IF_MSG 
       ,B.IF_GAS                AS IF_GAS 
       ,B.IF_WIRE               AS IF_WIRE 
       ,B.SIGN_FLAG             AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ST_DATE 
   FROM (SELECT FR_ID,CUST_ID,STATE,
                ROW_NUMBER() OVER(PARTITION BY FR_ID,CUST_ID ORDER BY STATE DESC ) RN 
           FROM TMP_OCRM_F_CI_CUST_SIGN_01) A                                                   --客户签约临时表01(电费)
   LEFT JOIN OCRM_F_CI_CUST_SIGN B                             --客户签约临时表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.CUST_ID IS NOT NULL 
    AND RN = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-04::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")
TMP_OCRM_F_CI_CUST_SIGN_02 = sqlContext.read.parquet(hdfs+'/TMP_OCRM_F_CI_CUST_SIGN_02/*')
TMP_OCRM_F_CI_CUST_SIGN_02.registerTempTable("TMP_OCRM_F_CI_CUST_SIGN_02")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,B.IF_ELEC               AS IF_ELEC 
       ,A.STATE                 AS IF_WATER 
       ,B.IF_TV                 AS IF_TV 
       ,B.IF_MOBILE             AS IF_MOBILE 
       ,B.IF_WY                 AS IF_WY 
       ,B.IF_MSG                AS IF_MSG 
       ,B.IF_GAS                AS IF_GAS 
       ,B.IF_WIRE               AS IF_WIRE 
       ,B.SIGN_FLAG             AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ST_DATE 
   FROM (SELECT FR_ID,CUST_ID,STATE, ROW_NUMBER()OVER(PARTITION BY FR_ID,CUST_ID ORDER BY STATE DESC) RN 
           FROM TMP_OCRM_F_CI_CUST_SIGN_02 
          WHERE TYPE = 'Water' AND CUST_ID IS NOT NULL
        ) A                                                   --客户签约临时表02(水费煤气费)
   LEFT JOIN OCRM_F_CI_CUST_SIGN B                             --客户签约临时表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE RN                      = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)

#任务[12] 001-05::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")

sql = """
 SELECT A.CIFNO                 AS CUST_ID 
       ,B.IF_ELEC               AS IF_ELEC 
       ,B.IF_WATER              AS IF_WATER 
       ,'1'                     AS IF_TV 
       ,B.IF_MOBILE             AS IF_MOBILE 
       ,B.IF_WY                 AS IF_WY 
       ,B.IF_MSG                AS IF_MSG 
       ,B.IF_GAS                AS IF_GAS 
       ,B.IF_WIRE               AS IF_WIRE 
       ,B.SIGN_FLAG             AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ST_DATE 
   FROM (SELECT DISTINCT CIFNO,FR_ID 
           FROM F_CSP_TVSIGNINFOHIST
          WHERE ODS_ST_DATE = V_DT 
            AND STATE = 'N'
            AND MAINTCODE IN('A', 'U')
           ) A                                 --代缴费广电签约信息历史表
   LEFT JOIN OCRM_F_CI_CUST_SIGN B                             --客户签约临时表
     ON A.CIFNO               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-06::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")

sql = """
 SELECT A.CUST_ID                 AS CUST_ID 
       ,C.IF_ELEC               AS IF_ELEC 
       ,C.IF_WATER              AS IF_WATER 
       ,C.IF_TV                 AS IF_TV 
       ,'1'                     AS IF_MOBILE 
       ,C.IF_WY                 AS IF_WY 
       ,C.IF_MSG                AS IF_MSG 
       ,C.IF_GAS                AS IF_GAS 
       ,C.IF_WIRE               AS IF_WIRE 
       ,C.SIGN_FLAG             AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ST_DATE 
   FROM (SELECT DISTINCT A.FR_ID,B.CIFNO AS CUST_ID 
               FROM F_CSP_MOBILEBANKCIFINFOHIST A 
               JOIN F_CSP_ENTBANKCIFINFOHIST B ON A.CIFSEQ = B.MAINTJNLNO AND B.FR_ID = A.FR_ID 
              WHERE A.ODS_ST_DATE = V_DT 
                AND A.MAINTCODE IN ('A','U')) A                          --手机银行开通信息历史表
   LEFT JOIN OCRM_F_CI_CUST_SIGN C                             --客户签约临时表
     ON A.CUST_ID                 = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-07::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,B.IF_ELEC               AS IF_ELEC 
       ,B.IF_WATER              AS IF_WATER 
       ,B.IF_TV                 AS IF_TV 
       ,'1'                     AS IF_MOBILE 
       ,B.IF_WY                 AS IF_WY 
       ,B.IF_MSG                AS IF_MSG 
       ,B.IF_GAS                AS IF_GAS 
       ,B.IF_WIRE               AS IF_WIRE 
       ,B.SIGN_FLAG             AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ST_DATE 
   FROM (SELECT DISTINCT CUST_ID ,FR_ID 
           FROM OCRM_F_CI_CUST_DESC A
          WHERE SUBSTR(A.ODS_SYS_ID, 11, 1) = '1'
          AND A.CRM_DT = V_DT) A                               --统一客户信息表
  INNER JOIN OCRM_F_CI_CUST_SIGN B                             --客户签约临时表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID 
       """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-08::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")

sql = """
 SELECT A.CIFNO                 AS CUST_ID 
       ,B.IF_ELEC               AS IF_ELEC 
       ,B.IF_WATER              AS IF_WATER 
       ,B.IF_TV                 AS IF_TV 
       ,B.IF_MOBILE             AS IF_MOBILE 
       ,B.IF_WY                 AS IF_WY 
       ,'1'                     AS IF_MSG 
       ,B.IF_GAS                AS IF_GAS 
       ,B.IF_WIRE               AS IF_WIRE 
       ,B.SIGN_FLAG             AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ST_DATE 
   FROM (SELECT DISTINCT CIFNO,FR_ID
           FROM F_CSP_SMSSIGNINFOHIST                                 --短信平台签约信息历史表
          WHERE ODS_ST_DATE = V_DT 
            AND MAINTCODE IN ('A', 'U') 
            AND STATE = 'N' ) A 
   LEFT JOIN OCRM_F_CI_CUST_SIGN B                             --客户签约临时表
     ON A.CIFNO                 = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-09::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,B.IF_ELEC               AS IF_ELEC 
       ,B.IF_WATER              AS IF_WATER 
       ,B.IF_TV                 AS IF_TV 
       ,B.IF_MOBILE             AS IF_MOBILE 
       ,B.IF_WY                 AS IF_WY 
       ,'1'                     AS IF_MSG 
       ,B.IF_GAS                AS IF_GAS 
       ,B.IF_WIRE               AS IF_WIRE 
       ,B.SIGN_FLAG             AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ST_DATE 
   FROM (SELECT DISTINCT CUST_ID ,FR_ID 
           FROM OCRM_F_CI_CUST_DESC A
          WHERE SUBSTR(A.ODS_SYS_ID, 9, 1) = '1'
          AND A.CRM_DT = V_DT) A                               --统一客户信息表
   LEFT JOIN OCRM_F_CI_CUST_SIGN B                             --客户签约临时表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-10::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,B.IF_ELEC               AS IF_ELEC 
       ,B.IF_WATER              AS IF_WATER 
       ,B.IF_TV                 AS IF_TV 
       ,B.IF_MOBILE             AS IF_MOBILE 
       ,B.IF_WY                 AS IF_WY 
       ,B.IF_MSG                AS IF_MSG 
       ,A.STATE                 AS IF_GAS 
       ,B.IF_WIRE               AS IF_WIRE 
       ,B.SIGN_FLAG             AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ST_DATE 
   FROM (SELECT CUST_ID,STATE,FR_ID, 
                ROW_NUMBER()OVER(PARTITION BY CUST_ID ORDER BY STATE DESC) RN 
           FROM TMP_OCRM_F_CI_CUST_SIGN_02 
           WHERE TYPE = 'Gas' AND CUST_ID IS NOT NULL
        ) A                                                   --客户签约临时表02(水费煤气费)
   LEFT JOIN OCRM_F_CI_CUST_SIGN B                             --客户签约临时表
     ON A.CUST_ID                 = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE RN                      = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-11::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")

sql = """
 SELECT A.CUST_ID                 AS CUST_ID 
       ,C.IF_ELEC               AS IF_ELEC 
       ,C.IF_WATER              AS IF_WATER 
       ,C.IF_TV                 AS IF_TV 
       ,C.IF_MOBILE             AS IF_MOBILE 
       ,'1'                     AS IF_WY 
       ,C.IF_MSG                AS IF_MSG 
       ,C.IF_GAS                AS IF_GAS 
       ,C.IF_WIRE               AS IF_WIRE 
       ,C.SIGN_FLAG             AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ST_DATE 
   FROM (SELECT DISTINCT B.CIFNO AS CUST_ID,A.FR_ID 
               FROM F_CSP_PERNETBANKCIFINFOHIST A 
               JOIN F_CSP_ENTBANKCIFINFOHIST B ON A.CIFSEQ = B.MAINTJNLNO AND B.FR_ID = A.FR_ID  
              WHERE A.ODS_ST_DATE = V_DT 
                AND A.MAINTCODE IN ('A','U')  --A:签约;U:变更;D:解约
             ) A                          --个人网银开通信息历史表
   LEFT JOIN OCRM_F_CI_CUST_SIGN C                             --客户签约临时表
     ON A.CUST_ID                 = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-12::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,B.IF_ELEC               AS IF_ELEC 
       ,B.IF_WATER              AS IF_WATER 
       ,B.IF_TV                 AS IF_TV 
       ,B.IF_MOBILE             AS IF_MOBILE 
       ,'1'                     AS IF_WY 
       ,B.IF_MSG                AS IF_MSG 
       ,B.IF_GAS                AS IF_GAS 
       ,B.IF_WIRE               AS IF_WIRE 
       ,B.SIGN_FLAG             AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ST_DATE 
   FROM (SELECT DISTINCT CUST_ID ,FR_ID 
           FROM OCRM_F_CI_CUST_DESC A
          WHERE SUBSTR(A.ODS_SYS_ID, 3, 1) = '1'
          AND A.CRM_DT = V_DT) A                                  --统一客户信息表
  INNER JOIN OCRM_F_CI_CUST_SIGN B                             --客户签约临时表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)

#任务[12] 001-13::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")

sql = """
 SELECT A.CUST_ID                 AS CUST_ID 
       ,B.IF_ELEC               AS IF_ELEC 
       ,B.IF_WATER              AS IF_WATER 
       ,B.IF_TV                 AS IF_TV 
       ,B.IF_MOBILE             AS IF_MOBILE 
       ,B.IF_WY                 AS IF_WY 
       ,B.IF_MSG                AS IF_MSG 
       ,B.IF_GAS                AS IF_GAS 
       ,'1'                     AS IF_WIRE 
       ,B.SIGN_FLAG             AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ST_DATE 
   FROM (SELECT DISTINCT FR_ID,CIFNO AS CUST_ID 
               FROM F_CSP_WIRESIGNINFOHIST 
              WHERE ODS_ST_DATE = V_DT  
                AND MAINTCODE IN ('A','U')  --A:签约;U:变更;D:解约
                AND STATE = 'N' --N:正常;C:关闭
             ) A                               --代缴费电信签约信息历史表
   LEFT JOIN OCRM_F_CI_CUST_SIGN B                             --客户签约临时表
     ON A.CUST_ID = B.CUST_ID 
    AND A.FR_ID = B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-14::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,IF_ELEC                 AS IF_ELEC 
       ,IF_WATER                AS IF_WATER 
       ,IF_TV                   AS IF_TV 
       ,IF_MOBILE               AS IF_MOBILE 
       ,IF_WY                   AS IF_WY 
       ,IF_MSG                  AS IF_MSG 
       ,IF_GAS                  AS IF_GAS 
       ,IF_WIRE                 AS IF_WIRE 
       ,CONCAT(IF_WY , IF_MOBILE , IF_MSG , IF_ELEC , IF_WATER , IF_GAS , IF_TV , IF_WIRE)                 AS SIGN_FLAG 
       ,FR_ID                   AS FR_ID 
       ,ST_DATE                 AS ST_DATE 
   FROM OCRM_F_CI_CUST_SIGN A                                  --客户签约临时表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_SIGN_INNTMP1.registerTempTable("OCRM_F_CI_CUST_SIGN_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.IF_ELEC                                            --是否电费签约:src.IF_ELEC
       ,DST.IF_WATER                                           --是否水费签约:src.IF_WATER
       ,DST.IF_TV                                              --是否广电签约:src.IF_TV
       ,DST.IF_MOBILE                                          --是否手机银行签约:src.IF_MOBILE
       ,DST.IF_WY                                              --是否网银签约:src.IF_WY
       ,DST.IF_MSG                                             --是否短信签约:src.IF_MSG
       ,DST.IF_GAS                                             --是否代缴费燃气签约:src.IF_GAS
       ,DST.IF_WIRE                                            --是否代缴费电信签约:src.IF_WIRE
       ,DST.SIGN_FLAG                                          --签约汇总(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ST_DATE                                            --ETL日期:src.ST_DATE
   FROM OCRM_F_CI_CUST_SIGN DST 
   LEFT JOIN OCRM_F_CI_CUST_SIGN_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_SIGN_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet"
OCRM_F_CI_CUST_SIGN_INNTMP2=OCRM_F_CI_CUST_SIGN_INNTMP2.unionAll(OCRM_F_CI_CUST_SIGN_INNTMP1)
OCRM_F_CI_CUST_SIGN_INNTMP1.cache()
OCRM_F_CI_CUST_SIGN_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_SIGN_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_SIGN_INNTMP2.count()

#装载数据
OCRM_F_CI_CUST_SIGN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_SIGN_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_CUST_SIGN/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_CUST_SIGN_BK/"+V_DT+".parquet")


OCRM_F_CI_CUST_SIGN_INNTMP1.unpersist()
OCRM_F_CI_CUST_SIGN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_SIGN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)

