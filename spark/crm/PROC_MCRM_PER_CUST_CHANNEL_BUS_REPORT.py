#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MCRM_PER_CUST_CHANNEL_BUS_REPORT').setMaster(sys.argv[2])
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

OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
SELECT T.CUST_ID            AS CUST_ID,
       T.INSTITUTION_CODE   AS ORG_ID,
       T.CUST_BIR           AS CUST_BIR,
       T.CUST_CRE_TYP       AS CUST_CER_TYP,
       T.CUST_CER_NO        AS CUST_CER_NO,
       CASE
          WHEN (T.AGE_VALUE IS NULL
                OR (T.AGE_VALUE > SUBSTR(V_DT,1,4) OR T.AGE_VALUE < '1900'))
               AND SUBSTR (CUST_BIR, 1, 4) < SUBSTR(V_DT,1,4)
               AND SUBSTR (CUST_BIR, 1, 4) > '1900'
               AND CUST_BIR <> '1900-01-01'
          THEN
             SUBSTR(V_DT,1,4) - SUBSTR (CUST_BIR, 1, 4)
          WHEN T.AGE_VALUE IS NULL
               OR (T.AGE_VALUE > SUBSTR(V_DT,1,4) OR T.AGE_VALUE < '1900')
          THEN ''
          ELSE SUBSTR(V_DT,1,4) - T.AGE_VALUE
       END AS AGE,
       '104' AS FR_ID,
       V_DT AS ETL_DT
  FROM (SELECT A.CUST_ID,
               A.CUST_CER_NO,
               A.CUST_CRE_TYP,
               A.CUST_BIR,
               B.INSTITUTION_CODE INSTITUTION_CODE,
               CASE
                  WHEN LENGTH (CUST_CER_NO) = 18 AND CUST_CRE_TYP = '0'
                  THEN SUBSTR (CUST_CER_NO, 7, 4)
                  WHEN (   CUST_CRE_TYP <> '0'
                        OR LENGTH (CUST_CER_NO) <> 18
                        OR CUST_CER_NO IS NULL)
                       AND CUST_BIR <> ''
                       AND CUST_BIR <> '1900-01-01'
                  THEN SUBSTR (CUST_BIR, 1, 4)
                  ELSE NULL
               END AS AGE_VALUE
            FROM  OCRM_F_CI_PER_CUST_INFO A
             JOIN OCRM_F_CI_BELONG_ORG B ON A.CUST_ID = B.CUST_ID
                  AND B.FR_ID = '104'
                  AND B.MAIN_TYPE = '1'
         WHERE A.FR_ID = '104') T
     
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_PER_CUST_AGE_VALUE = sqlContext.sql(sql)
TMP_PER_CUST_AGE_VALUE.registerTempTable("TMP_PER_CUST_AGE_VALUE")
dfn="TMP_PER_CUST_AGE_VALUE/"+V_DT+".parquet"
TMP_PER_CUST_AGE_VALUE.cache()
nrows = TMP_PER_CUST_AGE_VALUE.count()
TMP_PER_CUST_AGE_VALUE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_PER_CUST_AGE_VALUE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_PER_CUST_AGE_VALUE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_PER_CUST_CHANNEL_BUS_REPORT/*")
#ret = os.system("hdfs dfs -cp /"+dbname+"/MCRM_PER_CUST_CHANNEL_BUS_REPORT_BK/"+V_DT_LD+".parquet /"+dbname+"/MCRM_PER_CUST_CHANNEL_BUS_REPORT/")

ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
OCRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_DP_CARD_INFO/*')
OCRM_F_DP_CARD_INFO.registerTempTable("OCRM_F_DP_CARD_INFO")
F_CI_CBOD_PDPDPPDP = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_PDPDPPDP/*')
F_CI_CBOD_PDPDPPDP.registerTempTable("F_CI_CBOD_PDPDPPDP")
TMP_PER_CUST_AGE_VALUE = sqlContext.read.parquet(hdfs+'/TMP_PER_CUST_AGE_VALUE/*')
TMP_PER_CUST_AGE_VALUE.registerTempTable("TMP_PER_CUST_AGE_VALUE")

sql = """
SELECT  A.ORG_ID     AS ORG_ID,
        CASE WHEN D.AGE <30 THEN '1'
             WHEN D.AGE <45 AND D.AGE >=30 THEN '2'
             WHEN D.AGE <55 AND D.AGE >=45 THEN '3'
             WHEN D.AGE <65 AND D.AGE >=55 THEN '4'
             WHEN D.AGE >=65 THEN '5' 
        ELSE '0' END AS CUST_AGE,
        COUNT(DISTINCT D.CUST_ID) AS CUST_COUNT,
		 CAST('' AS DECIMAL(10))  AS  BANKBOOK_COUNT,
        COUNT(DISTINCT CASE WHEN C.IS_JJK = 'Y' THEN D.CUST_ID END) AS DPCARD_COUNT,
        COUNT(DISTINCT CASE WHEN F.PD_DES LIKE '%员工卡%' THEN D.CUST_ID END) AS YGCARD_COUNT,
        COUNT(DISTINCT CASE WHEN F.PD_DES LIKE '%圆鼎%' THEN D.CUST_ID END) AS YDCARD_COUNT,
        COUNT(DISTINCT CASE WHEN F.PD_DES LIKE '%圆鼎IC卡%' THEN D.CUST_ID END) AS YDICCARD_COUNT,
        COUNT(DISTINCT CASE WHEN F.PD_DES LIKE '%圆鼎易贷通卡%' THEN D.CUST_ID END) AS YDYDTCARD_COUNT,
        COUNT(DISTINCT CASE WHEN F.PD_DES LIKE '%圆鼎易贷通IC卡%' THEN D.CUST_ID END) AS YDYDTICCARD_COUNT,
        COUNT(DISTINCT CASE WHEN C.IS_WY = 'Y' THEN D.CUST_ID END) AS NETBANK_COUNT,
        COUNT(DISTINCT CASE WHEN C.IS_SJYH = 'Y' THEN D.CUST_ID END) AS MOBBANK_COUNT,
        COUNT(DISTINCT CASE WHEN C.IS_DXYH = 'Y' THEN D.CUST_ID END) AS MESBANK_COUNT,
        COUNT(DISTINCT CASE WHEN C.IS_ZFB = 'Y' THEN D.CUST_ID END) AS ZFBCARD_COUNT,
        COUNT(DISTINCT CASE WHEN C.IS_ELEC = 'Y' THEN D.CUST_ID END) AS ELECSIGN_COUNT,
        V_DT AS REPORT_DATE,
        '104' AS FR_ID
	 FROM ADMIN_AUTH_ORG A
	    LEFT JOIN TMP_PER_CUST_AGE_VALUE D ON A.ORG_ID = D.ORG_ID AND D.FR_ID = '104' 
	    LEFT JOIN ACRM_A_PROD_CHANNEL_INFO C ON D.CUST_ID = C.CUST_ID AND C.FR_ID = '104'
	    LEFT JOIN OCRM_F_DP_CARD_INFO E ON D.CUST_ID = E.CR_CUST_NO AND E.FR_ID = '104'
	    LEFT JOIN F_CI_CBOD_PDPDPPDP F ON E.CR_PDP_CODE = F.PD_CODE
		WHERE A.FR_ID = '104'
		AND A.ORG_LEVEL = '3'
    GROUP BY A.ORG_ID,
    CASE WHEN D.AGE <30 THEN '1'
             WHEN D.AGE <45 AND D.AGE >=30 THEN '2'
             WHEN D.AGE <55 AND D.AGE >=45 THEN '3'
             WHEN D.AGE <65 AND D.AGE >=55 THEN '4'
             WHEN D.AGE >=65 THEN '5'
        ELSE '0' END
     
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_PER_CUST_CHANNEL_BUS_REPORT = sqlContext.sql(sql)
MCRM_PER_CUST_CHANNEL_BUS_REPORT.registerTempTable("MCRM_PER_CUST_CHANNEL_BUS_REPORT")
dfn="MCRM_PER_CUST_CHANNEL_BUS_REPORT/"+V_DT+".parquet"
MCRM_PER_CUST_CHANNEL_BUS_REPORT.cache()
nrows = MCRM_PER_CUST_CHANNEL_BUS_REPORT.count()
MCRM_PER_CUST_CHANNEL_BUS_REPORT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_PER_CUST_CHANNEL_BUS_REPORT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_PER_CUST_CHANNEL_BUS_REPORT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
TMP_PER_CUST_AGE_VALUE = sqlContext.read.parquet(hdfs+'/TMP_PER_CUST_AGE_VALUE/*')
TMP_PER_CUST_AGE_VALUE.registerTempTable("TMP_PER_CUST_AGE_VALUE")
F_DP_CBOD_TDACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_TDACNACN/*')
F_DP_CBOD_TDACNACN.registerTempTable("F_DP_CBOD_TDACNACN")

sql = """
 SELECT  A.ORG_ID,
        CASE WHEN D.AGE <30 THEN '1'
             WHEN D.AGE <45 AND D.AGE >=30 THEN '2'
             WHEN D.AGE <55 AND D.AGE >=45 THEN '3'
             WHEN D.AGE <65 AND D.AGE >=55 THEN '4'
             WHEN D.AGE >=65 THEN '5'
        ELSE '0' END AS CUST_AGE,
        COUNT(DISTINCT G.TD_CUST_NO) BANKBOOK_COUNT
	 FROM ADMIN_AUTH_ORG A
	    LEFT JOIN TMP_PER_CUST_AGE_VALUE D ON A.ORG_ID = D.ORG_ID AND D.FR_ID = '104' 
	    LEFT JOIN F_DP_CBOD_TDACNACN G ON D.CUST_ID = G.TD_CUST_NO AND G.TD_TDP_PSBK_FLG = '1' AND G.FR_ID = '104' 
		WHERE A.FR_ID = '104'
      AND A.ORG_LEVEL = '3'
    GROUP BY A.ORG_ID,
    CASE WHEN D.AGE <30 THEN '1'
             WHEN D.AGE <45 AND D.AGE >=30 THEN '2'
             WHEN D.AGE <55 AND D.AGE >=45 THEN '3'
             WHEN D.AGE <65 AND D.AGE >=55 THEN '4'
             WHEN D.AGE >=65 THEN '5'
        ELSE '0' END
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP1 = sqlContext.sql(sql)
MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP1.registerTempTable("MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP1")


#MCRM_PER_CUST_CHANNEL_BUS_REPORT = sqlContext.read.parquet(hdfs+'/MCRM_PER_CUST_CHANNEL_BUS_REPORT/*')
#MCRM_PER_CUST_CHANNEL_BUS_REPORT.registerTempTable("MCRM_PER_CUST_CHANNEL_BUS_REPORT")

sql = """
SELECT
           DST.ORG_ID,
           DST.CUST_AGE,
           DST.CUST_COUNT,
           SRC.BANKBOOK_COUNT,
           DST.DPCARD_COUNT,
           DST.YGCARD_COUNT,
           DST.YDCARD_COUNT,
           DST.YDICCARD_COUNT,
           DST.YDYDTCARD_COUNT,
           DST.YDYDTICCARD_COUNT,
           DST.NETBANK_COUNT,
           DST.MOBBANK_COUNT,
           DST.MESBANK_COUNT,
           DST.ZFBCARD_COUNT,
           DST.ELECSIGN_COUNT,
           DST.REPORT_DATE,
           DST.FR_ID
   FROM MCRM_PER_CUST_CHANNEL_BUS_REPORT DST 
    INNER JOIN MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP1 SRC 
     ON DST.ORG_ID = SRC.ORG_ID AND DST.CUST_AGE = SRC.CUST_AGE
  """
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP2 = sqlContext.sql(sql)
MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP2.registerTempTable("MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP2")

sql = """
SELECT
           DST.ORG_ID,
           DST.CUST_AGE,
           DST.CUST_COUNT,
           DST.BANKBOOK_COUNT,
           DST.DPCARD_COUNT,
           DST.YGCARD_COUNT,
           DST.YDCARD_COUNT,
           DST.YDICCARD_COUNT,
           DST.YDYDTCARD_COUNT,
           DST.YDYDTICCARD_COUNT,
           DST.NETBANK_COUNT,
           DST.MOBBANK_COUNT,
           DST.MESBANK_COUNT,
           DST.ZFBCARD_COUNT,
           DST.ELECSIGN_COUNT,
           DST.REPORT_DATE,
           DST.FR_ID
   FROM MCRM_PER_CUST_CHANNEL_BUS_REPORT DST 
    LEFT JOIN MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP1 SRC 
     ON DST.ORG_ID = SRC.ORG_ID AND DST.CUST_AGE = SRC.CUST_AGE
	 WHERE SRC.ORG_ID IS NULL
  """
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP3 = sqlContext.sql(sql)
MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP3.registerTempTable("MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP3")
  
  
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP3 = sqlContext.sql(sql)
dfn="MCRM_PER_CUST_CHANNEL_BUS_REPORT/"+V_DT+".parquet"
UNION=MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP3.unionAll(MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP2)
MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP2.cache()
MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP3.cache()
nrowsi = MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP2.count()
nrowsa = MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP3.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP2.unpersist()
MCRM_PER_CUST_CHANNEL_BUS_REPORT_INNTMP3.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_PER_CUST_CHANNEL_BUS_REPORT  lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_PER_CUST_CHANNEL_BUS_REPORT_BK/"+V_DT+".parquet")
#ret = os.system("hdfs dfs -cp /"+dbname+"/MCRM_PER_CUST_CHANNEL_BUS_REPORT/"+V_DT+".parquet /"+dbname+"/MCRM_PER_CUST_CHANNEL_BUS_REPORT_BK/")
