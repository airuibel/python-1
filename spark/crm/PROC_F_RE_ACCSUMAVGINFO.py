#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_RE_ACCSUMAVGINFO').setMaster(sys.argv[2])
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
#当年2月天数
V_DT_2_DAYS = ((date(int(etl_date[0:4]), 3, 1)+ timedelta(-1)).strftime("%Y%m%d"))[6:8]
#当月截止当天的天数
V_MONTH_DAYS=etl_date[6:8]

V_STEP = 0

#清除数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO/*.parquet")
#恢复数据到今日数据文件
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO/"+V_DT+".parquet")


OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_TX_FIN_SALEINFO = sqlContext.read.parquet(hdfs+'/F_TX_FIN_SALEINFO/*')
F_TX_FIN_SALEINFO.registerTempTable("F_TX_FIN_SALEINFO")
F_TX_FIN_CUSTINFO = sqlContext.read.parquet(hdfs+'/F_TX_FIN_CUSTINFO/*')
F_TX_FIN_CUSTINFO.registerTempTable("F_TX_FIN_CUSTINFO")
#OCRM_F_CI_SYS_RESOURCE_FIN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE_FIN/*')
#OCRM_F_CI_SYS_RESOURCE_FIN.registerTempTable("OCRM_F_CI_SYS_RESOURCE_FIN")
ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")



#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ODS_CUST_ID             AS ODS_CUST_ID 
       ,CERT_TYPE               AS CERT_TYPE 
       ,CERT_NO                 AS CERT_NO 
       ,ODS_CUST_NAME           AS ODS_CUST_NAME 
       ,FR_ID                   AS FR_ID 
   FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
  WHERE ODS_SYS_ID              = 'FIN' 
  GROUP BY ODS_CUST_ID,CERT_TYPE 
       ,CERT_NO 
       ,ODS_CUST_NAME 
       ,FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_ACRM_F_RE_ACCSUMAVGINFO_01 = sqlContext.sql(sql)
TMP_ACRM_F_RE_ACCSUMAVGINFO_01.registerTempTable("TMP_ACRM_F_RE_ACCSUMAVGINFO_01")
dfn="TMP_ACRM_F_RE_ACCSUMAVGINFO_01/"+V_DT+".parquet"
TMP_ACRM_F_RE_ACCSUMAVGINFO_01.cache()
nrows = TMP_ACRM_F_RE_ACCSUMAVGINFO_01.count()
TMP_ACRM_F_RE_ACCSUMAVGINFO_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_ACRM_F_RE_ACCSUMAVGINFO_01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_ACRM_F_RE_ACCSUMAVGINFO_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_ACRM_F_RE_ACCSUMAVGINFO_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

TMP_ACRM_F_RE_ACCSUMAVGINFO_01 = sqlContext.read.parquet(hdfs+'/TMP_ACRM_F_RE_ACCSUMAVGINFO_01/*')
TMP_ACRM_F_RE_ACCSUMAVGINFO_01.registerTempTable("TMP_ACRM_F_RE_ACCSUMAVGINFO_01")

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,ACCT_NAME               AS ACCT_NAME 
       ,ACCT_NO                 AS ACCT_NO 
       ,CURR                    AS CURR 
       ,PRDT_CODE               AS PRDT_CODE 
       ,AMT                     AS AMT 
       ,LAST_AMOUNT             AS LAST_AMOUNT 
       ,CAST(YEAR(V_DT) AS VARCHAR(4))  AS YEAR 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_1 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_1 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_2 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_2 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_3 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_3 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_4 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_4 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_5 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_5 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_6 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_6 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_7 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_7 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_8 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_8 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_9 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_9 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_10 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_10 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_11 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_11 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_12 
       ,CAST(0 AS INTEGER)                       AS MONTH_DAYS_12 
       ,V_DT                    AS ODS_ST_DATE 
       ,MONTH_BAL_SUM_12        AS OLD_YEAR_BAL_SUM 
       ,MONTH_BAL_12            AS OLD_YEAR_BAL 
       ,MONTH_DAYS_12           AS OLD_YEAR_DAYS 
       ,ORG_ID                  AS ORG_ID 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_1 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_2 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_3 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_4 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_5 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_6 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_7 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_8 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_9 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_10 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_11 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_12 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_ACCSUMAVGINFO A                              --理财积数均值表
  WHERE YEAR                    = YEAR(V_DT) - 1 
    AND AMT > 0 
    AND V_DT                    = CONCAT(SUBSTR(V_DT,1,4),'-01-01') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_ACCSUMAVGINFO = sqlContext.sql(sql)
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")
dfn="ACRM_F_RE_ACCSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_ACCSUMAVGINFO.cache()
nrows = ACRM_F_RE_ACCSUMAVGINFO.count()
ACRM_F_RE_ACCSUMAVGINFO.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_RE_ACCSUMAVGINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_ACCSUMAVGINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-03::
V_STEP = V_STEP + 1

ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,ACCT_NAME               AS ACCT_NAME 
       ,ACCT_NO                 AS ACCT_NO 
       ,CURR                    AS CURR 
       ,PRDT_CODE               AS PRDT_CODE 
       ,AMT                     AS AMT 
       ,AMT                     AS LAST_AMOUNT 
       ,YEAR                    AS YEAR 
       ,MONTH_BAL_SUM_1         AS MONTH_BAL_SUM_1 
       ,MONTH_DAYS_1            AS MONTH_DAYS_1 
       ,MONTH_BAL_SUM_2         AS MONTH_BAL_SUM_2 
       ,MONTH_DAYS_2            AS MONTH_DAYS_2 
       ,MONTH_BAL_SUM_3         AS MONTH_BAL_SUM_3 
       ,MONTH_DAYS_3            AS MONTH_DAYS_3 
       ,MONTH_BAL_SUM_4         AS MONTH_BAL_SUM_4 
       ,MONTH_DAYS_4            AS MONTH_DAYS_4 
       ,MONTH_BAL_SUM_5         AS MONTH_BAL_SUM_5 
       ,MONTH_DAYS_5            AS MONTH_DAYS_5 
       ,MONTH_BAL_SUM_6         AS MONTH_BAL_SUM_6 
       ,MONTH_DAYS_6            AS MONTH_DAYS_6 
       ,MONTH_BAL_SUM_7         AS MONTH_BAL_SUM_7 
       ,MONTH_DAYS_7            AS MONTH_DAYS_7 
       ,MONTH_BAL_SUM_8         AS MONTH_BAL_SUM_8 
       ,MONTH_DAYS_8            AS MONTH_DAYS_8 
       ,MONTH_BAL_SUM_9         AS MONTH_BAL_SUM_9 
       ,MONTH_DAYS_9            AS MONTH_DAYS_9 
       ,MONTH_BAL_SUM_10        AS MONTH_BAL_SUM_10 
       ,MONTH_DAYS_10           AS MONTH_DAYS_10 
       ,MONTH_BAL_SUM_11        AS MONTH_BAL_SUM_11 
       ,MONTH_DAYS_11           AS MONTH_DAYS_11 
       ,MONTH_BAL_SUM_12        AS MONTH_BAL_SUM_12 
       ,MONTH_DAYS_12           AS MONTH_DAYS_12 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
       ,OLD_YEAR_BAL_SUM        AS OLD_YEAR_BAL_SUM 
       ,OLD_YEAR_BAL            AS OLD_YEAR_BAL 
       ,OLD_YEAR_DAYS           AS OLD_YEAR_DAYS 
       ,ORG_ID                  AS ORG_ID 
       ,MONTH_BAL_1             AS MONTH_BAL_1 
       ,MONTH_BAL_2             AS MONTH_BAL_2 
       ,MONTH_BAL_3             AS MONTH_BAL_3 
       ,MONTH_BAL_4             AS MONTH_BAL_4 
       ,MONTH_BAL_5             AS MONTH_BAL_5 
       ,MONTH_BAL_6             AS MONTH_BAL_6 
       ,MONTH_BAL_7             AS MONTH_BAL_7 
       ,MONTH_BAL_8             AS MONTH_BAL_8 
       ,MONTH_BAL_9             AS MONTH_BAL_9 
       ,MONTH_BAL_10            AS MONTH_BAL_10 
       ,MONTH_BAL_11            AS MONTH_BAL_11 
       ,MONTH_BAL_12            AS MONTH_BAL_12 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_ACCSUMAVGINFO A                              --理财积数均值表
  WHERE YEAR                    = YEAR(V_DT) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO_INNTMP1")

ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.ACCT_NAME                                          --账户名称:src.ACCT_NAME
       ,DST.ACCT_NO                                            --帐号:src.ACCT_NO
       ,DST.CURR                                               --币种:src.CURR
       ,DST.PRDT_CODE                                          --产品号:src.PRDT_CODE
       ,DST.AMT                                                --余额:src.AMT
       ,DST.LAST_AMOUNT                                        --昨日余额:src.LAST_AMOUNT
       ,DST.YEAR                                               --年份:src.YEAR
       ,DST.MONTH_BAL_SUM_1                                    --1月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --1月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --2月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --2月天数:src.MONTH_DAYS_2
       ,DST.MONTH_BAL_SUM_3                                    --3月余额积数:src.MONTH_BAL_SUM_3
       ,DST.MONTH_DAYS_3                                       --3月天数:src.MONTH_DAYS_3
       ,DST.MONTH_BAL_SUM_4                                    --4月余额积数:src.MONTH_BAL_SUM_4
       ,DST.MONTH_DAYS_4                                       --4月天数:src.MONTH_DAYS_4
       ,DST.MONTH_BAL_SUM_5                                    --5月余额积数:src.MONTH_BAL_SUM_5
       ,DST.MONTH_DAYS_5                                       --5月天数:src.MONTH_DAYS_5
       ,DST.MONTH_BAL_SUM_6                                    --6月余额积数:src.MONTH_BAL_SUM_6
       ,DST.MONTH_DAYS_6                                       --6月天数:src.MONTH_DAYS_6
       ,DST.MONTH_BAL_SUM_7                                    --7月余额积数:src.MONTH_BAL_SUM_7
       ,DST.MONTH_DAYS_7                                       --7月天数:src.MONTH_DAYS_7
       ,DST.MONTH_BAL_SUM_8                                    --8月余额积数:src.MONTH_BAL_SUM_8
       ,DST.MONTH_DAYS_8                                       --8月天数:src.MONTH_DAYS_8
       ,DST.MONTH_BAL_SUM_9                                    --9月余额积数:src.MONTH_BAL_SUM_9
       ,DST.MONTH_DAYS_9                                       --9月天数:src.MONTH_DAYS_9
       ,DST.MONTH_BAL_SUM_10                                   --10月余额积数:src.MONTH_BAL_SUM_10
       ,DST.MONTH_DAYS_10                                      --10月天数:src.MONTH_DAYS_10
       ,DST.MONTH_BAL_SUM_11                                   --11月余额积数:src.MONTH_BAL_SUM_11
       ,DST.MONTH_DAYS_11                                      --11月天数:src.MONTH_DAYS_11
       ,DST.MONTH_BAL_SUM_12                                   --12月余额积数:src.MONTH_BAL_SUM_12
       ,DST.MONTH_DAYS_12                                      --12月天数:src.MONTH_DAYS_12
       ,DST.ODS_ST_DATE                                        --加工日期:src.ODS_ST_DATE
       ,DST.OLD_YEAR_BAL_SUM                                   --去年余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --年初余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --去年年天数:src.OLD_YEAR_DAYS
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.MONTH_BAL_1                                        --1月月末剩余金额（原币种）:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月月末剩余金额（原币种）:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月月末剩余金额（原币种）:src.MONTH_BAL_3
       ,DST.MONTH_BAL_4                                        --4月月末剩余金额（原币种）:src.MONTH_BAL_4
       ,DST.MONTH_BAL_5                                        --5月月末剩余金额（原币种）:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月月末剩余金额（原币种）:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月月末剩余金额（原币种）:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月月末剩余金额（原币种）:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月月末剩余金额（原币种）:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月月末剩余金额（原币种）:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月月末剩余金额（原币种）:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月月末剩余金额（原币种）:src.MONTH_BAL_12
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_F_RE_ACCSUMAVGINFO DST 
   LEFT JOIN ACRM_F_RE_ACCSUMAVGINFO_INNTMP1 SRC 
     ON SRC.ACCT_NO             = DST.ACCT_NO 
    AND SRC.YEAR                = DST.YEAR 
    AND SRC.CURR                = DST.CURR 
    AND SRC.PRDT_CODE           = DST.PRDT_CODE 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.YEAR                = DST.YEAR 
  WHERE SRC.ACCT_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_ACCSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2=ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_ACCSUMAVGINFO_INNTMP1)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.count()
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_ACCSUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO_BK/")

#任务[12] 001-04::
V_STEP = V_STEP + 1

ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")

sql = """
 SELECT E.ODS_CUST_ID           AS CUST_ID 
       ,D.ACCTNAME              AS ACCT_NAME 
       ,D.ACCTNO                AS ACCT_NO 
       ,C.CURRENCY              AS CURR 
       ,C.PRODCODE              AS PRDT_CODE 
       ,CAST(C.LEFTAMT AS DECIMAL(24,6))              AS AMT 
       ,A.LAST_AMOUNT           AS LAST_AMOUNT 
       ,YEAR(V_DT)                       AS YEAR 
       ,COALESCE(A.MONTH_BAL_SUM_1, 0)                       AS MONTH_BAL_SUM_1 
       ,CASE WHEN MONTH(V_DT)                       = 1 THEN CAST(rint(V_MONTH_DAYS) AS INT) WHEN MONTH(V_DT) > 1 THEN 31 ELSE 0 END                     AS MONTH_DAYS_1 
       ,COALESCE(A.MONTH_BAL_SUM_2, 0)                       AS MONTH_BAL_SUM_2 
       ,CASE WHEN MONTH(V_DT)                       = 2 THEN CAST(rint(V_MONTH_DAYS) AS INT) WHEN MONTH(V_DT) > 2 THEN CAST(rint(V_DT_2_DAYS) AS INT) ELSE 0 END                     AS MONTH_DAYS_2 
       ,COALESCE(A.MONTH_BAL_SUM_3, 0)                       AS MONTH_BAL_SUM_3 
       ,CASE WHEN MONTH(V_DT)                       = 3 THEN CAST(rint(V_MONTH_DAYS) AS INT) WHEN MONTH(V_DT) > 3 THEN 31 ELSE 0 END                     AS MONTH_DAYS_3 
       ,COALESCE(A.MONTH_BAL_SUM_4, 0)                       AS MONTH_BAL_SUM_4 
       ,CASE WHEN MONTH(V_DT)                       = 4 THEN CAST(rint(V_MONTH_DAYS) AS INT) WHEN MONTH(V_DT) > 4 THEN 30 ELSE 0 END                     AS MONTH_DAYS_4 
       ,COALESCE(A.MONTH_BAL_SUM_5, 0)                       AS MONTH_BAL_SUM_5 
       ,CASE WHEN MONTH(V_DT)                       = 5 THEN CAST(rint(V_MONTH_DAYS) AS INT) WHEN MONTH(V_DT) > 5 THEN 31 ELSE 0 END                     AS MONTH_DAYS_5 
       ,COALESCE(A.MONTH_BAL_SUM_6, 0)                       AS MONTH_BAL_SUM_6 
       ,CASE WHEN MONTH(V_DT)                       = 6 THEN CAST(rint(V_MONTH_DAYS) AS INT) WHEN MONTH(V_DT) > 6 THEN 30 ELSE 0 END                     AS MONTH_DAYS_6 
       ,COALESCE(A.MONTH_BAL_SUM_7, 0)                       AS MONTH_BAL_SUM_7 
       ,CASE WHEN MONTH(V_DT)                       = 7 THEN CAST(rint(V_MONTH_DAYS) AS INT) WHEN MONTH(V_DT) > 7 THEN 31 ELSE 0 END                     AS MONTH_DAYS_7 
       ,COALESCE(A.MONTH_BAL_SUM_8, 0)                       AS MONTH_BAL_SUM_8 
       ,CASE WHEN MONTH(V_DT)                       = 8 THEN CAST(rint(V_MONTH_DAYS) AS INT) WHEN MONTH(V_DT) > 8 THEN 31 ELSE 0 END                     AS MONTH_DAYS_8 
       ,COALESCE(A.MONTH_BAL_SUM_9, 0)                       AS MONTH_BAL_SUM_9 
       ,CASE WHEN MONTH(V_DT)                       = 9 THEN CAST(rint(V_MONTH_DAYS) AS INT) WHEN MONTH(V_DT) > 9 THEN 30 ELSE 0 END                     AS MONTH_DAYS_9 
       ,COALESCE(A.MONTH_BAL_SUM_10, 0)                       AS MONTH_BAL_SUM_10 
       ,CASE WHEN MONTH(V_DT)                       = 10 THEN CAST(rint(V_MONTH_DAYS) AS INT) WHEN MONTH(V_DT) > 10 THEN 31 ELSE 0 END                     AS MONTH_DAYS_10 
       ,COALESCE(A.MONTH_BAL_SUM_11, 0)                       AS MONTH_BAL_SUM_11 
       ,CASE WHEN MONTH(V_DT)                       = 11 THEN CAST(rint(V_MONTH_DAYS) AS INT) WHEN MONTH(V_DT) > 11 THEN 30 ELSE 0 END                     AS MONTH_DAYS_11 
       ,COALESCE(A.MONTH_BAL_SUM_12, 0)                       AS MONTH_BAL_SUM_12 
       ,CASE WHEN MONTH(V_DT)                       = 12 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE 0 END                     AS MONTH_DAYS_12 
       ,V_DT                    AS ODS_ST_DATE 
       ,COALESCE(A.OLD_YEAR_BAL_SUM, 0)                       AS OLD_YEAR_BAL_SUM 
       ,COALESCE(A.OLD_YEAR_BAL, 0)                       AS OLD_YEAR_BAL 
       ,COALESCE(A.OLD_YEAR_DAYS, 0)                       AS OLD_YEAR_DAYS 
       ,D.REGBRNO               AS ORG_ID 
       ,COALESCE(A.MONTH_BAL_1, 0)                       AS MONTH_BAL_1 
       ,COALESCE(A.MONTH_BAL_2, 0)                       AS MONTH_BAL_2 
       ,COALESCE(A.MONTH_BAL_3, 0)                       AS MONTH_BAL_3 
       ,COALESCE(A.MONTH_BAL_4, 0)                       AS MONTH_BAL_4 
       ,COALESCE(A.MONTH_BAL_5, 0)                       AS MONTH_BAL_5 
       ,COALESCE(A.MONTH_BAL_6, 0)                       AS MONTH_BAL_6 
       ,COALESCE(A.MONTH_BAL_7, 0)                       AS MONTH_BAL_7 
       ,COALESCE(A.MONTH_BAL_8, 0)                       AS MONTH_BAL_8 
       ,COALESCE(A.MONTH_BAL_9, 0)                       AS MONTH_BAL_9 
       ,COALESCE(A.MONTH_BAL_10, 0)                       AS MONTH_BAL_10 
       ,COALESCE(A.MONTH_BAL_11, 0)                       AS MONTH_BAL_11 
       ,COALESCE(A.MONTH_BAL_12, 0)                       AS MONTH_BAL_12 
       ,D.FR_ID                 AS FR_ID 
   FROM F_TX_FIN_CUSTINFO D                                    --客户签约表
  INNER JOIN F_TX_FIN_SALEINFO C                               --认购明细表
     ON C.IDTYPE                = D.IDTYPE 
    AND C.IDNO                  = D.IDNO 
    AND C.FR_ID                 = D.FR_ID 
  INNER JOIN TMP_ACRM_F_RE_ACCSUMAVGINFO_01 E                      --系统来源中间表
     ON C.IDTYPE                = E.CERT_TYPE 
    AND C.IDNO                  = E.CERT_NO 
    AND C.ACCTNAME              = E.ODS_CUST_NAME 
    AND E.FR_ID                 = D.FR_ID 
   LEFT JOIN ACRM_F_RE_ACCSUMAVGINFO A                         --理财积数均值表
     ON A.ACCT_NO               = D.ACCTNO 
    AND A.FR_ID                 = C.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT_2_DAYS\b", "'"+V_DT_2_DAYS+"'", sql)
sql = re.sub(r"\bV_MONTH_DAYS\b", "'"+V_MONTH_DAYS+"'", sql)

ACRM_F_RE_ACCSUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO_INNTMP1")

ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.ACCT_NAME                                          --账户名称:src.ACCT_NAME
       ,DST.ACCT_NO                                            --帐号:src.ACCT_NO
       ,DST.CURR                                               --币种:src.CURR
       ,DST.PRDT_CODE                                          --产品号:src.PRDT_CODE
       ,DST.AMT                                                --余额:src.AMT
       ,DST.LAST_AMOUNT                                        --昨日余额:src.LAST_AMOUNT
       ,DST.YEAR                                               --年份:src.YEAR
       ,DST.MONTH_BAL_SUM_1                                    --1月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --1月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --2月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --2月天数:src.MONTH_DAYS_2
       ,DST.MONTH_BAL_SUM_3                                    --3月余额积数:src.MONTH_BAL_SUM_3
       ,DST.MONTH_DAYS_3                                       --3月天数:src.MONTH_DAYS_3
       ,DST.MONTH_BAL_SUM_4                                    --4月余额积数:src.MONTH_BAL_SUM_4
       ,DST.MONTH_DAYS_4                                       --4月天数:src.MONTH_DAYS_4
       ,DST.MONTH_BAL_SUM_5                                    --5月余额积数:src.MONTH_BAL_SUM_5
       ,DST.MONTH_DAYS_5                                       --5月天数:src.MONTH_DAYS_5
       ,DST.MONTH_BAL_SUM_6                                    --6月余额积数:src.MONTH_BAL_SUM_6
       ,DST.MONTH_DAYS_6                                       --6月天数:src.MONTH_DAYS_6
       ,DST.MONTH_BAL_SUM_7                                    --7月余额积数:src.MONTH_BAL_SUM_7
       ,DST.MONTH_DAYS_7                                       --7月天数:src.MONTH_DAYS_7
       ,DST.MONTH_BAL_SUM_8                                    --8月余额积数:src.MONTH_BAL_SUM_8
       ,DST.MONTH_DAYS_8                                       --8月天数:src.MONTH_DAYS_8
       ,DST.MONTH_BAL_SUM_9                                    --9月余额积数:src.MONTH_BAL_SUM_9
       ,DST.MONTH_DAYS_9                                       --9月天数:src.MONTH_DAYS_9
       ,DST.MONTH_BAL_SUM_10                                   --10月余额积数:src.MONTH_BAL_SUM_10
       ,DST.MONTH_DAYS_10                                      --10月天数:src.MONTH_DAYS_10
       ,DST.MONTH_BAL_SUM_11                                   --11月余额积数:src.MONTH_BAL_SUM_11
       ,DST.MONTH_DAYS_11                                      --11月天数:src.MONTH_DAYS_11
       ,DST.MONTH_BAL_SUM_12                                   --12月余额积数:src.MONTH_BAL_SUM_12
       ,DST.MONTH_DAYS_12                                      --12月天数:src.MONTH_DAYS_12
       ,DST.ODS_ST_DATE                                        --加工日期:src.ODS_ST_DATE
       ,DST.OLD_YEAR_BAL_SUM                                   --去年余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --年初余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --去年年天数:src.OLD_YEAR_DAYS
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.MONTH_BAL_1                                        --1月月末剩余金额（原币种）:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月月末剩余金额（原币种）:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月月末剩余金额（原币种）:src.MONTH_BAL_3
       ,DST.MONTH_BAL_4                                        --4月月末剩余金额（原币种）:src.MONTH_BAL_4
       ,DST.MONTH_BAL_5                                        --5月月末剩余金额（原币种）:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月月末剩余金额（原币种）:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月月末剩余金额（原币种）:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月月末剩余金额（原币种）:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月月末剩余金额（原币种）:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月月末剩余金额（原币种）:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月月末剩余金额（原币种）:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月月末剩余金额（原币种）:src.MONTH_BAL_12
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_F_RE_ACCSUMAVGINFO DST 
   LEFT JOIN ACRM_F_RE_ACCSUMAVGINFO_INNTMP1 SRC 
     ON SRC.ACCT_NO             = DST.ACCT_NO 
    AND SRC.YEAR                = DST.YEAR 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.ACCT_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_ACCSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2=ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_ACCSUMAVGINFO_INNTMP1)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.count()
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_ACCSUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO_BK/")

#任务[12] 001-05::
V_STEP = V_STEP + 1

ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,ACCT_NAME               AS ACCT_NAME 
       ,ACCT_NO                 AS ACCT_NO 
       ,CURR                    AS CURR 
       ,PRDT_CODE               AS PRDT_CODE 
       ,AMT                     AS AMT 
       ,LAST_AMOUNT             AS LAST_AMOUNT 
       ,YEAR                    AS YEAR 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 1 THEN MONTH_BAL_SUM_1 + AMT ELSE MONTH_BAL_SUM_1 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_1 
       ,CASE WHEN MONTH(V_DT)                       = 1 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_1 END                     AS MONTH_DAYS_1 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 2 THEN MONTH_BAL_SUM_2 + AMT ELSE MONTH_BAL_SUM_2 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_2 
       ,CASE WHEN MONTH(V_DT)                       = 2 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_2 END                     AS MONTH_DAYS_2 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 3 THEN MONTH_BAL_SUM_3 + AMT ELSE MONTH_BAL_SUM_3 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_3 
       ,CASE WHEN MONTH(V_DT)                       = 3 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_3 END                     AS MONTH_DAYS_3 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 4 THEN MONTH_BAL_SUM_4 + AMT ELSE MONTH_BAL_SUM_4 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_4 
       ,CASE WHEN MONTH(V_DT)                       = 4 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_4 END                     AS MONTH_DAYS_4 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 5 THEN MONTH_BAL_SUM_5 + AMT ELSE MONTH_BAL_SUM_5 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_5 
       ,CASE WHEN MONTH(V_DT)                       = 5 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_5 END                     AS MONTH_DAYS_5 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 6 THEN MONTH_BAL_SUM_6 + AMT ELSE MONTH_BAL_SUM_6 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_6 
       ,CASE WHEN MONTH(V_DT)                       = 6 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_6 END                     AS MONTH_DAYS_6 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 7 THEN MONTH_BAL_SUM_7 + AMT ELSE MONTH_BAL_SUM_7 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_7 
       ,CASE WHEN MONTH(V_DT)                       = 7 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_7 END                     AS MONTH_DAYS_7 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 8 THEN MONTH_BAL_SUM_8 + AMT ELSE MONTH_BAL_SUM_8 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_8 
       ,CASE WHEN MONTH(V_DT)                       = 8 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_8 END                     AS MONTH_DAYS_8 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 9 THEN MONTH_BAL_SUM_9 + AMT ELSE MONTH_BAL_SUM_9 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_9 
       ,CASE WHEN MONTH(V_DT)                       = 9 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_9 END                     AS MONTH_DAYS_9 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 10 THEN MONTH_BAL_SUM_10 + AMT ELSE MONTH_BAL_SUM_10 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_10 
       ,CASE WHEN MONTH(V_DT)                       = 10 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_10 END                     AS MONTH_DAYS_10 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 11 THEN MONTH_BAL_SUM_11 + AMT ELSE MONTH_BAL_SUM_11 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_11 
       ,CASE WHEN MONTH(V_DT)                       = 11 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_11 END                     AS MONTH_DAYS_11 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 12 THEN MONTH_BAL_SUM_12 + AMT ELSE MONTH_BAL_SUM_12 END AS DECIMAL(24,6))                     AS MONTH_BAL_SUM_12 
       ,CASE WHEN MONTH(V_DT)                       = 12 THEN CAST(rint(V_MONTH_DAYS) AS INT) ELSE MONTH_DAYS_12 END                     AS MONTH_DAYS_12 
       ,V_DT                    AS ODS_ST_DATE 
       ,OLD_YEAR_BAL_SUM        AS OLD_YEAR_BAL_SUM 
       ,OLD_YEAR_BAL            AS OLD_YEAR_BAL 
       ,OLD_YEAR_DAYS           AS OLD_YEAR_DAYS 
       ,ORG_ID                  AS ORG_ID 
       ,CASE WHEN MONTH(V_DT)                       = 1 THEN AMT ELSE MONTH_BAL_1 END                     AS MONTH_BAL_1 
       ,CASE WHEN MONTH(V_DT)                       = 2 THEN AMT ELSE MONTH_BAL_2 END                     AS MONTH_BAL_2 
       ,CASE WHEN MONTH(V_DT)                       = 3 THEN AMT ELSE MONTH_BAL_3 END                     AS MONTH_BAL_3 
       ,CASE WHEN MONTH(V_DT)                       = 4 THEN AMT ELSE MONTH_BAL_4 END                     AS MONTH_BAL_4 
       ,CASE WHEN MONTH(V_DT)                       = 5 THEN AMT ELSE MONTH_BAL_5 END                     AS MONTH_BAL_5 
       ,CASE WHEN MONTH(V_DT)                       = 6 THEN AMT ELSE MONTH_BAL_6 END                     AS MONTH_BAL_6 
       ,CASE WHEN MONTH(V_DT)                       = 7 THEN AMT ELSE MONTH_BAL_7 END                     AS MONTH_BAL_7 
       ,CASE WHEN MONTH(V_DT)                       = 8 THEN AMT ELSE MONTH_BAL_8 END                     AS MONTH_BAL_8 
       ,CASE WHEN MONTH(V_DT)                       = 9 THEN AMT ELSE MONTH_BAL_9 END                     AS MONTH_BAL_9 
       ,CASE WHEN MONTH(V_DT)                       = 10 THEN AMT ELSE MONTH_BAL_10 END                     AS MONTH_BAL_10 
       ,CASE WHEN MONTH(V_DT)                       = 11 THEN AMT ELSE MONTH_BAL_11 END                     AS MONTH_BAL_11 
       ,CASE WHEN MONTH(V_DT)                       = 12 THEN AMT ELSE MONTH_BAL_12 END                     AS MONTH_BAL_12 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_ACCSUMAVGINFO A                              --理财积数均值表
  WHERE YEAR                    = YEAR(V_DT) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT_2_DAYS\b", "'"+V_DT_2_DAYS+"'", sql)
sql = re.sub(r"\bV_MONTH_DAYS\b", "'"+V_MONTH_DAYS+"'", sql)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO_INNTMP1")

ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.ACCT_NAME                                          --账户名称:src.ACCT_NAME
       ,DST.ACCT_NO                                            --帐号:src.ACCT_NO
       ,DST.CURR                                               --币种:src.CURR
       ,DST.PRDT_CODE                                          --产品号:src.PRDT_CODE
       ,DST.AMT                                                --余额:src.AMT
       ,DST.LAST_AMOUNT                                        --昨日余额:src.LAST_AMOUNT
       ,DST.YEAR                                               --年份:src.YEAR
       ,DST.MONTH_BAL_SUM_1                                    --1月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --1月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --2月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --2月天数:src.MONTH_DAYS_2
       ,DST.MONTH_BAL_SUM_3                                    --3月余额积数:src.MONTH_BAL_SUM_3
       ,DST.MONTH_DAYS_3                                       --3月天数:src.MONTH_DAYS_3
       ,DST.MONTH_BAL_SUM_4                                    --4月余额积数:src.MONTH_BAL_SUM_4
       ,DST.MONTH_DAYS_4                                       --4月天数:src.MONTH_DAYS_4
       ,DST.MONTH_BAL_SUM_5                                    --5月余额积数:src.MONTH_BAL_SUM_5
       ,DST.MONTH_DAYS_5                                       --5月天数:src.MONTH_DAYS_5
       ,DST.MONTH_BAL_SUM_6                                    --6月余额积数:src.MONTH_BAL_SUM_6
       ,DST.MONTH_DAYS_6                                       --6月天数:src.MONTH_DAYS_6
       ,DST.MONTH_BAL_SUM_7                                    --7月余额积数:src.MONTH_BAL_SUM_7
       ,DST.MONTH_DAYS_7                                       --7月天数:src.MONTH_DAYS_7
       ,DST.MONTH_BAL_SUM_8                                    --8月余额积数:src.MONTH_BAL_SUM_8
       ,DST.MONTH_DAYS_8                                       --8月天数:src.MONTH_DAYS_8
       ,DST.MONTH_BAL_SUM_9                                    --9月余额积数:src.MONTH_BAL_SUM_9
       ,DST.MONTH_DAYS_9                                       --9月天数:src.MONTH_DAYS_9
       ,DST.MONTH_BAL_SUM_10                                   --10月余额积数:src.MONTH_BAL_SUM_10
       ,DST.MONTH_DAYS_10                                      --10月天数:src.MONTH_DAYS_10
       ,DST.MONTH_BAL_SUM_11                                   --11月余额积数:src.MONTH_BAL_SUM_11
       ,DST.MONTH_DAYS_11                                      --11月天数:src.MONTH_DAYS_11
       ,DST.MONTH_BAL_SUM_12                                   --12月余额积数:src.MONTH_BAL_SUM_12
       ,DST.MONTH_DAYS_12                                      --12月天数:src.MONTH_DAYS_12
       ,DST.ODS_ST_DATE                                        --加工日期:src.ODS_ST_DATE
       ,DST.OLD_YEAR_BAL_SUM                                   --去年余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --年初余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --去年年天数:src.OLD_YEAR_DAYS
       ,DST.ORG_ID                                             --机构号:src.ORG_ID
       ,DST.MONTH_BAL_1                                        --1月月末剩余金额（原币种）:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月月末剩余金额（原币种）:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月月末剩余金额（原币种）:src.MONTH_BAL_3
       ,DST.MONTH_BAL_4                                        --4月月末剩余金额（原币种）:src.MONTH_BAL_4
       ,DST.MONTH_BAL_5                                        --5月月末剩余金额（原币种）:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月月末剩余金额（原币种）:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月月末剩余金额（原币种）:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月月末剩余金额（原币种）:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月月末剩余金额（原币种）:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月月末剩余金额（原币种）:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月月末剩余金额（原币种）:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月月末剩余金额（原币种）:src.MONTH_BAL_12
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_F_RE_ACCSUMAVGINFO DST 
   LEFT JOIN ACRM_F_RE_ACCSUMAVGINFO_INNTMP1 SRC 
     ON SRC.ACCT_NO             = DST.ACCT_NO 
    AND SRC.YEAR                = DST.YEAR 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.ACCT_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_ACCSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2=ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_ACCSUMAVGINFO_INNTMP1)
ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.count()

#装载数据
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO_BK/"+V_DT+".parquet")

ACRM_F_RE_ACCSUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_ACCSUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_ACCSUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_ACCSUMAVGINFO_BK/")
