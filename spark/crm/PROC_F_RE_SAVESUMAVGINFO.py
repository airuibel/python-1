#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_RE_SAVESUMAVGINFO').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO/*.parquet")
#恢复数据到今日数据文件
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO/"+V_DT+".parquet")


F_DP_CBOD_TDACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_TDACNACN/*')
F_DP_CBOD_TDACNACN.registerTempTable("F_DP_CBOD_TDACNACN")
F_DP_CBOD_SAACNAMT = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNAMT/*')
F_DP_CBOD_SAACNAMT.registerTempTable("F_DP_CBOD_SAACNAMT")
F_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNACN/*')
F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")
ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")


#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,TYPE                    AS TYPE 
       ,CURR                    AS CURR 
       ,CURR_IDEN               AS CURR_IDEN 
       ,ACCT_NO                 AS ACCT_NO 
       ,OPEN_BRC                AS OPEN_BRC 
       ,PRDT_CODE               AS PRDT_CODE 
       ,AMOUNT                  AS AMOUNT 
       ,CAST(0 AS DECIMAL(24,6))                       AS LAST_AMOUNT 
       ,CAST(YEAR(V_DT) AS VARCHAR(4))                       AS YEAR 
       ,CAST(MONTH_BAL_SUM_12 AS DECIMAL(24,6))       AS OLD_YEAR_BAL_SUM 
       ,MONTH_BAL_12            AS OLD_YEAR_BAL 
       ,CAST(MONTH_DAYS_12 AS INTEGER)           AS OLD_YEAR_DAYS 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_1 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_1 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_2 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_2 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_3 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_3 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_4 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_4 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_5 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_5 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_6 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_6 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_7 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_7 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_8 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_8 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_9 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_9 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_10 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_10 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_11 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_11 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_12 
       ,CAST( 0 AS INTEGER)                       AS MONTH_DAYS_12 
       ,V_DT                    AS ODS_ST_DATE 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_4 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_1 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_2 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_3 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_5 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_6 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_7 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_8 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_9 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_10 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_11 
       ,CAST( 0 AS DECIMAL(24,6))                       AS MONTH_BAL_12 
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
       ,CAST( 0 AS DECIMAL(24,6))                       AS YEAR_BAL_SUM 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --存款积数表
  WHERE YEAR                    = YEAR(V_DT) - 1 
    AND V_DT                    = TRUNC(V_DT, 'Y') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO = sqlContext.sql(sql)
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")
dfn="ACRM_F_RE_SAVESUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_SAVESUMAVGINFO.cache()
nrows = ACRM_F_RE_SAVESUMAVGINFO.count()
ACRM_F_RE_SAVESUMAVGINFO.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_RE_SAVESUMAVGINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_SAVESUMAVGINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1

ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")

sql = """
 SELECT DISTINCT A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,'H'                     AS TYPE 
       ,CURR                    AS CURR 
       ,CURR_IDEN               AS CURR_IDEN 
       ,ACCT_NO                 AS ACCT_NO 
       ,A.OPEN_BRC              AS OPEN_BRC 
       ,A.PRDT_CODE             AS PRDT_CODE 
       ,A.AMOUNT                AS AMOUNT 
       ,AMOUNT                  AS LAST_AMOUNT 
       ,CAST(YEAR(V_DT) AS VARCHAR(4)) AS YEAR 
       ,A.OLD_YEAR_BAL_SUM      AS OLD_YEAR_BAL_SUM 
       ,A.OLD_YEAR_BAL          AS OLD_YEAR_BAL 
       ,A.OLD_YEAR_DAYS         AS OLD_YEAR_DAYS 
       ,A.MONTH_BAL_SUM_1       AS MONTH_BAL_SUM_1 
       ,A.MONTH_DAYS_1          AS MONTH_DAYS_1 
       ,A.MONTH_BAL_SUM_2       AS MONTH_BAL_SUM_2 
       ,A.MONTH_DAYS_2          AS MONTH_DAYS_2 
       ,A.MONTH_BAL_SUM_3       AS MONTH_BAL_SUM_3 
       ,A.MONTH_DAYS_3          AS MONTH_DAYS_3 
       ,A.MONTH_BAL_SUM_4       AS MONTH_BAL_SUM_4 
       ,A.MONTH_DAYS_4          AS MONTH_DAYS_4 
       ,A.MONTH_BAL_SUM_5       AS MONTH_BAL_SUM_5 
       ,A.MONTH_DAYS_5          AS MONTH_DAYS_5 
       ,A.MONTH_BAL_SUM_6       AS MONTH_BAL_SUM_6 
       ,A.MONTH_DAYS_6          AS MONTH_DAYS_6 
       ,A.MONTH_BAL_SUM_7       AS MONTH_BAL_SUM_7 
       ,A.MONTH_DAYS_7          AS MONTH_DAYS_7 
       ,A.MONTH_BAL_SUM_8       AS MONTH_BAL_SUM_8 
       ,A.MONTH_DAYS_8          AS MONTH_DAYS_8 
       ,A.MONTH_BAL_SUM_9       AS MONTH_BAL_SUM_9 
       ,A.MONTH_DAYS_9          AS MONTH_DAYS_9 
       ,A.MONTH_BAL_SUM_10      AS MONTH_BAL_SUM_10 
       ,A.MONTH_DAYS_10         AS MONTH_DAYS_10 
       ,A.MONTH_BAL_SUM_11      AS MONTH_BAL_SUM_11 
       ,A.MONTH_DAYS_11         AS MONTH_DAYS_11 
       ,A.MONTH_BAL_SUM_12      AS MONTH_BAL_SUM_12 
       ,A.MONTH_DAYS_12         AS MONTH_DAYS_12 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.MONTH_BAL_4           AS MONTH_BAL_4 
       ,A.MONTH_BAL_1           AS MONTH_BAL_1 
       ,A.MONTH_BAL_2           AS MONTH_BAL_2 
       ,A.MONTH_BAL_3           AS MONTH_BAL_3 
       ,A.MONTH_BAL_5           AS MONTH_BAL_5 
       ,A.MONTH_BAL_6           AS MONTH_BAL_6 
       ,A.MONTH_BAL_7           AS MONTH_BAL_7 
       ,A.MONTH_BAL_8           AS MONTH_BAL_8 
       ,A.MONTH_BAL_9           AS MONTH_BAL_9 
       ,A.MONTH_BAL_10          AS MONTH_BAL_10 
       ,A.MONTH_BAL_11          AS MONTH_BAL_11 
       ,A.MONTH_BAL_12          AS MONTH_BAL_12 
       ,A.CUST_TYP              AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
       ,A.YEAR_BAL_SUM          AS YEAR_BAL_SUM 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --存款积数表
  WHERE YEAR                    = YEAR(V_DT) 
    AND TYPE                    = 'H' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO_INNTMP1")


sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.TYPE                                               --存款类型:src.TYPE
       ,DST.CURR                                               --币种:src.CURR
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.ACCT_NO                                            --帐号:src.ACCT_NO
       ,DST.OPEN_BRC                                           --开户机构:src.OPEN_BRC
       ,DST.PRDT_CODE                                          --产品:src.PRDT_CODE
       ,DST.AMOUNT                                             --当日余额:src.AMOUNT
       ,DST.LAST_AMOUNT                                        --昨日余额:src.LAST_AMOUNT
       ,DST.YEAR                                               --年度:src.YEAR
       ,DST.OLD_YEAR_BAL_SUM                                   --去年12月余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --去年年末余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --去年12月天数:src.OLD_YEAR_DAYS
       ,DST.MONTH_BAL_SUM_1                                    --一月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --一月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --二月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --二月天数:src.MONTH_DAYS_2
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
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.MONTH_BAL_4                                        --4月底余额:src.MONTH_BAL_4
       ,DST.MONTH_BAL_1                                        --1月底余额:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月底余额:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月底余额:src.MONTH_BAL_3
       ,DST.MONTH_BAL_5                                        --5月底余额:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月底余额:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月底余额:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月底余额:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月底余额:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月底余额:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月底余额:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月底余额:src.MONTH_BAL_12
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.YEAR_BAL_SUM                                       --年余额积数:src.YEAR_BAL_SUM
   FROM ACRM_F_RE_SAVESUMAVGINFO DST 
   WHERE YEAR = YEAR(V_DT) -1 OR TYPE = 'D' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_SAVESUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2=ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_SAVESUMAVGINFO_INNTMP1)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.count()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_SAVESUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO_BK/")

#任务[12] 001-03::
V_STEP = V_STEP + 1

ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")

sql = """
 SELECT DISTINCT A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,'D'                     AS TYPE 
       ,A.CURR                  AS CURR 
       ,A.CURR_IDEN             AS CURR_IDEN 
       ,ACCT_NO                 AS ACCT_NO 
       ,A.OPEN_BRC              AS OPEN_BRC 
       ,A.PRDT_CODE             AS PRDT_CODE 
       ,A.AMOUNT                AS AMOUNT 
       ,AMOUNT                  AS LAST_AMOUNT 
       ,CAST(YEAR(V_DT) AS VARCHAR(4))                      AS YEAR 
       ,A.OLD_YEAR_BAL_SUM      AS OLD_YEAR_BAL_SUM 
       ,A.OLD_YEAR_BAL          AS OLD_YEAR_BAL 
       ,A.OLD_YEAR_DAYS         AS OLD_YEAR_DAYS 
       ,A.MONTH_BAL_SUM_1       AS MONTH_BAL_SUM_1 
       ,A.MONTH_DAYS_1          AS MONTH_DAYS_1 
       ,A.MONTH_BAL_SUM_2       AS MONTH_BAL_SUM_2 
       ,A.MONTH_DAYS_2          AS MONTH_DAYS_2 
       ,A.MONTH_BAL_SUM_3       AS MONTH_BAL_SUM_3 
       ,A.MONTH_DAYS_3          AS MONTH_DAYS_3 
       ,A.MONTH_BAL_SUM_4       AS MONTH_BAL_SUM_4 
       ,A.MONTH_DAYS_4          AS MONTH_DAYS_4 
       ,A.MONTH_BAL_SUM_5       AS MONTH_BAL_SUM_5 
       ,A.MONTH_DAYS_5          AS MONTH_DAYS_5 
       ,A.MONTH_BAL_SUM_6       AS MONTH_BAL_SUM_6 
       ,A.MONTH_DAYS_6          AS MONTH_DAYS_6 
       ,A.MONTH_BAL_SUM_7       AS MONTH_BAL_SUM_7 
       ,A.MONTH_DAYS_7          AS MONTH_DAYS_7 
       ,A.MONTH_BAL_SUM_8       AS MONTH_BAL_SUM_8 
       ,A.MONTH_DAYS_8          AS MONTH_DAYS_8 
       ,A.MONTH_BAL_SUM_9       AS MONTH_BAL_SUM_9 
       ,A.MONTH_DAYS_9          AS MONTH_DAYS_9 
       ,A.MONTH_BAL_SUM_10      AS MONTH_BAL_SUM_10 
       ,A.MONTH_DAYS_10         AS MONTH_DAYS_10 
       ,A.MONTH_BAL_SUM_11      AS MONTH_BAL_SUM_11 
       ,A.MONTH_DAYS_11         AS MONTH_DAYS_11 
       ,A.MONTH_BAL_SUM_12      AS MONTH_BAL_SUM_12 
       ,A.MONTH_DAYS_12         AS MONTH_DAYS_12 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.MONTH_BAL_4           AS MONTH_BAL_4 
       ,A.MONTH_BAL_1           AS MONTH_BAL_1 
       ,A.MONTH_BAL_2           AS MONTH_BAL_2 
       ,A.MONTH_BAL_3           AS MONTH_BAL_3 
       ,A.MONTH_BAL_5           AS MONTH_BAL_5 
       ,A.MONTH_BAL_6           AS MONTH_BAL_6 
       ,A.MONTH_BAL_7           AS MONTH_BAL_7 
       ,A.MONTH_BAL_8           AS MONTH_BAL_8 
       ,A.MONTH_BAL_9           AS MONTH_BAL_9 
       ,A.MONTH_BAL_10          AS MONTH_BAL_10 
       ,A.MONTH_BAL_11          AS MONTH_BAL_11 
       ,A.MONTH_BAL_12          AS MONTH_BAL_12 
       ,A.CUST_TYP              AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
       ,A.YEAR_BAL_SUM          AS YEAR_BAL_SUM 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --存款积数表
  WHERE YEAR                    = YEAR(V_DT) 
    AND TYPE                    = 'D' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO_INNTMP1")

sql = """
 SELECT DISTINCT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.TYPE                                               --存款类型:src.TYPE
       ,DST.CURR                                               --币种:src.CURR
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.ACCT_NO                                            --帐号:src.ACCT_NO
       ,DST.OPEN_BRC                                           --开户机构:src.OPEN_BRC
       ,DST.PRDT_CODE                                          --产品:src.PRDT_CODE
       ,DST.AMOUNT                                             --当日余额:src.AMOUNT
       ,DST.LAST_AMOUNT                                        --昨日余额:src.LAST_AMOUNT
       ,DST.YEAR                                               --年度:src.YEAR
       ,DST.OLD_YEAR_BAL_SUM                                   --去年12月余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --去年年末余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --去年12月天数:src.OLD_YEAR_DAYS
       ,DST.MONTH_BAL_SUM_1                                    --一月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --一月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --二月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --二月天数:src.MONTH_DAYS_2
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
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.MONTH_BAL_4                                        --4月底余额:src.MONTH_BAL_4
       ,DST.MONTH_BAL_1                                        --1月底余额:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月底余额:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月底余额:src.MONTH_BAL_3
       ,DST.MONTH_BAL_5                                        --5月底余额:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月底余额:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月底余额:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月底余额:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月底余额:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月底余额:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月底余额:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月底余额:src.MONTH_BAL_12
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.YEAR_BAL_SUM                                       --年余额积数:src.YEAR_BAL_SUM
   FROM ACRM_F_RE_SAVESUMAVGINFO DST 
   WHERE YEAR = YEAR(V_DT) - 1
    OR TYPE = 'H'  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_SAVESUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2=ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_SAVESUMAVGINFO_INNTMP1)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.count()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_SAVESUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO_BK/")

#任务[12] 001-04::
V_STEP = V_STEP + 1

ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")

sql = """
 SELECT C.SA_CUST_NO            AS CUST_ID 
       ,C.SA_CUST_NAME          AS CUST_NAME 
       ,'H'                     AS TYPE 
       ,D.SA_CURR_COD           AS CURR 
       ,D.SA_CURR_IDEN          AS CURR_IDEN 
       ,C.SA_ACCT_NO            AS ACCT_NO 
       ,C.SA_BELONG_INSTN_COD   AS OPEN_BRC 
       ,C.SA_PDP_CODE           AS PRDT_CODE 
       ,CAST(D.SA_ACCT_BAL AS DECIMAL(24,6))          AS AMOUNT 
       ,A.LAST_AMOUNT           AS LAST_AMOUNT 
       ,CAST(YEAR(V_DT) AS VARCHAR(4))                      AS YEAR 
       ,COALESCE(A.OLD_YEAR_BAL_SUM, 0)                       AS OLD_YEAR_BAL_SUM 
       ,COALESCE(A.OLD_YEAR_BAL, 0)                       AS OLD_YEAR_BAL 
       ,CAST(COALESCE(A.OLD_YEAR_DAYS, 0) AS INTEGER)                       AS OLD_YEAR_DAYS 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_1, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_1 
       ,CASE WHEN MONTH(V_DT)                       = 1 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 1 THEN 31 ELSE 0 END                     AS MONTH_DAYS_1 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_2, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_2 
       ,CASE WHEN MONTH(V_DT)                       = 2 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 2 THEN CAST(rint(V_DT_2_DAYS) AS INTEGER) ELSE 0 END                     AS MONTH_DAYS_2 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_3, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_3 
       ,CASE WHEN MONTH(V_DT)                       = 3 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 3 THEN 31 ELSE 0 END                     AS MONTH_DAYS_3 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_4, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_4 
       ,CASE WHEN MONTH(V_DT)                       = 4 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 4 THEN 30 ELSE 0 END                     AS MONTH_DAYS_4 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_5, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_5 
       ,CASE WHEN MONTH(V_DT)                       = 5 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 5 THEN 31 ELSE 0 END                     AS MONTH_DAYS_5 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_6, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_6 
       ,CASE WHEN MONTH(V_DT)                       = 6 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 6 THEN 30 ELSE 0 END                     AS MONTH_DAYS_6 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_7, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_7 
       ,CASE WHEN MONTH(V_DT)                       = 7 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 7 THEN 31 ELSE 0 END                     AS MONTH_DAYS_7 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_8, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_8 
       ,CASE WHEN MONTH(V_DT)                       = 8 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 8 THEN 31 ELSE 0 END                     AS MONTH_DAYS_8 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_9, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_9 
       ,CASE WHEN MONTH(V_DT)                       = 9 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 9 THEN 30 ELSE 0 END                     AS MONTH_DAYS_9 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_10, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_10 
       ,CASE WHEN MONTH(V_DT)                       = 10 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 10 THEN 31 ELSE 0 END                     AS MONTH_DAYS_10 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_11, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_11 
       ,CASE WHEN MONTH(V_DT)                       = 11 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 11 THEN 30 ELSE 0 END                     AS MONTH_DAYS_11 
       ,CAST(COALESCE(A.MONTH_BAL_SUM_12, 0) AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_12 
       ,CASE WHEN MONTH(V_DT)                       = 12 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE 0 END                     AS MONTH_DAYS_12 
       ,V_DT                    AS ODS_ST_DATE 
       ,COALESCE(A.MONTH_BAL_4, 0)                       AS MONTH_BAL_4 
       ,COALESCE(A.MONTH_BAL_1, 0)                       AS MONTH_BAL_1 
       ,COALESCE(A.MONTH_BAL_2, 0)                       AS MONTH_BAL_2 
       ,COALESCE(A.MONTH_BAL_3, 0)                       AS MONTH_BAL_3 
       ,COALESCE(A.MONTH_BAL_5, 0)                       AS MONTH_BAL_5 
       ,COALESCE(A.MONTH_BAL_6, 0)                       AS MONTH_BAL_6 
       ,COALESCE(A.MONTH_BAL_7, 0)                       AS MONTH_BAL_7 
       ,COALESCE(A.MONTH_BAL_8, 0)                       AS MONTH_BAL_8 
       ,COALESCE(A.MONTH_BAL_9, 0)                       AS MONTH_BAL_9 
       ,COALESCE(A.MONTH_BAL_10, 0)                       AS MONTH_BAL_10 
       ,COALESCE(A.MONTH_BAL_11, 0)                       AS MONTH_BAL_11 
       ,COALESCE(A.MONTH_BAL_12, 0)                       AS MONTH_BAL_12 
       ,SUBSTR(C.SA_CUST_NO, 1, 1)                       AS CUST_TYP 
       ,C.FR_ID                 AS FR_ID 
       ,A.YEAR_BAL_SUM          AS YEAR_BAL_SUM 
   FROM F_DP_CBOD_SAACNACN C                                   --活存主档
  INNER JOIN F_DP_CBOD_SAACNAMT D                              --活存资金档
     ON C.SA_ACCT_NO            = D.FK_SAACN_KEY 
    AND C.FR_ID                 = D.FR_ID 
   LEFT JOIN ACRM_F_RE_SAVESUMAVGINFO A                        --存款积数表
     ON A.FR_ID                 = C.FR_ID 
    AND A.YEAR                  = YEAR(V_DT) 
    AND A.ACCT_NO               = C.SA_ACCT_NO 
    AND A.CURR                  = D.SA_CURR_COD 
    AND A.CURR_IDEN             = D.SA_CURR_IDEN 
    AND A.TYPE                  = 'H' 
  WHERE C.SA_CUST_NO <> 'X9999999999999999999' 
    AND C.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_MONTH_DAYS\b", "'"+V_MONTH_DAYS+"'", sql)
sql = re.sub(r"\bV_DT_2_DAYS\b", "'"+V_DT_2_DAYS+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO_INNTMP1")


sql = """
 SELECT DISTINCT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.TYPE                                               --存款类型:src.TYPE
       ,DST.CURR                                               --币种:src.CURR
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.ACCT_NO                                            --帐号:src.ACCT_NO
       ,DST.OPEN_BRC                                           --开户机构:src.OPEN_BRC
       ,DST.PRDT_CODE                                          --产品:src.PRDT_CODE
       ,DST.AMOUNT                                             --当日余额:src.AMOUNT
       ,DST.LAST_AMOUNT                                        --昨日余额:src.LAST_AMOUNT
       ,DST.YEAR                                               --年度:src.YEAR
       ,DST.OLD_YEAR_BAL_SUM                                   --去年12月余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --去年年末余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --去年12月天数:src.OLD_YEAR_DAYS
       ,DST.MONTH_BAL_SUM_1                                    --一月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --一月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --二月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --二月天数:src.MONTH_DAYS_2
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
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.MONTH_BAL_4                                        --4月底余额:src.MONTH_BAL_4
       ,DST.MONTH_BAL_1                                        --1月底余额:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月底余额:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月底余额:src.MONTH_BAL_3
       ,DST.MONTH_BAL_5                                        --5月底余额:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月底余额:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月底余额:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月底余额:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月底余额:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月底余额:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月底余额:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月底余额:src.MONTH_BAL_12
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.YEAR_BAL_SUM                                       --年余额积数:src.YEAR_BAL_SUM
   FROM ACRM_F_RE_SAVESUMAVGINFO DST 
   LEFT JOIN ACRM_F_RE_SAVESUMAVGINFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ACCT_NO             = DST.ACCT_NO 
    AND SRC.TYPE                = DST.TYPE 
    AND SRC.CURR                = DST.CURR 
    AND SRC.CURR_IDEN           = DST.CURR_IDEN 
    AND SRC.YEAR                = DST.YEAR 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_SAVESUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2=ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_SAVESUMAVGINFO_INNTMP1)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.count()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_SAVESUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO_BK/")

#任务[12] 001-05::
V_STEP = V_STEP + 1

ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")

sql = """
 SELECT D.TD_CUST_NO            AS CUST_ID 
       ,D.TD_CUST_NAME          AS CUST_NAME 
       ,'D'                     AS TYPE 
       ,D.TD_CURR_COD           AS CURR 
       ,'3'                     AS CURR_IDEN 
       ,D.TD_TD_ACCT_NO         AS ACCT_NO 
       ,D.TD_BELONG_INSTN_COD   AS OPEN_BRC 
       ,D.TD_PDP_CODE           AS PRDT_CODE 
       ,CAST(D.TD_ACTU_AMT AS DECIMAL(24,6))          AS AMOUNT 
       ,A.LAST_AMOUNT           AS LAST_AMOUNT 
       ,CAST(YEAR(V_DT) AS VARCHAR(4))                      AS YEAR 
       ,COALESCE(A.OLD_YEAR_BAL_SUM, 0)                       AS OLD_YEAR_BAL_SUM 
       ,COALESCE(A.OLD_YEAR_BAL, 0)                       AS OLD_YEAR_BAL 
       ,CAST(COALESCE(A.OLD_YEAR_DAYS, 0) AS INTEGER)                      AS OLD_YEAR_DAYS 
       ,COALESCE(A.MONTH_BAL_SUM_1, 0)                       AS MONTH_BAL_SUM_1 
       ,CASE WHEN MONTH(V_DT)                       = 1 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 1 THEN 31 ELSE 0 END                     AS MONTH_DAYS_1 
       ,COALESCE(A.MONTH_BAL_SUM_2, 0)                       AS MONTH_BAL_SUM_2 
       ,CASE WHEN MONTH(V_DT)                       = 2 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 2 THEN CAST(rint(V_DT_2_DAYS) AS INTEGER) ELSE 0 END                     AS MONTH_DAYS_2 
       ,COALESCE(A.MONTH_BAL_SUM_3, 0)                       AS MONTH_BAL_SUM_3 
       ,CASE WHEN MONTH(V_DT)                       = 3 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 3 THEN 31 ELSE 0 END                     AS MONTH_DAYS_3 
       ,COALESCE(A.MONTH_BAL_SUM_4, 0)                       AS MONTH_BAL_SUM_4 
       ,CASE WHEN MONTH(V_DT)                       = 4 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 4 THEN 30 ELSE 0 END                     AS MONTH_DAYS_4 
       ,COALESCE(A.MONTH_BAL_SUM_5, 0)                       AS MONTH_BAL_SUM_5 
       ,CASE WHEN MONTH(V_DT)                       = 5 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 5 THEN 31 ELSE 0 END                     AS MONTH_DAYS_5 
       ,COALESCE(A.MONTH_BAL_SUM_6, 0)                       AS MONTH_BAL_SUM_6 
       ,CASE WHEN MONTH(V_DT)                       = 6 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 6 THEN 30 ELSE 0 END                     AS MONTH_DAYS_6 
       ,COALESCE(A.MONTH_BAL_SUM_7, 0)                       AS MONTH_BAL_SUM_7 
       ,CASE WHEN MONTH(V_DT)                       = 7 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 7 THEN 31 ELSE 0 END                     AS MONTH_DAYS_7 
       ,COALESCE(A.MONTH_BAL_SUM_8, 0)                       AS MONTH_BAL_SUM_8 
       ,CASE WHEN MONTH(V_DT)                       = 8 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 8 THEN 31 ELSE 0 END                     AS MONTH_DAYS_8 
       ,COALESCE(A.MONTH_BAL_SUM_9, 0)                       AS MONTH_BAL_SUM_9 
       ,CASE WHEN MONTH(V_DT)                       = 9 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 9 THEN 30 ELSE 0 END                     AS MONTH_DAYS_9 
       ,COALESCE(A.MONTH_BAL_SUM_10, 0)                       AS MONTH_BAL_SUM_10 
       ,CASE WHEN MONTH(V_DT)                       = 10 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 10 THEN 31 ELSE 0 END                     AS MONTH_DAYS_10 
       ,COALESCE(A.MONTH_BAL_SUM_11, 0)                       AS MONTH_BAL_SUM_11 
       ,CASE WHEN MONTH(V_DT)                       = 11 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 11 THEN 30 ELSE 0 END                     AS MONTH_DAYS_11 
       ,COALESCE(A.MONTH_BAL_SUM_12, 0)                       AS MONTH_BAL_SUM_12 
       ,CASE WHEN MONTH(V_DT)                       = 12 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE 0 END                     AS MONTH_DAYS_12 
       ,V_DT                    AS ODS_ST_DATE 
       ,COALESCE(A.MONTH_BAL_4, 0)                       AS MONTH_BAL_4 
       ,COALESCE(A.MONTH_BAL_1, 0)                       AS MONTH_BAL_1 
       ,COALESCE(A.MONTH_BAL_2, 0)                       AS MONTH_BAL_2 
       ,COALESCE(A.MONTH_BAL_3, 0)                       AS MONTH_BAL_3 
       ,COALESCE(A.MONTH_BAL_5, 0)                       AS MONTH_BAL_5 
       ,COALESCE(A.MONTH_BAL_6, 0)                       AS MONTH_BAL_6 
       ,COALESCE(A.MONTH_BAL_7, 0)                       AS MONTH_BAL_7 
       ,COALESCE(A.MONTH_BAL_8, 0)                       AS MONTH_BAL_8 
       ,COALESCE(A.MONTH_BAL_9, 0)                       AS MONTH_BAL_9 
       ,COALESCE(A.MONTH_BAL_10, 0)                       AS MONTH_BAL_10 
       ,COALESCE(A.MONTH_BAL_11, 0)                       AS MONTH_BAL_11 
       ,COALESCE(A.MONTH_BAL_12, 0)                       AS MONTH_BAL_12 
       ,SUBSTR(D.TD_CUST_NO, 1, 1)                       AS CUST_TYP 
       ,D.FR_ID                 AS FR_ID 
       ,A.YEAR_BAL_SUM          AS YEAR_BAL_SUM 
   FROM F_DP_CBOD_TDACNACN D                                   --定存主档
   LEFT JOIN ACRM_F_RE_SAVESUMAVGINFO A                        --存款积数表
     ON A.FR_ID                 = D.FR_ID 
    AND A.YEAR                  = YEAR(V_DT) 
    AND A.ACCT_NO               = D.TD_TD_ACCT_NO 
    AND A.CURR                  = D.TD_CURR_COD 
    AND A.TYPE                  = 'D' 
  WHERE TD_ACCT_STS             = '01' 
    AND D.ODS_ST_DATE           = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_MONTH_DAYS\b", "'"+V_MONTH_DAYS+"'", sql)
sql = re.sub(r"\bV_DT_2_DAYS\b", "'"+V_DT_2_DAYS+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO_INNTMP1")


sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.TYPE                                               --存款类型:src.TYPE
       ,DST.CURR                                               --币种:src.CURR
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.ACCT_NO                                            --帐号:src.ACCT_NO
       ,DST.OPEN_BRC                                           --开户机构:src.OPEN_BRC
       ,DST.PRDT_CODE                                          --产品:src.PRDT_CODE
       ,DST.AMOUNT                                             --当日余额:src.AMOUNT
       ,DST.LAST_AMOUNT                                        --昨日余额:src.LAST_AMOUNT
       ,DST.YEAR                                               --年度:src.YEAR
       ,DST.OLD_YEAR_BAL_SUM                                   --去年12月余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --去年年末余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --去年12月天数:src.OLD_YEAR_DAYS
       ,DST.MONTH_BAL_SUM_1                                    --一月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --一月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --二月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --二月天数:src.MONTH_DAYS_2
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
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.MONTH_BAL_4                                        --4月底余额:src.MONTH_BAL_4
       ,DST.MONTH_BAL_1                                        --1月底余额:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月底余额:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月底余额:src.MONTH_BAL_3
       ,DST.MONTH_BAL_5                                        --5月底余额:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月底余额:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月底余额:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月底余额:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月底余额:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月底余额:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月底余额:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月底余额:src.MONTH_BAL_12
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.YEAR_BAL_SUM                                       --年余额积数:src.YEAR_BAL_SUM
   FROM ACRM_F_RE_SAVESUMAVGINFO DST 
   LEFT JOIN ACRM_F_RE_SAVESUMAVGINFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ACCT_NO             = DST.ACCT_NO 
    AND SRC.TYPE                = DST.TYPE 
    AND SRC.CURR                = DST.CURR 
    AND SRC.YEAR                = DST.YEAR 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_SAVESUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2=ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_SAVESUMAVGINFO_INNTMP1)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.count()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_SAVESUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO_BK/")

#任务[12] 001-06::
V_STEP = V_STEP + 1

ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")

sql = """
 SELECT B.CUST_ID               AS CUST_ID 
       ,B.CUST_NAME             AS CUST_NAME 
       ,'D'                     AS TYPE 
       ,B.CURR                  AS CURR 
       ,B.CURR_IDEN             AS CURR_IDEN 
       ,TD_TD_ACCT_NO           AS ACCT_NO 
       ,B.OPEN_BRC              AS OPEN_BRC 
       ,B.PRDT_CODE             AS PRDT_CODE 
       ,CAST(0 AS DECIMAL(24,6))                       AS AMOUNT 
       ,B.LAST_AMOUNT           AS LAST_AMOUNT 
       ,CAST(YEAR(V_DT) AS VARCHAR(4))                       AS YEAR 
       ,B.OLD_YEAR_BAL_SUM      AS OLD_YEAR_BAL_SUM 
       ,B.OLD_YEAR_BAL          AS OLD_YEAR_BAL 
       ,B.OLD_YEAR_DAYS         AS OLD_YEAR_DAYS 
       ,B.MONTH_BAL_SUM_1       AS MONTH_BAL_SUM_1 
       ,B.MONTH_DAYS_1          AS MONTH_DAYS_1 
       ,B.MONTH_BAL_SUM_2       AS MONTH_BAL_SUM_2 
       ,B.MONTH_DAYS_2          AS MONTH_DAYS_2 
       ,B.MONTH_BAL_SUM_3       AS MONTH_BAL_SUM_3 
       ,B.MONTH_DAYS_3          AS MONTH_DAYS_3 
       ,B.MONTH_BAL_SUM_4       AS MONTH_BAL_SUM_4 
       ,B.MONTH_DAYS_4          AS MONTH_DAYS_4 
       ,B.MONTH_BAL_SUM_5       AS MONTH_BAL_SUM_5 
       ,B.MONTH_DAYS_5          AS MONTH_DAYS_5 
       ,B.MONTH_BAL_SUM_6       AS MONTH_BAL_SUM_6 
       ,B.MONTH_DAYS_6          AS MONTH_DAYS_6 
       ,B.MONTH_BAL_SUM_7       AS MONTH_BAL_SUM_7 
       ,B.MONTH_DAYS_7          AS MONTH_DAYS_7 
       ,B.MONTH_BAL_SUM_8       AS MONTH_BAL_SUM_8 
       ,B.MONTH_DAYS_8          AS MONTH_DAYS_8 
       ,B.MONTH_BAL_SUM_9       AS MONTH_BAL_SUM_9 
       ,B.MONTH_DAYS_9          AS MONTH_DAYS_9 
       ,B.MONTH_BAL_SUM_10      AS MONTH_BAL_SUM_10 
       ,B.MONTH_DAYS_10         AS MONTH_DAYS_10 
       ,B.MONTH_BAL_SUM_11      AS MONTH_BAL_SUM_11 
       ,B.MONTH_DAYS_11         AS MONTH_DAYS_11 
       ,B.MONTH_BAL_SUM_12      AS MONTH_BAL_SUM_12 
       ,B.MONTH_DAYS_12         AS MONTH_DAYS_12 
       ,B.ODS_ST_DATE           AS ODS_ST_DATE 
       ,B.MONTH_BAL_4           AS MONTH_BAL_4 
       ,B.MONTH_BAL_1           AS MONTH_BAL_1 
       ,B.MONTH_BAL_2           AS MONTH_BAL_2 
       ,B.MONTH_BAL_3           AS MONTH_BAL_3 
       ,B.MONTH_BAL_5           AS MONTH_BAL_5 
       ,B.MONTH_BAL_6           AS MONTH_BAL_6 
       ,B.MONTH_BAL_7           AS MONTH_BAL_7 
       ,B.MONTH_BAL_8           AS MONTH_BAL_8 
       ,B.MONTH_BAL_9           AS MONTH_BAL_9 
       ,B.MONTH_BAL_10          AS MONTH_BAL_10 
       ,B.MONTH_BAL_11          AS MONTH_BAL_11 
       ,B.MONTH_BAL_12          AS MONTH_BAL_12 
       ,B.CUST_TYP              AS CUST_TYP 
       ,A.FR_ID                 AS FR_ID 
       ,B.YEAR_BAL_SUM          AS YEAR_BAL_SUM 
   FROM F_DP_CBOD_TDACNACN A                                   --定存主档
  INNER JOIN ACRM_F_RE_SAVESUMAVGINFO B                        --存款积数表
     ON A.FR_ID                 = B.FR_ID 
    AND B.ACCT_NO               = A.TD_TD_ACCT_NO 
    AND B.YEAR                  = YEAR(V_DT) 
    AND B.TYPE                  = 'D' 
  WHERE A.TD_ACCT_STS <> '01' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO_INNTMP1")


sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.TYPE                                               --存款类型:src.TYPE
       ,DST.CURR                                               --币种:src.CURR
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.ACCT_NO                                            --帐号:src.ACCT_NO
       ,DST.OPEN_BRC                                           --开户机构:src.OPEN_BRC
       ,DST.PRDT_CODE                                          --产品:src.PRDT_CODE
       ,DST.AMOUNT                                             --当日余额:src.AMOUNT
       ,DST.LAST_AMOUNT                                        --昨日余额:src.LAST_AMOUNT
       ,DST.YEAR                                               --年度:src.YEAR
       ,DST.OLD_YEAR_BAL_SUM                                   --去年12月余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --去年年末余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --去年12月天数:src.OLD_YEAR_DAYS
       ,DST.MONTH_BAL_SUM_1                                    --一月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --一月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --二月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --二月天数:src.MONTH_DAYS_2
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
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.MONTH_BAL_4                                        --4月底余额:src.MONTH_BAL_4
       ,DST.MONTH_BAL_1                                        --1月底余额:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月底余额:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月底余额:src.MONTH_BAL_3
       ,DST.MONTH_BAL_5                                        --5月底余额:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月底余额:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月底余额:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月底余额:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月底余额:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月底余额:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月底余额:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月底余额:src.MONTH_BAL_12
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.YEAR_BAL_SUM                                       --年余额积数:src.YEAR_BAL_SUM
   FROM ACRM_F_RE_SAVESUMAVGINFO DST 
   LEFT JOIN ACRM_F_RE_SAVESUMAVGINFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.ACCT_NO             = DST.ACCT_NO 
    AND SRC.TYPE                = DST.TYPE 
    AND SRC.YEAR                = DST.YEAR 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_SAVESUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2=ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_SAVESUMAVGINFO_INNTMP1)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.count()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_SAVESUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO_BK/")

#任务[12] 001-07::
V_STEP = V_STEP + 1

ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")

sql = """
 SELECT DISTINCT CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,TYPE                    AS TYPE 
       ,CURR                    AS CURR 
       ,CURR_IDEN               AS CURR_IDEN 
       ,ACCT_NO                 AS ACCT_NO 
       ,OPEN_BRC                AS OPEN_BRC 
       ,PRDT_CODE               AS PRDT_CODE 
       ,AMOUNT                  AS AMOUNT 
       ,LAST_AMOUNT             AS LAST_AMOUNT 
       ,YEAR                    AS YEAR 
       ,OLD_YEAR_BAL_SUM        AS OLD_YEAR_BAL_SUM 
       ,OLD_YEAR_BAL            AS OLD_YEAR_BAL 
       ,OLD_YEAR_DAYS           AS OLD_YEAR_DAYS 
       ,CASE WHEN MONTH(V_DT)                       = 1 THEN CAST(MONTH_BAL_SUM_1 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_1 END                     AS MONTH_BAL_SUM_1 
       ,CASE WHEN MONTH(V_DT)                       = 1 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_1 END                     AS MONTH_DAYS_1 
       ,CASE WHEN MONTH(V_DT)                       = 2 THEN CAST(MONTH_BAL_SUM_2 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_2 END                     AS MONTH_BAL_SUM_2 
       ,CASE WHEN MONTH(V_DT)                       = 2 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE CAST(MONTH_DAYS_2 AS INTEGER) END                     AS MONTH_DAYS_2 
       ,CASE WHEN MONTH(V_DT)                       = 3 THEN CAST(MONTH_BAL_SUM_3 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_3 END                     AS MONTH_BAL_SUM_3 
       ,CASE WHEN MONTH(V_DT)                       = 3 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_3 END                     AS MONTH_DAYS_3 
       ,CASE WHEN MONTH(V_DT)                       = 4 THEN CAST(MONTH_BAL_SUM_4 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_4 END                     AS MONTH_BAL_SUM_4 
       ,CASE WHEN MONTH(V_DT)                       = 4 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_4 END                     AS MONTH_DAYS_4 
       ,CASE WHEN MONTH(V_DT)                       = 5 THEN CAST(MONTH_BAL_SUM_5 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_5 END                     AS MONTH_BAL_SUM_5 
       ,CASE WHEN MONTH(V_DT)                       = 5 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_5 END                     AS MONTH_DAYS_5 
       ,CASE WHEN MONTH(V_DT)                       = 6 THEN CAST(MONTH_BAL_SUM_6 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_6 END                     AS MONTH_BAL_SUM_6 
       ,CASE WHEN MONTH(V_DT)                       = 6 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_6 END                     AS MONTH_DAYS_6 
       ,CASE WHEN MONTH(V_DT)                       = 7 THEN CAST(MONTH_BAL_SUM_7 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_7 END                     AS MONTH_BAL_SUM_7 
       ,CASE WHEN MONTH(V_DT)                       = 7 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_7 END                     AS MONTH_DAYS_7 
       ,CASE WHEN MONTH(V_DT)                       = 8 THEN CAST(MONTH_BAL_SUM_8 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_8 END                     AS MONTH_BAL_SUM_8 
       ,CASE WHEN MONTH(V_DT)                       = 8 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_8 END                     AS MONTH_DAYS_8 
       ,CASE WHEN MONTH(V_DT)                       = 9 THEN CAST(MONTH_BAL_SUM_9 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_9 END                     AS MONTH_BAL_SUM_9 
       ,CASE WHEN MONTH(V_DT)                       = 9 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_9 END                     AS MONTH_DAYS_9 
       ,CASE WHEN MONTH(V_DT)                       = 10 THEN CAST(MONTH_BAL_SUM_10 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_10 END                     AS MONTH_BAL_SUM_10 
       ,CASE WHEN MONTH(V_DT)                       = 10 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_10 END                     AS MONTH_DAYS_10 
       ,CASE WHEN MONTH(V_DT)                       = 11 THEN CAST(MONTH_BAL_SUM_11 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_11 END                     AS MONTH_BAL_SUM_11 
       ,CASE WHEN MONTH(V_DT)                       = 11 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_11 END                     AS MONTH_DAYS_11 
       ,CASE WHEN MONTH(V_DT)                       = 12 THEN CAST(MONTH_BAL_SUM_12 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_12 END                     AS MONTH_BAL_SUM_12 
       ,CASE WHEN MONTH(V_DT)                       = 12 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_12 END                     AS MONTH_DAYS_12 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
       ,CASE WHEN MONTH(V_DT)                       = 4 THEN AMOUNT ELSE MONTH_BAL_4 END                     AS MONTH_BAL_4 
       ,CASE WHEN MONTH(V_DT)                       = 1 THEN AMOUNT ELSE MONTH_BAL_1 END                     AS MONTH_BAL_1 
       ,CASE WHEN MONTH(V_DT)                       = 2 THEN AMOUNT ELSE MONTH_BAL_2 END                     AS MONTH_BAL_2 
       ,CASE WHEN MONTH(V_DT)                       = 3 THEN AMOUNT ELSE MONTH_BAL_3 END                     AS MONTH_BAL_3 
       ,CASE WHEN MONTH(V_DT)                       = 5 THEN AMOUNT ELSE MONTH_BAL_5 END                     AS MONTH_BAL_5 
       ,CASE WHEN MONTH(V_DT)                       = 6 THEN AMOUNT ELSE MONTH_BAL_6 END                     AS MONTH_BAL_6 
       ,CASE WHEN MONTH(V_DT)                       = 7 THEN AMOUNT ELSE MONTH_BAL_7 END                     AS MONTH_BAL_7 
       ,CASE WHEN MONTH(V_DT)                       = 8 THEN AMOUNT ELSE MONTH_BAL_8 END                     AS MONTH_BAL_8 
       ,CASE WHEN MONTH(V_DT)                       = 9 THEN AMOUNT ELSE MONTH_BAL_9 END                     AS MONTH_BAL_9 
       ,CASE WHEN MONTH(V_DT)                       = 10 THEN AMOUNT ELSE MONTH_BAL_10 END                     AS MONTH_BAL_10 
       ,CASE WHEN MONTH(V_DT)                       = 11 THEN AMOUNT ELSE MONTH_BAL_11 END                     AS MONTH_BAL_11 
       ,CASE WHEN MONTH(V_DT)                       = 12 THEN AMOUNT ELSE MONTH_BAL_12 END                     AS MONTH_BAL_12 
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
       ,CAST(YEAR_BAL_SUM + AMOUNT AS DECIMAL(24,6))                 AS YEAR_BAL_SUM 
   FROM ACRM_F_RE_SAVESUMAVGINFO A                             --存款积数表
  WHERE YEAR                    = YEAR(V_DT) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_MONTH_DAYS\b", "'"+V_MONTH_DAYS+"'", sql)
sql = re.sub(r"\bV_DT_2_DAYS\b", "'"+V_DT_2_DAYS+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO_INNTMP1")


sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.TYPE                                               --存款类型:src.TYPE
       ,DST.CURR                                               --币种:src.CURR
       ,DST.CURR_IDEN                                          --钞汇标志:src.CURR_IDEN
       ,DST.ACCT_NO                                            --帐号:src.ACCT_NO
       ,DST.OPEN_BRC                                           --开户机构:src.OPEN_BRC
       ,DST.PRDT_CODE                                          --产品:src.PRDT_CODE
       ,DST.AMOUNT                                             --当日余额:src.AMOUNT
       ,DST.LAST_AMOUNT                                        --昨日余额:src.LAST_AMOUNT
       ,DST.YEAR                                               --年度:src.YEAR
       ,DST.OLD_YEAR_BAL_SUM                                   --去年12月余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --去年年末余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --去年12月天数:src.OLD_YEAR_DAYS
       ,DST.MONTH_BAL_SUM_1                                    --一月余额积数:src.MONTH_BAL_SUM_1
       ,DST.MONTH_DAYS_1                                       --一月天数:src.MONTH_DAYS_1
       ,DST.MONTH_BAL_SUM_2                                    --二月余额积数:src.MONTH_BAL_SUM_2
       ,DST.MONTH_DAYS_2                                       --二月天数:src.MONTH_DAYS_2
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
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.MONTH_BAL_4                                        --4月底余额:src.MONTH_BAL_4
       ,DST.MONTH_BAL_1                                        --1月底余额:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月底余额:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月底余额:src.MONTH_BAL_3
       ,DST.MONTH_BAL_5                                        --5月底余额:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月底余额:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月底余额:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月底余额:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月底余额:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月底余额:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月底余额:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月底余额:src.MONTH_BAL_12
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.YEAR_BAL_SUM                                       --年余额积数:src.YEAR_BAL_SUM
   FROM ACRM_F_RE_SAVESUMAVGINFO DST 
    WHERE YEAR = YEAR(V_DT) - 1  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_SAVESUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2=ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_SAVESUMAVGINFO_INNTMP1)
ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.count()

#装载数据
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO_BK/"+V_DT+".parquet")


ACRM_F_RE_SAVESUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_SAVESUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_SAVESUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_SAVESUMAVGINFO_BK/")
