#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_RE_LENDSUMAVGINFO').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_LENDSUMAVGINFO/*.parquet")
#恢复数据到今日数据文件
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_RE_LENDSUMAVGINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_LENDSUMAVGINFO/"+V_DT+".parquet")


F_DP_CBOD_LNLNSLNS = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_LNLNSLNS/*')
F_DP_CBOD_LNLNSLNS.registerTempTable("F_DP_CBOD_LNLNSLNS")
F_LN_XDXT_BUSINESS_DUEBILL = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BUSINESS_DUEBILL/*')
F_LN_XDXT_BUSINESS_DUEBILL.registerTempTable("F_LN_XDXT_BUSINESS_DUEBILL")
ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,ACCOUNT                 AS ACCOUNT 
       ,OPEN_BRC                AS OPEN_BRC 
       ,PRDT_CODE               AS PRDT_CODE 
       ,MONEY_TYPE              AS MONEY_TYPE 
       ,AMOUNT                  AS AMOUNT 
       ,LAST_AMOUNT             AS LAST_AMOUNT 
       ,CAST(YEAR(V_DT) AS VARCHAR(4))                       AS YEAR 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_1 
       ,CAST(0   AS INTEGER)                    AS MONTH_DAYS_1 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_2 
       ,CAST(0   AS INTEGER)                       AS MONTH_DAYS_2 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_3 
       ,CAST(0   AS INTEGER)                       AS MONTH_DAYS_3 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_4 
       ,CAST(0   AS INTEGER)                       AS MONTH_DAYS_4 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_5 
       ,CAST(0   AS INTEGER)                       AS MONTH_DAYS_5 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_6 
       ,CAST(0   AS INTEGER)                       AS MONTH_DAYS_6 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_7 
       ,CAST(0   AS INTEGER)                       AS MONTH_DAYS_7 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_8 
       ,CAST(0   AS INTEGER)                       AS MONTH_DAYS_8 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_9 
       ,CAST(0   AS INTEGER)                       AS MONTH_DAYS_9 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_10 
       ,CAST(0   AS INTEGER)                       AS MONTH_DAYS_10 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_11 
       ,CAST(0   AS INTEGER)                       AS MONTH_DAYS_11 
       ,CAST(0 AS DECIMAL(24,6))                       AS MONTH_BAL_SUM_12 
       ,CAST(0   AS INTEGER)                       AS MONTH_DAYS_12 
       ,V_DT                    AS ODS_ST_DATE 
       ,CAST(MONTH_BAL_SUM_12 AS DECIMAL(24,6))        AS OLD_YEAR_BAL_SUM 
       ,MONTH_BAL_12            AS OLD_YEAR_BAL 
       ,CAST(MONTH_DAYS_12 AS INTEGER)          AS OLD_YEAR_DAYS 
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
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_LENDSUMAVGINFO A                             --贷款账户积数表
  WHERE YEAR                    = YEAR(V_DT) - 1 
    AND AMOUNT > 0 
    AND V_DT                    = CONCAT(SUBSTR(V_DT,1,4),'-01-01') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_LENDSUMAVGINFO = sqlContext.sql(sql)
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")
dfn="ACRM_F_RE_LENDSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_LENDSUMAVGINFO.cache()
nrows = ACRM_F_RE_LENDSUMAVGINFO.count()
ACRM_F_RE_LENDSUMAVGINFO.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_RE_LENDSUMAVGINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_LENDSUMAVGINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1

ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,ACCOUNT                 AS ACCOUNT 
       ,OPEN_BRC                AS OPEN_BRC 
       ,PRDT_CODE               AS PRDT_CODE 
       ,MONEY_TYPE              AS MONEY_TYPE 
       ,AMOUNT                  AS AMOUNT 
       ,CASE WHEN YEAR = YEAR(V_DT) THEN AMOUNT ELSE  LAST_AMOUNT END                  AS LAST_AMOUNT 
       ,YEAR                    AS YEAR 
       ,MONTH_BAL_SUM_1         AS MONTH_BAL_SUM_1 
       ,CAST(MONTH_DAYS_1  AS INTEGER)          AS MONTH_DAYS_1 
       ,MONTH_BAL_SUM_2         AS MONTH_BAL_SUM_2 
       ,CAST(MONTH_DAYS_2  AS INTEGER)          AS MONTH_DAYS_2 
       ,MONTH_BAL_SUM_3         AS MONTH_BAL_SUM_3 
       ,CAST(MONTH_DAYS_3  AS INTEGER)          AS MONTH_DAYS_3 
       ,MONTH_BAL_SUM_4         AS MONTH_BAL_SUM_4 
       ,CAST(MONTH_DAYS_4  AS INTEGER)          AS MONTH_DAYS_4 
       ,MONTH_BAL_SUM_5         AS MONTH_BAL_SUM_5 
       ,CAST(MONTH_DAYS_5   AS INTEGER)         AS MONTH_DAYS_5 
       ,MONTH_BAL_SUM_6         AS MONTH_BAL_SUM_6 
       ,CAST(MONTH_DAYS_6   AS INTEGER)         AS MONTH_DAYS_6 
       ,MONTH_BAL_SUM_7         AS MONTH_BAL_SUM_7 
       ,CAST(MONTH_DAYS_7  AS INTEGER)          AS MONTH_DAYS_7 
       ,MONTH_BAL_SUM_8         AS MONTH_BAL_SUM_8 
       ,CAST(MONTH_DAYS_8  AS INTEGER)          AS MONTH_DAYS_8 
       ,MONTH_BAL_SUM_9         AS MONTH_BAL_SUM_9 
       ,CAST(MONTH_DAYS_9  AS INTEGER)          AS MONTH_DAYS_9 
       ,MONTH_BAL_SUM_10        AS MONTH_BAL_SUM_10 
       ,CAST(MONTH_DAYS_10  AS INTEGER)         AS MONTH_DAYS_10 
       ,MONTH_BAL_SUM_11        AS MONTH_BAL_SUM_11 
       ,CAST(MONTH_DAYS_11  AS INTEGER)         AS MONTH_DAYS_11 
       ,MONTH_BAL_SUM_12        AS MONTH_BAL_SUM_12 
       ,CAST(MONTH_DAYS_12  AS INTEGER)         AS MONTH_DAYS_12 
       ,CASE WHEN YEAR = YEAR(V_DT) THEN V_DT ELSE  ODS_ST_DATE END            AS ODS_ST_DATE 
       ,OLD_YEAR_BAL_SUM        AS OLD_YEAR_BAL_SUM 
       ,OLD_YEAR_BAL            AS OLD_YEAR_BAL 
       ,OLD_YEAR_DAYS           AS OLD_YEAR_DAYS 
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
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_LENDSUMAVGINFO A                          --贷款账户积数表
    """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_LENDSUMAVGINFO = sqlContext.sql(sql)
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")
dfn="ACRM_F_RE_LENDSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_LENDSUMAVGINFO.cache()
nrows = ACRM_F_RE_LENDSUMAVGINFO.count()
ACRM_F_RE_LENDSUMAVGINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_LENDSUMAVGINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_LENDSUMAVGINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-03::
V_STEP = V_STEP + 1

ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")

sql = """
 SELECT B.LN_CUST_NO            AS CUST_ID 
       ,B.LN_CUST_NAME          AS CUST_NAME 
       ,B.LN_LN_BAL              AS ACCOUNT 
       ,B.LN_FLST_OPUN_NO       AS OPEN_BRC 
       ,B.BUSINESSTYPE          AS PRDT_CODE 
       ,B.LN_CURR_COD           AS MONEY_TYPE 
       ,CAST(B.LN_LN_BAL AS DECIMAL(24,6))     AS AMOUNT 
       ,COALESCE(A.LAST_AMOUNT, 0)                AS LAST_AMOUNT 
       ,CAST(YEAR(V_DT) AS VARCHAR(4))               AS YEAR 
       ,COALESCE(A.MONTH_BAL_SUM_1, 0)                AS MONTH_BAL_SUM_1 
       ,CASE WHEN MONTH(V_DT)                = 1 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 1 THEN 31 ELSE 0 END                     AS MONTH_DAYS_1 
       ,COALESCE(A.MONTH_BAL_SUM_2, 0)                AS MONTH_BAL_SUM_2 
       ,CASE WHEN MONTH(V_DT)                = 2 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 2 THEN CAST(rint(V_DT_2_DAYS) AS INTEGER) ELSE 0 END      AS MONTH_DAYS_2 
       ,COALESCE(A.MONTH_BAL_SUM_3, 0)                AS MONTH_BAL_SUM_3 
       ,CASE WHEN MONTH(V_DT)                = 3 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 3 THEN 31 ELSE 0 END                     AS MONTH_DAYS_3 
       ,COALESCE(A.MONTH_BAL_SUM_4, 0)                AS MONTH_BAL_SUM_4 
       ,CASE WHEN MONTH(V_DT)                = 4 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 4 THEN 30 ELSE 0 END                     AS MONTH_DAYS_4 
       ,COALESCE(A.MONTH_BAL_SUM_5, 0)                AS MONTH_BAL_SUM_5 
       ,CASE WHEN MONTH(V_DT)                = 5 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 5 THEN 31 ELSE 0 END                     AS MONTH_DAYS_5 
       ,COALESCE(A.MONTH_BAL_SUM_6, 0)                AS MONTH_BAL_SUM_6 
       ,CASE WHEN MONTH(V_DT)                = 6 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 6 THEN 30 ELSE 0 END                     AS MONTH_DAYS_6 
       ,COALESCE(A.MONTH_BAL_SUM_7, 0)                AS MONTH_BAL_SUM_7 
       ,CASE WHEN MONTH(V_DT)                = 7 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 7 THEN 31 ELSE 0 END                     AS MONTH_DAYS_7 
       ,COALESCE(A.MONTH_BAL_SUM_8, 0)                AS MONTH_BAL_SUM_8 
       ,CASE WHEN MONTH(V_DT)                = 8 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 8 THEN 31 ELSE 0 END                     AS MONTH_DAYS_8 
       ,COALESCE(A.MONTH_BAL_SUM_9, 0)                AS MONTH_BAL_SUM_9 
       ,CASE WHEN MONTH(V_DT)                = 9 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 9 THEN 30 ELSE 0 END                     AS MONTH_DAYS_9 
       ,COALESCE(A.MONTH_BAL_SUM_10, 0)                AS MONTH_BAL_SUM_10 
       ,CASE WHEN MONTH(V_DT)                = 10 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 10 THEN 31 ELSE 0 END                     AS MONTH_DAYS_10 
       ,COALESCE(A.MONTH_BAL_SUM_11, 0)                AS MONTH_BAL_SUM_11 
       ,CASE WHEN MONTH(V_DT)                = 11 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) WHEN MONTH(V_DT) > 11 THEN 30 ELSE 0 END                     AS MONTH_DAYS_11 
       ,COALESCE(A.MONTH_BAL_SUM_12, 0)                AS MONTH_BAL_SUM_12 
       ,CASE WHEN MONTH(V_DT)                = 12 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE 0 END                     AS MONTH_DAYS_12 
       ,V_DT                    AS ODS_ST_DATE 
       ,COALESCE(A.OLD_YEAR_BAL_SUM, 0)                       AS OLD_YEAR_BAL_SUM 
       ,COALESCE(A.OLD_YEAR_BAL, 0)                       AS OLD_YEAR_BAL 
       ,COALESCE(A.OLD_YEAR_DAYS, 0)                       AS OLD_YEAR_DAYS 
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
       ,SUBSTR(B.LN_CUST_NO, 1, 1)                       AS CUST_TYP 
       ,COALESCE(B.FR_ID,'UNK')                      AS FR_ID 
   FROM (SELECT F.SERIALNO ACCT_NO ,F.SUBJECTNO,F.BUSINESSTYPE ,C.LN_CUST_NO,
				 C.LN_CUST_NAME,C.LN_FLST_OPUN_NO,C.LN_BUSN_TYP,C.LN_CURR_COD,C.LN_LN_BAL,C.FR_ID
				   FROM F_DP_CBOD_LNLNSLNS C 
                   JOIN F_LN_XDXT_BUSINESS_DUEBILL F 
				  ON F.SERIALNO=C.LN_LN_ACCT_NO AND C.LN_APCL_FLG='N' 
                  AND TRIM(F.BUSINESSTYPE) <> '' 
                  AND F.BUSINESSTYPE IS NOT NULL 
                  AND F.BUSINESSTYPE <> '000000'
                  AND C.FR_ID=F.FR_ID 
                   WHERE   (  F.SUBJECTNO LIKE '1301%'
                            OR   F.SUBJECTNO LIKE '1302%'
                            OR   F.SUBJECTNO LIKE '1303%'
                            OR   F.SUBJECTNO LIKE '1304%'
                            OR   F.SUBJECTNO LIKE '1305%'
                            OR   F.SUBJECTNO LIKE '1306%'
                            OR   F.SUBJECTNO LIKE '1307%'
                            OR   F.SUBJECTNO LIKE '1308%')
                  AND  C.ODS_ST_DATE=V_DT  ) B
   LEFT JOIN ACRM_F_RE_LENDSUMAVGINFO A                        --贷款账户积数表
     ON A.YEAR = YEAR(V_DT) 
	 AND A.ACCOUNT = B.ACCT_NO 
	 AND A.FR_ID=B.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_MONTH_DAYS\b", "'"+V_MONTH_DAYS+"'", sql)
sql = re.sub(r"\bV_DT_2_DAYS\b", "'"+V_DT_2_DAYS+"'", sql)
ACRM_F_RE_LENDSUMAVGINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_LENDSUMAVGINFO_INNTMP1.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO_INNTMP1")



sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.ACCOUNT                                            --账号:src.ACCOUNT
       ,DST.OPEN_BRC                                           --机构号(账号):src.OPEN_BRC
       ,DST.PRDT_CODE                                          --产品代码:src.PRDT_CODE
       ,DST.MONEY_TYPE                                         --币种:src.MONEY_TYPE
       ,DST.AMOUNT                                             --本日余额(原币种):src.AMOUNT
       ,DST.LAST_AMOUNT                                        --昨日余额(原币种):src.LAST_AMOUNT
       ,DST.YEAR                                               --积数年份:src.YEAR
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
       ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
       ,DST.OLD_YEAR_BAL_SUM                                   --年初余额积数:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --年初余额:src.OLD_YEAR_BAL
       ,DST.OLD_YEAR_DAYS                                      --上年天数合计:src.OLD_YEAR_DAYS
       ,DST.MONTH_BAL_1                                        --1月余额积数:src.MONTH_BAL_1
       ,DST.MONTH_BAL_2                                        --2月余额积数:src.MONTH_BAL_2
       ,DST.MONTH_BAL_3                                        --3月余额积数:src.MONTH_BAL_3
       ,DST.MONTH_BAL_4                                        --4月余额积数:src.MONTH_BAL_4
       ,DST.MONTH_BAL_5                                        --5月余额积数:src.MONTH_BAL_5
       ,DST.MONTH_BAL_6                                        --6月余额积数:src.MONTH_BAL_6
       ,DST.MONTH_BAL_7                                        --7月余额积数:src.MONTH_BAL_7
       ,DST.MONTH_BAL_8                                        --8月余额积数:src.MONTH_BAL_8
       ,DST.MONTH_BAL_9                                        --9月余额积数:src.MONTH_BAL_9
       ,DST.MONTH_BAL_10                                       --10月余额积数:src.MONTH_BAL_10
       ,DST.MONTH_BAL_11                                       --11月余额积数:src.MONTH_BAL_11
       ,DST.MONTH_BAL_12                                       --12月余额积数:src.MONTH_BAL_12
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_F_RE_LENDSUMAVGINFO DST 
   LEFT JOIN ACRM_F_RE_LENDSUMAVGINFO_INNTMP1 SRC 
     ON SRC.YEAR                = DST.YEAR 
	AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.ACCOUNT             = DST.ACCOUNT 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_LENDSUMAVGINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_LENDSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_LENDSUMAVGINFO_INNTMP2=ACRM_F_RE_LENDSUMAVGINFO_INNTMP2.unionAll(ACRM_F_RE_LENDSUMAVGINFO_INNTMP1)
ACRM_F_RE_LENDSUMAVGINFO_INNTMP1.cache()
ACRM_F_RE_LENDSUMAVGINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_LENDSUMAVGINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_LENDSUMAVGINFO_INNTMP2.count()
ACRM_F_RE_LENDSUMAVGINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_LENDSUMAVGINFO_INNTMP1.unpersist()
ACRM_F_RE_LENDSUMAVGINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_LENDSUMAVGINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_LENDSUMAVGINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_LENDSUMAVGINFO_BK/")

#任务[12] 001-04::
V_STEP = V_STEP + 1

ACRM_F_RE_LENDSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_LENDSUMAVGINFO/*')
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_NAME               AS CUST_NAME 
       ,ACCOUNT                 AS ACCOUNT 
       ,OPEN_BRC                AS OPEN_BRC 
       ,PRDT_CODE               AS PRDT_CODE 
       ,MONEY_TYPE              AS MONEY_TYPE 
       ,AMOUNT                  AS AMOUNT 
       ,LAST_AMOUNT             AS LAST_AMOUNT 
       ,YEAR                    AS YEAR 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 1 THEN CAST(MONTH_BAL_SUM_1 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_1 END                     AS MONTH_BAL_SUM_1 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 1 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_1 END                     AS MONTH_DAYS_1 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 2 THEN CAST(MONTH_BAL_SUM_2 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_2 END                     AS MONTH_BAL_SUM_2 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 2 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_2 END                     AS MONTH_DAYS_2 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 3 THEN CAST(MONTH_BAL_SUM_3 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_3 END                     AS MONTH_BAL_SUM_3 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 3 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_3 END                     AS MONTH_DAYS_3 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 4 THEN CAST(MONTH_BAL_SUM_4 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_4 END                     AS MONTH_BAL_SUM_4 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 4 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_4 END                     AS MONTH_DAYS_4 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 5 THEN CAST(MONTH_BAL_SUM_5 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_5 END                     AS MONTH_BAL_SUM_5 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 5 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_5 END                     AS MONTH_DAYS_5 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 6 THEN CAST(MONTH_BAL_SUM_6 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_6 END                     AS MONTH_BAL_SUM_6 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 6 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_6 END                     AS MONTH_DAYS_6 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 7 THEN CAST(MONTH_BAL_SUM_7 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_7 END                     AS MONTH_BAL_SUM_7 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 7 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_7 END                     AS MONTH_DAYS_7 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 8 THEN CAST(MONTH_BAL_SUM_8 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_8 END                     AS MONTH_BAL_SUM_8 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 8 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_8 END                     AS MONTH_DAYS_8 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 9 THEN CAST(MONTH_BAL_SUM_9 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_9 END                     AS MONTH_BAL_SUM_9 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 9 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_9 END                     AS MONTH_DAYS_9 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 10 THEN CAST(MONTH_BAL_SUM_10 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_10 END                     AS MONTH_BAL_SUM_10 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 10 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_10 END                     AS MONTH_DAYS_10 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 11 THEN CAST(MONTH_BAL_SUM_11 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_11 END                     AS MONTH_BAL_SUM_11 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 11 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_11 END                     AS MONTH_DAYS_11 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 12 THEN CAST(MONTH_BAL_SUM_12 + AMOUNT AS DECIMAL(24,6)) ELSE MONTH_BAL_SUM_12 END                     AS MONTH_BAL_SUM_12 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT) = 12 THEN CAST(rint(V_MONTH_DAYS) AS INTEGER) ELSE MONTH_DAYS_12 END                     AS MONTH_DAYS_12 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
       ,OLD_YEAR_BAL_SUM        AS OLD_YEAR_BAL_SUM 
       ,OLD_YEAR_BAL            AS OLD_YEAR_BAL 
       ,OLD_YEAR_DAYS           AS OLD_YEAR_DAYS 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 1 THEN AMOUNT ELSE MONTH_BAL_1 END                     AS MONTH_BAL_1 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 2 THEN AMOUNT ELSE MONTH_BAL_2 END                     AS MONTH_BAL_2 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 3 THEN AMOUNT ELSE MONTH_BAL_3 END                     AS MONTH_BAL_3 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 4 THEN AMOUNT ELSE MONTH_BAL_4 END                     AS MONTH_BAL_4 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 5 THEN AMOUNT ELSE MONTH_BAL_5 END                     AS MONTH_BAL_5 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 6 THEN AMOUNT ELSE MONTH_BAL_6 END                     AS MONTH_BAL_6 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 7 THEN AMOUNT ELSE MONTH_BAL_7 END                     AS MONTH_BAL_7 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 8 THEN AMOUNT ELSE MONTH_BAL_8 END                     AS MONTH_BAL_8 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 9 THEN AMOUNT ELSE MONTH_BAL_9 END                     AS MONTH_BAL_9 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 10 THEN AMOUNT ELSE MONTH_BAL_10 END                     AS MONTH_BAL_10 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 11 THEN AMOUNT ELSE MONTH_BAL_11 END                     AS MONTH_BAL_11 
       ,CASE WHEN YEAR=YEAR(V_DT) AND MONTH(V_DT)                       = 12 THEN AMOUNT ELSE MONTH_BAL_12 END                     AS MONTH_BAL_12 
       ,CUST_TYP                AS CUST_TYP 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_LENDSUMAVGINFO A                             --贷款账户积数表
   """
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_MONTH_DAYS\b", "'"+V_MONTH_DAYS+"'", sql)
ACRM_F_RE_LENDSUMAVGINFO = sqlContext.sql(sql)
ACRM_F_RE_LENDSUMAVGINFO.registerTempTable("ACRM_F_RE_LENDSUMAVGINFO")
dfn="ACRM_F_RE_LENDSUMAVGINFO/"+V_DT+".parquet"
ACRM_F_RE_LENDSUMAVGINFO.cache()
nrows = ACRM_F_RE_LENDSUMAVGINFO.count()
ACRM_F_RE_LENDSUMAVGINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_LENDSUMAVGINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_LENDSUMAVGINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_LENDSUMAVGINFO_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_RE_LENDSUMAVGINFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_RE_LENDSUMAVGINFO_BK/"+V_DT+".parquet")
