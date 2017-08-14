#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_RE_INSUSUMINFO').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_INSUSUMINFO/*.parquet")
#恢复数据到今日数据文件
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_RE_INSUSUMINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_INSUSUMINFO/"+V_DT+".parquet")


ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
F_NI_AFA_BIS_INFO = sqlContext.read.parquet(hdfs+'/F_NI_AFA_BIS_INFO/*')
F_NI_AFA_BIS_INFO.registerTempTable("F_NI_AFA_BIS_INFO")
ACRM_F_RE_INSUSUMINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_INSUSUMINFO/*')
ACRM_F_RE_INSUSUMINFO.registerTempTable("ACRM_F_RE_INSUSUMINFO")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME_CN          AS CUST_NAME_CN 
       ,A.ACCT_NO               AS ACCT_NO 
       ,A.PRDT_CODE             AS PRDT_CODE 
       ,A.ORG_NO                AS ORG_NO 
       ,A.LAST_AMOUNT           AS LAST_AMOUNT 
       ,CAST(YEAR(V_DT) AS VARCHAR(4))                       AS YEAR 
       ,A.MONTH_BAL_SUM_1       AS MONTH_BAL_SUM_1 
       ,A.MONTH_COUNT_1         AS MONTH_COUNT_1 
       ,A.MONTH_BAL_SUM_2       AS MONTH_BAL_SUM_2 
       ,A.MONTH_COUNT_2         AS MONTH_COUNT_2 
       ,A.MONTH_BAL_SUM_3       AS MONTH_BAL_SUM_3 
       ,A.MONTH_COUNT_3         AS MONTH_COUNT_3 
       ,A.MONTH_BAL_SUM_4       AS MONTH_BAL_SUM_4 
       ,A.MONTH_COUNT_4         AS MONTH_COUNT_4 
       ,A.MONTH_BAL_SUM_5       AS MONTH_BAL_SUM_5 
       ,A.MONTH_COUNT_5         AS MONTH_COUNT_5 
       ,A.MONTH_BAL_SUM_6       AS MONTH_BAL_SUM_6 
       ,A.MONTH_COUNT_6         AS MONTH_COUNT_6 
       ,A.MONTH_BAL_SUM_7       AS MONTH_BAL_SUM_7 
       ,A.MONTH_COUNT_7         AS MONTH_COUNT_7 
       ,A.MONTH_BAL_SUM_8       AS MONTH_BAL_SUM_8 
       ,A.MONTH_COUNT_8         AS MONTH_COUNT_8 
       ,CAST(A.MONTH_BAL_SUM_9 AS decimal(24,6))      AS MONTH_BAL_SUM_9 
       ,A.MONTH_COUNT_9         AS MONTH_COUNT_9 
       ,A.MONTH_BAL_SUM_10      AS MONTH_BAL_SUM_10 
       ,A.MONTH_COUNT_10        AS MONTH_COUNT_10 
       ,A.MONTH_BAL_SUM_11      AS MONTH_BAL_SUM_11 
       ,A.MONTH_COUNT_11        AS MONTH_COUNT_11 
       ,A.MONTH_BAL_SUM_12      AS MONTH_BAL_SUM_12 
       ,A.MONTH_COUNT_12        AS MONTH_COUNT_12 
       ,A.ALL_MONEY             AS ALL_MONEY 
       ,V_DT                    AS ODS_ST_DATE 
       ,A.ALL_COUNT             AS ALL_COUNT 
       ,A.ALL_MONEY             AS OLD_YEAR_BAL_SUM 
       ,CAST(A.ALL_COUNT AS DECIMAL(24,6))            AS OLD_YEAR_BAL 
       ,A.AMOUNT                AS AMOUNT 
       ,A.COUNT                 AS COUNT 
       ,A.LAST_COUNT            AS LAST_COUNT 
       ,B.FR_ID                 AS FR_ID 
   FROM ACRM_F_RE_INSUSUMINFO A                                --保险账户累计表
   LEFT JOIN ADMIN_AUTH_ORG B                                  --机构表
     ON A.ORG_NO                = B.ORG_ID 
  WHERE A.YEAR                  = SUBSTR(V_DT,1,4) - 1 
    AND V_DT                    = CONCAT(SUBSTR(V_DT,1,4),'-01-01') """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_INSUSUMINFO = sqlContext.sql(sql)
ACRM_F_RE_INSUSUMINFO.registerTempTable("ACRM_F_RE_INSUSUMINFO")
dfn="ACRM_F_RE_INSUSUMINFO/"+V_DT+".parquet"
ACRM_F_RE_INSUSUMINFO.cache()
nrows = ACRM_F_RE_INSUSUMINFO.count()
ACRM_F_RE_INSUSUMINFO.write.save(path=hdfs + '/' + dfn, mode='append')
ACRM_F_RE_INSUSUMINFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_INSUSUMINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1

ACRM_F_RE_INSUSUMINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_INSUSUMINFO/*')
ACRM_F_RE_INSUSUMINFO.registerTempTable("ACRM_F_RE_INSUSUMINFO")

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_NAME_CN            AS CUST_NAME_CN 
       ,ACCT_NO                 AS ACCT_NO 
       ,PRDT_CODE               AS PRDT_CODE 
       ,ORG_NO                  AS ORG_NO 
       ,AMOUNT                  AS LAST_AMOUNT 
       ,YEAR                    AS YEAR 
       ,MONTH_BAL_SUM_1         AS MONTH_BAL_SUM_1 
       ,MONTH_COUNT_1           AS MONTH_COUNT_1 
       ,MONTH_BAL_SUM_2         AS MONTH_BAL_SUM_2 
       ,MONTH_COUNT_2           AS MONTH_COUNT_2 
       ,MONTH_BAL_SUM_3         AS MONTH_BAL_SUM_3 
       ,MONTH_COUNT_3           AS MONTH_COUNT_3 
       ,MONTH_BAL_SUM_4         AS MONTH_BAL_SUM_4 
       ,MONTH_COUNT_4           AS MONTH_COUNT_4 
       ,MONTH_BAL_SUM_5         AS MONTH_BAL_SUM_5 
       ,MONTH_COUNT_5           AS MONTH_COUNT_5 
       ,MONTH_BAL_SUM_6         AS MONTH_BAL_SUM_6 
       ,MONTH_COUNT_6           AS MONTH_COUNT_6 
       ,MONTH_BAL_SUM_7         AS MONTH_BAL_SUM_7 
       ,MONTH_COUNT_7           AS MONTH_COUNT_7 
       ,MONTH_BAL_SUM_8         AS MONTH_BAL_SUM_8 
       ,MONTH_COUNT_8           AS MONTH_COUNT_8 
       ,CAST(MONTH_BAL_SUM_9 AS decimal(24,6))          AS MONTH_BAL_SUM_9 
       ,MONTH_COUNT_9           AS MONTH_COUNT_9 
       ,MONTH_BAL_SUM_10        AS MONTH_BAL_SUM_10 
       ,MONTH_COUNT_10          AS MONTH_COUNT_10 
       ,MONTH_BAL_SUM_11        AS MONTH_BAL_SUM_11 
       ,MONTH_COUNT_11          AS MONTH_COUNT_11 
       ,MONTH_BAL_SUM_12        AS MONTH_BAL_SUM_12 
       ,MONTH_COUNT_12          AS MONTH_COUNT_12 
       ,ALL_MONEY               AS ALL_MONEY 
       ,V_DT                    AS ODS_ST_DATE 
       ,ALL_COUNT               AS ALL_COUNT 
       ,OLD_YEAR_BAL_SUM        AS OLD_YEAR_BAL_SUM 
       ,OLD_YEAR_BAL            AS OLD_YEAR_BAL 
       ,AMOUNT                  AS AMOUNT 
       ,COUNT                   AS COUNT 
       ,COUNT                   AS LAST_COUNT 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_INSUSUMINFO A                             --保险账户累计表
  WHERE YEAR                    = YEAR(V_DT) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_INSUSUMINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_INSUSUMINFO_INNTMP1.registerTempTable("ACRM_F_RE_INSUSUMINFO_INNTMP1")


sql = """
 SELECT DST.CUST_ID                                             --客户编号:src.CUST_ID
       ,DST.CUST_NAME_CN                                       --客户名称:src.CUST_NAME_CN
       ,DST.ACCT_NO                                            --账户:src.ACCT_NO
       ,DST.PRDT_CODE                                          --产品编号:src.PRDT_CODE
       ,DST.ORG_NO                                             --机构代码:src.ORG_NO
       ,DST.LAST_AMOUNT                                        --去年累计金额:src.LAST_AMOUNT
       ,DST.YEAR                                               --积数年份:src.YEAR
       ,DST.MONTH_BAL_SUM_1                                    --1月购买金额:src.MONTH_BAL_SUM_1
       ,DST.MONTH_COUNT_1                                      --1月购买笔数:src.MONTH_COUNT_1
       ,DST.MONTH_BAL_SUM_2                                    --2月购买金额:src.MONTH_BAL_SUM_2
       ,DST.MONTH_COUNT_2                                      --2月购买笔数:src.MONTH_COUNT_2
       ,DST.MONTH_BAL_SUM_3                                    --3月购买金额:src.MONTH_BAL_SUM_3
       ,DST.MONTH_COUNT_3                                      --3月购买笔数:src.MONTH_COUNT_3
       ,DST.MONTH_BAL_SUM_4                                    --4月购买金额:src.MONTH_BAL_SUM_4
       ,DST.MONTH_COUNT_4                                      --4月购买笔数:src.MONTH_COUNT_4
       ,DST.MONTH_BAL_SUM_5                                    --5月购买金额:src.MONTH_BAL_SUM_5
       ,DST.MONTH_COUNT_5                                      --5月购买笔数:src.MONTH_COUNT_5
       ,DST.MONTH_BAL_SUM_6                                    --6月购买金额:src.MONTH_BAL_SUM_6
       ,DST.MONTH_COUNT_6                                      --6月购买笔数:src.MONTH_COUNT_6
       ,DST.MONTH_BAL_SUM_7                                    --7月购买金额:src.MONTH_BAL_SUM_7
       ,DST.MONTH_COUNT_7                                      --7月购买笔数:src.MONTH_COUNT_7
       ,DST.MONTH_BAL_SUM_8                                    --8月购买金额:src.MONTH_BAL_SUM_8
       ,DST.MONTH_COUNT_8                                      --8月购买笔数:src.MONTH_COUNT_8
       ,DST.MONTH_BAL_SUM_9                                    --9月购买金额:src.MONTH_BAL_SUM_9
       ,DST.MONTH_COUNT_9                                      --9月购买笔数:src.MONTH_COUNT_9
       ,DST.MONTH_BAL_SUM_10                                   --10月购买金额:src.MONTH_BAL_SUM_10
       ,DST.MONTH_COUNT_10                                     --10月购买笔数:src.MONTH_COUNT_10
       ,DST.MONTH_BAL_SUM_11                                   --11月购买金额:src.MONTH_BAL_SUM_11
       ,DST.MONTH_COUNT_11                                     --11月购买笔数:src.MONTH_COUNT_11
       ,DST.MONTH_BAL_SUM_12                                   --12月购买金额:src.MONTH_BAL_SUM_12
       ,DST.MONTH_COUNT_12                                     --12月购买笔数:src.MONTH_COUNT_12
       ,DST.ALL_MONEY                                          --保费累计:src.ALL_MONEY
       ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
       ,DST.ALL_COUNT                                          --累计购买笔数:src.ALL_COUNT
       ,DST.OLD_YEAR_BAL_SUM                                   --年初累计购买金额:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --年初累计购买笔数:src.OLD_YEAR_BAL
       ,DST.AMOUNT                                             --余额:src.AMOUNT
       ,DST.COUNT                                              --数量:src.COUNT
       ,DST.LAST_COUNT                                         --昨日数量:src.LAST_COUNT
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_F_RE_INSUSUMINFO DST 
   LEFT JOIN ACRM_F_RE_INSUSUMINFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.ORG_NO              = DST.ORG_NO 
    AND SRC.YEAR                = DST.YEAR 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_INSUSUMINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_INSUSUMINFO/"+V_DT+".parquet"
ACRM_F_RE_INSUSUMINFO_INNTMP2=ACRM_F_RE_INSUSUMINFO_INNTMP2.unionAll(ACRM_F_RE_INSUSUMINFO_INNTMP1)
ACRM_F_RE_INSUSUMINFO_INNTMP1.cache()
ACRM_F_RE_INSUSUMINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_INSUSUMINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_INSUSUMINFO_INNTMP2.count()
ACRM_F_RE_INSUSUMINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_INSUSUMINFO_INNTMP1.unpersist()
ACRM_F_RE_INSUSUMINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_INSUSUMINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_INSUSUMINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_INSUSUMINFO_BK/")

#任务[12] 001-03::
V_STEP = V_STEP + 1

ACRM_F_RE_INSUSUMINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_INSUSUMINFO/*')
ACRM_F_RE_INSUSUMINFO.registerTempTable("ACRM_F_RE_INSUSUMINFO")

sql = """
 SELECT A.HOLDERCUSTNO          AS CUST_ID 
       ,B.CUST_NAME_CN          AS CUST_NAME_CN 
       ,B.ACCT_NO               AS ACCT_NO 
       ,B.PRDT_CODE             AS PRDT_CODE 
       ,A.BRNO                  AS ORG_NO 
       ,B.LAST_AMOUNT           AS LAST_AMOUNT 
       ,CAST(YEAR(V_DT) AS VARCHAR(4))                       AS YEAR 
       ,B.MONTH_BAL_SUM_1       AS MONTH_BAL_SUM_1 
       ,B.MONTH_COUNT_1         AS MONTH_COUNT_1 
       ,B.MONTH_BAL_SUM_2       AS MONTH_BAL_SUM_2 
       ,B.MONTH_COUNT_2         AS MONTH_COUNT_2 
       ,B.MONTH_BAL_SUM_3       AS MONTH_BAL_SUM_3 
       ,B.MONTH_COUNT_3         AS MONTH_COUNT_3 
       ,B.MONTH_BAL_SUM_4       AS MONTH_BAL_SUM_4 
       ,B.MONTH_COUNT_4         AS MONTH_COUNT_4 
       ,B.MONTH_BAL_SUM_5       AS MONTH_BAL_SUM_5 
       ,B.MONTH_COUNT_5         AS MONTH_COUNT_5 
       ,B.MONTH_BAL_SUM_6       AS MONTH_BAL_SUM_6 
       ,B.MONTH_COUNT_6         AS MONTH_COUNT_6 
       ,B.MONTH_BAL_SUM_7       AS MONTH_BAL_SUM_7 
       ,B.MONTH_COUNT_7         AS MONTH_COUNT_7 
       ,B.MONTH_BAL_SUM_8       AS MONTH_BAL_SUM_8 
       ,B.MONTH_COUNT_8         AS MONTH_COUNT_8 
       ,CAST(b.MONTH_BAL_SUM_9 AS decimal(24,6))      AS MONTH_BAL_SUM_9 
       ,B.MONTH_COUNT_9         AS MONTH_COUNT_9 
       ,B.MONTH_BAL_SUM_10      AS MONTH_BAL_SUM_10 
       ,B.MONTH_COUNT_10        AS MONTH_COUNT_10 
       ,B.MONTH_BAL_SUM_11      AS MONTH_BAL_SUM_11 
       ,B.MONTH_COUNT_11        AS MONTH_COUNT_11 
       ,B.MONTH_BAL_SUM_12      AS MONTH_BAL_SUM_12 
       ,B.MONTH_COUNT_12        AS MONTH_COUNT_12 
       ,B.ALL_MONEY             AS ALL_MONEY 
       ,V_DT                    AS ODS_ST_DATE 
       ,B.ALL_COUNT             AS ALL_COUNT 
       ,B.OLD_YEAR_BAL_SUM      AS OLD_YEAR_BAL_SUM 
       ,B.OLD_YEAR_BAL          AS OLD_YEAR_BAL 
       ,CAST(A.AMT AS decimal(24,6))                  AS AMOUNT 
       ,CAST(A.NUM AS INTEGER)                  AS COUNT 
       ,CAST(B.LAST_COUNT AS INTEGER)            AS LAST_COUNT 
       ,A.FR_ID                 AS FR_ID 
   FROM (SELECT HOLDERCUSTNO,BRNO,FR_ID,COUNT(1) AS NUM,SUM(SURALLAMOUNT) AS AMT 
           FROM F_NI_AFA_BIS_INFO A 
          WHERE NOTES4 = '1' AND ODS_ST_DATE = V_DT
          GROUP BY HOLDERCUSTNO,BRNO,FR_ID) A                                                   --缴款主信息登记薄
   LEFT JOIN ACRM_F_RE_INSUSUMINFO B                           --保险账户累计表
     ON B.CUST_ID               = A.HOLDERCUSTNO 
    AND B.ORG_NO                = A.BRNO 
    AND B.ODS_ST_DATE           = V_DT 
    AND B.YEAR                  = YEAR(V_DT) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_INSUSUMINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_INSUSUMINFO_INNTMP1.registerTempTable("ACRM_F_RE_INSUSUMINFO_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户编号:src.CUST_ID
       ,DST.CUST_NAME_CN                                       --客户名称:src.CUST_NAME_CN
       ,DST.ACCT_NO                                            --账户:src.ACCT_NO
       ,DST.PRDT_CODE                                          --产品编号:src.PRDT_CODE
       ,DST.ORG_NO                                             --机构代码:src.ORG_NO
       ,DST.LAST_AMOUNT                                        --去年累计金额:src.LAST_AMOUNT
       ,DST.YEAR                                               --积数年份:src.YEAR
       ,DST.MONTH_BAL_SUM_1                                    --1月购买金额:src.MONTH_BAL_SUM_1
       ,DST.MONTH_COUNT_1                                      --1月购买笔数:src.MONTH_COUNT_1
       ,DST.MONTH_BAL_SUM_2                                    --2月购买金额:src.MONTH_BAL_SUM_2
       ,DST.MONTH_COUNT_2                                      --2月购买笔数:src.MONTH_COUNT_2
       ,DST.MONTH_BAL_SUM_3                                    --3月购买金额:src.MONTH_BAL_SUM_3
       ,DST.MONTH_COUNT_3                                      --3月购买笔数:src.MONTH_COUNT_3
       ,DST.MONTH_BAL_SUM_4                                    --4月购买金额:src.MONTH_BAL_SUM_4
       ,DST.MONTH_COUNT_4                                      --4月购买笔数:src.MONTH_COUNT_4
       ,DST.MONTH_BAL_SUM_5                                    --5月购买金额:src.MONTH_BAL_SUM_5
       ,DST.MONTH_COUNT_5                                      --5月购买笔数:src.MONTH_COUNT_5
       ,DST.MONTH_BAL_SUM_6                                    --6月购买金额:src.MONTH_BAL_SUM_6
       ,DST.MONTH_COUNT_6                                      --6月购买笔数:src.MONTH_COUNT_6
       ,DST.MONTH_BAL_SUM_7                                    --7月购买金额:src.MONTH_BAL_SUM_7
       ,DST.MONTH_COUNT_7                                      --7月购买笔数:src.MONTH_COUNT_7
       ,DST.MONTH_BAL_SUM_8                                    --8月购买金额:src.MONTH_BAL_SUM_8
       ,DST.MONTH_COUNT_8                                      --8月购买笔数:src.MONTH_COUNT_8
       ,DST.MONTH_BAL_SUM_9                                    --9月购买金额:src.MONTH_BAL_SUM_9
       ,DST.MONTH_COUNT_9                                      --9月购买笔数:src.MONTH_COUNT_9
       ,DST.MONTH_BAL_SUM_10                                   --10月购买金额:src.MONTH_BAL_SUM_10
       ,DST.MONTH_COUNT_10                                     --10月购买笔数:src.MONTH_COUNT_10
       ,DST.MONTH_BAL_SUM_11                                   --11月购买金额:src.MONTH_BAL_SUM_11
       ,DST.MONTH_COUNT_11                                     --11月购买笔数:src.MONTH_COUNT_11
       ,DST.MONTH_BAL_SUM_12                                   --12月购买金额:src.MONTH_BAL_SUM_12
       ,DST.MONTH_COUNT_12                                     --12月购买笔数:src.MONTH_COUNT_12
       ,DST.ALL_MONEY                                          --保费累计:src.ALL_MONEY
       ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
       ,DST.ALL_COUNT                                          --累计购买笔数:src.ALL_COUNT
       ,DST.OLD_YEAR_BAL_SUM                                   --年初累计购买金额:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --年初累计购买笔数:src.OLD_YEAR_BAL
       ,DST.AMOUNT                                             --余额:src.AMOUNT
       ,DST.COUNT                                              --数量:src.COUNT
       ,DST.LAST_COUNT                                         --昨日数量:src.LAST_COUNT
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_F_RE_INSUSUMINFO DST 
   LEFT JOIN ACRM_F_RE_INSUSUMINFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.ORG_NO              = DST.ORG_NO 
    AND SRC.YEAR                = DST.YEAR 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_INSUSUMINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_INSUSUMINFO/"+V_DT+".parquet"
ACRM_F_RE_INSUSUMINFO_INNTMP2=ACRM_F_RE_INSUSUMINFO_INNTMP2.unionAll(ACRM_F_RE_INSUSUMINFO_INNTMP1)
ACRM_F_RE_INSUSUMINFO_INNTMP1.cache()
ACRM_F_RE_INSUSUMINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_INSUSUMINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_INSUSUMINFO_INNTMP2.count()
ACRM_F_RE_INSUSUMINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_INSUSUMINFO_INNTMP1.unpersist()
ACRM_F_RE_INSUSUMINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_INSUSUMINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_INSUSUMINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_INSUSUMINFO_BK/")

#任务[12] 001-04::
V_STEP = V_STEP + 1

ACRM_F_RE_INSUSUMINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_INSUSUMINFO/*')
ACRM_F_RE_INSUSUMINFO.registerTempTable("ACRM_F_RE_INSUSUMINFO")

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,CUST_NAME_CN            AS CUST_NAME_CN 
       ,ACCT_NO                 AS ACCT_NO 
       ,PRDT_CODE               AS PRDT_CODE 
       ,ORG_NO                  AS ORG_NO 
       ,LAST_AMOUNT             AS LAST_AMOUNT 
       ,YEAR                    AS YEAR 
       ,CASE WHEN MONTH(V_DT)                       = 1 THEN AMOUNT ELSE MONTH_BAL_SUM_1 END                     AS MONTH_BAL_SUM_1 
       ,CASE WHEN MONTH(V_DT)                       = 1 THEN COUNT ELSE MONTH_COUNT_1 END                     AS MONTH_COUNT_1 
       ,CASE WHEN MONTH(V_DT)                       = 2 THEN AMOUNT ELSE MONTH_BAL_SUM_2 END                     AS MONTH_BAL_SUM_2 
       ,CASE WHEN MONTH(V_DT)                       = 2 THEN COUNT ELSE MONTH_COUNT_2 END                     AS MONTH_COUNT_2 
       ,CASE WHEN MONTH(V_DT)                       = 3 THEN AMOUNT ELSE MONTH_BAL_SUM_3 END                     AS MONTH_BAL_SUM_3 
       ,CASE WHEN MONTH(V_DT)                       = 3 THEN COUNT ELSE MONTH_COUNT_3 END                     AS MONTH_COUNT_3 
       ,CASE WHEN MONTH(V_DT)                       = 4 THEN AMOUNT ELSE MONTH_BAL_SUM_4 END                     AS MONTH_BAL_SUM_4 
       ,CASE WHEN MONTH(V_DT)                       = 4 THEN COUNT ELSE MONTH_COUNT_4 END                     AS MONTH_COUNT_4 
       ,CASE WHEN MONTH(V_DT)                       = 5 THEN AMOUNT ELSE MONTH_BAL_SUM_5 END                     AS MONTH_BAL_SUM_5 
       ,CASE WHEN MONTH(V_DT)                       = 5 THEN COUNT ELSE MONTH_COUNT_5 END                     AS MONTH_COUNT_5 
       ,CASE WHEN MONTH(V_DT)                       = 6 THEN AMOUNT ELSE MONTH_BAL_SUM_6 END                     AS MONTH_BAL_SUM_6 
       ,CASE WHEN MONTH(V_DT)                       = 6 THEN COUNT ELSE MONTH_COUNT_6 END                     AS MONTH_COUNT_6 
       ,CASE WHEN MONTH(V_DT)                       = 7 THEN AMOUNT ELSE MONTH_BAL_SUM_7 END                     AS MONTH_BAL_SUM_7 
       ,CASE WHEN MONTH(V_DT)                       = 7 THEN COUNT ELSE MONTH_COUNT_7 END                     AS MONTH_COUNT_7 
       ,CASE WHEN MONTH(V_DT)                       = 8 THEN AMOUNT ELSE MONTH_BAL_SUM_8 END                     AS MONTH_BAL_SUM_8 
       ,CASE WHEN MONTH(V_DT)                       = 8 THEN COUNT ELSE MONTH_COUNT_8 END                     AS MONTH_COUNT_8 
       ,CAST(CASE WHEN MONTH(V_DT)                       = 9 THEN AMOUNT ELSE MONTH_BAL_SUM_9 END  AS DECIMAL(24,6))                   AS MONTH_BAL_SUM_9 
       ,CASE WHEN MONTH(V_DT)                       = 9 THEN COUNT ELSE MONTH_COUNT_9 END                     AS MONTH_COUNT_9 
       ,CASE WHEN MONTH(V_DT)                       = 10 THEN AMOUNT ELSE MONTH_BAL_SUM_10 END                     AS MONTH_BAL_SUM_10 
       ,CASE WHEN MONTH(V_DT)                       = 10 THEN COUNT ELSE MONTH_COUNT_10 END                     AS MONTH_COUNT_10 
       ,CASE WHEN MONTH(V_DT)                       = 11 THEN AMOUNT ELSE MONTH_BAL_SUM_11 END                     AS MONTH_BAL_SUM_11 
       ,CASE WHEN MONTH(V_DT)                       = 11 THEN COUNT ELSE MONTH_COUNT_11 END                     AS MONTH_COUNT_11 
       ,CASE WHEN MONTH(V_DT)                       = 12 THEN AMOUNT ELSE MONTH_BAL_SUM_12 END                     AS MONTH_BAL_SUM_12 
       ,CASE WHEN MONTH(V_DT)                       = 12 THEN COUNT ELSE MONTH_COUNT_12 END                     AS MONTH_COUNT_12 
       ,ALL_MONEY               AS ALL_MONEY 
       ,ODS_ST_DATE             AS ODS_ST_DATE 
       ,ALL_COUNT               AS ALL_COUNT 
       ,OLD_YEAR_BAL_SUM        AS OLD_YEAR_BAL_SUM 
       ,OLD_YEAR_BAL            AS OLD_YEAR_BAL 
       ,AMOUNT                  AS AMOUNT 
       ,COUNT                   AS COUNT 
       ,LAST_COUNT              AS LAST_COUNT 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_INSUSUMINFO A                                --保险账户累计表
  WHERE YEAR                    = YEAR(V_DT) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_INSUSUMINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_INSUSUMINFO_INNTMP1.registerTempTable("ACRM_F_RE_INSUSUMINFO_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户编号:src.CUST_ID
       ,DST.CUST_NAME_CN                                       --客户名称:src.CUST_NAME_CN
       ,DST.ACCT_NO                                            --账户:src.ACCT_NO
       ,DST.PRDT_CODE                                          --产品编号:src.PRDT_CODE
       ,DST.ORG_NO                                             --机构代码:src.ORG_NO
       ,DST.LAST_AMOUNT                                        --去年累计金额:src.LAST_AMOUNT
       ,DST.YEAR                                               --积数年份:src.YEAR
       ,DST.MONTH_BAL_SUM_1                                    --1月购买金额:src.MONTH_BAL_SUM_1
       ,DST.MONTH_COUNT_1                                      --1月购买笔数:src.MONTH_COUNT_1
       ,DST.MONTH_BAL_SUM_2                                    --2月购买金额:src.MONTH_BAL_SUM_2
       ,DST.MONTH_COUNT_2                                      --2月购买笔数:src.MONTH_COUNT_2
       ,DST.MONTH_BAL_SUM_3                                    --3月购买金额:src.MONTH_BAL_SUM_3
       ,DST.MONTH_COUNT_3                                      --3月购买笔数:src.MONTH_COUNT_3
       ,DST.MONTH_BAL_SUM_4                                    --4月购买金额:src.MONTH_BAL_SUM_4
       ,DST.MONTH_COUNT_4                                      --4月购买笔数:src.MONTH_COUNT_4
       ,DST.MONTH_BAL_SUM_5                                    --5月购买金额:src.MONTH_BAL_SUM_5
       ,DST.MONTH_COUNT_5                                      --5月购买笔数:src.MONTH_COUNT_5
       ,DST.MONTH_BAL_SUM_6                                    --6月购买金额:src.MONTH_BAL_SUM_6
       ,DST.MONTH_COUNT_6                                      --6月购买笔数:src.MONTH_COUNT_6
       ,DST.MONTH_BAL_SUM_7                                    --7月购买金额:src.MONTH_BAL_SUM_7
       ,DST.MONTH_COUNT_7                                      --7月购买笔数:src.MONTH_COUNT_7
       ,DST.MONTH_BAL_SUM_8                                    --8月购买金额:src.MONTH_BAL_SUM_8
       ,DST.MONTH_COUNT_8                                      --8月购买笔数:src.MONTH_COUNT_8
       ,DST.MONTH_BAL_SUM_9                                    --9月购买金额:src.MONTH_BAL_SUM_9
       ,DST.MONTH_COUNT_9                                      --9月购买笔数:src.MONTH_COUNT_9
       ,DST.MONTH_BAL_SUM_10                                   --10月购买金额:src.MONTH_BAL_SUM_10
       ,DST.MONTH_COUNT_10                                     --10月购买笔数:src.MONTH_COUNT_10
       ,DST.MONTH_BAL_SUM_11                                   --11月购买金额:src.MONTH_BAL_SUM_11
       ,DST.MONTH_COUNT_11                                     --11月购买笔数:src.MONTH_COUNT_11
       ,DST.MONTH_BAL_SUM_12                                   --12月购买金额:src.MONTH_BAL_SUM_12
       ,DST.MONTH_COUNT_12                                     --12月购买笔数:src.MONTH_COUNT_12
       ,DST.ALL_MONEY                                          --保费累计:src.ALL_MONEY
       ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
       ,DST.ALL_COUNT                                          --累计购买笔数:src.ALL_COUNT
       ,DST.OLD_YEAR_BAL_SUM                                   --年初累计购买金额:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --年初累计购买笔数:src.OLD_YEAR_BAL
       ,DST.AMOUNT                                             --余额:src.AMOUNT
       ,DST.COUNT                                              --数量:src.COUNT
       ,DST.LAST_COUNT                                         --昨日数量:src.LAST_COUNT
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_F_RE_INSUSUMINFO DST 
   LEFT JOIN ACRM_F_RE_INSUSUMINFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.ORG_NO              = DST.ORG_NO 
    AND SRC.YEAR                = DST.YEAR 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_INSUSUMINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_INSUSUMINFO/"+V_DT+".parquet"
ACRM_F_RE_INSUSUMINFO_INNTMP2=ACRM_F_RE_INSUSUMINFO_INNTMP2.unionAll(ACRM_F_RE_INSUSUMINFO_INNTMP1)
ACRM_F_RE_INSUSUMINFO_INNTMP1.cache()
ACRM_F_RE_INSUSUMINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_INSUSUMINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_INSUSUMINFO_INNTMP2.count()
ACRM_F_RE_INSUSUMINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_RE_INSUSUMINFO_INNTMP1.unpersist()
ACRM_F_RE_INSUSUMINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_INSUSUMINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_INSUSUMINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_INSUSUMINFO_BK/")

#任务[12] 001-05::
V_STEP = V_STEP + 1

ACRM_F_RE_INSUSUMINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_INSUSUMINFO/*')
ACRM_F_RE_INSUSUMINFO.registerTempTable("ACRM_F_RE_INSUSUMINFO")

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,A.CUST_NAME_CN          AS CUST_NAME_CN 
       ,A.ACCT_NO               AS ACCT_NO 
       ,A.PRDT_CODE             AS PRDT_CODE 
       ,ORG_NO                  AS ORG_NO 
       ,A.LAST_AMOUNT           AS LAST_AMOUNT 
       ,YEAR                    AS YEAR 
       ,A.MONTH_BAL_SUM_1       AS MONTH_BAL_SUM_1 
       ,A.MONTH_COUNT_1         AS MONTH_COUNT_1 
       ,A.MONTH_BAL_SUM_2       AS MONTH_BAL_SUM_2 
       ,A.MONTH_COUNT_2         AS MONTH_COUNT_2 
       ,A.MONTH_BAL_SUM_3       AS MONTH_BAL_SUM_3 
       ,A.MONTH_COUNT_3         AS MONTH_COUNT_3 
       ,A.MONTH_BAL_SUM_4       AS MONTH_BAL_SUM_4 
       ,A.MONTH_COUNT_4         AS MONTH_COUNT_4 
       ,A.MONTH_BAL_SUM_5       AS MONTH_BAL_SUM_5 
       ,A.MONTH_COUNT_5         AS MONTH_COUNT_5 
       ,A.MONTH_BAL_SUM_6       AS MONTH_BAL_SUM_6 
       ,A.MONTH_COUNT_6         AS MONTH_COUNT_6 
       ,A.MONTH_BAL_SUM_7       AS MONTH_BAL_SUM_7 
       ,A.MONTH_COUNT_7         AS MONTH_COUNT_7 
       ,A.MONTH_BAL_SUM_8       AS MONTH_BAL_SUM_8 
       ,A.MONTH_COUNT_8         AS MONTH_COUNT_8 
       ,CAST(A.MONTH_BAL_SUM_9 AS decimal(24,6))        AS MONTH_BAL_SUM_9 
       ,A.MONTH_COUNT_9         AS MONTH_COUNT_9 
       ,A.MONTH_BAL_SUM_10      AS MONTH_BAL_SUM_10 
       ,A.MONTH_COUNT_10        AS MONTH_COUNT_10 
       ,A.MONTH_BAL_SUM_11      AS MONTH_BAL_SUM_11 
       ,A.MONTH_COUNT_11        AS MONTH_COUNT_11 
       ,A.MONTH_BAL_SUM_12      AS MONTH_BAL_SUM_12 
       ,A.MONTH_COUNT_12        AS MONTH_COUNT_12 
       ,CAST(COALESCE(OLD_YEAR_BAL_SUM, 0) + COALESCE(MONTH_BAL_SUM_1, 0) + COALESCE(MONTH_BAL_SUM_2, 0) + COALESCE(MONTH_BAL_SUM_3, 0) + COALESCE(MONTH_BAL_SUM_4, 0) + COALESCE(MONTH_BAL_SUM_5, 0) + COALESCE(MONTH_BAL_SUM_6, 0) + COALESCE(MONTH_BAL_SUM_7, 0) + COALESCE(MONTH_BAL_SUM_8, 0) + COALESCE(MONTH_BAL_SUM_9, 0) + COALESCE(MONTH_BAL_SUM_10, 0) + COALESCE(MONTH_BAL_SUM_11, 0) + COALESCE(MONTH_BAL_SUM_12, 0) AS DECIMAL(24,6)) AS ALL_MONEY 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,CAST(COALESCE(CAST(OLD_YEAR_BAL AS INTEGER), 0) +(COALESCE(MONTH_COUNT_1, 0) + COALESCE(MONTH_COUNT_2, 0) + COALESCE(MONTH_COUNT_3, 0) + COALESCE(MONTH_COUNT_4, 0) + COALESCE(MONTH_COUNT_5, 0) + COALESCE(MONTH_COUNT_6, 0) + COALESCE(MONTH_COUNT_7, 0) + COALESCE(MONTH_COUNT_8, 0) + COALESCE(MONTH_COUNT_9, 0) + COALESCE(MONTH_COUNT_10, 0) + COALESCE(MONTH_COUNT_11, 0) + COALESCE(MONTH_COUNT_12, 0)) AS INTEGER)                       AS ALL_COUNT 
       ,A.OLD_YEAR_BAL_SUM      AS OLD_YEAR_BAL_SUM 
       ,A.OLD_YEAR_BAL          AS OLD_YEAR_BAL 
       ,A.AMOUNT                AS AMOUNT 
       ,A.COUNT                 AS COUNT 
       ,A.LAST_COUNT            AS LAST_COUNT 
       ,FR_ID                   AS FR_ID 
   FROM ACRM_F_RE_INSUSUMINFO A                                --保险账户累计表
  WHERE YEAR                    = YEAR(V_DT) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_INSUSUMINFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_RE_INSUSUMINFO_INNTMP1.registerTempTable("ACRM_F_RE_INSUSUMINFO_INNTMP1")


sql = """
 SELECT DST.CUST_ID                                             --客户编号:src.CUST_ID
       ,DST.CUST_NAME_CN                                       --客户名称:src.CUST_NAME_CN
       ,DST.ACCT_NO                                            --账户:src.ACCT_NO
       ,DST.PRDT_CODE                                          --产品编号:src.PRDT_CODE
       ,DST.ORG_NO                                             --机构代码:src.ORG_NO
       ,DST.LAST_AMOUNT                                        --去年累计金额:src.LAST_AMOUNT
       ,DST.YEAR                                               --积数年份:src.YEAR
       ,DST.MONTH_BAL_SUM_1                                    --1月购买金额:src.MONTH_BAL_SUM_1
       ,DST.MONTH_COUNT_1                                      --1月购买笔数:src.MONTH_COUNT_1
       ,DST.MONTH_BAL_SUM_2                                    --2月购买金额:src.MONTH_BAL_SUM_2
       ,DST.MONTH_COUNT_2                                      --2月购买笔数:src.MONTH_COUNT_2
       ,DST.MONTH_BAL_SUM_3                                    --3月购买金额:src.MONTH_BAL_SUM_3
       ,DST.MONTH_COUNT_3                                      --3月购买笔数:src.MONTH_COUNT_3
       ,DST.MONTH_BAL_SUM_4                                    --4月购买金额:src.MONTH_BAL_SUM_4
       ,DST.MONTH_COUNT_4                                      --4月购买笔数:src.MONTH_COUNT_4
       ,DST.MONTH_BAL_SUM_5                                    --5月购买金额:src.MONTH_BAL_SUM_5
       ,DST.MONTH_COUNT_5                                      --5月购买笔数:src.MONTH_COUNT_5
       ,DST.MONTH_BAL_SUM_6                                    --6月购买金额:src.MONTH_BAL_SUM_6
       ,DST.MONTH_COUNT_6                                      --6月购买笔数:src.MONTH_COUNT_6
       ,DST.MONTH_BAL_SUM_7                                    --7月购买金额:src.MONTH_BAL_SUM_7
       ,DST.MONTH_COUNT_7                                      --7月购买笔数:src.MONTH_COUNT_7
       ,DST.MONTH_BAL_SUM_8                                    --8月购买金额:src.MONTH_BAL_SUM_8
       ,DST.MONTH_COUNT_8                                      --8月购买笔数:src.MONTH_COUNT_8
       ,DST.MONTH_BAL_SUM_9                                    --9月购买金额:src.MONTH_BAL_SUM_9
       ,DST.MONTH_COUNT_9                                      --9月购买笔数:src.MONTH_COUNT_9
       ,DST.MONTH_BAL_SUM_10                                   --10月购买金额:src.MONTH_BAL_SUM_10
       ,DST.MONTH_COUNT_10                                     --10月购买笔数:src.MONTH_COUNT_10
       ,DST.MONTH_BAL_SUM_11                                   --11月购买金额:src.MONTH_BAL_SUM_11
       ,DST.MONTH_COUNT_11                                     --11月购买笔数:src.MONTH_COUNT_11
       ,DST.MONTH_BAL_SUM_12                                   --12月购买金额:src.MONTH_BAL_SUM_12
       ,DST.MONTH_COUNT_12                                     --12月购买笔数:src.MONTH_COUNT_12
       ,DST.ALL_MONEY                                          --保费累计:src.ALL_MONEY
       ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
       ,DST.ALL_COUNT                                          --累计购买笔数:src.ALL_COUNT
       ,DST.OLD_YEAR_BAL_SUM                                   --年初累计购买金额:src.OLD_YEAR_BAL_SUM
       ,DST.OLD_YEAR_BAL                                       --年初累计购买笔数:src.OLD_YEAR_BAL
       ,DST.AMOUNT                                             --余额:src.AMOUNT
       ,DST.COUNT                                              --数量:src.COUNT
       ,DST.LAST_COUNT                                         --昨日数量:src.LAST_COUNT
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM ACRM_F_RE_INSUSUMINFO DST 
   LEFT JOIN ACRM_F_RE_INSUSUMINFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.ORG_NO              = DST.ORG_NO 
    AND SRC.YEAR                = DST.YEAR 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_RE_INSUSUMINFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_RE_INSUSUMINFO/"+V_DT+".parquet"
ACRM_F_RE_INSUSUMINFO_INNTMP2=ACRM_F_RE_INSUSUMINFO_INNTMP2.unionAll(ACRM_F_RE_INSUSUMINFO_INNTMP1)
ACRM_F_RE_INSUSUMINFO_INNTMP1.cache()
ACRM_F_RE_INSUSUMINFO_INNTMP2.cache()
nrowsi = ACRM_F_RE_INSUSUMINFO_INNTMP1.count()
nrowsa = ACRM_F_RE_INSUSUMINFO_INNTMP2.count()

#装载数据
ACRM_F_RE_INSUSUMINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_RE_INSUSUMINFO_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_RE_INSUSUMINFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_RE_INSUSUMINFO_BK/"+V_DT+".parquet")


ACRM_F_RE_INSUSUMINFO_INNTMP1.unpersist()
ACRM_F_RE_INSUSUMINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_RE_INSUSUMINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_RE_INSUSUMINFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_RE_INSUSUMINFO_BK/")
