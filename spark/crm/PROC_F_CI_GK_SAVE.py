#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_GK_SAVE').setMaster(sys.argv[2])
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
V_YEAR_ST=(date(int(etl_date[0:4]),1,1)).strftime("%Y-%m-%d")
V_DT_DAYS = etl_date[6:8]
V_LT_YEAR_ST=(date(int(etl_date[0:4])-1,1,1)).strftime("%Y-%m-%d")
V_LT_YEAR_ET=(date(int(etl_date[0:4])-1,12,31)).strftime("%Y-%m-%d")
V_STEP = 0

ACRM_F_RE_SAVESUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_SAVESUMAVGINFO/*')
ACRM_F_RE_SAVESUMAVGINFO.registerTempTable("ACRM_F_RE_SAVESUMAVGINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT FR_ID as FR_ID
       ,CUST_ID as CUST_ID 
       ,TYPE as TYPE 
       ,cast(SUM(AMOUNT) as decimal(18,2)) as AMOUNT 
       ,cast(SUM(COALESCE(YEAR_BAL_SUM,0))/datediff(V_DT,V_YEAR_ST) as decimal(18,2)) as CUR_YEAR_AVG
       ,cast(SUM(CASE MONTH(V_DT) WHEN 1 THEN A.MONTH_BAL_SUM_1 / A.MONTH_DAYS_1 WHEN 2 THEN A.MONTH_BAL_SUM_2 / A.MONTH_DAYS_2 WHEN 3 THEN A.MONTH_BAL_SUM_3 / A.MONTH_DAYS_3 WHEN 4 THEN A.MONTH_BAL_SUM_4 / A.MONTH_DAYS_4 WHEN 5 THEN A.MONTH_BAL_SUM_5 / A.MONTH_DAYS_5 WHEN 6 THEN A.MONTH_BAL_SUM_6 / A.MONTH_DAYS_6 WHEN 7 THEN A.MONTH_BAL_SUM_7 / A.MONTH_DAYS_7 WHEN 8 THEN A.MONTH_BAL_SUM_8 / A.MONTH_DAYS_8 WHEN 9 THEN A.MONTH_BAL_SUM_9 / A.MONTH_DAYS_9 WHEN 10 THEN A.MONTH_BAL_SUM_10 / A.MONTH_DAYS_10 WHEN 11 THEN A.MONTH_BAL_SUM_11 / A.MONTH_DAYS_11 WHEN 12 THEN A.MONTH_BAL_SUM_12 / A.MONTH_DAYS_12 END)as decimal(18,2)) as CUR_MONTH_AVG 
       ,cast(CASE QUARTER(V_DT)   WHEN 1 THEN  SUM((COALESCE(MONTH_BAL_SUM_1,0)+COALESCE(MONTH_BAL_SUM_2,0)+COALESCE(MONTH_BAL_SUM_3,0))/
                             (CASE  COALESCE(MONTH_DAYS_1,0) + COALESCE(MONTH_DAYS_2,0) + COALESCE(MONTH_DAYS_3,0) WHEN 0 THEN 1
		                                                    ELSE COALESCE(MONTH_DAYS_1,0) + COALESCE(MONTH_DAYS_2,0) + COALESCE(MONTH_DAYS_3,0)
		                                                     END )) 
          WHEN 2 THEN  SUM((COALESCE(MONTH_BAL_SUM_4,0)+COALESCE(MONTH_BAL_SUM_5,0)+COALESCE(MONTH_BAL_SUM_6,0))/
                             (CASE  COALESCE(MONTH_DAYS_4,0) + COALESCE(MONTH_DAYS_5,0) + COALESCE(MONTH_DAYS_6,0) WHEN 0 THEN 1
		                                                    ELSE COALESCE(MONTH_DAYS_4,0) + COALESCE(MONTH_DAYS_5,0) + COALESCE(MONTH_DAYS_6,0)
		                                                     END  ))      
          WHEN 3 THEN   SUM((COALESCE(MONTH_BAL_SUM_7,0)+COALESCE(MONTH_BAL_SUM_8,0)+COALESCE(MONTH_BAL_SUM_9,0))/
                             (CASE  COALESCE(MONTH_DAYS_7,0) + COALESCE(MONTH_DAYS_8,0) + COALESCE(MONTH_DAYS_9,0) WHEN 0 THEN 1
		                                                    ELSE COALESCE(MONTH_DAYS_7,0) + COALESCE(MONTH_DAYS_8,0) + COALESCE(MONTH_DAYS_9,0)
		                                                     END ))                     
                  ELSE  SUM((COALESCE(MONTH_BAL_SUM_10,0)+COALESCE(MONTH_BAL_SUM_11,0)+COALESCE(MONTH_BAL_SUM_12,0))/
                             (CASE  COALESCE(MONTH_DAYS_10,0) + COALESCE(MONTH_DAYS_11,0) + COALESCE(MONTH_DAYS_12,0) WHEN 0 THEN 1
		                                                    ELSE COALESCE(MONTH_DAYS_10,0) + COALESCE(MONTH_DAYS_11,0) + COALESCE(MONTH_DAYS_12,0)
		                                                     END ))                 
           END as decimal(18,2)) as CUR_QUARTER_AVG 
       ,CUST_TYP as CUST_TYP 
   FROM ACRM_F_RE_SAVESUMAVGINFO  A
   WHERE YEAR = YEAR(V_DT) 
  GROUP BY FR_ID 
       ,CUST_ID 
       ,TYPE 
       ,CUST_TYP """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_YEAR_ST\b", "'"+V_YEAR_ST+"'", sql)
sql = re.sub(r"\bV_DT_DAYS\b", "'"+V_DT_DAYS+"'", sql)
TMP_ACRM_F_CI_GK_SAVE_01 = sqlContext.sql(sql)
TMP_ACRM_F_CI_GK_SAVE_01.registerTempTable("TMP_ACRM_F_CI_GK_SAVE_01")
dfn="TMP_ACRM_F_CI_GK_SAVE_01/"+V_DT+".parquet"
TMP_ACRM_F_CI_GK_SAVE_01.cache()
nrows = TMP_ACRM_F_CI_GK_SAVE_01.count()
TMP_ACRM_F_CI_GK_SAVE_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_ACRM_F_CI_GK_SAVE_01.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_ACRM_F_CI_GK_SAVE_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_ACRM_F_CI_GK_SAVE_01 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02:
V_STEP = V_STEP + 1
#年初清除表中数据
if etl_date[4:8] == '0101' :
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_GK_SAVE/*.parquet ")
	
	sql = """
	 SELECT CUST_ID                 AS CUST_ID 
		   ,cast(0 as decimal(18,2)) AS CUR_AC_BL 
		   ,cast(SUM(A.AMOUNT) as decimal(18,2))     AS LAST_AC_BL 
		   ,cast(SUM(COALESCE(YEAR_BAL_SUM,0))/datediff(V_LT_YEAR_ET,V_LT_YEAR_ST)as decimal(18,2)) AS LAST_YEAR_AVG 
		   ,cast(0 as decimal(18,2))  AS CUR_YEAR_AVG 
		   ,cast(SUM(CASE MONTH(V_DT) WHEN 1 THEN COALESCE(MONTH_BAL_SUM_1, 0) / COALESCE(MONTH_DAYS_1, 0) WHEN 2 THEN COALESCE(MONTH_BAL_SUM_2, 0) / COALESCE(MONTH_DAYS_2, 0) WHEN 3 THEN COALESCE(MONTH_BAL_SUM_3, 0) / COALESCE(MONTH_DAYS_3, 0) WHEN 4 THEN COALESCE(MONTH_BAL_SUM_4, 0) / COALESCE(MONTH_DAYS_4, 0) WHEN 5 THEN COALESCE(MONTH_BAL_SUM_5, 0) / COALESCE(MONTH_DAYS_5, 0) WHEN 6 THEN COALESCE(MONTH_BAL_SUM_6, 0) / COALESCE(MONTH_DAYS_6, 0) WHEN 7 THEN COALESCE(MONTH_BAL_SUM_7, 0) / COALESCE(MONTH_DAYS_7, 0) WHEN 8 THEN COALESCE(MONTH_BAL_SUM_8, 0) / COALESCE(MONTH_DAYS_8, 0) WHEN 9 THEN COALESCE(MONTH_BAL_SUM_9, 0) / COALESCE(MONTH_DAYS_9, 0) WHEN 10 THEN COALESCE(MONTH_BAL_SUM_10, 0) / COALESCE(MONTH_DAYS_10, 0) WHEN 11 THEN COALESCE(MONTH_BAL_SUM_11, 0) / COALESCE(MONTH_DAYS_11, 0) WHEN 12 THEN COALESCE(MONTH_BAL_SUM_12, 0) / COALESCE(MONTH_DAYS_12, 0) END) as decimal(18,2)) AS LAST_MONTH_AVG 
		   ,cast(0 as decimal(18,2)) AS CUR_MONTH_AVG 
		   ,cast(CASE QUARTER(V_DT)   WHEN 1 THEN  SUM((COALESCE(MONTH_BAL_SUM_1,0)+COALESCE(MONTH_BAL_SUM_2,0)+COALESCE(MONTH_BAL_SUM_3,0))/
                             (CASE  COALESCE(MONTH_DAYS_1,0) + COALESCE(MONTH_DAYS_2,0) + COALESCE(MONTH_DAYS_3,0) WHEN 0 THEN 1
		                                                    ELSE COALESCE(MONTH_DAYS_1,0) + COALESCE(MONTH_DAYS_2,0) + COALESCE(MONTH_DAYS_3,0)
		                                                     END )) 
          WHEN 2 THEN  SUM((COALESCE(MONTH_BAL_SUM_4,0)+COALESCE(MONTH_BAL_SUM_5,0)+COALESCE(MONTH_BAL_SUM_6,0))/
                             (CASE  COALESCE(MONTH_DAYS_4,0) + COALESCE(MONTH_DAYS_5,0) + COALESCE(MONTH_DAYS_6,0) WHEN 0 THEN 1
		                                                    ELSE COALESCE(MONTH_DAYS_4,0) + COALESCE(MONTH_DAYS_5,0) + COALESCE(MONTH_DAYS_6,0)
		                                                     END  ))      
          WHEN 3 THEN   SUM((COALESCE(MONTH_BAL_SUM_7,0)+COALESCE(MONTH_BAL_SUM_8,0)+COALESCE(MONTH_BAL_SUM_9,0))/
                             (CASE  COALESCE(MONTH_DAYS_7,0) + COALESCE(MONTH_DAYS_8,0) + COALESCE(MONTH_DAYS_9,0) WHEN 0 THEN 1
		                                                    ELSE COALESCE(MONTH_DAYS_7,0) + COALESCE(MONTH_DAYS_8,0) + COALESCE(MONTH_DAYS_9,0)
		                                                     END ))                     
                  ELSE  SUM((COALESCE(MONTH_BAL_SUM_10,0)+COALESCE(MONTH_BAL_SUM_11,0)+COALESCE(MONTH_BAL_SUM_12,0))/
                             (CASE  COALESCE(MONTH_DAYS_10,0) + COALESCE(MONTH_DAYS_11,0) + COALESCE(MONTH_DAYS_12,0) WHEN 0 THEN 1
		                                                    ELSE COALESCE(MONTH_DAYS_10,0) + COALESCE(MONTH_DAYS_11,0) + COALESCE(MONTH_DAYS_12,0)
		                                                     END ))                 
           END as decimal(18,2)) AS LAST_QUARTER_AVG 
		   ,cast(0 as decimal(18,2))  AS CUR_QUARTER_AVG 
		   ,CUST_TYP                AS CUST_TYP 
		   ,TYPE                    AS ACCOUNT_TYPE 
		   ,FR_ID                   AS FR_ORG_ID 
		   ,V_DT                    AS ETL_DATE 
	   FROM ACRM_F_RE_SAVESUMAVGINFO A   WHERE YEAR= YEAR(add_months(V_DT,-12)) 
	  GROUP BY FR_ID 
		   ,CUST_ID 
		   ,TYPE 
		   ,CUST_TYP """

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	sql = re.sub(r"\bV_LT_YEAR_ET\b", "'"+V_LT_YEAR_ET+"'", sql)
	sql = re.sub(r"\bV_LT_YEAR_ST\b", "'"+V_LT_YEAR_ST+"'", sql)
	ACRM_F_CI_GK_SAVE = sqlContext.sql(sql)
	ACRM_F_CI_GK_SAVE.registerTempTable("ACRM_F_CI_GK_SAVE")
	dfn="ACRM_F_CI_GK_SAVE/"+V_DT+".parquet"
	ACRM_F_CI_GK_SAVE.cache()
	nrows = ACRM_F_CI_GK_SAVE.count()
	ACRM_F_CI_GK_SAVE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
	ACRM_F_CI_GK_SAVE.unpersist()
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_GK_SAVE/"+V_DT_LD+".parquet")
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_GK_SAVE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-03::
V_STEP = V_STEP + 1

ACRM_F_CI_GK_SAVE = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_GK_SAVE_BK/'+V_DT_LD+'.parquet/*')
ACRM_F_CI_GK_SAVE.registerTempTable("ACRM_F_CI_GK_SAVE")

sql = """
 SELECT B.CUST_ID               AS CUST_ID 
       ,B.AMOUNT                AS CUR_AC_BL 
       ,cast(COALESCE(A.LAST_AC_BL, 0)as decimal(18,2))  AS LAST_AC_BL 
       ,cast(COALESCE(A.LAST_YEAR_AVG, 0) as decimal(18,2))    AS LAST_YEAR_AVG 
       ,B.CUR_YEAR_AVG          AS CUR_YEAR_AVG 
       ,cast(COALESCE(A.LAST_MONTH_AVG, 0)as decimal(18,2))  AS LAST_MONTH_AVG 
       ,B.CUR_MONTH_AVG         AS CUR_MONTH_AVG 
       ,cast(COALESCE(A.LAST_QUARTER_AVG, 0)as decimal(18,2))  AS LAST_QUARTER_AVG 
       ,B.CUR_QUARTER_AVG       AS CUR_QUARTER_AVG 
       ,B.CUST_TYP              AS CUST_TYP 
       ,B.TYPE                  AS ACCOUNT_TYPE 
       ,B.FR_ID                 AS FR_ORG_ID 
       ,V_DT                    AS ETL_DATE 
   FROM TMP_ACRM_F_CI_GK_SAVE_01 B                             --存款业务概况临时表01
   LEFT JOIN ACRM_F_CI_GK_SAVE A                               --存款业务概况
     ON A.FR_ORG_ID             = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID 
    AND A.ACCOUNT_TYPE          = B.TYPE 
    AND A.CUST_TYP              = B.CUST_TYP """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_GK_SAVE_INNTMP1 = sqlContext.sql(sql)
ACRM_F_CI_GK_SAVE_INNTMP1.registerTempTable("ACRM_F_CI_GK_SAVE_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                --客户号:src.CUST_ID
       ,DST.CUR_AC_BL                              --本年余额:src.CUR_AC_BL
       ,DST.LAST_AC_BL                             --上年余额:src.LAST_AC_BL
       ,DST.LAST_YEAR_AVG                          --上年日均:src.LAST_YEAR_AVG
       ,DST.CUR_YEAR_AVG                           --本年日均:src.CUR_YEAR_AVG
       ,DST.LAST_MONTH_AVG                        --上年月均:src.LAST_MONTH_AVG
       ,DST.CUR_MONTH_AVG                          --本年月均:src.CUR_MONTH_AVG
       ,DST.LAST_QUARTER_AVG                    --上年季均:src.LAST_QUARTER_AVG
       ,DST.CUR_QUARTER_AVG                     --本年季均:src.CUR_QUARTER_AVG
       ,DST.CUST_TYP                              --客户类型:src.CUST_TYP
       ,DST.ACCOUNT_TYPE                           --账户类型:src.ACCOUNT_TYPE
       ,DST.FR_ORG_ID                            --所属法人机构ID:src.FR_ORG_ID
       ,DST.ETL_DATE                             --加工日期:src.ETL_DATE
   FROM ACRM_F_CI_GK_SAVE DST 
   LEFT JOIN ACRM_F_CI_GK_SAVE_INNTMP1 SRC 
     ON SRC.FR_ORG_ID           = DST.FR_ORG_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.ACCOUNT_TYPE        = DST.ACCOUNT_TYPE 
  WHERE SRC.FR_ORG_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_CI_GK_SAVE_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_CI_GK_SAVE/"+V_DT+".parquet"
ACRM_F_CI_GK_SAVE_INNTMP2=ACRM_F_CI_GK_SAVE_INNTMP2.unionAll(ACRM_F_CI_GK_SAVE_INNTMP1)
ACRM_F_CI_GK_SAVE_INNTMP1.cache()
ACRM_F_CI_GK_SAVE_INNTMP2.cache()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_GK_SAVE/*.parquet")
ACRM_F_CI_GK_SAVE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
nrowsi = ACRM_F_CI_GK_SAVE_INNTMP1.count()
nrowsa = ACRM_F_CI_GK_SAVE_INNTMP2.count()
ACRM_F_CI_GK_SAVE_INNTMP1.unpersist()
ACRM_F_CI_GK_SAVE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_CI_GK_SAVE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_CI_GK_SAVE_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/ACRM_F_CI_GK_SAVE/"+V_DT+".parquet /"+dbname+"/ACRM_F_CI_GK_SAVE_BK/")

