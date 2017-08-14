#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os, calendar

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_DP_CARD_INFO').setMaster(sys.argv[2])
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
#本月月末日期（10位）
monthRange = calendar.monthrange(int(etl_date[0:3]),int(etl_date[4:6]))#得到本月的天数   
V_LAST_DAY = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(str(monthRange[1])))).strftime("%Y-%m-%d") 
#上月末日期
V_DT_LMD = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1) + timedelta(-1)).strftime("%Y%m%d")
#10位日期
V_DT10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8]))).strftime("%Y-%m-%d")
V_STEP = 0

OCRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_DP_CARD_INFO/*')
OCRM_F_DP_CARD_INFO.registerTempTable("OCRM_F_DP_CARD_INFO")
F_DP_CBOD_CRCRDCOM = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_CRCRDCOM/*')
F_DP_CBOD_CRCRDCOM.registerTempTable("F_DP_CBOD_CRCRDCOM")
ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

#目标表：ACRM_F_DP_CARD_INFO，连续三次增改
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_DP_CARD_INFO/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_DP_CARD_INFO_BK/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_DP_CARD_INFO/"+V_DT+".parquet")

ACRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_CARD_INFO/*')
ACRM_F_DP_CARD_INFO.registerTempTable("ACRM_F_DP_CARD_INFO")
#任务[12] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT N.CUST_ID               AS CUST_ID 
       ,N.ODS_ACCT_NO           AS ACCT_NO 
       ,N.BAL_RMB               AS CARD_BAL 
       ,N.MONTH_AVG             AS CARD_MONTH_BAL 
       ,N.MVAL_RMB              AS CARD_YEAR_BAL 
       ,''                    AS IF_NEW_CARD 
       ,N.FR_ID                 AS FR_ID 
       ,V_DT               AS ODS_ST_DATE 
   FROM (SELECT B.CUST_ID,B.ODS_ACCT_NO,B.BAL_RMB,B.MONTH_AVG,B.MVAL_RMB,B.FR_ID
               FROM (SELECT DISTINCT CR_RSV_ACCT_NO,CR_CURR_COD ,FR_ID
                       FROM F_DP_CBOD_CRCRDCOM WHERE ODS_ST_DATE = V_DT_8
												) A
                  ,ACRM_F_DP_SAVE_INFO B
              WHERE B.CRM_DT = V_DT
                AND B.FR_ID = A.FR_ID AND B.ACCONT_TYPE = 'H'
                AND A.CR_RSV_ACCT_NO = B.ODS_ACCT_NO AND A.CR_CURR_COD = B.CYNO)  N                                  --
  LEFT JOIN ACRM_F_DP_CARD_INFO M                             --负债协议表
     ON M.ACCT_NO = N.ODS_ACCT_NO AND M.FR_ID = N.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT_8\b", "'"+V_DT+"'", sql)
ACRM_F_DP_CARD_INFO_INNTMP1 = sqlContext.sql(sql)
ACRM_F_DP_CARD_INFO_INNTMP1.registerTempTable("ACRM_F_DP_CARD_INFO_INNTMP1")

sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.ACCT_NO                                            --账号:src.ACCT_NO
       ,DST.CARD_BAL                                           --借记卡余额:src.CARD_BAL
       ,DST.CARD_MONTH_BAL                                     --借记卡月日均:src.CARD_MONTH_BAL
       ,DST.CARD_YEAR_BAL                                      --借记卡年日均:src.CARD_YEAR_BAL
       ,DST.IF_NEW_CARD                                        --是否新开卡客户(一个月):src.IF_NEW_CARD
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --CRM日期:src.ODS_ST_DATE
   FROM ACRM_F_DP_CARD_INFO DST 
   LEFT JOIN ACRM_F_DP_CARD_INFO_INNTMP1 SRC 
     ON SRC.ACCT_NO             = DST.ACCT_NO 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.ACCT_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
ACRM_F_DP_CARD_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="ACRM_F_DP_CARD_INFO/"+V_DT+".parquet"
ACRM_F_DP_CARD_INFO_INNTMP2=ACRM_F_DP_CARD_INFO_INNTMP2.unionAll(ACRM_F_DP_CARD_INFO_INNTMP1)
ACRM_F_DP_CARD_INFO_INNTMP1.cache()
ACRM_F_DP_CARD_INFO_INNTMP2.cache()
nrowsi = ACRM_F_DP_CARD_INFO_INNTMP1.count()
nrowsa = ACRM_F_DP_CARD_INFO_INNTMP2.count()
ACRM_F_DP_CARD_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
ACRM_F_DP_CARD_INFO_INNTMP1.unpersist()
ACRM_F_DP_CARD_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_CARD_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_DP_CARD_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_DP_CARD_INFO_BK/")

#判断是否为月末日期
if V_LAST_DAY == V_DT10 :
	
	#更新所有客户的是否新开卡客户为否
	#任务[12] 001-02::
	V_STEP = V_STEP + 1
	ACRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_CARD_INFO/*')
	ACRM_F_DP_CARD_INFO.registerTempTable("ACRM_F_DP_CARD_INFO")
	sql = """
	 SELECT A.CUST_ID               AS CUST_ID 
		   ,A.CUST_ID               AS ACCT_NO 
		   ,A.CARD_BAL              AS CARD_BAL 
		   ,A.CARD_MONTH_BAL        AS CARD_MONTH_BAL 
		   ,A.CARD_YEAR_BAL         AS CARD_YEAR_BAL 
		   ,CASE WHEN IF_NEW_CARD = '1' THEN '2'   ELSE IF_NEW_CARD END                  AS IF_NEW_CARD 
		   ,A.FR_ID                 AS FR_ID 
		   ,A.ODS_ST_DATE           AS ODS_ST_DATE 
	   FROM ACRM_F_DP_CARD_INFO A                                  --借记卡信息表
	   """
	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	ACRM_F_DP_CARD_INFO = sqlContext.sql(sql)
	ACRM_F_DP_CARD_INFO.registerTempTable("ACRM_F_DP_CARD_INFO")
	dfn="ACRM_F_DP_CARD_INFO/"+V_DT+".parquet"
	ACRM_F_DP_CARD_INFO.cache()
	nrows = ACRM_F_DP_CARD_INFO.count()
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_DP_CARD_INFO/*.parquet")
	ACRM_F_DP_CARD_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
	ACRM_F_DP_CARD_INFO.unpersist()
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_CARD_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

		
	#是否新开卡客户（判断开卡日期是否本月）	
	#任务[12] 001-03::
	V_STEP = V_STEP + 1
	ACRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_CARD_INFO/*')
	ACRM_F_DP_CARD_INFO.registerTempTable("ACRM_F_DP_CARD_INFO")
	sql = """
	 SELECT A.CUST_ID               AS CUST_ID 
		   ,A.ACCT_NO               AS ACCT_NO 
		   ,A.CARD_BAL              AS CARD_BAL 
		   ,A.CARD_MONTH_BAL        AS CARD_MONTH_BAL 
		   ,A.CARD_YEAR_BAL         AS CARD_YEAR_BAL 
		   ,'1'                     AS IF_NEW_CARD 
		   ,A.FR_ID                 AS FR_ID 
		   ,A.ODS_ST_DATE           AS ODS_ST_DATE 
	   FROM ACRM_F_DP_CARD_INFO A                                  --借记卡信息表
	  INNER JOIN 
	 (SELECT DISTINCT A.CR_RSV_ACCT_NO,A.FR_ID
					   FROM F_DP_CBOD_CRCRDCOM A,OCRM_F_DP_CARD_INFO B
					  WHERE A.FR_ID = B.FR_ID 
						AND A.FK_CRCRD_KEY = B.CR_CRD_NO 
						AND substr(B.CR_OPCR_DATE,1,6) = substr(V_DT8,1,6)) B                                          --
		 ON A.FR_ID                 = B.FR_ID 
		AND A.ACCT_NO               = B.CR_RSV_ACCT_NO """

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	sql = re.sub(r"\bV_DT8\b", "'"+V_DT+"'", sql)
	ACRM_F_DP_CARD_INFO_INNTMP1 = sqlContext.sql(sql)
	ACRM_F_DP_CARD_INFO_INNTMP1.registerTempTable("ACRM_F_DP_CARD_INFO_INNTMP1")

	
	sql = """
	 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
		   ,DST.ACCT_NO                                            --账号:src.ACCT_NO
		   ,DST.CARD_BAL                                           --借记卡余额:src.CARD_BAL
		   ,DST.CARD_MONTH_BAL                                     --借记卡月日均:src.CARD_MONTH_BAL
		   ,DST.CARD_YEAR_BAL                                      --借记卡年日均:src.CARD_YEAR_BAL
		   ,DST.IF_NEW_CARD                                        --是否新开卡客户(一个月):src.IF_NEW_CARD
		   ,DST.FR_ID                                              --法人号:src.FR_ID
		   ,DST.ODS_ST_DATE                                        --CRM日期:src.ODS_ST_DATE
	   FROM ACRM_F_DP_CARD_INFO DST 
	   LEFT JOIN ACRM_F_DP_CARD_INFO_INNTMP1 SRC 
		 ON SRC.ACCT_NO             = DST.ACCT_NO 
		AND SRC.FR_ID               = DST.FR_ID 
	  WHERE SRC.ACCT_NO IS NULL """

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	ACRM_F_DP_CARD_INFO_INNTMP2 = sqlContext.sql(sql)
	dfn="ACRM_F_DP_CARD_INFO/"+V_DT+".parquet"
	ACRM_F_DP_CARD_INFO_INNTMP2=ACRM_F_DP_CARD_INFO_INNTMP2.unionAll(ACRM_F_DP_CARD_INFO_INNTMP1)
	ACRM_F_DP_CARD_INFO_INNTMP1.cache()
	ACRM_F_DP_CARD_INFO_INNTMP2.cache()
	nrowsi = ACRM_F_DP_CARD_INFO_INNTMP1.count()
	nrowsa = ACRM_F_DP_CARD_INFO_INNTMP2.count()
	ACRM_F_DP_CARD_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
	ACRM_F_DP_CARD_INFO_INNTMP1.unpersist()
	ACRM_F_DP_CARD_INFO_INNTMP2.unpersist()
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_F_DP_CARD_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
	#ret = os.system("hdfs dfs -mv /"+dbname+"/ACRM_F_DP_CARD_INFO/"+V_DT_LD+".parquet /"+dbname+"/ACRM_F_DP_CARD_INFO_BK/")

#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_F_DP_CARD_INFO_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/ACRM_F_DP_CARD_INFO/"+V_DT+".parquet /"+dbname+"/ACRM_F_DP_CARD_INFO_BK/"+V_DT+".parquet")
