#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_SYS_PROD_INFO_UPDATE').setMaster(sys.argv[2])
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

#任务[21] 001-01::
V_STEP = V_STEP + 1

OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")

OCRM_F_PD_PROD_CATL_VIEW = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_CATL_VIEW/*')
OCRM_F_PD_PROD_CATL_VIEW.registerTempTable("OCRM_F_PD_PROD_CATL_VIEW")

ACRM_F_DP_SAVE_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_SAVE_INFO/*')
ACRM_F_DP_SAVE_INFO.registerTempTable("ACRM_F_DP_SAVE_INFO")

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")

ACRM_F_NI_FINANCING = sqlContext.read.parquet(hdfs+'/ACRM_F_NI_FINANCING/*')
ACRM_F_NI_FINANCING.registerTempTable("ACRM_F_NI_FINANCING")


# --默认产品为非热销
sql = """
 SELECT IS_CHOICE               AS IS_CHOICE 
       ,'00'                    AS IS_HOT 
       ,PROD_CASE               AS PROD_CASE 
       ,TRANSCONDITION          AS TRANSCONDITION 
       ,FR_ID                   AS FR_ID 
       ,PROD_BUS_ID             AS PROD_BUS_ID 
       ,RISK_LEVEL              AS RISK_LEVEL 
       ,PROD_SWITCH             AS PROD_SWITCH 
       ,ASSURE_DISC             AS ASSURE_DISC 
       ,CHANNEL_DISC            AS CHANNEL_DISC 
       ,DANGER_DISC             AS DANGER_DISC 
       ,OBJ_CUST_DISC           AS OBJ_CUST_DISC 
       ,PROD_CHARACT            AS PROD_CHARACT 
       ,LIMIT_TIME              AS LIMIT_TIME 
       ,COST_RATE               AS COST_RATE 
       ,RATE                    AS RATE 
       ,PROD_DEPT               AS PROD_DEPT 
       ,PROD_SEQ                AS PROD_SEQ 
       ,PROD_QUERY_URL          AS PROD_QUERY_URL 
       ,PROD_SHOW_URL           AS PROD_SHOW_URL 
       ,CREATE_DATE             AS CREATE_DATE 
       ,PROD_CREATOR            AS PROD_CREATOR 
       ,PROD_STATE              AS PROD_STATE 
       ,PROD_END_DATE           AS PROD_END_DATE 
       ,PROD_START_DATE         AS PROD_START_DATE 
       ,DISPLAY_FLAG            AS DISPLAY_FLAG 
       ,PROD_DESC               AS PROD_DESC 
       ,PROD_TYPE_ID            AS PROD_TYPE_ID 
       ,TJKJ                    AS TJKJ 
       ,CATL_CODE               AS CATL_CODE 
       ,PROD_NAME               AS PROD_NAME 
       ,PRODUCT_ID              AS PRODUCT_ID 
       ,ID                      AS ID 
   FROM OCRM_F_PD_PROD_INFO A                                  --
"""

OCRM_F_PD_PROD_INFO = sqlContext.sql(sql)
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")

V_STEP = V_STEP + 1
#  --更新存款产品(按金额汇总)
sql = """
 SELECT *
               FROM   
                   (  SELECT PRODUCT_ID,
                          CATL_CODE,
                          FR_ID,
                          BAL,
                          ROW_NUMBER () OVER ( PARTITION BY CATL_CODE,FR_ID ORDER BY BAL DESC) AS RN
                       FROM (
                              SELECT A.PRODUCT_ID,
                                     M.CATL_CODE,
                                     A.FR_ID,
                                     SUM(A.BAL_RMB) AS BAL
                                FROM  ACRM_F_DP_SAVE_INFO A
                                LEFT JOIN (
                                            SELECT  B.PRODUCT_ID,A.CATL_CODE 
                                               FROM OCRM_F_PD_PROD_CATL_VIEW A,
                                                    OCRM_F_PD_PROD_INFO B
                                         INNER JOIN OCRM_F_PD_PROD_CATL_VIEW C
                                                 ON B.CATL_CODE = C.CATL_CODE
                                             WHERE A.CATL_LEVEL = '2'                                 --取存款产品二级目录
                                                AND A.CATL_PARENT = 'A' 
                                                AND LOCATE(A.CATLSEQ,C.CATLSEQ) > 0 ) M
                                       ON A.PRODUCT_ID = M.PRODUCT_ID
                               WHERE  A.ACCT_STATUS = '01'
                               GROUP BY  A.PRODUCT_ID,A.FR_ID ,M.CATL_CODE                 
                         )
                )
           WHERE RN = 1                                  --
"""
TMP_01 = sqlContext.sql(sql)
TMP_01.registerTempTable("TMP_01")

sql = """
 SELECT A.IS_CHOICE               AS IS_CHOICE 
       ,concat(SUBSTR(A.IS_HOT,1,1),case when B.PRODUCT_ID is not null then 1 else 0 end ) AS IS_HOT 
       ,A.PROD_CASE               AS PROD_CASE 
       ,A.TRANSCONDITION          AS TRANSCONDITION 
       ,A.FR_ID                   AS FR_ID 
       ,A.PROD_BUS_ID             AS PROD_BUS_ID 
       ,A.RISK_LEVEL              AS RISK_LEVEL 
       ,A.PROD_SWITCH             AS PROD_SWITCH 
       ,A.ASSURE_DISC             AS ASSURE_DISC 
       ,A.CHANNEL_DISC            AS CHANNEL_DISC 
       ,A.DANGER_DISC             AS DANGER_DISC 
       ,A.OBJ_CUST_DISC           AS OBJ_CUST_DISC 
       ,A.PROD_CHARACT            AS PROD_CHARACT 
       ,A.LIMIT_TIME              AS LIMIT_TIME 
       ,A.COST_RATE               AS COST_RATE 
       ,A.RATE                    AS RATE 
       ,A.PROD_DEPT               AS PROD_DEPT 
       ,A.PROD_SEQ                AS PROD_SEQ 
       ,A.PROD_QUERY_URL          AS PROD_QUERY_URL 
       ,A.PROD_SHOW_URL           AS PROD_SHOW_URL 
       ,A.CREATE_DATE             AS CREATE_DATE 
       ,A.PROD_CREATOR            AS PROD_CREATOR 
       ,A.PROD_STATE              AS PROD_STATE 
       ,A.PROD_END_DATE           AS PROD_END_DATE 
       ,A.PROD_START_DATE         AS PROD_START_DATE 
       ,A.DISPLAY_FLAG            AS DISPLAY_FLAG 
       ,A.PROD_DESC               AS PROD_DESC 
       ,A.PROD_TYPE_ID            AS PROD_TYPE_ID 
       ,A.TJKJ                    AS TJKJ 
       ,A.CATL_CODE               AS CATL_CODE 
       ,A.PROD_NAME               AS PROD_NAME 
       ,A.PRODUCT_ID              AS PRODUCT_ID 
       ,A.ID                      AS ID 
   FROM OCRM_F_PD_PROD_INFO A LEFT JOIN TMP_01 B ON A.PRODUCT_ID = B.PRODUCT_ID AND A.FR_ID = B.FR_ID 
"""
OCRM_F_PD_PROD_INFO = sqlContext.sql(sql)
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")

V_STEP = V_STEP + 1
# --更新存款产品(按持有数量汇总)
sql = """
 SELECT *
                 FROM   
                   (  SELECT PRODUCT_ID,
                          CATL_CODE,
                          FR_ID,
                          NUM,
                          ROW_NUMBER () OVER ( PARTITION BY CATL_CODE,FR_ID ORDER BY NUM DESC) AS RN
                       FROM (
                              SELECT A.PRODUCT_ID,
                                     M.CATL_CODE,
                                     A.FR_ID,
                                     COUNT(1) AS NUM
                                FROM  ( SELECT DISTINCT PRODUCT_ID,CUST_ID,FR_ID FROM ACRM_F_DP_SAVE_INFO WHERE  ACCT_STATUS = '01') A
                                LEFT JOIN (
                                            SELECT  B.PRODUCT_ID,A.CATL_CODE 
                                               FROM OCRM_F_PD_PROD_CATL_VIEW A,
                                                    OCRM_F_PD_PROD_INFO B
                                         INNER JOIN OCRM_F_PD_PROD_CATL_VIEW C
                                                 ON B.CATL_CODE = C.CATL_CODE 
                                             WHERE A.CATL_LEVEL = '2'                                 --取存款产品二级目录
                                                AND A.CATL_PARENT = 'A' 
                                                AND LOCATE(A.CATLSEQ,C.CATLSEQ) > 0 ) M
                                       ON A.PRODUCT_ID = M.PRODUCT_ID
                               GROUP BY  A.PRODUCT_ID,A.FR_ID ,M.CATL_CODE                 
                         )
                )
           WHERE RN = 1 
"""
TMP_01 = sqlContext.sql(sql)
TMP_01.registerTempTable("TMP_01")



sql = """
 SELECT A.IS_CHOICE               AS IS_CHOICE 
       ,concat(case when B.PRODUCT_ID is not null then 1 else 0 end,SUBSTR(A.IS_HOT,1,1)) AS IS_HOT 
       ,A.PROD_CASE               AS PROD_CASE 
       ,A.TRANSCONDITION          AS TRANSCONDITION 
       ,A.FR_ID                   AS FR_ID 
       ,A.PROD_BUS_ID             AS PROD_BUS_ID 
       ,A.RISK_LEVEL              AS RISK_LEVEL 
       ,A.PROD_SWITCH             AS PROD_SWITCH 
       ,A.ASSURE_DISC             AS ASSURE_DISC 
       ,A.CHANNEL_DISC            AS CHANNEL_DISC 
       ,A.DANGER_DISC             AS DANGER_DISC 
       ,A.OBJ_CUST_DISC           AS OBJ_CUST_DISC 
       ,A.PROD_CHARACT            AS PROD_CHARACT 
       ,A.LIMIT_TIME              AS LIMIT_TIME 
       ,A.COST_RATE               AS COST_RATE 
       ,A.RATE                    AS RATE 
       ,A.PROD_DEPT               AS PROD_DEPT 
       ,A.PROD_SEQ                AS PROD_SEQ 
       ,A.PROD_QUERY_URL          AS PROD_QUERY_URL 
       ,A.PROD_SHOW_URL           AS PROD_SHOW_URL 
       ,A.CREATE_DATE             AS CREATE_DATE 
       ,A.PROD_CREATOR            AS PROD_CREATOR 
       ,A.PROD_STATE              AS PROD_STATE 
       ,A.PROD_END_DATE           AS PROD_END_DATE 
       ,A.PROD_START_DATE         AS PROD_START_DATE 
       ,A.DISPLAY_FLAG            AS DISPLAY_FLAG 
       ,A.PROD_DESC               AS PROD_DESC 
       ,A.PROD_TYPE_ID            AS PROD_TYPE_ID 
       ,A.TJKJ                    AS TJKJ 
       ,A.CATL_CODE               AS CATL_CODE 
       ,A.PROD_NAME               AS PROD_NAME 
       ,A.PRODUCT_ID              AS PRODUCT_ID 
       ,A.ID                      AS ID 
   FROM OCRM_F_PD_PROD_INFO A LEFT JOIN TMP_01 B ON A.PRODUCT_ID = B.PRODUCT_ID AND A.FR_ID = B.FR_ID 
"""
OCRM_F_PD_PROD_INFO = sqlContext.sql(sql)
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")

V_STEP = V_STEP + 1
#  --更新贷款产品(按销售金额汇总)
sql = """
		SELECT *
					   FROM   
						   (  SELECT PRODUCT_ID,
								  CATL_CODE,
								  FR_ID,
								  BAL,
								  ROW_NUMBER () OVER ( PARTITION BY CATL_CODE,FR_ID ORDER BY BAL DESC) AS RN
							   FROM (
									  SELECT A.PRODUCT_ID,
											 M.CATL_CODE,
											 A.FR_ID,
											 SUM(A.BAL_RMB) AS BAL
										FROM  ACRM_F_CI_ASSET_BUSI_PROTO A
										LEFT JOIN (
													SELECT  B.PRODUCT_ID,A.CATL_CODE 
													   FROM OCRM_F_PD_PROD_CATL_VIEW A,
															OCRM_F_PD_PROD_INFO B
												 INNER JOIN OCRM_F_PD_PROD_CATL_VIEW C
														 ON B.CATL_CODE = C.CATL_CODE 
													 WHERE A.CATL_LEVEL = '2'                                 --取贷款产品二级目录
														AND A.CATL_PARENT = 'B' 
														AND LOCATE(A.CATLSEQ,C.CATLSEQ) > 0 ) M
											   ON A.PRODUCT_ID = M.PRODUCT_ID
									   WHERE   A.LN_APCL_FLG = 'N'
									   GROUP BY  A.PRODUCT_ID,A.FR_ID ,M.CATL_CODE                 
								 )
						)
				   WHERE RN = 1
"""
TMP_01 = sqlContext.sql(sql)
TMP_01.registerTempTable("TMP_01")



sql = """
 SELECT A.IS_CHOICE               AS IS_CHOICE 
       ,concat(SUBSTR(A.IS_HOT,1,1),case when B.PRODUCT_ID is not null then 1 else 0 end ) AS IS_HOT 
       ,A.PROD_CASE               AS PROD_CASE 
       ,A.TRANSCONDITION          AS TRANSCONDITION 
       ,A.FR_ID                   AS FR_ID 
       ,A.PROD_BUS_ID             AS PROD_BUS_ID 
       ,A.RISK_LEVEL              AS RISK_LEVEL 
       ,A.PROD_SWITCH             AS PROD_SWITCH 
       ,A.ASSURE_DISC             AS ASSURE_DISC 
       ,A.CHANNEL_DISC            AS CHANNEL_DISC 
       ,A.DANGER_DISC             AS DANGER_DISC 
       ,A.OBJ_CUST_DISC           AS OBJ_CUST_DISC 
       ,A.PROD_CHARACT            AS PROD_CHARACT 
       ,A.LIMIT_TIME              AS LIMIT_TIME 
       ,A.COST_RATE               AS COST_RATE 
       ,A.RATE                    AS RATE 
       ,A.PROD_DEPT               AS PROD_DEPT 
       ,A.PROD_SEQ                AS PROD_SEQ 
       ,A.PROD_QUERY_URL          AS PROD_QUERY_URL 
       ,A.PROD_SHOW_URL           AS PROD_SHOW_URL 
       ,A.CREATE_DATE             AS CREATE_DATE 
       ,A.PROD_CREATOR            AS PROD_CREATOR 
       ,A.PROD_STATE              AS PROD_STATE 
       ,A.PROD_END_DATE           AS PROD_END_DATE 
       ,A.PROD_START_DATE         AS PROD_START_DATE 
       ,A.DISPLAY_FLAG            AS DISPLAY_FLAG 
       ,A.PROD_DESC               AS PROD_DESC 
       ,A.PROD_TYPE_ID            AS PROD_TYPE_ID 
       ,A.TJKJ                    AS TJKJ 
       ,A.CATL_CODE               AS CATL_CODE 
       ,A.PROD_NAME               AS PROD_NAME 
       ,A.PRODUCT_ID              AS PRODUCT_ID 
       ,A.ID                      AS ID 
   FROM OCRM_F_PD_PROD_INFO A LEFT JOIN TMP_01 B ON A.PRODUCT_ID = B.PRODUCT_ID AND A.FR_ID = B.FR_ID 
"""
OCRM_F_PD_PROD_INFO = sqlContext.sql(sql)
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")

V_STEP = V_STEP + 1
# --更新贷款产品(按持有数量汇总)
sql = """
		SELECT *
                 FROM   
                   (  SELECT PRODUCT_ID,
                          CATL_CODE,
                          FR_ID,
                          NUM,
                          ROW_NUMBER () OVER ( PARTITION BY CATL_CODE,FR_ID ORDER BY NUM DESC) AS RN
                       FROM (
                              SELECT A.PRODUCT_ID,
                                     M.CATL_CODE,
                                     A.FR_ID,
                                     COUNT(1) AS NUM
                                FROM  ( SELECT DISTINCT PRODUCT_ID,CUST_ID,FR_ID FROM ACRM_F_CI_ASSET_BUSI_PROTO WHERE  LN_APCL_FLG = 'N'  ) A
                                LEFT JOIN (
                                            SELECT  B.PRODUCT_ID,A.CATL_CODE 
                                               FROM OCRM_F_PD_PROD_CATL_VIEW A,
                                                    OCRM_F_PD_PROD_INFO B
                                         INNER JOIN OCRM_F_PD_PROD_CATL_VIEW C
                                                 ON B.CATL_CODE = C.CATL_CODE 
                                             WHERE A.CATL_LEVEL = '2'                                 --取贷款产品二级目录
                                                AND A.CATL_PARENT = 'B' 
                                                AND LOCATE(A.CATLSEQ,C.CATLSEQ) > 0 ) M
                                       ON A.PRODUCT_ID = M.PRODUCT_ID
                               GROUP BY  A.PRODUCT_ID,A.FR_ID ,M.CATL_CODE                 
                         )
                )
           WHERE RN = 1
"""
TMP_01 = sqlContext.sql(sql)
TMP_01.registerTempTable("TMP_01")


sql = """
 SELECT A.IS_CHOICE               AS IS_CHOICE 
       ,concat(case when B.PRODUCT_ID is not null then 1 else 0 end,SUBSTR(A.IS_HOT,1,1))                  AS IS_HOT 
       ,A.PROD_CASE               AS PROD_CASE 
       ,A.TRANSCONDITION          AS TRANSCONDITION 
       ,A.FR_ID                   AS FR_ID 
       ,A.PROD_BUS_ID             AS PROD_BUS_ID 
       ,A.RISK_LEVEL              AS RISK_LEVEL 
       ,A.PROD_SWITCH             AS PROD_SWITCH 
       ,A.ASSURE_DISC             AS ASSURE_DISC 
       ,A.CHANNEL_DISC            AS CHANNEL_DISC 
       ,A.DANGER_DISC             AS DANGER_DISC 
       ,A.OBJ_CUST_DISC           AS OBJ_CUST_DISC 
       ,A.PROD_CHARACT            AS PROD_CHARACT 
       ,A.LIMIT_TIME              AS LIMIT_TIME 
       ,A.COST_RATE               AS COST_RATE 
       ,A.RATE                    AS RATE 
       ,A.PROD_DEPT               AS PROD_DEPT 
       ,A.PROD_SEQ                AS PROD_SEQ 
       ,A.PROD_QUERY_URL          AS PROD_QUERY_URL 
       ,A.PROD_SHOW_URL           AS PROD_SHOW_URL 
       ,A.CREATE_DATE             AS CREATE_DATE 
       ,A.PROD_CREATOR            AS PROD_CREATOR 
       ,A.PROD_STATE              AS PROD_STATE 
       ,A.PROD_END_DATE           AS PROD_END_DATE 
       ,A.PROD_START_DATE         AS PROD_START_DATE 
       ,A.DISPLAY_FLAG            AS DISPLAY_FLAG 
       ,A.PROD_DESC               AS PROD_DESC 
       ,A.PROD_TYPE_ID            AS PROD_TYPE_ID 
       ,A.TJKJ                    AS TJKJ 
       ,A.CATL_CODE               AS CATL_CODE 
       ,A.PROD_NAME               AS PROD_NAME 
       ,A.PRODUCT_ID              AS PRODUCT_ID 
       ,A.ID                      AS ID 
   FROM OCRM_F_PD_PROD_INFO A LEFT JOIN TMP_01 B ON A.PRODUCT_ID = B.PRODUCT_ID AND A.FR_ID = B.FR_ID WHERE B.PRODUCT_ID IS NULL
"""
OCRM_F_PD_PROD_INFO= sqlContext.sql(sql)
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")


V_STEP = V_STEP + 1
#  --更新理财产品(按购买金额汇总)
sql = """
		SELECT   PRODUCT_ID,FR_ID
                     FROM  ( SELECT PRODUCT_ID,
                                    FR_ID,
                                    SUM(A.APPLAMT) AS APPLAMT
                               FROM ACRM_F_NI_FINANCING  A
                             GROUP BY PRODUCT_ID,FR_ID 
                     )
                     ORDER BY APPLAMT DESC
"""
TMP_01 = sqlContext.sql(sql)
TMP_01.registerTempTable("TMP_01")



sql = """
 SELECT A.IS_CHOICE               AS IS_CHOICE 
       ,concat(SUBSTR(A.IS_HOT,1,1),case when B.PRODUCT_ID is not null then 1 else 0 end ) AS IS_HOT 
       ,A.PROD_CASE               AS PROD_CASE 
       ,A.TRANSCONDITION          AS TRANSCONDITION 
       ,A.FR_ID                   AS FR_ID 
       ,A.PROD_BUS_ID             AS PROD_BUS_ID 
       ,A.RISK_LEVEL              AS RISK_LEVEL 
       ,A.PROD_SWITCH             AS PROD_SWITCH 
       ,A.ASSURE_DISC             AS ASSURE_DISC 
       ,A.CHANNEL_DISC            AS CHANNEL_DISC 
       ,A.DANGER_DISC             AS DANGER_DISC 
       ,A.OBJ_CUST_DISC           AS OBJ_CUST_DISC 
       ,A.PROD_CHARACT            AS PROD_CHARACT 
       ,A.LIMIT_TIME              AS LIMIT_TIME 
       ,A.COST_RATE               AS COST_RATE 
       ,A.RATE                    AS RATE 
       ,A.PROD_DEPT               AS PROD_DEPT 
       ,A.PROD_SEQ                AS PROD_SEQ 
       ,A.PROD_QUERY_URL          AS PROD_QUERY_URL 
       ,A.PROD_SHOW_URL           AS PROD_SHOW_URL 
       ,A.CREATE_DATE             AS CREATE_DATE 
       ,A.PROD_CREATOR            AS PROD_CREATOR 
       ,A.PROD_STATE              AS PROD_STATE 
       ,A.PROD_END_DATE           AS PROD_END_DATE 
       ,A.PROD_START_DATE         AS PROD_START_DATE 
       ,A.DISPLAY_FLAG            AS DISPLAY_FLAG 
       ,A.PROD_DESC               AS PROD_DESC 
       ,A.PROD_TYPE_ID            AS PROD_TYPE_ID 
       ,A.TJKJ                    AS TJKJ 
       ,A.CATL_CODE               AS CATL_CODE 
       ,A.PROD_NAME               AS PROD_NAME 
       ,A.PRODUCT_ID              AS PRODUCT_ID 
       ,A.ID                      AS ID 
   FROM OCRM_F_PD_PROD_INFO A LEFT JOIN TMP_01 B ON A.PRODUCT_ID = B.PRODUCT_ID AND A.FR_ID = B.FR_ID WHERE B.PRODUCT_ID IS NULL
"""
OCRM_F_PD_PROD_INFO = sqlContext.sql(sql)
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")

V_STEP = V_STEP + 1
# --更新理财产品(按购买份数汇总)
sql = """
		 SELECT   PRODUCT_ID,FR_ID
                     FROM  ( SELECT PRODUCT_ID,
                                    FR_ID,
                                    SUM(A.APPLPER) AS APPLPER
                               FROM ACRM_F_NI_FINANCING  A
                             GROUP BY PRODUCT_ID,FR_ID 
                     )
                     ORDER BY APPLPER DESC
"""
TMP_01 = sqlContext.sql(sql)
TMP_01.registerTempTable("TMP_01")


sql = """
 SELECT A.IS_CHOICE               AS IS_CHOICE 
       ,concat(case when B.PRODUCT_ID is not null then 1 else 0 end,SUBSTR(A.IS_HOT,1,1)) AS IS_HOT 
       ,A.PROD_CASE               AS PROD_CASE 
       ,A.TRANSCONDITION          AS TRANSCONDITION 
       ,A.FR_ID                   AS FR_ID 
       ,A.PROD_BUS_ID             AS PROD_BUS_ID 
       ,A.RISK_LEVEL              AS RISK_LEVEL 
       ,A.PROD_SWITCH             AS PROD_SWITCH 
       ,A.ASSURE_DISC             AS ASSURE_DISC 
       ,A.CHANNEL_DISC            AS CHANNEL_DISC 
       ,A.DANGER_DISC             AS DANGER_DISC 
       ,A.OBJ_CUST_DISC           AS OBJ_CUST_DISC 
       ,A.PROD_CHARACT            AS PROD_CHARACT 
       ,A.LIMIT_TIME              AS LIMIT_TIME 
       ,A.COST_RATE               AS COST_RATE 
       ,A.RATE                    AS RATE 
       ,A.PROD_DEPT               AS PROD_DEPT 
       ,A.PROD_SEQ                AS PROD_SEQ 
       ,A.PROD_QUERY_URL          AS PROD_QUERY_URL 
       ,A.PROD_SHOW_URL           AS PROD_SHOW_URL 
       ,A.CREATE_DATE             AS CREATE_DATE 
       ,A.PROD_CREATOR            AS PROD_CREATOR 
       ,A.PROD_STATE              AS PROD_STATE 
       ,A.PROD_END_DATE           AS PROD_END_DATE 
       ,A.PROD_START_DATE         AS PROD_START_DATE 
       ,A.DISPLAY_FLAG            AS DISPLAY_FLAG 
       ,A.PROD_DESC               AS PROD_DESC 
       ,A.PROD_TYPE_ID            AS PROD_TYPE_ID 
       ,A.TJKJ                    AS TJKJ 
       ,A.CATL_CODE               AS CATL_CODE 
       ,A.PROD_NAME               AS PROD_NAME 
       ,A.PRODUCT_ID              AS PRODUCT_ID 
       ,A.ID                      AS ID 
   FROM OCRM_F_PD_PROD_INFO A LEFT JOIN TMP_01 B ON A.PRODUCT_ID = B.PRODUCT_ID AND A.FR_ID = B.FR_ID WHERE B.PRODUCT_ID IS NULL
"""
OCRM_F_PD_PROD_INFO = sqlContext.sql(sql)
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")

dfn="OCRM_F_PD_PROD_INFO/"+V_DT+".parquet"
OCRM_F_PD_PROD_INFO.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_PD_PROD_INFO.unpersist()

et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_PD_PROD_INFO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_PD_PROD_INFO_BK/")
