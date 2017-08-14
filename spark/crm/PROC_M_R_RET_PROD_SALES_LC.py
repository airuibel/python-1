#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_RET_PROD_SALES_LC').setMaster(sys.argv[2])
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
#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
ACRM_F_RE_ACCSUMAVGINFO = sqlContext.read.parquet(hdfs+'/ACRM_F_RE_ACCSUMAVGINFO/*')
ACRM_F_RE_ACCSUMAVGINFO.registerTempTable("ACRM_F_RE_ACCSUMAVGINFO")
OCRM_F_CI_BELONG_CUSTMGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_CUSTMGR/*')
OCRM_F_CI_BELONG_CUSTMGR.registerTempTable("OCRM_F_CI_BELONG_CUSTMGR")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
OCRM_F_CI_BELONG_ORG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_BELONG_ORG/*')
OCRM_F_CI_BELONG_ORG.registerTempTable("OCRM_F_CI_BELONG_ORG")
OCRM_F_CM_EXCHANGE_RATE = sqlContext.read.parquet(hdfs+'/OCRM_F_CM_EXCHANGE_RATE/*')
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
#目标表：
#MCRM_RET_PROD_SALES_LC_TMP 临时表全量
#MCRM_RET_PROD_SALES_LC 全量


#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.FR_ID                 AS FR_ID 
       ,A.PRDT_CODE             AS PROD_ID 
       ,B.PROD_NAME             AS PROD_NAME 
       ,B.CATL_CODE             AS CATL_CODE 
       ,A.CURR                  AS CURR 
       ,CAST(SUM(CASE MONTH(V_DT) 
           WHEN '1' THEN NVL(A.OLD_YEAR_BAL, 0.00) 
           WHEN '2' THEN NVL(A.MONTH_BAL_1, 0.00) 
           WHEN '3' THEN NVL(A.MONTH_BAL_2, 0.00) 
           WHEN '4' THEN NVL(A.MONTH_BAL_3, 0.00) 
           WHEN '5' THEN NVL(A.MONTH_BAL_4, 0.00) 
           WHEN '6' THEN NVL(A.MONTH_BAL_5, 0.00) 
           WHEN '7' THEN NVL(A.MONTH_BAL_6, 0.00) 
           WHEN '8' THEN NVL(A.MONTH_BAL_7, 0.00) 
           WHEN '9' THEN NVL(A.MONTH_BAL_8, 0.00) 
           WHEN '10' THEN NVL(A.MONTH_BAL_9, 0.00) 
           WHEN '11' THEN NVL(A.MONTH_BAL_10, 0.00) 
           WHEN '12' THEN NVL(A.MONTH_BAL_11, 0.00) END)   AS DECIMAL(24,6))                    AS LAST_MONTH_BAL 
       ,CAST(SUM(CASE MONTH(V_DT) 
           WHEN '1' THEN NVL(A.OLD_YEAR_BAL * NVL(C.EXCHANGE_RATE, 1), 0.00) 
           WHEN '2' THEN NVL(A.MONTH_BAL_1 * NVL(C.EXCHANGE_RATE, 1), 0.00) 
           WHEN '3' THEN NVL(A.MONTH_BAL_2 * NVL(C.EXCHANGE_RATE, 1), 0.00) 
           WHEN '4' THEN NVL(A.MONTH_BAL_3 * NVL(C.EXCHANGE_RATE, 1), 0.00) 
           WHEN '5' THEN NVL(A.MONTH_BAL_4 * NVL(C.EXCHANGE_RATE, 1), 0.00) 
           WHEN '6' THEN NVL(A.MONTH_BAL_5 * NVL(C.EXCHANGE_RATE, 1), 0.00) 
           WHEN '7' THEN NVL(A.MONTH_BAL_6 * NVL(C.EXCHANGE_RATE, 1), 0.00) 
           WHEN '8' THEN NVL(A.MONTH_BAL_7 * NVL(C.EXCHANGE_RATE, 1), 0.00) 
           WHEN '9' THEN NVL(A.MONTH_BAL_8 * NVL(C.EXCHANGE_RATE, 1), 0.00) 
           WHEN '10' THEN NVL(A.MONTH_BAL_9 * NVL(C.EXCHANGE_RATE, 1), 0.00) 
           WHEN '11' THEN NVL(A.MONTH_BAL_10 * NVL(C.EXCHANGE_RATE, 1), 0.00) 
           WHEN '12' THEN NVL(A.MONTH_BAL_11 * NVL(C.EXCHANGE_RATE, 1), 0.00) END)    AS DECIMAL(24,6))                   AS LAST_MONTH_CNY_BAL 
       ,CAST(SUM(NVL(A.AMT, 0.00))    AS DECIMAL(24,6))                   AS BAL 
       ,CAST(SUM(NVL(A.AMT * NVL(C.EXCHANGE_RATE, 1), 0.00) )   AS DECIMAL(24,6))                   AS CNY_BAL 
   FROM ACRM_F_RE_ACCSUMAVGINFO A                              --理财积数均值表
   LEFT JOIN OCRM_F_PD_PROD_INFO B                             --汇率表
     ON A.PRDT_CODE             = B.PRODUCT_ID 
    AND A.FR_ID                 = B.FR_ID 
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE C                         --
     ON A.CURR                  = C.CURRENCY_CD 
    AND C.OBJECT_CURRENCY_CD    = 'CNY' 
    AND C.ETL_DT                = V_DT8 
	GROUP BY A.CUST_ID                                            
       ,A.FR_ID                                      
       ,A.PRDT_CODE                                  
       ,B.PROD_NAME                                  
       ,B.CATL_CODE                                  
       ,A.CURR"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_DT8\b", "'"+V_DT+"'", sql)
MCRM_RET_PROD_SALES_LC_TMP = sqlContext.sql(sql)
MCRM_RET_PROD_SALES_LC_TMP.registerTempTable("MCRM_RET_PROD_SALES_LC_TMP")
dfn="MCRM_RET_PROD_SALES_LC_TMP/"+V_DT+".parquet"
MCRM_RET_PROD_SALES_LC_TMP.cache()
nrows = MCRM_RET_PROD_SALES_LC_TMP.count()
MCRM_RET_PROD_SALES_LC_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_RET_PROD_SALES_LC_TMP.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_PROD_SALES_LC_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_RET_PROD_SALES_LC_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1
MCRM_RET_PROD_SALES_LC_TMP = sqlContext.read.parquet(hdfs+'/MCRM_RET_PROD_SALES_LC_TMP/*')
MCRM_RET_PROD_SALES_LC_TMP.registerTempTable("MCRM_RET_PROD_SALES_LC_TMP")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,B.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,D.MGR_ID                AS CUST_MANAGER 
       ,D.MGR_NAME              AS CUST_MANAGER_NAME 
       ,C.INSTITUTION_CODE      AS ORG_ID 
       ,C.INSTITUTION_NAME      AS ORG_NAME 
       ,B.OBJ_RATING            AS CUST_LEVEL 
       ,A.PROD_ID               AS PROD_ID 
       ,A.PROD_NAME             AS PROD_NAME 
       ,A.CATL_CODE             AS CATL_CODE 
       ,A.CURR                  AS CURR 
       ,SUM(A.LAST_MONTH_BAL)                       AS LAST_MONTH_BAL 
       ,SUM(A.LAST_MONTH_CNY_BAL)                       AS LAST_MONTH_CNY_BAL 
       ,SUM(A.BAL)                       AS BAL 
       ,SUM(A.CNY_BAL)                       AS CNY_BAL 
       ,V_DT            AS ST_DATE 
       ,'1'                     AS M_MAIN_TYPE 
       ,'1'                     AS O_MAIN_TYPE 
       ,A.FR_ID                 AS FR_ID 
   FROM MCRM_RET_PROD_SALES_LC_TMP A                           --理财销量明细表-临时表
  INNER JOIN OCRM_F_CI_CUST_DESC B                             --统一客户信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  INNER JOIN OCRM_F_CI_BELONG_ORG C                            --机构归属
     ON A.CUST_ID               = C.CUST_ID 
    AND A.FR_ID                 = C.FR_ID 
    AND C.MAIN_TYPE             = '1' 
  INNER JOIN OCRM_F_CI_BELONG_CUSTMGR D                        --客户归属
     ON A.CUST_ID               = D.CUST_ID 
    AND A.FR_ID                 = D.FR_ID 
    AND D.MAIN_TYPE             = '1' 
  GROUP BY A.CUST_ID                
       ,B.CUST_ZH_NAME                                     
       ,D.MGR_ID                                           
       ,D.MGR_NAME                                         
       ,C.INSTITUTION_CODE                                 
       ,C.INSTITUTION_NAME                                 
       ,B.OBJ_RATING                                       
       ,A.PROD_ID                                          
       ,A.PROD_NAME                                        
       ,A.CATL_CODE                                        
       ,A.CURR                                                                                                                 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_PROD_SALES_LC = sqlContext.sql(sql)
MCRM_RET_PROD_SALES_LC.registerTempTable("MCRM_RET_PROD_SALES_LC")
dfn="MCRM_RET_PROD_SALES_LC/"+V_DT+".parquet"
MCRM_RET_PROD_SALES_LC.cache()
nrows = MCRM_RET_PROD_SALES_LC.count()
MCRM_RET_PROD_SALES_LC.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_RET_PROD_SALES_LC.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_PROD_SALES_LC/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_RET_PROD_SALES_LC lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-03::
V_STEP = V_STEP + 1
MCRM_RET_PROD_SALES_LC = sqlContext.read.parquet(hdfs+'/MCRM_RET_PROD_SALES_LC/*')
MCRM_RET_PROD_SALES_LC.registerTempTable("MCRM_RET_PROD_SALES_LC")
sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_MANAGER          AS CUST_MANAGER 
       ,A.CUST_MANAGER_NAME     AS CUST_MANAGER_NAME 
       ,A.ORG_ID                AS ORG_ID 
       ,A.ORG_NAME              AS ORG_NAME 
       ,A.CUST_LEVEL            AS CUST_LEVEL 
       ,A.PROD_ID               AS PROD_ID 
       ,A.PROD_NAME             AS PROD_NAME 
       ,A.CATL_CODE             AS CATL_CODE 
       ,A.CURR                  AS CURR 
       ,B.BAL                   AS LAST_MONTH_BAL 
       ,B.CNY_BAL               AS LAST_MONTH_CNY_BAL 
       ,A.BAL                   AS BAL 
       ,A.CNY_BAL               AS CNY_BAL 
       ,A.ST_DATE               AS ST_DATE 
       ,A.M_MAIN_TYPE           AS M_MAIN_TYPE 
       ,A.O_MAIN_TYPE           AS O_MAIN_TYPE 
       ,A.FR_ID                 AS FR_ID 
   FROM MCRM_RET_PROD_SALES_LC A                               --理财销量明细表
  INNER JOIN MCRM_RET_PROD_SALES_LC B                          --理财销量明细表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND A.PROD_ID               = B.PROD_ID 
    AND A.CURR                  = B.CURR 
    AND B.ST_DATE               = DATE_ADD(TRUNC(V_DT, 'MM'),-1)
  WHERE A.ST_DATE               = V_DT """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_PROD_SALES_LC_INNTMP1 = sqlContext.sql(sql)
MCRM_RET_PROD_SALES_LC_INNTMP1.registerTempTable("MCRM_RET_PROD_SALES_LC_INNTMP1")


sql = """
 SELECT DST.CUST_ID                                             --客户编号:src.CUST_ID
       ,DST.CUST_ZH_NAME                                       --客户名称:src.CUST_ZH_NAME
       ,DST.CUST_MANAGER                                       --客户经理编号:src.CUST_MANAGER
       ,DST.CUST_MANAGER_NAME                                  --客户经理名称:src.CUST_MANAGER_NAME
       ,DST.ORG_ID                                             --机构编号:src.ORG_ID
       ,DST.ORG_NAME                                           --机构名称:src.ORG_NAME
       ,DST.CUST_LEVEL                                         --客户等级:src.CUST_LEVEL
       ,DST.PROD_ID                                            --产品编号:src.PROD_ID
       ,DST.PROD_NAME                                          --产品名称:src.PROD_NAME
       ,DST.CATL_CODE                                          --大类产品标识:src.CATL_CODE
       ,DST.CURR                                               --币种:src.CURR
       ,DST.LAST_MONTH_BAL                                     --上月末余额:src.LAST_MONTH_BAL
       ,DST.LAST_MONTH_CNY_BAL                                 --上月末余额折人民币:src.LAST_MONTH_CNY_BAL
       ,DST.BAL                                                --余额:src.BAL
       ,DST.CNY_BAL                                            --余额折人民币:src.CNY_BAL
       ,DST.ST_DATE                                            --业务日期:src.ST_DATE
       ,DST.M_MAIN_TYPE                                        --客户经理主协办类型:src.M_MAIN_TYPE
       ,DST.O_MAIN_TYPE                                        --机构主协办类型:src.O_MAIN_TYPE
       ,DST.FR_ID                                              --:src.FR_ID
   FROM MCRM_RET_PROD_SALES_LC DST 
   LEFT JOIN MCRM_RET_PROD_SALES_LC_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.PROD_ID             = DST.PROD_ID 
    AND SRC.CURR                = DST.CURR 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_PROD_SALES_LC_INNTMP2 = sqlContext.sql(sql)
dfn="MCRM_RET_PROD_SALES_LC/"+V_DT+".parquet"
UNION=MCRM_RET_PROD_SALES_LC_INNTMP2.unionAll(MCRM_RET_PROD_SALES_LC_INNTMP1)
MCRM_RET_PROD_SALES_LC_INNTMP1.cache()
MCRM_RET_PROD_SALES_LC_INNTMP2.cache()
nrowsi = MCRM_RET_PROD_SALES_LC_INNTMP1.count()
nrowsa = MCRM_RET_PROD_SALES_LC_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
MCRM_RET_PROD_SALES_LC_INNTMP1.unpersist()
MCRM_RET_PROD_SALES_LC_INNTMP2.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_PROD_SALES_LC/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_RET_PROD_SALES_LC lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
