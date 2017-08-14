#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_RET_PROD_SALES_DK').setMaster(sys.argv[2])
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

ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
OCRM_F_CUST_ORG_MGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CUST_ORG_MGR/*')
OCRM_F_CUST_ORG_MGR.registerTempTable("OCRM_F_CUST_ORG_MGR")
OCRM_F_CM_EXCHANGE_RATE = sqlContext.read.parquet(hdfs+'/OCRM_F_CM_EXCHANGE_RATE/*')
OCRM_F_CM_EXCHANGE_RATE.registerTempTable("OCRM_F_CM_EXCHANGE_RATE")
MCRM_RET_PROD_SALES_DK = sqlContext.read.parquet(hdfs+'/MCRM_RET_PROD_SALES_DK/*')
MCRM_RET_PROD_SALES_DK.registerTempTable("MCRM_RET_PROD_SALES_DK")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,B.CUST_NAME             AS CUST_ZH_NAME 
       ,B.MGR_ID                AS CUST_MANAGER 
       ,B.MGR_NAME              AS CUST_MANAGER_NAME 
       ,B.ORG_ID                AS ORG_ID 
       ,B.ORG_NAME              AS ORG_NAME 
       ,B.OBJ_RATING            AS CUST_LEVEL 
       ,A.PRODUCT_ID            AS PROD_ID 
       ,C.PROD_NAME             AS PROD_NAME 
       ,CAST(C.CATL_CODE AS BIGINT) AS CATL_CODE 
       ,A.CURR                  AS CURR 
       ,SUM(E.BAL)                       AS LAST_MONTH_BAL 
       ,SUM(E.CNY_BAL)                       AS LAST_MONTH_CNY_BAL 
       ,SUM(A.BAL)                       AS BAL 
       ,CAST(SUM(A.BAL * COALESCE(D.EXCHANGE_RATE, 1)) AS DECIMAL(24,6)) AS CNY_BAL 
       ,V_DT                    AS ST_DATE 
       ,B.M_MAIN_TYPE           AS M_MAIN_TYPE 
       ,B.O_MAIN_TYPE           AS O_MAIN_TYPE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议
  INNER JOIN OCRM_F_CUST_ORG_MGR B                             --客户归属信息表
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.CUST_TYP              = '1' 
   LEFT JOIN OCRM_F_PD_PROD_INFO C                             --产品表
     ON A.PRODUCT_ID            = C.PRODUCT_ID 
    AND A.FR_ID                 = C.FR_ID 
   LEFT JOIN OCRM_F_CM_EXCHANGE_RATE D                         --汇率表
     ON D.OBJECT_CURRENCY_CD    = 'CNY' 
    AND D.ODS_ST_DATE           = V_DT 
    AND A.CURR                  = D.CURRENCY_CD 
   LEFT JOIN MCRM_RET_PROD_SALES_DK E                          --客户贷款明细表
     ON E.ST_DATE               = add_months(V_DT,-1)
    AND A.CUST_ID               = E.CUST_ID 
    AND A.FR_ID                 = E.FR_ID 
    AND A.PRODUCT_ID            = E.PROD_ID 
    AND A.CURR                  = E.CURR 
    AND B.ORG_ID                = E.ORG_ID 
  WHERE A.SUBJECTNO LIKE '1301%' 
     OR A.SUBJECTNO LIKE '1302%' 
     OR A.SUBJECTNO LIKE '1303%' 
     OR A.SUBJECTNO LIKE '1304%' 
     OR A.SUBJECTNO LIKE '1305%' 
     OR A.SUBJECTNO LIKE '1306%' 
     OR A.SUBJECTNO LIKE '1307%' 
     OR A.SUBJECTNO LIKE '1308%' 
  GROUP BY A.PRODUCT_ID 
       ,A.CUST_ID
       ,B.CUST_NAME  
       ,B.MGR_ID     
       ,B.MGR_NAME   
       ,B.ORG_ID     
       ,B.ORG_NAME   
       ,B.OBJ_RATING 
       ,C.PROD_NAME  
       ,C.CATL_CODE  
       ,A.CURR
       ,B.M_MAIN_TYPE
       ,B.O_MAIN_TYPE
       ,A.FR_ID
       """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
sql = re.sub(r"\bV_LAST_MONTH\b", "'"+V_DT10+"'", sql)

MCRM_RET_PROD_SALES_DK = sqlContext.sql(sql)
MCRM_RET_PROD_SALES_DK.registerTempTable("MCRM_RET_PROD_SALES_DK")
dfn="MCRM_RET_PROD_SALES_DK/"+V_DT+".parquet"
MCRM_RET_PROD_SALES_DK.cache()
nrows = MCRM_RET_PROD_SALES_DK.count()
MCRM_RET_PROD_SALES_DK.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MCRM_RET_PROD_SALES_DK.unpersist()
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_PROD_SALES_DK/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MCRM_RET_PROD_SALES_DK lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
