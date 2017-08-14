#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_M_R_RET_CUST_ASSETS').setMaster(sys.argv[2])
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
OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")

OCRM_F_CUST_ORG_MGR = sqlContext.read.parquet(hdfs+'/OCRM_F_CUST_ORG_MGR/*')
OCRM_F_CUST_ORG_MGR.registerTempTable("OCRM_F_CUST_ORG_MGR")

TMP_PER_ASSETS_SUM = sqlContext.read.parquet(hdfs+'/TMP_PER_ASSETS_SUM/*')
TMP_PER_ASSETS_SUM.registerTempTable("TMP_PER_ASSETS_SUM")

#目标表
#MCRM_RET_CUST_ASSETS 全量表

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql="""               
     SELECT 
                 A.CUST_ID       as CUST_ID                                
                ,A.CUST_NAME     as CUST_ZH_NAME                          
                ,C.MGR_ID        as CUST_MANAGER        
                ,C.MGR_NAME      as CUST_MANAGER_NAME     
                ,C.ORG_ID        as ORG_ID     
                ,C.ORG_NAME      as ORG_NAME  
                ,A.CUST_SEX    	 as SEX      
                ,cast(case when A.CUST_BIR = '' or A.CUST_BIR is null then 0
					 when SUBSTR (A.CUST_BIR, 3, 1) = '-' then (SUBSTR(V_DT,1,4) - SUBSTR(A.CUST_BIR, 7, 4)) 
					 when SUBSTR (A.CUST_BIR, 5, 1) = '-' then (SUBSTR(V_DT,1,4) - SUBSTR(A.CUST_BIR, 1, 4))
					 else 0 end as INTEGER) AS AGE
                ,A.CUST_EDU_LVL_COD  as EDUCATION                    
                ,C.OBJ_RATING      as CUST_LEVEL                       
                ,cast(COALESCE(D.MONTH_BAL,0) as DECIMAL(24,6))    as MONTH_BAL                     
                ,cast(COALESCE(D.MONTH_AVG_BAL,0) as DECIMAL(24,6))   as MONTH_AVG_BAL                 
                ,cast(COALESCE(D.THREE_MONTH_AVG_BAL,0) as DECIMAL(24,6))    as THREE_MONTH_AVG_BAL         
                ,cast(COALESCE(D.LAST_MONTH_BAL,0) as DECIMAL(24,6))     as LAST_MONTH_BAL             
                ,cast(COALESCE(D.LAST_MONTH_AVG_BAL,0) as DECIMAL(24,6))  as LAST_MONTH_AVG_BAL
                ,cast(COALESCE(D.LTHREE_MONTH_AVG_BAL,0) as DECIMAL(24,6)) as LTHREE_MONTH_AVG_BAL
                ,cast(COALESCE(D.YEAR_BAL,0) as DECIMAL(24,6))      as YEAR_BAL      
                ,cast(COALESCE(D.YEAR_AVG_BAL,0) as DECIMAL(24,6))   as YEAR_AVG_BAL     
                ,cast(COALESCE(D.YEAR_THREE_AVG_BAL,0) as DECIMAL(24,6)) as YEAR_THREE_AVG_BAL 
                ,V_DT            as ST_DATE 
				,C.M_MAIN_TYPE     as MAIN_TYPE 
				,C.O_MAIN_TYPE  as O_MAIN_TYPE 				
                ,C.OBJ_DATE     	as GRADE_DATE							    
                ,C.OLD_OBJ_RATING      as   OLD_CUST_LEVEL
				,A.FR_ID  as FR_ID
         FROM  OCRM_F_CI_PER_CUST_INFO   A 
			   INNER JOIN OCRM_F_CUST_ORG_MGR C ON A.CUST_ID = C.CUST_ID AND CUST_TYP='1' AND O_MAIN_TYPE ='1' AND C.FR_ID =A.FR_ID
			   LEFT JOIN  TMP_PER_ASSETS_SUM D ON A.CUST_ID = D.CUST_ID AND  D.FR_ID =A.FR_ID
	  -- WHERE A.FR_ID=V_FR_ID
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MCRM_RET_CUST_ASSETS = sqlContext.sql(sql)
dfn="MCRM_RET_CUST_ASSETS/"+V_DT+".parquet"
MCRM_RET_CUST_ASSETS.write.save(path=hdfs + '/' + dfn, mode='overwrite')
#全量表，保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MCRM_RET_CUST_ASSETS/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

