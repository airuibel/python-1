#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MID_CI_CUSTOMER_RELATIVE').setMaster(sys.argv[2])
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

F_CI_XDXT_CUSTOMER_RELATIVE = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_RELATIVE/*')
F_CI_XDXT_CUSTOMER_RELATIVE.registerTempTable("F_CI_XDXT_CUSTOMER_RELATIVE")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
SELECT       A.CUSTOMERID             AS              CUSTOMERID     
           , A.RELATIVEID             AS              RELATIVEID     
           , A.RELATIONSHIP           AS              RELATIONSHIP   
           , A.CUSTOMERNAME           AS              CUSTOMERNAME   
           , A.CERTTYPE               AS              CERTTYPE       
           , A.CERTID                 AS              CERTID         
           , A.FICTITIOUSPERSON       AS              FICTITIOUSPERSO
           , A.CURRENCYTYPE           AS              CURRENCYTYPE   
           , A.INVESTMENTSUM          AS              INVESTMENTSUM  
           , A.OUGHTSUM               AS              OUGHTSUM       
           , A.INVESTMENTPROP         AS              INVESTMENTPROP 
           , A.INVESTDATE             AS              INVESTDATE     
           , A.STOCKCERTNO            AS              STOCKCERTNO    
           , A.DUTY                   AS              DUTY           
           , A.TELEPHONE              AS              TELEPHONE      
           , A.EFFECT                 AS              EFFECT         
           , A.WHETHEN1               AS              WHETHEN1       
           , A.WHETHEN2               AS              WHETHEN2       
           , A.WHETHEN3               AS              WHETHEN3       
           , A.WHETHEN4               AS              WHETHEN4       
           , A.WHETHEN5               AS              WHETHEN5       
           , A.DESCRIBE               AS              DESCRIBE       
           , A.INPUTORGID             AS              INPUTORGID     
           , A.INPUTUSERID            AS              INPUTUSERID    
           , A.INPUTDATE              AS              INPUTDATE 
           , CAST(' ' AS VARCHAR(10))  AS              UPDATEDATE 
           , A.REMARK                 AS              REMARK         
           , A.SEX                    AS              SEX            
           , A.BIRTHDAY               AS              BIRTHDAY       
           , A.SINO                   AS              SINO           
           , A.FAMILYADD              AS              FAMILYADD      
           , A.FAMILYZIP              AS              FAMILYZIP      
           , A.EDUEXPERIENCE          AS              EDUEXPERIENCE  
           , A.INVESTYIELD            AS              INVESTYIELD    
           , A.HOLDDATE               AS              HOLDDATE       
           , A.ENGAGETERM             AS              ENGAGETERM     
           , A.HOLDSTOCK              AS              HOLDSTOCK      
           , A.LOANCARDNO             AS              LOANCARDNO     
           , A.EFFSTATUS              AS              EFFSTATUS      
           , A.CUSTOMERTYPE           AS              CUSTOMERTYPE   
           , A.INVESINITIALSUM        AS              INVESINITIALSUM
           , A.ACCOUNTSUM             AS              ACCOUNTSUM     
           , A.FAIRSUM                AS              FAIRSUM        
           , A.DIATHESIS              AS              DIATHESIS      
           , A.ABILITY                AS              ABILITY        
           , A.INNOVATION             AS              INNOVATION     
           , A.CHARACTER              AS              CHARACTER      
           , A.COMPETITION            AS              COMPETITION    
           , A.STRATEGY               AS              STRATEGY       
           , A.RISE                   AS              RISE           
           , A.POSSESS                AS              POSSESS        
           , A.EYESHOT                AS              EYESHOT        
           , A.FORESIGHT              AS              FORESIGHT      
           , A.STATUS                 AS              STATUS         
           , A.INDUSTRY               AS              INDUSTRY       
           , A.PROSECUTION            AS              PROSECUTION    
           , A.FIRSTINVESTSUM         AS              FIRSTINVESTSUM 
           , A.FIRSTINVESTDATE        AS              FIRSTINVESTDATE
           , A.LASTINVESTSUM          AS              LASTINVESTSUM  
           , A.LASTINVESTDATE         AS              LASTINVESTDATE 
           , A.DEADLINE               AS              DEADLINE       
           , A.FR_ID                  AS              FR_ID          
           , A.ODS_ST_DATE            AS              ODS_ST_DATE    
           , A.ODS_SYS_ID             AS              ODS_SYS_ID     
    FROM F_CI_XDXT_CUSTOMER_RELATIVE   A 
   INNER JOIN ( SELECT  CUSTOMERID ,FR_ID ,MAX(RELATIVEID) AS RELATIVEID FROM  F_CI_XDXT_CUSTOMER_RELATIVE 
                                                                           WHERE     RELATIONSHIP='5100'  
                                                                           GROUP BY    CUSTOMERID ,FR_ID            )  B
    ON  A.CUSTOMERID=B.CUSTOMERID   AND A.FR_ID=B.FR_ID AND  A.RELATIVEID=B.RELATIVEID AND A.RELATIONSHIP='5100' 
     
 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_CI_CUSTOMER_RELATIVE = sqlContext.sql(sql)
MID_CI_CUSTOMER_RELATIVE.registerTempTable("MID_CI_CUSTOMER_RELATIVE")
dfn="MID_CI_CUSTOMER_RELATIVE/"+V_DT+".parquet"
MID_CI_CUSTOMER_RELATIVE.cache()
nrows = MID_CI_CUSTOMER_RELATIVE.count()
MID_CI_CUSTOMER_RELATIVE.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_CI_CUSTOMER_RELATIVE.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_CI_CUSTOMER_RELATIVE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

