#coding=UTF-8                                                                                                                                                                       
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext                                                                                                           
from pyspark.sql.types import *                                                                                                                                                     
from datetime import date, datetime, timedelta                                                                                                                                      
import sys, re, os                                                                                                                                                                  
                                                                                                                                                                                    
st = datetime.now()                                                                                                                                                                 
conf = SparkConf().setAppName('PROC_F_CI_SUN_POTEN_UPDATE').setMaster(sys.argv[2])                                                                                                  
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
#上三十日期                                                                                                                                                                         
V_DT_30LD = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8])) + timedelta(-30)).strftime("%Y%m%d")                                                                  
#月初日期                                                                                                                                                                           
V_DT_FMD = date(int(etl_date[0:4]), int(etl_date[4:6]), 1).strftime("%Y%m%d")                                                                                                       
#上月末日期                                                                                                                                                                         
V_DT_LMD = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1) + timedelta(-1)).strftime("%Y%m%d")                                                                                     
#10位日期                                                                                                                                                                           
V_DT10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1)).strftime("%Y-%m-%d")                                                                                                     
                                                                                                                                                                                    
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_POTENTIAL_CUSTOMER/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_POTENTIAL_CUSTOMER_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_POTENTIAL_CUSTOMER/") 
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_BASEINFO/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_BASEINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SUN_BASEINFO/")                                                                                                                                                                                    
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_BASE/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_BASE_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SUN_BASE/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_FARMERTYPE_DETAIL/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_FARMERTYPE_DETAIL_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SUN_FARMERTYPE_DETAIL/") 
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_EVALUATION/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_EVALUATION_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SUN_EVALUATION/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_HOUSEINFO/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_HOUSEINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SUN_HOUSEINFO/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_CARINFO/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_CARINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SUN_CARINFO/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_OTHERINFO/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_OTHERINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SUN_OTHERINFO/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_OTHFLOWINFO/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_OTHFLOWINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SUN_OTHFLOWINFO/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_DEBTINFO/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_DEBTINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SUN_DEBTINFO/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_CREDITINFO/*")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_CREDITINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SUN_CREDITINFO/")
                                                                                                                                                                                                                                 
V_STEP = 0                                                                                                                                                                          
                                                                                                                                                                                    
OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')                                                                                                
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")                                                                                                                
OCRM_F_CI_POTENTIAL_CUSTOMER = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_POTENTIAL_CUSTOMER/*')                                                                                      
OCRM_F_CI_POTENTIAL_CUSTOMER.registerTempTable("OCRM_F_CI_POTENTIAL_CUSTOMER") 
OCRM_F_CI_SUN_BASEINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_BASEINFO/*')                                                                                      
OCRM_F_CI_SUN_BASEINFO.registerTempTable("OCRM_F_CI_SUN_BASEINFO") 
OCRM_F_CI_SUN_BASE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_BASE/*')                                                                                      
OCRM_F_CI_SUN_BASE.registerTempTable("OCRM_F_CI_SUN_BASE") 
#OCRM_F_CI_SUN_PHOTO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_PHOTO/*')                                                                                      
#OCRM_F_CI_SUN_PHOTO.registerTempTable("OCRM_F_CI_SUN_PHOTO") 
OCRM_F_CI_SUN_FARMERTYPE_DETAIL = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_FARMERTYPE_DETAIL/*')                                                                                     
OCRM_F_CI_SUN_FARMERTYPE_DETAIL.registerTempTable("OCRM_F_CI_SUN_FARMERTYPE_DETAIL")  
OCRM_F_CI_SUN_EVALUATION = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_EVALUATION/*')                                                                                     
OCRM_F_CI_SUN_EVALUATION.registerTempTable("OCRM_F_CI_SUN_EVALUATION")  
OCRM_F_CI_SUN_HOUSEINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_HOUSEINFO/*')                                                                                     
OCRM_F_CI_SUN_HOUSEINFO.registerTempTable("OCRM_F_CI_SUN_HOUSEINFO") 
OCRM_F_CI_SUN_CARINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_CARINFO/*')                                                                                     
OCRM_F_CI_SUN_CARINFO.registerTempTable("OCRM_F_CI_SUN_CARINFO")  
OCRM_F_CI_SUN_OTHERINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_OTHERINFO/*')                                                                                     
OCRM_F_CI_SUN_OTHERINFO.registerTempTable("OCRM_F_CI_SUN_OTHERINFO")  
OCRM_F_CI_SUN_OTHFLOWINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_OTHFLOWINFO/*')                                                                                     
OCRM_F_CI_SUN_OTHFLOWINFO.registerTempTable("OCRM_F_CI_SUN_OTHFLOWINFO") 
OCRM_F_CI_SUN_DEBTINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_DEBTINFO/*')                                                                                     
OCRM_F_CI_SUN_DEBTINFO.registerTempTable("OCRM_F_CI_SUN_DEBTINFO") 
OCRM_F_CI_SUN_CREDITINFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SUN_CREDITINFO/*')                                                                                     
OCRM_F_CI_SUN_CREDITINFO.registerTempTable("OCRM_F_CI_SUN_CREDITINFO") 
                                                                                                 
                                                                                                                                                                                    
#任务[21] 001-01::                                                                                                                                                                  
V_STEP = V_STEP + 1                                                                                                                                                                 
ret = os.system("hdfs dfs -rm -r /"+dbname+"/A_R_CUST_ID_TEMP/*")                                                                                                                                                                                  
sql = """                                                                                                                                                                           
	SELECT 	CAST(NVL(B.CUST_ID,'  ') AS VARCHAR(40))		AS  	SOURCE_CUST_ID	,                                                                                                                 
			CAST(A.CUST_ID 	AS VARCHAR(40))		AS		CUST_ID                                                                                                                               
      FROM OCRM_F_CI_PER_CUST_INFO A,                                                                                                                                               
	  OCRM_F_CI_POTENTIAL_CUSTOMER B                                                                                                                                                  
		WHERE A.FR_ID=B.FR_ID
		--A.FR_ID = V_FR_ID 
		--AND B.FR_ID = V_FR_ID 
		AND 
		A.CUST_NAME = B.CUST_NAME AND A.CUST_CER_NO = B.CERT_NO                                                                                                                     
		AND A.CUST_CRE_TYP = B.CERT_TYP                                                                                                                                                 
		AND B.POSITIVE_FLAG = 'N' --潜在客户                                                                                                                                            
 """                                                                                                                                                                                
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                      
A_R_CUST_ID_TEMP = sqlContext.sql(sql)                                                                                                                                              
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                              
dfn="A_R_CUST_ID_TEMP/"+V_DT+".parquet"                                                                                                                                             
A_R_CUST_ID_TEMP.cache()                                                                                                                                                            
nrows = A_R_CUST_ID_TEMP.count()                                                                                                                                                    
A_R_CUST_ID_TEMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                                
A_R_CUST_ID_TEMP.unpersist()                                                                                                                                                        
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/A_R_CUST_ID_TEMP/"+V_DT_LD+".parquet")
#ret = os.system("hdfs dfs -cp /"+dbname+"/A_R_CUST_ID_TEMP/"+V_DT+".parquet /"+dbname+"/A_R_CUST_ID_TEMP_BK/")                                                                                                  
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert A_R_CUST_ID_TEMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)            
                                                                                                                                                                                    
#任务[21] 001-02::                                                                                                                                                                 
V_STEP = V_STEP + 1                                                                                                                                                                 
A_R_CUST_ID_TEMP = sqlContext.read.parquet(hdfs+'/A_R_CUST_ID_TEMP/*')                                                                                     
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                                                                    
sql = """                                                                                                                                                                           
			SELECT                                                                                                                                                                        
				M.ID                         AS    ID                                                                                                                                       
				,CAST(NVL(N.CUST_ID,M.CUST_ID)   AS VARCHAR(32))     AS    CUST_ID                                                                                                                                          
				,M.CUST_NAME                  AS    CUST_NAME                                                                                                                                         
				,M.CUST_EN_NAME               AS    CUST_EN_NAME                                                                                                                                      
				,M.OTHER_NAME                 AS    OTHER_NAME                                                                                                                                       
				,M.CUST_TYPE                  AS    CUST_TYPE                                                                                                                                       
				,M.CERT_TYP                   AS    CERT_TYP                                                                                                                                          
				,M.CERT_NO                    AS    CERT_NO                                                                                                                                           
				,M.CUST_CLASS                 AS    CUST_CLASS                                                                                                                                      
				,M.LINK_MAN                   AS    LINK_MAN                                                                                                                                           
				,M.LINK_PHONE                 AS    LINK_PHONE                                                                                                                                         
				,M.ZIP_CODE                   AS    ZIP_CODE                                                                                                                                          
				,M.ADDRESS                    AS    ADDRESS                                                                                                                                            
				,CAST('1' AS    VARCHAR(1))           AS    POSITIVE_FLAG                                                                                                                                     
				,M.BEGIN_DATE                 AS    BEGIN_DATE                                                                                                                                         
				,M.POSITIVE_DATE              AS    POSITIVE_DATE                                                                                                                                      
				,V_DT                    AS    CRM_DT                                                                                                                                             
				,M.CREATOR                    AS    CREATOR                                                                                                                                            
				,M.CREATE_ORG                 AS    CREATE_ORG                                                                                                                                        
				,M.FR_ID                      AS    FR_ID                                                                                                                                              
				,M.CUST_SOURCE                AS    CUST_SOURCE                                                                                                                                        
				,M.ASSIGN_ORG_ID              AS    ASSIGN_ORG_ID                                                                                                                                      
				,M.CUST_SEX                   AS    CUST_SEX                                                                                                                                           
				,M.CUST_BIR                   AS    CUST_BIR                                                                                                                                           
				,M.CUST_MRG                   AS    CUST_MRG                                                                                                                                          
				,M.CUST_NATION                AS    CUST_NATION                                                                                                                                        
				,M.CUST_REGISTER              AS    CUST_REGISTER                                                                                                                                      
				,M.CUST_REGADDR               AS    CUST_REGADDR                                                                                                                                      
				,M.CUST_FAMSTATUS             AS    CUST_FAMSTATUS                                                                                                                                     
				,M.CUST_HEALTH                AS    CUST_HEALTH                                                                                                                                        
				,M.CUST_WEIGHT                AS    CUST_WEIGHT                                                                                                                                        
				,M.CUST_HEIGHT                AS    CUST_HEIGHT                                                                                                                                        
				,M.HIGHEST_DEGREE             AS    HIGHEST_DEGREE                                                                                                                                    
				,M.CUST_OCCUP_COD             AS    CUST_OCCUP_COD                                                                                                                                     
				,M.CUST_EDU_LVL_COD           AS    CUST_EDU_LVL_COD                                                                                                                                   
				,M.CUST_WORK_UNIT_NAME        AS    CUST_WORK_UNIT_NAME                                                                                                                               
				,M.CUST_UTELL                 AS    CUST_UTELL                                                                                                                                         
				,M.CUST_POSN                  AS    CUST_POSN                                                                                                                                          
				,M.CUST_TITL                  AS    CUST_TITL                                                                                                                                          
				,M.IS_MEDICARE                AS    IS_MEDICARE                                                                                                                                        
				,M.MAINPROORINCOME            AS    MAINPROORINCOME                                                                                                                                   
				,M.CUST_POSTCOD               AS    CUST_POSTCOD                                                                                                                                       
				,M.CUST_TEL_NO                AS    CUST_TEL_NO                                                                                                                                        
				,M.CUST_MBTELNO               AS    CUST_MBTELNO                                                                                                                                       
				,M.CUST_FAMADDR               AS    CUST_FAMADDR                                                                                                                                       
				,M.CUST_FAM_NUM               AS    CUST_FAM_NUM                                                                                                                                       
				,M.INDUSTRYTYPE               AS    INDUSTRYTYPE                                                                                                                                       
				,M.CUST_WORKADDR              AS    CUST_WORKADDR                                                                                                                                      
				,M.CUST_COMMADD               AS    CUST_COMMADD                                                                                                                                       
				,M.CI_ADDR                    AS    CI_ADDR                                                                                                                                            
				,M.CUST_PECON_RESUR           AS    CUST_PECON_RESUR                                                                                                                                   
				,M.FAM_INCOMEACC              AS    FAM_INCOMEACC                                                                                                                                      
				,M.CUST_TOT_ASS               AS    CUST_TOT_ASS                                                                                                                                      
				,M.CRE_RECORD                 AS    CRE_RECORD                                                                                                                                         
				,M.CUST_TOT_DEBT              AS    CUST_TOT_DEBT                                                                                                                                      
				,M.COM_START_DATE             AS    COM_START_DATE                                                                                                                                     
				,M.REG_CCY                    AS    REG_CCY                                                                                                                                            
				,M.REG_CAPITAL                AS    REG_CAPITAL                                                                                                                                       
				,M.INCOME_CCY                 AS    INCOME_CCY                                                                                                                                         
				,M.INCOME_SETTLE              AS    INCOME_SETTLE                                                                                                                                     
				,M.INDUS_CLASS_MAIN           AS    INDUS_CLASS_MAIN                                                                                                                                  
				,M.INDUS_CLASS_DEPUTY         AS    INDUS_CLASS_DEPUTY                                                                                                                                 
				,M.ORG_TYP                    AS    ORG_TYP                                                                                                                                           
				,M.COM_TYP                    AS    COM_TYP                                                                                                                                            
				,M.LINCENSE_NO                AS    LINCENSE_NO                                                                                                                                        
				,M.IF_AGRICULTRUE             AS    IF_AGRICULTRUE                                                                                                                                     
				,M.LISTING_CORP_TYP           AS    LISTING_CORP_TYP                                                                                                                                  
				,M.IF_HIGH_TECH               AS    IF_HIGH_TECH                                                                                                                                       
				,M.IF_ESTATE                  AS    IF_ESTATE                                                                                                                                         
				,M.COM_SCALE                  AS    COM_SCALE                                                                                                                                          
				,M.IF_SHARE_CUST              AS    IF_SHARE_CUST                                                                                                                                      
				,M.ENT_QUA_LEVEL              AS    ENT_QUA_LEVEL                                                                                                                                      
				,M.IF_FANACING                AS    IF_FANACING                                                                                                                                        
				,M.LEGAL_NAME                 AS    LEGAL_NAME                                                                                                                                         
				,M.FR_CERT_TYP                AS    FR_CERT_TYP                                                                                                                                        
				,M.FR_SEX                     AS    FR_SEX                                                                                                                                             
				,M.LEGAL_CERT_NO              AS    LEGAL_CERT_NO                                                                                                                                      
				,M.LINK_MOBILE                AS    LINK_MOBILE                                                                                                                                        
				,M.LINK_TEL_FIN               AS    LINK_TEL_FIN                                                                                                                                       
				,M.WORK_ADDRESS               AS    WORK_ADDRESS                                                                                                                                      
				,M.BUSINESS                   AS    BUSINESS                                                                                                                                           
				,M.SALE_ASSET                 AS    SALE_ASSET                                                                                                                                         
				,M.EMPLOYEE_NUM               AS    EMPLOYEE_NUM                                                                                                                                       
				,M.LONGITUDE                  AS    LONGITUDE                                                                                                                                         
				,M.LATITUDE                   AS    LATITUDE                                                                                                                                           
				,M.CUST_LIVE_STATUS           AS    CUST_LIVE_STATUS                                                                                                                                 
				,M.COM_LOGO                   AS    COM_LOGO                                                                                                                                           
				,M.ANNUAL_FAMILY_INCOME       AS    ANNUAL_FAMILY_INCOME                                                                                                                               
				,M.NOTE                       AS    NOTE                                                                                                                                               
				,M.CREATE_DATE                AS    CREATE_DATE                                                                                                                                        
				,M.UPDATE_DATE                AS    UPDATE_DATE                                                                                                                                        
				,M.UPDATE_ORG                 AS    UPDATE_ORG                                                                                                                                        
				,M.UPDATE_USER                AS    UPDATE_USER                                                                                                                                        
				,M.GIHHEST_DEGRE              AS    GIHHEST_DEGRE                                                                                                                                      
				,M.DEGREE                     AS    DEGREE                                                                                                                                             
				,M.CUST_YEL_NO                AS    CUST_YEL_NO                                                                                                                                        
				,M.INDUS_CALSS_MAIN           AS    INDUS_CALSS_MAIN                                                                                                                                   
				,M.INDUS_CLAS_DEPUTY          AS    INDUS_CLAS_DEPUTY                                                                                                                                  
				,M.SEX                        AS    SEX                                                                                                                                                
				,M.TOTAL_ASSET                AS    TOTAL_ASSET                                                                                                                                        
				,M.LINK_MAN_CERT_TYPE         AS    LINK_MAN_CERT_TYPE                                                                                                                                 
				,M.LINK_MAN_CERT_NO           AS    LINK_MAN_CERT_NO                                                                                                                                   
				,M.BACK                       AS    BACK                                                                                                                                               
     FROM   OCRM_F_CI_POTENTIAL_CUSTOMER  M
     LEFT JOIN      A_R_CUST_ID_TEMP   N  
     ON  
      -- M.FR_ID = V_FR_ID AND 
      		M.CUST_ID = N.SOURCE_CUST_ID 
     			AND M.POSITIVE_FLAG = '0'                                                                                                                                            
                                                                                                                                                                                    
"""                                                                                                                                                                                 
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                        
OCRM_F_CI_POTENTIAL_CUSTOMER = sqlContext.sql(sql) 
OCRM_F_CI_POTENTIAL_CUSTOMER.registerTempTable("OCRM_F_CI_POTENTIAL_CUSTOMER")                                                                                                                                  
dfn="OCRM_F_CI_POTENTIAL_CUSTOMER/"+V_DT+".parquet"                                                                                                                                 
OCRM_F_CI_POTENTIAL_CUSTOMER.cache()                                                                                                                                                
nrows = OCRM_F_CI_POTENTIAL_CUSTOMER.count()                                                                                                                                        
OCRM_F_CI_POTENTIAL_CUSTOMER.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
OCRM_F_CI_POTENTIAL_CUSTOMER.unpersist()                                                                                                                                            
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_POTENTIAL_CUSTOMER/"+V_DT_LD+".parquet") 
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_POTENTIAL_CUSTOMER/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_POTENTIAL_CUSTOMER_BK/")                                                                                   
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_POTENTIAL_CUSTOMER lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
                                                                                                                                                                                    
#任务[11] 001-03::                                                                                                                                                                  
V_STEP = V_STEP + 1                                                                                                                                                                 
A_R_CUST_ID_TEMP = sqlContext.read.parquet(hdfs+'/A_R_CUST_ID_TEMP/*')                                                                                     
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                                                                    
sql = """   
			SELECT 
				M.ID             AS  ID          
				,CAST(N.CUST_ID  AS VARCHAR(30))      AS    CUST_ID      
				,M.CUST_NAME      AS    CUST_NAME    
				,M.CERT_NO        AS    CERT_NO      
				,M.MANAGEORGID    AS    MANAGEORGID  
				,M.MANAGEUSERID   AS    MANAGEUSERID 
				,M.COUNTY         AS    COUNTY       
				,M.TOWN           AS    TOWN         
				,M.VILLAGE        AS    VILLAGE      
				,M.GROUPID        AS    GROUPID      
				,M.DOORID         AS    DOORID       
				,M.ISHOUSEHOST    AS    ISHOUSEHOST  
				,M.FAMILY_LDTY    AS    FAMILY_LDTY  
				,M.TEL            AS    TEL          
				,M.FR_ID          AS    FR_ID        
				,M.CRM_DT         AS    CRM_DT   
			FROM     OCRM_F_CI_SUN_BASEINFO M
			LEFT JOIN  A_R_CUST_ID_TEMP   N
			ON 
			--M.FR_ID = V_FR_ID AND 
			M.CUST_ID = N.SOURCE_CUST_ID                                                                                                                                                                                                                                                                                                                              
"""                                                                                                                                                                                
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                    
OCRM_F_CI_SUN_BASEINFO = sqlContext.sql(sql) 
OCRM_F_CI_SUN_BASEINFO.registerTempTable("OCRM_F_CI_SUN_BASEINFO")                                                                                                                                       
dfn="OCRM_F_CI_SUN_BASEINFO/"+V_DT+".parquet"                                                                                                                                      
OCRM_F_CI_SUN_BASEINFO.cache()                                                                                                                                                     
nrows = OCRM_F_CI_SUN_BASEINFO.count()                                                                                                                                             
OCRM_F_CI_SUN_BASEINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
OCRM_F_CI_SUN_BASEINFO.unpersist()                                                                                                                                            
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_BASEINFO/"+V_DT_LD+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_BASEINFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SUN_BASEINFO_BK/")                                                                                                                                                 
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_BASEINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)     
    
#任务[11] 001-05::                                                                                                                                                                  
V_STEP = V_STEP + 1                                                                                                                                                                 
A_R_CUST_ID_TEMP = sqlContext.read.parquet(hdfs+'/A_R_CUST_ID_TEMP/*')                                                                                     
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                                                                    
sql = """   
			SELECT 
				M.ID                AS    ID                        
        ,        CAST(N.CUST_ID AS VARCHAR(30))          AS    CUST_ID                   
				,M.SEX               AS    SEX                       
				,M.HEALTH            AS    HEALTH                    
				,M.MARRIAGE          AS    MARRIAGE                  
				,M.EDU               AS    EDU                       
				,M.CENSUS_ADD        AS    CENSUS_ADD                
				,M.FACT_ADD          AS    FACT_ADD                  
				,M.MANAGE_PRJ        AS    MANAGE_PRJ                
				,M.JOB_DETAIL        AS    JOB_DETAIL                
				,M.YEAR_COME         AS    YEAR_COME                 
				,M.FARMER_TYPE       AS    FARMER_TYPE               
				,M.FAM_NUM           AS    FAM_NUM                   
				,M.FAM_TOTASSET      AS    FAM_TOTASSET              
				,M.FAM_DEBT          AS    FAM_DEBT                  
				,M.FAM_NETASSET      AS    FAM_NETASSET              
				,M.FAM_NETINCOME     AS    FAM_NETINCOME             
				,M.PRECREDIT         AS    PRECREDIT                 
				,M.LAST_PRECREDIT    AS    LAST_PRECREDIT            
				,M.FR_ID             AS    FR_ID                     
			FROM     OCRM_F_CI_SUN_BASE M
			LEFT JOIN  A_R_CUST_ID_TEMP   N
			ON 
			--M.FR_ID = V_FR_ID AND 
			M.CUST_ID = N.SOURCE_CUST_ID                                                                                                                                                                                                                                                                                                                                                      
 """                                                                                                                                                                                
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                     
OCRM_F_CI_SUN_BASE	= sqlContext.sql(sql) 
OCRM_F_CI_SUN_BASE.registerTempTable("OCRM_F_CI_SUN_BASE")                                                                                                                                      
dfn="OCRM_F_CI_SUN_BASE/"+V_DT+".parquet"                                                                                                                                      
OCRM_F_CI_SUN_BASE.cache()                                                                                                                                                     
nrows = OCRM_F_CI_SUN_BASE.count()                                                                                                                                             
OCRM_F_CI_SUN_BASE.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
OCRM_F_CI_SUN_BASE.unpersist()                                                                                                                                            
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_BASE/"+V_DT_LD+".parquet") 
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_BASE/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SUN_BASE_BK/")                                                                                                                                                 
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_BASE lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)     


#涉及大字段，暂不处理  
#任务[11] 001-06::                                                                                                                                                                  
#V_STEP = V_STEP + 1                                                                                                                                                                 
                                                                                                                                                                                    
#sql = """   
#			SELECT 
#				M.ID           AS   ID          
#				,CAST(N.CUST_ID AS VARCHAR(64))     AS   CUST_ID     
#				,M.TYPE         AS   TYPE        
#				,M.PHOTO        AS   PHOTO       
#				,M.CREATE_DATE  AS   CREATE_DATE 
#				,M.UPDATE_DATE  AS   UPDATE_DATE 
#				,M.IS_MOD       AS   IS_MOD      
#				,M.FR_ID        AS   FR_ID       
#				,M.PHOTO_NAME   AS   PHOTO_NAME  
#				,M.PHOTO_URL    AS   PHOTO_URL   
#				,M.PHOTO_ID     AS   PHOTO_ID       
#			FROM     OCRM_F_CI_SUN_PHOTO M
#			LEFT JOIN  A_R_CUST_ID_TEMP   N
#			ON 
#			--M.FR_ID = V_FR_ID AND 
#			M.CUST_ID = N.SOURCE_CUST_ID                                                                                                                                                                                                                                                                                                                                                      
# """                                                                                                                                                                                
#                                                                                                                                                                                  
#sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                     
#OCRM_F_CI_SUN_PHOTO = sqlContext.sql(sql)                                                                                                                                       
#dfn="OCRM_F_CI_SUN_PHOTO/"+V_DT+".parquet"                                                                                                                                      
#OCRM_F_CI_SUN_PHOTO.cache()                                                                                                                                                     
#nrows = OCRM_F_CI_SUN_PHOTO.count()                                                                                                                                             
#OCRM_F_CI_SUN_PHOTO.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
#OCRM_F_CI_SUN_PHOTO.unpersist()                                                                                                                                            
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_PHOTO/"+V_DT_LD+".parquet")                                                                                                                                                 
#et = datetime.now()                                                                                                                                                                 
#print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_PHOTO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)     


#任务[11] 001-07::                                                                                                                                                                  
V_STEP = V_STEP + 1                                                                                                                                                                 
A_R_CUST_ID_TEMP = sqlContext.read.parquet(hdfs+'/A_R_CUST_ID_TEMP/*')                                                                                     
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                                                                    
sql = """   
			SELECT 
				M.ID                            AS   ID             
				,CAST(N.CUST_ID  AS VARCHAR(30))                     AS   CUST_ID        
				,M.FARMER_TYPE                   AS   FARMER_TYPE    
				,M.NATURE                        AS   NATURE         
				,M.COMPANY_TYPE                  AS   COMPANY_TYPE   
				,M.BUSI_TYPE                     AS   BUSI_TYPE      
				,M.POVERTY_REASON                AS   POVERTY_REASON 
				,M.STORE_VARIETY                 AS   STORE_VARIETY  
				,M.WORK_DETAIL                   AS   WORK_DETAIL    
				,M.PROJECT                       AS   PROJECT        
				,M.ANNUAL_INCOME                 AS   ANNUAL_INCOME  
				,M.SERVICE_TYPE                  AS   SERVICE_TYPE   
				,M.STATUS                        AS   STATUS         
				,M.FR_ID                         AS   FR_ID          
			FROM     OCRM_F_CI_SUN_FARMERTYPE_DETAIL M
			LEFT JOIN  A_R_CUST_ID_TEMP   N
			ON 
			--M.FR_ID = V_FR_ID AND 
			M.CUST_ID = N.SOURCE_CUST_ID                                                                                                                                                                                                                                                                                                                                                      
 """                                                                                                                                                                                
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                     
OCRM_F_CI_SUN_FARMERTYPE_DETAIL = sqlContext.sql(sql)
OCRM_F_CI_SUN_FARMERTYPE_DETAIL.registerTempTable("OCRM_F_CI_SUN_FARMERTYPE_DETAIL")                                                                                                                                         
dfn="OCRM_F_CI_SUN_FARMERTYPE_DETAIL/"+V_DT+".parquet"                                                                                                                                      
OCRM_F_CI_SUN_FARMERTYPE_DETAIL.cache()                                                                                                                                                     
nrows = OCRM_F_CI_SUN_FARMERTYPE_DETAIL.count()                                                                                                                                             
OCRM_F_CI_SUN_FARMERTYPE_DETAIL.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
OCRM_F_CI_SUN_FARMERTYPE_DETAIL.unpersist()                                                                                                                                            
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_FARMERTYPE_DETAIL/"+V_DT_LD+".parquet") 
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_FARMERTYPE_DETAIL/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SUN_FARMERTYPE_DETAIL_BK/")                                                                                                                                                 
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_FARMERTYPE_DETAIL lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)     



#任务[11] 001-08::                                                                                                                                                                  
V_STEP = V_STEP + 1                                                                                                                                                                 
A_R_CUST_ID_TEMP = sqlContext.read.parquet(hdfs+'/A_R_CUST_ID_TEMP/*')                                                                                     
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                                                                    
sql = """   
			SELECT 
				M.ID              AS   ID             
				,CAST(N.CUST_ID  AS VARCHAR(30))       AS   CUST_ID        
				,M.PER_QUAL        AS   PER_QUAL       
				,M.CREDIT_STATUS   AS   CREDIT_STATUS  
				,M.BAD_HABITS      AS   BAD_HABITS     
				,M.FAMILY_HARMONY  AS   FAMILY_HARMONY 
				,M.NEIGHBORHOOD    AS   NEIGHBORHOOD   
				,M.FR_ID           AS   FR_ID          
    
			FROM     OCRM_F_CI_SUN_EVALUATION M
			LEFT JOIN  A_R_CUST_ID_TEMP   N
			ON 
			--M.FR_ID = V_FR_ID AND 
			M.CUST_ID = N.SOURCE_CUST_ID                                                                                                                                                                                                                                                                                                                                                      
 """                                                                                                                                                                                
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                     
OCRM_F_CI_SUN_EVALUATION = sqlContext.sql(sql) 
OCRM_F_CI_SUN_EVALUATION.registerTempTable("OCRM_F_CI_SUN_EVALUATION")                                                                                                                                     
dfn="OCRM_F_CI_SUN_EVALUATION/"+V_DT+".parquet"                                                                                                                                      
OCRM_F_CI_SUN_EVALUATION.cache()                                                                                                                                                     
nrows = OCRM_F_CI_SUN_EVALUATION.count()                                                                                                                                             
OCRM_F_CI_SUN_EVALUATION.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
OCRM_F_CI_SUN_EVALUATION.unpersist()                                                                                                                                            
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_EVALUATION/"+V_DT_LD+".parquet") 
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_EVALUATION/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SUN_EVALUATION_BK/")                                                                                                                                                 
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_EVALUATION lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)     


#任务[11] 001-09::                                                                                                                                                                  
V_STEP = V_STEP + 1                                                                                                                                                                 
A_R_CUST_ID_TEMP = sqlContext.read.parquet(hdfs+'/A_R_CUST_ID_TEMP/*')                                                                                     
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                                                                    
sql = """   
			SELECT 
				M.ID       AS   ID     
				,CAST(N.CUST_ID AS VARCHAR(30))  AS   CUST_ID
				,M.ADDRESS  AS   ADDRESS
				,M.FLOOR    AS   FLOOR  
				,M.AREA     AS   AREA   
				,M.PRICE    AS   PRICE  
				,M.FR_ID    AS   FR_ID  
			FROM     OCRM_F_CI_SUN_HOUSEINFO M
			LEFT JOIN  A_R_CUST_ID_TEMP   N
			ON 
			--M.FR_ID = V_FR_ID AND 
			M.CUST_ID = N.SOURCE_CUST_ID                                                                                                                                                                                                                                                                                                                                                      
 """                                                                                                                                                                                
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                     
OCRM_F_CI_SUN_HOUSEINFO= sqlContext.sql(sql) 
OCRM_F_CI_SUN_HOUSEINFO.registerTempTable("OCRM_F_CI_SUN_HOUSEINFO")                                                                                                                                        
dfn="OCRM_F_CI_SUN_HOUSEINFO/"+V_DT+".parquet"                                                                                                                                      
OCRM_F_CI_SUN_HOUSEINFO.cache()                                                                                                                                                     
nrows = OCRM_F_CI_SUN_HOUSEINFO.count()                                                                                                                                             
OCRM_F_CI_SUN_HOUSEINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
OCRM_F_CI_SUN_HOUSEINFO.unpersist()                                                                                                                                            
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_HOUSEINFO/"+V_DT_LD+".parquet") 
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_HOUSEINFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SUN_HOUSEINFO_BK/")                                                                                                                                                 
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_HOUSEINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)     


#任务[11] 001-10:                                                                                                                                                                  
V_STEP = V_STEP + 1                                                                                                                                                                 
A_R_CUST_ID_TEMP = sqlContext.read.parquet(hdfs+'/A_R_CUST_ID_TEMP/*')                                                                                     
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                                                                    
sql = """   
			SELECT 
				M.ID              AS  ID             
				,CAST(N.CUST_ID   AS VARCHAR(30))      AS  CUST_ID        
				,M.TYPE            AS  TYPE           
				,M.BRAND           AS  BRAND          
				,M.PURCHASE_PRICE  AS  PURCHASE_PRICE 
				,M.YEAR            AS  YEAR           
				,M.ASSESS          AS  ASSESS         
				,M.FR_ID           AS  FR_ID          

			FROM     OCRM_F_CI_SUN_CARINFO M
			LEFT JOIN  A_R_CUST_ID_TEMP   N
			ON 
			--M.FR_ID = V_FR_ID AND 
			M.CUST_ID = N.SOURCE_CUST_ID                                                                                                                                                                                                                                                                                                                                                      
 """                                                                                                                                                                                
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                     
OCRM_F_CI_SUN_CARINFO= sqlContext.sql(sql)  
OCRM_F_CI_SUN_CARINFO.registerTempTable("OCRM_F_CI_SUN_CARINFO")                                                                                                                                        
dfn="OCRM_F_CI_SUN_CARINFO/"+V_DT+".parquet"                                                                                                                                      
OCRM_F_CI_SUN_CARINFO.cache()                                                                                                                                                     
nrows = OCRM_F_CI_SUN_CARINFO.count()                                                                                                                                             
OCRM_F_CI_SUN_CARINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
OCRM_F_CI_SUN_CARINFO.unpersist()                                                                                                                                            
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_CARINFO/"+V_DT_LD+".parquet") 
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_CARINFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SUN_CARINFO_BK/")                                                                                                                                                 
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_CARINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)     


#任务[11] 001-11:                                                                                                                                                                  
V_STEP = V_STEP + 1                                                                                                                                                                 
A_R_CUST_ID_TEMP = sqlContext.read.parquet(hdfs+'/A_R_CUST_ID_TEMP/*')                                                                                     
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                                                                    
sql = """   
			SELECT 
				 M.ID      AS ID     
				,CAST(N.CUST_ID AS VARCHAR(30)) AS CUST_ID
				,M.OTHER   AS OTHER  
				,M.ASSESS  AS ASSESS 
				,M.FR_ID   AS FR_ID  
      
			FROM     OCRM_F_CI_SUN_OTHERINFO M
			LEFT JOIN  A_R_CUST_ID_TEMP   N
			ON 
			--M.FR_ID = V_FR_ID AND 
			M.CUST_ID = N.SOURCE_CUST_ID                                                                                                                                                                                                                                                                                                                                                      
 """                                                                                                                                                                                
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                     
OCRM_F_CI_SUN_OTHERINFO= sqlContext.sql(sql)  
OCRM_F_CI_SUN_OTHERINFO.registerTempTable("OCRM_F_CI_SUN_OTHERINFO")                                                                                                                                      
dfn="OCRM_F_CI_SUN_OTHERINFO/"+V_DT+".parquet"                                                                                                                                      
OCRM_F_CI_SUN_OTHERINFO.cache()                                                                                                                                                     
nrows = OCRM_F_CI_SUN_OTHERINFO.count()                                                                                                                                             
OCRM_F_CI_SUN_OTHERINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
OCRM_F_CI_SUN_OTHERINFO.unpersist()                                                                                                                                            
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_OTHERINFO/"+V_DT_LD+".parquet") 
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_OTHERINFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SUN_OTHERINFO_BK/")                                                                                                                                                 
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_OTHERINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)     



#任务[11] 001-12:                                                                                                                                                                  
V_STEP = V_STEP + 1                                                                                                                                                                 
A_R_CUST_ID_TEMP = sqlContext.read.parquet(hdfs+'/A_R_CUST_ID_TEMP/*')                                                                                     
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                                                                    
sql = """   
			SELECT 
				M.ID                 AS ID                  
				,CAST(N.CUST_ID AS VARCHAR(30))           AS CUST_ID             
				,M.RECEIVABLE_CREDIT  AS RECEIVABLE_CREDIT   
				,M.STOCK              AS STOCK               
				,M.BANK_DEPOSITS      AS BANK_DEPOSITS       
				,M.FR_ID              AS FR_ID               
                                            

      
			FROM     OCRM_F_CI_SUN_OTHFLOWINFO M
			LEFT JOIN  A_R_CUST_ID_TEMP   N
			ON 
			--M.FR_ID = V_FR_ID AND 
			M.CUST_ID = N.SOURCE_CUST_ID                                                                                                                                                                                                                                                                                                                                                      
 """                                                                                                                                                                                
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                     
OCRM_F_CI_SUN_OTHFLOWINFO= sqlContext.sql(sql) 
OCRM_F_CI_SUN_OTHFLOWINFO.registerTempTable("OCRM_F_CI_SUN_OTHFLOWINFO")                                                                                                                                       
dfn="OCRM_F_CI_SUN_OTHFLOWINFO/"+V_DT+".parquet"                                                                                                                                      
OCRM_F_CI_SUN_OTHFLOWINFO.cache()                                                                                                                                                     
nrows = OCRM_F_CI_SUN_OTHFLOWINFO.count()                                                                                                                                             
OCRM_F_CI_SUN_OTHFLOWINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
OCRM_F_CI_SUN_OTHFLOWINFO.unpersist()                                                                                                                                            
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_OTHFLOWINFO/"+V_DT_LD+".parquet")  
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_OTHFLOWINFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SUN_OTHFLOWINFO_BK/")                                                                                                                                                
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_OTHFLOWINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)     

#任务[11] 001-13:                                                                                                                                                                  
V_STEP = V_STEP + 1                                                                                                                                                                 
A_R_CUST_ID_TEMP = sqlContext.read.parquet(hdfs+'/A_R_CUST_ID_TEMP/*')                                                                                     
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                                                                    
sql = """   
			SELECT 
				M.ID                 AS  ID                
				,CAST(N.CUST_ID  AS VARCHAR(30))          AS  CUST_ID           
				,M.PRIVATE_LOANS      AS  PRIVATE_LOANS     
				,M.EXTERNAL_SECURITY  AS  EXTERNAL_SECURITY 
				,M.BANK_LOAN          AS  BANK_LOAN         
				,M.FR_ID              AS  FR_ID             
			FROM     OCRM_F_CI_SUN_DEBTINFO M
			LEFT JOIN  A_R_CUST_ID_TEMP   N
			ON 
			--M.FR_ID = V_FR_ID AND 
			M.CUST_ID = N.SOURCE_CUST_ID                                                                                                                                                                                                                                                                                                                                                      
 """                                                                                                                                                                                
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                     
OCRM_F_CI_SUN_DEBTINFO = sqlContext.sql(sql)
OCRM_F_CI_SUN_DEBTINFO.registerTempTable("OCRM_F_CI_SUN_DEBTINFO")                                                                                                                                       
dfn="OCRM_F_CI_SUN_DEBTINFO/"+V_DT+".parquet"                                                                                                                                      
OCRM_F_CI_SUN_DEBTINFO.cache()                                                                                                                                                     
nrows = OCRM_F_CI_SUN_DEBTINFO.count()                                                                                                                                             
OCRM_F_CI_SUN_DEBTINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
OCRM_F_CI_SUN_DEBTINFO.unpersist()                                                                                                                                            
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_DEBTINFO/"+V_DT_LD+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_DEBTINFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SUN_DEBTINFO_BK/")                                                                                                                                                  
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_DEBTINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)     


#任务[11] 001-14:                                                                                                                                                                  
V_STEP = V_STEP + 1                                                                                                                                                                 
A_R_CUST_ID_TEMP = sqlContext.read.parquet(hdfs+'/A_R_CUST_ID_TEMP/*')                                                                                     
A_R_CUST_ID_TEMP.registerTempTable("A_R_CUST_ID_TEMP")                                                                                                                                                                                    
sql = """   
			SELECT 
				M.ID                  AS  ID                 
				,CAST(N.CUST_ID  AS VARCHAR(30))           AS  CUST_ID            
				,M.RATING_SCORE        AS  RATING_SCORE       
				,M.RATING_RESULT       AS  RATING_RESULT      
				,M.RATING_RATIO        AS  RATING_RATIO       
				,M.CREDIT_LINE         AS  CREDIT_LINE        
				,M.FINANCIAL_PRODUCTS  AS  FINANCIAL_PRODUCTS 
				,M.ISINVEST            AS  ISINVEST           
				,M.FR_ID               AS  FR_ID              
                                             
       
			FROM     OCRM_F_CI_SUN_CREDITINFO M
			LEFT JOIN  A_R_CUST_ID_TEMP   N
			ON 
			--M.FR_ID = V_FR_ID AND 
			M.CUST_ID = N.SOURCE_CUST_ID                                                                                                                                                                                                                                                                                                                                                      
 """                                                                                                                                                                                
                                                                                                                                                                                    
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)                                                                                                                                     
OCRM_F_CI_SUN_CREDITINFO= sqlContext.sql(sql)
OCRM_F_CI_SUN_CREDITINFO.registerTempTable("OCRM_F_CI_SUN_CREDITINFO")                                                                                                                                       
dfn="OCRM_F_CI_SUN_CREDITINFO/"+V_DT+".parquet"                                                                                                                                      
OCRM_F_CI_SUN_CREDITINFO.cache()                                                                                                                                                     
nrows = OCRM_F_CI_SUN_CREDITINFO.count()                                                                                                                                             
OCRM_F_CI_SUN_CREDITINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')                                                                                                    
OCRM_F_CI_SUN_CREDITINFO.unpersist()                                                                                                                                            
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_CREDITINFO/"+V_DT_LD+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_SUN_CREDITINFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SUN_CREDITINFO_BK/")                                                                                                                                                  
et = datetime.now()                                                                                                                                                                 
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_CREDITINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)     







































































































































































































