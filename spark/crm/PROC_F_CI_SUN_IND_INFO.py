#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_SUN_IND_INFO').setMaster(sys.argv[2])
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

F_CI_SUN_IND_INFO = sqlContext.read.parquet(hdfs+'/F_CI_SUN_IND_INFO/*')
F_CI_SUN_IND_INFO.registerTempTable("F_CI_SUN_IND_INFO")
F_LN_SUNCREDIT_APPLY = sqlContext.read.parquet(hdfs+'/F_LN_SUNCREDIT_APPLY/*')
F_LN_SUNCREDIT_APPLY.registerTempTable("F_LN_SUNCREDIT_APPLY")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT M.CUSTOMERID            AS CUSTOMERID 
       ,M.BUSINESSSUM           AS BUSINESSSUM 
       ,M.OPERATEUSERID         AS OPERATEUSERID 
       ,M.OPERATEORGID          AS OPERATEORGID 
       ,M.OCCURDATE             AS OCCURDATE 
       ,M.MATURITY              AS MATURITY 
       ,M.TERMMONTH             AS TERMMONTH 
       ,N.FR_ID                 AS FR_ID 
   FROM F_LN_SUNCREDIT_APPLY M                                 --阳光信贷预授信申请表
  INNER JOIN ADMIN_AUTH_ORG N                                  --ADMIN_AUTH_ORG
     ON M.OPERATEORGID          = N.ORG_ID 
  WHERE M.FINISHDATE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_F_LN_SUNCREDIT_APPLY = sqlContext.sql(sql)
TMP_F_LN_SUNCREDIT_APPLY.registerTempTable("TMP_F_LN_SUNCREDIT_APPLY")
dfn="TMP_F_LN_SUNCREDIT_APPLY/"+V_DT+".parquet"
TMP_F_LN_SUNCREDIT_APPLY.cache()
nrows = TMP_F_LN_SUNCREDIT_APPLY.count()
TMP_F_LN_SUNCREDIT_APPLY.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_F_LN_SUNCREDIT_APPLY.unpersist()
F_LN_SUNCREDIT_APPLY.unpersist()
ADMIN_AUTH_ORG.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_F_LN_SUNCREDIT_APPLY/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert TMP_F_LN_SUNCREDIT_APPLY lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1
TMP_F_LN_SUNCREDIT_APPLY = sqlContext.read.parquet(hdfs+'/TMP_F_LN_SUNCREDIT_APPLY/*')
TMP_F_LN_SUNCREDIT_APPLY.registerTempTable("TMP_F_LN_SUNCREDIT_APPLY")
sql = """
 SELECT monotonically_increasing_id() AS ID
           ,NVL(C.ODS_CUST_ID, A.CUSTOMERID) AS CUST_ID
           ,A.FULLNAME AS FULLNAME
           ,A.CERTTYPE AS CERTTYPE
           ,A.CERTID AS CERTID
           ,A.SEX AS SEX
           ,A.BIRTHDAY AS BIRTHDAY
           ,NVL(A.FAMILYADD, A.NATIVEPLACE) AS NATIVEPLACE
           ,A.MYBANKDORM AS MYBANKDORM
           ,A.STAFF AS STAFF
           ,A.ISREALTIVEMAN AS ISREALTIVEMAN
           ,A.NATIONALITY AS NATIONALITY
           ,A.POLITICALFACE AS POLITICALFACE
           ,A.MARRIAGE AS MARRIAGE
           ,A.HEALTH AS HEALTH
           ,A.EDUEXPERIENCE AS EDUEXPERIENCE
           ,A.EDUDEGREE AS EDUDEGREE
           ,A.WORKCORP AS WORKCORP
           ,A.HEADSHIP AS HEADSHIP
           ,A.POSITION AS POSITION
           ,CAST(A.FAMILYAMOUNT AS DECIMAL(10)) AS FAMILYAMOUNT
           ,A.TEAMNAME AS TEAMNAME
           ,A.REMARK2 AS REMARK2
           ,A.REMARK3 AS REMARK3
           ,A.CREDITFARMER AS CREDITFARMER
           ,A.REMARK4 AS REMARK4
           ,A.FAMILYSTATUS AS FAMILYSTATUS
           ,A.FAMILYADD AS FAMILYADD
           ,A.FAMILYZIP AS FAMILYZIP
           ,A.FAMILYTEL AS FAMILYTEL
           ,A.MOBILETELEPHONE AS MOBILETELEPHONE
           ,A.EMAILADD AS EMAILADD
           ,A.OCCUPATION AS OCCUPATION
           ,A.DELAYCREDIT AS DELAYCREDIT
           ,A.DELAYCREDITREASON AS DELAYCREDITREASON
           ,A.FARMERCARD AS FARMERCARD
           ,A.MYBALANCEACCOUNT AS MYBALANCEACCOUNT
           ,A.MARKUP AS MARKUP
           ,CAST(A.FAMILYMONTHINCOME AS DECIMAL(10)) AS FAMILYMONTHINCOME
           ,A.MAINPROORINCOME AS MAINPROORINCOME
           ,A.CREDITLEVEL AS CREDITLEVEL
           ,A.EVALUATEDATE AS EVALUATEDATE
           ,A.REMARK AS REMARK
           ,A.INPUTUSERID AS INPUTUSERID
           ,A.INPUTORGID AS INPUTORGID
           ,A.INPUTDATE AS INPUTDATE
           ,'' AS UPDATEDATE
           ,A.UPDATEUSERID AS UPDATEUSERID
           ,A.UPDATEORGID AS UPDATEORGID
           ,A.FARMILYID AS FARMILYID
           ,A.FAMILYROLE AS FAMILYROLE
           ,A.ISHZ AS ISHZ
           ,CAST(A.WORKINGCAPITAL AS DECIMAL(24,6)) AS WORKINGCAPITAL
           ,CAST(A.CAPITALASSETS AS DECIMAL(24,6)) AS CAPITALASSETS
           ,CAST(A.FAMILYAVERAGEINCOME AS DECIMAL(24,6)) AS FAMILYAVERAGEINCOME
           ,CAST(A.FAMILYALLINCOME AS DECIMAL(24,6)) AS FAMILYALLINCOME
           ,CAST(A.FAMILYALLOUT AS DECIMAL(24,6)) AS FAMILYALLOUT
           ,CAST(A.FAMILYPUREINCOME AS DECIMAL(24,6)) AS FAMILYPUREINCOME
           ,CAST(A.TOTALASSETS AS DECIMAL(24,6)) AS TOTALASSETS
           ,CAST(A.TOTALINDEBTEDNESS AS DECIMAL(24,7)) AS TOTALINDEBTEDNESS
           ,CAST(A.FAMILYPUREASSET AS DECIMAL(24,6)) AS FAMILYPUREASSET
           ,CAST(A.LANDSIZE AS DECIMAL(24,6)) AS LANDSIZE
           ,A.LANDNO AS LANDNO
           ,CAST(A.YEAROUTCOME AS DECIMAL(24,8)) AS YEAROUTCOME
           ,A.BUSINESSADDRESS AS BUSINESSADDRESS
           ,A.ALLGUARANTYADDRESS AS ALLGUARANTYADDRESS
           ,A.ALLGUARANTYTEL AS ALLGUARANTYTEL
           ,A.CREDITDATE AS CREDITDATE
           ,A.INFRINGEMENTTIMES AS INFRINGEMENTTIMES
           ,CAST(A.AVERAGEDEPOSIT AS DECIMAL(24,6)) AS AVERAGEDEPOSIT
           ,A.PROJECTNO AS PROJECTNO
           ,CAST(A.MAINPROSCOPE AS DECIMAL(24,6)) AS MAINPROSCOPE
           ,A.MANAGEUSERID AS MANAGEUSERID
           ,A.MANAGEORGID AS MANAGEORGID
           ,CAST(A.ORDERDEPOSIT AS DECIMAL(24,6)) AS ORDERDEPOSIT
           ,A.MHOUSESTRUCTURE AS MHOUSESTRUCTURE
           ,A.MHOUSENO AS MHOUSENO
           ,CAST(A.ACTUALEVALUATE AS DECIMAL(24,6)) AS ACTUALEVALUATE
           ,A.OHOUSESTRUCTURE AS OHOUSESTRUCTURE
           ,A.OHOUSENO AS OHOUSENO
           ,CAST(A.OACTUALEVALUATE AS DECIMAL(24,6)) AS OACTUALEVALUATE
           ,A.MACHINENAME AS MACHINENAME
           ,CAST(A.MACHINEVALUE AS DECIMAL(24,6)) AS MACHINEVALUE
           ,CAST(A.OTHERASSET AS DECIMAL(24,6)) AS OTHERASSET
           ,A.HOUSEAREANAME AS HOUSEAREANAME
           ,A.HOUSEID AS HOUSEID
           ,CAST(A.HOUSEAREANO AS DECIMAL(24,6)) AS HOUSEAREANO
           ,A.CUSTOMERTYPE AS CUSTOMERTYPE
           ,CAST(A.YEARLNCOME AS DECIMAL(24,6)) AS YEARLNCOME
           ,A.CORPORATEORGID AS CORPORATEORGID
           ,A.TEMPSAVEFLAG AS TEMPSAVEFLAG
           ,A.TEAMNO AS TEAMNO
           ,A.VILLAGENO AS VILLAGENO
           ,A.LOCKORNOT AS LOCKORNOT
           ,A.ISUSINGCREDIT AS ISUSINGCREDIT
           ,A.XDCUSTOMERID AS XDCUSTOMERID
           ,A.ODS_ST_DATE AS ODS_ST_DATE
           ,A.ODS_SYS_ID AS ODS_SYS_ID
           ,CAST(B.BUSINESSSUM AS DECIMAL(24)) AS BUSINESSSUM
           ,A.CORPORATEORGID AS FR_ID
           ,B.OCCURDATE AS OCCURDATE
           ,B.MATURITY AS MATURITY
           ,B.TERMMONTH AS TERMMONTH
   FROM F_CI_SUN_IND_INFO A                                    --阳光信贷农户表
   LEFT JOIN TMP_F_LN_SUNCREDIT_APPLY B                        --TMP_F_LN_SUNCREDIT_APPLY
     ON A.CUSTOMERID            = B.CUSTOMERID 
    AND A.CORPORATEORGID        = B.FR_ID 
   LEFT JOIN (SELECT min(ODS_CUST_ID) ODS_CUST_ID,SOURCE_CUST_ID,FR_ID
                    FROM OCRM_F_CI_SYS_RESOURCE 
                   WHERE ODS_SYS_ID='SLNA' GROUP BY SOURCE_CUST_ID,FR_ID
) C                          --系统来源中间表
     ON A.CUSTOMERID            = C.SOURCE_CUST_ID 
    AND A.CORPORATEORGID        = C.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SUN_IND_INFO = sqlContext.sql(sql)
OCRM_F_CI_SUN_IND_INFO.registerTempTable("OCRM_F_CI_SUN_IND_INFO")
dfn="OCRM_F_CI_SUN_IND_INFO/"+V_DT+".parquet"
OCRM_F_CI_SUN_IND_INFO.cache()
nrows = OCRM_F_CI_SUN_IND_INFO.count()
OCRM_F_CI_SUN_IND_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SUN_IND_INFO.unpersist()
F_CI_SUN_IND_INFO.unpersist()
TMP_F_LN_SUNCREDIT_APPLY.unpersist()
OCRM_F_CI_SYS_RESOURCE.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SUN_IND_INFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SUN_IND_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
