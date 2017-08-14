#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_SUN_IND_INFO_TMP').setMaster(sys.argv[2])
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

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
SELECT CAST(F.CUSTOMERID AS VARCHAR(32)) AS CUSTOMERID,
       CAST(F.FULLNAME AS VARCHAR(32)) AS FULLNAME,
       CAST(F.CERTTYPE AS VARCHAR(10)) AS CERTTYPE,
       CAST(F.CERTID AS VARCHAR(18)) AS CERTID,
       CAST('' AS VARCHAR(4)) AS SEX,
       CAST('' AS VARCHAR(10)) AS BIRTHDAY,
       CAST(F.NATIVEPLACE AS VARCHAR(200)) AS NATIVEPLACE,
       CAST('' AS VARCHAR(4)) AS MYBANKDORM,
       CAST('' AS VARCHAR(4)) AS STAFF,
       CAST('' AS VARCHAR(4)) AS ISREALTIVEMAN,
       CAST('' AS VARCHAR(18)) AS NATIONALITY,
       CAST('' AS VARCHAR(18)) AS POLITICALFACE,
       CAST('' AS VARCHAR(18)) AS MARRIAGE,
       CAST('' AS VARCHAR(4)) AS HEALTH,
       CAST('' AS VARCHAR(18)) AS EDUEXPERIENCE,
       CAST('' AS VARCHAR(18)) AS EDUDEGREE,
       CAST('' AS VARCHAR(80)) AS WORKCORP,
       CAST('' AS VARCHAR(18)) AS HEADSHIP,
       CAST('' AS VARCHAR(18)) AS POSITION,
       CAST('' AS DECIMAL(10)) AS FAMILYAMOUNT,
       CAST('' AS VARCHAR(200)) AS TEAMNAME,
       CAST('' AS VARCHAR(250)) AS REMARK2,
       CAST('' AS VARCHAR(250)) AS REMARK3,
       CAST('' AS VARCHAR(18)) AS CREDITFARMER,
       CAST('' AS VARCHAR(250)) AS REMARK4,
       CAST('' AS VARCHAR(18)) AS FAMILYSTATUS,
       CAST(F.FAMILYADD AS VARCHAR(200)) AS FAMILYADD,
       CAST('' AS VARCHAR(32)) AS FAMILYZIP,
       CAST('' AS VARCHAR(40)) AS FAMILYTEL,
       CAST('' AS VARCHAR(32)) AS MOBILETELEPHONE,
       CAST('' AS VARCHAR(80)) AS EMAILADD,
       CAST('' AS VARCHAR(18)) AS OCCUPATION,
       CAST('' AS VARCHAR(4)) AS DELAYCREDIT,
       CAST('' AS VARCHAR(18)) AS DELAYCREDITREASON,
       CAST('' AS VARCHAR(50)) AS FARMERCARD,
       CAST('' AS VARCHAR(32)) AS MYBALANCEACCOUNT,
       CAST('' AS VARCHAR(40)) AS MARKUP,
       CAST('' AS DECIMAL(10)) AS FAMILYMONTHINCOME,
       CAST('' AS VARCHAR(200)) AS MAINPROORINCOME,
       CAST('' AS VARCHAR(18)) AS CREDITLEVEL,
       CAST('' AS VARCHAR(10)) AS EVALUATEDATE,
       CAST(F.REMARK AS VARCHAR(250)) AS REMARK,
       CAST('' AS VARCHAR(10)) AS INPUTUSERID,
       CAST('' AS VARCHAR(12)) AS INPUTORGID,
       CAST('' AS VARCHAR(10)) AS INPUTDATE,
       CAST('' AS VARCHAR(10)) AS UPDATEDATE,
       CAST('' AS VARCHAR(10)) AS UPDATEUSERID,
       CAST('' AS VARCHAR(12)) AS UPDATEORGID,
       CAST(F.FARMILYID AS VARCHAR(20)) AS FARMILYID,
       CAST('' AS VARCHAR(4)) AS FAMILYROLE,
       CAST('' AS VARCHAR(4)) AS ISHZ,
       CAST('' AS DECIMAL(24,6)) AS WORKINGCAPITAL,
       CAST('' AS DECIMAL(24,6)) AS CAPITALASSETS,
       CAST('' AS DECIMAL(24,6)) AS FAMILYAVERAGEINCOME,
       CAST('' AS DECIMAL(24,6)) AS FAMILYALLINCOME,
       CAST('' AS DECIMAL(24,6)) AS FAMILYALLOUT,
       CAST('' AS DECIMAL(24,6)) AS FAMILYPUREINCOME,
       CAST('' AS DECIMAL(24,6)) AS TOTALASSETS,
       CAST('' AS DECIMAL(24,7)) AS TOTALINDEBTEDNESS,
       CAST('' AS DECIMAL(24,6)) AS FAMILYPUREASSET,
       CAST('' AS DECIMAL(24,6)) AS LANDSIZE,
       CAST('' AS VARCHAR(80)) AS LANDNO,
       CAST('' AS DECIMAL(24,8)) AS YEAROUTCOME,
       CAST('' AS VARCHAR(80)) AS BUSINESSADDRESS,
       CAST('' AS VARCHAR(81)) AS ALLGUARANTYADDRESS,
       CAST('' AS VARCHAR(20)) AS ALLGUARANTYTEL,
       CAST('' AS VARCHAR(10)) AS CREDITDATE,
       CAST('' AS INTEGER) AS INFRINGEMENTTIMES,
       CAST('' AS DECIMAL(24,6)) AS AVERAGEDEPOSIT,
       CAST('' AS VARCHAR(20)) AS PROJECTNO,
       CAST('' AS DECIMAL(24,6)) AS MAINPROSCOPE,
       CAST('' AS VARCHAR(10)) AS MANAGEUSERID,
       CAST('' AS VARCHAR(12)) AS MANAGEORGID,
       CAST('' AS DECIMAL(24,6)) AS ORDERDEPOSIT,
       CAST('' AS VARCHAR(4)) AS MHOUSESTRUCTURE,
       CAST('' AS INTEGER) AS MHOUSENO,
       CAST('' AS DECIMAL(24,6)) AS ACTUALEVALUATE,
       CAST('' AS VARCHAR(4)) AS OHOUSESTRUCTURE,
       CAST('' AS INTEGER) AS OHOUSENO,
       CAST('' AS DECIMAL(24,6)) AS OACTUALEVALUATE,
       CAST('' AS VARCHAR(40)) AS MACHINENAME,
       CAST('' AS DECIMAL(24,6)) AS MACHINEVALUE,
       CAST('' AS DECIMAL(24,6)) AS OTHERASSET,
       CAST('' AS VARCHAR(80)) AS HOUSEAREANAME,
       CAST('' AS VARCHAR(40)) AS HOUSEID,
       CAST(F.HOUSEAREANO AS DECIMAL(24,6)) AS HOUSEAREANO,
       CAST('' AS VARCHAR(20)) AS CUSTOMERTYPE,
       CAST('' AS DECIMAL(24,6)) AS YEARLNCOME,
       CAST(F.FR_ID AS VARCHAR(32)) AS CORPORATEORGID,
       CAST('' AS VARCHAR(18)) AS TEMPSAVEFLAG,
       CAST('' AS VARCHAR(32)) AS TEAMNO,
       CAST(F.VILLAGENO AS VARCHAR(32)) AS VILLAGENO,
       CAST('' AS VARCHAR(2)) AS LOCKORNOT,
       CAST('' AS VARCHAR(2)) AS ISUSINGCREDIT,
       CAST('' AS VARCHAR(32)) AS XDCUSTOMERID,
       CAST(V_DT AS VARCHAR(10)) AS ODS_ST_DATE,
       CAST('' AS VARCHAR(5)) AS ODS_SYS_ID
FROM F_CI_SUN_IND_INFO F
              JOIN (SELECT MAX(CUSTOMERID) CUSTOMERID,FR_ID FROM F_CI_SUN_IND_INFO 
                    GROUP BY CERTTYPE,CERTID,FULLNAME,FR_ID ) C ON C.CUSTOMERID=F.CUSTOMERID AND C.FR_ID = F.FR_ID

"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_SUN_IND_INFO_TMP = sqlContext.sql(sql)
F_CI_SUN_IND_INFO_TMP.registerTempTable("F_CI_SUN_IND_INFO_TMP")
dfn="F_CI_SUN_IND_INFO_TMP/"+V_DT+".parquet"
F_CI_SUN_IND_INFO_TMP.cache()
nrows = F_CI_SUN_IND_INFO_TMP.count()
F_CI_SUN_IND_INFO_TMP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_SUN_IND_INFO_TMP.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_SUN_IND_INFO_TMP/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_SUN_IND_INFO_TMP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
