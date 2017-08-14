#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_VILLAGE_INFO').setMaster(sys.argv[2])
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

O_CI_XDXT_VILLAGE_INFO = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_VILLAGE_INFO/*')
O_CI_XDXT_VILLAGE_INFO.registerTempTable("O_CI_XDXT_VILLAGE_INFO")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.EVALUATERESULT        AS EVALUATERESULT 
       ,A.SERIALNO              AS SERIALNO 
       ,A.VILLAGETYPE           AS VILLAGETYPE 
       ,A.REGIONALISMCODE       AS REGIONALISMCODE 
       ,A.REGIONALISMNAME       AS REGIONALISMNAME 
       ,A.FINANCEBELONG         AS FINANCEBELONG 
       ,A.GROUPNUMBER           AS GROUPNUMBER 
       ,A.VILLAGENUMBER         AS VILLAGENUMBER 
       ,A.EMPLOYEENUMBER        AS EMPLOYEENUMBER 
       ,A.FARMERNUMBER          AS FARMERNUMBER 
       ,A.LOANFARMERNUMBER      AS LOANFARMERNUMBER 
       ,A.ACCORDFARMERNUMBER    AS ACCORDFARMERNUMBER 
       ,A.FINANCEINFO           AS FINANCEINFO 
       ,A.MOSTLYINDUSTRY        AS MOSTLYINDUSTRY 
       ,A.OFFICETEL             AS OFFICETEL 
       ,A.FIXTEL                AS FIXTEL 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.RELATIVESERIALNO      AS RELATIVESERIALNO 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.UPDATEDATE            AS UPDATEDATE 
       ,A.REMARK                AS REMARK 
       ,A.RELATIVENO            AS RELATIVENO 
       ,A.VALIDFARMERNUMBER     AS VALIDFARMERNUMBER 
       ,A.RELATIVENAME          AS RELATIVENAME 
       ,A.BELONGORGID           AS BELONGORGID 
       ,A.CHECKSTATUS           AS CHECKSTATUS 
       ,A.SMALLVILAGENUMBER     AS SMALLVILAGENUMBER 
       ,A.FAMILYNUMBER          AS FAMILYNUMBER 
       ,A.FARMERPERSONNUMBER    AS FARMERPERSONNUMBER 
       ,A.FARMERLOANNUMBER      AS FARMERLOANNUMBER 
       ,A.CREDITNUMBER          AS CREDITNUMBER 
       ,A.FIELDAREA             AS FIELDAREA 
       ,A.REGISTERFIELDAREA     AS REGISTERFIELDAREA 
       ,A.GLEBEFIELDAREA        AS GLEBEFIELDAREA 
       ,A.PADDYFIELDAREA        AS PADDYFIELDAREA 
       ,A.EVALUATEOVERDATE      AS EVALUATEOVERDATE 
       ,A.CREDITFLAG            AS CREDITFLAG 
       ,A.CORPORATEORGID        AS CORPORATEORGID 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_VILLAGE_INFO A                               --乡村镇信息
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_VILLAGE_INFO = sqlContext.sql(sql)
F_CI_XDXT_VILLAGE_INFO.registerTempTable("F_CI_XDXT_VILLAGE_INFO")
dfn="F_CI_XDXT_VILLAGE_INFO/"+V_DT+".parquet"
F_CI_XDXT_VILLAGE_INFO.cache()
nrows = F_CI_XDXT_VILLAGE_INFO.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_VILLAGE_INFO/*.parquet")
F_CI_XDXT_VILLAGE_INFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_VILLAGE_INFO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_VILLAGE_INFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
