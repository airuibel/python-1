#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_APY_ZFB_FT_USER_PRO_SIGNINFO').setMaster(sys.argv[2])
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

O_CI_ZFB_FT_USER_PRO_SIGNINFO = sqlContext.read.parquet(hdfs+'/O_CI_ZFB_FT_USER_PRO_SIGNINFO/*')
O_CI_ZFB_FT_USER_PRO_SIGNINFO.registerTempTable("O_CI_ZFB_FT_USER_PRO_SIGNINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT CAST('' AS VARCHAR(30))                    AS JGM 
       ,USER_CIFNO              AS USER_CIFNO 
       ,USER_ACCTNO             AS USER_ACCTNO 
       ,USER_ACCTTYPE           AS USER_ACCTTYPE 
       ,VALIDATEDATE            AS VALIDATEDATE 
       ,CVV2                    AS CVV2 
       ,SIGN_CHANNELID          AS SIGN_CHANNELID 
       ,PAY_TYPE                AS PAY_TYPE 
       ,PAY_STYLEID             AS PAY_STYLEID 
       ,TRANS_ID                AS TRANS_ID 
       ,MER_ID                  AS MER_ID 
       ,MER_ACCT                AS MER_ACCT 
       ,SIGN_ID                 AS SIGN_ID 
       ,USERTC_PERTRANSLIMIT    AS USERTC_PERTRANSLIMIT 
       ,USERTC_PERDAYLIMIT      AS USERTC_PERDAYLIMIT 
       ,USERTC_DAYAMT           AS USERTC_DAYAMT 
       ,USERTC_DAYAMTDATE       AS USERTC_DAYAMTDATE 
       ,USERTC_OPENDATE         AS USERTC_OPENDATE 
       ,USERTC_OPENUSER         AS USERTC_OPENUSER 
       ,USERTC_MODIFYDATE       AS USERTC_MODIFYDATE 
       ,USERTC_MODIFYUSER       AS USERTC_MODIFYUSER 
       ,USERTC_CLOSEDATE        AS USERTC_CLOSEDATE 
       ,USERTC_CLOSEUSER        AS USERTC_CLOSEUSER 
       ,SIGNDEPT_ID             AS SIGNDEPT_ID 
       ,SIGN_STATUS             AS SIGN_STATUS 
       ,TRANS_CURRENCY          AS TRANS_CURRENCY 
       ,USERTC_MOBILEPHONE      AS USERTC_MOBILEPHONE 
       ,USERTC_CLIENTNAME       AS USERTC_CLIENTNAME 
       ,USERTC_CRENDENTYPE      AS USERTC_CRENDENTYPE 
       ,USERTC_CRENDENID        AS USERTC_CRENDENID 
       ,DEPT_ID                 AS DEPT_ID 
       ,TRANS_CLEARDATE         AS TRANS_CLEARDATE 
       ,CREDITCARD_TYPE         AS CREDITCARD_TYPE 
       ,USERTC_PERMONTHLIMIT    AS USERTC_PERMONTHLIMIT 
       ,USERTC_MONTHAMT         AS USERTC_MONTHAMT 
       ,USERTC_MONTHAMTDATE     AS USERTC_MONTHAMTDATE 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'APY'                   AS ODS_SYS_ID 
   FROM O_CI_ZFB_FT_USER_PRO_SIGNINFO A                        --客户协议支付签约信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_ZFB_FT_USER_PRO_SIGNINFO = sqlContext.sql(sql)
F_CI_ZFB_FT_USER_PRO_SIGNINFO.registerTempTable("F_CI_ZFB_FT_USER_PRO_SIGNINFO")
dfn="F_CI_ZFB_FT_USER_PRO_SIGNINFO/"+V_DT+".parquet"
F_CI_ZFB_FT_USER_PRO_SIGNINFO.cache()
nrows = F_CI_ZFB_FT_USER_PRO_SIGNINFO.count()
F_CI_ZFB_FT_USER_PRO_SIGNINFO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CI_ZFB_FT_USER_PRO_SIGNINFO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_ZFB_FT_USER_PRO_SIGNINFO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_ZFB_FT_USER_PRO_SIGNINFO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
