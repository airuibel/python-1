#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CRE_EVENT').setMaster(sys.argv[2])
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

#删除当天的
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_EVENT/"+V_DT+".parquet")

#----------来源表---------------
O_TX_EVENT = sqlContext.read.parquet(hdfs+'/O_TX_EVENT/*')
O_TX_EVENT.registerTempTable("O_TX_EVENT")

#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT BANK                    AS BANK 
       ,TRANS_TYPE              AS TRANS_TYPE 
       ,DESC_NAR                AS DESC_NAR 
       ,CARD_NBR                AS CARD_NBR 
       ,BILL_AMT                AS BILL_AMT 
       ,BILL_AMT_S              AS BILL_AMT_S 
       ,MER_CAT_CD              AS MER_CAT_CD 
       ,MERCHANT                AS MERCHANT 
       ,TERMINALI               AS TERMINALI 
       ,RETRV_REF               AS RETRV_REF 
       ,AUTH_CODE               AS AUTH_CODE 
       ,SETTLE_DAY              AS SETTLE_DAY 
       ,CURRNCY_CD              AS CURRNCY_CD 
       ,REV_IND                 AS REV_IND 
       ,DES_LINE1               AS DES_LINE1 
       ,DES_LINE2               AS DES_LINE2 
       ,INP_DATE                AS INP_DATE 
       ,VAL_DATE                AS VAL_DATE 
       ,PUR_DATE                AS PUR_DATE 
       ,PUR_TIME                AS PUR_TIME 
       ,XTRANNO                 AS XTRANNO 
       ,BRNO                    AS BRNO 
       ,EMPNO                   AS EMPNO 
       ,OTH_AMT                 AS OTH_AMT 
       ,MONTH_NBR               AS MONTH_NBR 
       ,ORGN_CURR               AS ORGN_CURR 
       ,ORGN_AMT                AS ORGN_AMT 
       ,TRANS_SRC               AS TRANS_SRC 
       ,SLFBSM                  AS SLFBSM 
       ,SEC_LVL                 AS SEC_LVL 
       ,CB_RIGHTS               AS CB_RIGHTS 
       ,MERCH_SEQ               AS MERCH_SEQ 
       ,EXPAY_AMT               AS EXPAY_AMT 
       ,POINT_POST              AS POINT_POST 
       ,PURCHSE_ID              AS PURCHSE_ID 
       ,MCPOS_DATA              AS MCPOS_DATA 
       ,MERCH_STAT              AS MERCH_STAT 
       ,ORG_NO                  AS ORG_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CRE'                   AS ODS_SYS_ID 
   FROM O_TX_EVENT A                                           --客户交易数据
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_EVENT = sqlContext.sql(sql)
F_TX_EVENT.registerTempTable("F_TX_EVENT")
dfn="F_TX_EVENT/"+V_DT+".parquet"
F_TX_EVENT.cache()
nrows = F_TX_EVENT.count()
F_TX_EVENT.write.save(path=hdfs + '/' + dfn, mode='append')
F_TX_EVENT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_EVENT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
