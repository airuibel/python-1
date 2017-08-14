#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CRCRDCOM').setMaster(sys.argv[2])
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

O_DP_CBOD_CRCRDCOM = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_CRCRDCOM/*')
O_DP_CBOD_CRCRDCOM.registerTempTable("O_DP_CBOD_CRCRDCOM")

#任务[12] 001-01::
V_STEP = V_STEP + 1


ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_CRCRDCOM/*.parquet")

F_DP_CBOD_CRCRDCOM = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_CRCRDCOM_BK/'+V_DT_LD+'.parquet')
F_DP_CBOD_CRCRDCOM.registerTempTable("F_DP_CBOD_CRCRDCOM")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,FK_CRCRD_KEY            AS FK_CRCRD_KEY 
       ,CR_RSV_ACCT_NO          AS CR_RSV_ACCT_NO 
       ,CR_BELONGTO_BRH         AS CR_BELONGTO_BRH 
       ,CR_CURR_COD             AS CR_CURR_COD 
       ,CR_GUAR_MARGIN_STS      AS CR_GUAR_MARGIN_STS 
       ,CR_UDTK_TYP             AS CR_UDTK_TYP 
       ,CR_FSTM_CNSPN_FLG       AS CR_FSTM_CNSPN_FLG 
       ,CR_LTM_TX_DT            AS CR_LTM_TX_DT 
       ,CR_CRNM_TB_DRW_TM       AS CR_CRNM_TB_DRW_TM 
       ,CR_CUSN_MN_CNSPN_ITEM   AS CR_CUSN_MN_CNSPN_ITEM 
       ,CR_PDAY_CHDRW_TM        AS CR_PDAY_CHDRW_TM 
       ,CR_CRNT_DT_DRW_AMT      AS CR_CRNT_DT_DRW_AMT 
       ,CR_CRNT_DT_CNSPN_TM     AS CR_CRNT_DT_CNSPN_TM 
       ,CR_CRNT_DT_CNSPN_AMT    AS CR_CRNT_DT_CNSPN_AMT 
       ,CR_CRNT_DT_AUTH_TM      AS CR_CRNT_DT_AUTH_TM 
       ,CR_CUSN_MN_CNSPN_LOTP   AS CR_CUSN_MN_CNSPN_LOTP 
       ,CR_CNSPN_ADPT           AS CR_CNSPN_ADPT 
       ,CR_NO_AWARD_BASE_LOTP   AS CR_NO_AWARD_BASE_LOTP 
       ,CR_AWARD_BASE_LOTP      AS CR_AWARD_BASE_LOTP 
       ,CR_AWARD_LOTP           AS CR_AWARD_LOTP 
       ,CR_PRVSPRD_LOTP         AS CR_PRVSPRD_LOTP 
       ,CR_USE_LOTP             AS CR_USE_LOTP 
       ,CR_CD_ACCT_NO           AS CR_CD_ACCT_NO 
       ,CR_CRNT_DT_TRN_AMT      AS CR_CRNT_DT_TRN_AMT 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_DP_CBOD_CRCRDCOM A                                   --卡公用档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_CRCRDCOM_INNTMP1 = sqlContext.sql(sql)
F_DP_CBOD_CRCRDCOM_INNTMP1.registerTempTable("F_DP_CBOD_CRCRDCOM_INNTMP1")

#F_DP_CBOD_CRCRDCOM = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_CRCRDCOM/*')
#F_DP_CBOD_CRCRDCOM.registerTempTable("F_DP_CBOD_CRCRDCOM")
sql = """
 SELECT DST.ETLDT                                               --平台日期:src.ETLDT
       ,DST.FK_CRCRD_KEY                                       --FK_CRCRD_KEY:src.FK_CRCRD_KEY
       ,DST.CR_RSV_ACCT_NO                                     --备付金帐号:src.CR_RSV_ACCT_NO
       ,DST.CR_BELONGTO_BRH                                    --隶属机构代码:src.CR_BELONGTO_BRH
       ,DST.CR_CURR_COD                                        --币别:src.CR_CURR_COD
       ,DST.CR_GUAR_MARGIN_STS                                 --保证金状态:src.CR_GUAR_MARGIN_STS
       ,DST.CR_UDTK_TYP                                        --担保方法:src.CR_UDTK_TYP
       ,DST.CR_FSTM_CNSPN_FLG                                  --首次消费标志:src.CR_FSTM_CNSPN_FLG
       ,DST.CR_LTM_TX_DT                                       --上次交易日期:src.CR_LTM_TX_DT
       ,DST.CR_CRNM_TB_DRW_TM                                  --当月他行ATM取款次数:src.CR_CRNM_TB_DRW_TM
       ,DST.CR_CUSN_MN_CNSPN_ITEM                              --当月消费笔数:src.CR_CUSN_MN_CNSPN_ITEM
       ,DST.CR_PDAY_CHDRW_TM                                   --每天取现次数:src.CR_PDAY_CHDRW_TM
       ,DST.CR_CRNT_DT_DRW_AMT                                 --当日取款金额:src.CR_CRNT_DT_DRW_AMT
       ,DST.CR_CRNT_DT_CNSPN_TM                                --当日消费次数:src.CR_CRNT_DT_CNSPN_TM
       ,DST.CR_CRNT_DT_CNSPN_AMT                               --当日消费金额:src.CR_CRNT_DT_CNSPN_AMT
       ,DST.CR_CRNT_DT_AUTH_TM                                 --当日授权次数:src.CR_CRNT_DT_AUTH_TM
       ,DST.CR_CUSN_MN_CNSPN_LOTP                              --当月消费积分:src.CR_CUSN_MN_CNSPN_LOTP
       ,DST.CR_CNSPN_ADPT                                      --消费积分:src.CR_CNSPN_ADPT
       ,DST.CR_NO_AWARD_BASE_LOTP                              --未奖励基本积分:src.CR_NO_AWARD_BASE_LOTP
       ,DST.CR_AWARD_BASE_LOTP                                 --已奖励基本积分:src.CR_AWARD_BASE_LOTP
       ,DST.CR_AWARD_LOTP                                      --奖励积分:src.CR_AWARD_LOTP
       ,DST.CR_PRVSPRD_LOTP                                    --上期积分:src.CR_PRVSPRD_LOTP
       ,DST.CR_USE_LOTP                                        --已使用积分:src.CR_USE_LOTP
       ,DST.CR_CD_ACCT_NO                                      --信用帐号:src.CR_CD_ACCT_NO
       ,DST.CR_CRNT_DT_TRN_AMT                                 --当日转账金额:src.CR_CRNT_DT_TRN_AMT
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_DP_CBOD_CRCRDCOM DST 
   LEFT JOIN F_DP_CBOD_CRCRDCOM_INNTMP1 SRC 
     ON SRC.FK_CRCRD_KEY        = DST.FK_CRCRD_KEY 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.FK_CRCRD_KEY IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_CRCRDCOM_INNTMP2 = sqlContext.sql(sql)
dfn="F_DP_CBOD_CRCRDCOM/"+V_DT+".parquet"
F_DP_CBOD_CRCRDCOM_INNTMP2=F_DP_CBOD_CRCRDCOM_INNTMP2.unionAll(F_DP_CBOD_CRCRDCOM_INNTMP1)
F_DP_CBOD_CRCRDCOM_INNTMP1.cache()
F_DP_CBOD_CRCRDCOM_INNTMP2.cache()
nrowsi = F_DP_CBOD_CRCRDCOM_INNTMP1.count()
nrowsa = F_DP_CBOD_CRCRDCOM_INNTMP2.count()

F_DP_CBOD_CRCRDCOM_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_DP_CBOD_CRCRDCOM_INNTMP1.unpersist()
F_DP_CBOD_CRCRDCOM_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_CRCRDCOM lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_CRCRDCOM_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_DP_CBOD_CRCRDCOM/"+V_DT+".parquet /"+dbname+"/F_DP_CBOD_CRCRDCOM_BK/")
