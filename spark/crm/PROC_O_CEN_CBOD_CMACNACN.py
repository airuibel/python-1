#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMACNACN').setMaster(sys.argv[2])
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

O_DP_CBOD_CMACNACN = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_CMACNACN/*')
O_DP_CBOD_CMACNACN.registerTempTable("O_DP_CBOD_CMACNACN")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_DP_CBOD_CMACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_CMACNACN/*')
F_DP_CBOD_CMACNACN.registerTempTable("F_DP_CBOD_CMACNACN")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_BANK_CODE            AS CM_BANK_CODE 
       ,CM_ACCT_NO              AS CM_ACCT_NO 
       ,CM_SYS_ACCT_NO          AS CM_SYS_ACCT_NO 
       ,CM_BELONGTO_BRH         AS CM_BELONGTO_BRH 
       ,CM_BBN_CODE             AS CM_BBN_CODE 
       ,CM_PD_FAMILY_COD        AS CM_PD_FAMILY_COD 
       ,CM_ACCT_TYP             AS CM_ACCT_TYP 
       ,CM_ACCT_NO_TYPE         AS CM_ACCT_NO_TYPE 
       ,CM_TD_SA_CR_FLG         AS CM_TD_SA_CR_FLG 
       ,CM_PUB_PRIVATE_FLAG     AS CM_PUB_PRIVATE_FLAG 
       ,CM_ANI_FLG              AS CM_ANI_FLG 
       ,CM_ACCT_FLAG_3          AS CM_ACCT_FLAG_3 
       ,CM_ACCT_FLAG_4          AS CM_ACCT_FLAG_4 
       ,CM_ACCT_FEA_FLG         AS CM_ACCT_FEA_FLG 
       ,CM_ACCT_SATD_FEA_FLG    AS CM_ACCT_SATD_FEA_FLG 
       ,CM_DEPO_BUSN_KIND       AS CM_DEPO_BUSN_KIND 
       ,CM_LN_OLD_SYS_FLG       AS CM_LN_OLD_SYS_FLG 
       ,CM_ACCT_TRANS_TYP       AS CM_ACCT_TRANS_TYP 
       ,CM_TLR_GRP              AS CM_TLR_GRP 
       ,CM_DB_PART_ID           AS CM_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_DP_CBOD_CMACNACN A                                   --帐号属性档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_CMACNACN_INNTMP1 = sqlContext.sql(sql)
F_DP_CBOD_CMACNACN_INNTMP1.registerTempTable("F_DP_CBOD_CMACNACN_INNTMP1")

#F_DP_CBOD_CMACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_CMACNACN/*')
#F_DP_CBOD_CMACNACN.registerTempTable("F_DP_CBOD_CMACNACN")
sql = """
 SELECT DST.ETLDT                                               --源系统日期:src.ETLDT
       ,DST.CM_BANK_CODE                                       --银行码:src.CM_BANK_CODE
       ,DST.CM_ACCT_NO                                         --帐号:src.CM_ACCT_NO
       ,DST.CM_SYS_ACCT_NO                                     --系统帐号2:src.CM_SYS_ACCT_NO
       ,DST.CM_BELONGTO_BRH                                    --隶属机构代码:src.CM_BELONGTO_BRH
       ,DST.CM_BBN_CODE                                        --子系统代码:src.CM_BBN_CODE
       ,DST.CM_PD_FAMILY_COD                                   --产品家族:src.CM_PD_FAMILY_COD
       ,DST.CM_ACCT_TYP                                        --帐别2:src.CM_ACCT_TYP
       ,DST.CM_ACCT_NO_TYPE                                    --帐号类别(2位):src.CM_ACCT_NO_TYPE
       ,DST.CM_TD_SA_CR_FLG                                    --定期/活期/卡号标志:src.CM_TD_SA_CR_FLG
       ,DST.CM_PUB_PRIVATE_FLAG                                --对公/对私标志(属性档):src.CM_PUB_PRIVATE_FLAG
       ,DST.CM_ANI_FLG                                         --帐户性质(属性档):src.CM_ANI_FLG
       ,DST.CM_ACCT_FLAG_3                                     --标识三:src.CM_ACCT_FLAG_3
       ,DST.CM_ACCT_FLAG_4                                     --标识四:src.CM_ACCT_FLAG_4
       ,DST.CM_ACCT_FEA_FLG                                    --是否一本通标志:src.CM_ACCT_FEA_FLG
       ,DST.CM_ACCT_SATD_FEA_FLG                               --是否定活一本通标志:src.CM_ACCT_SATD_FEA_FLG
       ,DST.CM_DEPO_BUSN_KIND                                  --储种(KIND):src.CM_DEPO_BUSN_KIND
       ,DST.CM_LN_OLD_SYS_FLG                                  --老系统标志:src.CM_LN_OLD_SYS_FLG
       ,DST.CM_ACCT_TRANS_TYP                                  --帐号转换规则类别-暂不使:src.CM_ACCT_TRANS_TYP
       ,DST.CM_TLR_GRP                                         --柜组:src.CM_TLR_GRP
       ,DST.CM_DB_PART_ID                                      --分区键:src.CM_DB_PART_ID
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_DP_CBOD_CMACNACN DST 
   LEFT JOIN F_DP_CBOD_CMACNACN_INNTMP1 SRC 
     ON SRC.CM_BANK_CODE        = DST.CM_BANK_CODE 
    AND SRC.CM_ACCT_NO          = DST.CM_ACCT_NO 
  WHERE SRC.CM_BANK_CODE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_CMACNACN_INNTMP2 = sqlContext.sql(sql)
dfn="F_DP_CBOD_CMACNACN/"+V_DT+".parquet"
F_DP_CBOD_CMACNACN_INNTMP2=F_DP_CBOD_CMACNACN_INNTMP2.unionAll(F_DP_CBOD_CMACNACN_INNTMP1)
F_DP_CBOD_CMACNACN_INNTMP1.cache()
F_DP_CBOD_CMACNACN_INNTMP2.cache()
nrowsi = F_DP_CBOD_CMACNACN_INNTMP1.count()
nrowsa = F_DP_CBOD_CMACNACN_INNTMP2.count()
F_DP_CBOD_CMACNACN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_DP_CBOD_CMACNACN_INNTMP1.unpersist()
F_DP_CBOD_CMACNACN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_CMACNACN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_DP_CBOD_CMACNACN/"+V_DT_LD+".parquet /"+dbname+"/F_DP_CBOD_CMACNACN_BK/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_CMACNACN/"+V_DT_LD+".parquet")