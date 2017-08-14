#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CMSVCSVC').setMaster(sys.argv[2])
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
#----------来源表---------------
O_NI_CBOD_CMSVCSVC = sqlContext.read.parquet(hdfs+'/O_NI_CBOD_CMSVCSVC/*')
O_NI_CBOD_CMSVCSVC.registerTempTable("O_NI_CBOD_CMSVCSVC")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CM_CHRG_COD             AS CM_CHRG_COD 
       ,CM_OPUN_COD             AS CM_OPUN_COD 
       ,CM_CURR_COD             AS CM_CURR_COD 
       ,CM_CHRG_NAME            AS CM_CHRG_NAME 
       ,CM_CHRG_TYP             AS CM_CHRG_TYP 
       ,CM_CHRG_ACLG            AS CM_CHRG_ACLG 
       ,CM_CHRG_ACLG_NO         AS CM_CHRG_ACLG_NO 
       ,CM_CHRG_SIGN_FLG        AS CM_CHRG_SIGN_FLG 
       ,CM_CAL_TYP              AS CM_CAL_TYP 
       ,CM_LOW_BAL_HDL_TYP      AS CM_LOW_BAL_HDL_TYP 
       ,CM_DEFER_FLG            AS CM_DEFER_FLG 
       ,CM_DEFER_COD            AS CM_DEFER_COD 
       ,CM_DSCT_FLG             AS CM_DSCT_FLG 
       ,CM_DSCT_TYP             AS CM_DSCT_TYP 
       ,CM_VALD_DAT             AS CM_VALD_DAT 
       ,CM_SEGMT_FLG            AS CM_SEGMT_FLG 
       ,CM_BRKT_TYP             AS CM_BRKT_TYP 
       ,CM_ACM_CYC_UNT          AS CM_ACM_CYC_UNT 
       ,CM_ACM_CYC_VAL          AS CM_ACM_CYC_VAL 
       ,CM_ACM_CYC_STR_FLG      AS CM_ACM_CYC_STR_FLG 
       ,CM_ACM_CYC_STR_DAT      AS CM_ACM_CYC_STR_DAT 
       ,CM_BRKT_FEE_TYP_1       AS CM_BRKT_FEE_TYP_1 
       ,CM_BRKT_LOW_VAL_1       AS CM_BRKT_LOW_VAL_1 
       ,CM_BRKT_LOW_CMPSGN_1    AS CM_BRKT_LOW_CMPSGN_1 
       ,CM_BRKT_HGH_CMPSGN_1    AS CM_BRKT_HGH_CMPSGN_1 
       ,CM_BRKT_HGH_VAL_1       AS CM_BRKT_HGH_VAL_1 
       ,CM_BRKT_FEE_RAT_1       AS CM_BRKT_FEE_RAT_1 
       ,CM_BRKT_FIX_AMT_1       AS CM_BRKT_FIX_AMT_1 
       ,CM_BRKT_BSE_AMT_1       AS CM_BRKT_BSE_AMT_1 
       ,CM_LOWEST_FEE_AMT_1     AS CM_LOWEST_FEE_AMT_1 
       ,CM_HIGEST_FEE_AMT_1     AS CM_HIGEST_FEE_AMT_1 
       ,CM_BRKT_FEE_TYP_2       AS CM_BRKT_FEE_TYP_2 
       ,CM_BRKT_LOW_VAL_2       AS CM_BRKT_LOW_VAL_2 
       ,CM_BRKT_LOW_CMPSGN_2    AS CM_BRKT_LOW_CMPSGN_2 
       ,CM_BRKT_HGH_CMPSGN_2    AS CM_BRKT_HGH_CMPSGN_2 
       ,CM_BRKT_HGH_VAL_2       AS CM_BRKT_HGH_VAL_2 
       ,CM_BRKT_FEE_RAT_2       AS CM_BRKT_FEE_RAT_2 
       ,CM_BRKT_FIX_AMT_2       AS CM_BRKT_FIX_AMT_2 
       ,CM_BRKT_BSE_AMT_2       AS CM_BRKT_BSE_AMT_2 
       ,CM_LOWEST_FEE_AMT_2     AS CM_LOWEST_FEE_AMT_2 
       ,CM_HIGEST_FEE_AMT_2     AS CM_HIGEST_FEE_AMT_2 
       ,CM_BRKT_FEE_TYP_3       AS CM_BRKT_FEE_TYP_3 
       ,CM_BRKT_LOW_VAL_3       AS CM_BRKT_LOW_VAL_3 
       ,CM_BRKT_LOW_CMPSGN_3    AS CM_BRKT_LOW_CMPSGN_3 
       ,CM_BRKT_HGH_CMPSGN_3    AS CM_BRKT_HGH_CMPSGN_3 
       ,CM_BRKT_HGH_VAL_3       AS CM_BRKT_HGH_VAL_3 
       ,CM_BRKT_FEE_RAT_3       AS CM_BRKT_FEE_RAT_3 
       ,CM_BRKT_FIX_AMT_3       AS CM_BRKT_FIX_AMT_3 
       ,CM_BRKT_BSE_AMT_3       AS CM_BRKT_BSE_AMT_3 
       ,CM_LOWEST_FEE_AMT_3     AS CM_LOWEST_FEE_AMT_3 
       ,CM_HIGEST_FEE_AMT_3     AS CM_HIGEST_FEE_AMT_3 
       ,CM_BRKT_FEE_TYP_4       AS CM_BRKT_FEE_TYP_4 
       ,CM_BRKT_LOW_VAL_4       AS CM_BRKT_LOW_VAL_4 
       ,CM_BRKT_LOW_CMPSGN_4    AS CM_BRKT_LOW_CMPSGN_4 
       ,CM_BRKT_HGH_CMPSGN_4    AS CM_BRKT_HGH_CMPSGN_4 
       ,CM_BRKT_HGH_VAL_4       AS CM_BRKT_HGH_VAL_4 
       ,CM_BRKT_FEE_RAT_4       AS CM_BRKT_FEE_RAT_4 
       ,CM_BRKT_FIX_AMT_4       AS CM_BRKT_FIX_AMT_4 
       ,CM_BRKT_BSE_AMT_4       AS CM_BRKT_BSE_AMT_4 
       ,CM_LOWEST_FEE_AMT_4     AS CM_LOWEST_FEE_AMT_4 
       ,CM_HIGEST_FEE_AMT_4     AS CM_HIGEST_FEE_AMT_4 
       ,CM_BRKT_FEE_TYP_5       AS CM_BRKT_FEE_TYP_5 
       ,CM_BRKT_LOW_VAL_5       AS CM_BRKT_LOW_VAL_5 
       ,CM_BRKT_LOW_CMPSGN_5    AS CM_BRKT_LOW_CMPSGN_5 
       ,CM_BRKT_HGH_CMPSGN_5    AS CM_BRKT_HGH_CMPSGN_5 
       ,CM_BRKT_HGH_VAL_5       AS CM_BRKT_HGH_VAL_5 
       ,CM_BRKT_FEE_RAT_5       AS CM_BRKT_FEE_RAT_5 
       ,CM_BRKT_FIX_AMT_5       AS CM_BRKT_FIX_AMT_5 
       ,CM_BRKT_BSE_AMT_5       AS CM_BRKT_BSE_AMT_5 
       ,CM_LOWEST_FEE_AMT_5     AS CM_LOWEST_FEE_AMT_5 
       ,CM_HIGEST_FEE_AMT_5     AS CM_HIGEST_FEE_AMT_5 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_NI_CBOD_CMSVCSVC A                                   --费种代码表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_NI_CBOD_CMSVCSVC = sqlContext.sql(sql)
F_NI_CBOD_CMSVCSVC.registerTempTable("F_NI_CBOD_CMSVCSVC")
dfn="F_NI_CBOD_CMSVCSVC/"+V_DT+".parquet"
F_NI_CBOD_CMSVCSVC.cache()
nrows = F_NI_CBOD_CMSVCSVC.count()
F_NI_CBOD_CMSVCSVC.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_NI_CBOD_CMSVCSVC.unpersist()
O_NI_CBOD_CMSVCSVC.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_NI_CBOD_CMSVCSVC/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_NI_CBOD_CMSVCSVC lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
