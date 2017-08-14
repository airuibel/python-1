#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_DP_CBOD_TDACNACN_I').setMaster(sys.argv[2])
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

#----------------------------------------------------------------------------------
V_YEAR = etl_date[0:4]#年
V_MONTH = str(int(etl_date[4:6])) #月
V_DAY = str(int(etl_date[6:8])); #日
#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
O_DP_CBOD_TDACNACN = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_TDACNACN/*')
O_DP_CBOD_TDACNACN.registerTempTable("O_DP_CBOD_TDACNACN")
#目标表
#F_DP_CBOD_TDACNACN 增改表，从bk读取前一天数据
F_DP_CBOD_TDACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_TDACNACN_BK/'+V_DT_LD+'.parquet')
F_DP_CBOD_TDACNACN.registerTempTable("F_DP_CBOD_TDACNACN")

#任务[21] 001-01::
V_STEP = V_STEP + 1

#MERGE 操作 dst:F_DP_CBOD_TDACNACN src:O_DP_CBOD_TDACNACN
sql="""
		select 
				  SRC.ETLDT
				, SRC.TD_TD_ACCT_NO
				, SRC.TD_CURR_COD
				, SRC.TD_VIP_ACCT_FLG
				, SRC.TD_TDP_INTR
				, SRC.TD_DEP_PRD_N
				, SRC.TD_DEP_DRW_CYCL_N
				, SRC.TD_DUE_DT
				, SRC.TD_FRZ_STS
				, SRC.TD_FLTR_FVR
				, SRC.TD_DL_FLG
				, SRC.TD_DL_DT
				, SRC.TD_CLSD_DT
				, SRC.TD_OPAC_TLR_NO
				, SRC.TD_OPAC_DT
				, SRC.TD_CUST_NO
				, SRC.TD_ACCUM_DEP_AMT
				, SRC.TD_ACCUM_PDT
				, SRC.TD_ACCUM_DRW_AMT
				, SRC.TD_ACTU_AMT
				, SRC.TD_CACCT_TLR_NO
				, SRC.TD_PRDS_DEP_PRD2_N
				, SRC.TD_DRW_TYP
				, SRC.TD_LHYPO_FLG
				, SRC.TD_TRND_INTC_DT
				, SRC.TD_ACCT_STS
				, SRC.TD_LST_TX_DT
				, SRC.TD_DEP_TYP
				, SRC.TD_OPAC_INSTN_NO
				, SRC.TD_CACCT_INSTN_NO
				, SRC.TD_ACCT_CHAR
				, SRC.TD_TDP_PSBK_FLG
				, SRC.TD_SEAL_STS
				, SRC.TD_MNG_FLG
				, SRC.TD_MNG_ACC_NO
				, SRC.TD_CUST_NAME
				, SRC.TD_DOC_TYP
				, SRC.TD_CURR_CHAR
				, SRC.TD_CURR_IDEN
				, SRC.TD_TX_DT
				, SRC.TD_CONNTR_NO
				, SRC.TD_LARGE_DEP_FLG
				, SRC.TD_FLTR_TYP
				, SRC.TD_FLTR_FVR_SIGN
				, SRC.TD_TDP_PSBK_PRT_NO
				, SRC.TD_PDP_CODE
				, SRC.TD_INTR_COD
				, SRC.TD_PART_FRZ_AMT
				, SRC.TD_BELONG_INSTN_COD
				, SRC.FR_ID
				, V_DT
				, 'CEN'
				, SRC.TD_PSBK_NO_N
				, SRC.TD_ACC1_NO
		from O_DP_CBOD_TDACNACN SRC 
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_TDACNACN_INNER1 = sqlContext.sql(sql)
F_DP_CBOD_TDACNACN_INNER1.registerTempTable("F_DP_CBOD_TDACNACN_INNER1")
sql="""
		select 
				  DST.ETLDT
				, DST.TD_TD_ACCT_NO
				, DST.TD_CURR_COD
				, DST.TD_VIP_ACCT_FLG
				, DST.TD_TDP_INTR
				, DST.TD_DEP_PRD_N
				, DST.TD_DEP_DRW_CYCL_N
				, DST.TD_DUE_DT
				, DST.TD_FRZ_STS
				, DST.TD_FLTR_FVR
				, DST.TD_DL_FLG
				, DST.TD_DL_DT
				, DST.TD_CLSD_DT
				, DST.TD_OPAC_TLR_NO
				, DST.TD_OPAC_DT
				, DST.TD_CUST_NO
				, DST.TD_ACCUM_DEP_AMT
				, DST.TD_ACCUM_PDT
				, DST.TD_ACCUM_DRW_AMT
				, DST.TD_ACTU_AMT
				, DST.TD_CACCT_TLR_NO
				, DST.TD_PRDS_DEP_PRD2_N
				, DST.TD_DRW_TYP
				, DST.TD_LHYPO_FLG
				, DST.TD_TRND_INTC_DT
				, DST.TD_ACCT_STS
				, DST.TD_LST_TX_DT
				, DST.TD_DEP_TYP
				, DST.TD_OPAC_INSTN_NO
				, DST.TD_CACCT_INSTN_NO
				, DST.TD_ACCT_CHAR
				, DST.TD_TDP_PSBK_FLG
				, DST.TD_SEAL_STS
				, DST.TD_MNG_FLG
				, DST.TD_MNG_ACC_NO
				, DST.TD_CUST_NAME
				, DST.TD_DOC_TYP
				, DST.TD_CURR_CHAR
				, DST.TD_CURR_IDEN
				, DST.TD_TX_DT
				, DST.TD_CONNTR_NO
				, DST.TD_LARGE_DEP_FLG
				, DST.TD_FLTR_TYP
				, DST.TD_FLTR_FVR_SIGN
				, DST.TD_TDP_PSBK_PRT_NO
				, DST.TD_PDP_CODE
				, DST.TD_INTR_COD
				, DST.TD_PART_FRZ_AMT
				, DST.TD_BELONG_INSTN_COD
				, DST.FR_ID
				, DST.ODS_ST_DATE
				, DST.ODS_SYS_ID
				, DST.TD_PSBK_NO_N
				, DST.TD_ACC1_NO
		from F_DP_CBOD_TDACNACN DST 
			 LEFT JOIN 	
			 F_DP_CBOD_TDACNACN_INNER1 SRC ON SRC.TD_TD_ACCT_NO = DST.TD_TD_ACCT_NO AND SRC.FR_ID=DST.FR_ID
			 WHERE SRC.TD_TD_ACCT_NO IS NULL
"""
sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_TDACNACN_INNER2 = sqlContext.sql(sql)
F_DP_CBOD_TDACNACN_INNER2 = F_DP_CBOD_TDACNACN_INNER2.unionAll(F_DP_CBOD_TDACNACN_INNER1)
dfn="F_DP_CBOD_TDACNACN/"+V_DT+".parquet"
F_DP_CBOD_TDACNACN_INNER2.cache()
nrows = F_DP_CBOD_TDACNACN_INNER2.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_TDACNACN/"+V_DT+".parquet")
F_DP_CBOD_TDACNACN_INNER2.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_DP_CBOD_TDACNACN_INNER1.unpersist()
F_DP_CBOD_TDACNACN_INNER2.unpersist()
#增改表，备份到BK
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_TDACNACN_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_DP_CBOD_TDACNACN/"+V_DT+".parquet /"+dbname+"/F_DP_CBOD_TDACNACN_BK/"+V_DT+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

