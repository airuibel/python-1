#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_DP_CBOD_SAACNACN_A').setMaster(sys.argv[2])
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
O_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_SAACNACN/*')
O_DP_CBOD_SAACNACN.registerTempTable("O_DP_CBOD_SAACNACN")

#目标表
#F_DP_CBOD_SAACNACN 全量表

#任务[21] 001-01::
V_STEP = V_STEP + 1

if (V_MONTH in('3','6','9','12')) and V_DAY == '21':
	sql="""
			select 
					 ETLDT
					,SA_ACCT_NO
					,SA_PSBK_PRT_NO
					,SA_PSBK_STS
					,SA_MBSTMT_FLG
					,SA_OPAC_INSTN_NO
					,SA_OPAC_AMT
					,SA_OPAC_DT
					,SA_OPAC_PERM_NO
					,SA_CUST_NO
					,SA_CUST_NAME
					,SA_CONNTR_NO
					,SA_CACCT_INSTN_NO
					,SA_CACCT_DT
					,SA_ACCT_CHAR
					,SA_DRW_TYP
					,SA_SEAL_STS
					,SA_OPAC_TLR_NO
					,SA_CACCT_TLR_NO
					,SA_INTC_FLG
					,SA_DEP_TYP
					,SA_CURR_TYP
					,SA_VIR_ACCT_FLG
					,SA_GRP_SIGN_FLG
					,SA_VIP_ACCT_FLG
					,SA_RPT_ACCT_FLG
					,SA_CARD_NO
					,SA_ORG_DEP_TYPE
					,SA_PDP_CODE
					,SA_DOC_TYP
					,SA_REF_CNT_N
					,SA_BELONG_INSTN_COD
					,SA_RISK_LVL
					,SA_INSP_FLG
					,FR_ID
					,ODS_ST_DATE
					,ODS_SYS_ID
			from O_DP_CBOD_SAACNACN T1	
	"""
	print(sql)
	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	F_DP_CBOD_SAACNACN = sqlContext.sql(sql)
	dfn="F_DP_CBOD_SAACNACN/"+V_DT+".parquet"
	F_DP_CBOD_SAACNACN.cache()
	nrows = F_DP_CBOD_SAACNACN.count()
	F_DP_CBOD_SAACNACN.write.save(path=hdfs + '/' + dfn, mode='overwrite')
	F_DP_CBOD_SAACNACN.unpersist()
	#全量表需要删除前一天文件
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_SAACNACN/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

