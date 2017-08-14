#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_DP_CBOD_SAACNAMT_I').setMaster(sys.argv[2])
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
#----------------------------------------------业务逻辑开始------------------------
O_DP_CBOD_SAACNAMT = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_SAACNAMT/*')
O_DP_CBOD_SAACNAMT.registerTempTable("O_DP_CBOD_SAACNAMT")

#任务[12] 001-01::
V_STEP = V_STEP + 1
if (V_MONTH not in('3','6','9','12')) and V_DAY <> '21':
	sql = """
	 SELECT ETLDT                   AS ETLDT 
		   ,FK_SAACN_KEY            AS FK_SAACN_KEY 
		   ,SA_CURR_COD             AS SA_CURR_COD 
		   ,SA_CURR_IDEN            AS SA_CURR_IDEN 
		   ,SA_FRZ_AMT              AS SA_FRZ_AMT 
		   ,SA_ACLG                 AS SA_ACLG 
		   ,SA_COM_FLG              AS SA_COM_FLG 
		   ,SA_SIGN_PDT             AS SA_SIGN_PDT 
		   ,SA_DDP_PDT              AS SA_DDP_PDT 
		   ,SA_DDP_ACCT_STS         AS SA_DDP_ACCT_STS 
		   ,SA_LTM_TX_DT            AS SA_LTM_TX_DT 
		   ,SA_OD_TM                AS SA_OD_TM 
		   ,SA_OD_PDT               AS SA_OD_PDT 
		   ,SA_OD_AMT               AS SA_OD_AMT 
		   ,SA_OD_DAYS_N            AS SA_OD_DAYS_N 
		   ,SA_OD_INT               AS SA_OD_INT 
		   ,SA_ACCT_BAL             AS SA_ACCT_BAL 
		   ,SA_SVC                  AS SA_SVC 
		   ,SA_INTR_COD             AS SA_INTR_COD 
		   ,SA_INTR                 AS SA_INTR 
		   ,SA_FLTR_FVR_SIGN        AS SA_FLTR_FVR_SIGN 
		   ,SA_FLTR_FVR             AS SA_FLTR_FVR 
		   ,SA_SLEEP_STS            AS SA_SLEEP_STS 
		   ,SA_BELONG_INSTN_COD     AS SA_BELONG_INSTN_COD 
		   ,SA_FRZ_STS              AS SA_FRZ_STS 
		   ,SA_STP_STS              AS SA_STP_STS 
		   ,SA_PDP_CODE             AS SA_PDP_CODE 
		   ,FR_ID                   AS FR_ID 
		   ,V_DT                    AS ODS_ST_DATE 
		   ,'CEN'                   AS ODS_SYS_ID 
	   FROM (SELECT A.*,ROW_NUMBER() OVER(PARTITION BY A.FR_ID,A.FK_SAACN_KEY ORDER BY A.FK_SAACN_KEY ) RN
			   FROM O_DP_CBOD_SAACNAMT A) A --活存资金档 
	  WHERE RN = '1'                                                 
	"""

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	F_DP_CBOD_SAACNAMT_INNTMP1 = sqlContext.sql(sql)
	F_DP_CBOD_SAACNAMT_INNTMP1.registerTempTable("F_DP_CBOD_SAACNAMT_INNTMP1")

	F_DP_CBOD_SAACNAMT = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNAMT_BK/'+V_DT_LD+'.parquet')
	F_DP_CBOD_SAACNAMT.registerTempTable("F_DP_CBOD_SAACNAMT")
	sql = """
	 SELECT DST.ETLDT                                               --平台日期:src.ETLDT
		   ,DST.FK_SAACN_KEY                                       --账号:src.FK_SAACN_KEY
		   ,DST.SA_CURR_COD                                        --币别:src.SA_CURR_COD
		   ,DST.SA_CURR_IDEN                                       --钞汇鉴别:src.SA_CURR_IDEN
		   ,DST.SA_FRZ_AMT                                         --冻结金额:src.SA_FRZ_AMT
		   ,DST.SA_ACLG                                            --会计科目:src.SA_ACLG
		   ,DST.SA_COM_FLG                                         --账户公用标志:src.SA_COM_FLG
		   ,DST.SA_SIGN_PDT                                        --大额积数:src.SA_SIGN_PDT
		   ,DST.SA_DDP_PDT                                         --活存积数:src.SA_DDP_PDT
		   ,DST.SA_DDP_ACCT_STS                                    --活存帐户状态:src.SA_DDP_ACCT_STS
		   ,DST.SA_LTM_TX_DT                                       --上次交易日期:src.SA_LTM_TX_DT
		   ,DST.SA_OD_TM                                           --透支次数:src.SA_OD_TM
		   ,DST.SA_OD_PDT                                          --透支积数:src.SA_OD_PDT
		   ,DST.SA_OD_AMT                                          --透支金额:src.SA_OD_AMT
		   ,DST.SA_OD_DAYS_N                                       --透支天数:src.SA_OD_DAYS_N
		   ,DST.SA_OD_INT                                          --透支息:src.SA_OD_INT
		   ,DST.SA_ACCT_BAL                                        --帐户余额:src.SA_ACCT_BAL
		   ,DST.SA_SVC                                             --手续费:src.SA_SVC
		   ,DST.SA_INTR_COD                                        --利率依据:src.SA_INTR_COD
		   ,DST.SA_INTR                                            --利率:src.SA_INTR
		   ,DST.SA_FLTR_FVR_SIGN                                   --浮动利率加减码符号位:src.SA_FLTR_FVR_SIGN
		   ,DST.SA_FLTR_FVR                                        --浮动利率加减码:src.SA_FLTR_FVR
		   ,DST.SA_SLEEP_STS                                       --睡眠状态:src.SA_SLEEP_STS
		   ,DST.SA_BELONG_INSTN_COD                                --账户归属机构:src.SA_BELONG_INSTN_COD
		   ,DST.SA_FRZ_STS                                         --冻结状态:src.SA_FRZ_STS
		   ,DST.SA_STP_STS                                         --止付状态:src.SA_STP_STS
		   ,DST.SA_PDP_CODE                                        --产品码:src.SA_PDP_CODE
		   ,DST.FR_ID                                              --法人号:src.FR_ID
		   ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
		   ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
	   FROM F_DP_CBOD_SAACNAMT DST 
	   LEFT JOIN F_DP_CBOD_SAACNAMT_INNTMP1 SRC 
		 ON SRC.FK_SAACN_KEY        = DST.FK_SAACN_KEY 
		AND SRC.SA_CURR_COD         = DST.SA_CURR_COD 
		AND SRC.SA_CURR_IDEN        = DST.SA_CURR_IDEN 
		AND SRC.FR_ID               = DST.FR_ID 
	  WHERE SRC.FK_SAACN_KEY IS NULL """

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	F_DP_CBOD_SAACNAMT_INNTMP2 = sqlContext.sql(sql)
	dfn="F_DP_CBOD_SAACNAMT/"+V_DT+".parquet"
	F_DP_CBOD_SAACNAMT_INNTMP2=F_DP_CBOD_SAACNAMT_INNTMP2.unionAll(F_DP_CBOD_SAACNAMT_INNTMP1)
	F_DP_CBOD_SAACNAMT_INNTMP1.cache()
	F_DP_CBOD_SAACNAMT_INNTMP2.cache()
	nrowsi = F_DP_CBOD_SAACNAMT_INNTMP1.count()
	nrowsa = F_DP_CBOD_SAACNAMT_INNTMP2.count() 
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_SAACNAMT/*")
	F_DP_CBOD_SAACNAMT_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
	F_DP_CBOD_SAACNAMT_INNTMP1.unpersist()
	F_DP_CBOD_SAACNAMT_INNTMP2.unpersist() 
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_SAACNAMT_BK/"+V_DT+".parquet")
	ret = os.system("hdfs dfs -cp /"+dbname+"/F_DP_CBOD_SAACNAMT/"+V_DT+".parquet  /"+dbname+"/F_DP_CBOD_SAACNAMT_BK/")
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_SAACNAMT lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
