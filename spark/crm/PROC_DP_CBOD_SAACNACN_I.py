#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_DP_CBOD_SAACNACN_I').setMaster(sys.argv[2])
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
O_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_SAACNACN/*')
O_DP_CBOD_SAACNACN.registerTempTable("O_DP_CBOD_SAACNACN")

#任务[12] 001-01::
V_STEP = V_STEP + 1
if (V_MONTH not in('3','6','9','12')) and V_DAY <> '21':
	sql = """
	 SELECT ETLDT                   AS ETLDT 
		   ,SA_ACCT_NO              AS SA_ACCT_NO 
		   ,SA_PSBK_PRT_NO          AS SA_PSBK_PRT_NO 
		   ,SA_PSBK_STS             AS SA_PSBK_STS 
		   ,SA_MBSTMT_FLG           AS SA_MBSTMT_FLG 
		   ,SA_OPAC_INSTN_NO        AS SA_OPAC_INSTN_NO 
		   ,SA_OPAC_AMT             AS SA_OPAC_AMT 
		   ,SA_OPAC_DT              AS SA_OPAC_DT 
		   ,SA_OPAC_PERM_NO         AS SA_OPAC_PERM_NO 
		   ,SA_CUST_NO              AS SA_CUST_NO 
		   ,SA_CUST_NAME            AS SA_CUST_NAME 
		   ,SA_CONNTR_NO            AS SA_CONNTR_NO 
		   ,SA_CACCT_INSTN_NO       AS SA_CACCT_INSTN_NO 
		   ,SA_CACCT_DT             AS SA_CACCT_DT 
		   ,SA_ACCT_CHAR            AS SA_ACCT_CHAR 
		   ,SA_DRW_TYP              AS SA_DRW_TYP 
		   ,SA_SEAL_STS             AS SA_SEAL_STS 
		   ,SA_OPAC_TLR_NO          AS SA_OPAC_TLR_NO 
		   ,SA_CACCT_TLR_NO         AS SA_CACCT_TLR_NO 
		   ,SA_INTC_FLG             AS SA_INTC_FLG 
		   ,SA_DEP_TYP              AS SA_DEP_TYP 
		   ,SA_CURR_TYP             AS SA_CURR_TYP 
		   ,SA_VIR_ACCT_FLG         AS SA_VIR_ACCT_FLG 
		   ,SA_GRP_SIGN_FLG         AS SA_GRP_SIGN_FLG 
		   ,SA_VIP_ACCT_FLG         AS SA_VIP_ACCT_FLG 
		   ,SA_RPT_ACCT_FLG         AS SA_RPT_ACCT_FLG 
		   ,SA_CARD_NO              AS SA_CARD_NO 
		   ,SA_ORG_DEP_TYPE         AS SA_ORG_DEP_TYPE 
		   ,SA_PDP_CODE             AS SA_PDP_CODE 
		   ,SA_DOC_TYP              AS SA_DOC_TYP 
		   ,SA_REF_CNT_N            AS SA_REF_CNT_N 
		   ,SA_BELONG_INSTN_COD     AS SA_BELONG_INSTN_COD 
		   ,SA_RISK_LVL             AS SA_RISK_LVL 
		   ,SA_INSP_FLG             AS SA_INSP_FLG 
		   ,FR_ID                   AS FR_ID 
		   ,V_DT                    AS ODS_ST_DATE 
		   ,'CEN'                   AS ODS_SYS_ID 
	   FROM (SELECT A.*,
					ROW_NUMBER() OVER(PARTITION BY A.FR_ID,A.SA_ACCT_NO ORDER BY SA_ACCT_NO ) RN 
			   FROM O_DP_CBOD_SAACNACN A) A    --活存主档
	  WHERE RN = '1' 
	"""

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	F_DP_CBOD_SAACNACN_INNTMP1 = sqlContext.sql(sql)
	F_DP_CBOD_SAACNACN_INNTMP1.registerTempTable("F_DP_CBOD_SAACNACN_INNTMP1")

	F_DP_CBOD_SAACNACN = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNACN_BK/'+V_DT_LD+'.parquet/*')
	F_DP_CBOD_SAACNACN.registerTempTable("F_DP_CBOD_SAACNACN")
	sql = """
	 SELECT DST.ETLDT                                               --平台日期:src.ETLDT
		   ,DST.SA_ACCT_NO                                         --帐号:src.SA_ACCT_NO
		   ,DST.SA_PSBK_PRT_NO                                     --存折印刷号:src.SA_PSBK_PRT_NO
		   ,DST.SA_PSBK_STS                                        --存折状态:src.SA_PSBK_STS
		   ,DST.SA_MBSTMT_FLG                                      --寄对帐单标志:src.SA_MBSTMT_FLG
		   ,DST.SA_OPAC_INSTN_NO                                   --开户机构号:src.SA_OPAC_INSTN_NO
		   ,DST.SA_OPAC_AMT                                        --开户金额:src.SA_OPAC_AMT
		   ,DST.SA_OPAC_DT                                         --开户日期:src.SA_OPAC_DT
		   ,DST.SA_OPAC_PERM_NO                                    --开户许可证号:src.SA_OPAC_PERM_NO
		   ,DST.SA_CUST_NO                                         --客户编号:src.SA_CUST_NO
		   ,DST.SA_CUST_NAME                                       --客户名称:src.SA_CUST_NAME
		   ,DST.SA_CONNTR_NO                                       --联系人编号:src.SA_CONNTR_NO
		   ,DST.SA_CACCT_INSTN_NO                                  --销户机构号:src.SA_CACCT_INSTN_NO
		   ,DST.SA_CACCT_DT                                        --销户日期:src.SA_CACCT_DT
		   ,DST.SA_ACCT_CHAR                                       --帐户性质:src.SA_ACCT_CHAR
		   ,DST.SA_DRW_TYP                                         --支取方式:src.SA_DRW_TYP
		   ,DST.SA_SEAL_STS                                        --印鉴状态:src.SA_SEAL_STS
		   ,DST.SA_OPAC_TLR_NO                                     --开户柜员号:src.SA_OPAC_TLR_NO
		   ,DST.SA_CACCT_TLR_NO                                    --销户柜员号:src.SA_CACCT_TLR_NO
		   ,DST.SA_INTC_FLG                                        --计息标志:src.SA_INTC_FLG
		   ,DST.SA_DEP_TYP                                         --存款种类:src.SA_DEP_TYP
		   ,DST.SA_CURR_TYP                                        --钞汇属性:src.SA_CURR_TYP
		   ,DST.SA_VIR_ACCT_FLG                                    --集团客户虚账户标志:src.SA_VIR_ACCT_FLG
		   ,DST.SA_GRP_SIGN_FLG                                    --集团实虚账户签约标志:src.SA_GRP_SIGN_FLG
		   ,DST.SA_VIP_ACCT_FLG                                    --重要帐户标志:src.SA_VIP_ACCT_FLG
		   ,DST.SA_RPT_ACCT_FLG                                    --是否未申报帐户:src.SA_RPT_ACCT_FLG
		   ,DST.SA_CARD_NO                                         --卡号:src.SA_CARD_NO
		   ,DST.SA_ORG_DEP_TYPE                                    --原存款种类:src.SA_ORG_DEP_TYPE
		   ,DST.SA_PDP_CODE                                        --产品代码:src.SA_PDP_CODE
		   ,DST.SA_DOC_TYP                                         --凭证种类:src.SA_DOC_TYP
		   ,DST.SA_REF_CNT_N                                       --关联账户数:src.SA_REF_CNT_N
		   ,DST.SA_BELONG_INSTN_COD                                --归属机构:src.SA_BELONG_INSTN_COD
		   ,DST.SA_RISK_LVL                                        --风险等级:src.SA_RISK_LVL
		   ,DST.SA_INSP_FLG                                        --年检标志:src.SA_INSP_FLG
		   ,DST.FR_ID                                              --法人号:src.FR_ID
		   ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
		   ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
	   FROM F_DP_CBOD_SAACNACN DST 
	   LEFT JOIN F_DP_CBOD_SAACNACN_INNTMP1 SRC 
		 ON SRC.SA_ACCT_NO          = DST.SA_ACCT_NO 
		AND SRC.FR_ID               = DST.FR_ID 
	  WHERE SRC.SA_ACCT_NO IS NULL """

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	F_DP_CBOD_SAACNACN_INNTMP2 = sqlContext.sql(sql)
	dfn="F_DP_CBOD_SAACNACN/"+V_DT+".parquet"
	F_DP_CBOD_SAACNACN_INNTMP2=F_DP_CBOD_SAACNACN_INNTMP2.unionAll(F_DP_CBOD_SAACNACN_INNTMP1)
	F_DP_CBOD_SAACNACN_INNTMP1.cache()
	F_DP_CBOD_SAACNACN_INNTMP2.cache()
	nrowsi = F_DP_CBOD_SAACNACN_INNTMP1.count()
	nrowsa = F_DP_CBOD_SAACNACN_INNTMP2.count() 
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_SAACNACN/*")
	F_DP_CBOD_SAACNACN_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite') 
	F_DP_CBOD_SAACNACN_INNTMP1.unpersist()
	F_DP_CBOD_SAACNACN_INNTMP2.unpersist()
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_SAACNACN_BK/"+V_DT+".parquet")
	ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_DP_CBOD_SAACNACN/"+V_DT+".parquet /"+dbname+"/F_DP_CBOD_SAACNACN_BK/")
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_SAACNACN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
