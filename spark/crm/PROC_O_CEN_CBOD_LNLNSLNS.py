#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_LNLNSLNS').setMaster(sys.argv[2])
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

O_DP_CBOD_LNLNSLNS = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_LNLNSLNS/*')
O_DP_CBOD_LNLNSLNS.registerTempTable("O_DP_CBOD_LNLNSLNS")

#任务[12] 001-01::
V_STEP = V_STEP + 1

#读备份表
F_DP_CBOD_LNLNSLNS = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_LNLNSLNS_BK/'+V_DT_LD+'.parquet/*')
F_DP_CBOD_LNLNSLNS.registerTempTable("F_DP_CBOD_LNLNSLNS")

#F_DP_CBOD_LNLNSLNS = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_LNLNSLNS/*')
#F_DP_CBOD_LNLNSLNS.registerTempTable("F_DP_CBOD_LNLNSLNS")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,LN_LN_ACCT_NO           AS LN_LN_ACCT_NO 
       ,LN_CUST_NO              AS LN_CUST_NO 
       ,LN_CRLMT_NO             AS LN_CRLMT_NO 
       ,LN_PDP_CODE             AS LN_PDP_CODE 
       ,LN_BUSN_TYP             AS LN_BUSN_TYP 
       ,LN_ACCT_STS             AS LN_ACCT_STS 
       ,LN_VIP_ACCT_FLG         AS LN_VIP_ACCT_FLG 
       ,LN_CUST_TYP             AS LN_CUST_TYP 
       ,LN_CURR_COD             AS LN_CURR_COD 
       ,LN_CURR_IDEN            AS LN_CURR_IDEN 
       ,LN_FINBUSN              AS LN_FINBUSN 
       ,LN_LN_TYP               AS LN_LN_TYP 
       ,LN_HYPO_TYP             AS LN_HYPO_TYP 
       ,LN_NWLN_AMT_HYPO_AMT    AS LN_NWLN_AMT_HYPO_AMT 
       ,LN_TOTL_LN_AMT_HYPO_AMT AS LN_TOTL_LN_AMT_HYPO_AMT 
       ,LN_LN_BAL               AS LN_LN_BAL 
       ,LN_FRST_ALFD_DT_N       AS LN_FRST_ALFD_DT_N 
       ,LN_DUE_DT_N             AS LN_DUE_DT_N 
       ,LN_CLSD_DT_N            AS LN_CLSD_DT_N 
       ,LN_RFN_STY              AS LN_RFN_STY 
       ,LN_TOTL_PRD_N           AS LN_TOTL_PRD_N 
       ,LN_ARFN_SCHD_PR_N       AS LN_ARFN_SCHD_PR_N 
       ,LN_ARFN_SCHD_INT_N      AS LN_ARFN_SCHD_INT_N 
       ,LN_TNRNO_N              AS LN_TNRNO_N 
       ,LN_LN_MTHS_N            AS LN_LN_MTHS_N 
       ,LN_CLSD_INTC_TYP        AS LN_CLSD_INTC_TYP 
       ,LN_FRST_INTC_INTR       AS LN_FRST_INTC_INTR 
       ,LN_FRST_DELAY_INTR      AS LN_FRST_DELAY_INTR 
       ,LN_INTR_ADJ_STY         AS LN_INTR_ADJ_STY 
       ,LN_INTR_ADJ_CYCL        AS LN_INTR_ADJ_CYCL 
       ,LN_INTR_ADJ_STRT_DT_N   AS LN_INTR_ADJ_STRT_DT_N 
       ,LN_INTR_EFF_DT_N        AS LN_INTR_EFF_DT_N 
       ,LN_INTR_ACOR_STY        AS LN_INTR_ACOR_STY 
       ,LN_INTR_TYP             AS LN_INTR_TYP 
       ,LN_INTR_NEGO_SYMB       AS LN_INTR_NEGO_SYMB 
       ,LN_INTR_NEGO_RATE       AS LN_INTR_NEGO_RATE 
       ,LN_DLAY_INTR_ACOR_STY   AS LN_DLAY_INTR_ACOR_STY 
       ,LN_DLAY_INTR_TYP        AS LN_DLAY_INTR_TYP 
       ,LN_DLAY_INTR_PLMN_SYMB  AS LN_DLAY_INTR_PLMN_SYMB 
       ,LN_DLAY_INTR_PLMN_COD   AS LN_DLAY_INTR_PLMN_COD 
       ,LN_LN_FUND_RSUR         AS LN_LN_FUND_RSUR 
       ,LN_LN_PURP              AS LN_LN_PURP 
       ,LN_LN_STY               AS LN_LN_STY 
       ,LN_EXTN_TM_N            AS LN_EXTN_TM_N 
       ,LN_SVC_RTO              AS LN_SVC_RTO 
       ,LN_DEP_ACCT_NO          AS LN_DEP_ACCT_NO 
       ,LN_PAY_INT_ACCT_NO      AS LN_PAY_INT_ACCT_NO 
       ,LN_FUND_TO_AUOR_FLG     AS LN_FUND_TO_AUOR_FLG 
       ,LN_GUAR_MARGIN_ACCT_NO  AS LN_GUAR_MARGIN_ACCT_NO 
       ,LN_BAD_LN_FLG           AS LN_BAD_LN_FLG 
       ,LN_APCL_FLG             AS LN_APCL_FLG 
       ,LN_CLOSE_FLG            AS LN_CLOSE_FLG 
       ,LN_CUST_NAME            AS LN_CUST_NAME 
       ,LN_CERT_TYP             AS LN_CERT_TYP 
       ,LN_CERT_ID              AS LN_CERT_ID 
       ,LN_SVC_BAL              AS LN_SVC_BAL 
       ,LN_ACCP_TYP             AS LN_ACCP_TYP 
       ,LN_GUAR_ACCP_EXPT_NO    AS LN_GUAR_ACCP_EXPT_NO 
       ,LN_NOR_BACK_FLG         AS LN_NOR_BACK_FLG 
       ,LN_HUNT_DUE_DT_N        AS LN_HUNT_DUE_DT_N 
       ,LN_LOC_CITY_FLG         AS LN_LOC_CITY_FLG 
       ,LN_APPL_DUE_DT_N        AS LN_APPL_DUE_DT_N 
       ,LN_CNL_NO               AS LN_CNL_NO 
       ,LN_BELONG_INSTN_COD     AS LN_BELONG_INSTN_COD 
       ,LN_LTST_MNTN_OPUN_NO    AS LN_LTST_MNTN_OPUN_NO 
       ,LN_FLST_OPUN_NO         AS LN_FLST_OPUN_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
       ,LN_FLST_TLR_NO          AS LN_FLST_TLR_NO 
   FROM O_DP_CBOD_LNLNSLNS A                                   --放款主档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_LNLNSLNS_INNTMP1 = sqlContext.sql(sql)
F_DP_CBOD_LNLNSLNS_INNTMP1.registerTempTable("F_DP_CBOD_LNLNSLNS_INNTMP1")

#F_DP_CBOD_LNLNSLNS = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_LNLNSLNS/*')
#F_DP_CBOD_LNLNSLNS.registerTempTable("F_DP_CBOD_LNLNSLNS")
sql = """
 SELECT DST.ETLDT                                               --平台日期:src.ETLDT
       ,DST.LN_LN_ACCT_NO                                      --贷款帐号:src.LN_LN_ACCT_NO
       ,DST.LN_CUST_NO                                         --客户编号:src.LN_CUST_NO
       ,DST.LN_CRLMT_NO                                        --贷款额度编号:src.LN_CRLMT_NO
       ,DST.LN_PDP_CODE                                        --产品代码:src.LN_PDP_CODE
       ,DST.LN_BUSN_TYP                                        --业务别:src.LN_BUSN_TYP
       ,DST.LN_ACCT_STS                                        --帐户状态:src.LN_ACCT_STS
       ,DST.LN_VIP_ACCT_FLG                                    --重要帐户标志:src.LN_VIP_ACCT_FLG
       ,DST.LN_CUST_TYP                                        --客户类别:src.LN_CUST_TYP
       ,DST.LN_CURR_COD                                        --币别:src.LN_CURR_COD
       ,DST.LN_CURR_IDEN                                       --钞汇鉴别:src.LN_CURR_IDEN
       ,DST.LN_FINBUSN                                         --业务种类:src.LN_FINBUSN
       ,DST.LN_LN_TYP                                          --贷款种类:src.LN_LN_TYP
       ,DST.LN_HYPO_TYP                                        --质借种类:src.LN_HYPO_TYP
       ,DST.LN_NWLN_AMT_HYPO_AMT                               --新承做贷款金额/质借金额:src.LN_NWLN_AMT_HYPO_AMT
       ,DST.LN_TOTL_LN_AMT_HYPO_AMT                            --贷款总额/质借总额:src.LN_TOTL_LN_AMT_HYPO_AMT
       ,DST.LN_LN_BAL                                          --贷款余额:src.LN_LN_BAL
       ,DST.LN_FRST_ALFD_DT_N                                  --最初拨款日期:src.LN_FRST_ALFD_DT_N
       ,DST.LN_DUE_DT_N                                        --到期日期:src.LN_DUE_DT_N
       ,DST.LN_CLSD_DT_N                                       --结清日期:src.LN_CLSD_DT_N
       ,DST.LN_RFN_STY                                         --还款方式:src.LN_RFN_STY
       ,DST.LN_TOTL_PRD_N                                      --总期数:src.LN_TOTL_PRD_N
       ,DST.LN_ARFN_SCHD_PR_N                                  --已还期数(本金):src.LN_ARFN_SCHD_PR_N
       ,DST.LN_ARFN_SCHD_INT_N                                 --已还期数(利息):src.LN_ARFN_SCHD_INT_N
       ,DST.LN_TNRNO_N                                         --期数:src.LN_TNRNO_N
       ,DST.LN_LN_MTHS_N                                       --贷款期限月数:src.LN_LN_MTHS_N
       ,DST.LN_CLSD_INTC_TYP                                   --结息方式:src.LN_CLSD_INTC_TYP
       ,DST.LN_FRST_INTC_INTR                                  --最初计息利率:src.LN_FRST_INTC_INTR
       ,DST.LN_FRST_DELAY_INTR                                 --最初逾期利率:src.LN_FRST_DELAY_INTR
       ,DST.LN_INTR_ADJ_STY                                    --利率调整方式:src.LN_INTR_ADJ_STY
       ,DST.LN_INTR_ADJ_CYCL                                   --利率调整周期:src.LN_INTR_ADJ_CYCL
       ,DST.LN_INTR_ADJ_STRT_DT_N                              --利息调整起始日期:src.LN_INTR_ADJ_STRT_DT_N
       ,DST.LN_INTR_EFF_DT_N                                   --利率生效日期:src.LN_INTR_EFF_DT_N
       ,DST.LN_INTR_ACOR_STY                                   --利率依据方式:src.LN_INTR_ACOR_STY
       ,DST.LN_INTR_TYP                                        --利率类别:src.LN_INTR_TYP
       ,DST.LN_INTR_NEGO_SYMB                                  --利率加减符号:src.LN_INTR_NEGO_SYMB
       ,DST.LN_INTR_NEGO_RATE                                  --利率加减码:src.LN_INTR_NEGO_RATE
       ,DST.LN_DLAY_INTR_ACOR_STY                              --逾期利率依据方式:src.LN_DLAY_INTR_ACOR_STY
       ,DST.LN_DLAY_INTR_TYP                                   --逾期利率种类:src.LN_DLAY_INTR_TYP
       ,DST.LN_DLAY_INTR_PLMN_SYMB                             --逾期利率加减符号:src.LN_DLAY_INTR_PLMN_SYMB
       ,DST.LN_DLAY_INTR_PLMN_COD                              --逾期利率加减码:src.LN_DLAY_INTR_PLMN_COD
       ,DST.LN_LN_FUND_RSUR                                    --贷款资金来源:src.LN_LN_FUND_RSUR
       ,DST.LN_LN_PURP                                         --贷款用途:src.LN_LN_PURP
       ,DST.LN_LN_STY                                          --贷款方式:src.LN_LN_STY
       ,DST.LN_EXTN_TM_N                                       --展期次数:src.LN_EXTN_TM_N
       ,DST.LN_SVC_RTO                                         --手续费率:src.LN_SVC_RTO
       ,DST.LN_DEP_ACCT_NO                                     --存款帐号:src.LN_DEP_ACCT_NO
       ,DST.LN_PAY_INT_ACCT_NO                                 --贴现买方付息帐号:src.LN_PAY_INT_ACCT_NO
       ,DST.LN_FUND_TO_AUOR_FLG                                --基金自动划转标志:src.LN_FUND_TO_AUOR_FLG
       ,DST.LN_GUAR_MARGIN_ACCT_NO                             --保证金帐号:src.LN_GUAR_MARGIN_ACCT_NO
       ,DST.LN_BAD_LN_FLG                                      --不良贷款标志/置换贷款标志:src.LN_BAD_LN_FLG
       ,DST.LN_APCL_FLG                                        --呆帐核销标识:src.LN_APCL_FLG
       ,DST.LN_CLOSE_FLG                                       --封帐标志:src.LN_CLOSE_FLG
       ,DST.LN_CUST_NAME                                       --客户名称:src.LN_CUST_NAME
       ,DST.LN_CERT_TYP                                        --证件种类:src.LN_CERT_TYP
       ,DST.LN_CERT_ID                                         --证件号码:src.LN_CERT_ID
       ,DST.LN_SVC_BAL                                         --手续费余额:src.LN_SVC_BAL
       ,DST.LN_ACCP_TYP                                        --承兑分类:src.LN_ACCP_TYP
       ,DST.LN_GUAR_ACCP_EXPT_NO                               --保函/承兑/外销编号:src.LN_GUAR_ACCP_EXPT_NO
       ,DST.LN_NOR_BACK_FLG                                    --是否正常归还:src.LN_NOR_BACK_FLG
       ,DST.LN_HUNT_DUE_DT_N                                   --追索到期日:src.LN_HUNT_DUE_DT_N
       ,DST.LN_LOC_CITY_FLG                                    --同城标志:src.LN_LOC_CITY_FLG
       ,DST.LN_APPL_DUE_DT_N                                   --借款到期日:src.LN_APPL_DUE_DT_N
       ,DST.LN_CNL_NO                                          --借据印刷号:src.LN_CNL_NO
       ,DST.LN_BELONG_INSTN_COD                                --账户归属机构:src.LN_BELONG_INSTN_COD
       ,DST.LN_LTST_MNTN_OPUN_NO                               --最近维护交易机构:src.LN_LTST_MNTN_OPUN_NO
       ,DST.LN_FLST_OPUN_NO                                    --开户机构号:src.LN_FLST_OPUN_NO
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
       ,DST.LN_FLST_TLR_NO                                     --建档柜员代号:src.LN_FLST_TLR_NO
   FROM F_DP_CBOD_LNLNSLNS DST 
   LEFT JOIN F_DP_CBOD_LNLNSLNS_INNTMP1 SRC 
     ON SRC.LN_LN_ACCT_NO       = DST.LN_LN_ACCT_NO 
  WHERE SRC.LN_LN_ACCT_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_LNLNSLNS_INNTMP2 = sqlContext.sql(sql)
dfn="F_DP_CBOD_LNLNSLNS/"+V_DT+".parquet"
UNION=F_DP_CBOD_LNLNSLNS_INNTMP2.unionAll(F_DP_CBOD_LNLNSLNS_INNTMP1)
F_DP_CBOD_LNLNSLNS_INNTMP1.cache()
F_DP_CBOD_LNLNSLNS_INNTMP2.cache()
nrowsi = F_DP_CBOD_LNLNSLNS_INNTMP1.count()
nrowsa = F_DP_CBOD_LNLNSLNS_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_DP_CBOD_LNLNSLNS_INNTMP1.unpersist()
F_DP_CBOD_LNLNSLNS_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_LNLNSLNS lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_LNLNSLNS/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_LNLNSLNS_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_DP_CBOD_LNLNSLNS/"+V_DT+".parquet /"+dbname+"/F_DP_CBOD_LNLNSLNS_BK/")
