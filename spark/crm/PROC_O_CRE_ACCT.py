#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CRE_ACCT').setMaster(sys.argv[2])
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

O_CR_ACCT = sqlContext.read.parquet(hdfs+'/O_CR_ACCT/*')
O_CR_ACCT.registerTempTable("O_CR_ACCT")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CR_ACCT = sqlContext.read.parquet(hdfs+'/F_CR_ACCT_BK/'+V_DT_LD+'.parquet/*')
F_CR_ACCT.registerTempTable("F_CR_ACCT")

sql = """
 SELECT CARD_NBR                AS CARD_NBR 
       ,XACCOUNT                AS XACCOUNT 
       ,ADD_CHGDAY              AS ADD_CHGDAY 
       ,ADDR_TYPE               AS ADDR_TYPE 
       ,PRODUCT                 AS PRODUCT 
       ,BRANCH                  AS BRANCH 
       ,BUSINESS                AS BUSINESS 
       ,DAY_OPENED              AS DAY_OPENED 
       ,CRED_LIMIT              AS CRED_LIMIT 
       ,CREDLIM_X               AS CREDLIM_X 
       ,MP_LIMIT                AS MP_LIMIT 
       ,AUTHS_AMT               AS AUTHS_AMT 
       ,CUTOFF_DAY              AS CUTOFF_DAY 
       ,LASTPAYDAY              AS LASTPAYDAY 
       ,LASTPAYAMT              AS LASTPAYAMT 
       ,PURCHASES               AS PURCHASES 
       ,PURCHASES_S             AS PURCHASES_S 
       ,CARD_FEES               AS CARD_FEES 
       ,CASH_ADFEE              AS CASH_ADFEE 
       ,CASH_ADVCE              AS CASH_ADVCE 
       ,OTHER_FEES              AS OTHER_FEES 
       ,PEN_CHRG                AS PEN_CHRG 
       ,STM_OVERDU              AS STM_OVERDU 
       ,DAYS_ODUE               AS DAYS_ODUE 
       ,QUERY_AMT               AS QUERY_AMT 
       ,DEBIT_ADJ               AS DEBIT_ADJ 
       ,CRED_ADJ                AS CRED_ADJ 
       ,CLOSE_CODE              AS CLOSE_CODE 
       ,CLOSE_CHDY              AS CLOSE_CHDY 
       ,BAL_FREE                AS BAL_FREE 
       ,BAL_INT                 AS BAL_INT 
       ,BAL_INT_S               AS BAL_INT_S 
       ,BAL_NOINT               AS BAL_NOINT 
       ,BAL_ORINT               AS BAL_ORINT 
       ,COLLS_DAY               AS COLLS_DAY 
       ,CURR_NUM                AS CURR_NUM 
       ,INT_UPTODY              AS INT_UPTODY 
       ,INT_CHGD                AS INT_CHGD 
       ,NBR_PAYMNT              AS NBR_PAYMNT 
       ,NBR_PURCH               AS NBR_PURCH 
       ,NBR_TRANS               AS NBR_TRANS 
       ,POINT_ADJ               AS POINT_ADJ 
       ,POINT_CLM               AS POINT_CLM 
       ,POINT_CUM               AS POINT_CUM 
       ,POINT_EAR               AS POINT_EAR 
       ,REPAY_CODE_X            AS REPAY_CODE_X 
       ,STM_NOINT               AS STM_NOINT 
       ,STM_BALFRE              AS STM_BALFRE 
       ,STM_BALORI              AS STM_BALORI 
       ,STM_BALINT              AS STM_BALINT 
       ,STM_BALINT_S            AS STM_BALINT_S 
       ,STM_BALNCE              AS STM_BALNCE 
       ,STM_MINDUE              AS STM_MINDUE 
       ,STM_CODE                AS STM_CODE 
       ,STM_OVERDU1             AS STM_OVERDU1 
       ,STM_OVERDU2             AS STM_OVERDU2 
       ,STM_OVERDU3             AS STM_OVERDU3 
       ,STM_OVERDU4             AS STM_OVERDU4 
       ,STM_OVERDU5             AS STM_OVERDU5 
       ,STM_OVERDU6             AS STM_OVERDU6 
       ,MTHS_ODUE               AS MTHS_ODUE 
       ,BAL_CMPINT              AS BAL_CMPINT 
       ,TUIHUO_JE               AS TUIHUO_JE 
       ,CYCLE_NBR               AS CYCLE_NBR 
       ,EXCH_CODE_X             AS EXCH_CODE_X 
       ,MP_AUTHS                AS MP_AUTHS 
       ,MP_REM_PPL              AS MP_REM_PPL 
       ,ADDRESS1                AS ADDRESS1 
       ,ADDRESS2                AS ADDRESS2 
       ,ADDRESS3                AS ADDRESS3 
       ,ADDRESS4                AS ADDRESS4 
       ,ADDRESS5                AS ADDRESS5 
       ,POST_CODE               AS POST_CODE 
       ,BAL_MP                  AS BAL_MP 
       ,STM_BALMP               AS STM_BALMP 
       ,BANKACCT_A1             AS BANKACCT_A1 
       ,BANKACCT_A2             AS BANKACCT_A2 
       ,BANKACCT_A3             AS BANKACCT_A3 
       ,BANKACCT_A4             AS BANKACCT_A4 
       ,BANKCODE_A1             AS BANKCODE_A1 
       ,BANKCODE_A2             AS BANKCODE_A2 
       ,BANKCODE_A3             AS BANKCODE_A3 
       ,BANKCODE_A4             AS BANKCODE_A4 
       ,REPAY_CODE              AS REPAY_CODE 
       ,REPAY_CODX              AS REPAY_CODX 
       ,EXCH_FLAG               AS EXCH_FLAG 
       ,EXCH_CODE               AS EXCH_CODE 
       ,STM_CLOSDY              AS STM_CLOSDY 
       ,POINT_ADJ_S             AS POINT_ADJ_S 
       ,POINT_CUM_S             AS POINT_CUM_S 
       ,ZX_ZW                   AS ZX_ZW 
       ,ZX_XM                   AS ZX_XM 
       ,ACCT_FLAG               AS ACCT_FLAG 
       ,CA_PCNT                 AS CA_PCNT 
       ,BRNCH_INTR              AS BRNCH_INTR 
       ,BRNCHCRLMT              AS BRNCHCRLMT 
       ,VRF_CNTNT               AS VRF_CNTNT 
       ,STPAY                   AS STPAY 
       ,MONTH_NBR               AS MONTH_NBR 
       ,CRED_TEMP               AS CRED_TEMP 
       ,CRED_TEMP_BDAY          AS CRED_TEMP_BDAY 
       ,CRED_TEMP_LDAY          AS CRED_TEMP_LDAY 
       ,JF_BORROW               AS JF_BORROW 
       ,PAY_FLAG                AS PAY_FLAG 
       ,INT_CHDCMP              AS INT_CHDCMP 
       ,INT_NOTION              AS INT_NOTION 
       ,INT_CUNOT               AS INT_CUNOT 
       ,INT_CURCMP              AS INT_CURCMP 
       ,INT_CMPOND              AS INT_CMPOND 
       ,IMP_FLAG                AS IMP_FLAG 
       ,NOREPORT_REAS           AS NOREPORT_REAS 
       ,MONTR_CODE              AS MONTR_CODE 
       ,CRED_LMT2               AS CRED_LMT2 
       ,CRED_FLAG               AS CRED_FLAG 
       ,BAL_NINT01              AS BAL_NINT01 
       ,BAL_NINT02              AS BAL_NINT02 
       ,BAL_NINT03              AS BAL_NINT03 
       ,BAL_NINT04              AS BAL_NINT04 
       ,BAL_NINT05              AS BAL_NINT05 
       ,BAL_NINT06              AS BAL_NINT06 
       ,BAL_NINT07              AS BAL_NINT07 
       ,BAL_NINT08              AS BAL_NINT08 
       ,BAL_NINT09              AS BAL_NINT09 
       ,BAL_NINT10              AS BAL_NINT10 
       ,STM_NINT01              AS STM_NINT01 
       ,STM_NINT02              AS STM_NINT02 
       ,STM_NINT03              AS STM_NINT03 
       ,STM_NINT04              AS STM_NINT04 
       ,STM_NINT05              AS STM_NINT05 
       ,STM_NINT06              AS STM_NINT06 
       ,STM_NINT07              AS STM_NINT07 
       ,STM_NINT08              AS STM_NINT08 
       ,STM_NINT09              AS STM_NINT09 
       ,STM_NINT10              AS STM_NINT10 
       ,WROF_FLAG               AS WROF_FLAG 
       ,CANCL_RESN              AS CANCL_RESN 
       ,SHADOW_INT              AS SHADOW_INT 
       ,SHADOW_CMP              AS SHADOW_CMP 
       ,SHADOW_PEN              AS SHADOW_PEN 
       ,BAL_CMPFEE              AS BAL_CMPFEE 
       ,FP_FLAG                 AS FP_FLAG 
       ,FREE_LIMIT              AS FREE_LIMIT 
       ,FREE_PCNT               AS FREE_PCNT 
       ,FREE_MAX                AS FREE_MAX 
       ,FREE_MIN                AS FREE_MIN 
       ,MP_REM_PPLX             AS MP_REM_PPLX 
       ,CRED_LMT3               AS CRED_LMT3 
       ,MPAUTO_GW               AS MPAUTO_GW 
       ,INT_CODE                AS INT_CODE 
       ,CRED_CHDAY              AS CRED_CHDAY 
       ,ADJ_FLAG                AS ADJ_FLAG 
       ,NOTE_RST                AS NOTE_RST 
       ,NOTE_RSN                AS NOTE_RSN 
       ,RECLA_CODE              AS RECLA_CODE 
       ,POINT_CUM2              AS POINT_CUM2 
       ,POINT_REV               AS POINT_REV 
       ,PAYMT_UNCL              AS PAYMT_UNCL 
       ,QUERY_CODE              AS QUERY_CODE 
       ,ORG_NO                  AS ORG_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CRE'                   AS ODS_SYS_ID 
   FROM O_CR_ACCT A                                            --帐户资料
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CR_ACCT_INNTMP1 = sqlContext.sql(sql)
F_CR_ACCT_INNTMP1.registerTempTable("F_CR_ACCT_INNTMP1")

#F_CR_ACCT = sqlContext.read.parquet(hdfs+'/F_CR_ACCT/*')
#F_CR_ACCT.registerTempTable("F_CR_ACCT")
sql = """
 SELECT DST.CARD_NBR                                            --卡号:src.CARD_NBR
       ,DST.XACCOUNT                                           --账号:src.XACCOUNT
       ,DST.ADD_CHGDAY                                         --最近地址修改日期:src.ADD_CHGDAY
       ,DST.ADDR_TYPE                                          --帐单地址类型:src.ADDR_TYPE
       ,DST.PRODUCT                                            --卡片产品种类:src.PRODUCT
       ,DST.BRANCH                                             --分行编号:src.BRANCH
       ,DST.BUSINESS                                           --公司编号:src.BUSINESS
       ,DST.DAY_OPENED                                         --开户日期:src.DAY_OPENED
       ,DST.CRED_LIMIT                                         --信用额度:src.CRED_LIMIT
       ,DST.CREDLIM_X                                          --第二币种信用额度:src.CREDLIM_X
       ,DST.MP_LIMIT                                           --分期付款信用额度:src.MP_LIMIT
       ,DST.AUTHS_AMT                                          --授权未请款金额:src.AUTHS_AMT
       ,DST.CUTOFF_DAY                                         --还款截止日期:src.CUTOFF_DAY
       ,DST.LASTPAYDAY                                         --上次还款日期:src.LASTPAYDAY
       ,DST.LASTPAYAMT                                         --已结算付款额:src.LASTPAYAMT
       ,DST.PURCHASES                                          --本期消费净额:src.PURCHASES
       ,DST.PURCHASES_S                                        --本期消费净额(正负号):src.PURCHASES_S
       ,DST.CARD_FEES                                          --本期年费金额:src.CARD_FEES
       ,DST.CASH_ADFEE                                         --本期预借现金费用总额:src.CASH_ADFEE
       ,DST.CASH_ADVCE                                         --本期预借现金总额:src.CASH_ADVCE
       ,DST.OTHER_FEES                                         --本期其它费用的总额:src.OTHER_FEES
       ,DST.PEN_CHRG                                           --本期预收罚金:src.PEN_CHRG
       ,DST.STM_OVERDU                                         --逾期金额:src.STM_OVERDU
       ,DST.DAYS_ODUE                                          --逾期天数:src.DAYS_ODUE
       ,DST.QUERY_AMT                                          --争议金额:src.QUERY_AMT
       ,DST.DEBIT_ADJ                                          --本期借记调整金额:src.DEBIT_ADJ
       ,DST.CRED_ADJ                                           --本期贷记调帐金额:src.CRED_ADJ
       ,DST.CLOSE_CODE                                         --账户状态:src.CLOSE_CODE
       ,DST.CLOSE_CHDY                                         --账户状态日期:src.CLOSE_CHDY
       ,DST.BAL_FREE                                           --昨日消费余额:src.BAL_FREE
       ,DST.BAL_INT                                            --昨日预借现金余额:src.BAL_INT
       ,DST.BAL_INT_S                                          --昨日预借现金余额符号:src.BAL_INT_S
       ,DST.BAL_NOINT                                          --昨日费用余额:src.BAL_NOINT
       ,DST.BAL_ORINT                                          --昨日利息余额:src.BAL_ORINT
       ,DST.COLLS_DAY                                          --帐户进入催收队列的日期:src.COLLS_DAY
       ,DST.CURR_NUM                                           --货币代码:src.CURR_NUM
       ,DST.INT_UPTODY                                         --最近一次计息日期:src.INT_UPTODY
       ,DST.INT_CHGD                                           --应收利息:src.INT_CHGD
       ,DST.NBR_PAYMNT                                         --本帐期的还款笔数:src.NBR_PAYMNT
       ,DST.NBR_PURCH                                          --本帐期的一般消费笔数:src.NBR_PURCH
       ,DST.NBR_TRANS                                          --本帐期的所有交易笔数:src.NBR_TRANS
       ,DST.POINT_ADJ                                          --当期调整的红利积分:src.POINT_ADJ
       ,DST.POINT_CLM                                          --当期兑换的红利积分:src.POINT_CLM
       ,DST.POINT_CUM                                          --累计的红利积分:src.POINT_CUM
       ,DST.POINT_EAR                                          --当期新增的红利积分:src.POINT_EAR
       ,DST.REPAY_CODE_X                                       --自动扣款代码:src.REPAY_CODE_X
       ,DST.STM_NOINT                                          --最近一期帐单的不计息余额:src.STM_NOINT
       ,DST.STM_BALFRE                                         --最近一期帐单的消费余额:src.STM_BALFRE
       ,DST.STM_BALORI                                         --最近一期帐单的利息余额:src.STM_BALORI
       ,DST.STM_BALINT                                         --最近一期帐单的预借现金余额:src.STM_BALINT
       ,DST.STM_BALINT_S                                       --最近一期帐单的预借现金符号:src.STM_BALINT_S
       ,DST.STM_BALNCE                                         --最近一期帐单金额:src.STM_BALNCE
       ,DST.STM_MINDUE                                         --最近一期帐单的最低应缴款:src.STM_MINDUE
       ,DST.STM_CODE                                           --帐单代码:src.STM_CODE
       ,DST.STM_OVERDU1                                        --逾期金额1:src.STM_OVERDU1
       ,DST.STM_OVERDU2                                        --逾期金额2:src.STM_OVERDU2
       ,DST.STM_OVERDU3                                        --逾期金额3:src.STM_OVERDU3
       ,DST.STM_OVERDU4                                        --逾期金额4:src.STM_OVERDU4
       ,DST.STM_OVERDU5                                        --逾期金额5:src.STM_OVERDU5
       ,DST.STM_OVERDU6                                        --逾期金额6:src.STM_OVERDU6
       ,DST.MTHS_ODUE                                          --逾期最高期数:src.MTHS_ODUE
       ,DST.BAL_CMPINT                                         --复利余额:src.BAL_CMPINT
       ,DST.TUIHUO_JE                                          --当期发生退货金额:src.TUIHUO_JE
       ,DST.CYCLE_NBR                                          --帐单日:src.CYCLE_NBR
       ,DST.EXCH_CODE_X                                        --购汇功能选项:src.EXCH_CODE_X
       ,DST.MP_AUTHS                                           --分期付款授权金额:src.MP_AUTHS
       ,DST.MP_REM_PPL                                         --分期剩余本金:src.MP_REM_PPL
       ,DST.ADDRESS1                                           --帐单地址栏位1:src.ADDRESS1
       ,DST.ADDRESS2                                           --帐单地址栏位2:src.ADDRESS2
       ,DST.ADDRESS3                                           --帐单地址栏位3:src.ADDRESS3
       ,DST.ADDRESS4                                           --帐单地址栏位4:src.ADDRESS4
       ,DST.ADDRESS5                                           --帐单地址栏位5:src.ADDRESS5
       ,DST.POST_CODE                                          --帐单地址邮编:src.POST_CODE
       ,DST.BAL_MP                                             --昨日分期余额:src.BAL_MP
       ,DST.STM_BALMP                                          --上期帐单分期余额:src.STM_BALMP
       ,DST.BANKACCT_A1                                        --关联还款账号1:src.BANKACCT_A1
       ,DST.BANKACCT_A2                                        --关联还款账号2:src.BANKACCT_A2
       ,DST.BANKACCT_A3                                        --关联还款账号3:src.BANKACCT_A3
       ,DST.BANKACCT_A4                                        --关联还款账号4:src.BANKACCT_A4
       ,DST.BANKCODE_A1                                        --关联账号代码1:src.BANKCODE_A1
       ,DST.BANKCODE_A2                                        --关联账号代码1:src.BANKCODE_A2
       ,DST.BANKCODE_A3                                        --关联账号代码1:src.BANKCODE_A3
       ,DST.BANKCODE_A4                                        --关联账号代码1:src.BANKCODE_A4
       ,DST.REPAY_CODE                                         --人民币自扣还款标志:src.REPAY_CODE
       ,DST.REPAY_CODX                                         --美元自扣还款标志:src.REPAY_CODX
       ,DST.EXCH_FLAG                                          --购汇功能选项:src.EXCH_FLAG
       ,DST.EXCH_CODE                                          --购汇金额选项:src.EXCH_CODE
       ,DST.STM_CLOSDY                                         --最近一期帐单处理日:src.STM_CLOSDY
       ,DST.POINT_ADJ_S                                        --当期调整红利积分符号:src.POINT_ADJ_S
       ,DST.POINT_CUM_S                                        --累计总积分符号:src.POINT_CUM_S
       ,DST.ZX_ZW                                              --账户征信代码-职务:src.ZX_ZW
       ,DST.ZX_XM                                              --账户征信代码-项目:src.ZX_XM
       ,DST.ACCT_FLAG                                          --帐户标志位:src.ACCT_FLAG
       ,DST.CA_PCNT                                            --帐户预借现金比例:src.CA_PCNT
       ,DST.BRNCH_INTR                                         --有无分行推荐函:src.BRNCH_INTR
       ,DST.BRNCHCRLMT                                         --分行建议额度:src.BRNCHCRLMT
       ,DST.VRF_CNTNT                                          --核实内容:src.VRF_CNTNT
       ,DST.STPAY                                              --账户级短信费免收标志:src.STPAY
       ,DST.MONTH_NBR                                          --月份:src.MONTH_NBR
       ,DST.CRED_TEMP                                          --临时额度:src.CRED_TEMP
       ,DST.CRED_TEMP_BDAY                                     --临时额度失效日期:src.CRED_TEMP_BDAY
       ,DST.CRED_TEMP_LDAY                                     --临时额度生效日期:src.CRED_TEMP_LDAY
       ,DST.JF_BORROW                                          --预借积分额度:src.JF_BORROW
       ,DST.PAY_FLAG                                           --已全额还款标志:src.PAY_FLAG
       ,DST.INT_CHDCMP                                         --应收复利:src.INT_CHDCMP
       ,DST.INT_NOTION                                         --上期应计利息:src.INT_NOTION
       ,DST.INT_CUNOT                                          --当期应计利息:src.INT_CUNOT
       ,DST.INT_CURCMP                                         --当期应计复利:src.INT_CURCMP
       ,DST.INT_CMPOND                                         --应计复利利息:src.INT_CMPOND
       ,DST.IMP_FLAG                                           --账户质押办卡/调额标志:src.IMP_FLAG
       ,DST.NOREPORT_REAS                                      --不上报征信数据原因:src.NOREPORT_REAS
       ,DST.MONTR_CODE                                         --监控代码:src.MONTR_CODE
       ,DST.CRED_LMT2                                          --影子额度:src.CRED_LMT2
       ,DST.CRED_FLAG                                          --影子额度标志:src.CRED_FLAG
       ,DST.BAL_NINT01                                         --本期费用01:src.BAL_NINT01
       ,DST.BAL_NINT02                                         --本期费用02:src.BAL_NINT02
       ,DST.BAL_NINT03                                         --本期费用03:src.BAL_NINT03
       ,DST.BAL_NINT04                                         --本期费用04:src.BAL_NINT04
       ,DST.BAL_NINT05                                         --本期费用05:src.BAL_NINT05
       ,DST.BAL_NINT06                                         --本期费用06:src.BAL_NINT06
       ,DST.BAL_NINT07                                         --本期费用07:src.BAL_NINT07
       ,DST.BAL_NINT08                                         --本期费用08:src.BAL_NINT08
       ,DST.BAL_NINT09                                         --本期费用09:src.BAL_NINT09
       ,DST.BAL_NINT10                                         --本期费用10:src.BAL_NINT10
       ,DST.STM_NINT01                                         --上期费用01:src.STM_NINT01
       ,DST.STM_NINT02                                         --上期费用02:src.STM_NINT02
       ,DST.STM_NINT03                                         --上期费用03:src.STM_NINT03
       ,DST.STM_NINT04                                         --上期费用04:src.STM_NINT04
       ,DST.STM_NINT05                                         --上期费用05:src.STM_NINT05
       ,DST.STM_NINT06                                         --上期费用06:src.STM_NINT06
       ,DST.STM_NINT07                                         --上期费用07:src.STM_NINT07
       ,DST.STM_NINT08                                         --上期费用08:src.STM_NINT08
       ,DST.STM_NINT09                                         --上期费用09:src.STM_NINT09
       ,DST.STM_NINT10                                         --上期费用10:src.STM_NINT10
       ,DST.WROF_FLAG                                          --核销标志:src.WROF_FLAG
       ,DST.CANCL_RESN                                         --核销原因代码:src.CANCL_RESN
       ,DST.SHADOW_INT                                         --停收利息:src.SHADOW_INT
       ,DST.SHADOW_CMP                                         --停收复利:src.SHADOW_CMP
       ,DST.SHADOW_PEN                                         --停收滞纳金:src.SHADOW_PEN
       ,DST.BAL_CMPFEE                                         --表内转表外费用:src.BAL_CMPFEE
       ,DST.FP_FLAG                                            --消费免息标志:src.FP_FLAG
       ,DST.FREE_LIMIT                                         --消费免息额度固定值:src.FREE_LIMIT
       ,DST.FREE_PCNT                                          --消费免息额度比率:src.FREE_PCNT
       ,DST.FREE_MAX                                           --消费免息额度最大值:src.FREE_MAX
       ,DST.FREE_MIN                                           --消费免息额度最小值:src.FREE_MIN
       ,DST.MP_REM_PPLX                                        --分期剩余本金:src.MP_REM_PPLX
       ,DST.CRED_LMT3                                          --预设临额:src.CRED_LMT3
       ,DST.MPAUTO_GW                                          --自动分期计划相关字段:src.MPAUTO_GW
       ,DST.INT_CODE                                           --利率代码:src.INT_CODE
       ,DST.CRED_CHDAY                                         --上次信用额度更改日期:src.CRED_CHDAY
       ,DST.ADJ_FLAG                                           --不可调整额度账户:src.ADJ_FLAG
       ,DST.NOTE_RST                                           --照会结论:src.NOTE_RST
       ,DST.NOTE_RSN                                           --照会原因:src.NOTE_RSN
       ,DST.RECLA_CODE                                         --关帐详细原因:src.RECLA_CODE
       ,DST.POINT_CUM2                                         --宽限期新增积分:src.POINT_CUM2
       ,DST.POINT_REV                                          --被折算掉积分:src.POINT_REV
       ,DST.PAYMT_UNCL                                         --未结算还款金额:src.PAYMT_UNCL
       ,DST.QUERY_CODE                                         --争议代码:src.QUERY_CODE
       ,DST.ORG_NO                                             --机构编号:src.ORG_NO
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_CR_ACCT DST 
   LEFT JOIN F_CR_ACCT_INNTMP1 SRC 
     ON SRC.CARD_NBR            = DST.CARD_NBR 
    AND SRC.XACCOUNT            = DST.XACCOUNT 
  WHERE SRC.CARD_NBR IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CR_ACCT_INNTMP2 = sqlContext.sql(sql)
dfn="F_CR_ACCT/"+V_DT+".parquet"
UNION=F_CR_ACCT_INNTMP2.unionAll(F_CR_ACCT_INNTMP1)
F_CR_ACCT_INNTMP1.cache()
F_CR_ACCT_INNTMP2.cache()
nrowsi = F_CR_ACCT_INNTMP1.count()
nrowsa = F_CR_ACCT_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CR_ACCT_INNTMP1.unpersist()
F_CR_ACCT_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CR_ACCT lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_CR_ACCT/"+V_DT_LD+".parquet /"+dbname+"/F_CR_ACCT_BK/")


#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CR_ACCT/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CR_ACCT_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CR_ACCT/"+V_DT+".parquet /"+dbname+"/F_CR_ACCT_BK/")