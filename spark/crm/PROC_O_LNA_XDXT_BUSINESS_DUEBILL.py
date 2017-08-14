#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_BUSINESS_DUEBILL').setMaster(sys.argv[2])
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

O_LN_XDXT_BUSINESS_DUEBILL = sqlContext.read.parquet(hdfs+'/O_LN_XDXT_BUSINESS_DUEBILL/*')
O_LN_XDXT_BUSINESS_DUEBILL.registerTempTable("O_LN_XDXT_BUSINESS_DUEBILL")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_BUSINESS_DUEBILL/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_BUSINESS_DUEBILL_BK/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_BUSINESS_DUEBILL/"+V_DT+".parquet")


F_LN_XDXT_BUSINESS_DUEBILL = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BUSINESS_DUEBILL/*')
F_LN_XDXT_BUSINESS_DUEBILL.registerTempTable("F_LN_XDXT_BUSINESS_DUEBILL")

sql = """
 SELECT A.SERIALNO              AS SERIALNO 
       ,A.RELATIVESERIALNO1     AS RELATIVESERIALNO1 
       ,A.RELATIVESERIALNO2     AS RELATIVESERIALNO2 
       ,A.SUBJECTNO             AS SUBJECTNO 
       ,A.MFCUSTOMERID          AS MFCUSTOMERID 
       ,A.CUSTOMERID            AS CUSTOMERID 
       ,A.CUSTOMERNAME          AS CUSTOMERNAME 
       ,A.BUSINESSTYPE          AS BUSINESSTYPE 
       ,A.BUSINESSSUBTYPE       AS BUSINESSSUBTYPE 
       ,A.BUSINESSSTATUS        AS BUSINESSSTATUS 
       ,A.BUSINESSCURRENCY      AS BUSINESSCURRENCY 
       ,A.BUSINESSSUM           AS BUSINESSSUM 
       ,A.PUTOUTDATE            AS PUTOUTDATE 
       ,A.MATURITY              AS MATURITY 
       ,A.ACTUALMATURITY        AS ACTUALMATURITY 
       ,A.BUSINESSRATE          AS BUSINESSRATE 
       ,A.ACTUALBUSINESSRATE    AS ACTUALBUSINESSRATE 
       ,A.ICTYPE                AS ICTYPE 
       ,A.ICCYC                 AS ICCYC 
       ,A.PAYTIMES              AS PAYTIMES 
       ,A.PAYCYC                AS PAYCYC 
       ,A.CORPUSPAYMETHOD       AS CORPUSPAYMETHOD 
       ,A.EXTENDTIMES           AS EXTENDTIMES 
       ,A.REORGTIMES            AS REORGTIMES 
       ,A.RENEWTIMES            AS RENEWTIMES 
       ,A.GOLNTIMES             AS GOLNTIMES 
       ,A.BALANCE               AS BALANCE 
       ,A.NORMALBALANCE         AS NORMALBALANCE 
       ,A.OVERDUEBALANCE        AS OVERDUEBALANCE 
       ,A.DULLBALANCE           AS DULLBALANCE 
       ,A.BADBALANCE            AS BADBALANCE 
       ,A.INTERESTBALANCE1      AS INTERESTBALANCE1 
       ,A.INTERESTBALANCE2      AS INTERESTBALANCE2 
       ,A.FINEBALANCE1          AS FINEBALANCE1 
       ,A.FINEBALANCE2          AS FINEBALANCE2 
       ,A.RECEIVEBALANCE        AS RECEIVEBALANCE 
       ,A.PAYEDBALANCE          AS PAYEDBALANCE 
       ,A.OVERDUEDAYS           AS OVERDUEDAYS 
       ,A.PAYACCOUNT            AS PAYACCOUNT 
       ,A.PUTOUTACCOUNT         AS PUTOUTACCOUNT 
       ,A.PAYBACKACCOUNT        AS PAYBACKACCOUNT 
       ,A.PAYINTERESTACCOUNT    AS PAYINTERESTACCOUNT 
       ,A.OWEINTERESTDAYS       AS OWEINTERESTDAYS 
       ,A.TABALANCE             AS TABALANCE 
       ,A.TAINTERESTBALANCE     AS TAINTERESTBALANCE 
       ,A.TATIMES               AS TATIMES 
       ,A.LCATIMES              AS LCATIMES 
       ,A.SALEDATE              AS SALEDATE 
       ,A.FINISHTYPE            AS FINISHTYPE 
       ,A.FINISHDATE            AS FINISHDATE 
       ,A.MFAREAID              AS MFAREAID 
       ,A.MFORGID               AS MFORGID 
       ,A.MFUSERID              AS MFUSERID 
       ,A.OPERATEORGID          AS OPERATEORGID 
       ,A.OPERATEUSERID         AS OPERATEUSERID 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.INOUTFLAG             AS INOUTFLAG 
       ,A.DEALFLAG              AS DEALFLAG 
       ,A.OCCURDATE             AS OCCURDATE 
       ,A.BUSINESSPROP          AS BUSINESSPROP 
       ,A.BENEFITCORP           AS BENEFITCORP 
       ,A.ACTUALTERMMONTH       AS ACTUALTERMMONTH 
       ,A.ACTUALTERMDAY         AS ACTUALTERMDAY 
       ,A.BASERATETYPE          AS BASERATETYPE 
       ,A.BASERATE              AS BASERATE 
       ,A.RATEFLOATTYPE         AS RATEFLOATTYPE 
       ,A.RATEFLOAT             AS RATEFLOAT 
       ,A.TIMSFLAG              AS TIMSFLAG 
       ,A.BAILRATIO             AS BAILRATIO 
       ,A.LOGOUTDATE            AS LOGOUTDATE 
       ,A.CANCELLOGOUTDATE      AS CANCELLOGOUTDATE 
       ,A.BAILSUM               AS BAILSUM 
       ,A.BAILACCOUNT           AS BAILACCOUNT 
       ,A.PURPOSE               AS PURPOSE 
       ,A.ADVANCEFLAG           AS ADVANCEFLAG 
       ,A.RELATIVEDUEBILLNO     AS RELATIVEDUEBILLNO 
       ,A.ACTUALARTIFICIALNO    AS ACTUALARTIFICIALNO 
       ,A.ACCOUNTNO             AS ACCOUNTNO 
       ,A.LOANACCOUNTNO         AS LOANACCOUNTNO 
       ,A.SECONDPAYACCOUNT      AS SECONDPAYACCOUNT 
       ,A.ADJUSTRATETYPE        AS ADJUSTRATETYPE 
       ,A.ADJUSTRATETERM        AS ADJUSTRATETERM 
       ,A.OVERINTTYPE           AS OVERINTTYPE 
       ,A.RATEADJUSTCYC         AS RATEADJUSTCYC 
       ,A.PDGACCOUNTNO          AS PDGACCOUNTNO 
       ,A.DEDUCTDATE            AS DEDUCTDATE 
       ,A.FZANBALANCE           AS FZANBALANCE 
       ,A.ACCEPTINTTYPE         AS ACCEPTINTTYPE 
       ,A.RATIO                 AS RATIO 
       ,A.THIRDPARTYADD1        AS THIRDPARTYADD1 
       ,A.THIRDPARTYZIP1        AS THIRDPARTYZIP1 
       ,A.THIRDPARTYADD2        AS THIRDPARTYADD2 
       ,A.THIRDPARTYZIP2        AS THIRDPARTYZIP2 
       ,A.TERMDATE1             AS TERMDATE1 
       ,A.TERMDATE2             AS TERMDATE2 
       ,A.TERMDATE3             AS TERMDATE3 
       ,A.DESCRIBE2             AS DESCRIBE2 
       ,A.FIXCYC                AS FIXCYC 
       ,A.THIRDPARTY1           AS THIRDPARTY1 
       ,A.THIRDPARTYID1         AS THIRDPARTYID1 
       ,A.THIRDPARTY2           AS THIRDPARTY2 
       ,A.THIRDPARTY3           AS THIRDPARTY3 
       ,A.TYPE1                 AS TYPE1 
       ,A.TYPE2                 AS TYPE2 
       ,A.TYPE3                 AS TYPE3 
       ,A.BILLNO                AS BILLNO 
       ,A.FLAG1                 AS FLAG1 
       ,A.FLAG2                 AS FLAG2 
       ,A.FLAG3                 AS FLAG3 
       ,A.THIRDPARTYREGION      AS THIRDPARTYREGION 
       ,A.THIRDPARTYACCOUNTS    AS THIRDPARTYACCOUNTS 
       ,A.CARGOINFO             AS CARGOINFO 
       ,A.SECURITIESTYPE        AS SECURITIESTYPE 
       ,A.SECURITIESREGION      AS SECURITIESREGION 
       ,A.ABOUTBANKID2          AS ABOUTBANKID2 
       ,A.ABOUTBANKNAME2        AS ABOUTBANKNAME2 
       ,A.ABOUTBANKID3          AS ABOUTBANKID3 
       ,A.ABOUTBANKNAME         AS ABOUTBANKNAME 
       ,A.ABOUTBANKID           AS ABOUTBANKID 
       ,A.OLDLCTERMTYPE         AS OLDLCTERMTYPE 
       ,A.NEGOTIATENO           AS NEGOTIATENO 
       ,A.CREDITKIND            AS CREDITKIND 
       ,A.GATHERINGNAME         AS GATHERINGNAME 
       ,A.PREINTTYPE            AS PREINTTYPE 
       ,A.RESUMEINTTYPE         AS RESUMEINTTYPE 
       ,A.GUARANTYNO            AS GUARANTYNO 
       ,A.PZTYPE                AS PZTYPE 
       ,A.GRACEPERIOD           AS GRACEPERIOD 
       ,A.OLDLCVALIDDATE        AS OLDLCVALIDDATE 
       ,A.MFEEPAYMETHOD         AS MFEEPAYMETHOD 
       ,A.DESCRIBE1             AS DESCRIBE1 
       ,A.TRADECONTRACTNO       AS TRADECONTRACTNO 
       ,A.LOANTYPE              AS LOANTYPE 
       ,A.FIXTERM               AS FIXTERM 
       ,A.CANCELSUM             AS CANCELSUM 
       ,A.CANCELINTEREST        AS CANCELINTEREST 
       ,A.BAILACOUNT            AS BAILACOUNT 
       ,A.CLASSIFY4             AS CLASSIFY4 
       ,A.CLASSIFYRESULT        AS CLASSIFYRESULT 
       ,A.RETURNTYPE            AS RETURNTYPE 
       ,A.BAILPERCENT           AS BAILPERCENT 
       ,A.PAYMENTTYPE           AS PAYMENTTYPE 
       ,A.TERMSFREQ             AS TERMSFREQ 
       ,A.OVERDUEDATE           AS OVERDUEDATE 
       ,A.OWEINTERESTDATE       AS OWEINTERESTDATE 
       ,A.LCSTATUS              AS LCSTATUS 
       ,A.ICHANGEDATE           AS ICHANGEDATE 
       ,A.VOUCHTYPE             AS VOUCHTYPE 
       ,A.MANAGEUSERID          AS MANAGEUSERID 
       ,A.MANAGEORGID           AS MANAGEORGID 
       ,A.CORPORATEORGID        AS CORPORATEORGID 
       ,A.NEWBUSINESSRATE       AS NEWBUSINESSRATE 
       ,A.IRREGULATEFLAG        AS IRREGULATEFLAG 
       ,A.TENCLASSIFYRESULT     AS TENCLASSIFYRESULT 
       ,A.LASTTENCLASSIFYRESULT AS LASTTENCLASSIFYRESULT 
       ,A.LASTCLASSIFYRESULT    AS LASTCLASSIFYRESULT 
       ,A.SORTNO                AS SORTNO 
       ,A.MIGRATEFLAG           AS MIGRATEFLAG 
       ,A.BADDFLAG              AS BADDFLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_LN_XDXT_BUSINESS_DUEBILL A                           --业务借据信息
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_BUSINESS_DUEBILL_INNTMP1 = sqlContext.sql(sql)
F_LN_XDXT_BUSINESS_DUEBILL_INNTMP1.registerTempTable("F_LN_XDXT_BUSINESS_DUEBILL_INNTMP1")

#F_LN_XDXT_BUSINESS_DUEBILL = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BUSINESS_DUEBILL/*')
#F_LN_XDXT_BUSINESS_DUEBILL.registerTempTable("F_LN_XDXT_BUSINESS_DUEBILL")
sql = """
 SELECT DST.SERIALNO                                            --流水号:src.SERIALNO
       ,DST.RELATIVESERIALNO1                                  --借据流水号:src.RELATIVESERIALNO1
       ,DST.RELATIVESERIALNO2                                  --合同流水号:src.RELATIVESERIALNO2
       ,DST.SUBJECTNO                                          --会计科目:src.SUBJECTNO
       ,DST.MFCUSTOMERID                                       --主机客户号:src.MFCUSTOMERID
       ,DST.CUSTOMERID                                         --客户编号:src.CUSTOMERID
       ,DST.CUSTOMERNAME                                       --客户名称:src.CUSTOMERNAME
       ,DST.BUSINESSTYPE                                       --业务品种:src.BUSINESSTYPE
       ,DST.BUSINESSSUBTYPE                                    --子业务品种:src.BUSINESSSUBTYPE
       ,DST.BUSINESSSTATUS                                     --业务形态:src.BUSINESSSTATUS
       ,DST.BUSINESSCURRENCY                                   --业务币种:src.BUSINESSCURRENCY
       ,DST.BUSINESSSUM                                        --金额:src.BUSINESSSUM
       ,DST.PUTOUTDATE                                         --发放日期:src.PUTOUTDATE
       ,DST.MATURITY                                           --约定到期日:src.MATURITY
       ,DST.ACTUALMATURITY                                     --执行到期日:src.ACTUALMATURITY
       ,DST.BUSINESSRATE                                       --利率:src.BUSINESSRATE
       ,DST.ACTUALBUSINESSRATE                                 --执行利率:src.ACTUALBUSINESSRATE
       ,DST.ICTYPE                                             --计息方式:src.ICTYPE
       ,DST.ICCYC                                              --计息周期:src.ICCYC
       ,DST.PAYTIMES                                           --还款期次:src.PAYTIMES
       ,DST.PAYCYC                                             --还款周期:src.PAYCYC
       ,DST.CORPUSPAYMETHOD                                    --本金还款方式:src.CORPUSPAYMETHOD
       ,DST.EXTENDTIMES                                        --展期次数:src.EXTENDTIMES
       ,DST.REORGTIMES                                         --债务重组次数:src.REORGTIMES
       ,DST.RENEWTIMES                                         --借新还旧次数:src.RENEWTIMES
       ,DST.GOLNTIMES                                          --还旧借新次数:src.GOLNTIMES
       ,DST.BALANCE                                            --借据余额:src.BALANCE
       ,DST.NORMALBALANCE                                      --正常余额:src.NORMALBALANCE
       ,DST.OVERDUEBALANCE                                     --逾期金额:src.OVERDUEBALANCE
       ,DST.DULLBALANCE                                        --呆滞余额:src.DULLBALANCE
       ,DST.BADBALANCE                                         --呆帐余额:src.BADBALANCE
       ,DST.INTERESTBALANCE1                                   --表内欠息余额:src.INTERESTBALANCE1
       ,DST.INTERESTBALANCE2                                   --表外欠息余额:src.INTERESTBALANCE2
       ,DST.FINEBALANCE1                                       --本金罚息:src.FINEBALANCE1
       ,DST.FINEBALANCE2                                       --利息罚息:src.FINEBALANCE2
       ,DST.RECEIVEBALANCE                                     --到单金额:src.RECEIVEBALANCE
       ,DST.PAYEDBALANCE                                       --付款金额:src.PAYEDBALANCE
       ,DST.OVERDUEDAYS                                        --逾期天数:src.OVERDUEDAYS
       ,DST.PAYACCOUNT                                         --存款帐号:src.PAYACCOUNT
       ,DST.PUTOUTACCOUNT                                      --放款帐号:src.PUTOUTACCOUNT
       ,DST.PAYBACKACCOUNT                                     --还款帐号:src.PAYBACKACCOUNT
       ,DST.PAYINTERESTACCOUNT                                 --还息帐号:src.PAYINTERESTACCOUNT
       ,DST.OWEINTERESTDAYS                                    --欠息天数:src.OWEINTERESTDAYS
       ,DST.TABALANCE                                          --分期业务欠本金:src.TABALANCE
       ,DST.TAINTERESTBALANCE                                  --分期业务欠利息:src.TAINTERESTBALANCE
       ,DST.TATIMES                                            --累计欠款期数:src.TATIMES
       ,DST.LCATIMES                                           --连续欠款期数:src.LCATIMES
       ,DST.SALEDATE                                           --卖出日期:src.SALEDATE
       ,DST.FINISHTYPE                                         --终结类型:src.FINISHTYPE
       ,DST.FINISHDATE                                         --终结日期:src.FINISHDATE
       ,DST.MFAREAID                                           --主机地区号:src.MFAREAID
       ,DST.MFORGID                                            --主机机构号:src.MFORGID
       ,DST.MFUSERID                                           --主机柜员号:src.MFUSERID
       ,DST.OPERATEORGID                                       --经办机构:src.OPERATEORGID
       ,DST.OPERATEUSERID                                      --经办人:src.OPERATEUSERID
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTDATE                                          --输入日期:src.INPUTDATE
       ,DST.INOUTFLAG                                          --（Del）表内表外标志:src.INOUTFLAG
       ,DST.DEALFLAG                                           --（Del）处理标志:src.DEALFLAG
       ,DST.OCCURDATE                                          --（Del）发生日期:src.OCCURDATE
       ,DST.BUSINESSPROP                                       --（Del）贷款成数:src.BUSINESSPROP
       ,DST.BENEFITCORP                                        --（Del）受益人:src.BENEFITCORP
       ,DST.ACTUALTERMMONTH                                    --（Del）实际期限月:src.ACTUALTERMMONTH
       ,DST.ACTUALTERMDAY                                      --（Del）实际期限日:src.ACTUALTERMDAY
       ,DST.BASERATETYPE                                       --（Del）基准利率类型:src.BASERATETYPE
       ,DST.BASERATE                                           --（Del）基准利率:src.BASERATE
       ,DST.RATEFLOATTYPE                                      --（Del）浮动类型:src.RATEFLOATTYPE
       ,DST.RATEFLOAT                                          --（Del）浮动利率:src.RATEFLOAT
       ,DST.TIMSFLAG                                           --（Del）分期业务标志:src.TIMSFLAG
       ,DST.BAILRATIO                                          --（Del）担保费率:src.BAILRATIO
       ,DST.LOGOUTDATE                                         --（Del）注销日期:src.LOGOUTDATE
       ,DST.CANCELLOGOUTDATE                                   --（Del）解除注销日期:src.CANCELLOGOUTDATE
       ,DST.BAILSUM                                            --（Del）保证金金额:src.BAILSUM
       ,DST.BAILACCOUNT                                        --（Del）保证金帐号:src.BAILACCOUNT
       ,DST.PURPOSE                                            --（Del）用途:src.PURPOSE
       ,DST.ADVANCEFLAG                                        --垫款标志:src.ADVANCEFLAG
       ,DST.RELATIVEDUEBILLNO                                  --相关借据流水号:src.RELATIVEDUEBILLNO
       ,DST.ACTUALARTIFICIALNO                                 --实际合同号:src.ACTUALARTIFICIALNO
       ,DST.ACCOUNTNO                                          --结算帐号:src.ACCOUNTNO
       ,DST.LOANACCOUNTNO                                      --贷款入账账号:src.LOANACCOUNTNO
       ,DST.SECONDPAYACCOUNT                                   --第二还款帐号:src.SECONDPAYACCOUNT
       ,DST.ADJUSTRATETYPE                                     --利率调整方式:src.ADJUSTRATETYPE
       ,DST.ADJUSTRATETERM                                     --利率调整月数:src.ADJUSTRATETERM
       ,DST.OVERINTTYPE                                        --逾期计息方式:src.OVERINTTYPE
       ,DST.RATEADJUSTCYC                                      --利率调整周期:src.RATEADJUSTCYC
       ,DST.PDGACCOUNTNO                                       --手续费支出帐号:src.PDGACCOUNTNO
       ,DST.DEDUCTDATE                                         --扣款日期:src.DEDUCTDATE
       ,DST.FZANBALANCE                                        --发展商入帐净额:src.FZANBALANCE
       ,DST.ACCEPTINTTYPE                                      --收息类型:src.ACCEPTINTTYPE
       ,DST.RATIO                                              --比例:src.RATIO
       ,DST.THIRDPARTYADD1                                     --（new）涉及第三方地址1:src.THIRDPARTYADD1
       ,DST.THIRDPARTYZIP1                                     --（new）第三方法人邮编1:src.THIRDPARTYZIP1
       ,DST.THIRDPARTYADD2                                     --（new）涉及第三方地址2:src.THIRDPARTYADD2
       ,DST.THIRDPARTYZIP2                                     --（new）第三方法人邮编2:src.THIRDPARTYZIP2
       ,DST.TERMDATE1                                          --最晚装运期:src.TERMDATE1
       ,DST.TERMDATE2                                          --交单期:src.TERMDATE2
       ,DST.TERMDATE3                                          --付款期限:src.TERMDATE3
       ,DST.DESCRIBE2                                          --描述2:src.DESCRIBE2
       ,DST.FIXCYC                                             --固定周期:src.FIXCYC
       ,DST.THIRDPARTY1                                        --（new）涉及第三方1:src.THIRDPARTY1
       ,DST.THIRDPARTYID1                                      --（new）第三方法人代码1:src.THIRDPARTYID1
       ,DST.THIRDPARTY2                                        --（new）涉及第三方2:src.THIRDPARTY2
       ,DST.THIRDPARTY3                                        --（new）涉及第三方3:src.THIRDPARTY3
       ,DST.TYPE1                                              --通知行类别:src.TYPE1
       ,DST.TYPE2                                              --受益行类别:src.TYPE2
       ,DST.TYPE3                                              --议付行类别:src.TYPE3
       ,DST.BILLNO                                             --票据号:src.BILLNO
       ,DST.FLAG1                                              --（new）是否1:src.FLAG1
       ,DST.FLAG2                                              --（new）是否2:src.FLAG2
       ,DST.FLAG3                                              --（new）是否3:src.FLAG3
       ,DST.THIRDPARTYREGION                                   --涉及第三方所在地区和国家:src.THIRDPARTYREGION
       ,DST.THIRDPARTYACCOUNTS                                 --（new）第三方帐号:src.THIRDPARTYACCOUNTS
       ,DST.CARGOINFO                                          --（new）货物名称:src.CARGOINFO
       ,DST.SECURITIESTYPE                                     --（new）有价证券类型:src.SECURITIESTYPE
       ,DST.SECURITIESREGION                                   --（new）有价证券发行地:src.SECURITIESREGION
       ,DST.ABOUTBANKID2                                       --受益行行号:src.ABOUTBANKID2
       ,DST.ABOUTBANKNAME2                                     --受益行行名:src.ABOUTBANKNAME2
       ,DST.ABOUTBANKID3                                       --议付行行号:src.ABOUTBANKID3
       ,DST.ABOUTBANKNAME                                      --收款行行名:src.ABOUTBANKNAME
       ,DST.ABOUTBANKID                                        --收款行行号:src.ABOUTBANKID
       ,DST.OLDLCTERMTYPE                                      --（new）原信用证期限类型:src.OLDLCTERMTYPE
       ,DST.NEGOTIATENO                                        --押汇编号:src.NEGOTIATENO
       ,DST.CREDITKIND                                         --贷款形式:src.CREDITKIND
       ,DST.GATHERINGNAME                                      --收款人户名:src.GATHERINGNAME
       ,DST.PREINTTYPE                                         --预收息标志:src.PREINTTYPE
       ,DST.RESUMEINTTYPE                                      --计复息标志:src.RESUMEINTTYPE
       ,DST.GUARANTYNO                                         --抵质押物编号:src.GUARANTYNO
       ,DST.PZTYPE                                             --凭证种类:src.PZTYPE
       ,DST.GRACEPERIOD                                        --还款宽限期（月）:src.GRACEPERIOD
       ,DST.OLDLCVALIDDATE                                     --（new）原信用证效期:src.OLDLCVALIDDATE
       ,DST.MFEEPAYMETHOD                                      --管理费支付方式:src.MFEEPAYMETHOD
       ,DST.DESCRIBE1                                          --描述1:src.DESCRIBE1
       ,DST.TRADECONTRACTNO                                    --（new）相关贸易合同号:src.TRADECONTRACTNO
       ,DST.LOANTYPE                                           --贷款类型:src.LOANTYPE
       ,DST.FIXTERM                                            --周期:src.FIXTERM
       ,DST.CANCELSUM                                          --核销金额:src.CANCELSUM
       ,DST.CANCELINTEREST                                     --核销利息:src.CANCELINTEREST
       ,DST.BAILACOUNT                                         --保证金帐号:src.BAILACOUNT
       ,DST.CLASSIFY4                                          --四级分类:src.CLASSIFY4
       ,DST.CLASSIFYRESULT                                     --原四级分类:src.CLASSIFYRESULT
       ,DST.RETURNTYPE                                         --还款方式:src.RETURNTYPE
       ,DST.BAILPERCENT                                        --保证金比例:src.BAILPERCENT
       ,DST.PAYMENTTYPE                                        --信用证付款期限:src.PAYMENTTYPE
       ,DST.TERMSFREQ                                          --还款频率:src.TERMSFREQ
       ,DST.OVERDUEDATE                                        --逾期日期:src.OVERDUEDATE
       ,DST.OWEINTERESTDATE                                    --欠息日期:src.OWEINTERESTDATE
       ,DST.LCSTATUS                                           --信用证状态:src.LCSTATUS
       ,DST.ICHANGEDATE                                        --<null>:src.ICHANGEDATE
       ,DST.VOUCHTYPE                                          --主要担保方式:src.VOUCHTYPE
       ,DST.MANAGEUSERID                                       --管户用户ID:src.MANAGEUSERID
       ,DST.MANAGEORGID                                        --管户机构ID:src.MANAGEORGID
       ,DST.CORPORATEORGID                                     --法人机构号:src.CORPORATEORGID
       ,DST.NEWBUSINESSRATE                                    --新执行利率:src.NEWBUSINESSRATE
       ,DST.IRREGULATEFLAG                                     --利率调整标志:src.IRREGULATEFLAG
       ,DST.TENCLASSIFYRESULT                                  --新十级分类编码:src.TENCLASSIFYRESULT
       ,DST.LASTTENCLASSIFYRESULT                              --原十级分类编码:src.LASTTENCLASSIFYRESULT
       ,DST.LASTCLASSIFYRESULT                                 --原五级分类编码:src.LASTCLASSIFYRESULT
       ,DST.SORTNO                                             --机构排序号:src.SORTNO
       ,DST.MIGRATEFLAG                                        --移植标志:src.MIGRATEFLAG
       ,DST.BADDFLAG                                           --逾期初始标志:src.BADDFLAG
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_LN_XDXT_BUSINESS_DUEBILL DST 
   LEFT JOIN F_LN_XDXT_BUSINESS_DUEBILL_INNTMP1 SRC 
     ON SRC.SERIALNO            = DST.SERIALNO 
    AND SRC.SUBJECTNO           = DST.SUBJECTNO 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.SERIALNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_BUSINESS_DUEBILL_INNTMP2 = sqlContext.sql(sql)
dfn="F_LN_XDXT_BUSINESS_DUEBILL/"+V_DT+".parquet"
F_LN_XDXT_BUSINESS_DUEBILL_INNTMP2=F_LN_XDXT_BUSINESS_DUEBILL_INNTMP2.unionAll(F_LN_XDXT_BUSINESS_DUEBILL_INNTMP1)
F_LN_XDXT_BUSINESS_DUEBILL_INNTMP1.cache()
F_LN_XDXT_BUSINESS_DUEBILL_INNTMP2.cache()
nrowsi = F_LN_XDXT_BUSINESS_DUEBILL_INNTMP1.count()
nrowsa = F_LN_XDXT_BUSINESS_DUEBILL_INNTMP2.count()
F_LN_XDXT_BUSINESS_DUEBILL_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_LN_XDXT_BUSINESS_DUEBILL_INNTMP1.unpersist()
F_LN_XDXT_BUSINESS_DUEBILL_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_XDXT_BUSINESS_DUEBILL lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_LN_XDXT_BUSINESS_DUEBILL/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_BUSINESS_DUEBILL_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_BUSINESS_DUEBILL_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_BUSINESS_DUEBILL/"+V_DT+".parquet /"+dbname+"/F_LN_XDXT_BUSINESS_DUEBILL_BK/"+V_DT+".parquet")
