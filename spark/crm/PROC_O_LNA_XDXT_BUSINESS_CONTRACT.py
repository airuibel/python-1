#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_BUSINESS_CONTRACT').setMaster(sys.argv[2])
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

O_LN_XDXT_BUSINESS_CONTRACT = sqlContext.read.parquet(hdfs+'/O_LN_XDXT_BUSINESS_CONTRACT/*')
O_LN_XDXT_BUSINESS_CONTRACT.registerTempTable("O_LN_XDXT_BUSINESS_CONTRACT")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_BUSINESS_CONTRACT/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_BUSINESS_CONTRACT_BK/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_BUSINESS_CONTRACT/"+V_DT+".parquet")


F_LN_XDXT_BUSINESS_CONTRACT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BUSINESS_CONTRACT/*')
F_LN_XDXT_BUSINESS_CONTRACT.registerTempTable("F_LN_XDXT_BUSINESS_CONTRACT")

sql = """
 SELECT A.SERIALNO              AS SERIALNO 
       ,A.RELATIVESERIALNO      AS RELATIVESERIALNO 
       ,A.ARTIFICIALNO          AS ARTIFICIALNO 
       ,A.OCCURDATE             AS OCCURDATE 
       ,A.CUSTOMERID            AS CUSTOMERID 
       ,A.CUSTOMERNAME          AS CUSTOMERNAME 
       ,A.BUSINESSTYPE          AS BUSINESSTYPE 
       ,A.OCCURTYPE             AS OCCURTYPE 
       ,A.CREDITCYCLE           AS CREDITCYCLE 
       ,A.CREDITAGGREEMENT      AS CREDITAGGREEMENT 
       ,A.RELATIVEAGREEMENT     AS RELATIVEAGREEMENT 
       ,A.BILLNUM               AS BILLNUM 
       ,A.HOUSETYPE             AS HOUSETYPE 
       ,A.LCTERMTYPE            AS LCTERMTYPE 
       ,A.RISKATTRIBUTE         AS RISKATTRIBUTE 
       ,A.BUSINESSCURRENCY      AS BUSINESSCURRENCY 
       ,A.BUSINESSSUM           AS BUSINESSSUM 
       ,A.BUSINESSPROP          AS BUSINESSPROP 
       ,A.TERMYEAR              AS TERMYEAR 
       ,A.TERMMONTH             AS TERMMONTH 
       ,A.TERMDAY               AS TERMDAY 
       ,A.BASERATETYPE          AS BASERATETYPE 
       ,A.BASERATE              AS BASERATE 
       ,A.RATEFLOATTYPE         AS RATEFLOATTYPE 
       ,A.RATEFLOAT             AS RATEFLOAT 
       ,A.BUSINESSRATE          AS BUSINESSRATE 
       ,A.PDGRATIO              AS PDGRATIO 
       ,A.PDGSUM                AS PDGSUM 
       ,A.DISCOUNTSUM           AS DISCOUNTSUM 
       ,A.BAILRATIO             AS BAILRATIO 
       ,A.BAILCURRENCY          AS BAILCURRENCY 
       ,A.BAILSUM               AS BAILSUM 
       ,A.BAILACCOUNT           AS BAILACCOUNT 
       ,A.PAYTIMES              AS PAYTIMES 
       ,A.PAYCYC                AS PAYCYC 
       ,A.GRACEPERIOD           AS GRACEPERIOD 
       ,A.OLDLCNO               AS OLDLCNO 
       ,A.OLDLCCURRENCY         AS OLDLCCURRENCY 
       ,A.OLDLCSUM              AS OLDLCSUM 
       ,A.OLDLCLOADINGDATE      AS OLDLCLOADINGDATE 
       ,A.OLDLCVALIDDATE        AS OLDLCVALIDDATE 
       ,A.DIRECTION             AS DIRECTION 
       ,A.PURPOSE               AS PURPOSE 
       ,A.PAYSOURCE             AS PAYSOURCE 
       ,A.PUTOUTDATE            AS PUTOUTDATE 
       ,A.MATURITY              AS MATURITY 
       ,A.CARGOINFO             AS CARGOINFO 
       ,A.PROJECTNAME           AS PROJECTNAME 
       ,A.SECURITIESREGION      AS SECURITIESREGION 
       ,A.FLAG1                 AS FLAG1 
       ,A.FLAG2                 AS FLAG2 
       ,A.FLAG3                 AS FLAG3 
       ,A.TRADESUM              AS TRADESUM 
       ,A.LCNO                  AS LCNO 
       ,A.BEGINDATE             AS BEGINDATE 
       ,A.ENDDATE               AS ENDDATE 
       ,A.VOUCHCLASS            AS VOUCHCLASS 
       ,A.VOUCHTYPE             AS VOUCHTYPE 
       ,A.VOUCHTYPE1            AS VOUCHTYPE1 
       ,A.VOUCHTYPE2            AS VOUCHTYPE2 
       ,A.VOUCHFLAG             AS VOUCHFLAG 
       ,A.LOWRISK               AS LOWRISK 
       ,A.APPLYTYPE             AS APPLYTYPE 
       ,A.ORIGINALPUTOUTDATE    AS ORIGINALPUTOUTDATE 
       ,A.EXTENDTIMES           AS EXTENDTIMES 
       ,A.LNGOTIMES             AS LNGOTIMES 
       ,A.GOLNTIMES             AS GOLNTIMES 
       ,A.DRTIMES               AS DRTIMES 
       ,A.PUTOUTSUM             AS PUTOUTSUM 
       ,A.ACTUALPUTOUTSUM       AS ACTUALPUTOUTSUM 
       ,A.BALANCE               AS BALANCE 
       ,A.NORMALBALANCE         AS NORMALBALANCE 
       ,A.OVERDUEBALANCE        AS OVERDUEBALANCE 
       ,A.DULLBALANCE           AS DULLBALANCE 
       ,A.BADBALANCE            AS BADBALANCE 
       ,A.INTERESTBALANCE1      AS INTERESTBALANCE1 
       ,A.INTERESTBALANCE2      AS INTERESTBALANCE2 
       ,A.FINEBALANCE1          AS FINEBALANCE1 
       ,A.FINEBALANCE2          AS FINEBALANCE2 
       ,A.OVERDUEDAYS           AS OVERDUEDAYS 
       ,A.TABALANCE             AS TABALANCE 
       ,A.TAINTERESTBALANCE     AS TAINTERESTBALANCE 
       ,A.TATIMES               AS TATIMES 
       ,A.LCATIMES              AS LCATIMES 
       ,A.CLASSIFYRESULT        AS CLASSIFYRESULT 
       ,A.CLASSIFYDATE          AS CLASSIFYDATE 
       ,A.BAILRATE              AS BAILRATE 
       ,A.FINISHTYPE            AS FINISHTYPE 
       ,A.FINISHDATE            AS FINISHDATE 
       ,A.REINFORCEFLAG         AS REINFORCEFLAG 
       ,A.MANAGEORGID           AS MANAGEORGID 
       ,A.MANAGEUSERID          AS MANAGEUSERID 
       ,A.RECOVERYORGID         AS RECOVERYORGID 
       ,A.RECOVERYUSERID        AS RECOVERYUSERID 
       ,A.OPERATEORGID          AS OPERATEORGID 
       ,A.OPERATEUSERID         AS OPERATEUSERID 
       ,A.OPERATEDATE           AS OPERATEDATE 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.UPDATEDATE            AS UPDATEDATE 
       ,A.PIGEONHOLEDATE        AS PIGEONHOLEDATE 
       ,A.REMARK                AS REMARK 
       ,A.FLAG4                 AS FLAG4 
       ,A.PAYCURRENCY           AS PAYCURRENCY 
       ,A.PAYDATE               AS PAYDATE 
       ,A.FLAG5                 AS FLAG5 
       ,A.CLASSIFYSUM1          AS CLASSIFYSUM1 
       ,A.CLASSIFYSUM2          AS CLASSIFYSUM2 
       ,A.CLASSIFYSUM3          AS CLASSIFYSUM3 
       ,A.CLASSIFYSUM4          AS CLASSIFYSUM4 
       ,A.CLASSIFYSUM5          AS CLASSIFYSUM5 
       ,A.OPERATETYPE           AS OPERATETYPE 
       ,A.FUNDSOURCE            AS FUNDSOURCE 
       ,A.CREDITFREEZEFLAG      AS CREDITFREEZEFLAG 
       ,A.ADJUSTRATETYPE        AS ADJUSTRATETYPE 
       ,A.OVERINTTYPE           AS OVERINTTYPE 
       ,A.RATEADJUSTCYC         AS RATEADJUSTCYC 
       ,A.THIRDPARTYADD3        AS THIRDPARTYADD3 
       ,A.LOANTERM              AS LOANTERM 
       ,A.TEMPSAVEFLAG          AS TEMPSAVEFLAG 
       ,A.OVERDUEDATE           AS OVERDUEDATE 
       ,A.OWEINTERESTDATE       AS OWEINTERESTDATE 
       ,A.FREEZEFLAG            AS FREEZEFLAG 
       ,A.BAILACCOUNTNO         AS BAILACCOUNTNO 
       ,A.CONEXCFLOATPER        AS CONEXCFLOATPER 
       ,A.APPLICATIONSORT       AS APPLICATIONSORT 
       ,A.OUGHTRATE             AS OUGHTRATE 
       ,A.ACCEPTANCEMATURITY    AS ACCEPTANCEMATURITY 
       ,A.GUARANTEENO           AS GUARANTEENO 
       ,A.GUARANTEESORT         AS GUARANTEESORT 
       ,A.CAPITALDEADLINE       AS CAPITALDEADLINE 
       ,A.PARTICIPATECATEGORY   AS PARTICIPATECATEGORY 
       ,A.ACCEPTANCETYPE        AS ACCEPTANCETYPE 
       ,A.ACCEPTANCEMAXMATURITY AS ACCEPTANCEMAXMATURITY 
       ,A.REMITTERACCOUNT       AS REMITTERACCOUNT 
       ,A.BANKACCOUNT           AS BANKACCOUNT 
       ,A.LOANTYPE              AS LOANTYPE 
       ,A.LIMITATIONNO          AS LIMITATIONNO 
       ,A.LIMITATIONTYPE        AS LIMITATIONTYPE 
       ,A.HOUSEADDRESS          AS HOUSEADDRESS 
       ,A.BUILDINGAREA          AS BUILDINGAREA 
       ,A.INSIDEAREA            AS INSIDEAREA 
       ,A.REPAYMENTPERIOD       AS REPAYMENTPERIOD 
       ,A.PAYCYCMON             AS PAYCYCMON 
       ,A.PAYINDATE             AS PAYINDATE 
       ,A.ECONOMICNATURE        AS ECONOMICNATURE 
       ,A.PLUSMINUS             AS PLUSMINUS 
       ,A.PLUSMINUSNO           AS PLUSMINUSNO 
       ,A.RATETYPE              AS RATETYPE 
       ,A.RATEDEADLINE          AS RATEDEADLINE 
       ,A.ISFINPAY              AS ISFINPAY 
       ,A.ISFINDIS              AS ISFINDIS 
       ,A.ISONEQ                AS ISONEQ 
       ,A.ISONPUT               AS ISONPUT 
       ,A.ISSIGN                AS ISSIGN 
       ,A.ISAGR                 AS ISAGR 
       ,A.ISSUNCRE              AS ISSUNCRE 
       ,A.ISBUILDING            AS ISBUILDING 
       ,A.PAYEENUMBER           AS PAYEENUMBER 
       ,A.PAYEEBANK             AS PAYEEBANK 
       ,A.CHARGEMONTH           AS CHARGEMONTH 
       ,A.TRUSTEEACCOUNT        AS TRUSTEEACCOUNT 
       ,A.TRUSTEENO             AS TRUSTEENO 
       ,A.DISCOUNTINTERESTSUM   AS DISCOUNTINTERESTSUM 
       ,A.DISCOUNTRATE          AS DISCOUNTRATE 
       ,A.DISCOUNTDATE          AS DISCOUNTDATE 
       ,A.BANKNO                AS BANKNO 
       ,A.OVERPLUSMINUS         AS OVERPLUSMINUS 
       ,A.OVERPLUSMINUSNO       AS OVERPLUSMINUSNO 
       ,A.OVERRATEMETHOD        AS OVERRATEMETHOD 
       ,A.OLDCRENO              AS OLDCRENO 
       ,A.PAYTYPE               AS PAYTYPE 
       ,A.IMPAWNSUM             AS IMPAWNSUM 
       ,A.DEDUCTFLAG1           AS DEDUCTFLAG1 
       ,A.CORESTATUS            AS CORESTATUS 
       ,A.FIRSTDEADLINE         AS FIRSTDEADLINE 
       ,A.LASTDEADLINE          AS LASTDEADLINE 
       ,A.CREDITCONDITION       AS CREDITCONDITION 
       ,A.PAYDEADLINE           AS PAYDEADLINE 
       ,A.FIRSTPAY              AS FIRSTPAY 
       ,A.LASTPAY               AS LASTPAY 
       ,A.DEADLINEMETHOD        AS DEADLINEMETHOD 
       ,A.CREDITMAX             AS CREDITMAX 
       ,A.BENEFICIARY           AS BENEFICIARY 
       ,A.TRUSTFUND             AS TRUSTFUND 
       ,A.TRUSTNAME             AS TRUSTNAME 
       ,A.TRUSTACCOUT           AS TRUSTACCOUT 
       ,A.TRUSTNO               AS TRUSTNO 
       ,A.TRUSTTYPE             AS TRUSTTYPE 
       ,A.DUNFLAG               AS DUNFLAG 
       ,A.MAXDEADLINE           AS MAXDEADLINE 
       ,A.BILLNO                AS BILLNO 
       ,A.BILLCURRENCY          AS BILLCURRENCY 
       ,A.BILLSUM               AS BILLSUM 
       ,A.TALKDATE              AS TALKDATE 
       ,A.BILLBANK              AS BILLBANK 
       ,A.ACCEPTANCE            AS ACCEPTANCE 
       ,A.ADVANCEDAYS           AS ADVANCEDAYS 
       ,A.HOUSESUM              AS HOUSESUM 
       ,A.TENCLASSIFYRESULT     AS TENCLASSIFYRESULT 
       ,A.LASTTENCLASSIFYRESULT AS LASTTENCLASSIFYRESULT 
       ,A.LASTCLASSIFYRESULT    AS LASTCLASSIFYRESULT 
       ,A.PLUSCREDIT            AS PLUSCREDIT 
       ,A.PLUSRATIO             AS PLUSRATIO 
       ,A.PLUSNUM               AS PLUSNUM 
       ,A.PLUSNO                AS PLUSNO 
       ,A.VOUCHNAME             AS VOUCHNAME 
       ,A.GROUPCREDIT           AS GROUPCREDIT 
       ,A.VOUCHMIN              AS VOUCHMIN 
       ,A.OWNREF                AS OWNREF 
       ,A.CONCERT               AS CONCERT 
       ,A.PLANCERT              AS PLANCERT 
       ,A.HOUSECERTIFICATE      AS HOUSECERTIFICATE 
       ,A.CONSTRUCTIONCERTIFICATE       AS CONSTRUCTIONCERTIFICATE 
       ,A.HOUSELICENSE          AS HOUSELICENSE 
       ,A.MAXSELLING            AS MAXSELLING 
       ,A.DUTYDEADLINE          AS DUTYDEADLINE 
       ,A.COLTEXT               AS COLTEXT 
       ,A.CONFIRMVALUE          AS CONFIRMVALUE 
       ,A.FROZESUM              AS FROZESUM 
       ,A.CONCONTROL            AS CONCONTROL 
       ,A.PHASEOPINION          AS PHASEOPINION 
       ,A.CYCUSE                AS CYCUSE 
       ,A.RECEIVABLE            AS RECEIVABLE 
       ,A.HIGHPERCENT           AS HIGHPERCENT 
       ,A.ACCOUNTDATE           AS ACCOUNTDATE 
       ,A.DEVELOPERQUALIFICATION        AS DEVELOPERQUALIFICATION 
       ,A.EXCHANGERATE          AS EXCHANGERATE 
       ,A.LOANMAX               AS LOANMAX 
       ,A.LCTYPE                AS LCTYPE 
       ,A.EXCHANGETYPE          AS EXCHANGETYPE 
       ,A.LIMDATE               AS LIMDATE 
       ,A.LIMEFFECT             AS LIMEFFECT 
       ,A.LIMDEADLINE           AS LIMDEADLINE 
       ,A.TRANSFORMFLAG         AS TRANSFORMFLAG 
       ,A.SHIFTTYPE             AS SHIFTTYPE 
       ,A.PARTICIPATENATURE     AS PARTICIPATENATURE 
       ,A.CLASSIFYORGID         AS CLASSIFYORGID 
       ,A.OWEINTERESTDAYS       AS OWEINTERESTDAYS 
       ,A.GUARANTEEPROJECT      AS GUARANTEEPROJECT 
       ,A.INTERESTDAY           AS INTERESTDAY 
       ,A.ACTUALLYPAY           AS ACTUALLYPAY 
       ,A.DEALBALANCEVALUE      AS DEALBALANCEVALUE 
       ,A.RESONTYPE             AS RESONTYPE 
       ,A.SHIFTBALANCE          AS SHIFTBALANCE 
       ,A.CANCELSUM             AS CANCELSUM 
       ,A.CANCELINTEREST        AS CANCELINTEREST 
       ,A.CONFIRMRESON          AS CONFIRMRESON 
       ,A.BANKSNO               AS BANKSNO 
       ,A.DISCOUNTINTEREST      AS DISCOUNTINTEREST 
       ,A.BACURRENCY            AS BACURRENCY 
       ,A.THIRDUSERID           AS THIRDUSERID 
       ,A.DISCOUNTRATIO         AS DISCOUNTRATIO 
       ,A.DEDATE                AS DEDATE 
       ,A.TEMPLATETYPE          AS TEMPLATETYPE 
       ,A.RELATIVEGROUPSERIALNO AS RELATIVEGROUPSERIALNO 
       ,A.LIMITATIONTERM        AS LIMITATIONTERM 
       ,A.MIGRATEFLAG           AS MIGRATEFLAG 
       ,A.PUTOUTORGID           AS PUTOUTORGID 
       ,A.ISINSURANCE           AS ISINSURANCE 
       ,A.ACTUALBAILSUM         AS ACTUALBAILSUM 
       ,A.BAILEXCHANGERATE      AS BAILEXCHANGERATE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
       ,A.SUBBILLTYPE           AS SUBBILLTYPE 
   FROM O_LN_XDXT_BUSINESS_CONTRACT A                          --业务合同信息
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_BUSINESS_CONTRACT_INNTMP1 = sqlContext.sql(sql)
F_LN_XDXT_BUSINESS_CONTRACT_INNTMP1.registerTempTable("F_LN_XDXT_BUSINESS_CONTRACT_INNTMP1")

#F_LN_XDXT_BUSINESS_CONTRACT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BUSINESS_CONTRACT/*')
#F_LN_XDXT_BUSINESS_CONTRACT.registerTempTable("F_LN_XDXT_BUSINESS_CONTRACT")
sql = """
 SELECT DST.SERIALNO                                            --流水号:src.SERIALNO
       ,DST.RELATIVESERIALNO                                   --相关申请流水号:src.RELATIVESERIALNO
       ,DST.ARTIFICIALNO                                       --人工编号:src.ARTIFICIALNO
       ,DST.OCCURDATE                                          --发生日期:src.OCCURDATE
       ,DST.CUSTOMERID                                         --客户编号:src.CUSTOMERID
       ,DST.CUSTOMERNAME                                       --客户名称:src.CUSTOMERNAME
       ,DST.BUSINESSTYPE                                       --业务品种:src.BUSINESSTYPE
       ,DST.OCCURTYPE                                          --发生类型:src.OCCURTYPE
       ,DST.CREDITCYCLE                                        --额度是否循环:src.CREDITCYCLE
       ,DST.CREDITAGGREEMENT                                   --使用授信协议号:src.CREDITAGGREEMENT
       ,DST.RELATIVEAGREEMENT                                  --其他相关协议号:src.RELATIVEAGREEMENT
       ,DST.BILLNUM                                            --票据数量（张）:src.BILLNUM
       ,DST.HOUSETYPE                                          --房产类型:src.HOUSETYPE
       ,DST.LCTERMTYPE                                         --信用证期限类型:src.LCTERMTYPE
       ,DST.RISKATTRIBUTE                                      --风险类型:src.RISKATTRIBUTE
       ,DST.BUSINESSCURRENCY                                   --币种:src.BUSINESSCURRENCY
       ,DST.BUSINESSSUM                                        --金额:src.BUSINESSSUM
       ,DST.BUSINESSPROP                                       --贷款成数:src.BUSINESSPROP
       ,DST.TERMYEAR                                           --期限年:src.TERMYEAR
       ,DST.TERMMONTH                                          --期限月:src.TERMMONTH
       ,DST.TERMDAY                                            --期限日:src.TERMDAY
       ,DST.BASERATETYPE                                       --基准利率类型:src.BASERATETYPE
       ,DST.BASERATE                                           --基准利率:src.BASERATE
       ,DST.RATEFLOATTYPE                                      --浮动类型:src.RATEFLOATTYPE
       ,DST.RATEFLOAT                                          --利率浮动:src.RATEFLOAT
       ,DST.BUSINESSRATE                                       --利率:src.BUSINESSRATE
       ,DST.PDGRATIO                                           --手续费比例:src.PDGRATIO
       ,DST.PDGSUM                                             --手续费金额:src.PDGSUM
       ,DST.DISCOUNTSUM                                        --实付贴现金额:src.DISCOUNTSUM
       ,DST.BAILRATIO                                          --保证金比例:src.BAILRATIO
       ,DST.BAILCURRENCY                                       --保证金币种:src.BAILCURRENCY
       ,DST.BAILSUM                                            --保证金金额:src.BAILSUM
       ,DST.BAILACCOUNT                                        --保证金帐号:src.BAILACCOUNT
       ,DST.PAYTIMES                                           --还款期次:src.PAYTIMES
       ,DST.PAYCYC                                             --还款方式:src.PAYCYC
       ,DST.GRACEPERIOD                                        --还款宽限期（月）:src.GRACEPERIOD
       ,DST.OLDLCNO                                            --原信用证编号:src.OLDLCNO
       ,DST.OLDLCCURRENCY                                      --原信用证币种:src.OLDLCCURRENCY
       ,DST.OLDLCSUM                                           --原信用证金额:src.OLDLCSUM
       ,DST.OLDLCLOADINGDATE                                   --原信用证装期:src.OLDLCLOADINGDATE
       ,DST.OLDLCVALIDDATE                                     --原信用证效期:src.OLDLCVALIDDATE
       ,DST.DIRECTION                                          --投向:src.DIRECTION
       ,DST.PURPOSE                                            --用途:src.PURPOSE
       ,DST.PAYSOURCE                                          --还款来源:src.PAYSOURCE
       ,DST.PUTOUTDATE                                         --约定发放日:src.PUTOUTDATE
       ,DST.MATURITY                                           --到期日期:src.MATURITY
       ,DST.CARGOINFO                                          --货物名称:src.CARGOINFO
       ,DST.PROJECTNAME                                        --贷款项目名称:src.PROJECTNAME
       ,DST.SECURITIESREGION                                   --有价证券发行地:src.SECURITIESREGION
       ,DST.FLAG1                                              --是否1:src.FLAG1
       ,DST.FLAG2                                              --是否2:src.FLAG2
       ,DST.FLAG3                                              --是否3:src.FLAG3
       ,DST.TRADESUM                                           --贸易合同金额:src.TRADESUM
       ,DST.LCNO                                               --信用证编号（或承兑汇票号码）:src.LCNO
       ,DST.BEGINDATE                                          --起始日期:src.BEGINDATE
       ,DST.ENDDATE                                            --到期日:src.ENDDATE
       ,DST.VOUCHCLASS                                         --担保形式:src.VOUCHCLASS
       ,DST.VOUCHTYPE                                          --主要担保方式:src.VOUCHTYPE
       ,DST.VOUCHTYPE1                                         --担保方式1:src.VOUCHTYPE1
       ,DST.VOUCHTYPE2                                         --担保方式2:src.VOUCHTYPE2
       ,DST.VOUCHFLAG                                          --有无其他担保方式:src.VOUCHFLAG
       ,DST.LOWRISK                                            --是否低风险业务:src.LOWRISK
       ,DST.APPLYTYPE                                          --申请方式:src.APPLYTYPE
       ,DST.ORIGINALPUTOUTDATE                                 --首次发放日:src.ORIGINALPUTOUTDATE
       ,DST.EXTENDTIMES                                        --展期次数:src.EXTENDTIMES
       ,DST.LNGOTIMES                                          --借新还旧次数:src.LNGOTIMES
       ,DST.GOLNTIMES                                          --还旧借新次数:src.GOLNTIMES
       ,DST.DRTIMES                                            --债务重组次数:src.DRTIMES
       ,DST.PUTOUTSUM                                          --已出帐金额:src.PUTOUTSUM
       ,DST.ACTUALPUTOUTSUM                                    --已实际出帐金额:src.ACTUALPUTOUTSUM
       ,DST.BALANCE                                            --当前余额:src.BALANCE
       ,DST.NORMALBALANCE                                      --正常余额:src.NORMALBALANCE
       ,DST.OVERDUEBALANCE                                     --逾期余额:src.OVERDUEBALANCE
       ,DST.DULLBALANCE                                        --呆滞余额:src.DULLBALANCE
       ,DST.BADBALANCE                                         --呆帐余额:src.BADBALANCE
       ,DST.INTERESTBALANCE1                                   --表内欠息余额:src.INTERESTBALANCE1
       ,DST.INTERESTBALANCE2                                   --表外欠息余额:src.INTERESTBALANCE2
       ,DST.FINEBALANCE1                                       --本金罚息:src.FINEBALANCE1
       ,DST.FINEBALANCE2                                       --利息罚息:src.FINEBALANCE2
       ,DST.OVERDUEDAYS                                        --逾期天数:src.OVERDUEDAYS
       ,DST.TABALANCE                                          --分期业务欠本金:src.TABALANCE
       ,DST.TAINTERESTBALANCE                                  --分期业务欠利息:src.TAINTERESTBALANCE
       ,DST.TATIMES                                            --累计欠款期数:src.TATIMES
       ,DST.LCATIMES                                           --连续欠款期数:src.LCATIMES
       ,DST.CLASSIFYRESULT                                     --五级分类结果:src.CLASSIFYRESULT
       ,DST.CLASSIFYDATE                                       --最新风险分类时间:src.CLASSIFYDATE
       ,DST.BAILRATE                                           --保证金比率:src.BAILRATE
       ,DST.FINISHTYPE                                         --终结类型:src.FINISHTYPE
       ,DST.FINISHDATE                                         --终结日期:src.FINISHDATE
       ,DST.REINFORCEFLAG                                      --补登标志:src.REINFORCEFLAG
       ,DST.MANAGEORGID                                        --当前管户机构:src.MANAGEORGID
       ,DST.MANAGEUSERID                                       --当前管户人:src.MANAGEUSERID
       ,DST.RECOVERYORGID                                      --保全管理机构:src.RECOVERYORGID
       ,DST.RECOVERYUSERID                                     --保全管理人:src.RECOVERYUSERID
       ,DST.OPERATEORGID                                       --经办机构:src.OPERATEORGID
       ,DST.OPERATEUSERID                                      --经办人:src.OPERATEUSERID
       ,DST.OPERATEDATE                                        --经办日期:src.OPERATEDATE
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTDATE                                          --登记日期:src.INPUTDATE
       ,DST.UPDATEDATE                                         --更新日期:src.UPDATEDATE
       ,DST.PIGEONHOLEDATE                                     --归档日期:src.PIGEONHOLEDATE
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.FLAG4                                              --是否4:src.FLAG4
       ,DST.PAYCURRENCY                                        --付款币种:src.PAYCURRENCY
       ,DST.PAYDATE                                            --付款时间:src.PAYDATE
       ,DST.FLAG5                                              --转建行标志:src.FLAG5
       ,DST.CLASSIFYSUM1                                       --最新分类正常金额:src.CLASSIFYSUM1
       ,DST.CLASSIFYSUM2                                       --最新分类关注金额:src.CLASSIFYSUM2
       ,DST.CLASSIFYSUM3                                       --最新分类次级金额:src.CLASSIFYSUM3
       ,DST.CLASSIFYSUM4                                       --最新分类可疑金额:src.CLASSIFYSUM4
       ,DST.CLASSIFYSUM5                                       --最新分类损失金额:src.CLASSIFYSUM5
       ,DST.OPERATETYPE                                        --操作方式:src.OPERATETYPE
       ,DST.FUNDSOURCE                                         --资金来源:src.FUNDSOURCE
       ,DST.CREDITFREEZEFLAG                                   --额度是否冻结:src.CREDITFREEZEFLAG
       ,DST.ADJUSTRATETYPE                                     --利率调整方式:src.ADJUSTRATETYPE
       ,DST.OVERINTTYPE                                        --逾期计息方式:src.OVERINTTYPE
       ,DST.RATEADJUSTCYC                                      --利率调整周期:src.RATEADJUSTCYC
       ,DST.THIRDPARTYADD3                                     --涉及第三方地址3:src.THIRDPARTYADD3
       ,DST.LOANTERM                                           --期限:src.LOANTERM
       ,DST.TEMPSAVEFLAG                                       --暂存标志:src.TEMPSAVEFLAG
       ,DST.OVERDUEDATE                                        --逾期日期:src.OVERDUEDATE
       ,DST.OWEINTERESTDATE                                    --欠息日期:src.OWEINTERESTDATE
       ,DST.FREEZEFLAG                                         --冻结标志:src.FREEZEFLAG
       ,DST.BAILACCOUNTNO                                      --保证金账号:src.BAILACCOUNTNO
       ,DST.CONEXCFLOATPER                                     --合同逾期罚息浮动系数（%）:src.CONEXCFLOATPER
       ,DST.APPLICATIONSORT                                    --合同初始风险分类:src.APPLICATIONSORT
       ,DST.OUGHTRATE                                          --应执行年利率:src.OUGHTRATE
       ,DST.ACCEPTANCEMATURITY                                 --承兑汇票到期日:src.ACCEPTANCEMATURITY
       ,DST.GUARANTEENO                                        --保函编号:src.GUARANTEENO
       ,DST.GUARANTEESORT                                      --保函种类分类:src.GUARANTEESORT
       ,DST.CAPITALDEADLINE                                    --本金宽限日数:src.CAPITALDEADLINE
       ,DST.PARTICIPATECATEGORY                                --参与性质（银团）（银团贷款）:src.PARTICIPATECATEGORY
       ,DST.ACCEPTANCETYPE                                     --承兑方式:src.ACCEPTANCETYPE
       ,DST.ACCEPTANCEMAXMATURITY                              --承兑汇票最大到期日期:src.ACCEPTANCEMAXMATURITY
       ,DST.REMITTERACCOUNT                                    --出票人账号:src.REMITTERACCOUNT
       ,DST.BANKACCOUNT                                        --结算户帐号:src.BANKACCOUNT
       ,DST.LOANTYPE                                           --放贷到期日控制方式:src.LOANTYPE
       ,DST.LIMITATIONNO                                       --政府融资平台投向:src.LIMITATIONNO
       ,DST.LIMITATIONTYPE                                     --授信额度产品名称:src.LIMITATIONTYPE
       ,DST.HOUSEADDRESS                                       --房产地址（老系统）:src.HOUSEADDRESS
       ,DST.BUILDINGAREA                                       --房产建筑面积（老系统）:src.BUILDINGAREA
       ,DST.INSIDEAREA                                         --房产套内面积（老系统）:src.INSIDEAREA
       ,DST.REPAYMENTPERIOD                                    --还本宽限期:src.REPAYMENTPERIOD
       ,DST.PAYCYCMON                                          --还本周期月数:src.PAYCYCMON
       ,DST.PAYINDATE                                          --缴息日期:src.PAYINDATE
       ,DST.ECONOMICNATURE                                     --经济性质:src.ECONOMICNATURE
       ,DST.PLUSMINUS                                          --利率加减符号:src.PLUSMINUS
       ,DST.PLUSMINUSNO                                        --利率加减码:src.PLUSMINUSNO
       ,DST.RATETYPE                                           --利率类别:src.RATETYPE
       ,DST.RATEDEADLINE                                       --利息宽限日数:src.RATEDEADLINE
       ,DST.ISFINPAY                                           --是否财政还款:src.ISFINPAY
       ,DST.ISFINDIS                                           --是否财政贴息:src.ISFINDIS
       ,DST.ISONEQ                                             --是否政府融资平台:src.ISONEQ
       ,DST.ISONPUT                                            --是否柜面直接放款:src.ISONPUT
       ,DST.ISSIGN                                             --是否签署额度协议:src.ISSIGN
       ,DST.ISAGR                                              --是否涉农:src.ISAGR
       ,DST.ISSUNCRE                                           --是否阳光授信:src.ISSUNCRE
       ,DST.ISBUILDING                                         --是否在建工程（老系统）:src.ISBUILDING
       ,DST.PAYEENUMBER                                        --收款人帐号:src.PAYEENUMBER
       ,DST.PAYEEBANK                                          --收款人开户行名称:src.PAYEEBANK
       ,DST.CHARGEMONTH                                        --结息周期:src.CHARGEMONTH
       ,DST.TRUSTEEACCOUNT                                     --委托人的基金账号:src.TRUSTEEACCOUNT
       ,DST.TRUSTEENO                                          --收息账号:src.TRUSTEENO
       ,DST.DISCOUNTINTERESTSUM                                --贴现利息汇总:src.DISCOUNTINTERESTSUM
       ,DST.DISCOUNTRATE                                       --贴现率:src.DISCOUNTRATE
       ,DST.DISCOUNTDATE                                       --贴现日期:src.DISCOUNTDATE
       ,DST.BANKNO                                             --银行编号:src.BANKNO
       ,DST.OVERPLUSMINUS                                      --逾期利率加减符号:src.OVERPLUSMINUS
       ,DST.OVERPLUSMINUSNO                                    --逾期利率加减码:src.OVERPLUSMINUSNO
       ,DST.OVERRATEMETHOD                                     --逾期利率依据方式:src.OVERRATEMETHOD
       ,DST.OLDCRENO                                           --原承诺账号:src.OLDCRENO
       ,DST.PAYTYPE                                            --签发模式:src.PAYTYPE
       ,DST.IMPAWNSUM                                          --质押总金额:src.IMPAWNSUM
       ,DST.DEDUCTFLAG1                                        --自动扣款帐号1:src.DEDUCTFLAG1
       ,DST.CORESTATUS                                         --发生核心状态:src.CORESTATUS
       ,DST.FIRSTDEADLINE                                      --首次合同签订截止日期:src.FIRSTDEADLINE
       ,DST.LASTDEADLINE                                       --最迟合同签订截止日期:src.LASTDEADLINE
       ,DST.CREDITCONDITION                                    --授信使用条件:src.CREDITCONDITION
       ,DST.PAYDEADLINE                                        --合同签订到期截止日期:src.PAYDEADLINE
       ,DST.FIRSTPAY                                           --首次放款截止日期:src.FIRSTPAY
       ,DST.LASTPAY                                            --放贷到期截止日期:src.LASTPAY
       ,DST.DEADLINEMETHOD                                     --结息方式:src.DEADLINEMETHOD
       ,DST.CREDITMAX                                          --单户最大担保金额:src.CREDITMAX
       ,DST.BENEFICIARY                                        --受益人:src.BENEFICIARY
       ,DST.TRUSTFUND                                          --委托基金:src.TRUSTFUND
       ,DST.TRUSTNAME                                          --委托人名称:src.TRUSTNAME
       ,DST.TRUSTACCOUT                                        --委托人帐号:src.TRUSTACCOUT
       ,DST.TRUSTNO                                            --委托协议编号:src.TRUSTNO
       ,DST.TRUSTTYPE                                          --委托贷款类型:src.TRUSTTYPE
       ,DST.DUNFLAG                                            --不良记录标志:src.DUNFLAG
       ,DST.MAXDEADLINE                                        --放贷最长期限:src.MAXDEADLINE
       ,DST.BILLNO                                             --票据号码:src.BILLNO
       ,DST.BILLCURRENCY                                       --票据币种:src.BILLCURRENCY
       ,DST.BILLSUM                                            --票据金额:src.BILLSUM
       ,DST.TALKDATE                                           --议付日期:src.TALKDATE
       ,DST.BILLBANK                                           --开证行:src.BILLBANK
       ,DST.ACCEPTANCE                                         --承兑情况:src.ACCEPTANCE
       ,DST.ADVANCEDAYS                                        --押汇天数:src.ADVANCEDAYS
       ,DST.HOUSESUM                                           --购房合同金额（老系统）:src.HOUSESUM
       ,DST.TENCLASSIFYRESULT                                  --十级分类结果:src.TENCLASSIFYRESULT
       ,DST.LASTTENCLASSIFYRESULT                              --最近十级分类结果:src.LASTTENCLASSIFYRESULT
       ,DST.LASTCLASSIFYRESULT                                 --最新风险分类结果:src.LASTCLASSIFYRESULT
       ,DST.PLUSCREDIT                                         --递增减额:src.PLUSCREDIT
       ,DST.PLUSRATIO                                          --等比递增减比例:src.PLUSRATIO
       ,DST.PLUSNUM                                            --递增减初始期数:src.PLUSNUM
       ,DST.PLUSNO                                             --递增减间隔期数:src.PLUSNO
       ,DST.VOUCHNAME                                          --再担保的公司名称:src.VOUCHNAME
       ,DST.GROUPCREDIT                                        --宽限期（老系统）:src.GROUPCREDIT
       ,DST.VOUCHMIN                                           --担保保证金最低比例:src.VOUCHMIN
       ,DST.OWNREF                                             --信用证编号:src.OWNREF
       ,DST.CONCERT                                            --工程施工许可证情况:src.CONCERT
       ,DST.PLANCERT                                           --建设工程规划许可证情况:src.PLANCERT
       ,DST.HOUSECERTIFICATE                                   --土地使用权证情况:src.HOUSECERTIFICATE
       ,DST.CONSTRUCTIONCERTIFICATE                            --建设用地许可证情况:src.CONSTRUCTIONCERTIFICATE
       ,DST.HOUSELICENSE                                       --房屋销售（预售）许可证情况:src.HOUSELICENSE
       ,DST.MAXSELLING                                         --预计销售总额:src.MAXSELLING
       ,DST.DUTYDEADLINE                                       --连带责任保证截止时间:src.DUTYDEADLINE
       ,DST.COLTEXT                                            --合作内容:src.COLTEXT
       ,DST.CONFIRMVALUE                                       --认定价值:src.CONFIRMVALUE
       ,DST.FROZESUM                                           --冻结金额:src.FROZESUM
       ,DST.CONCONTROL                                         --合同控制方式:src.CONCONTROL
       ,DST.PHASEOPINION                                       --审批意见:src.PHASEOPINION
       ,DST.CYCUSE                                             --逾期利率浮比例:src.CYCUSE
       ,DST.RECEIVABLE                                         --应收账款认定价值:src.RECEIVABLE
       ,DST.HIGHPERCENT                                        --认定价值最高贷款比例:src.HIGHPERCENT
       ,DST.ACCOUNTDATE                                        --应收账款最后付款日期:src.ACCOUNTDATE
       ,DST.DEVELOPERQUALIFICATION                             --开发商资质等级:src.DEVELOPERQUALIFICATION
       ,DST.EXCHANGERATE                                       --申请币种对人民币汇率:src.EXCHANGERATE
       ,DST.LOANMAX                                            --单笔最大贷款成数:src.LOANMAX
       ,DST.LCTYPE                                             --备用信用证种类:src.LCTYPE
       ,DST.EXCHANGETYPE                                       --结售汇类型:src.EXCHANGETYPE
       ,DST.LIMDATE                                            --租赁到期日:src.LIMDATE
       ,DST.LIMEFFECT                                          --租赁起始日:src.LIMEFFECT
       ,DST.LIMDEADLINE                                        --租赁期限（月）:src.LIMDEADLINE
       ,DST.TRANSFORMFLAG                                      --合同是否处于担保合同变更流程中:src.TRANSFORMFLAG
       ,DST.SHIFTTYPE                                          --移交类型:src.SHIFTTYPE
       ,DST.PARTICIPATENATURE                                  --参与性质:src.PARTICIPATENATURE
       ,DST.CLASSIFYORGID                                      --风险分类机构:src.CLASSIFYORGID
       ,DST.OWEINTERESTDAYS                                    --欠息天数:src.OWEINTERESTDAYS
       ,DST.GUARANTEEPROJECT                                   --租赁物:src.GUARANTEEPROJECT
       ,DST.INTERESTDAY                                        --计息天数:src.INTERESTDAY
       ,DST.ACTUALLYPAY                                        --实付金额:src.ACTUALLYPAY
       ,DST.DEALBALANCEVALUE                                   --合同释放金额:src.DEALBALANCEVALUE
       ,DST.RESONTYPE                                          --原因类型:src.RESONTYPE
       ,DST.SHIFTBALANCE                                       --移交余额:src.SHIFTBALANCE
       ,DST.CANCELSUM                                          --核销本金:src.CANCELSUM
       ,DST.CANCELINTEREST                                     --核销利息:src.CANCELINTEREST
       ,DST.CONFIRMRESON                                       --认定原因:src.CONFIRMRESON
       ,DST.BANKSNO                                            --参与行数量:src.BANKSNO
       ,DST.DISCOUNTINTEREST                                   --贴现利息:src.DISCOUNTINTEREST
       ,DST.BACURRENCY                                         --保证金账户币种:src.BACURRENCY
       ,DST.THIRDUSERID                                        --受益人编号:src.THIRDUSERID
       ,DST.DISCOUNTRATIO                                      --贴息比例:src.DISCOUNTRATIO
       ,DST.DEDATE                                             --贴息到期日期:src.DEDATE
       ,DST.TEMPLATETYPE                                       --电子合同模板类型:src.TEMPLATETYPE
       ,DST.RELATIVEGROUPSERIALNO                              --关联联保授信流水号:src.RELATIVEGROUPSERIALNO
       ,DST.LIMITATIONTERM                                     --额度使用最迟日期:src.LIMITATIONTERM
       ,DST.MIGRATEFLAG                                        --迁移标志:src.MIGRATEFLAG
       ,DST.PUTOUTORGID                                        --入账机构:src.PUTOUTORGID
       ,DST.ISINSURANCE                                        --是否中信保:src.ISINSURANCE
       ,DST.ACTUALBAILSUM                                      --实际保证金金额:src.ACTUALBAILSUM
       ,DST.BAILEXCHANGERATE                                   --保证金币种对人民币汇率:src.BAILEXCHANGERATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
       ,DST.SUBBILLTYPE                                        --票据种类:src.SUBBILLTYPE
   FROM F_LN_XDXT_BUSINESS_CONTRACT DST 
   LEFT JOIN F_LN_XDXT_BUSINESS_CONTRACT_INNTMP1 SRC 
     ON SRC.SERIALNO            = DST.SERIALNO 
  WHERE SRC.SERIALNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_BUSINESS_CONTRACT_INNTMP2 = sqlContext.sql(sql)
dfn="F_LN_XDXT_BUSINESS_CONTRACT/"+V_DT+".parquet"
F_LN_XDXT_BUSINESS_CONTRACT_INNTMP2=F_LN_XDXT_BUSINESS_CONTRACT_INNTMP2.unionAll(F_LN_XDXT_BUSINESS_CONTRACT_INNTMP1)
F_LN_XDXT_BUSINESS_CONTRACT_INNTMP1.cache()
F_LN_XDXT_BUSINESS_CONTRACT_INNTMP2.cache()
nrowsi = F_LN_XDXT_BUSINESS_CONTRACT_INNTMP1.count()
nrowsa = F_LN_XDXT_BUSINESS_CONTRACT_INNTMP2.count()
F_LN_XDXT_BUSINESS_CONTRACT_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_LN_XDXT_BUSINESS_CONTRACT_INNTMP1.unpersist()
F_LN_XDXT_BUSINESS_CONTRACT_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_XDXT_BUSINESS_CONTRACT lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_LN_XDXT_BUSINESS_CONTRACT/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_BUSINESS_CONTRACT_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_BUSINESS_CONTRACT_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_BUSINESS_CONTRACT/"+V_DT+".parquet /"+dbname+"/F_LN_XDXT_BUSINESS_CONTRACT_BK/"+V_DT+".parquet")
