#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_AFA_MID_MAINTRANSDTL').setMaster(sys.argv[2])
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

O_TX_MID_MAINTRANSDTL = sqlContext.read.parquet(hdfs+'/O_TX_MID_MAINTRANSDTL/*')
O_TX_MID_MAINTRANSDTL.registerTempTable("O_TX_MID_MAINTRANSDTL")

#任务[12] 001-01::
V_STEP = V_STEP + 1



sql = """
 SELECT WORKDATE                AS WORKDATE 
       ,AGENTSERIALNO           AS AGENTSERIALNO 
       ,SYSID                   AS SYSID 
       ,SUBSYSID                AS SUBSYSID 
       ,AGENTFLAG               AS AGENTFLAG 
       ,TEMPLATECODE            AS TEMPLATECODE 
       ,TRANSCODE               AS TRANSCODE 
       ,CHANNELSEQ              AS CHANNELSEQ 
       ,CHANNELDATE             AS CHANNELDATE 
       ,CHANNELCODE             AS CHANNELCODE 
       ,BRNO                    AS BRNO 
       ,TELLERNO                AS TELLERNO 
       ,AUTHTELLERNO            AS AUTHTELLERNO 
       ,CHKTELLERNO             AS CHKTELLERNO 
       ,SENDTELLERNO            AS SENDTELLERNO 
       ,TERMINALNO              AS TERMINALNO 
       ,MBFLAG                  AS MBFLAG 
       ,DCFLAG                  AS DCFLAG 
       ,TRANSFLAG               AS TRANSFLAG 
       ,CASHPRONO               AS CASHPRONO 
       ,ACCCLASS                AS ACCCLASS 
       ,CLEARACCSEQ             AS CLEARACCSEQ 
       ,CURRENCY                AS CURRENCY 
       ,ACCTNO                  AS ACCTNO 
       ,ACCTNAME                AS ACCTNAME 
       ,PAYERACC1               AS PAYERACC1 
       ,PAYERNAME1              AS PAYERNAME1 
       ,PAYEEACC1               AS PAYEEACC1 
       ,PAYEENAME               AS PAYEENAME 
       ,AMOUNT1                 AS AMOUNT1 
       ,PAYERACC2               AS PAYERACC2 
       ,PAYERNAME2              AS PAYERNAME2 
       ,PAYEEACC2               AS PAYEEACC2 
       ,PAYEENAME2              AS PAYEENAME2 
       ,AMOUNT2                 AS AMOUNT2 
       ,AMOUNT                  AS AMOUNT 
       ,REALAMOUNT              AS REALAMOUNT 
       ,FEEFLAG                 AS FEEFLAG 
       ,TRANSFEECODE            AS TRANSFEECODE 
       ,FEECODE                 AS FEECODE 
       ,COSTDEECODE             AS COSTDEECODE 
       ,FEEAMOUNT               AS FEEAMOUNT 
       ,TRANSAMOUNT             AS TRANSAMOUNT 
       ,COSTAMOUNT              AS COSTAMOUNT 
       ,ENTRUSTDATE             AS ENTRUSTDATE 
       ,BUSSEQNO                AS BUSSEQNO 
       ,SENDBANK                AS SENDBANK 
       ,SENDSETTELBANK          AS SENDSETTELBANK 
       ,RECVBANK                AS RECVBANK 
       ,RECVSETTELBANK          AS RECVSETTELBANK 
       ,PAYEEOPENBANK           AS PAYEEOPENBANK 
       ,VOUCHTYPE               AS VOUCHTYPE 
       ,VOUCHNO                 AS VOUCHNO 
       ,DRVOUCHTYPE1            AS DRVOUCHTYPE1 
       ,DRVOUCHNO1              AS DRVOUCHNO1 
       ,CRVOUCHTYPE1            AS CRVOUCHTYPE1 
       ,CRVOUCHNO1              AS CRVOUCHNO1 
       ,DRVOUCHTYPE2            AS DRVOUCHTYPE2 
       ,DRVOUCHNO2              AS DRVOUCHNO2 
       ,CRVOUCHTYPE2            AS CRVOUCHTYPE2 
       ,CRVOUCHNO2              AS CRVOUCHNO2 
       ,IDTYPE                  AS IDTYPE 
       ,IDNO                    AS IDNO 
       ,TRADEBUSISTEP           AS TRADEBUSISTEP 
       ,TRADESTATUS             AS TRADESTATUS 
       ,TRADESTEP               AS TRADESTEP 
       ,STATUS                  AS STATUS 
       ,PRIORITY                AS PRIORITY 
       ,POSTSCRIPT              AS POSTSCRIPT 
       ,BOOKNAME                AS BOOKNAME 
       ,BUSITYPE                AS BUSITYPE 
       ,SUBBUSITYPE             AS SUBBUSITYPE 
       ,THIRDID                 AS THIRDID 
       ,BUSSUBTYPE              AS BUSSUBTYPE 
       ,PREWOEKDATE             AS PREWOEKDATE 
       ,PREAGENTSERIALNO        AS PREAGENTSERIALNO 
       ,PRINTCNT                AS PRINTCNT 
       ,PKGTYPE                 AS PKGTYPE 
       ,PKGAGTDATE              AS PKGAGTDATE 
       ,PKGNO                   AS PKGNO 
       ,PKGCOSEQ                AS PKGCOSEQ 
       ,CLRDATE                 AS CLRDATE 
       ,BATSEQNO                AS BATSEQNO 
       ,AGENTPROTOCOLNO         AS AGENTPROTOCOLNO 
       ,OTXSTAT                 AS OTXSTAT 
       ,RESPONSELIMIT           AS RESPONSELIMIT 
       ,RESPONSEDATE            AS RESPONSEDATE 
       ,ACCTBRNO                AS ACCTBRNO 
       ,CNAPSTYPE               AS CNAPSTYPE 
       ,MESGTYPE                AS MESGTYPE 
       ,AREACODE                AS AREACODE 
       ,BANKSYSDATE             AS BANKSYSDATE 
       ,BANKSYSSEQ              AS BANKSYSSEQ 
       ,ABSCODE                 AS ABSCODE 
       ,ABSINFO                 AS ABSINFO 
       ,SERVICESEQ              AS SERVICESEQ 
       ,LOGFILENAME             AS LOGFILENAME 
       ,THIRDSYSERRCODE         AS THIRDSYSERRCODE 
       ,THIRDSYSERRMSG          AS THIRDSYSERRMSG 
       ,THIRDCHKFLAG            AS THIRDCHKFLAG 
       ,HOSTRET                 AS HOSTRET 
       ,HOSTRETMSG              AS HOSTRETMSG 
       ,THIRDERRORCODE          AS THIRDERRORCODE 
       ,THIRDERRORDESC          AS THIRDERRORDESC 
       ,ERRORCODE               AS ERRORCODE 
       ,ERRORDESC               AS ERRORDESC 
       ,ZONENO                  AS ZONENO 
       ,WORKSTAMP1              AS WORKSTAMP1 
       ,WORKSTAMP2              AS WORKSTAMP2 
       ,NOTE1                   AS NOTE1 
       ,NOTE2                   AS NOTE2 
       ,NOTE3                   AS NOTE3 
       ,NOTE4                   AS NOTE4 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'AFA'                   AS ODS_SYS_ID 
   FROM O_TX_MID_MAINTRANSDTL A                                --统一中间业务流水表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_MID_MAINTRANSDTL_INNTMP1 = sqlContext.sql(sql)
F_TX_MID_MAINTRANSDTL_INNTMP1.registerTempTable("F_TX_MID_MAINTRANSDTL_INNTMP1")

F_TX_MID_MAINTRANSDTL = sqlContext.read.parquet(hdfs+'/F_TX_MID_MAINTRANSDTL/*')
F_TX_MID_MAINTRANSDTL.registerTempTable("F_TX_MID_MAINTRANSDTL")
sql = """
 SELECT DST.WORKDATE                                            --平台受理日期:src.WORKDATE
       ,DST.AGENTSERIALNO                                      --平台业务流水号:src.AGENTSERIALNO
       ,DST.SYSID                                              --系统标识:src.SYSID
       ,DST.SUBSYSID                                           --系统子标识:src.SUBSYSID
       ,DST.AGENTFLAG                                          --业务标识:src.AGENTFLAG
       ,DST.TEMPLATECODE                                       --模板代码:src.TEMPLATECODE
       ,DST.TRANSCODE                                          --交易码:src.TRANSCODE
       ,DST.CHANNELSEQ                                         --外围流水号:src.CHANNELSEQ
       ,DST.CHANNELDATE                                        --外围日期:src.CHANNELDATE
       ,DST.CHANNELCODE                                        --发起渠道代码:src.CHANNELCODE
       ,DST.BRNO                                               --机构码:src.BRNO
       ,DST.TELLERNO                                           --操作员:src.TELLERNO
       ,DST.AUTHTELLERNO                                       --授权柜员:src.AUTHTELLERNO
       ,DST.CHKTELLERNO                                        --发起/复核柜员号:src.CHKTELLERNO
       ,DST.SENDTELLERNO                                       --发送柜员:src.SENDTELLERNO
       ,DST.TERMINALNO                                         --操作终端号:src.TERMINALNO
       ,DST.MBFLAG                                             --来往帐标识:src.MBFLAG
       ,DST.DCFLAG                                             --借贷方标识:src.DCFLAG
       ,DST.TRANSFLAG                                          --现转标识:src.TRANSFLAG
       ,DST.CASHPRONO                                          --现金项目号:src.CASHPRONO
       ,DST.ACCCLASS                                           --主账号的折标志:src.ACCCLASS
       ,DST.CLEARACCSEQ                                        --应解汇款序号:src.CLEARACCSEQ
       ,DST.CURRENCY                                           --币种号:src.CURRENCY
       ,DST.ACCTNO                                             --交易主账号:src.ACCTNO
       ,DST.ACCTNAME                                           --交易主账号户名:src.ACCTNAME
       ,DST.PAYERACC1                                          --借方账号1:src.PAYERACC1
       ,DST.PAYERNAME1                                         --借方户名1:src.PAYERNAME1
       ,DST.PAYEEACC1                                          --贷方账号1:src.PAYEEACC1
       ,DST.PAYEENAME                                          --贷方户名1:src.PAYEENAME
       ,DST.AMOUNT1                                            --发生额1:src.AMOUNT1
       ,DST.PAYERACC2                                          --借方账号2:src.PAYERACC2
       ,DST.PAYERNAME2                                         --借方户名2:src.PAYERNAME2
       ,DST.PAYEEACC2                                          --贷方账号2:src.PAYEEACC2
       ,DST.PAYEENAME2                                         --贷方户名2:src.PAYEENAME2
       ,DST.AMOUNT2                                            --发生额2:src.AMOUNT2
       ,DST.AMOUNT                                             --交易金额:src.AMOUNT
       ,DST.REALAMOUNT                                         --实际交易金额:src.REALAMOUNT
       ,DST.FEEFLAG                                            --手续费收取标志:src.FEEFLAG
       ,DST.TRANSFEECODE                                       --TRANSFEECODE:src.TRANSFEECODE
       ,DST.FEECODE                                            --FEECODE:src.FEECODE
       ,DST.COSTDEECODE                                        --COSTDEECODE:src.COSTDEECODE
       ,DST.FEEAMOUNT                                          --实收手续费金额:src.FEEAMOUNT
       ,DST.TRANSAMOUNT                                        --实收汇划费金额:src.TRANSAMOUNT
       ,DST.COSTAMOUNT                                         --工本费:src.COSTAMOUNT
       ,DST.ENTRUSTDATE                                        --委托日期:src.ENTRUSTDATE
       ,DST.BUSSEQNO                                           --支付交易序号:src.BUSSEQNO
       ,DST.SENDBANK                                           --发起行行号:src.SENDBANK
       ,DST.SENDSETTELBANK                                     --发起清算行行号:src.SENDSETTELBANK
       ,DST.RECVBANK                                           --接受行行号:src.RECVBANK
       ,DST.RECVSETTELBANK                                     --接受清算行行号:src.RECVSETTELBANK
       ,DST.PAYEEOPENBANK                                      --收款人开户行行号:src.PAYEEOPENBANK
       ,DST.VOUCHTYPE                                          --凭证种类:src.VOUCHTYPE
       ,DST.VOUCHNO                                            --凭证号码:src.VOUCHNO
       ,DST.DRVOUCHTYPE1                                       --借方凭证种类1:src.DRVOUCHTYPE1
       ,DST.DRVOUCHNO1                                         --借方凭证号1:src.DRVOUCHNO1
       ,DST.CRVOUCHTYPE1                                       --贷方凭证种类1:src.CRVOUCHTYPE1
       ,DST.CRVOUCHNO1                                         --贷方凭证号1:src.CRVOUCHNO1
       ,DST.DRVOUCHTYPE2                                       --借方凭证种类2:src.DRVOUCHTYPE2
       ,DST.DRVOUCHNO2                                         --借方凭证号2:src.DRVOUCHNO2
       ,DST.CRVOUCHTYPE2                                       --贷方凭证种类2:src.CRVOUCHTYPE2
       ,DST.CRVOUCHNO2                                         --贷方凭证号2:src.CRVOUCHNO2
       ,DST.IDTYPE                                             --证件类型:src.IDTYPE
       ,DST.IDNO                                               --证件号码:src.IDNO
       ,DST.TRADEBUSISTEP                                      --操作步骤:src.TRADEBUSISTEP
       ,DST.TRADESTATUS                                        --交易状态:src.TRADESTATUS
       ,DST.TRADESTEP                                          --TRADESTEP:src.TRADESTEP
       ,DST.STATUS                                             --业务状态:src.STATUS
       ,DST.PRIORITY                                           --业务优先级:src.PRIORITY
       ,DST.POSTSCRIPT                                         --业务附言:src.POSTSCRIPT
       ,DST.BOOKNAME                                           --登记簿表名:src.BOOKNAME
       ,DST.BUSITYPE                                           --业务大类编码:src.BUSITYPE
       ,DST.SUBBUSITYPE                                        --业务子类编码:src.SUBBUSITYPE
       ,DST.THIRDID                                            --第三方编码:src.THIRDID
       ,DST.BUSSUBTYPE                                         --业务种类:src.BUSSUBTYPE
       ,DST.PREWOEKDATE                                        --原业务平台受理日期:src.PREWOEKDATE
       ,DST.PREAGENTSERIALNO                                   --原业务平台业务流水号:src.PREAGENTSERIALNO
       ,DST.PRINTCNT                                           --打印次数:src.PRINTCNT
       ,DST.PKGTYPE                                            --包类型号:src.PKGTYPE
       ,DST.PKGAGTDATE                                         --包委托日期:src.PKGAGTDATE
       ,DST.PKGNO                                              --包序号:src.PKGNO
       ,DST.PKGCOSEQ                                           --PKGCOSEQ:src.PKGCOSEQ
       ,DST.CLRDATE                                            --清算日期:src.CLRDATE
       ,DST.BATSEQNO                                           --批次号:src.BATSEQNO
       ,DST.AGENTPROTOCOLNO                                    --多方协议号:src.AGENTPROTOCOLNO
       ,DST.OTXSTAT                                            --回执状态:src.OTXSTAT
       ,DST.RESPONSELIMIT                                      --回执期限（借机业务）:src.RESPONSELIMIT
       ,DST.RESPONSEDATE                                       --回执日期（借机业务）:src.RESPONSEDATE
       ,DST.ACCTBRNO                                           --开户机构:src.ACCTBRNO
       ,DST.CNAPSTYPE                                          --CNAPSTYPE:src.CNAPSTYPE
       ,DST.MESGTYPE                                           --MESGTYPE:src.MESGTYPE
       ,DST.AREACODE                                           --地区编码:src.AREACODE
       ,DST.BANKSYSDATE                                        --主机账务日期:src.BANKSYSDATE
       ,DST.BANKSYSSEQ                                         --主机流水号:src.BANKSYSSEQ
       ,DST.ABSCODE                                            --摘要代码:src.ABSCODE
       ,DST.ABSINFO                                            --摘要内容:src.ABSINFO
       ,DST.SERVICESEQ                                         --服务序号:src.SERVICESEQ
       ,DST.LOGFILENAME                                        --日志文件名:src.LOGFILENAME
       ,DST.THIRDSYSERRCODE                                    --第三方错误码:src.THIRDSYSERRCODE
       ,DST.THIRDSYSERRMSG                                     --第三方错误信息:src.THIRDSYSERRMSG
       ,DST.THIRDCHKFLAG                                       --第三方对账状态:src.THIRDCHKFLAG
       ,DST.HOSTRET                                            --主机响应码:src.HOSTRET
       ,DST.HOSTRETMSG                                         --主机响应信息:src.HOSTRETMSG
       ,DST.THIRDERRORCODE                                     --第三方响应码:src.THIRDERRORCODE
       ,DST.THIRDERRORDESC                                     --第三方响应信息:src.THIRDERRORDESC
       ,DST.ERRORCODE                                          --返回请求响应码:src.ERRORCODE
       ,DST.ERRORDESC                                          --返回请求响应信息:src.ERRORDESC
       ,DST.ZONENO                                             --法人号:src.ZONENO
       ,DST.WORKSTAMP1                                         --开始时间:src.WORKSTAMP1
       ,DST.WORKSTAMP2                                         --结束时间:src.WORKSTAMP2
       ,DST.NOTE1                                              --备注1:src.NOTE1
       ,DST.NOTE2                                              --备注2:src.NOTE2
       ,DST.NOTE3                                              --备注3:src.NOTE3
       ,DST.NOTE4                                              --备注4:src.NOTE4
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_TX_MID_MAINTRANSDTL DST 
   LEFT JOIN F_TX_MID_MAINTRANSDTL_INNTMP1 SRC 
     ON SRC.WORKDATE            = DST.WORKDATE 
    AND SRC.AGENTSERIALNO       = DST.AGENTSERIALNO 
  WHERE SRC.WORKDATE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_MID_MAINTRANSDTL_INNTMP2 = sqlContext.sql(sql)
dfn="F_TX_MID_MAINTRANSDTL/"+V_DT+".parquet"
F_TX_MID_MAINTRANSDTL_INNTMP2=F_TX_MID_MAINTRANSDTL_INNTMP2.unionAll(F_TX_MID_MAINTRANSDTL_INNTMP1)
F_TX_MID_MAINTRANSDTL_INNTMP1.cache()
F_TX_MID_MAINTRANSDTL_INNTMP2.cache()
nrowsi = F_TX_MID_MAINTRANSDTL_INNTMP1.count()
nrowsa = F_TX_MID_MAINTRANSDTL_INNTMP2.count()
F_TX_MID_MAINTRANSDTL_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_TX_MID_MAINTRANSDTL_INNTMP1.unpersist()
F_TX_MID_MAINTRANSDTL_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_MID_MAINTRANSDTL lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_TX_MID_MAINTRANSDTL/"+V_DT_LD+".parquet /"+dbname+"/F_TX_MID_MAINTRANSDTL_BK/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_MID_MAINTRANSDTL/"+V_DT_LD+".parquet")