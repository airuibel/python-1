#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_GUARANTY_INFO').setMaster(sys.argv[2])
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

O_LN_XDXT_GUARANTY_INFO = sqlContext.read.parquet(hdfs+'/O_LN_XDXT_GUARANTY_INFO/*')
O_LN_XDXT_GUARANTY_INFO.registerTempTable("O_LN_XDXT_GUARANTY_INFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_GUARANTY_INFO/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_GUARANTY_INFO_BK/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_GUARANTY_INFO/"+V_DT+".parquet")


F_LN_XDXT_GUARANTY_INFO = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_GUARANTY_INFO/*')
F_LN_XDXT_GUARANTY_INFO.registerTempTable("F_LN_XDXT_GUARANTY_INFO")

sql = """
 SELECT A.GUARANTYID            AS GUARANTYID 
       ,A.GUARANTYTYPE          AS GUARANTYTYPE 
       ,A.GUARANTYSTATUS        AS GUARANTYSTATUS 
       ,A.OWNERID               AS OWNERID 
       ,A.OWNERNAME             AS OWNERNAME 
       ,A.OWNERTYPE             AS OWNERTYPE 
       ,A.RATE                  AS RATE 
       ,A.CUSTGUARANTYTYPE      AS CUSTGUARANTYTYPE 
       ,A.SUBJECTNO             AS SUBJECTNO 
       ,A.RELATIVEACCOUNT       AS RELATIVEACCOUNT 
       ,A.GUARANTYRIGHTID       AS GUARANTYRIGHTID 
       ,A.OTHERGUARANTYRIGHT    AS OTHERGUARANTYRIGHT 
       ,A.GUARANTYNAME          AS GUARANTYNAME 
       ,A.GUARANTYSUBTYPE       AS GUARANTYSUBTYPE 
       ,A.GUARANTYOWNWAY        AS GUARANTYOWNWAY 
       ,A.GUARANTYUSING         AS GUARANTYUSING 
       ,A.GUARANTYLOCATION      AS GUARANTYLOCATION 
       ,A.GUARANTYAMOUNT        AS GUARANTYAMOUNT 
       ,A.GUARANTYAMOUNT1       AS GUARANTYAMOUNT1 
       ,A.GUARANTYAMOUNT2       AS GUARANTYAMOUNT2 
       ,A.GUARANTYRESOUCE       AS GUARANTYRESOUCE 
       ,A.GUARANTYDATE          AS GUARANTYDATE 
       ,A.BEGINDATE             AS BEGINDATE 
       ,A.OWNERTIME             AS OWNERTIME 
       ,A.GUARANTYDESCRIPT      AS GUARANTYDESCRIPT 
       ,A.ABOUTOTHERID1         AS ABOUTOTHERID1 
       ,A.ABOUTOTHERID2         AS ABOUTOTHERID2 
       ,A.ABOUTOTHERID3         AS ABOUTOTHERID3 
       ,A.ABOUTOTHERID4         AS ABOUTOTHERID4 
       ,A.PURPOSE               AS PURPOSE 
       ,A.ABOUTSUM1             AS ABOUTSUM1 
       ,A.ABOUTSUM2             AS ABOUTSUM2 
       ,A.ABOUTRATE             AS ABOUTRATE 
       ,A.GUARANTYANA           AS GUARANTYANA 
       ,A.GUARANTYPRICE         AS GUARANTYPRICE 
       ,A.EVALMETHOD            AS EVALMETHOD 
       ,A.EVALORGID             AS EVALORGID 
       ,A.EVALORGNAME           AS EVALORGNAME 
       ,A.EVALDATE              AS EVALDATE 
       ,A.EVALNETVALUE          AS EVALNETVALUE 
       ,A.CONFIRMVALUE          AS CONFIRMVALUE 
       ,A.GUARANTYRATE          AS GUARANTYRATE 
       ,A.THIRDPARTY1           AS THIRDPARTY1 
       ,A.THIRDPARTY2           AS THIRDPARTY2 
       ,A.THIRDPARTY3           AS THIRDPARTY3 
       ,A.GUARANTYDESCRIBE1     AS GUARANTYDESCRIBE1 
       ,A.GUARANTYDESCRIBE2     AS GUARANTYDESCRIBE2 
       ,A.GUARANTYDESCRIBE3     AS GUARANTYDESCRIBE3 
       ,A.FLAG1                 AS FLAG1 
       ,A.FLAG2                 AS FLAG2 
       ,A.FLAG3                 AS FLAG3 
       ,A.FLAG4                 AS FLAG4 
       ,A.GUARANTYREGNO         AS GUARANTYREGNO 
       ,A.GUARANTYREGORG        AS GUARANTYREGORG 
       ,A.GUARANTYREGDATE       AS GUARANTYREGDATE 
       ,A.GUARANTYWODATE        AS GUARANTYWODATE 
       ,A.INSURECERTNO          AS INSURECERTNO 
       ,A.OTHERASSUMPSIT        AS OTHERASSUMPSIT 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.UPDATEUSERID          AS UPDATEUSERID 
       ,A.REMARK                AS REMARK 
       ,A.SAPVOUCHTYPE          AS SAPVOUCHTYPE 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CERTID                AS CERTID 
       ,A.LOANCARDNO            AS LOANCARDNO 
       ,A.GUARANTYCURRENCY      AS GUARANTYCURRENCY 
       ,A.EVALCURRENCY          AS EVALCURRENCY 
       ,A.GUARANTYDESCRIBE4     AS GUARANTYDESCRIBE4 
       ,A.DYNAMICVALUE          AS DYNAMICVALUE 
       ,A.YAPINNAME             AS YAPINNAME 
       ,A.YAPINCOUNT            AS YAPINCOUNT 
       ,A.COMPLETEYEAR          AS COMPLETEYEAR 
       ,A.STORETYPE             AS STORETYPE 
       ,A.RENTORNOT             AS RENTORNOT 
       ,A.RENTDATE              AS RENTDATE 
       ,A.RECORDTERM            AS RECORDTERM 
       ,A.RECORDTYPE            AS RECORDTYPE 
       ,A.ASSURENO              AS ASSURENO 
       ,A.ASSURETERM            AS ASSURETERM 
       ,A.ASSURESUM             AS ASSURESUM 
       ,A.ASSUREDEFINE          AS ASSUREDEFINE 
       ,A.GUARANTYTOCORE        AS GUARANTYTOCORE 
       ,A.IMPAWNFLAG            AS IMPAWNFLAG 
       ,A.ACCRUALBEGINDATE      AS ACCRUALBEGINDATE 
       ,A.ACCRUALENDDATE        AS ACCRUALENDDATE 
       ,A.DEPREBEGINDATE        AS DEPREBEGINDATE 
       ,A.DEPREENDDATE          AS DEPREENDDATE 
       ,A.IMPAWNDEPACCOUNTM     AS IMPAWNDEPACCOUNTM 
       ,A.IMPAWNDEPACCOUNTO     AS IMPAWNDEPACCOUNTO 
       ,A.DEBTORRIGHT           AS DEBTORRIGHT 
       ,A.DEPOSITACCOUNT        AS DEPOSITACCOUNT 
       ,A.IMPAWNBILLNO          AS IMPAWNBILLNO 
       ,A.STOPPAYNO             AS STOPPAYNO 
       ,A.RULEVALUE             AS RULEVALUE 
       ,A.CASHVALUE             AS CASHVALUE 
       ,A.SAFETYVALUE           AS SAFETYVALUE 
       ,A.GARNORVALUE           AS GARNORVALUE 
       ,A.CERTINTEGRITY         AS CERTINTEGRITY 
       ,A.CARRYINGCAPACITY      AS CARRYINGCAPACITY 
       ,A.MIGRATEFLAG           AS MIGRATEFLAG 
       ,A.MFGUARANTYID          AS MFGUARANTYID 
       ,A.GUARANTYCONTRACTNO    AS GUARANTYCONTRACTNO 
       ,A.INSURANCECATEGORY     AS INSURANCECATEGORY 
       ,A.INSURANCEENTNAME      AS INSURANCEENTNAME 
       ,A.INSURANCEENDDATE      AS INSURANCEENDDATE 
       ,A.GUARANTYENDDATE       AS GUARANTYENDDATE 
       ,A.ISASSURE              AS ISASSURE 
       ,A.SIGNORG               AS SIGNORG 
       ,A.SIGNNO                AS SIGNNO 
       ,A.ABATEFLAG             AS ABATEFLAG 
       ,A.HOSEVALMETHOD         AS HOSEVALMETHOD 
       ,A.HOSEVALORGNAME        AS HOSEVALORGNAME 
       ,A.HOSEVALORGID          AS HOSEVALORGID 
       ,A.HOSEVALDATE           AS HOSEVALDATE 
       ,A.HOSEVALCURRENCY       AS HOSEVALCURRENCY 
       ,A.HOSEVALNETVALUE       AS HOSEVALNETVALUE 
       ,A.HOSCONFIRMCURRENCY    AS HOSCONFIRMCURRENCY 
       ,A.HOSCONFIRMVALUE       AS HOSCONFIRMVALUE 
       ,A.GURCONFIRMCURRENCY    AS GURCONFIRMCURRENCY 
       ,A.GURCONFIRMVALUE       AS GURCONFIRMVALUE 
       ,A.CONTRACTCURRENCY      AS CONTRACTCURRENCY 
       ,A.CONTRACTSUM           AS CONTRACTSUM 
       ,A.GUARANTYRATIO         AS GUARANTYRATIO 
       ,A.HOSCONTRACTCURRENCY   AS HOSCONTRACTCURRENCY 
       ,A.HOSCONTRACTSUM        AS HOSCONTRACTSUM 
       ,A.HOSGUARANTYRATIO      AS HOSGUARANTYRATIO 
       ,A.HOSGUARANTYRATIO1     AS HOSGUARANTYRATIO1 
       ,A.CURCONTRACTCURRENCY   AS CURCONTRACTCURRENCY 
       ,A.CURCONTRACTSUM        AS CURCONTRACTSUM 
       ,A.CURGUARANTYRATIO      AS CURGUARANTYRATIO 
       ,A.CURGUARANTYRATIO1     AS CURGUARANTYRATIO1 
       ,A.CUREVALCURRENCY       AS CUREVALCURRENCY 
       ,A.CUREVALNETVALUE       AS CUREVALNETVALUE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
       ,A.SUBBILLTYPE           AS SUBBILLTYPE 
   FROM O_LN_XDXT_GUARANTY_INFO A                              --押品基本信息
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_GUARANTY_INFO_INNTMP1 = sqlContext.sql(sql)
F_LN_XDXT_GUARANTY_INFO_INNTMP1.registerTempTable("F_LN_XDXT_GUARANTY_INFO_INNTMP1")

#F_LN_XDXT_GUARANTY_INFO = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_GUARANTY_INFO/*')
#F_LN_XDXT_GUARANTY_INFO.registerTempTable("F_LN_XDXT_GUARANTY_INFO")
sql = """
 SELECT DST.GUARANTYID                                          --担保物编号:src.GUARANTYID
       ,DST.GUARANTYTYPE                                       --担保物类型:src.GUARANTYTYPE
       ,DST.GUARANTYSTATUS                                     --担保物状态:src.GUARANTYSTATUS
       ,DST.OWNERID                                            --权属人编号:src.OWNERID
       ,DST.OWNERNAME                                          --权属人名称:src.OWNERNAME
       ,DST.OWNERTYPE                                          --权属人类型:src.OWNERTYPE
       ,DST.RATE                                               --权属人权益比例:src.RATE
       ,DST.CUSTGUARANTYTYPE                                   --客户担保物类型:src.CUSTGUARANTYTYPE
       ,DST.SUBJECTNO                                          --行内担保物科目号:src.SUBJECTNO
       ,DST.RELATIVEACCOUNT                                    --核心相关账号:src.RELATIVEACCOUNT
       ,DST.GUARANTYRIGHTID                                    --权证号:src.GUARANTYRIGHTID
       ,DST.OTHERGUARANTYRIGHT                                 --他项权证号:src.OTHERGUARANTYRIGHT
       ,DST.GUARANTYNAME                                       --担保物名称:src.GUARANTYNAME
       ,DST.GUARANTYSUBTYPE                                    --担保物子类型:src.GUARANTYSUBTYPE
       ,DST.GUARANTYOWNWAY                                     --担保物获取方式:src.GUARANTYOWNWAY
       ,DST.GUARANTYUSING                                      --担保物使用状况:src.GUARANTYUSING
       ,DST.GUARANTYLOCATION                                   --担保物地点位置:src.GUARANTYLOCATION
       ,DST.GUARANTYAMOUNT                                     --担保物数量:src.GUARANTYAMOUNT
       ,DST.GUARANTYAMOUNT1                                    --担保物度量1:src.GUARANTYAMOUNT1
       ,DST.GUARANTYAMOUNT2                                    --担保物度量2:src.GUARANTYAMOUNT2
       ,DST.GUARANTYRESOUCE                                    --担保物生产源:src.GUARANTYRESOUCE
       ,DST.GUARANTYDATE                                       --担保物生产日期:src.GUARANTYDATE
       ,DST.BEGINDATE                                          --开始拥有时间:src.BEGINDATE
       ,DST.OWNERTIME                                          --占有时间:src.OWNERTIME
       ,DST.GUARANTYDESCRIPT                                   --担保物描述:src.GUARANTYDESCRIPT
       ,DST.ABOUTOTHERID1                                      --担保物其他相关号码1:src.ABOUTOTHERID1
       ,DST.ABOUTOTHERID2                                      --担保物其他相关号码2:src.ABOUTOTHERID2
       ,DST.ABOUTOTHERID3                                      --担保物其他相关号码3:src.ABOUTOTHERID3
       ,DST.ABOUTOTHERID4                                      --担保物其他相关号码4:src.ABOUTOTHERID4
       ,DST.PURPOSE                                            --用途:src.PURPOSE
       ,DST.ABOUTSUM1                                          --相关金额1:src.ABOUTSUM1
       ,DST.ABOUTSUM2                                          --相关金额2:src.ABOUTSUM2
       ,DST.ABOUTRATE                                          --相关比率:src.ABOUTRATE
       ,DST.GUARANTYANA                                        --担保物分析:src.GUARANTYANA
       ,DST.GUARANTYPRICE                                      --担保物原价值:src.GUARANTYPRICE
       ,DST.EVALMETHOD                                         --担保物价值评估方式:src.EVALMETHOD
       ,DST.EVALORGID                                          --评估单位代码:src.EVALORGID
       ,DST.EVALORGNAME                                        --评估单位名称:src.EVALORGNAME
       ,DST.EVALDATE                                           --评估日期:src.EVALDATE
       ,DST.EVALNETVALUE                                       --评估值:src.EVALNETVALUE
       ,DST.CONFIRMVALUE                                       --认定价值:src.CONFIRMVALUE
       ,DST.GUARANTYRATE                                       --抵押率:src.GUARANTYRATE
       ,DST.THIRDPARTY1                                        --第三方1:src.THIRDPARTY1
       ,DST.THIRDPARTY2                                        --第三方2:src.THIRDPARTY2
       ,DST.THIRDPARTY3                                        --第三方3:src.THIRDPARTY3
       ,DST.GUARANTYDESCRIBE1                                  --担保物描述1:src.GUARANTYDESCRIBE1
       ,DST.GUARANTYDESCRIBE2                                  --担保物描述2:src.GUARANTYDESCRIBE2
       ,DST.GUARANTYDESCRIBE3                                  --担保物描述3:src.GUARANTYDESCRIBE3
       ,DST.FLAG1                                              --是否1:src.FLAG1
       ,DST.FLAG2                                              --是否2:src.FLAG2
       ,DST.FLAG3                                              --是否3:src.FLAG3
       ,DST.FLAG4                                              --是否4:src.FLAG4
       ,DST.GUARANTYREGNO                                      --抵押登记编号:src.GUARANTYREGNO
       ,DST.GUARANTYREGORG                                     --抵押登记机关:src.GUARANTYREGORG
       ,DST.GUARANTYREGDATE                                    --抵押登记日期:src.GUARANTYREGDATE
       ,DST.GUARANTYWODATE                                     --抵押登记注销日期:src.GUARANTYWODATE
       ,DST.INSURECERTNO                                       --抵押物保险单编号:src.INSURECERTNO
       ,DST.OTHERASSUMPSIT                                     --其它特别约定:src.OTHERASSUMPSIT
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTDATE                                          --登记日期:src.INPUTDATE
       ,DST.UPDATEUSERID                                       --更新人:src.UPDATEUSERID
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.SAPVOUCHTYPE                                       --sap抵质押物类型:src.SAPVOUCHTYPE
       ,DST.CERTTYPE                                           --权属人证件类型:src.CERTTYPE
       ,DST.CERTID                                             --权属人证件号码:src.CERTID
       ,DST.LOANCARDNO                                         --权属人贷款卡编号:src.LOANCARDNO
       ,DST.GUARANTYCURRENCY                                   --抵/质押币种:src.GUARANTYCURRENCY
       ,DST.EVALCURRENCY                                       --评估币种:src.EVALCURRENCY
       ,DST.GUARANTYDESCRIBE4                                  --担保物描述4:src.GUARANTYDESCRIBE4
       ,DST.DYNAMICVALUE                                       --动态价值:src.DYNAMICVALUE
       ,DST.YAPINNAME                                          --名称:src.YAPINNAME
       ,DST.YAPINCOUNT                                         --数量:src.YAPINCOUNT
       ,DST.COMPLETEYEAR                                       --年份:src.COMPLETEYEAR
       ,DST.STORETYPE                                          --层数类型:src.STORETYPE
       ,DST.RENTORNOT                                          --是否出租:src.RENTORNOT
       ,DST.RENTDATE                                           --出租合同到期日:src.RENTDATE
       ,DST.RECORDTERM                                         --登记存续期限:src.RECORDTERM
       ,DST.RECORDTYPE                                         --登记类型:src.RECORDTYPE
       ,DST.ASSURENO                                           --保险单号:src.ASSURENO
       ,DST.ASSURETERM                                         --保险期限:src.ASSURETERM
       ,DST.ASSURESUM                                          --保险金额:src.ASSURESUM
       ,DST.ASSUREDEFINE                                       --保险权益第一受益人:src.ASSUREDEFINE
       ,DST.GUARANTYTOCORE                                     --实时接口发生标志:src.GUARANTYTOCORE
       ,DST.IMPAWNFLAG                                         --质押炒汇标志:src.IMPAWNFLAG
       ,DST.ACCRUALBEGINDATE                                   --计息起始日期:src.ACCRUALBEGINDATE
       ,DST.ACCRUALENDDATE                                     --计息截止日期:src.ACCRUALENDDATE
       ,DST.DEPREBEGINDATE                                     --折旧期间<起>:src.DEPREBEGINDATE
       ,DST.DEPREENDDATE                                       --折旧期间<迄>:src.DEPREENDDATE
       ,DST.IMPAWNDEPACCOUNTM                                  --本行质押存款账户:src.IMPAWNDEPACCOUNTM
       ,DST.IMPAWNDEPACCOUNTO                                  --他行质押存款账户:src.IMPAWNDEPACCOUNTO
       ,DST.DEBTORRIGHT                                        --债务人权利:src.DEBTORRIGHT
       ,DST.DEPOSITACCOUNT                                     --定存单帐号:src.DEPOSITACCOUNT
       ,DST.IMPAWNBILLNO                                       --质借序号:src.IMPAWNBILLNO
       ,DST.STOPPAYNO                                          --老系统止付通知书编号:src.STOPPAYNO
       ,DST.RULEVALUE                                          --变现能力_合法性:src.RULEVALUE
       ,DST.CASHVALUE                                          --变现能力_流动性:src.CASHVALUE
       ,DST.SAFETYVALUE                                        --变现能力_安全性:src.SAFETYVALUE
       ,DST.GARNORVALUE                                        --抵/质押标准值:src.GARNORVALUE
       ,DST.CERTINTEGRITY                                      --四证是否齐全:src.CERTINTEGRITY
       ,DST.CARRYINGCAPACITY                                   --载重吨位:src.CARRYINGCAPACITY
       ,DST.MIGRATEFLAG                                        --移植标志:src.MIGRATEFLAG
       ,DST.MFGUARANTYID                                       --抵质押单编号:src.MFGUARANTYID
       ,DST.GUARANTYCONTRACTNO                                 --抵押合同公证书编号:src.GUARANTYCONTRACTNO
       ,DST.INSURANCECATEGORY                                  --保险种类:src.INSURANCECATEGORY
       ,DST.INSURANCEENTNAME                                   --保险公司名称:src.INSURANCEENTNAME
       ,DST.INSURANCEENDDATE                                   --保险到期日期:src.INSURANCEENDDATE
       ,DST.GUARANTYENDDATE                                    --抵押到期日期:src.GUARANTYENDDATE
       ,DST.ISASSURE                                           --是否保险:src.ISASSURE
       ,DST.SIGNORG                                            --登记机关:src.SIGNORG
       ,DST.SIGNNO                                             --登记权证号:src.SIGNNO
       ,DST.ABATEFLAG                                          --解除标志:src.ABATEFLAG
       ,DST.HOSEVALMETHOD                                      --房屋价值评估方式:src.HOSEVALMETHOD
       ,DST.HOSEVALORGNAME                                     --房屋价值评估机构名称:src.HOSEVALORGNAME
       ,DST.HOSEVALORGID                                       --房屋评估机构组织机构代码:src.HOSEVALORGID
       ,DST.HOSEVALDATE                                        --房屋价值评估时间:src.HOSEVALDATE
       ,DST.HOSEVALCURRENCY                                    --房屋评估币种:src.HOSEVALCURRENCY
       ,DST.HOSEVALNETVALUE                                    --房屋评估价值:src.HOSEVALNETVALUE
       ,DST.HOSCONFIRMCURRENCY                                 --房屋认定币种:src.HOSCONFIRMCURRENCY
       ,DST.HOSCONFIRMVALUE                                    --房屋认定价值:src.HOSCONFIRMVALUE
       ,DST.GURCONFIRMCURRENCY                                 --土地认定币种:src.GURCONFIRMCURRENCY
       ,DST.GURCONFIRMVALUE                                    --土地认定价值:src.GURCONFIRMVALUE
       ,DST.CONTRACTCURRENCY                                   --抵质押物参与担保币种:src.CONTRACTCURRENCY
       ,DST.CONTRACTSUM                                        --抵押物参与担保金额:src.CONTRACTSUM
       ,DST.GUARANTYRATIO                                      --抵押比例:src.GUARANTYRATIO
       ,DST.HOSCONTRACTCURRENCY                                --房屋担保币种:src.HOSCONTRACTCURRENCY
       ,DST.HOSCONTRACTSUM                                     --房屋参与担保金额:src.HOSCONTRACTSUM
       ,DST.HOSGUARANTYRATIO                                   --房屋抵押比例:src.HOSGUARANTYRATIO
       ,DST.HOSGUARANTYRATIO1                                  --房屋抵押动态比例:src.HOSGUARANTYRATIO1
       ,DST.CURCONTRACTCURRENCY                                --土地担保币种:src.CURCONTRACTCURRENCY
       ,DST.CURCONTRACTSUM                                     --土地参与担保金额:src.CURCONTRACTSUM
       ,DST.CURGUARANTYRATIO                                   --土地抵押比例:src.CURGUARANTYRATIO
       ,DST.CURGUARANTYRATIO1                                  --土地抵押动态比例:src.CURGUARANTYRATIO1
       ,DST.CUREVALCURRENCY                                    --土地评估币种:src.CUREVALCURRENCY
       ,DST.CUREVALNETVALUE                                    --土地评估价值:src.CUREVALNETVALUE
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
       ,DST.SUBBILLTYPE                                        --票据种类:src.SUBBILLTYPE
   FROM F_LN_XDXT_GUARANTY_INFO DST 
   LEFT JOIN F_LN_XDXT_GUARANTY_INFO_INNTMP1 SRC 
     ON SRC.GUARANTYID          = DST.GUARANTYID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.GUARANTYID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_GUARANTY_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_LN_XDXT_GUARANTY_INFO/"+V_DT+".parquet"
F_LN_XDXT_GUARANTY_INFO_INNTMP2=F_LN_XDXT_GUARANTY_INFO_INNTMP2.unionAll(F_LN_XDXT_GUARANTY_INFO_INNTMP1)
F_LN_XDXT_GUARANTY_INFO_INNTMP1.cache()
F_LN_XDXT_GUARANTY_INFO_INNTMP2.cache()
nrowsi = F_LN_XDXT_GUARANTY_INFO_INNTMP1.count()
nrowsa = F_LN_XDXT_GUARANTY_INFO_INNTMP2.count()
F_LN_XDXT_GUARANTY_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_LN_XDXT_GUARANTY_INFO_INNTMP1.unpersist()
F_LN_XDXT_GUARANTY_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_XDXT_GUARANTY_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_LN_XDXT_GUARANTY_INFO/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_GUARANTY_INFO_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_GUARANTY_INFO_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_GUARANTY_INFO/"+V_DT+".parquet /"+dbname+"/F_LN_XDXT_GUARANTY_INFO_BK/"+V_DT+".parquet")
