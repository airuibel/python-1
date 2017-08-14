#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_ENT_INFO').setMaster(sys.argv[2])
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

O_CI_XDXT_ENT_INFO = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_ENT_INFO/*')
O_CI_XDXT_ENT_INFO.registerTempTable("O_CI_XDXT_ENT_INFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_ENT_INFO/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_ENT_INFO_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_ENT_INFO/"+V_DT+".parquet")


F_CI_XDXT_ENT_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_ENT_INFO/*')
F_CI_XDXT_ENT_INFO.registerTempTable("F_CI_XDXT_ENT_INFO")

sql = """
 SELECT A.CUSTOMERID            AS CUSTOMERID 
       ,A.CORPID                AS CORPID 
       ,A.ENTERPRISENAME        AS ENTERPRISENAME 
       ,A.ENGLISHNAME           AS ENGLISHNAME 
       ,A.FICTITIOUSPERSON      AS FICTITIOUSPERSON 
       ,A.ORGNATURE             AS ORGNATURE 
       ,A.FINANCETYPE           AS FINANCETYPE 
       ,A.ENTERPRISEBELONG      AS ENTERPRISEBELONG 
       ,A.INDUSTRYTYPE          AS INDUSTRYTYPE 
       ,A.INDUSTRYTYPE1         AS INDUSTRYTYPE1 
       ,A.INDUSTRYTYPE2         AS INDUSTRYTYPE2 
       ,A.PRIVATE               AS PRIVATE 
       ,A.ECONOMYTYPE           AS ECONOMYTYPE 
       ,A.ORGTYPE               AS ORGTYPE 
       ,A.MOSTBUSINESS          AS MOSTBUSINESS 
       ,A.BUDGETTYPE            AS BUDGETTYPE 
       ,A.RCCURRENCY            AS RCCURRENCY 
       ,A.REGISTERCAPITAL       AS REGISTERCAPITAL 
       ,A.PCCURRENCY            AS PCCURRENCY 
       ,A.PAICLUPCAPITAL        AS PAICLUPCAPITAL 
       ,A.FUNDSOURCE            AS FUNDSOURCE 
       ,A.TOTALASSETS           AS TOTALASSETS 
       ,A.NETASSETS             AS NETASSETS 
       ,A.ANNUALINCOME          AS ANNUALINCOME 
       ,A.SCOPE                 AS SCOPE 
       ,A.LIMIT                 AS LIMIT 
       ,A.CREDITDATE            AS CREDITDATE 
       ,A.LICENSENO             AS LICENSENO 
       ,A.LICENSEDATE           AS LICENSEDATE 
       ,A.LICENSEMATURITY       AS LICENSEMATURITY 
       ,A.SETUPDATE             AS SETUPDATE 
       ,A.INSPECTIONYEAR        AS INSPECTIONYEAR 
       ,A.LOCKSITUATION         AS LOCKSITUATION 
       ,A.TAXNO                 AS TAXNO 
       ,A.BANKLICENSE           AS BANKLICENSE 
       ,A.BANKID                AS BANKID 
       ,A.MANAGEAREA            AS MANAGEAREA 
       ,A.BANCHAMOUNT           AS BANCHAMOUNT 
       ,A.EXCHANGEID            AS EXCHANGEID 
       ,A.REGISTERADD           AS REGISTERADD 
       ,A.CHARGEDEPARTMENT      AS CHARGEDEPARTMENT 
       ,A.OFFICEADD             AS OFFICEADD 
       ,A.OFFICEZIP             AS OFFICEZIP 
       ,A.COUNTRYCODE           AS COUNTRYCODE 
       ,A.REGIONCODE            AS REGIONCODE 
       ,A.VILLAGECODE           AS VILLAGECODE 
       ,A.VILLAGENAME           AS VILLAGENAME 
       ,A.RELATIVETYPE          AS RELATIVETYPE 
       ,A.OFFICETEL             AS OFFICETEL 
       ,A.OFFICEFAX             AS OFFICEFAX 
       ,A.WEBADD                AS WEBADD 
       ,A.EMAILADD              AS EMAILADD 
       ,A.EMPLOYEENUMBER        AS EMPLOYEENUMBER 
       ,A.MAINPRODUCTION        AS MAINPRODUCTION 
       ,A.NEWTECHCORPORNOT      AS NEWTECHCORPORNOT 
       ,A.LISTINGCORPORNOT      AS LISTINGCORPORNOT 
       ,A.HASIERIGHT            AS HASIERIGHT 
       ,A.HASDIRECTORATE        AS HASDIRECTORATE 
       ,A.BASICBANK             AS BASICBANK 
       ,A.BASICACCOUNT          AS BASICACCOUNT 
       ,A.MANAGEINFO            AS MANAGEINFO 
       ,A.CUSTOMERHISTORY       AS CUSTOMERHISTORY 
       ,A.PROJECTFLAG           AS PROJECTFLAG 
       ,A.REALTYFLAG            AS REALTYFLAG 
       ,A.WORKFIELDAREA         AS WORKFIELDAREA 
       ,A.WORKFIELDFEE          AS WORKFIELDFEE 
       ,A.ACCOUNTDATE           AS ACCOUNTDATE 
       ,A.LOANCARDNO            AS LOANCARDNO 
       ,A.LOANCARDPASSWORD      AS LOANCARDPASSWORD 
       ,A.LOANCARDINSYEAR       AS LOANCARDINSYEAR 
       ,A.LOANCARDINSRESULT     AS LOANCARDINSRESULT 
       ,A.LOANFLAG              AS LOANFLAG 
       ,A.FINANCEORNOT          AS FINANCEORNOT 
       ,A.FINANCEBELONG         AS FINANCEBELONG 
       ,A.CREDITBELONG          AS CREDITBELONG 
       ,A.CREDITLEVEL           AS CREDITLEVEL 
       ,A.EVALUATEDATE          AS EVALUATEDATE 
       ,A.OTHERCREDITLEVEL      AS OTHERCREDITLEVEL 
       ,A.OTHEREVALUATEDATE     AS OTHEREVALUATEDATE 
       ,A.OTHERORGNAME          AS OTHERORGNAME 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.UPDATEORGID           AS UPDATEORGID 
       ,A.UPDATEUSERID          AS UPDATEUSERID 
       ,A.REMARK                AS REMARK 
       ,A.TAXNO1                AS TAXNO1 
       ,A.FICTITIOUSPERSONID    AS FICTITIOUSPERSONID 
       ,A.GROUPFLAG             AS GROUPFLAG 
       ,A.EVALUATELEVEL         AS EVALUATELEVEL 
       ,A.MYBANK                AS MYBANK 
       ,A.MYBANKACCOUNT         AS MYBANKACCOUNT 
       ,A.OTHERBANK             AS OTHERBANK 
       ,A.OTHERBANKACCOUNT      AS OTHERBANKACCOUNT 
       ,A.TEMPSAVEFLAG          AS TEMPSAVEFLAG 
       ,A.FINANCEDEPTTEL        AS FINANCEDEPTTEL 
       ,A.ECGROUPFLAG           AS ECGROUPFLAG 
       ,A.SUPERCORPNAME         AS SUPERCORPNAME 
       ,A.SUPERLOANCARDNO       AS SUPERLOANCARDNO 
       ,A.SUPERCERTTYPE         AS SUPERCERTTYPE 
       ,A.SMEINDUSTRYTYPE       AS SMEINDUSTRYTYPE 
       ,A.SELLSUM               AS SELLSUM 
       ,A.SUPERCERTID           AS SUPERCERTID 
       ,A.AGRRELATE             AS AGRRELATE 
       ,A.AGRLEVEL              AS AGRLEVEL 
       ,A.MYBANKSCOPE           AS MYBANKSCOPE 
       ,A.MYBANKDORM            AS MYBANKDORM 
       ,A.RECOMMENDPER          AS RECOMMENDPER 
       ,A.TRADE                 AS TRADE 
       ,A.STEPRLASTTIME         AS STEPRLASTTIME 
       ,A.WHOLEPROPERTY         AS WHOLEPROPERTY 
       ,A.SALEROOM              AS SALEROOM 
       ,A.ACCCREDITUSETIME      AS ACCCREDITUSETIME 
       ,A.RATETIME              AS RATETIME 
       ,A.MANAGEUSERID          AS MANAGEUSERID 
       ,A.MANAGEORGID           AS MANAGEORGID 
       ,A.RISKSTATUS            AS RISKSTATUS 
       ,A.MYACCOUNTTYPE         AS MYACCOUNTTYPE 
       ,A.MYACCOUNTNO           AS MYACCOUNTNO 
       ,A.FINANCESOURCE         AS FINANCESOURCE 
       ,A.LOANCARDYEAR          AS LOANCARDYEAR 
       ,A.CORPORATEORGID        AS CORPORATEORGID 
       ,A.LICENSECHECK          AS LICENSECHECK 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.IDENTIFYFLAG          AS IDENTIFYFLAG 
       ,A.CUSTOMERKEYTYPE       AS CUSTOMERKEYTYPE 
       ,A.CLIENTCLASSN          AS CLIENTCLASSN 
       ,A.CLIENTCLASSM          AS CLIENTCLASSM 
       ,A.ISREALTIVEMAN         AS ISREALTIVEMAN 
       ,A.GOUPCREDITSUM         AS GOUPCREDITSUM 
       ,A.GROUPCREDITMODE       AS GROUPCREDITMODE 
       ,A.ISBANKSIGN            AS ISBANKSIGN 
       ,A.ISRELATVIE            AS ISRELATVIE 
       ,A.CUSTOMERCLASSIFY      AS CUSTOMERCLASSIFY 
       ,A.CUSTOMERCLASSIFYDATE  AS CUSTOMERCLASSIFYDATE 
       ,A.ENTINDUSTRIALSCALE    AS ENTINDUSTRIALSCALE 
       ,A.ENTRETAILSCALE        AS ENTRETAILSCALE 
       ,A.ORGCREDITCODE         AS ORGCREDITCODE 
       ,A.GROUPTYPE             AS GROUPTYPE 
       ,A.CORPIDASDATE          AS CORPIDASDATE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_ENT_INFO A                                   --企业基本信息
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_ENT_INFO_INNTMP1 = sqlContext.sql(sql)
F_CI_XDXT_ENT_INFO_INNTMP1.registerTempTable("F_CI_XDXT_ENT_INFO_INNTMP1")

#F_CI_XDXT_ENT_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_ENT_INFO/*')
#F_CI_XDXT_ENT_INFO.registerTempTable("F_CI_XDXT_ENT_INFO")
sql = """
 SELECT DST.CUSTOMERID                                          --客户编号:src.CUSTOMERID
       ,DST.CORPID                                             --法人或组织机构代码:src.CORPID
       ,DST.ENTERPRISENAME                                     --企业名称:src.ENTERPRISENAME
       ,DST.ENGLISHNAME                                        --客户英文名称:src.ENGLISHNAME
       ,DST.FICTITIOUSPERSON                                   --法人代表:src.FICTITIOUSPERSON
       ,DST.ORGNATURE                                          --机构类型:src.ORGNATURE
       ,DST.FINANCETYPE                                        --金融机构类型:src.FINANCETYPE
       ,DST.ENTERPRISEBELONG                                   --企业隶属:src.ENTERPRISEBELONG
       ,DST.INDUSTRYTYPE                                       --行业类型:src.INDUSTRYTYPE
       ,DST.INDUSTRYTYPE1                                      --行业类型1:src.INDUSTRYTYPE1
       ,DST.INDUSTRYTYPE2                                      --行业类型2:src.INDUSTRYTYPE2
       ,DST.PRIVATE                                            --民营标志:src.PRIVATE
       ,DST.ECONOMYTYPE                                        --经济类型:src.ECONOMYTYPE
       ,DST.ORGTYPE                                            --组织形式:src.ORGTYPE
       ,DST.MOSTBUSINESS                                       --主营业务:src.MOSTBUSINESS
       ,DST.BUDGETTYPE                                         --预算管理类型:src.BUDGETTYPE
       ,DST.RCCURRENCY                                         --注册资本币种:src.RCCURRENCY
       ,DST.REGISTERCAPITAL                                    --注册资本:src.REGISTERCAPITAL
       ,DST.PCCURRENCY                                         --实收资本币种:src.PCCURRENCY
       ,DST.PAICLUPCAPITAL                                     --实收资本:src.PAICLUPCAPITAL
       ,DST.FUNDSOURCE                                         --经费来源:src.FUNDSOURCE
       ,DST.TOTALASSETS                                        --总资产:src.TOTALASSETS
       ,DST.NETASSETS                                          --净资产:src.NETASSETS
       ,DST.ANNUALINCOME                                       --年收入:src.ANNUALINCOME
       ,DST.SCOPE                                              --企业规模:src.SCOPE
       ,DST.LIMIT                                              --额度:src.LIMIT
       ,DST.CREDITDATE                                         --首次建立信贷关系年月:src.CREDITDATE
       ,DST.LICENSENO                                          --工商执照登记号:src.LICENSENO
       ,DST.LICENSEDATE                                        --营业执照到期日期:src.LICENSEDATE
       ,DST.LICENSEMATURITY                                    --工商执照到期日:src.LICENSEMATURITY
       ,DST.SETUPDATE                                          --公司成立日期:src.SETUPDATE
       ,DST.INSPECTIONYEAR                                     --工商执照最新年检年份:src.INSPECTIONYEAR
       ,DST.LOCKSITUATION                                      --工商局锁定情况:src.LOCKSITUATION
       ,DST.TAXNO                                              --税务登记证号(国税):src.TAXNO
       ,DST.BANKLICENSE                                        --金融机构许可证:src.BANKLICENSE
       ,DST.BANKID                                             --金融机构代码:src.BANKID
       ,DST.MANAGEAREA                                         --金融机构经营区域范围:src.MANAGEAREA
       ,DST.BANCHAMOUNT                                        --金融机构一级分支机构数量:src.BANCHAMOUNT
       ,DST.EXCHANGEID                                         --交换号:src.EXCHANGEID
       ,DST.REGISTERADD                                        --注册地址:src.REGISTERADD
       ,DST.CHARGEDEPARTMENT                                   --上级主管部门:src.CHARGEDEPARTMENT
       ,DST.OFFICEADD                                          --实际办公地址:src.OFFICEADD
       ,DST.OFFICEZIP                                          --邮政编码:src.OFFICEZIP
       ,DST.COUNTRYCODE                                        --所在国家地区:src.COUNTRYCODE
       ,DST.REGIONCODE                                         --所在行政区域:src.REGIONCODE
       ,DST.VILLAGECODE                                        --所属行政村编号:src.VILLAGECODE
       ,DST.VILLAGENAME                                        --所属行政村名称:src.VILLAGENAME
       ,DST.RELATIVETYPE                                       --联系方式:src.RELATIVETYPE
       ,DST.OFFICETEL                                          --联系电话:src.OFFICETEL
       ,DST.OFFICEFAX                                          --传真号码:src.OFFICEFAX
       ,DST.WEBADD                                             --公司网址:src.WEBADD
       ,DST.EMAILADD                                           --公司邮件地址:src.EMAILADD
       ,DST.EMPLOYEENUMBER                                     --员工人数:src.EMPLOYEENUMBER
       ,DST.MAINPRODUCTION                                     --主要产品和服务情况:src.MAINPRODUCTION
       ,DST.NEWTECHCORPORNOT                                   --是否高新技术企业:src.NEWTECHCORPORNOT
       ,DST.LISTINGCORPORNOT                                   --是否上市企业:src.LISTINGCORPORNOT
       ,DST.HASIERIGHT                                         --由无进出口经营权:src.HASIERIGHT
       ,DST.HASDIRECTORATE                                     --有无董事会:src.HASDIRECTORATE
       ,DST.BASICBANK                                          --基本帐户行:src.BASICBANK
       ,DST.BASICACCOUNT                                       --基本帐户号:src.BASICACCOUNT
       ,DST.MANAGEINFO                                         --合法经营情况:src.MANAGEINFO
       ,DST.CUSTOMERHISTORY                                    --客户历史沿革、管理水平简介:src.CUSTOMERHISTORY
       ,DST.PROJECTFLAG                                        --企业目前是否有项目:src.PROJECTFLAG
       ,DST.REALTYFLAG                                         --是否从事房地产开发:src.REALTYFLAG
       ,DST.WORKFIELDAREA                                      --经营场所面积:src.WORKFIELDAREA
       ,DST.WORKFIELDFEE                                       --经营场地所有权:src.WORKFIELDFEE
       ,DST.ACCOUNTDATE                                        --开户时间:src.ACCOUNTDATE
       ,DST.LOANCARDNO                                         --贷款卡号:src.LOANCARDNO
       ,DST.LOANCARDPASSWORD                                   --贷款卡密码:src.LOANCARDPASSWORD
       ,DST.LOANCARDINSYEAR                                    --贷款卡最新年审年份:src.LOANCARDINSYEAR
       ,DST.LOANCARDINSRESULT                                  --贷款卡最新年审结果:src.LOANCARDINSRESULT
       ,DST.LOANFLAG                                           --贷款卡是否有效:src.LOANFLAG
       ,DST.FINANCEORNOT                                       --是否无须提供财务报表:src.FINANCEORNOT
       ,DST.FINANCEBELONG                                      --财务报表所属:src.FINANCEBELONG
       ,DST.CREDITBELONG                                       --信用等级评估表类型:src.CREDITBELONG
       ,DST.CREDITLEVEL                                        --本行评估即期信用等级:src.CREDITLEVEL
       ,DST.EVALUATEDATE                                       --本行评估日期:src.EVALUATEDATE
       ,DST.OTHERCREDITLEVEL                                   --外部机构评估信用等级:src.OTHERCREDITLEVEL
       ,DST.OTHEREVALUATEDATE                                  --外部机构评估日期:src.OTHEREVALUATEDATE
       ,DST.OTHERORGNAME                                       --外部评级机构名称:src.OTHERORGNAME
       ,DST.INPUTORGID                                         --登记单位:src.INPUTORGID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTDATE                                          --登记日期:src.INPUTDATE
       ,DST.UPDATEORGID                                        --更新机构:src.UPDATEORGID
       ,DST.UPDATEUSERID                                       --更新人:src.UPDATEUSERID
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.TAXNO1                                             --税务登记证号(地税):src.TAXNO1
       ,DST.FICTITIOUSPERSONID                                 --法人代表身份证:src.FICTITIOUSPERSONID
       ,DST.GROUPFLAG                                          --是否关联集团:src.GROUPFLAG
       ,DST.EVALUATELEVEL                                      --信用等级认定级别:src.EVALUATELEVEL
       ,DST.MYBANK                                             --我行账户行:src.MYBANK
       ,DST.MYBANKACCOUNT                                      --我行帐号:src.MYBANKACCOUNT
       ,DST.OTHERBANK                                          --他行账户行:src.OTHERBANK
       ,DST.OTHERBANKACCOUNT                                   --他行帐号:src.OTHERBANKACCOUNT
       ,DST.TEMPSAVEFLAG                                       --暂存标志:src.TEMPSAVEFLAG
       ,DST.FINANCEDEPTTEL                                     --财务部联系方式:src.FINANCEDEPTTEL
       ,DST.ECGROUPFLAG                                        --是否集团客户:src.ECGROUPFLAG
       ,DST.SUPERCORPNAME                                      --上级公司名称:src.SUPERCORPNAME
       ,DST.SUPERLOANCARDNO                                    --上级公司贷款卡号:src.SUPERLOANCARDNO
       ,DST.SUPERCERTTYPE                                      --上级公司证件类型:src.SUPERCERTTYPE
       ,DST.SMEINDUSTRYTYPE                                    --SMEINDUSTRYTYPE:src.SMEINDUSTRYTYPE
       ,DST.SELLSUM                                            --SELLSUM:src.SELLSUM
       ,DST.SUPERCERTID                                        --上级公司证件编号:src.SUPERCERTID
       ,DST.AGRRELATE                                          --是否涉农企业:src.AGRRELATE
       ,DST.AGRLEVEL                                           --农业产业化企业级别:src.AGRLEVEL
       ,DST.MYBANKSCOPE                                        --我行企业规模:src.MYBANKSCOPE
       ,DST.MYBANKDORM                                         --是否我行股东:src.MYBANKDORM
       ,DST.RECOMMENDPER                                       --推荐人:src.RECOMMENDPER
       ,DST.TRADE                                              --行业类型:src.TRADE
       ,DST.STEPRLASTTIME                                      --分期筹资的最后一期的时间:src.STEPRLASTTIME
       ,DST.WHOLEPROPERTY                                      --资产总额:src.WHOLEPROPERTY
       ,DST.SALEROOM                                           --销售额:src.SALEROOM
       ,DST.ACCCREDITUSETIME                                   --客户等级即期评级有效期:src.ACCCREDITUSETIME
       ,DST.RATETIME                                           --等级评级日期:src.RATETIME
       ,DST.MANAGEUSERID                                       --管户人:src.MANAGEUSERID
       ,DST.MANAGEORGID                                        --管户机构:src.MANAGEORGID
       ,DST.RISKSTATUS                                         --是否我行风险预警客户:src.RISKSTATUS
       ,DST.MYACCOUNTTYPE                                      --本社开立帐户类型:src.MYACCOUNTTYPE
       ,DST.MYACCOUNTNO                                        --MYACCOUNTNO:src.MYACCOUNTNO
       ,DST.FINANCESOURCE                                      --<null>:src.FINANCESOURCE
       ,DST.LOANCARDYEAR                                       --贷款卡最新年审年份:src.LOANCARDYEAR
       ,DST.CORPORATEORGID                                     --法人机构号:src.CORPORATEORGID
       ,DST.LICENSECHECK                                       --营业执照年检日期:src.LICENSECHECK
       ,DST.CERTTYPE                                           --证件类型:src.CERTTYPE
       ,DST.IDENTIFYFLAG                                       --集团认定标志:src.IDENTIFYFLAG
       ,DST.CUSTOMERKEYTYPE                                    --CustomerKeyType:src.CUSTOMERKEYTYPE
       ,DST.CLIENTCLASSN                                       --当前客户分类:src.CLIENTCLASSN
       ,DST.CLIENTCLASSM                                       --证件类型客户分类调整:src.CLIENTCLASSM
       ,DST.ISREALTIVEMAN                                      --是否本行社其他关系人:src.ISREALTIVEMAN
       ,DST.GOUPCREDITSUM                                      --集团授信限额:src.GOUPCREDITSUM
       ,DST.GROUPCREDITMODE                                    --集团成员授信控制方式:src.GROUPCREDITMODE
       ,DST.ISBANKSIGN                                         --ISBANKSIGN:src.ISBANKSIGN
       ,DST.ISRELATVIE                                         --ISRELATVIE:src.ISRELATVIE
       ,DST.CUSTOMERCLASSIFY                                   --CUSTOMERCLASSIFY:src.CUSTOMERCLASSIFY
       ,DST.CUSTOMERCLASSIFYDATE                               --CUSTOMERCLASSIFYDATE:src.CUSTOMERCLASSIFYDATE
       ,DST.ENTINDUSTRIALSCALE                                 --ENTINDUSTRIALSCALE:src.ENTINDUSTRIALSCALE
       ,DST.ENTRETAILSCALE                                     --ENTRETAILSCALE:src.ENTRETAILSCALE
       ,DST.ORGCREDITCODE                                      --ORGCREDITCODE:src.ORGCREDITCODE
       ,DST.GROUPTYPE                                          --GROUPTYPE:src.GROUPTYPE
       ,DST.CORPIDASDATE                                       --CORPIDASDATE:src.CORPIDASDATE
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_CI_XDXT_ENT_INFO DST 
   LEFT JOIN F_CI_XDXT_ENT_INFO_INNTMP1 SRC 
     ON SRC.CUSTOMERID          = DST.CUSTOMERID 
  WHERE SRC.CUSTOMERID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_ENT_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_XDXT_ENT_INFO/"+V_DT+".parquet"
F_CI_XDXT_ENT_INFO_INNTMP2=F_CI_XDXT_ENT_INFO_INNTMP2.unionAll(F_CI_XDXT_ENT_INFO_INNTMP1)
F_CI_XDXT_ENT_INFO_INNTMP1.cache()
F_CI_XDXT_ENT_INFO_INNTMP2.cache()
nrowsi = F_CI_XDXT_ENT_INFO_INNTMP1.count()
nrowsa = F_CI_XDXT_ENT_INFO_INNTMP2.count()
F_CI_XDXT_ENT_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_ENT_INFO_INNTMP1.unpersist()
F_CI_XDXT_ENT_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_ENT_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_XDXT_ENT_INFO/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_ENT_INFO_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_ENT_INFO_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_ENT_INFO/"+V_DT+".parquet /"+dbname+"/F_CI_XDXT_ENT_INFO_BK/"+V_DT+".parquet")
