#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_IND_INFO').setMaster(sys.argv[2])
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

O_CI_XDXT_IND_INFO = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_IND_INFO/*')
O_CI_XDXT_IND_INFO.registerTempTable("O_CI_XDXT_IND_INFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_IND_INFO/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_IND_INFO_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_IND_INFO/"+V_DT+".parquet")


F_CI_XDXT_IND_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_INFO/*')
F_CI_XDXT_IND_INFO.registerTempTable("F_CI_XDXT_IND_INFO")

sql = """
 SELECT A.CUSTOMERID            AS CUSTOMERID 
       ,A.FULLNAME              AS FULLNAME 
       ,A.SEX                   AS SEX 
       ,A.BIRTHDAY              AS BIRTHDAY 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CERTID                AS CERTID 
       ,A.SINO                  AS SINO 
       ,A.COUNTRY               AS COUNTRY 
       ,A.NATIONALITY           AS NATIONALITY 
       ,A.NATIVEPLACE           AS NATIVEPLACE 
       ,A.POLITICALFACE         AS POLITICALFACE 
       ,A.MARRIAGE              AS MARRIAGE 
       ,A.RELATIVETYPE          AS RELATIVETYPE 
       ,A.FAMILYADD             AS FAMILYADD 
       ,A.FAMILYZIP             AS FAMILYZIP 
       ,A.EMAILADD              AS EMAILADD 
       ,A.FAMILYTEL             AS FAMILYTEL 
       ,A.MOBILETELEPHONE       AS MOBILETELEPHONE 
       ,A.UNITKIND              AS UNITKIND 
       ,A.WORKCORP              AS WORKCORP 
       ,A.WORKADD               AS WORKADD 
       ,A.WORKTEL               AS WORKTEL 
       ,A.OCCUPATION            AS OCCUPATION 
       ,A.POSITION              AS POSITION 
       ,A.EMPLOYRECORD          AS EMPLOYRECORD 
       ,A.EDURECORD             AS EDURECORD 
       ,A.EDUEXPERIENCE         AS EDUEXPERIENCE 
       ,A.EDUDEGREE             AS EDUDEGREE 
       ,A.GRADUATEYEAR          AS GRADUATEYEAR 
       ,A.FINANCEBELONG         AS FINANCEBELONG 
       ,A.CREDITLEVEL           AS CREDITLEVEL 
       ,A.EVALUATEDATE          AS EVALUATEDATE 
       ,A.BALANCESHEET          AS BALANCESHEET 
       ,A.INTRO                 AS INTRO 
       ,A.SELFMONTHINCOME       AS SELFMONTHINCOME 
       ,A.FAMILYMONTHINCOME     AS FAMILYMONTHINCOME 
       ,A.INCOMESOURCE          AS INCOMESOURCE 
       ,A.POPULATION            AS POPULATION 
       ,A.LOANCARDNO            AS LOANCARDNO 
       ,A.LOANCARDINSYEAR       AS LOANCARDINSYEAR 
       ,A.FARMERSORT            AS FARMERSORT 
       ,A.REGIONALISM           AS REGIONALISM 
       ,A.STAFF                 AS STAFF 
       ,A.CREDITFARMER          AS CREDITFARMER 
       ,A.RISKINCLINATION       AS RISKINCLINATION 
       ,A.CHARACTER             AS CHARACTER 
       ,A.DATAQUALITY           AS DATAQUALITY 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.REMARK                AS REMARK 
       ,A.UPDATEORGID           AS UPDATEORGID 
       ,A.UPDATEUSERID          AS UPDATEUSERID 
       ,A.COMMADD               AS COMMADD 
       ,A.COMMZIP               AS COMMZIP 
       ,A.NATIVEADD             AS NATIVEADD 
       ,A.WORKZIP               AS WORKZIP 
       ,A.HEADSHIP              AS HEADSHIP 
       ,A.WORKBEGINDATE         AS WORKBEGINDATE 
       ,A.YEARINCOME            AS YEARINCOME 
       ,A.PAYACCOUNT            AS PAYACCOUNT 
       ,A.PAYACCOUNTBANK        AS PAYACCOUNTBANK 
       ,A.FAMILYSTATUS          AS FAMILYSTATUS 
       ,A.TEMPSAVEFLAG          AS TEMPSAVEFLAG 
       ,A.CERTID18              AS CERTID18 
       ,A.ECONOMYDCOID          AS ECONOMYDCOID 
       ,A.MYBALANCEACCOUNT      AS MYBALANCEACCOUNT 
       ,A.HEALTH                AS HEALTH 
       ,A.HABIT                 AS HABIT 
       ,A.SPECIALITY            AS SPECIALITY 
       ,A.MYBANKDORM            AS MYBANKDORM 
       ,A.ASSOCIATOR            AS ASSOCIATOR 
       ,A.ISBUSINESSMAN         AS ISBUSINESSMAN 
       ,A.TEAMNO                AS TEAMNO 
       ,A.HOUSEMASTERNAME       AS HOUSEMASTERNAME 
       ,A.FAMILYNUM             AS FAMILYNUM 
       ,A.FAMILYYEARINCOME      AS FAMILYYEARINCOME 
       ,A.TEAMNAME              AS TEAMNAME 
       ,A.FARMERMAININDUS       AS FARMERMAININDUS 
       ,A.LOANFLAG              AS LOANFLAG 
       ,A.VILLAGECADRE          AS VILLAGECADRE 
       ,A.MEDICARE              AS MEDICARE 
       ,A.POORISNO              AS POORISNO 
       ,A.POORNO                AS POORNO 
       ,A.MARKUP                AS MARKUP 
       ,A.MANAGETYPE            AS MANAGETYPE 
       ,A.MANAGEPLACE           AS MANAGEPLACE 
       ,A.LICENSENO             AS LICENSENO 
       ,A.LICENSEINPUTDATE      AS LICENSEINPUTDATE 
       ,A.TAXNO                 AS TAXNO 
       ,A.EVALUATEOVERDATE      AS EVALUATEOVERDATE 
       ,A.BEHAVE                AS BEHAVE 
       ,A.MAINPROORINCOME       AS MAINPROORINCOME 
       ,A.WEALTHNAME            AS WEALTHNAME 
       ,A.WEALTHVALUE           AS WEALTHVALUE 
       ,A.SAFEBALANCE           AS SAFEBALANCE 
       ,A.CREDITRECORD          AS CREDITRECORD 
       ,A.ACCOUNTID             AS ACCOUNTID 
       ,A.FAMILYYEARIN          AS FAMILYYEARIN 
       ,A.SYMBIOSIS             AS SYMBIOSIS 
       ,A.FARMERCARD            AS FARMERCARD 
       ,A.CONLANDAREA           AS CONLANDAREA 
       ,A.CONPOOLAREA           AS CONPOOLAREA 
       ,A.MANAGEUSERID          AS MANAGEUSERID 
       ,A.MANAGEORGID           AS MANAGEORGID 
       ,A.FAMILYAMOUNT          AS FAMILYAMOUNT 
       ,A.CORPORATEORGID        AS CORPORATEORGID 
       ,A.CREDITBELONG          AS CREDITBELONG 
       ,A.ISREALTIVEMAN         AS ISREALTIVEMAN 
       ,A.OCCUPATION1           AS OCCUPATION1 
       ,A.LOCALYEAR             AS LOCALYEAR 
       ,A.REMARK1               AS REMARK1 
       ,A.REMARK2               AS REMARK2 
       ,A.REMARK3               AS REMARK3 
       ,A.REMARK4               AS REMARK4 
       ,A.CENSUSBOOKID          AS CENSUSBOOKID 
       ,A.HOUSEMASTERCERTTYPE   AS HOUSEMASTERCERTTYPE 
       ,A.HOUSEMASTERCERTID     AS HOUSEMASTERCERTID 
       ,A.CUSTOMERKEYTYPE       AS CUSTOMERKEYTYPE 
       ,A.VILLAGENO             AS VILLAGENO 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_IND_INFO A                                   --个人基本信息
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_IND_INFO_INNTMP1 = sqlContext.sql(sql)
F_CI_XDXT_IND_INFO_INNTMP1.registerTempTable("F_CI_XDXT_IND_INFO_INNTMP1")

#F_CI_XDXT_IND_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_IND_INFO/*')
#F_CI_XDXT_IND_INFO.registerTempTable("F_CI_XDXT_IND_INFO")
sql = """
 SELECT DST.CUSTOMERID                                          --客户编号:src.CUSTOMERID
       ,DST.FULLNAME                                           --姓名:src.FULLNAME
       ,DST.SEX                                                --性别:src.SEX
       ,DST.BIRTHDAY                                           --出生日期:src.BIRTHDAY
       ,DST.CERTTYPE                                           --证件类型:src.CERTTYPE
       ,DST.CERTID                                             --证件号:src.CERTID
       ,DST.SINO                                               --社会保险号:src.SINO
       ,DST.COUNTRY                                            --国籍:src.COUNTRY
       ,DST.NATIONALITY                                        --民族:src.NATIONALITY
       ,DST.NATIVEPLACE                                        --籍贯:src.NATIVEPLACE
       ,DST.POLITICALFACE                                      --政治面貌:src.POLITICALFACE
       ,DST.MARRIAGE                                           --婚姻状况:src.MARRIAGE
       ,DST.RELATIVETYPE                                       --联系方式:src.RELATIVETYPE
       ,DST.FAMILYADD                                          --家庭住址:src.FAMILYADD
       ,DST.FAMILYZIP                                          --邮政编码:src.FAMILYZIP
       ,DST.EMAILADD                                           --电子邮箱:src.EMAILADD
       ,DST.FAMILYTEL                                          --住宅电话:src.FAMILYTEL
       ,DST.MOBILETELEPHONE                                    --手机号码:src.MOBILETELEPHONE
       ,DST.UNITKIND                                           --单位性质:src.UNITKIND
       ,DST.WORKCORP                                           --工作单位:src.WORKCORP
       ,DST.WORKADD                                            --工作地址:src.WORKADD
       ,DST.WORKTEL                                            --单位电话:src.WORKTEL
       ,DST.OCCUPATION                                         --职业:src.OCCUPATION
       ,DST.POSITION                                           --职称:src.POSITION
       ,DST.EMPLOYRECORD                                       --工作履历:src.EMPLOYRECORD
       ,DST.EDURECORD                                          --学业履历:src.EDURECORD
       ,DST.EDUEXPERIENCE                                      --最高学历:src.EDUEXPERIENCE
       ,DST.EDUDEGREE                                          --最高学位:src.EDUDEGREE
       ,DST.GRADUATEYEAR                                       --毕业年份:src.GRADUATEYEAR
       ,DST.FINANCEBELONG                                      --财务报表所属:src.FINANCEBELONG
       ,DST.CREDITLEVEL                                        --本行评估即期信用等级:src.CREDITLEVEL
       ,DST.EVALUATEDATE                                       --评估日期:src.EVALUATEDATE
       ,DST.BALANCESHEET                                       --个人资产与负债详情:src.BALANCESHEET
       ,DST.INTRO                                              --个人情况简介:src.INTRO
       ,DST.SELFMONTHINCOME                                    --个人月收入:src.SELFMONTHINCOME
       ,DST.FAMILYMONTHINCOME                                  --家庭月收入:src.FAMILYMONTHINCOME
       ,DST.INCOMESOURCE                                       --主要收入来源:src.INCOMESOURCE
       ,DST.POPULATION                                         --家庭人口数:src.POPULATION
       ,DST.LOANCARDNO                                         --贷款卡编号:src.LOANCARDNO
       ,DST.LOANCARDINSYEAR                                    --贷款卡最新年审年份:src.LOANCARDINSYEAR
       ,DST.FARMERSORT                                         --农户分类:src.FARMERSORT
       ,DST.REGIONALISM                                        --所属行政区域:src.REGIONALISM
       ,DST.STAFF                                              --是否本行员工:src.STAFF
       ,DST.CREDITFARMER                                       --是否信用农户:src.CREDITFARMER
       ,DST.RISKINCLINATION                                    --风险偏好:src.RISKINCLINATION
       ,DST.CHARACTER                                          --性格爱好:src.CHARACTER
       ,DST.DATAQUALITY                                        --数据质量检查标志:src.DATAQUALITY
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTDATE                                          --输入日期:src.INPUTDATE
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.UPDATEORGID                                        --更新机构:src.UPDATEORGID
       ,DST.UPDATEUSERID                                       --更新人:src.UPDATEUSERID
       ,DST.COMMADD                                            --通讯地址:src.COMMADD
       ,DST.COMMZIP                                            --通讯地址邮政编码:src.COMMZIP
       ,DST.NATIVEADD                                          --户籍地址:src.NATIVEADD
       ,DST.WORKZIP                                            --单位地址邮政编码:src.WORKZIP
       ,DST.HEADSHIP                                           --职务:src.HEADSHIP
       ,DST.WORKBEGINDATE                                      --本单位工作起始年份:src.WORKBEGINDATE
       ,DST.YEARINCOME                                         --年收入:src.YEARINCOME
       ,DST.PAYACCOUNT                                         --存款帐号:src.PAYACCOUNT
       ,DST.PAYACCOUNTBANK                                     --工资帐号开户银行:src.PAYACCOUNTBANK
       ,DST.FAMILYSTATUS                                       --居住状况:src.FAMILYSTATUS
       ,DST.TEMPSAVEFLAG                                       --暂存标志:src.TEMPSAVEFLAG
       ,DST.CERTID18                                           --真实18位身份证号码:src.CERTID18
       ,DST.ECONOMYDCOID                                       --经济档案编号:src.ECONOMYDCOID
       ,DST.MYBALANCEACCOUNT                                   --本社结算账号:src.MYBALANCEACCOUNT
       ,DST.HEALTH                                             --健康状况:src.HEALTH
       ,DST.HABIT                                              --有无不良嗜好:src.HABIT
       ,DST.SPECIALITY                                         --有无技术特长:src.SPECIALITY
       ,DST.MYBANKDORM                                         --是否本社股东:src.MYBANKDORM
       ,DST.ASSOCIATOR                                         --是否社员:src.ASSOCIATOR
       ,DST.ISBUSINESSMAN                                      --是否个体工商户:src.ISBUSINESSMAN
       ,DST.TEAMNO                                             --小组编号:src.TEAMNO
       ,DST.HOUSEMASTERNAME                                    --户主姓名:src.HOUSEMASTERNAME
       ,DST.FAMILYNUM                                          --家庭劳动力:src.FAMILYNUM
       ,DST.FAMILYYEARINCOME                                   --家庭年纯收入:src.FAMILYYEARINCOME
       ,DST.TEAMNAME                                           --所属行政乡_镇_村名:src.TEAMNAME
       ,DST.FARMERMAININDUS                                    --农户主导产业:src.FARMERMAININDUS
       ,DST.LOANFLAG                                           --贷款卡是否有效标志:src.LOANFLAG
       ,DST.VILLAGECADRE                                       --是否村组干部:src.VILLAGECADRE
       ,DST.MEDICARE                                           --是否参加农村新型合作医疗保险:src.MEDICARE
       ,DST.POORISNO                                           --是否扶贫户:src.POORISNO
       ,DST.POORNO                                             --扶贫证件号:src.POORNO
       ,DST.MARKUP                                             --经营模式:src.MARKUP
       ,DST.MANAGETYPE                                         --管理方式:src.MANAGETYPE
       ,DST.MANAGEPLACE                                        --经营场所:src.MANAGEPLACE
       ,DST.LICENSENO                                          --营业执照号码:src.LICENSENO
       ,DST.LICENSEINPUTDATE                                   --营业执照登记时间:src.LICENSEINPUTDATE
       ,DST.TAXNO                                              --税务登记证号码_国税:src.TAXNO
       ,DST.EVALUATEOVERDATE                                   --信用等级有效日到期日:src.EVALUATEOVERDATE
       ,DST.BEHAVE                                             --社会表现:src.BEHAVE
       ,DST.MAINPROORINCOME                                    --主要经营项目及收入来源:src.MAINPROORINCOME
       ,DST.WEALTHNAME                                         --家庭财产名称:src.WEALTHNAME
       ,DST.WEALTHVALUE                                        --家庭财产估价:src.WEALTHVALUE
       ,DST.SAFEBALANCE                                        --授信安全控制量:src.SAFEBALANCE
       ,DST.CREDITRECORD                                       --信用记录:src.CREDITRECORD
       ,DST.ACCOUNTID                                          --本行/社存款账户:src.ACCOUNTID
       ,DST.FAMILYYEARIN                                       --家庭年度纯收入:src.FAMILYYEARIN
       ,DST.SYMBIOSIS                                          --与本行、社合作关系:src.SYMBIOSIS
       ,DST.FARMERCARD                                         --农户易贷通卡号:src.FARMERCARD
       ,DST.CONLANDAREA                                        --承包土地面积:src.CONLANDAREA
       ,DST.CONPOOLAREA                                        --承包水塘面积:src.CONPOOLAREA
       ,DST.MANAGEUSERID                                       --管户用户ID:src.MANAGEUSERID
       ,DST.MANAGEORGID                                        --管户机构ID:src.MANAGEORGID
       ,DST.FAMILYAMOUNT                                       --家庭人口:src.FAMILYAMOUNT
       ,DST.CORPORATEORGID                                     --法人机构号:src.CORPORATEORGID
       ,DST.CREDITBELONG                                       --信用等级评级模板名称:src.CREDITBELONG
       ,DST.ISREALTIVEMAN                                      --是否关键人:src.ISREALTIVEMAN
       ,DST.OCCUPATION1                                        --职业_人行:src.OCCUPATION1
       ,DST.LOCALYEAR                                          --入住本地时间:src.LOCALYEAR
       ,DST.REMARK1                                            --备注1:src.REMARK1
       ,DST.REMARK2                                            --备注2:src.REMARK2
       ,DST.REMARK3                                            --备注3:src.REMARK3
       ,DST.REMARK4                                            --备注4:src.REMARK4
       ,DST.CENSUSBOOKID                                       --户口簿编号:src.CENSUSBOOKID
       ,DST.HOUSEMASTERCERTTYPE                                --户主证件类型:src.HOUSEMASTERCERTTYPE
       ,DST.HOUSEMASTERCERTID                                  --户主证件号码:src.HOUSEMASTERCERTID
       ,DST.CUSTOMERKEYTYPE                                    --客户分类标记:src.CUSTOMERKEYTYPE
       ,DST.VILLAGENO                                          --行政村代码:src.VILLAGENO
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
   FROM F_CI_XDXT_IND_INFO DST 
   LEFT JOIN F_CI_XDXT_IND_INFO_INNTMP1 SRC 
     ON SRC.CUSTOMERID          = DST.CUSTOMERID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUSTOMERID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_IND_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_XDXT_IND_INFO/"+V_DT+".parquet"
F_CI_XDXT_IND_INFO_INNTMP2=F_CI_XDXT_IND_INFO_INNTMP2.unionAll(F_CI_XDXT_IND_INFO_INNTMP1)
F_CI_XDXT_IND_INFO_INNTMP1.cache()
F_CI_XDXT_IND_INFO_INNTMP2.cache()
nrowsi = F_CI_XDXT_IND_INFO_INNTMP1.count()
nrowsa = F_CI_XDXT_IND_INFO_INNTMP2.count()
F_CI_XDXT_IND_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_IND_INFO_INNTMP1.unpersist()
F_CI_XDXT_IND_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_IND_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_XDXT_IND_INFO/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_IND_INFO_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_IND_INFO_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_IND_INFO/"+V_DT+".parquet /"+dbname+"/F_CI_XDXT_IND_INFO_BK/"+V_DT+".parquet")
