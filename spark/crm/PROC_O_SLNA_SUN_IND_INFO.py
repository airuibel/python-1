#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_SLNA_SUN_IND_INFO').setMaster(sys.argv[2])
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

O_CI_SUN_IND_INFO = sqlContext.read.parquet(hdfs+'/O_CI_SUN_IND_INFO/*')
O_CI_SUN_IND_INFO.registerTempTable("O_CI_SUN_IND_INFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_SUN_IND_INFO/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_SUN_IND_INFO_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_SUN_IND_INFO/"+V_DT+".parquet")


F_CI_SUN_IND_INFO = sqlContext.read.parquet(hdfs+'/F_CI_SUN_IND_INFO/*')
F_CI_SUN_IND_INFO.registerTempTable("F_CI_SUN_IND_INFO")

sql = """
 SELECT A.CUSTOMERID            AS CUSTOMERID 
       ,A.FULLNAME              AS FULLNAME 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CERTID                AS CERTID 
       ,A.SEX                   AS SEX 
       ,A.BIRTHDAY              AS BIRTHDAY 
       ,A.NATIVEPLACE           AS NATIVEPLACE 
       ,A.MYBANKDORM            AS MYBANKDORM 
       ,A.STAFF                 AS STAFF 
       ,A.ISREALTIVEMAN         AS ISREALTIVEMAN 
       ,A.NATIONALITY           AS NATIONALITY 
       ,A.POLITICALFACE         AS POLITICALFACE 
       ,A.MARRIAGE              AS MARRIAGE 
       ,A.HEALTH                AS HEALTH 
       ,A.EDUEXPERIENCE         AS EDUEXPERIENCE 
       ,A.EDUDEGREE             AS EDUDEGREE 
       ,A.WORKCORP              AS WORKCORP 
       ,A.HEADSHIP              AS HEADSHIP 
       ,A.POSITION              AS POSITION 
       ,A.FAMILYAMOUNT          AS FAMILYAMOUNT 
       ,A.TEAMNAME              AS TEAMNAME 
       ,A.REMARK2               AS REMARK2 
       ,A.REMARK3               AS REMARK3 
       ,A.CREDITFARMER          AS CREDITFARMER 
       ,A.REMARK4               AS REMARK4 
       ,A.FAMILYSTATUS          AS FAMILYSTATUS 
       ,A.FAMILYADD             AS FAMILYADD 
       ,A.FAMILYZIP             AS FAMILYZIP 
       ,A.FAMILYTEL             AS FAMILYTEL 
       ,A.MOBILETELEPHONE       AS MOBILETELEPHONE 
       ,A.EMAILADD              AS EMAILADD 
       ,A.OCCUPATION            AS OCCUPATION 
       ,A.DELAYCREDIT           AS DELAYCREDIT 
       ,A.DELAYCREDITREASON     AS DELAYCREDITREASON 
       ,A.FARMERCARD            AS FARMERCARD 
       ,A.MYBALANCEACCOUNT      AS MYBALANCEACCOUNT 
       ,A.MARKUP                AS MARKUP 
       ,A.FAMILYMONTHINCOME     AS FAMILYMONTHINCOME 
       ,A.MAINPROORINCOME       AS MAINPROORINCOME 
       ,A.CREDITLEVEL           AS CREDITLEVEL 
       ,A.EVALUATEDATE          AS EVALUATEDATE 
       ,A.REMARK                AS REMARK 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.UPDATEUSERID          AS UPDATEUSERID 
       ,A.UPDATEORGID           AS UPDATEORGID 
       ,A.FARMILYID             AS FARMILYID 
       ,A.FAMILYROLE            AS FAMILYROLE 
       ,A.ISHZ                  AS ISHZ 
       ,A.WORKINGCAPITAL        AS WORKINGCAPITAL 
       ,A.CAPITALASSETS         AS CAPITALASSETS 
       ,A.FAMILYAVERAGEINCOME   AS FAMILYAVERAGEINCOME 
       ,A.FAMILYALLINCOME       AS FAMILYALLINCOME 
       ,A.FAMILYALLOUT          AS FAMILYALLOUT 
       ,A.FAMILYPUREINCOME      AS FAMILYPUREINCOME 
       ,A.TOTALASSETS           AS TOTALASSETS 
       ,A.TOTALINDEBTEDNESS     AS TOTALINDEBTEDNESS 
       ,A.FAMILYPUREASSET       AS FAMILYPUREASSET 
       ,A.LANDSIZE              AS LANDSIZE 
       ,A.LANDNO                AS LANDNO 
       ,A.YEAROUTCOME           AS YEAROUTCOME 
       ,A.BUSINESSADDRESS       AS BUSINESSADDRESS 
       ,A.ALLGUARANTYADDRESS    AS ALLGUARANTYADDRESS 
       ,A.ALLGUARANTYTEL        AS ALLGUARANTYTEL 
       ,A.CREDITDATE            AS CREDITDATE 
       ,A.INFRINGEMENTTIMES     AS INFRINGEMENTTIMES 
       ,A.AVERAGEDEPOSIT        AS AVERAGEDEPOSIT 
       ,A.PROJECTNO             AS PROJECTNO 
       ,A.MAINPROSCOPE          AS MAINPROSCOPE 
       ,A.MANAGEUSERID          AS MANAGEUSERID 
       ,A.MANAGEORGID           AS MANAGEORGID 
       ,A.ORDERDEPOSIT          AS ORDERDEPOSIT 
       ,A.MHOUSESTRUCTURE       AS MHOUSESTRUCTURE 
       ,A.MHOUSENO              AS MHOUSENO 
       ,A.ACTUALEVALUATE        AS ACTUALEVALUATE 
       ,A.OHOUSESTRUCTURE       AS OHOUSESTRUCTURE 
       ,A.OHOUSENO              AS OHOUSENO 
       ,A.OACTUALEVALUATE       AS OACTUALEVALUATE 
       ,A.MACHINENAME           AS MACHINENAME 
       ,A.MACHINEVALUE          AS MACHINEVALUE 
       ,A.OTHERASSET            AS OTHERASSET 
       ,A.HOUSEAREANAME         AS HOUSEAREANAME 
       ,A.HOUSEID               AS HOUSEID 
       ,A.HOUSEAREANO           AS HOUSEAREANO 
       ,A.CUSTOMERTYPE          AS CUSTOMERTYPE 
       ,A.YEARLNCOME            AS YEARLNCOME 
       ,A.CORPORATEORGID        AS CORPORATEORGID 
       ,A.TEMPSAVEFLAG          AS TEMPSAVEFLAG 
       ,A.TEAMNO                AS TEAMNO 
       ,A.VILLAGENO             AS VILLAGENO 
       ,A.LOCKORNOT             AS LOCKORNOT 
       ,A.ISUSINGCREDIT         AS ISUSINGCREDIT 
       ,A.XDCUSTOMERID          AS XDCUSTOMERID 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'SLNA'                  AS ODS_SYS_ID 
   FROM O_CI_SUN_IND_INFO A                                    --阳光信贷农户表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_SUN_IND_INFO_INNTMP1 = sqlContext.sql(sql)
F_CI_SUN_IND_INFO_INNTMP1.registerTempTable("F_CI_SUN_IND_INFO_INNTMP1")

#F_CI_SUN_IND_INFO = sqlContext.read.parquet(hdfs+'/F_CI_SUN_IND_INFO/*')
#F_CI_SUN_IND_INFO.registerTempTable("F_CI_SUN_IND_INFO")
sql = """
 SELECT DST.CUSTOMERID                                          --客户编号:src.CUSTOMERID
       ,DST.FULLNAME                                           --姓名:src.FULLNAME
       ,DST.CERTTYPE                                           --证件类型:src.CERTTYPE
       ,DST.CERTID                                             --证件号码:src.CERTID
       ,DST.SEX                                                --性别:src.SEX
       ,DST.BIRTHDAY                                           --出生日期:src.BIRTHDAY
       ,DST.NATIVEPLACE                                        --户籍地址:src.NATIVEPLACE
       ,DST.MYBANKDORM                                         --是否本行/社股东:src.MYBANKDORM
       ,DST.STAFF                                              --是否本行/社员工:src.STAFF
       ,DST.ISREALTIVEMAN                                      --是否本行/社其他关系人:src.ISREALTIVEMAN
       ,DST.NATIONALITY                                        --民族:src.NATIONALITY
       ,DST.POLITICALFACE                                      --政治面貌:src.POLITICALFACE
       ,DST.MARRIAGE                                           --婚姻状况:src.MARRIAGE
       ,DST.HEALTH                                             --健康状况:src.HEALTH
       ,DST.EDUEXPERIENCE                                      --最高学历:src.EDUEXPERIENCE
       ,DST.EDUDEGREE                                          --最高学位:src.EDUDEGREE
       ,DST.WORKCORP                                           --工作单位名称:src.WORKCORP
       ,DST.HEADSHIP                                           --职务:src.HEADSHIP
       ,DST.POSITION                                           --职称:src.POSITION
       ,DST.FAMILYAMOUNT                                       --家庭人口数:src.FAMILYAMOUNT
       ,DST.TEAMNAME                                           --所属行政乡(镇):src.TEAMNAME
       ,DST.REMARK2                                            --是否信用乡(镇):src.REMARK2
       ,DST.REMARK3                                            --所属行政村名称:src.REMARK3
       ,DST.CREDITFARMER                                       --是否信用村:src.CREDITFARMER
       ,DST.REMARK4                                            --是否信用户:src.REMARK4
       ,DST.FAMILYSTATUS                                       --居住状况:src.FAMILYSTATUS
       ,DST.FAMILYADD                                          --居住地址:src.FAMILYADD
       ,DST.FAMILYZIP                                          --居住地址邮编:src.FAMILYZIP
       ,DST.FAMILYTEL                                          --住宅电话:src.FAMILYTEL
       ,DST.MOBILETELEPHONE                                    --联系号码（短信提醒）:src.MOBILETELEPHONE
       ,DST.EMAILADD                                           --电子邮箱:src.EMAILADD
       ,DST.OCCUPATION                                         --职业（国标）:src.OCCUPATION
       ,DST.DELAYCREDIT                                        --是否暂缓授信:src.DELAYCREDIT
       ,DST.DELAYCREDITREASON                                  --暂缓授信原因:src.DELAYCREDITREASON
       ,DST.FARMERCARD                                         --易贷通卡号:src.FARMERCARD
       ,DST.MYBALANCEACCOUNT                                   --本行/社结算账户:src.MYBALANCEACCOUNT
       ,DST.MARKUP                                             --经营模式:src.MARKUP
       ,DST.FAMILYMONTHINCOME                                  --家庭月收入:src.FAMILYMONTHINCOME
       ,DST.MAINPROORINCOME                                    --主要经营项目及收入来源:src.MAINPROORINCOME
       ,DST.CREDITLEVEL                                        --即期信用等级:src.CREDITLEVEL
       ,DST.EVALUATEDATE                                       --即期评级时间:src.EVALUATEDATE
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTDATE                                          --登记日期:src.INPUTDATE
       ,DST.UPDATEUSERID                                       --更新人员:src.UPDATEUSERID
       ,DST.UPDATEORGID                                        --更新机构:src.UPDATEORGID
       ,DST.FARMILYID                                          --户籍编号(户号代码):src.FARMILYID
       ,DST.FAMILYROLE                                         --家庭角色:src.FAMILYROLE
       ,DST.ISHZ                                               --是否为户主:src.ISHZ
       ,DST.WORKINGCAPITAL                                     --流动资金:src.WORKINGCAPITAL
       ,DST.CAPITALASSETS                                      --固定资产:src.CAPITALASSETS
       ,DST.FAMILYAVERAGEINCOME                                --家庭人均收入:src.FAMILYAVERAGEINCOME
       ,DST.FAMILYALLINCOME                                    --家庭年收入:src.FAMILYALLINCOME
       ,DST.FAMILYALLOUT                                       --家庭总支出:src.FAMILYALLOUT
       ,DST.FAMILYPUREINCOME                                   --家庭净收入:src.FAMILYPUREINCOME
       ,DST.TOTALASSETS                                        --家庭总资产:src.TOTALASSETS
       ,DST.TOTALINDEBTEDNESS                                  --家庭总负债:src.TOTALINDEBTEDNESS
       ,DST.FAMILYPUREASSET                                    --家庭净资产:src.FAMILYPUREASSET
       ,DST.LANDSIZE                                           --承包土地面积:src.LANDSIZE
       ,DST.LANDNO                                             --地址码:src.LANDNO
       ,DST.YEAROUTCOME                                        --年支出:src.YEAROUTCOME
       ,DST.BUSINESSADDRESS                                    --经营地址:src.BUSINESSADDRESS
       ,DST.ALLGUARANTYADDRESS                                 --所有抵质押、担保住址:src.ALLGUARANTYADDRESS
       ,DST.ALLGUARANTYTEL                                     --所有抵质押、担保联系号码:src.ALLGUARANTYTEL
       ,DST.CREDITDATE                                         --与我行首次建立信贷关系时间:src.CREDITDATE
       ,DST.INFRINGEMENTTIMES                                  --违约次数:src.INFRINGEMENTTIMES
       ,DST.AVERAGEDEPOSIT                                     --一年日均存款金额:src.AVERAGEDEPOSIT
       ,DST.PROJECTNO                                          --经营项目编号:src.PROJECTNO
       ,DST.MAINPROSCOPE                                       --经营规模:src.MAINPROSCOPE
       ,DST.MANAGEUSERID                                       --管户人:src.MANAGEUSERID
       ,DST.MANAGEORGID                                        --管户机构:src.MANAGEORGID
       ,DST.ORDERDEPOSIT                                       --预约存款:src.ORDERDEPOSIT
       ,DST.MHOUSESTRUCTURE                                    --主屋结构:src.MHOUSESTRUCTURE
       ,DST.MHOUSENO                                           --主屋间数:src.MHOUSENO
       ,DST.ACTUALEVALUATE                                     --实际估价:src.ACTUALEVALUATE
       ,DST.OHOUSESTRUCTURE                                    --边屋结构:src.OHOUSESTRUCTURE
       ,DST.OHOUSENO                                           --边屋间数:src.OHOUSENO
       ,DST.OACTUALEVALUATE                                    --边屋估价:src.OACTUALEVALUATE
       ,DST.MACHINENAME                                        --机械名称:src.MACHINENAME
       ,DST.MACHINEVALUE                                       --机械价值:src.MACHINEVALUE
       ,DST.OTHERASSET                                         --其他资产:src.OTHERASSET
       ,DST.HOUSEAREANAME                                      --房屋小区名:src.HOUSEAREANAME
       ,DST.HOUSEID                                            --房号:src.HOUSEID
       ,DST.HOUSEAREANO                                        --面积:src.HOUSEAREANO
       ,DST.CUSTOMERTYPE                                       --客户类型:src.CUSTOMERTYPE
       ,DST.YEARLNCOME                                         --家庭年收入:src.YEARLNCOME
       ,DST.CORPORATEORGID                                     --法人机构号:src.CORPORATEORGID
       ,DST.TEMPSAVEFLAG                                       --是否暂存:src.TEMPSAVEFLAG
       ,DST.TEAMNO                                             --所在乡镇代码:src.TEAMNO
       ,DST.VILLAGENO                                          --所在村编号:src.VILLAGENO
       ,DST.LOCKORNOT                                          --是否锁定:src.LOCKORNOT
       ,DST.ISUSINGCREDIT                                      --是否用信客户:src.ISUSINGCREDIT
       ,DST.XDCUSTOMERID                                       --信贷系统客户编号:src.XDCUSTOMERID
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_CI_SUN_IND_INFO DST 
   LEFT JOIN F_CI_SUN_IND_INFO_INNTMP1 SRC 
     ON SRC.CUSTOMERID          = DST.CUSTOMERID 
  WHERE SRC.CUSTOMERID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_SUN_IND_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_SUN_IND_INFO/"+V_DT+".parquet"
F_CI_SUN_IND_INFO_INNTMP2=F_CI_SUN_IND_INFO_INNTMP2.unionAll(F_CI_SUN_IND_INFO_INNTMP1)
F_CI_SUN_IND_INFO_INNTMP1.cache()
F_CI_SUN_IND_INFO_INNTMP2.cache()
nrowsi = F_CI_SUN_IND_INFO_INNTMP1.count()
nrowsa = F_CI_SUN_IND_INFO_INNTMP2.count()
F_CI_SUN_IND_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_SUN_IND_INFO_INNTMP1.unpersist()
F_CI_SUN_IND_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_SUN_IND_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_SUN_IND_INFO/"+V_DT_LD+".parquet /"+dbname+"/F_CI_SUN_IND_INFO_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_SUN_IND_INFO_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_SUN_IND_INFO/"+V_DT+".parquet /"+dbname+"/F_CI_SUN_IND_INFO_BK/"+V_DT+".parquet")
