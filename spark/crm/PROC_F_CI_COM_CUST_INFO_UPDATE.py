#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_COM_CUST_INFO_UPDATE').setMaster(sys.argv[2])
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

#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
ACRM_F_AG_AGREEMENT = sqlContext.read.parquet(hdfs+'/ACRM_F_AG_AGREEMENT/*')
ACRM_F_AG_AGREEMENT.registerTempTable("ACRM_F_AG_AGREEMENT")

#目标表：
#OCRM_F_CI_COM_CUST_INFO 增改表 多个PY 非第一个
#处理过程：先删除运算目录所有数据文件，然后复制BK目录当天数据到运算目录，作为今天数据文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/*")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet")
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.COM_BELONG            AS COM_BELONG 
       ,A.HOLDING_TYP           AS HOLDING_TYP 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.OBJECT_RATE           AS OBJECT_RATE 
       ,A.SUBJECT_RATE          AS SUBJECT_RATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.RATE_DATE             AS RATE_DATE 
       ,A.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,A.IF_SMALL              AS IF_SMALL 
       ,A.IF_IBK                AS IF_IBK 
       ,A.PLICY_TYP             AS PLICY_TYP 
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,A.AREA_ID               AS AREA_ID 
       ,A.UNION_FLAG            AS UNION_FLAG 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.AUTH_ORG              AS AUTH_ORG 
       ,B.ORG_NO                AS OPEN_ORG1 
       ,B.TYPE                  AS FIRST_OPEN_TYP 
       ,A.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LOAN_CARDNO           AS LOAN_CARDNO 
       ,A.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,B.FIRST_OPEN_DATE         AS FIRST_OPEN_DATE 
       ,A.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,A.LOAN_RATE             AS LOAN_RATE 
       ,A.DEP_RATE              AS DEP_RATE 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.SETTLE_RATIO          AS SETTLE_RATIO 
       ,A.BASIC_ACCT            AS BASIC_ACCT 
       ,A.LEGAL_NAME            AS LEGAL_NAME 
       ,A.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,A.LINK_MOBILE           AS LINK_MOBILE 
       ,A.FAX_NO                AS FAX_NO 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.REGISTER_ZIP          AS REGISTER_ZIP 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.E_MAIL                AS E_MAIL 
       ,A.WEB_ADDRESS           AS WEB_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,A.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.OPEN_DATE             AS OPEN_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.LAST_DATE             AS LAST_DATE 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_OWNER             AS BUS_OWNER 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,A.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,A.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,A.MERGE_ORG             AS MERGE_ORG 
       ,A.MERGE_DATE            AS MERGE_DATE 
       ,A.MERGE_OFFICER         AS MERGE_OFFICER 
       ,A.REMARK1               AS REMARK1 
       ,A.BIRTH_DATE            AS BIRTH_DATE 
       ,A.KEY_CERT_NO           AS KEY_CERT_NO 
       ,A.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,A.KEY_CUST_ID           AS KEY_CUST_ID 
       ,A.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,A.EDU_LEVEL             AS EDU_LEVEL 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,A.HOUSE_ZIP             AS HOUSE_ZIP 
       ,A.DUTY_TIME             AS DUTY_TIME 
       ,A.SHARE_HOLDING         AS SHARE_HOLDING 
       ,A.DUTY                  AS DUTY 
       ,A.REMARK2               AS REMARK2 
       ,A.SEX                   AS SEX 
       ,A.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.GREEN_FLAG            AS GREEN_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO A                              
  INNER JOIN (SELECT CUST_ID,FR_ID,ORG_NO,TYPE,START_DATE AS FIRST_OPEN_DATE,
                     ROW_NUMBER() OVER(PARTITION BY CUST_ID,FR_ID ORDER BY START_DATE ASC) AS RANK
                FROM ACRM_F_AG_AGREEMENT
                WHERE START_DATE IS NOT NULL ) B                             
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.RANK = '1'
  WHERE A.OPEN_ORG1 IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --:src.LONGITUDE
       ,DST.LATITUDE                                           --:src.LATITUDE
       ,DST.GPS                                                --:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
#增改表保存后，复制当天数据进BK：先删除BK当天日期，然后复制
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-02::
V_STEP = V_STEP + 1
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_NAME             AS CUST_NAME 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.COM_BELONG            AS COM_BELONG 
       ,A.HOLDING_TYP           AS HOLDING_TYP 
       ,A.INDUS_CALSS_MAIN      AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.OBJECT_RATE           AS OBJECT_RATE 
       ,A.SUBJECT_RATE          AS SUBJECT_RATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.RATE_DATE             AS RATE_DATE 
       ,A.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,A.IF_SMALL              AS IF_SMALL 
       ,A.IF_IBK                AS IF_IBK 
       ,A.PLICY_TYP             AS PLICY_TYP 
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,A.IF_INT                AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,A.AREA_ID               AS AREA_ID 
       ,A.UNION_FLAG            AS UNION_FLAG 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.AUTH_ORG              AS AUTH_ORG 
       ,A.OPEN_ORG1             AS OPEN_ORG1 
       ,A.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,A.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LOAN_CARDNO           AS LOAN_CARDNO 
       ,A.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,A.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,B.BEGIN_DATE            AS FIRST_LOAN_DATE 
       ,A.LOAN_RATE             AS LOAN_RATE 
       ,A.DEP_RATE              AS DEP_RATE 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.SETTLE_RATIO          AS SETTLE_RATIO 
       ,A.BASIC_ACCT            AS BASIC_ACCT 
       ,A.LEGAL_NAME            AS LEGAL_NAME 
       ,A.LEGAL_CERT_NO         AS LEGAL_CERT_NO 
       ,A.LINK_MOBILE           AS LINK_MOBILE 
       ,A.FAX_NO                AS FAX_NO 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.REGISTER_ZIP          AS REGISTER_ZIP 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.E_MAIL                AS E_MAIL 
       ,A.WEB_ADDRESS           AS WEB_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,A.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.OPEN_DATE             AS OPEN_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,A.REG_CAPITAL           AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.LAST_DATE             AS LAST_DATE 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_OWNER             AS BUS_OWNER 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,A.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,A.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,A.MERGE_ORG             AS MERGE_ORG 
       ,A.MERGE_DATE            AS MERGE_DATE 
       ,A.MERGE_OFFICER         AS MERGE_OFFICER 
       ,A.REMARK1               AS REMARK1 
       ,A.BIRTH_DATE            AS BIRTH_DATE 
       ,A.KEY_CERT_NO           AS KEY_CERT_NO 
       ,A.KEY_CERT_TYP          AS KEY_CERT_TYP 
       ,A.KEY_CUST_ID           AS KEY_CUST_ID 
       ,A.KEY_PEOPLE_NAME       AS KEY_PEOPLE_NAME 
       ,A.EDU_LEVEL             AS EDU_LEVEL 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,A.HOUSE_ZIP             AS HOUSE_ZIP 
       ,A.DUTY_TIME             AS DUTY_TIME 
       ,A.SHARE_HOLDING         AS SHARE_HOLDING 
       ,A.DUTY                  AS DUTY 
       ,A.REMARK2               AS REMARK2 
       ,A.SEX                   AS SEX 
       ,A.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.GREEN_FLAG            AS GREEN_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --
  INNER JOIN (SELECT FR_ID,CUST_ID,MIN(BEGIN_DATE) AS BEGIN_DATE 
                FROM ACRM_F_CI_ASSET_BUSI_PROTO
               GROUP BY FR_ID,CUST_ID ) B                      --
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.FIRST_LOAN_DATE IS NULL  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")


sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --:src.LONGITUDE
       ,DST.LATITUDE                                           --:src.LATITUDE
       ,DST.GPS                                                --:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.CUST_ID             = DST.CUST_ID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
#增改表保存后，复制当天数据进BK：先删除BK当天日期，然后复制
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_COM_CUST_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
