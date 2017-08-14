#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CRE_CUSTR').setMaster(sys.argv[2])
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

#----------来源表---------------
O_CI_CUSTR = sqlContext.read.parquet(hdfs+'/O_CI_CUSTR/*')
O_CI_CUSTR.registerTempTable("O_CI_CUSTR")

#任务[12] 001-01::
V_STEP = V_STEP + 1

#----------目标表---------------
F_CI_CUSTR = sqlContext.read.parquet(hdfs+'/F_CI_CUSTR_BK/'+V_DT_LD+'.parquet/*')
F_CI_CUSTR.registerTempTable("F_CI_CUSTR")

sql = """
 SELECT CUSTR_NBR               AS CUSTR_NBR 
       ,SECUR_NBR               AS SECUR_NBR 
       ,NATION                  AS NATION 
       ,BUSI_PHONE              AS BUSI_PHONE 
       ,CAR_DATE                AS CAR_DATE 
       ,CAR_ID                  AS CAR_ID 
       ,CAR_NAM                 AS CAR_NAM 
       ,CAR_CODE                AS CAR_CODE 
       ,CLASS_CODE              AS CLASS_CODE 
       ,COMP_NAME               AS COMP_NAME 
       ,CON_NAM1                AS CON_NAM1 
       ,CON_NAM2                AS CON_NAM2 
       ,CON_TEL1                AS CON_TEL1 
       ,CON_TEL2                AS CON_TEL2 
       ,CONTR_NAM1              AS CONTR_NAM1 
       ,CONTR_NAM2              AS CONTR_NAM2 
       ,CONTR_NAM3              AS CONTR_NAM3 
       ,CONTR_NAM4              AS CONTR_NAM4 
       ,CONTR_NAM5              AS CONTR_NAM5 
       ,CONTR_TEL1              AS CONTR_TEL1 
       ,CONTR_TEL2              AS CONTR_TEL2 
       ,CRED_LIMIT              AS CRED_LIMIT 
       ,CREDLIM_X               AS CREDLIM_X 
       ,DAY_BIRTH               AS DAY_BIRTH 
       ,DEPENDENTS              AS DEPENDENTS 
       ,EDUCA                   AS EDUCA 
       ,EMAIL_ADDR              AS EMAIL_ADDR 
       ,EMPLY_DEPT              AS EMPLY_DEPT 
       ,EMPLY_NBR               AS EMPLY_NBR 
       ,EXTENSION               AS EXTENSION 
       ,GENDER                  AS GENDER 
       ,HOME_CODE               AS HOME_CODE 
       ,HOME_PHONE              AS HOME_PHONE 
       ,INCOME_AN2              AS INCOME_AN2 
       ,INCOME_ANN              AS INCOME_ANN 
       ,INCOME_SR2              AS INCOME_SR2 
       ,INCOME_SRC              AS INCOME_SRC 
       ,IRD_NUMBER              AS IRD_NUMBER 
       ,LANG_CODE               AS LANG_CODE 
       ,MAIL_CODE               AS MAIL_CODE 
       ,MAR_STATUS              AS MAR_STATUS 
       ,MO_PHONE                AS MO_PHONE 
       ,MP_LIMIT                AS MP_LIMIT 
       ,MTHR_MNAME              AS MTHR_MNAME 
       ,OCC_CATGRY              AS OCC_CATGRY 
       ,OCC_CODE                AS OCC_CODE 
       ,POSN_EMPLY              AS POSN_EMPLY 
       ,RACE_CODE               AS RACE_CODE 
       ,REL_NAM                 AS REL_NAM 
       ,REL_TEL                 AS REL_TEL 
       ,FORECOMP                AS FORECOMP 
       ,FOREDEPT                AS FOREDEPT 
       ,FOREJOB                 AS FOREJOB 
       ,FOREBUSI                AS FOREBUSI 
       ,FOREANN                 AS FOREANN 
       ,YR_FORECOM              AS YR_FORECOM 
       ,SPU_NAM                 AS SPU_NAM 
       ,SPU_TEL                 AS SPU_TEL 
       ,SURNAME                 AS SURNAME 
       ,XTITLE                  AS XTITLE 
       ,WORK_CALLS              AS WORK_CALLS 
       ,YR_IN_COM2              AS YR_IN_COM2 
       ,YR_IN_COMP              AS YR_IN_COMP 
       ,YR_THERE                AS YR_THERE 
       ,SPU_MOBILE              AS SPU_MOBILE 
       ,HOME_LOAN               AS HOME_LOAN 
       ,CON_MO1                 AS CON_MO1 
       ,CON_MO2                 AS CON_MO2 
       ,REL_MOBILE              AS REL_MOBILE 
       ,INT_TAXCOD              AS INT_TAXCOD 
       ,ID_VERIFY               AS ID_VERIFY 
       ,IDCP_YN                 AS IDCP_YN 
       ,IV_RST                  AS IV_RST 
       ,IV_RSN                  AS IV_RSN 
       ,IV_DISPO                AS IV_DISPO 
       ,ID_EXPDT                AS ID_EXPDT 
       ,QUESTION                AS QUESTION 
       ,ANSWER                  AS ANSWER 
       ,OCT_SCORE               AS OCT_SCORE 
       ,NATION_CD               AS NATION_CD 
       ,ORG_NO                  AS ORG_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CRE'                   AS ODS_SYS_ID 
   FROM O_CI_CUSTR A                                           --客户资料
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CUSTR_INNTMP1 = sqlContext.sql(sql)
F_CI_CUSTR_INNTMP1.registerTempTable("F_CI_CUSTR_INNTMP1")

#F_CI_CUSTR = sqlContext.read.parquet(hdfs+'/F_CI_CUSTR/*')
#F_CI_CUSTR.registerTempTable("F_CI_CUSTR")
sql = """
 SELECT DST.CUSTR_NBR                                           --客户证件号码:src.CUSTR_NBR
       ,DST.SECUR_NBR                                          --社保卡号:src.SECUR_NBR
       ,DST.NATION                                             --国籍:src.NATION
       ,DST.BUSI_PHONE                                         --单位电话号码:src.BUSI_PHONE
       ,DST.CAR_DATE                                           --自购车辆购买时间:src.CAR_DATE
       ,DST.CAR_ID                                             --自购车辆车牌号:src.CAR_ID
       ,DST.CAR_NAM                                            --自购车辆品牌:src.CAR_NAM
       ,DST.CAR_CODE                                           --自购车辆情况:src.CAR_CODE
       ,DST.CLASS_CODE                                         --客户分类:src.CLASS_CODE
       ,DST.COMP_NAME                                          --单位名称:src.COMP_NAME
       ,DST.CON_NAM1                                           --联系人1姓名:src.CON_NAM1
       ,DST.CON_NAM2                                           --联系人2姓名:src.CON_NAM2
       ,DST.CON_TEL1                                           --联系人1电话号码:src.CON_TEL1
       ,DST.CON_TEL2                                           --联系人2电话号码:src.CON_TEL2
       ,DST.CONTR_NAM1                                         --联系人1单位名称:src.CONTR_NAM1
       ,DST.CONTR_NAM2                                         --联系人1关系:src.CONTR_NAM2
       ,DST.CONTR_NAM3                                         --联系人2单位名称:src.CONTR_NAM3
       ,DST.CONTR_NAM4                                         --联系人2关系:src.CONTR_NAM4
       ,DST.CONTR_NAM5                                         --直系亲属关系:src.CONTR_NAM5
       ,DST.CONTR_TEL1                                         --配偶单位名称:src.CONTR_TEL1
       ,DST.CONTR_TEL2                                         --配偶证件号码:src.CONTR_TEL2
       ,DST.CRED_LIMIT                                         --信用额度:src.CRED_LIMIT
       ,DST.CREDLIM_X                                          --美元帐户额度:src.CREDLIM_X
       ,DST.DAY_BIRTH                                          --出生日期:src.DAY_BIRTH
       ,DST.DEPENDENTS                                         --抚养人数:src.DEPENDENTS
       ,DST.EDUCA                                              --教育程度:src.EDUCA
       ,DST.EMAIL_ADDR                                         --电子邮件地址:src.EMAIL_ADDR
       ,DST.EMPLY_DEPT                                         --部门:src.EMPLY_DEPT
       ,DST.EMPLY_NBR                                          --员工编号:src.EMPLY_NBR
       ,DST.EXTENSION                                          --分机:src.EXTENSION
       ,DST.GENDER                                             --性别:src.GENDER
       ,DST.HOME_CODE                                          --住房情况:src.HOME_CODE
       ,DST.HOME_PHONE                                         --住宅电话号码:src.HOME_PHONE
       ,DST.INCOME_AN2                                         --配偶税前年收入:src.INCOME_AN2
       ,DST.INCOME_ANN                                         --个人税前年收入:src.INCOME_ANN
       ,DST.INCOME_SR2                                         --配偶主要收入来源:src.INCOME_SR2
       ,DST.INCOME_SRC                                         --个人主要收入来源:src.INCOME_SRC
       ,DST.IRD_NUMBER                                         --传真号码:src.IRD_NUMBER
       ,DST.LANG_CODE                                          --语言代码:src.LANG_CODE
       ,DST.MAIL_CODE                                          --可接受广告类别:src.MAIL_CODE
       ,DST.MAR_STATUS                                         --婚姻状况:src.MAR_STATUS
       ,DST.MO_PHONE                                           --手机号码:src.MO_PHONE
       ,DST.MP_LIMIT                                           --分期付款额度:src.MP_LIMIT
       ,DST.MTHR_MNAME                                         --英文性名:src.MTHR_MNAME
       ,DST.OCC_CATGRY                                         --公司性质:src.OCC_CATGRY
       ,DST.OCC_CODE                                           --行业类别代码:src.OCC_CODE
       ,DST.POSN_EMPLY                                         --职务:src.POSN_EMPLY
       ,DST.RACE_CODE                                          --证件类型:src.RACE_CODE
       ,DST.REL_NAM                                            --直系亲属姓名:src.REL_NAM
       ,DST.REL_TEL                                            --直系亲属电话号码:src.REL_TEL
       ,DST.FORECOMP                                           --前公司中文全称:src.FORECOMP
       ,DST.FOREDEPT                                           --前公司部门:src.FOREDEPT
       ,DST.FOREJOB                                            --前公司职务:src.FOREJOB
       ,DST.FOREBUSI                                           --前公司电话:src.FOREBUSI
       ,DST.FOREANN                                            --前公司收入:src.FOREANN
       ,DST.YR_FORECOM                                         --前公司工龄:src.YR_FORECOM
       ,DST.SPU_NAM                                            --配偶姓名:src.SPU_NAM
       ,DST.SPU_TEL                                            --配偶电话号码:src.SPU_TEL
       ,DST.SURNAME                                            --姓名:src.SURNAME
       ,DST.XTITLE                                             --称谓:src.XTITLE
       ,DST.WORK_CALLS                                         --能否工作时间联系:src.WORK_CALLS
       ,DST.YR_IN_COM2                                         --配偶工龄:src.YR_IN_COM2
       ,DST.YR_IN_COMP                                         --个人工龄:src.YR_IN_COMP
       ,DST.YR_THERE                                           --居住年数:src.YR_THERE
       ,DST.SPU_MOBILE                                         --配偶手机号码:src.SPU_MOBILE
       ,DST.HOME_LOAN                                          --月租金额/月还款额:src.HOME_LOAN
       ,DST.CON_MO1                                            --联系人1手机:src.CON_MO1
       ,DST.CON_MO2                                            --联系人2手机:src.CON_MO2
       ,DST.REL_MOBILE                                         --直系亲属手机:src.REL_MOBILE
       ,DST.INT_TAXCOD                                         --职务/岗位:src.INT_TAXCOD
       ,DST.ID_VERIFY                                          --身份核查结果:src.ID_VERIFY
       ,DST.IDCP_YN                                            --是否留存身份证复印件:src.IDCP_YN
       ,DST.IV_RST                                             --核实结果:src.IV_RST
       ,DST.IV_RSN                                             --无法核实原因:src.IV_RSN
       ,DST.IV_DISPO                                           --处置方法:src.IV_DISPO
       ,DST.ID_EXPDT                                           --身份证件有效期:src.ID_EXPDT
       ,DST.QUESTION                                           --客户预留问题:src.QUESTION
       ,DST.ANSWER                                             --客户预留问题答案:src.ANSWER
       ,DST.OCT_SCORE                                          --催收评分:src.OCT_SCORE
       ,DST.NATION_CD                                          --国家代码:src.NATION_CD
       ,DST.ORG_NO                                             --机构编号:src.ORG_NO
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_CI_CUSTR DST 
   LEFT JOIN F_CI_CUSTR_INNTMP1 SRC 
     ON SRC.CUSTR_NBR           = DST.CUSTR_NBR 
  WHERE SRC.CUSTR_NBR IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CUSTR_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_CUSTR/"+V_DT+".parquet"
UNION=F_CI_CUSTR_INNTMP2.unionAll(F_CI_CUSTR_INNTMP1)
F_CI_CUSTR_INNTMP1.cache()
F_CI_CUSTR_INNTMP2.cache()
nrowsi = F_CI_CUSTR_INNTMP1.count()
nrowsa = F_CI_CUSTR_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_CUSTR_INNTMP1.unpersist()
F_CI_CUSTR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CUSTR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_CUSTR/"+V_DT_LD+".parquet /"+dbname+"/F_CI_CUSTR_BK/")

#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CUSTR/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CUSTR_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CI_CUSTR/"+V_DT+".parquet /"+dbname+"/F_CI_CUSTR_BK/")