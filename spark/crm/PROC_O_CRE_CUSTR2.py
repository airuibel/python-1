#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CRE_CUSTR2').setMaster(sys.argv[2])
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

O_CI_CUSTR2 = sqlContext.read.parquet(hdfs+'/O_CI_CUSTR2/*')
O_CI_CUSTR2.registerTempTable("O_CI_CUSTR2")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CI_CUSTR2 = sqlContext.read.parquet(hdfs+'/F_CI_CUSTR2_BK/'+V_DT_LD+'.parquet/*')
F_CI_CUSTR2.registerTempTable("F_CI_CUSTR2")

sql = """
 SELECT BANK                    AS BANK 
       ,CUSTR_NBR               AS CUSTR_NBR 
       ,VALUE_LEVEL             AS VALUE_LEVEL 
       ,RISK_LEVEL              AS RISK_LEVEL 
       ,TEL_YN                  AS TEL_YN 
       ,MOBILE_YN               AS MOBILE_YN 
       ,SMS_YN                  AS SMS_YN 
       ,EMAIL_YN                AS EMAIL_YN 
       ,LETTER_YN               AS LETTER_YN 
       ,ZX_HY                   AS ZX_HY 
       ,ZX_ZW                   AS ZX_ZW 
       ,CRMUPD_YN               AS CRMUPD_YN 
       ,SUT_YN                  AS SUT_YN 
       ,CA_PCNT                 AS CA_PCNT 
       ,CUSTR_REF               AS CUSTR_REF 
       ,CUAPP_TYPE              AS CUAPP_TYPE 
       ,STU_GRADE               AS STU_GRADE 
       ,YEAR_EARN               AS YEAR_EARN 
       ,SAVINGSUM               AS SAVINGSUM 
       ,NSA_VALUE               AS NSA_VALUE 
       ,HP_VALUE                AS HP_VALUE 
       ,CAR_VALUE               AS CAR_VALUE 
       ,BNK_RPT                 AS BNK_RPT 
       ,CRMUTM_YN               AS CRMUTM_YN 
       ,TRANS_TY                AS TRANS_TY 
       ,IMP_FLAG                AS IMP_FLAG 
       ,PER_SCORE               AS PER_SCORE 
       ,NPERS_RSN               AS NPERS_RSN 
       ,REDUCE_YN               AS REDUCE_YN 
       ,SP_CUSTR1               AS SP_CUSTR1 
       ,SP_CUSTR2               AS SP_CUSTR2 
       ,SP_CUSTR3               AS SP_CUSTR3 
       ,SP_CUSTR4               AS SP_CUSTR4 
       ,SP_CUSTR5               AS SP_CUSTR5 
       ,NATION_DET              AS NATION_DET 
       ,ORG_ID                  AS ORG_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CRE'                   AS ODS_SYS_ID 
   FROM O_CI_CUSTR2 A                                          --客户资料2
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CUSTR2_INNTMP1 = sqlContext.sql(sql)
F_CI_CUSTR2_INNTMP1.registerTempTable("F_CI_CUSTR2_INNTMP1")

#F_CI_CUSTR2 = sqlContext.read.parquet(hdfs+'/F_CI_CUSTR2/*')
#F_CI_CUSTR2.registerTempTable("F_CI_CUSTR2")
sql = """
 SELECT DST.BANK                                                --银行:src.BANK
       ,DST.CUSTR_NBR                                          --客户证件号码:src.CUSTR_NBR
       ,DST.VALUE_LEVEL                                        --客户价值分层代码:src.VALUE_LEVEL
       ,DST.RISK_LEVEL                                         --客户风险分层代码:src.RISK_LEVEL
       ,DST.TEL_YN                                             --固话接听喜好:src.TEL_YN
       ,DST.MOBILE_YN                                          --手机接听喜好:src.MOBILE_YN
       ,DST.SMS_YN                                             --短信接收喜好:src.SMS_YN
       ,DST.EMAIL_YN                                           --邮件接收喜好:src.EMAIL_YN
       ,DST.LETTER_YN                                          --纸质信函接收喜好:src.LETTER_YN
       ,DST.ZX_HY                                              --客户征信代码-行业:src.ZX_HY
       ,DST.ZX_ZW                                              --客户征信代码-职务:src.ZX_ZW
       ,DST.CRMUPD_YN                                          --客户愿意接收批量固定额度调整:src.CRMUPD_YN
       ,DST.SUT_YN                                             --若为学生卡，学生未毕业:src.SUT_YN
       ,DST.CA_PCNT                                            --客户预借现金比例:src.CA_PCNT
       ,DST.CUSTR_REF                                          --客户参考资料编号:src.CUSTR_REF
       ,DST.CUAPP_TYPE                                         --申请人类型:src.CUAPP_TYPE
       ,DST.STU_GRADE                                          --学生年级:src.STU_GRADE
       ,DST.YEAR_EARN                                          --征信认定年收入:src.YEAR_EARN
       ,DST.SAVINGSUM                                          --存款金额:src.SAVINGSUM
       ,DST.NSA_VALUE                                          --证券资产价值:src.NSA_VALUE
       ,DST.HP_VALUE                                           --房产价值:src.HP_VALUE
       ,DST.CAR_VALUE                                          --车产价值:src.CAR_VALUE
       ,DST.BNK_RPT                                            --人行报告情况:src.BNK_RPT
       ,DST.CRMUTM_YN                                          --客户愿意接收批量临时额度调整:src.CRMUTM_YN
       ,DST.TRANS_TY                                           --交易类型:src.TRANS_TY
       ,DST.IMP_FLAG                                           --质押客户标识:src.IMP_FLAG
       ,DST.PER_SCORE                                          --客户行为评分:src.PER_SCORE
       ,DST.NPERS_RSN                                          --无行为评分原因:src.NPERS_RSN
       ,DST.REDUCE_YN                                          --不愿意接受低使用率或睡眠户降额:src.REDUCE_YN
       ,DST.SP_CUSTR1                                          --特殊客户标示:src.SP_CUSTR1
       ,DST.SP_CUSTR2                                          --保险S2S客户:src.SP_CUSTR2
       ,DST.SP_CUSTR3                                          --营销活动3客户:src.SP_CUSTR3
       ,DST.SP_CUSTR4                                          --营销活动4客户:src.SP_CUSTR4
       ,DST.SP_CUSTR5                                          --营销活动5客户:src.SP_CUSTR5
       ,DST.NATION_DET                                         --国籍详细:src.NATION_DET
       ,DST.ORG_ID                                             --机构编号:src.ORG_ID
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_CI_CUSTR2 DST 
   LEFT JOIN F_CI_CUSTR2_INNTMP1 SRC 
     ON SRC.BANK                = DST.BANK 
    AND SRC.CUSTR_NBR           = DST.CUSTR_NBR 
  WHERE SRC.BANK IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CUSTR2_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_CUSTR2/"+V_DT+".parquet"
UNION=F_CI_CUSTR2_INNTMP2.unionAll(F_CI_CUSTR2_INNTMP1)
F_CI_CUSTR2_INNTMP1.cache()
F_CI_CUSTR2_INNTMP2.cache()
nrowsi = F_CI_CUSTR2_INNTMP1.count()
nrowsa = F_CI_CUSTR2_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_CUSTR2_INNTMP1.unpersist()
F_CI_CUSTR2_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CUSTR2 lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_CUSTR2/"+V_DT_LD+".parquet /"+dbname+"/F_CI_CUSTR2_BK/")
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CUSTR2/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CUSTR2_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CI_CUSTR2/"+V_DT+".parquet /"+dbname+"/F_CI_CUSTR2_BK/")