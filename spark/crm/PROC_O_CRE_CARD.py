#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CRE_CARD').setMaster(sys.argv[2])
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

O_CR_CARD = sqlContext.read.parquet(hdfs+'/O_CR_CARD/*')
O_CR_CARD.registerTempTable("O_CR_CARD")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CR_CARD = sqlContext.read.parquet(hdfs+'/F_CR_CARD_BK/'+V_DT_LD+'.parquet/*')
F_CR_CARD.registerTempTable("F_CR_CARD")

sql = """
 SELECT CARD_NBR                AS CARD_NBR 
       ,PRODUCT                 AS PRODUCT 
       ,CANCL_CODE              AS CANCL_CODE 
       ,CANCL_DAY               AS CANCL_DAY 
       ,CANCL_EMPL              AS CANCL_EMPL 
       ,CANCL_TIME              AS CANCL_TIME 
       ,ACTIONCODE              AS ACTIONCODE 
       ,CREATE_DAY              AS CREATE_DAY 
       ,CASHAD_NOX              AS CASHAD_NOX 
       ,CASHADV_NO              AS CASHADV_NO 
       ,DESPATCH                AS DESPATCH 
       ,EMBOSS_LN2              AS EMBOSS_LN2 
       ,EMBOSS_NME              AS EMBOSS_NME 
       ,EXPIRY_DTE              AS EXPIRY_DTE 
       ,EXPIRY_NEW              AS EXPIRY_NEW 
       ,FEE_CODE                AS FEE_CODE 
       ,ISSUE_DAY               AS ISSUE_DAY 
       ,ISSUE_REAS              AS ISSUE_REAS 
       ,LOSS_DAY                AS LOSS_DAY 
       ,LOSS_REPRT              AS LOSS_REPRT 
       ,LOSS_TIME               AS LOSS_TIME 
       ,DEPAM_TDX               AS DEPAM_TDX 
       ,DEPAM_TDY               AS DEPAM_TDY 
       ,DEPNO_TDX               AS DEPNO_TDX 
       ,DEPNO_TDY               AS DEPNO_TDY 
       ,DEPOSIT_NO              AS DEPOSIT_NO 
       ,HRCASH_NO               AS HRCASH_NO 
       ,HRCASH_NOX              AS HRCASH_NOX 
       ,MAX_PAMTX               AS MAX_PAMTX 
       ,MAX_PINTRY              AS MAX_PINTRY 
       ,PIN_FAILDL              AS PIN_FAILDL 
       ,PIN_FAILS               AS PIN_FAILS 
       ,PURCAM_TDX              AS PURCAM_TDX 
       ,PURCAM_TDY              AS PURCAM_TDY 
       ,PURCHS_NOX              AS PURCHS_NOX 
       ,PURCHSE_NO              AS PURCHSE_NO 
       ,REISS_DTE               AS REISS_DTE 
       ,URGENTFEE               AS URGENTFEE 
       ,VALID_FROM              AS VALID_FROM 
       ,VALID_NEW               AS VALID_NEW 
       ,SMS_YN                  AS SMS_YN 
       ,PURCH_YN                AS PURCH_YN 
       ,CARD_FACE               AS CARD_FACE 
       ,FK_ED                   AS FK_ED 
       ,FK_EDBL                 AS FK_EDBL 
       ,CUSTR_NBR               AS CUSTR_NBR 
       ,RWJG                    AS RWJG 
       ,FEE_MONTH               AS FEE_MONTH 
       ,NETTRAN_YN              AS NETTRAN_YN 
       ,NETTRAN_DATE            AS NETTRAN_DATE 
       ,INTRAN_YN               AS INTRAN_YN 
       ,NOMAG_YN                AS NOMAG_YN 
       ,ACTIVE_DAY              AS ACTIVE_DAY 
       ,ACTIVE_CHA              AS ACTIVE_CHA 
       ,CANCL_REA               AS CANCL_REA 
       ,WCWM_YN                 AS WCWM_YN 
       ,PIN_LMT                 AS PIN_LMT 
       ,CDPTFEE_YN              AS CDPTFEE_YN 
       ,WITHDRW_YN              AS WITHDRW_YN 
       ,XFRFROM_YN              AS XFRFROM_YN 
       ,DEPOSIT_YN              AS DEPOSIT_YN 
       ,BALINQ_YN               AS BALINQ_YN 
       ,PURCH_YN_O              AS PURCH_YN_O 
       ,CASHBCK_YN              AS CASHBCK_YN 
       ,XFRCHNL                 AS XFRCHNL 
       ,REISS_TYPE              AS REISS_TYPE 
       ,CLMLIM_YN               AS CLMLIM_YN 
       ,REISS_FLG               AS REISS_FLG 
       ,REPLACEFEE              AS REPLACEFEE 
       ,PBOC_YN                 AS PBOC_YN 
       ,AUTH_PDAY               AS AUTH_PDAY 
       ,AUTH_PTIME              AS AUTH_PTIME 
       ,HDWR_SN                 AS HDWR_SN 
       ,ISSUE_NBR               AS ISSUE_NBR 
       ,ORG_NO                  AS ORG_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CRE'                   AS ODS_SYS_ID 
   FROM O_CR_CARD A                                            --卡片资料
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CR_CARD_INNTMP1 = sqlContext.sql(sql)
F_CR_CARD_INNTMP1.registerTempTable("F_CR_CARD_INNTMP1")

#F_CR_CARD = sqlContext.read.parquet(hdfs+'/F_CR_CARD/*')
#F_CR_CARD.registerTempTable("F_CR_CARD")
sql = """
 SELECT DST.CARD_NBR                                            --卡号:src.CARD_NBR
       ,DST.PRODUCT                                            --产品编号:src.PRODUCT
       ,DST.CANCL_CODE                                         --卡片状态代码:src.CANCL_CODE
       ,DST.CANCL_DAY                                          --卡片状态日期:src.CANCL_DAY
       ,DST.CANCL_EMPL                                         --卡片挂失员工:src.CANCL_EMPL
       ,DST.CANCL_TIME                                         --卡片挂失时间:src.CANCL_TIME
       ,DST.ACTIONCODE                                         --外卡黑名单动作代码:src.ACTIONCODE
       ,DST.CREATE_DAY                                         --产生日期:src.CREATE_DAY
       ,DST.CASHAD_NOX                                         --每日外币预借现金笔数(已作废):src.CASHAD_NOX
       ,DST.CASHADV_NO                                         --每日本币预借现金笔数（作废）:src.CASHADV_NO
       ,DST.DESPATCH                                           --递送方式:src.DESPATCH
       ,DST.EMBOSS_LN2                                         --第二道凸字(企业卡的公司名称):src.EMBOSS_LN2
       ,DST.EMBOSS_NME                                         --凸字名称:src.EMBOSS_NME
       ,DST.EXPIRY_DTE                                         --到期日(YYMM):src.EXPIRY_DTE
       ,DST.EXPIRY_NEW                                         --重发卡的新到期日(YYMM):src.EXPIRY_NEW
       ,DST.FEE_CODE                                           --卡片费用代码:src.FEE_CODE
       ,DST.ISSUE_DAY                                          --发卡日期:src.ISSUE_DAY
       ,DST.ISSUE_REAS                                         --发卡原因代码:src.ISSUE_REAS
       ,DST.LOSS_DAY                                           --卡片遗失日期:src.LOSS_DAY
       ,DST.LOSS_REPRT                                         --挂失方式:src.LOSS_REPRT
       ,DST.LOSS_TIME                                          --卡片遗失时间:src.LOSS_TIME
       ,DST.DEPAM_TDX                                          --允许每日外币存款最大金额:src.DEPAM_TDX
       ,DST.DEPAM_TDY                                          --允许每日本币存款最大金额:src.DEPAM_TDY
       ,DST.DEPNO_TDX                                          --允许每日外币存款最多次数（作废）:src.DEPNO_TDX
       ,DST.DEPNO_TDY                                          --允许每日本币存款最多次数（作废）:src.DEPNO_TDY
       ,DST.DEPOSIT_NO                                         --允许每日存款最多次数(作废):src.DEPOSIT_NO
       ,DST.HRCASH_NO                                          --允许本币预借现金最多次数:src.HRCASH_NO
       ,DST.HRCASH_NOX                                         --允许外币预借现金最多次数:src.HRCASH_NOX
       ,DST.MAX_PAMTX                                          --允许每日外币缴款最大金额:src.MAX_PAMTX
       ,DST.MAX_PINTRY                                         --允许使用PIN错误最多次数:src.MAX_PINTRY
       ,DST.PIN_FAILDL                                         --允许每日PIN错误次数:src.PIN_FAILDL
       ,DST.PIN_FAILS                                          --允许PIN错误次数:src.PIN_FAILS
       ,DST.PURCAM_TDX                                         --允许每日外币购货总额:src.PURCAM_TDX
       ,DST.PURCAM_TDY                                         --允许每日本币购货总额:src.PURCAM_TDY
       ,DST.PURCHS_NOX                                         --允许今日外币购货次数:src.PURCHS_NOX
       ,DST.PURCHSE_NO                                         --允许今日本币购货次数:src.PURCHSE_NO
       ,DST.REISS_DTE                                          --重发卡日期:src.REISS_DTE
       ,DST.URGENTFEE                                          --紧急替代卡标志:src.URGENTFEE
       ,DST.VALID_FROM                                         --有效期的起始日期(YYMM):src.VALID_FROM
       ,DST.VALID_NEW                                          --重发卡有效期的起始日期(YYMM):src.VALID_NEW
       ,DST.SMS_YN                                             --短信通知标志:src.SMS_YN
       ,DST.PURCH_YN                                           --消费使用密码标志:src.PURCH_YN
       ,DST.CARD_FACE                                          --卡片版面:src.CARD_FACE
       ,DST.FK_ED                                              --附卡单独设置额度比例:src.FK_ED
       ,DST.FK_EDBL                                            --附卡额度比例:src.FK_EDBL
       ,DST.CUSTR_NBR                                          --证件号码:src.CUSTR_NBR
       ,DST.RWJG                                               --入网机构代码:src.RWJG
       ,DST.FEE_MONTH                                          --下次收年费月份:src.FEE_MONTH
       ,DST.NETTRAN_YN                                         --网上交易开通标志:src.NETTRAN_YN
       ,DST.NETTRAN_DATE                                       --网上交易开通期限:src.NETTRAN_DATE
       ,DST.INTRAN_YN                                          --境外交易开关:src.INTRAN_YN
       ,DST.NOMAG_YN                                           --无磁交易开关:src.NOMAG_YN
       ,DST.ACTIVE_DAY                                         --卡片激活日期:src.ACTIVE_DAY
       ,DST.ACTIVE_CHA                                         --卡片激活渠道:src.ACTIVE_CHA
       ,DST.CANCL_REA                                          --卡片注销原因代码:src.CANCL_REA
       ,DST.WCWM_YN                                            --是否允许无磁无密商户交易:src.WCWM_YN
       ,DST.PIN_LMT                                            --消费密码生效金额:src.PIN_LMT
       ,DST.CDPTFEE_YN                                         --卡片单独积分兑换年费:src.CDPTFEE_YN
       ,DST.WITHDRW_YN                                         --可取现功能:src.WITHDRW_YN
       ,DST.XFRFROM_YN                                         --可转出功能:src.XFRFROM_YN
       ,DST.DEPOSIT_YN                                         --可存款功能:src.DEPOSIT_YN
       ,DST.BALINQ_YN                                          --可余额查询功能:src.BALINQ_YN
       ,DST.PURCH_YN_O                                         --可消费功能:src.PURCH_YN_O
       ,DST.CASHBCK_YN                                         --可找现功能:src.CASHBCK_YN
       ,DST.XFRCHNL                                            --ATM转账、电话转账、网银转账、手机转账功能字段:src.XFRCHNL
       ,DST.REISS_TYPE                                         --换卡标志:src.REISS_TYPE
       ,DST.CLMLIM_YN                                          --取消PRMMT账期控制:src.CLMLIM_YN
       ,DST.REISS_FLG                                          --是否续卡标志位:src.REISS_FLG
       ,DST.REPLACEFEE                                         --换卡收费标志位:src.REPLACEFEE
       ,DST.PBOC_YN                                            --IC卡标志位:src.PBOC_YN
       ,DST.AUTH_PDAY                                          --最后一次交易发生日期:src.AUTH_PDAY
       ,DST.AUTH_PTIME                                         --最后一次交易发生时间:src.AUTH_PTIME
       ,DST.HDWR_SN                                            --硬件序列号:src.HDWR_SN
       ,DST.ISSUE_NBR                                          --卡片序号:src.ISSUE_NBR
       ,DST.ORG_NO                                             --机构编号:src.ORG_NO
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_CR_CARD DST 
   LEFT JOIN F_CR_CARD_INNTMP1 SRC 
     ON SRC.CARD_NBR            = DST.CARD_NBR 
  WHERE SRC.CARD_NBR IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CR_CARD_INNTMP2 = sqlContext.sql(sql)
dfn="F_CR_CARD/"+V_DT+".parquet"
UNION=F_CR_CARD_INNTMP2.unionAll(F_CR_CARD_INNTMP1)
F_CR_CARD_INNTMP1.cache()
F_CR_CARD_INNTMP2.cache()
nrowsi = F_CR_CARD_INNTMP1.count()
nrowsa = F_CR_CARD_INNTMP2.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CR_CARD/*.parquet ")
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CR_CARD_INNTMP1.unpersist()
F_CR_CARD_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CR_CARD lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_CR_CARD/"+V_DT_LD+".parquet /"+dbname+"/F_CR_CARD_BK/")


#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CR_CARD_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CR_CARD/"+V_DT+".parquet /"+dbname+"/F_CR_CARD_BK/")
