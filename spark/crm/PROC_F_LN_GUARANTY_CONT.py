#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_LN_GUARANTY_CONT').setMaster(sys.argv[2])
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

OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
F_LN_XDXT_GUARANTY_CONTRACT = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_GUARANTY_CONTRACT/*')
F_LN_XDXT_GUARANTY_CONTRACT.registerTempTable("F_LN_XDXT_GUARANTY_CONTRACT")

#任务[12] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.SERIALNO              AS SERIALNO 
       ,A.CONTRACTTYPE          AS CONTRACTTYPE 
       ,A.GUARANTYTYPE          AS GUARANTYTYPE 
       ,A.CONTRACTSTATUS        AS CONTRACTSTATUS 
       ,A.CONTRACTNO            AS CONTRACTNO 
       ,A.SIGNDATE              AS SIGNDATE 
       ,A.BEGINDATE             AS BEGINDATE 
       ,A.ENDDATE               AS ENDDATE 
       ,B.ODS_CUST_ID           AS CUST_ID 
       ,A.GUARANTORID           AS GUARANTORID 
       ,A.GUARANTORNAME         AS GUARANTORNAME 
       ,A.CREDITORGID           AS CREDITORGID 
       ,A.CREDITORGNAME         AS CREDITORGNAME 
       ,A.GUARANTYCURRENCY      AS GUARANTYCURRENCY 
       ,CAST(A.GUARANTYVALUE  AS DECIMAL(24,6))       AS GUARANTYVALUE 
       ,A.GUARANTYINFO          AS GUARANTYINFO 
       ,A.OTHERDESCRIBE         AS OTHERDESCRIBE 
       ,A.CHECKGUARANTY         AS CHECKGUARANTY 
       ,A.RECEPTION             AS RECEPTION 
       ,A.RECEPTIONDUTY         AS RECEPTIONDUTY 
       ,A.GUARANRYOPINION       AS GUARANRYOPINION 
       ,A.CHECKGUARANTYMAN1     AS CHECKGUARANTYMAN1 
       ,A.CHECKGUARANTYMAN2     AS CHECKGUARANTYMAN2 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.UPDATEUSERID          AS UPDATEUSERID 
       ,''            AS UPDATEDATE 
       ,A.REMARK                AS REMARK 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CERTID                AS CERTID 
       ,A.OTHERNAME             AS OTHERNAME 
       ,A.LOANCARDNO            AS LOANCARDNO 
       ,A.GUARANTEEFORM         AS GUARANTEEFORM 
       ,A.COMMONDATE            AS COMMONDATE 
       ,CAST(A.BAILRATIO  AS DECIMAL(24,6))           AS BAILRATIO 
       ,A.APPENDGUARANTYSTATUS  AS APPENDGUARANTYSTATUS 
       ,CAST(A.GUARANTYBALANCEVALUE AS DECIMAL(24,6)) AS GUARANTYBALANCEVALUE 
       ,CAST(A.CLOANBAL    AS DECIMAL(24,6))          AS CLOANBAL 
       ,CAST(A.CBAL      AS DECIMAL(24,6))            AS CBAL 
       ,A.BAILFREEZEDATE        AS BAILFREEZEDATE 
       ,A.BAILFREEZENO          AS BAILFREEZENO 
       ,CAST(A.GUARANTYBALANCEVALUE1  AS DECIMAL(24,6))   AS GUARANTYBALANCEVALUE1 
       ,A.PAYTYPE               AS PAYTYPE 
       ,A.MIGRATEFLAG           AS MIGRATEFLAG 
       ,A.BAILFREEZETYPE        AS BAILFREEZETYPE 
       ,A.BAILFREEZEIDN         AS BAILFREEZEIDN 
       ,A.CONTRACTSERIALNO      AS CONTRACTSERIALNO 
       ,A.GUARANTYIDCURRENCY    AS GUARANTYIDCURRENCY 
       ,CAST(A.EXCHANGERATE   AS DECIMAL(24,6))         AS EXCHANGERATE 
       ,NVL(A.ODS_ST_DATE, 'UNK')                       AS ODS_ST_DATE 
       ,monotonically_increasing_id()     AS ID 
       ,B.FR_ID                 AS FR_ID 
   FROM F_LN_XDXT_GUARANTY_CONTRACT A                          --担保合同信息
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --系统来源中间表
     ON A.CUSTOMERID            = B.SOURCE_CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.ODS_SYS_ID            = 'LNA' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_LN_GUARANTY_CONT_INNTMP1 = sqlContext.sql(sql)
OCRM_F_LN_GUARANTY_CONT_INNTMP1.registerTempTable("OCRM_F_LN_GUARANTY_CONT_INNTMP1")

OCRM_F_LN_GUARANTY_CONT = sqlContext.read.parquet(hdfs+'/OCRM_F_LN_GUARANTY_CONT_BK/'+V_DT_LD+'.parquet/*')
OCRM_F_LN_GUARANTY_CONT.registerTempTable("OCRM_F_LN_GUARANTY_CONT")
sql = """
 SELECT DST.SERIALNO                                            --合同流水号:src.SERIALNO
       ,DST.CONTRACTTYPE                                       --合同类型:src.CONTRACTTYPE
       ,DST.GUARANTYTYPE                                       --担保类型:src.GUARANTYTYPE
       ,DST.CONTRACTSTATUS                                     --合同状态:src.CONTRACTSTATUS
       ,DST.CONTRACTNO                                         --担保合同编号:src.CONTRACTNO
       ,DST.SIGNDATE                                           --协议签定日期:src.SIGNDATE
       ,DST.BEGINDATE                                          --合同生效日:src.BEGINDATE
       ,DST.ENDDATE                                            --合同到期日:src.ENDDATE
       ,DST.CUST_ID                                            --客户号:src.CUST_ID
       ,DST.GUARANTORID                                        --担保人编号:src.GUARANTORID
       ,DST.GUARANTORNAME                                      --担保人名称:src.GUARANTORNAME
       ,DST.CREDITORGID                                        --债权人机构代码:src.CREDITORGID
       ,DST.CREDITORGNAME                                      --债权人机构名称:src.CREDITORGNAME
       ,DST.GUARANTYCURRENCY                                   --担保币种:src.GUARANTYCURRENCY
       ,DST.GUARANTYVALUE                                      --担保总金额:src.GUARANTYVALUE
       ,DST.GUARANTYINFO                                       --担保物概况:src.GUARANTYINFO
       ,DST.OTHERDESCRIBE                                      --其它特别约定:src.OTHERDESCRIBE
       ,DST.CHECKGUARANTY                                      --核保时间:src.CHECKGUARANTY
       ,DST.RECEPTION                                          --接待人姓名:src.RECEPTION
       ,DST.RECEPTIONDUTY                                      --接待人职务:src.RECEPTIONDUTY
       ,DST.GUARANRYOPINION                                    --担保意见:src.GUARANRYOPINION
       ,DST.CHECKGUARANTYMAN1                                  --核保人（一）:src.CHECKGUARANTYMAN1
       ,DST.CHECKGUARANTYMAN2                                  --核保人（二）:src.CHECKGUARANTYMAN2
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTDATE                                          --登记日期:src.INPUTDATE
       ,DST.UPDATEUSERID                                       --更新人:src.UPDATEUSERID
       ,DST.UPDATEDATE                                         --更新日期:src.UPDATEDATE
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.CERTTYPE                                           --担保人证件类型:src.CERTTYPE
       ,DST.CERTID                                             --担保人证件号码:src.CERTID
       ,DST.OTHERNAME                                          --其他名称:src.OTHERNAME
       ,DST.LOANCARDNO                                         --担保人贷款卡编号:src.LOANCARDNO
       ,DST.GUARANTEEFORM                                      --保证担保形式:src.GUARANTEEFORM
       ,DST.COMMONDATE                                         --通用日期:src.COMMONDATE
       ,DST.BAILRATIO                                          --保证金比例:src.BAILRATIO
       ,DST.APPENDGUARANTYSTATUS                               --担保合同追加状态:src.APPENDGUARANTYSTATUS
       ,DST.GUARANTYBALANCEVALUE                               --可用余额:src.GUARANTYBALANCEVALUE
       ,DST.CLOANBAL                                           --金额:src.CLOANBAL
       ,DST.CBAL                                               --保证金利率:src.CBAL
       ,DST.BAILFREEZEDATE                                     --保证金止付日期:src.BAILFREEZEDATE
       ,DST.BAILFREEZENO                                       --保证金止付编号:src.BAILFREEZENO
       ,DST.GUARANTYBALANCEVALUE1                              --余额:src.GUARANTYBALANCEVALUE1
       ,DST.PAYTYPE                                            --钞汇鉴别:src.PAYTYPE
       ,DST.MIGRATEFLAG                                        --移植标志:src.MIGRATEFLAG
       ,DST.BAILFREEZETYPE                                     --冻结类型:src.BAILFREEZETYPE
       ,DST.BAILFREEZEIDN                                      --钞汇鉴别2:src.BAILFREEZEIDN
       ,DST.CONTRACTSERIALNO                                   --业务合同编号:src.CONTRACTSERIALNO
       ,DST.GUARANTYIDCURRENCY                                 --保证金账户币种:src.GUARANTYIDCURRENCY
       ,DST.EXCHANGERATE                                       --担保金额汇率转换值:src.EXCHANGERATE
       ,DST.ODS_ST_DATE                                        --:src.ODS_ST_DATE
       ,DST.ID                                                 --:src.ID
       ,DST.FR_ID                                              --:src.FR_ID
   FROM OCRM_F_LN_GUARANTY_CONT DST 
   LEFT JOIN OCRM_F_LN_GUARANTY_CONT_INNTMP1 SRC 
     ON SRC.SERIALNO            = DST.SERIALNO 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.SERIALNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_LN_GUARANTY_CONT_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_LN_GUARANTY_CONT/"+V_DT+".parquet"
UNION=OCRM_F_LN_GUARANTY_CONT_INNTMP2.unionAll(OCRM_F_LN_GUARANTY_CONT_INNTMP1)
OCRM_F_LN_GUARANTY_CONT_INNTMP1.cache()
OCRM_F_LN_GUARANTY_CONT_INNTMP2.cache()
nrowsi = OCRM_F_LN_GUARANTY_CONT_INNTMP1.count()
nrowsa = OCRM_F_LN_GUARANTY_CONT_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_LN_GUARANTY_CONT_INNTMP1.unpersist()
OCRM_F_LN_GUARANTY_CONT_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_LN_GUARANTY_CONT lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_LN_GUARANTY_CONT/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_LN_GUARANTY_CONT_BK/")

ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_LN_GUARANTY_CONT_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_LN_GUARANTY_CONT/"+V_DT+".parquet /"+dbname+"/OCRM_F_LN_GUARANTY_CONT_BK/")
