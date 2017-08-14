#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_SLNA_SUNCREDIT_APPLY').setMaster(sys.argv[2])
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

O_LN_SUNCREDIT_APPLY = sqlContext.read.parquet(hdfs+'/O_LN_SUNCREDIT_APPLY/*')
O_LN_SUNCREDIT_APPLY.registerTempTable("O_LN_SUNCREDIT_APPLY")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_SUNCREDIT_APPLY/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_SUNCREDIT_APPLY_BK/"+V_DT_LD+".parquet /"+dbname+"/F_LN_SUNCREDIT_APPLY/"+V_DT+".parquet")


F_LN_SUNCREDIT_APPLY = sqlContext.read.parquet(hdfs+'/F_LN_SUNCREDIT_APPLY/*')
F_LN_SUNCREDIT_APPLY.registerTempTable("F_LN_SUNCREDIT_APPLY")

sql = """
 SELECT A.SERIALNO              AS SERIALNO 
       ,A.CUSTOMERID            AS CUSTOMERID 
       ,A.CUSTOMERNAME          AS CUSTOMERNAME 
       ,A.BUSINESSSUM           AS BUSINESSSUM 
       ,A.CURRENCY              AS CURRENCY 
       ,A.OPERATEUSERID         AS OPERATEUSERID 
       ,A.OPERATEORGID          AS OPERATEORGID 
       ,A.OPERATEDATE           AS OPERATEDATE 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.UPDATEDATE            AS UPDATEDATE 
       ,A.OCCURDATE             AS OCCURDATE 
       ,A.MATURITY              AS MATURITY 
       ,A.TERMMONTH             AS TERMMONTH 
       ,A.PIGEONHOLEDATE        AS PIGEONHOLEDATE 
       ,A.TEMPSAVEFLAG          AS TEMPSAVEFLAG 
       ,A.REMARK                AS REMARK 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CERTID                AS CERTID 
       ,A.FAMILYID              AS FAMILYID 
       ,A.FLAG                  AS FLAG 
       ,A.REGISTERFLAG          AS REGISTERFLAG 
       ,A.FINISHDATE            AS FINISHDATE 
       ,A.CREDITLEVEL           AS CREDITLEVEL 
       ,A.DELAYCREDITREASON     AS DELAYCREDITREASON 
       ,A.APPROVEPERSONFLAG     AS APPROVEPERSONFLAG 
       ,A.APPROVEFLAG           AS APPROVEFLAG 
       ,A.BELONGFLAG            AS BELONGFLAG 
       ,A.CREDITFLAG            AS CREDITFLAG 
       ,A.CREDITADJUST          AS CREDITADJUST 
       ,A.YEARAPPROVEFLAG       AS YEARAPPROVEFLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'SLNA'                  AS ODS_SYS_ID 
   FROM O_LN_SUNCREDIT_APPLY A                                 --阳光信贷预授信申请表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_SUNCREDIT_APPLY_INNTMP1 = sqlContext.sql(sql)
F_LN_SUNCREDIT_APPLY_INNTMP1.registerTempTable("F_LN_SUNCREDIT_APPLY_INNTMP1")

#F_LN_SUNCREDIT_APPLY = sqlContext.read.parquet(hdfs+'/F_LN_SUNCREDIT_APPLY/*')
#F_LN_SUNCREDIT_APPLY.registerTempTable("F_LN_SUNCREDIT_APPLY")
sql = """
 SELECT DST.SERIALNO                                            --流水号:src.SERIALNO
       ,DST.CUSTOMERID                                         --客户编号:src.CUSTOMERID
       ,DST.CUSTOMERNAME                                       --客户名称:src.CUSTOMERNAME
       ,DST.BUSINESSSUM                                        --预授信金额:src.BUSINESSSUM
       ,DST.CURRENCY                                           --币种:src.CURRENCY
       ,DST.OPERATEUSERID                                      --管户人编号:src.OPERATEUSERID
       ,DST.OPERATEORGID                                       --管户机构编号:src.OPERATEORGID
       ,DST.OPERATEDATE                                        --操作时间:src.OPERATEDATE
       ,DST.INPUTUSERID                                        --录入人编号:src.INPUTUSERID
       ,DST.INPUTORGID                                         --录入机构编号:src.INPUTORGID
       ,DST.INPUTDATE                                          --录入时间:src.INPUTDATE
       ,DST.UPDATEDATE                                         --更新时间:src.UPDATEDATE
       ,DST.OCCURDATE                                          --预授信起始时间:src.OCCURDATE
       ,DST.MATURITY                                           --预授信到期时间:src.MATURITY
       ,DST.TERMMONTH                                          --期限:src.TERMMONTH
       ,DST.PIGEONHOLEDATE                                     --归档时间:src.PIGEONHOLEDATE
       ,DST.TEMPSAVEFLAG                                       --暂存标志:src.TEMPSAVEFLAG
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.CERTTYPE                                           --证件类型:src.CERTTYPE
       ,DST.CERTID                                             --证件号码:src.CERTID
       ,DST.FAMILYID                                           --户籍号:src.FAMILYID
       ,DST.FLAG                                               --是否生效标志:src.FLAG
       ,DST.REGISTERFLAG                                       --是否手动登记:src.REGISTERFLAG
       ,DST.FINISHDATE                                         --失效时间:src.FINISHDATE
       ,DST.CREDITLEVEL                                        --实际用信等级:src.CREDITLEVEL
       ,DST.DELAYCREDITREASON                                  --待授信原因编号:src.DELAYCREDITREASON
       ,DST.APPROVEPERSONFLAG                                  --审批人身份标志:src.APPROVEPERSONFLAG
       ,DST.APPROVEFLAG                                        --审批状态:src.APPROVEFLAG
       ,DST.BELONGFLAG                                         --归属标志:src.BELONGFLAG
       ,DST.CREDITFLAG                                         --授信状态:src.CREDITFLAG
       ,DST.CREDITADJUST                                       --额度调整形态:src.CREDITADJUST
       ,DST.YEARAPPROVEFLAG                                    --本年度年审否:src.YEARAPPROVEFLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_LN_SUNCREDIT_APPLY DST 
   LEFT JOIN F_LN_SUNCREDIT_APPLY_INNTMP1 SRC 
     ON SRC.SERIALNO            = DST.SERIALNO 
  WHERE SRC.SERIALNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_SUNCREDIT_APPLY_INNTMP2 = sqlContext.sql(sql)
dfn="F_LN_SUNCREDIT_APPLY/"+V_DT+".parquet"
F_LN_SUNCREDIT_APPLY_INNTMP2=F_LN_SUNCREDIT_APPLY_INNTMP2.unionAll(F_LN_SUNCREDIT_APPLY_INNTMP1)
F_LN_SUNCREDIT_APPLY_INNTMP1.cache()
F_LN_SUNCREDIT_APPLY_INNTMP2.cache()
nrowsi = F_LN_SUNCREDIT_APPLY_INNTMP1.count()
nrowsa = F_LN_SUNCREDIT_APPLY_INNTMP2.count()
F_LN_SUNCREDIT_APPLY_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_LN_SUNCREDIT_APPLY_INNTMP1.unpersist()
F_LN_SUNCREDIT_APPLY_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_SUNCREDIT_APPLY lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_LN_SUNCREDIT_APPLY/"+V_DT_LD+".parquet /"+dbname+"/F_LN_SUNCREDIT_APPLY_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_SUNCREDIT_APPLY_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_SUNCREDIT_APPLY/"+V_DT+".parquet /"+dbname+"/F_LN_SUNCREDIT_APPLY_BK/"+V_DT+".parquet")
