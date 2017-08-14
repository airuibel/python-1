#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_CUSTOMER_INFO').setMaster(sys.argv[2])
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

O_CI_XDXT_CUSTOMER_INFO = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_CUSTOMER_INFO/*')
O_CI_XDXT_CUSTOMER_INFO.registerTempTable("O_CI_XDXT_CUSTOMER_INFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_CUSTOMER_INFO/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_CUSTOMER_INFO_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_CUSTOMER_INFO/"+V_DT+".parquet")


F_CI_XDXT_CUSTOMER_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_INFO/*')
F_CI_XDXT_CUSTOMER_INFO.registerTempTable("F_CI_XDXT_CUSTOMER_INFO")

sql = """
 SELECT A.CUSTOMERID            AS CUSTOMERID 
       ,A.CUSTOMERNAME          AS CUSTOMERNAME 
       ,A.CUSTOMERTYPE          AS CUSTOMERTYPE 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CERTID                AS CERTID 
       ,A.CUSTOMERPASSWORD      AS CUSTOMERPASSWORD 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.REMARK                AS REMARK 
       ,A.MFCUSTOMERID          AS MFCUSTOMERID 
       ,A.STATUS                AS STATUS 
       ,A.BELONGGROUPID         AS BELONGGROUPID 
       ,A.CHANNEL               AS CHANNEL 
       ,A.LOANCARDNO            AS LOANCARDNO 
       ,A.CUSTOMERSCALE         AS CUSTOMERSCALE 
       ,A.CORPORATEORGID        AS CORPORATEORGID 
       ,A.REMEDYFLAG            AS REMEDYFLAG 
       ,A.DRAWFLAG              AS DRAWFLAG 
       ,A.MANAGERUSERID         AS MANAGERUSERID 
       ,A.MANAGERORGID          AS MANAGERORGID 
       ,A.DRAWELIGIBILITY       AS DRAWELIGIBILITY 
       ,A.BLACKSHEETORNOT       AS BLACKSHEETORNOT 
       ,A.CONFIRMORNOT          AS CONFIRMORNOT 
       ,A.CLIENTCLASSN          AS CLIENTCLASSN 
       ,A.CLIENTCLASSM          AS CLIENTCLASSM 
       ,A.BUSINESSSTATE         AS BUSINESSSTATE 
       ,A.MASTERBALANCE         AS MASTERBALANCE 
       ,A.UPDATEDATE            AS UPDATEDATE 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_CUSTOMER_INFO A                              --客户基本信息
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_CUSTOMER_INFO_INNTMP1 = sqlContext.sql(sql)
F_CI_XDXT_CUSTOMER_INFO_INNTMP1.registerTempTable("F_CI_XDXT_CUSTOMER_INFO_INNTMP1")

#F_CI_XDXT_CUSTOMER_INFO = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_INFO/*')
#F_CI_XDXT_CUSTOMER_INFO.registerTempTable("F_CI_XDXT_CUSTOMER_INFO")
sql = """
 SELECT DST.CUSTOMERID                                          --客户编号:src.CUSTOMERID
       ,DST.CUSTOMERNAME                                       --客户名称:src.CUSTOMERNAME
       ,DST.CUSTOMERTYPE                                       --客户类型:src.CUSTOMERTYPE
       ,DST.CERTTYPE                                           --证件类型:src.CERTTYPE
       ,DST.CERTID                                             --证件号:src.CERTID
       ,DST.CUSTOMERPASSWORD                                   --客户口令:src.CUSTOMERPASSWORD
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTDATE                                          --登记日期:src.INPUTDATE
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.MFCUSTOMERID                                       --核心客户号:src.MFCUSTOMERID
       ,DST.STATUS                                             --状态:src.STATUS
       ,DST.BELONGGROUPID                                      --所属关联集团代码:src.BELONGGROUPID
       ,DST.CHANNEL                                            --渠道:src.CHANNEL
       ,DST.LOANCARDNO                                         --贷款卡编号:src.LOANCARDNO
       ,DST.CUSTOMERSCALE                                      --客户规模:src.CUSTOMERSCALE
       ,DST.CORPORATEORGID                                     --法人机构号:src.CORPORATEORGID
       ,DST.REMEDYFLAG                                         --补登标志:src.REMEDYFLAG
       ,DST.DRAWFLAG                                           --领取标志:src.DRAWFLAG
       ,DST.MANAGERUSERID                                      --管户人:src.MANAGERUSERID
       ,DST.MANAGERORGID                                       --管户机构ID:src.MANAGERORGID
       ,DST.DRAWELIGIBILITY                                    --领取信息:src.DRAWELIGIBILITY
       ,DST.BLACKSHEETORNOT                                    --是否黑名当客户:src.BLACKSHEETORNOT
       ,DST.CONFIRMORNOT                                       --是否生效:src.CONFIRMORNOT
       ,DST.CLIENTCLASSN                                       --当前客户分类:src.CLIENTCLASSN
       ,DST.CLIENTCLASSM                                       --客户分类调整:src.CLIENTCLASSM
       ,DST.BUSINESSSTATE                                      --存量字段标志:src.BUSINESSSTATE
       ,DST.MASTERBALANCE                                      --单户余额:src.MASTERBALANCE
       ,DST.UPDATEDATE                                         --更新日期:src.UPDATEDATE
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
   FROM F_CI_XDXT_CUSTOMER_INFO DST 
   LEFT JOIN F_CI_XDXT_CUSTOMER_INFO_INNTMP1 SRC 
     ON SRC.CUSTOMERID          = DST.CUSTOMERID 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CUSTOMERID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_CUSTOMER_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_XDXT_CUSTOMER_INFO/"+V_DT+".parquet"
F_CI_XDXT_CUSTOMER_INFO_INNTMP2=F_CI_XDXT_CUSTOMER_INFO_INNTMP2.unionAll(F_CI_XDXT_CUSTOMER_INFO_INNTMP1)
F_CI_XDXT_CUSTOMER_INFO_INNTMP1.cache()
F_CI_XDXT_CUSTOMER_INFO_INNTMP2.cache()
nrowsi = F_CI_XDXT_CUSTOMER_INFO_INNTMP1.count()
nrowsa = F_CI_XDXT_CUSTOMER_INFO_INNTMP2.count()
F_CI_XDXT_CUSTOMER_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_CUSTOMER_INFO_INNTMP1.unpersist()
F_CI_XDXT_CUSTOMER_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_CUSTOMER_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_XDXT_CUSTOMER_INFO/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_CUSTOMER_INFO_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_CUSTOMER_INFO_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_CUSTOMER_INFO/"+V_DT+".parquet /"+dbname+"/F_CI_XDXT_CUSTOMER_INFO_BK/"+V_DT+".parquet")
