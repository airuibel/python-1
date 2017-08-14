#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_CUSTOMER_SPECIAL').setMaster(sys.argv[2])
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

O_CI_XDXT_CUSTOMER_SPECIAL = sqlContext.read.parquet(hdfs+'/O_CI_XDXT_CUSTOMER_SPECIAL/*')
O_CI_XDXT_CUSTOMER_SPECIAL.registerTempTable("O_CI_XDXT_CUSTOMER_SPECIAL")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_CUSTOMER_SPECIAL/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_CUSTOMER_SPECIAL_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_CUSTOMER_SPECIAL/"+V_DT+".parquet")


F_CI_XDXT_CUSTOMER_SPECIAL = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_SPECIAL/*')
F_CI_XDXT_CUSTOMER_SPECIAL.registerTempTable("F_CI_XDXT_CUSTOMER_SPECIAL")

sql = """
 SELECT A.SERIALNO              AS SERIALNO 
       ,A.CUSTOMERNAME          AS CUSTOMERNAME 
       ,A.ORGNATURE             AS ORGNATURE 
       ,A.INLISTREASON          AS INLISTREASON 
       ,A.INLISTSTATUS          AS INLISTSTATUS 
       ,A.BEGINDATE             AS BEGINDATE 
       ,A.ENDDATE               AS ENDDATE 
       ,A.SUM1                  AS SUM1 
       ,A.SUM2                  AS SUM2 
       ,A.ATTRIBUTE1            AS ATTRIBUTE1 
       ,A.ATTRIBUTE2            AS ATTRIBUTE2 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.REMARK                AS REMARK 
       ,A.SPECIALTYPE           AS SPECIALTYPE 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CERTID                AS CERTID 
       ,A.CUSTOMERID            AS CUSTOMERID 
       ,A.SECTIONTYPE           AS SECTIONTYPE 
       ,A.ATTRIBUTE3            AS ATTRIBUTE3 
       ,A.BLACKSHEETORNOT       AS BLACKSHEETORNOT 
       ,A.CONFIRMORNOT          AS CONFIRMORNOT 
       ,A.CORPORATEORGID        AS CORPORATEORGID 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_CI_XDXT_CUSTOMER_SPECIAL A                           --特定客户名单表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP1 = sqlContext.sql(sql)
F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP1.registerTempTable("F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP1")

#F_CI_XDXT_CUSTOMER_SPECIAL = sqlContext.read.parquet(hdfs+'/F_CI_XDXT_CUSTOMER_SPECIAL/*')
#F_CI_XDXT_CUSTOMER_SPECIAL.registerTempTable("F_CI_XDXT_CUSTOMER_SPECIAL")
sql = """
 SELECT DST.SERIALNO                                            --流水号:src.SERIALNO
       ,DST.CUSTOMERNAME                                       --客户名称:src.CUSTOMERNAME
       ,DST.ORGNATURE                                          --机构类型:src.ORGNATURE
       ,DST.INLISTREASON                                       --列入原因:src.INLISTREASON
       ,DST.INLISTSTATUS                                       --状态标志:src.INLISTSTATUS
       ,DST.BEGINDATE                                          --起始日期:src.BEGINDATE
       ,DST.ENDDATE                                            --结束时间:src.ENDDATE
       ,DST.SUM1                                               --金额1:src.SUM1
       ,DST.SUM2                                               --金额2:src.SUM2
       ,DST.ATTRIBUTE1                                         --集合属性1:src.ATTRIBUTE1
       ,DST.ATTRIBUTE2                                         --集合属性2:src.ATTRIBUTE2
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTDATE                                          --输入日期:src.INPUTDATE
       ,DST.REMARK                                             --说明:src.REMARK
       ,DST.SPECIALTYPE                                        --特殊类型:src.SPECIALTYPE
       ,DST.CERTTYPE                                           --证件类型:src.CERTTYPE
       ,DST.CERTID                                             --证件号:src.CERTID
       ,DST.CUSTOMERID                                         --客户号:src.CUSTOMERID
       ,DST.SECTIONTYPE                                        --特定客户类型:src.SECTIONTYPE
       ,DST.ATTRIBUTE3                                         --集合属性3:src.ATTRIBUTE3
       ,DST.BLACKSHEETORNOT                                    --是否黑名当客户:src.BLACKSHEETORNOT
       ,DST.CONFIRMORNOT                                       --是否生效:src.CONFIRMORNOT
       ,DST.CORPORATEORGID                                     --法人机构号:src.CORPORATEORGID
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_CI_XDXT_CUSTOMER_SPECIAL DST 
   LEFT JOIN F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP1 SRC 
     ON SRC.SERIALNO            = DST.SERIALNO 
  WHERE SRC.SERIALNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_XDXT_CUSTOMER_SPECIAL/"+V_DT+".parquet"
F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP2=F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP2.unionAll(F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP1)
F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP1.cache()
F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP2.cache()
nrowsi = F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP1.count()
nrowsa = F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP2.count()
F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP1.unpersist()
F_CI_XDXT_CUSTOMER_SPECIAL_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_XDXT_CUSTOMER_SPECIAL lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_XDXT_CUSTOMER_SPECIAL/"+V_DT_LD+".parquet /"+dbname+"/F_CI_XDXT_CUSTOMER_SPECIAL_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_XDXT_CUSTOMER_SPECIAL_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_XDXT_CUSTOMER_SPECIAL/"+V_DT+".parquet /"+dbname+"/F_CI_XDXT_CUSTOMER_SPECIAL_BK/"+V_DT+".parquet")
