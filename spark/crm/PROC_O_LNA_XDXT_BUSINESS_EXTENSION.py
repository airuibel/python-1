#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_BUSINESS_EXTENSION').setMaster(sys.argv[2])
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

O_LN_XDXT_BUSINESS_EXTENSION = sqlContext.read.parquet(hdfs+'/O_LN_XDXT_BUSINESS_EXTENSION/*')
O_LN_XDXT_BUSINESS_EXTENSION.registerTempTable("O_LN_XDXT_BUSINESS_EXTENSION")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_BUSINESS_EXTENSION/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_BUSINESS_EXTENSION_BK/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_BUSINESS_EXTENSION/"+V_DT+".parquet")


F_LN_XDXT_BUSINESS_EXTENSION = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BUSINESS_EXTENSION/*')
F_LN_XDXT_BUSINESS_EXTENSION.registerTempTable("F_LN_XDXT_BUSINESS_EXTENSION")

sql = """
 SELECT A.SERIALNO              AS SERIALNO 
       ,A.RELATIVESERIALNO      AS RELATIVESERIALNO 
       ,A.TRANSACTIONFLAG       AS TRANSACTIONFLAG 
       ,A.OCCURDATE             AS OCCURDATE 
       ,A.OCCURTIME             AS OCCURTIME 
       ,A.LASTRATE              AS LASTRATE 
       ,A.LASTMATURITY          AS LASTMATURITY 
       ,A.EXTENSIONSUM          AS EXTENSIONSUM 
       ,A.LASTSUM               AS LASTSUM 
       ,A.EXTENDTERMYEAR        AS EXTENDTERMYEAR 
       ,A.EXTENDTERMMONTH       AS EXTENDTERMMONTH 
       ,A.EXTENDTERMDAY         AS EXTENDTERMDAY 
       ,A.EXTENDRATE            AS EXTENDRATE 
       ,A.EXTENDMATURITY        AS EXTENDMATURITY 
       ,A.VOUCHERNO             AS VOUCHERNO 
       ,A.ORGID                 AS ORGID 
       ,A.USERID                AS USERID 
       ,A.EXTENDFLAG            AS EXTENDFLAG 
       ,A.REMARK                AS REMARK 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
   FROM O_LN_XDXT_BUSINESS_EXTENSION A                         --业务展期记录
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_BUSINESS_EXTENSION_INNTMP1 = sqlContext.sql(sql)
F_LN_XDXT_BUSINESS_EXTENSION_INNTMP1.registerTempTable("F_LN_XDXT_BUSINESS_EXTENSION_INNTMP1")

#F_LN_XDXT_BUSINESS_EXTENSION = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BUSINESS_EXTENSION/*')
#F_LN_XDXT_BUSINESS_EXTENSION.registerTempTable("F_LN_XDXT_BUSINESS_EXTENSION")
sql = """
 SELECT DST.SERIALNO                                            --信息流水号:src.SERIALNO
       ,DST.RELATIVESERIALNO                                   --相关借据号:src.RELATIVESERIALNO
       ,DST.TRANSACTIONFLAG                                    --交易标志:src.TRANSACTIONFLAG
       ,DST.OCCURDATE                                          --发生日期:src.OCCURDATE
       ,DST.OCCURTIME                                          --发生时间:src.OCCURTIME
       ,DST.LASTRATE                                           --原利率:src.LASTRATE
       ,DST.LASTMATURITY                                       --原到期日:src.LASTMATURITY
       ,DST.EXTENSIONSUM                                       --展期金额:src.EXTENSIONSUM
       ,DST.LASTSUM                                            --展期前金额:src.LASTSUM
       ,DST.EXTENDTERMYEAR                                     --展期期限年:src.EXTENDTERMYEAR
       ,DST.EXTENDTERMMONTH                                    --展期期限月:src.EXTENDTERMMONTH
       ,DST.EXTENDTERMDAY                                      --展期期限日:src.EXTENDTERMDAY
       ,DST.EXTENDRATE                                         --展期利率:src.EXTENDRATE
       ,DST.EXTENDMATURITY                                     --展期后到期日:src.EXTENDMATURITY
       ,DST.VOUCHERNO                                          --凭证号码:src.VOUCHERNO
       ,DST.ORGID                                              --创建机构:src.ORGID
       ,DST.USERID                                             --操作员:src.USERID
       ,DST.EXTENDFLAG                                         --更新标志:src.EXTENDFLAG
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
   FROM F_LN_XDXT_BUSINESS_EXTENSION DST 
   LEFT JOIN F_LN_XDXT_BUSINESS_EXTENSION_INNTMP1 SRC 
     ON SRC.SERIALNO            = DST.SERIALNO 
  WHERE SRC.SERIALNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_BUSINESS_EXTENSION_INNTMP2 = sqlContext.sql(sql)
dfn="F_LN_XDXT_BUSINESS_EXTENSION/"+V_DT+".parquet"
F_LN_XDXT_BUSINESS_EXTENSION_INNTMP2=F_LN_XDXT_BUSINESS_EXTENSION_INNTMP2.unionAll(F_LN_XDXT_BUSINESS_EXTENSION_INNTMP1)
F_LN_XDXT_BUSINESS_EXTENSION_INNTMP1.cache()
F_LN_XDXT_BUSINESS_EXTENSION_INNTMP2.cache()
nrowsi = F_LN_XDXT_BUSINESS_EXTENSION_INNTMP1.count()
nrowsa = F_LN_XDXT_BUSINESS_EXTENSION_INNTMP2.count()
F_LN_XDXT_BUSINESS_EXTENSION_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_LN_XDXT_BUSINESS_EXTENSION_INNTMP1.unpersist()
F_LN_XDXT_BUSINESS_EXTENSION_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_XDXT_BUSINESS_EXTENSION lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_LN_XDXT_BUSINESS_EXTENSION/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_BUSINESS_EXTENSION_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_BUSINESS_EXTENSION_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_BUSINESS_EXTENSION/"+V_DT+".parquet /"+dbname+"/F_LN_XDXT_BUSINESS_EXTENSION_BK/"+V_DT+".parquet")
