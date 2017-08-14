#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CICIEADR').setMaster(sys.argv[2])
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

O_CI_CBOD_CICIEADR = sqlContext.read.parquet(hdfs+'/O_CI_CBOD_CICIEADR/*')
O_CI_CBOD_CICIEADR.registerTempTable("O_CI_CBOD_CICIEADR")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CI_CBOD_CICIEADR = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIEADR/*')
F_CI_CBOD_CICIEADR.registerTempTable("F_CI_CBOD_CICIEADR")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,FK_CICIF_KEY            AS FK_CICIF_KEY 
       ,CI_ADDR_COD             AS CI_ADDR_COD 
       ,CI_ADDR                 AS CI_ADDR 
       ,CI_POSTCOD              AS CI_POSTCOD 
       ,CI_TEL_NO               AS CI_TEL_NO 
       ,CI_CNTY_COD             AS CI_CNTY_COD 
       ,CI_AREA_COD             AS CI_AREA_COD 
       ,CI_SUB_TEL              AS CI_SUB_TEL 
       ,CI_MOBILE_PHONE         AS CI_MOBILE_PHONE 
       ,CI_EMAIL                AS CI_EMAIL 
       ,CI_WEBSITE              AS CI_WEBSITE 
       ,CI_CNNT_DESC            AS CI_CNNT_DESC 
       ,CI_CRT_SYS              AS CI_CRT_SYS 
       ,CI_CRT_SCT_N            AS CI_CRT_SCT_N 
       ,CI_CRT_OPR              AS CI_CRT_OPR 
       ,CI_UPD_SYS              AS CI_UPD_SYS 
       ,CI_UPD_OPR              AS CI_UPD_OPR 
       ,CI_CRT_ORG              AS CI_CRT_ORG 
       ,CI_UPD_ORG              AS CI_UPD_ORG 
       ,CI_DB_PART_ID           AS CI_DB_PART_ID 
       ,CI_INSTN_COD            AS CI_INSTN_COD 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CI_CBOD_CICIEADR A                                   --对公客户地址信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_CICIEADR_INNTMP1 = sqlContext.sql(sql)
F_CI_CBOD_CICIEADR_INNTMP1.registerTempTable("F_CI_CBOD_CICIEADR_INNTMP1")

#F_CI_CBOD_CICIEADR = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIEADR/*')
#F_CI_CBOD_CICIEADR.registerTempTable("F_CI_CBOD_CICIEADR")
sql = """
 SELECT DST.ETLDT                                               --系统日期:src.ETLDT
       ,DST.FK_CICIF_KEY                                       --FK_CICIF_KEY:src.FK_CICIF_KEY
       ,DST.CI_ADDR_COD                                        --地址代码(COD):src.CI_ADDR_COD
       ,DST.CI_ADDR                                            --地址(ADDR):src.CI_ADDR
       ,DST.CI_POSTCOD                                         --邮政编码:src.CI_POSTCOD
       ,DST.CI_TEL_NO                                          --电话号码:src.CI_TEL_NO
       ,DST.CI_CNTY_COD                                        --国家代码(CNTY-4位):src.CI_CNTY_COD
       ,DST.CI_AREA_COD                                        --区号1:src.CI_AREA_COD
       ,DST.CI_SUB_TEL                                         --分机(TEL):src.CI_SUB_TEL
       ,DST.CI_MOBILE_PHONE                                    --移动电话:src.CI_MOBILE_PHONE
       ,DST.CI_EMAIL                                           --电子邮箱:src.CI_EMAIL
       ,DST.CI_WEBSITE                                         --网址:src.CI_WEBSITE
       ,DST.CI_CNNT_DESC                                       --联系描述:src.CI_CNNT_DESC
       ,DST.CI_CRT_SYS                                         --创建系统:src.CI_CRT_SYS
       ,DST.CI_CRT_SCT_N                                       --创建时间(8位):src.CI_CRT_SCT_N
       ,DST.CI_CRT_OPR                                         --创建人:src.CI_CRT_OPR
       ,DST.CI_UPD_SYS                                         --更新系统:src.CI_UPD_SYS
       ,DST.CI_UPD_OPR                                         --更新人:src.CI_UPD_OPR
       ,DST.CI_CRT_ORG                                         --设置机构:src.CI_CRT_ORG
       ,DST.CI_UPD_ORG                                         --更新机构:src.CI_UPD_ORG
       ,DST.CI_DB_PART_ID                                      --分区键:src.CI_DB_PART_ID
       ,DST.CI_INSTN_COD                                       --机构代号:src.CI_INSTN_COD
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_CI_CBOD_CICIEADR DST 
   LEFT JOIN F_CI_CBOD_CICIEADR_INNTMP1 SRC 
     ON SRC.FK_CICIF_KEY        = DST.FK_CICIF_KEY 
    AND SRC.CI_ADDR_COD         = DST.CI_ADDR_COD 
  WHERE SRC.FK_CICIF_KEY IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_CICIEADR_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_CBOD_CICIEADR/"+V_DT+".parquet"
F_CI_CBOD_CICIEADR_INNTMP2=F_CI_CBOD_CICIEADR_INNTMP2.unionAll(F_CI_CBOD_CICIEADR_INNTMP1)
F_CI_CBOD_CICIEADR_INNTMP1.cache()
F_CI_CBOD_CICIEADR_INNTMP2.cache()
nrowsi = F_CI_CBOD_CICIEADR_INNTMP1.count()
nrowsa = F_CI_CBOD_CICIEADR_INNTMP2.count()
F_CI_CBOD_CICIEADR_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_CBOD_CICIEADR_INNTMP1.unpersist()
F_CI_CBOD_CICIEADR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CBOD_CICIEADR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_CBOD_CICIEADR/"+V_DT_LD+".parquet /"+dbname+"/F_CI_CBOD_CICIEADR_BK/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_CICIEADR/"+V_DT_LD+".parquet")