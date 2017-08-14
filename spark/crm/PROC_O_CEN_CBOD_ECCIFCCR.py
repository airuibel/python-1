#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_ECCIFCCR').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_ECCIFCCR/"+V_DT_LD+".parquet")
F_CI_CBOD_ECCIFCCR = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCIFCCR_BK/'+V_DT_LD+'.parquet/*')
F_CI_CBOD_ECCIFCCR.registerTempTable("F_CI_CBOD_ECCIFCCR")

#任务[12] 001-01::
V_STEP = V_STEP + 1

O_CI_CBOD_ECCIFCCR = sqlContext.read.parquet(hdfs+'/O_CI_CBOD_ECCIFCCR/*')
O_CI_CBOD_ECCIFCCR.registerTempTable("O_CI_CBOD_ECCIFCCR")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,EC_BNK_NO               AS EC_BNK_NO 
       ,EC_SEQ_NO               AS EC_SEQ_NO 
       ,EC_CUST_ID_A            AS EC_CUST_ID_A 
       ,EC_CUST_TYP_A           AS EC_CUST_TYP_A 
       ,EC_CUST_ID_B            AS EC_CUST_ID_B 
       ,EC_CUST_TYP_B           AS EC_CUST_TYP_B 
       ,EC_REL_TYP              AS EC_REL_TYP 
       ,EC_REL_DESC             AS EC_REL_DESC 
       ,EC_CCR_FRDAT_N          AS EC_CCR_FRDAT_N 
       ,EC_CCR_TODAT_N          AS EC_CCR_TODAT_N 
       ,EC_CRT_SYS              AS EC_CRT_SYS 
       ,EC_CRT_SCT_N            AS EC_CRT_SCT_N 
       ,EC_CRT_OPR              AS EC_CRT_OPR 
       ,EC_UPD_SYS              AS EC_UPD_SYS 
       ,EC_UPD_OPR              AS EC_UPD_OPR 
       ,EC_CRT_ORG              AS EC_CRT_ORG 
       ,EC_UPD_ORG              AS EC_UPD_ORG 
       ,EC_DB_PART_ID           AS EC_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CI_CBOD_ECCIFCCR A                                   --客户关系信息档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_ECCIFCCR_INNTMP1 = sqlContext.sql(sql)
F_CI_CBOD_ECCIFCCR_INNTMP1.registerTempTable("F_CI_CBOD_ECCIFCCR_INNTMP1")

#F_CI_CBOD_ECCIFCCR = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCIFCCR/*')
#F_CI_CBOD_ECCIFCCR.registerTempTable("F_CI_CBOD_ECCIFCCR")
sql = """
 SELECT DST.ETLDT                                               --平台日期:src.ETLDT
       ,DST.EC_BNK_NO                                          --法人银行号:src.EC_BNK_NO
       ,DST.EC_SEQ_NO                                          --ECIF顺序号:src.EC_SEQ_NO
       ,DST.EC_CUST_ID_A                                       --关系开始客户ID:src.EC_CUST_ID_A
       ,DST.EC_CUST_TYP_A                                      --关系开始客户类型:src.EC_CUST_TYP_A
       ,DST.EC_CUST_ID_B                                       --关系结束客户ID:src.EC_CUST_ID_B
       ,DST.EC_CUST_TYP_B                                      --关系结束客户类型:src.EC_CUST_TYP_B
       ,DST.EC_REL_TYP                                         --关系类型代码:src.EC_REL_TYP
       ,DST.EC_REL_DESC                                        --关系描述(REL):src.EC_REL_DESC
       ,DST.EC_CCR_FRDAT_N                                     --关系建立日期:src.EC_CCR_FRDAT_N
       ,DST.EC_CCR_TODAT_N                                     --关系终止日期:src.EC_CCR_TODAT_N
       ,DST.EC_CRT_SYS                                         --创建系统:src.EC_CRT_SYS
       ,DST.EC_CRT_SCT_N                                       --创建时间(8位):src.EC_CRT_SCT_N
       ,DST.EC_CRT_OPR                                         --创建人:src.EC_CRT_OPR
       ,DST.EC_UPD_SYS                                         --更新系统:src.EC_UPD_SYS
       ,DST.EC_UPD_OPR                                         --更新人:src.EC_UPD_OPR
       ,DST.EC_CRT_ORG                                         --设置机构:src.EC_CRT_ORG
       ,DST.EC_UPD_ORG                                         --更新机构:src.EC_UPD_ORG
       ,DST.EC_DB_PART_ID                                      --分区键:src.EC_DB_PART_ID
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_CI_CBOD_ECCIFCCR DST 
   LEFT JOIN F_CI_CBOD_ECCIFCCR_INNTMP1 SRC 
     ON SRC.EC_BNK_NO           = DST.EC_BNK_NO 
    AND SRC.EC_SEQ_NO           = DST.EC_SEQ_NO 
  WHERE SRC.EC_BNK_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_ECCIFCCR_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_CBOD_ECCIFCCR/"+V_DT+".parquet"
F_CI_CBOD_ECCIFCCR_INNTMP2=F_CI_CBOD_ECCIFCCR_INNTMP2.unionAll(F_CI_CBOD_ECCIFCCR_INNTMP1)
F_CI_CBOD_ECCIFCCR_INNTMP1.cache()
F_CI_CBOD_ECCIFCCR_INNTMP2.cache()
nrowsi = F_CI_CBOD_ECCIFCCR_INNTMP1.count()
nrowsa = F_CI_CBOD_ECCIFCCR_INNTMP2.count()
F_CI_CBOD_ECCIFCCR_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_CBOD_ECCIFCCR_INNTMP1.unpersist()
F_CI_CBOD_ECCIFCCR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CBOD_ECCIFCCR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_ECCIFCCR_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CI_CBOD_ECCIFCCR/"+V_DT+".parquet /"+dbname+"/F_CI_CBOD_ECCIFCCR_BK/")
