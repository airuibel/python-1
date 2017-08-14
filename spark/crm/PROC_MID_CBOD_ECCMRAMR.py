#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MID_CBOD_ECCMRAMR').setMaster(sys.argv[2])
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

F_CM_CBOD_ECCMRAMR = sqlContext.read.parquet(hdfs+'/F_CM_CBOD_ECCMRAMR/*')
F_CM_CBOD_ECCMRAMR.registerTempTable("F_CM_CBOD_ECCMRAMR")


#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT ''                      AS ETLDT 
       ,CAST(0                       AS DECIMAL(4))                       AS ECAMR_LL 
       ,''                      AS EC_BNK_NO 
       ,MAX(EC_SEQ_NO)                       AS EC_SEQ_NO 
       ,CAST(0                       AS DECIMAL(15))                       AS ECAMR_DB_TIMESTAMP 
       ,SUBSTR(A.EC_CMS_ID, 4, 17)                       AS EC_CMS_ID 
       ,EC_ACCT_NO              AS EC_ACCT_NO 
       ,''                      AS EC_CUST_TYP 
       ,CAST(0                       AS DECIMAL(8, 5))                       AS EC_ACN_PERC 
       ,''                      AS EC_CRT_SYS 
       ,CAST(EC_CRT_SCT_N AS DECIMAL(8))           AS EC_CRT_SCT_N 
       ,''                      AS EC_CRT_OPR 
       ,''                      AS EC_UPD_SYS 
       ,''                      AS EC_UPD_OPR 
       ,''                      AS EC_CRT_ORG 
       ,''                      AS EC_UPD_ORG 
       ,''                      AS EC_DB_PART_ID 
       ,''                      AS ODS_ST_DATE 
       ,''                      AS ODS_SYS_ID 
       ,FR_ID                   AS FR_ID 
   FROM F_CM_CBOD_ECCMRAMR A                       
  WHERE A.EC_CRT_SCT_N          =(
         SELECT MAX(EC_CRT_SCT_N) 
           FROM F_CM_CBOD_ECCMRAMR B 
          WHERE B.EC_ACCT_NO            = A.EC_ACCT_NO 
            AND A.FR_ID                 = B.FR_ID) 
  GROUP BY FR_ID 
       ,EC_CRT_SCT_N 
       ,EC_ACCT_NO 
       ,EC_CMS_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_CBOD_ECCMRAMR = sqlContext.sql(sql)
MID_CBOD_ECCMRAMR.registerTempTable("MID_CBOD_ECCMRAMR")
dfn="MID_CBOD_ECCMRAMR/"+V_DT+".parquet"
MID_CBOD_ECCMRAMR.cache()
nrows = MID_CBOD_ECCMRAMR.count() 
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_CBOD_ECCMRAMR/*")
MID_CBOD_ECCMRAMR.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_CBOD_ECCMRAMR.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_CBOD_ECCMRAMR lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1

MID_CBOD_ECCMRAMR = sqlContext.read.parquet(hdfs+'/MID_CBOD_ECCMRAMR/'+V_DT+'.parquet')
MID_CBOD_ECCMRAMR.registerTempTable("MID_CBOD_ECCMRAMR")

sql = """
 SELECT B.ETLDT                 AS ETLDT 
       ,B.ECAMR_LL              AS ECAMR_LL 
       ,B.EC_BNK_NO             AS EC_BNK_NO 
       ,B.EC_SEQ_NO             AS EC_SEQ_NO 
       ,B.ECAMR_DB_TIMESTAMP    AS ECAMR_DB_TIMESTAMP 
       ,A.EC_CMS_ID             AS EC_CMS_ID 
       ,B.EC_ACCT_NO            AS EC_ACCT_NO 
       ,B.EC_CUST_TYP           AS EC_CUST_TYP 
       ,B.EC_ACN_PERC           AS EC_ACN_PERC 
       ,B.EC_CRT_SYS            AS EC_CRT_SYS 
       ,B.EC_CRT_SCT_N          AS EC_CRT_SCT_N 
       ,B.EC_CRT_OPR            AS EC_CRT_OPR 
       ,B.EC_UPD_SYS            AS EC_UPD_SYS 
       ,B.EC_UPD_OPR            AS EC_UPD_OPR 
       ,B.EC_CRT_ORG            AS EC_CRT_ORG 
       ,B.EC_UPD_ORG            AS EC_UPD_ORG 
       ,B.EC_DB_PART_ID         AS EC_DB_PART_ID 
       ,B.ODS_ST_DATE           AS ODS_ST_DATE 
       ,B.ODS_SYS_ID            AS ODS_SYS_ID 
       ,B.FR_ID                 AS FR_ID 
   FROM F_CM_CBOD_ECCMRAMR A                                   --客户经理与账户关系档
  INNER JOIN MID_CBOD_ECCMRAMR B                               --
     ON A.EC_CRT_SCT_N          = B.EC_CRT_SCT_N 
    AND A.EC_ACCT_NO            = B.EC_ACCT_NO 
    AND A.EC_SEQ_NO             = B.EC_SEQ_NO 
    AND B.FR_ID                 = A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_CBOD_ECCMRAMR_INNTMP1 = sqlContext.sql(sql)
MID_CBOD_ECCMRAMR_INNTMP1.registerTempTable("MID_CBOD_ECCMRAMR_INNTMP1")

sql = """
 SELECT DST.ETLDT                                               --平台日期:src.ETLDT
       ,DST.ECAMR_LL                                           --ECAMR_LL:src.ECAMR_LL
       ,DST.EC_BNK_NO                                          --法人银行号:src.EC_BNK_NO
       ,DST.EC_SEQ_NO                                          --ECIF顺序号:src.EC_SEQ_NO
       ,DST.ECAMR_DB_TIMESTAMP                                 --时间戳:src.ECAMR_DB_TIMESTAMP
       ,DST.EC_CMS_ID                                          --客户经理管辖范围代理键:src.EC_CMS_ID
       ,DST.EC_ACCT_NO                                         --资金账号:src.EC_ACCT_NO
       ,DST.EC_CUST_TYP                                        --客户类别:src.EC_CUST_TYP
       ,DST.EC_ACN_PERC                                        --业绩占比:src.EC_ACN_PERC
       ,DST.EC_CRT_SYS                                         --创建系统:src.EC_CRT_SYS
       ,DST.EC_CRT_SCT_N                                       --创建时间(8位):src.EC_CRT_SCT_N
       ,DST.EC_CRT_OPR                                         --创建人:src.EC_CRT_OPR
       ,DST.EC_UPD_SYS                                         --更新系统:src.EC_UPD_SYS
       ,DST.EC_UPD_OPR                                         --更新人:src.EC_UPD_OPR
       ,DST.EC_CRT_ORG                                         --设置机构:src.EC_CRT_ORG
       ,DST.EC_UPD_ORG                                         --更新机构:src.EC_UPD_ORG
       ,DST.EC_DB_PART_ID                                      --部门编号:src.EC_DB_PART_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM MID_CBOD_ECCMRAMR DST 
   LEFT JOIN MID_CBOD_ECCMRAMR_INNTMP1 SRC 
     ON SRC.EC_CRT_SCT_N        = DST.EC_CRT_SCT_N 
    AND SRC.EC_ACCT_NO          = DST.EC_ACCT_NO 
    AND SRC.EC_SEQ_NO           = DST.EC_SEQ_NO 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.EC_CRT_SCT_N IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_CBOD_ECCMRAMR_INNTMP2 = sqlContext.sql(sql)
dfn="MID_CBOD_ECCMRAMR/"+V_DT+".parquet"
MID_CBOD_ECCMRAMR_INNTMP2.unionAll(MID_CBOD_ECCMRAMR_INNTMP1)
MID_CBOD_ECCMRAMR_INNTMP1.cache()
MID_CBOD_ECCMRAMR_INNTMP2.cache()
nrowsi = MID_CBOD_ECCMRAMR_INNTMP1.count()
nrowsa = MID_CBOD_ECCMRAMR_INNTMP2.count()
MID_CBOD_ECCMRAMR_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite') 
MID_CBOD_ECCMRAMR_INNTMP1.unpersist()
MID_CBOD_ECCMRAMR_INNTMP2.unpersist() 
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_CBOD_ECCMRAMR_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/MID_CBOD_ECCMRAMR/"+V_DT+".parquet  /"+dbname+"/MID_CBOD_ECCMRAMR_BK/")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_CBOD_ECCMRAMR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/MID_CBOD_ECCMRAMR/"+V_DT_LD+".parquet /"+dbname+"/MID_CBOD_ECCMRAMR_BK/")
