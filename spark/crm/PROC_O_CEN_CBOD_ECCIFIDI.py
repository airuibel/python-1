#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_ECCIFIDI').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_ECCIFIDI/"+V_DT_LD+".parquet")
O_CI_CBOD_ECCIFIDI = sqlContext.read.parquet(hdfs+'/O_CI_CBOD_ECCIFIDI/*')
O_CI_CBOD_ECCIFIDI.registerTempTable("O_CI_CBOD_ECCIFIDI")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CI_CBOD_ECCIFIDI = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCIFIDI_BK/'+V_DT_LD+'.parquet/*')
F_CI_CBOD_ECCIFIDI.registerTempTable("F_CI_CBOD_ECCIFIDI")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,EC_BNK_NO               AS EC_BNK_NO 
       ,EC_SEQ_NO               AS EC_SEQ_NO 
       ,EC_CER_TYP              AS EC_CER_TYP 
       ,EC_CER_NO               AS EC_CER_NO 
       ,EC_FULL_NAM             AS EC_FULL_NAM 
       ,EC_CUST_NO              AS EC_CUST_NO 
       ,EC_CUST_TYP             AS EC_CUST_TYP 
       ,EC_CER_EXPD_DT_N        AS EC_CER_EXPD_DT_N 
       ,EC_ID_CHK_FLG           AS EC_ID_CHK_FLG 
       ,EC_CHK_CHANNEL          AS EC_CHK_CHANNEL 
       ,EC_CHK_ER               AS EC_CHK_ER 
       ,EC_CHK_DT_N             AS EC_CHK_DT_N 
       ,EC_PRI_CER_FLG          AS EC_PRI_CER_FLG 
       ,EC_CER_STS              AS EC_CER_STS 
       ,EC_NAM_TYP              AS EC_NAM_TYP 
       ,EC_CRT_SYS              AS EC_CRT_SYS 
       ,EC_CRT_SCT_N            AS EC_CRT_SCT_N 
       ,EC_CRT_OPR              AS EC_CRT_OPR 
       ,EC_CRT_ORG              AS EC_CRT_ORG 
       ,EC_UPD_SYS              AS EC_UPD_SYS 
       ,EC_UPD_OPR              AS EC_UPD_OPR 
       ,EC_UPD_ORG              AS EC_UPD_ORG 
       ,EC_DB_PART_ID           AS EC_DB_PART_ID 
       ,EC_INSTN_COD            AS EC_INSTN_COD 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CI_CBOD_ECCIFIDI A                                   --客户识别信息档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_ECCIFIDI_INNTMP1 = sqlContext.sql(sql)
F_CI_CBOD_ECCIFIDI_INNTMP1.registerTempTable("F_CI_CBOD_ECCIFIDI_INNTMP1")

#F_CI_CBOD_ECCIFIDI = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCIFIDI/*')
#F_CI_CBOD_ECCIFIDI.registerTempTable("F_CI_CBOD_ECCIFIDI")
sql = """
 SELECT DST.ETLDT                                               --平台日期:src.ETLDT
       ,DST.EC_BNK_NO                                          --法人银行号:src.EC_BNK_NO
       ,DST.EC_SEQ_NO                                          --ECIF顺序号:src.EC_SEQ_NO
       ,DST.EC_CER_TYP                                         --证件类型:src.EC_CER_TYP
       ,DST.EC_CER_NO                                          --证件号码:src.EC_CER_NO
       ,DST.EC_FULL_NAM                                        --名称:src.EC_FULL_NAM
       ,DST.EC_CUST_NO                                         --客户号:src.EC_CUST_NO
       ,DST.EC_CUST_TYP                                        --客户类型:src.EC_CUST_TYP
       ,DST.EC_CER_EXPD_DT_N                                   --证件有效期:src.EC_CER_EXPD_DT_N
       ,DST.EC_ID_CHK_FLG                                      --身份核查标记:src.EC_ID_CHK_FLG
       ,DST.EC_CHK_CHANNEL                                     --核查渠道:src.EC_CHK_CHANNEL
       ,DST.EC_CHK_ER                                          --最终核查人:src.EC_CHK_ER
       ,DST.EC_CHK_DT_N                                        --最终核查日期:src.EC_CHK_DT_N
       ,DST.EC_PRI_CER_FLG                                     --是否为主证件:src.EC_PRI_CER_FLG
       ,DST.EC_CER_STS                                         --证件状态:src.EC_CER_STS
       ,DST.EC_NAM_TYP                                         --名称类型:src.EC_NAM_TYP
       ,DST.EC_CRT_SYS                                         --创建系统:src.EC_CRT_SYS
       ,DST.EC_CRT_SCT_N                                       --创建时间:src.EC_CRT_SCT_N
       ,DST.EC_CRT_OPR                                         --创建人:src.EC_CRT_OPR
       ,DST.EC_CRT_ORG                                         --创建机构号:src.EC_CRT_ORG
       ,DST.EC_UPD_SYS                                         --更新系统:src.EC_UPD_SYS
       ,DST.EC_UPD_OPR                                         --更新人:src.EC_UPD_OPR
       ,DST.EC_UPD_ORG                                         --更新机构号:src.EC_UPD_ORG
       ,DST.EC_DB_PART_ID                                      --分区键:src.EC_DB_PART_ID
       ,DST.EC_INSTN_COD                                       --机构代号:src.EC_INSTN_COD
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
   FROM F_CI_CBOD_ECCIFIDI DST 
   LEFT JOIN F_CI_CBOD_ECCIFIDI_INNTMP1 SRC 
     ON SRC.EC_BNK_NO           = DST.EC_BNK_NO 
    AND SRC.EC_SEQ_NO           = DST.EC_SEQ_NO 
  WHERE SRC.EC_BNK_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_ECCIFIDI_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_CBOD_ECCIFIDI/"+V_DT+".parquet"
UNION=F_CI_CBOD_ECCIFIDI_INNTMP2.unionAll(F_CI_CBOD_ECCIFIDI_INNTMP1)
F_CI_CBOD_ECCIFIDI_INNTMP1.cache()
F_CI_CBOD_ECCIFIDI_INNTMP2.cache()
nrowsi = F_CI_CBOD_ECCIFIDI_INNTMP1.count()
nrowsa = F_CI_CBOD_ECCIFIDI_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_CBOD_ECCIFIDI_INNTMP1.unpersist()
F_CI_CBOD_ECCIFIDI_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CBOD_ECCIFIDI lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_ECCIFIDI_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CI_CBOD_ECCIFIDI/"+V_DT+".parquet /"+dbname+"/F_CI_CBOD_ECCIFIDI_BK/")
