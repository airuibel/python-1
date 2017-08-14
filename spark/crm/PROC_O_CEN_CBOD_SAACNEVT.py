#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_SAACNEVT').setMaster(sys.argv[2])
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

O_DP_CBOD_SAACNEVT = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_SAACNEVT/*')
O_DP_CBOD_SAACNEVT.registerTempTable("O_DP_CBOD_SAACNEVT")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_DP_CBOD_SAACNEVT = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNEVT_BK/'+V_DT_LD+'.parquet/*')
F_DP_CBOD_SAACNEVT.registerTempTable("F_DP_CBOD_SAACNEVT")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,FK_SAACN_KEY            AS FK_SAACN_KEY 
       ,SA_ACCD_COD             AS SA_ACCD_COD 
       ,SA_ACCD_DT              AS SA_ACCD_DT 
       ,SA_EVT_SRL_NO_N         AS SA_EVT_SRL_NO_N 
       ,SA_ACCD_TM              AS SA_ACCD_TM 
       ,SA_CURR_COD             AS SA_CURR_COD 
       ,SA_OPR_NO               AS SA_OPR_NO 
       ,SA_CURR_IDEN            AS SA_CURR_IDEN 
       ,SA_ACCD_RMRK            AS SA_ACCD_RMRK 
       ,SA_ACCD_INSTN_NO        AS SA_ACCD_INSTN_NO 
       ,SA_ACCD_AMT             AS SA_ACCD_AMT 
       ,SA_CERT_ID              AS SA_CERT_ID 
       ,SA_CERT_TYP             AS SA_CERT_TYP 
       ,SA_PROCESS_FLAG         AS SA_PROCESS_FLAG 
       ,SA_PRO_EVT_SRL_NO_N     AS SA_PRO_EVT_SRL_NO_N 
       ,SA_UNLCK_DT             AS SA_UNLCK_DT 
       ,SA_FRZ_TYPE_FLG         AS SA_FRZ_TYPE_FLG 
       ,SA_ACCD_AMT_1           AS SA_ACCD_AMT_1 
       ,SA_CURR_IDEN_1          AS SA_CURR_IDEN_1 
       ,SA_ACCT_DT_CLONE        AS SA_ACCT_DT_CLONE 
       ,SA_BRANCH_NAME          AS SA_BRANCH_NAME 
       ,SA_SHIFT_ADV_NO         AS SA_SHIFT_ADV_NO 
       ,SA_STAF_NAME            AS SA_STAF_NAME 
       ,SA_B_AUTH_PIC_NO        AS SA_B_AUTH_PIC_NO 
       ,SA_A_AUTH_PIC_NO        AS SA_A_AUTH_PIC_NO 
       ,SA_AGT_CERT_TYP         AS SA_AGT_CERT_TYP 
       ,SA_AGT_CERT_ID          AS SA_AGT_CERT_ID 
       ,SA_AGT_CUST_NAME        AS SA_AGT_CUST_NAME 
       ,SA_BELONG_INSTN_COD     AS SA_BELONG_INSTN_COD 
       ,SA_EVT_SRL_CLONE_N      AS SA_EVT_SRL_CLONE_N 
       ,SA_BRANCH_TYPE          AS SA_BRANCH_TYPE 
       ,SA_CLR_INTR             AS SA_CLR_INTR 
       ,SA_PDP_CODE             AS SA_PDP_CODE 
       ,SA_DB_PART_ID           AS SA_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_DP_CBOD_SAACNEVT A                                   --活存事故档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_SAACNEVT_INNTMP1 = sqlContext.sql(sql)
F_DP_CBOD_SAACNEVT_INNTMP1.registerTempTable("F_DP_CBOD_SAACNEVT_INNTMP1")

#F_DP_CBOD_SAACNEVT = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_SAACNEVT/*')
#F_DP_CBOD_SAACNEVT.registerTempTable("F_DP_CBOD_SAACNEVT")
sql = """
 SELECT DST.ETLDT                                               --源系统日期:src.ETLDT
       ,DST.FK_SAACN_KEY                                       --账号:src.FK_SAACN_KEY
       ,DST.SA_ACCD_COD                                        --事故代码:src.SA_ACCD_COD
       ,DST.SA_ACCD_DT                                         --事故日期:src.SA_ACCD_DT
       ,DST.SA_EVT_SRL_NO_N                                    --事故序号:src.SA_EVT_SRL_NO_N
       ,DST.SA_ACCD_TM                                         --事故时间:src.SA_ACCD_TM
       ,DST.SA_CURR_COD                                        --币别:src.SA_CURR_COD
       ,DST.SA_OPR_NO                                          --操作员号:src.SA_OPR_NO
       ,DST.SA_CURR_IDEN                                       --钞汇鉴别:src.SA_CURR_IDEN
       ,DST.SA_ACCD_RMRK                                       --事故备注:src.SA_ACCD_RMRK
       ,DST.SA_ACCD_INSTN_NO                                   --事故机构号:src.SA_ACCD_INSTN_NO
       ,DST.SA_ACCD_AMT                                        --事故金额:src.SA_ACCD_AMT
       ,DST.SA_CERT_ID                                         --证件号码:src.SA_CERT_ID
       ,DST.SA_CERT_TYP                                        --证件种类:src.SA_CERT_TYP
       ,DST.SA_PROCESS_FLAG                                    --是否已处理标志:src.SA_PROCESS_FLAG
       ,DST.SA_PRO_EVT_SRL_NO_N                                --处理事故序号:src.SA_PRO_EVT_SRL_NO_N
       ,DST.SA_UNLCK_DT                                        --解控日期:src.SA_UNLCK_DT
       ,DST.SA_FRZ_TYPE_FLG                                    --冻结类型标志:src.SA_FRZ_TYPE_FLG
       ,DST.SA_ACCD_AMT_1                                      --事故金额:src.SA_ACCD_AMT_1
       ,DST.SA_CURR_IDEN_1                                     --事故钞汇:src.SA_CURR_IDEN_1
       ,DST.SA_ACCT_DT_CLONE                                   --事故日期:src.SA_ACCT_DT_CLONE
       ,DST.SA_BRANCH_NAME                                     --单位名称:src.SA_BRANCH_NAME
       ,DST.SA_SHIFT_ADV_NO                                    --通知书编号:src.SA_SHIFT_ADV_NO
       ,DST.SA_STAF_NAME                                       --经办人名称:src.SA_STAF_NAME
       ,DST.SA_B_AUTH_PIC_NO                                   --B级授权主管代号:src.SA_B_AUTH_PIC_NO
       ,DST.SA_A_AUTH_PIC_NO                                   --A级授权主管代号:src.SA_A_AUTH_PIC_NO
       ,DST.SA_AGT_CERT_TYP                                    --代理人证件种类:src.SA_AGT_CERT_TYP
       ,DST.SA_AGT_CERT_ID                                     --代理人证件号:src.SA_AGT_CERT_ID
       ,DST.SA_AGT_CUST_NAME                                   --代理人名称:src.SA_AGT_CUST_NAME
       ,DST.SA_BELONG_INSTN_COD                                --账户归属机构:src.SA_BELONG_INSTN_COD
       ,DST.SA_EVT_SRL_CLONE_N                                 --事故序号:src.SA_EVT_SRL_CLONE_N
       ,DST.SA_BRANCH_TYPE                                     --执行机关类型:src.SA_BRANCH_TYPE
       ,DST.SA_CLR_INTR                                        --锁定资金利率:src.SA_CLR_INTR
       ,DST.SA_PDP_CODE                                        --产品代码:src.SA_PDP_CODE
       ,DST.SA_DB_PART_ID                                      --分区键:src.SA_DB_PART_ID
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_DP_CBOD_SAACNEVT DST 
   LEFT JOIN F_DP_CBOD_SAACNEVT_INNTMP1 SRC 
     ON SRC.FK_SAACN_KEY        = DST.FK_SAACN_KEY 
    AND SRC.SA_ACCD_COD         = DST.SA_ACCD_COD 
    AND SRC.SA_ACCD_DT          = DST.SA_ACCD_DT 
    AND SRC.SA_EVT_SRL_NO_N     = DST.SA_EVT_SRL_NO_N 
  WHERE SRC.FK_SAACN_KEY IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_SAACNEVT_INNTMP2 = sqlContext.sql(sql)
dfn="F_DP_CBOD_SAACNEVT/"+V_DT+".parquet"
UNION=F_DP_CBOD_SAACNEVT_INNTMP2.unionAll(F_DP_CBOD_SAACNEVT_INNTMP1)
F_DP_CBOD_SAACNEVT_INNTMP1.cache()
F_DP_CBOD_SAACNEVT_INNTMP2.cache()
nrowsi = F_DP_CBOD_SAACNEVT_INNTMP1.count()
nrowsa = F_DP_CBOD_SAACNEVT_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_DP_CBOD_SAACNEVT_INNTMP1.unpersist()
F_DP_CBOD_SAACNEVT_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_SAACNEVT lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_DP_CBOD_SAACNEVT/"+V_DT_LD+".parquet /"+dbname+"/F_DP_CBOD_SAACNEVT_BK/")
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_SAACNEVT/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_SAACNEVT_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_DP_CBOD_SAACNEVT/"+V_DT+".parquet /"+dbname+"/F_DP_CBOD_SAACNEVT_BK/")