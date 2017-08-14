#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_TDAC1AC1').setMaster(sys.argv[2])
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

O_DP_CBOD_TDAC1AC1 = sqlContext.read.parquet(hdfs+'/O_DP_CBOD_TDAC1AC1/*')
O_DP_CBOD_TDAC1AC1.registerTempTable("O_DP_CBOD_TDAC1AC1")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_DP_CBOD_TDAC1AC1 = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_TDAC1AC1_BK/'+V_DT_LD+'.parquet/*')
F_DP_CBOD_TDAC1AC1.registerTempTable("F_DP_CBOD_TDAC1AC1")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,TD_ACC1_NO              AS TD_ACC1_NO 
       ,TD_ACCT_STS             AS TD_ACCT_STS 
       ,TD_CUST_NO              AS TD_CUST_NO 
       ,TD_MBSTMT_ADDR          AS TD_MBSTMT_ADDR 
       ,TD_PSBK_NO_CNT_N        AS TD_PSBK_NO_CNT_N 
       ,TD_SQ_NO_CNT_N          AS TD_SQ_NO_CNT_N 
       ,TD_FRZ_STS              AS TD_FRZ_STS 
       ,TD_FRZ_DT               AS TD_FRZ_DT 
       ,TD_FRZ_TM               AS TD_FRZ_TM 
       ,TD_DW_RANG              AS TD_DW_RANG 
       ,TD_DRW_TYP              AS TD_DRW_TYP 
       ,TD_SEAL_STS             AS TD_SEAL_STS 
       ,TD_SEAL_DL_DT           AS TD_SEAL_DL_DT 
       ,TD_SEAL_DL_TM           AS TD_SEAL_DL_TM 
       ,TD_PSWD_RULE            AS TD_PSWD_RULE 
       ,TD_PSWD_PVK_VRSN_NO     AS TD_PSWD_PVK_VRSN_NO 
       ,TD_PSWD_ERR_TIMES_N     AS TD_PSWD_ERR_TIMES_N 
       ,TD_REF_CNT_N            AS TD_REF_CNT_N 
       ,TD_QPSWD                AS TD_QPSWD 
       ,TD_PSWD_STS             AS TD_PSWD_STS 
       ,TD_PSWD_DL_DT           AS TD_PSWD_DL_DT 
       ,TD_PSWD_DL_TM           AS TD_PSWD_DL_TM 
       ,TD_OPAC_INSTN_NO        AS TD_OPAC_INSTN_NO 
       ,TD_OPAC_TLR_NO          AS TD_OPAC_TLR_NO 
       ,TD_OPAC_DT              AS TD_OPAC_DT 
       ,TD_CACCT_INSTN_NO       AS TD_CACCT_INSTN_NO 
       ,TD_CACCT_TLR_NO         AS TD_CACCT_TLR_NO 
       ,TD_CACCT_DT             AS TD_CACCT_DT 
       ,TD_CUST_NAME            AS TD_CUST_NAME 
       ,TD_LST_TX_DT            AS TD_LST_TX_DT 
       ,TD_CURR_CHAR            AS TD_CURR_CHAR 
       ,TD_ACC1_BLNG_TO_FLG     AS TD_ACC1_BLNG_TO_FLG 
       ,TD_NON_FRZ_CONSIGN_TIME_N       AS TD_NON_FRZ_CONSIGN_TIME_N 
       ,TD_VIP_ACCT_FLG         AS TD_VIP_ACCT_FLG 
       ,TD_PDP_CODE             AS TD_PDP_CODE 
       ,TD_PDP_CODE_C1          AS TD_PDP_CODE_C1 
       ,TD_PDP_CODE_C2          AS TD_PDP_CODE_C2 
       ,TD_DP_RANG              AS TD_DP_RANG 
       ,TD_SLCRD_NO             AS TD_SLCRD_NO 
       ,TD_CONNTR_NO            AS TD_CONNTR_NO 
       ,TD_SA_ACCT_NO           AS TD_SA_ACCT_NO 
       ,TD_CERT_TYP             AS TD_CERT_TYP 
       ,TD_CERT_ID              AS TD_CERT_ID 
       ,TD_BELONG_INSTN_COD     AS TD_BELONG_INSTN_COD 
       ,TD_JUR_FRZ_STS          AS TD_JUR_FRZ_STS 
       ,TD_JUR_FRZ_DT           AS TD_JUR_FRZ_DT 
       ,TD_JUR_FRZ_TM           AS TD_JUR_FRZ_TM 
       ,TD_ASES_INSTN_COD       AS TD_ASES_INSTN_COD 
       ,TD_DB_PART_ID           AS TD_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_DP_CBOD_TDAC1AC1 A                                   --定存一本通主档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_TDAC1AC1_INNTMP1 = sqlContext.sql(sql)
F_DP_CBOD_TDAC1AC1_INNTMP1.registerTempTable("F_DP_CBOD_TDAC1AC1_INNTMP1")

#F_DP_CBOD_TDAC1AC1 = sqlContext.read.parquet(hdfs+'/F_DP_CBOD_TDAC1AC1/*')
#F_DP_CBOD_TDAC1AC1.registerTempTable("F_DP_CBOD_TDAC1AC1")
sql = """
 SELECT DST.ETLDT                                               --源系统日期:src.ETLDT
       ,DST.TD_ACC1_NO                                         --一折通帐号:src.TD_ACC1_NO
       ,DST.TD_ACCT_STS                                        --帐户状态/保兑仓协议状态:src.TD_ACCT_STS
       ,DST.TD_CUST_NO                                         --客户编号:src.TD_CUST_NO
       ,DST.TD_MBSTMT_ADDR                                     --对帐单地址码:src.TD_MBSTMT_ADDR
       ,DST.TD_PSBK_NO_CNT_N                                   --末册册号:src.TD_PSBK_NO_CNT_N
       ,DST.TD_SQ_NO_CNT_N                                     --末册笔数:src.TD_SQ_NO_CNT_N
       ,DST.TD_FRZ_STS                                         --止付状态:src.TD_FRZ_STS
       ,DST.TD_FRZ_DT                                          --止付日期1:src.TD_FRZ_DT
       ,DST.TD_FRZ_TM                                          --止付时间:src.TD_FRZ_TM
       ,DST.TD_DW_RANG                                         --通兑范围:src.TD_DW_RANG
       ,DST.TD_DRW_TYP                                         --支取方式:src.TD_DRW_TYP
       ,DST.TD_SEAL_STS                                        --印鉴状态:src.TD_SEAL_STS
       ,DST.TD_SEAL_DL_DT                                      --印鉴挂失日期:src.TD_SEAL_DL_DT
       ,DST.TD_SEAL_DL_TM                                      --印鉴挂失时间:src.TD_SEAL_DL_TM
       ,DST.TD_PSWD_RULE                                       --密码规则:src.TD_PSWD_RULE
       ,DST.TD_PSWD_PVK_VRSN_NO                                --密码版本1:src.TD_PSWD_PVK_VRSN_NO
       ,DST.TD_PSWD_ERR_TIMES_N                                --密码出错次数:src.TD_PSWD_ERR_TIMES_N
       ,DST.TD_REF_CNT_N                                       --关联账户数:src.TD_REF_CNT_N
       ,DST.TD_QPSWD                                           --查询密码:src.TD_QPSWD
       ,DST.TD_PSWD_STS                                        --密码状态:src.TD_PSWD_STS
       ,DST.TD_PSWD_DL_DT                                      --密码挂失日期:src.TD_PSWD_DL_DT
       ,DST.TD_PSWD_DL_TM                                      --密码挂失时间:src.TD_PSWD_DL_TM
       ,DST.TD_OPAC_INSTN_NO                                   --开户机构号:src.TD_OPAC_INSTN_NO
       ,DST.TD_OPAC_TLR_NO                                     --开户柜员号:src.TD_OPAC_TLR_NO
       ,DST.TD_OPAC_DT                                         --开户日期-X8:src.TD_OPAC_DT
       ,DST.TD_CACCT_INSTN_NO                                  --销户机构号:src.TD_CACCT_INSTN_NO
       ,DST.TD_CACCT_TLR_NO                                    --销户柜员号:src.TD_CACCT_TLR_NO
       ,DST.TD_CACCT_DT                                        --销户日期:src.TD_CACCT_DT
       ,DST.TD_CUST_NAME                                       --客户名称:src.TD_CUST_NAME
       ,DST.TD_LST_TX_DT                                       --最后交易日期:src.TD_LST_TX_DT
       ,DST.TD_CURR_CHAR                                       --钞汇性质:src.TD_CURR_CHAR
       ,DST.TD_ACC1_BLNG_TO_FLG                                --一本通归属标志位:src.TD_ACC1_BLNG_TO_FLG
       ,DST.TD_NON_FRZ_CONSIGN_TIME_N                          --非冻结委托次数:src.TD_NON_FRZ_CONSIGN_TIME_N
       ,DST.TD_VIP_ACCT_FLG                                    --重要帐户标志:src.TD_VIP_ACCT_FLG
       ,DST.TD_PDP_CODE                                        --产品代码PDP:src.TD_PDP_CODE
       ,DST.TD_PDP_CODE_C1                                     --产品代码PDP_C1:src.TD_PDP_CODE_C1
       ,DST.TD_PDP_CODE_C2                                     --产品代码PDP_C2:src.TD_PDP_CODE_C2
       ,DST.TD_DP_RANG                                         --通存范围:src.TD_DP_RANG
       ,DST.TD_SLCRD_NO                                        --印鉴卡编号:src.TD_SLCRD_NO
       ,DST.TD_CONNTR_NO                                       --联系人编号:src.TD_CONNTR_NO
       ,DST.TD_SA_ACCT_NO                                      --理财卡备付金帐号:src.TD_SA_ACCT_NO
       ,DST.TD_CERT_TYP                                        --证件种类:src.TD_CERT_TYP
       ,DST.TD_CERT_ID                                         --证件号码:src.TD_CERT_ID
       ,DST.TD_BELONG_INSTN_COD                                --账户归属机构:src.TD_BELONG_INSTN_COD
       ,DST.TD_JUR_FRZ_STS                                     --司法冻结状态:src.TD_JUR_FRZ_STS
       ,DST.TD_JUR_FRZ_DT                                      --司法冻结日期:src.TD_JUR_FRZ_DT
       ,DST.TD_JUR_FRZ_TM                                      --司法冻结时间:src.TD_JUR_FRZ_TM
       ,DST.TD_ASES_INSTN_COD                                  --考核机构:src.TD_ASES_INSTN_COD
       ,DST.TD_DB_PART_ID                                      --分区键:src.TD_DB_PART_ID
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_DP_CBOD_TDAC1AC1 DST 
   LEFT JOIN F_DP_CBOD_TDAC1AC1_INNTMP1 SRC 
     ON SRC.TD_ACC1_NO          = DST.TD_ACC1_NO 
  WHERE SRC.TD_ACC1_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_DP_CBOD_TDAC1AC1_INNTMP2 = sqlContext.sql(sql)
dfn="F_DP_CBOD_TDAC1AC1/"+V_DT+".parquet"
UNION=F_DP_CBOD_TDAC1AC1_INNTMP2.unionAll(F_DP_CBOD_TDAC1AC1_INNTMP1)
F_DP_CBOD_TDAC1AC1_INNTMP1.cache()
F_DP_CBOD_TDAC1AC1_INNTMP2.cache()
nrowsi = F_DP_CBOD_TDAC1AC1_INNTMP1.count()
nrowsa = F_DP_CBOD_TDAC1AC1_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_DP_CBOD_TDAC1AC1_INNTMP1.unpersist()
F_DP_CBOD_TDAC1AC1_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_DP_CBOD_TDAC1AC1 lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_DP_CBOD_TDAC1AC1/"+V_DT_LD+".parquet /"+dbname+"/F_DP_CBOD_TDAC1AC1_BK/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_TDAC1AC1/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_DP_CBOD_TDAC1AC1_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_DP_CBOD_TDAC1AC1/"+V_DT+".parquet /"+dbname+"/F_DP_CBOD_TDAC1AC1_BK/")
