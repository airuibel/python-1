#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_LNWRRWRR').setMaster(sys.argv[2])
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

O_LN_CBOD_LNWRRWRR = sqlContext.read.parquet(hdfs+'/O_LN_CBOD_LNWRRWRR/*')
O_LN_CBOD_LNWRRWRR.registerTempTable("O_LN_CBOD_LNWRRWRR")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_LN_CBOD_LNWRRWRR = sqlContext.read.parquet(hdfs+'/F_LN_CBOD_LNWRRWRR_BK/'+V_DT_LD+'.parquet/*')
F_LN_CBOD_LNWRRWRR.registerTempTable("F_LN_CBOD_LNWRRWRR")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,LN_CLTRL_NO             AS LN_CLTRL_NO 
       ,LN_FINL_VRSN_N          AS LN_FINL_VRSN_N 
       ,LN_CUST_NO              AS LN_CUST_NO 
       ,LN_CUST_NAME            AS LN_CUST_NAME 
       ,LN_CLTRL_STS            AS LN_CLTRL_STS 
       ,LN_INDBTR_RGHT          AS LN_INDBTR_RGHT 
       ,LN_CLTRL_FLG            AS LN_CLTRL_FLG 
       ,LN_CLTRL_TYP            AS LN_CLTRL_TYP 
       ,LN_CLTRL_NAME           AS LN_CLTRL_NAME 
       ,LN_CLTRL_ADDR           AS LN_CLTRL_ADDR 
       ,LN_TDP_ACCT_NO          AS LN_TDP_ACCT_NO 
       ,LN_HYPO_SQ_NO           AS LN_HYPO_SQ_NO 
       ,LN_INTER_ACCT_NOS       AS LN_INTER_ACCT_NOS 
       ,LN_OUTER_ACCT_NOS       AS LN_OUTER_ACCT_NOS 
       ,LN_ESCO_COD             AS LN_ESCO_COD 
       ,LN_ESCO_NAME            AS LN_ESCO_NAME 
       ,LN_ESDT_N               AS LN_ESDT_N 
       ,LN_FACE_VAL             AS LN_FACE_VAL 
       ,LN_APPRC_VAL            AS LN_APPRC_VAL 
       ,LN_APPRC_CURR_COD       AS LN_APPRC_CURR_COD 
       ,LN_UDTK_AMT             AS LN_UDTK_AMT 
       ,LN_CVRT_EQB_AMT         AS LN_CVRT_EQB_AMT 
       ,LN_CVRT_EQB_CURR_COD    AS LN_CVRT_EQB_CURR_COD 
       ,LN_RESOLD_AMT           AS LN_RESOLD_AMT 
       ,LN_UDTK_CTRT_NO         AS LN_UDTK_CTRT_NO 
       ,LN_INS_PL_NO            AS LN_INS_PL_NO 
       ,LN_DOCTTL_ORG_COD       AS LN_DOCTTL_ORG_COD 
       ,LN_DOCTTL_NO            AS LN_DOCTTL_NO 
       ,LN_DOCTTL_NAME          AS LN_DOCTTL_NAME 
       ,LN_RFN_TYP              AS LN_RFN_TYP 
       ,LN_RMRK                 AS LN_RMRK 
       ,LN_OBTN_RGHT_DT_N       AS LN_OBTN_RGHT_DT_N 
       ,LN_RTRN_DT_N            AS LN_RTRN_DT_N 
       ,LN_INV_STRT_DT_N        AS LN_INV_STRT_DT_N 
       ,LN_INV_END_DT_N         AS LN_INV_END_DT_N 
       ,LN_COLL_DT_N            AS LN_COLL_DT_N 
       ,LN_TRNO_DT_N            AS LN_TRNO_DT_N 
       ,LN_APRV_DOC_NO          AS LN_APRV_DOC_NO 
       ,LN_APRV_DT_N            AS LN_APRV_DT_N 
       ,LN_APRV_PIC_NO          AS LN_APRV_PIC_NO 
       ,LN_LTST_MNTN_OPR_NO     AS LN_LTST_MNTN_OPR_NO 
       ,LN_LTST_MNTN_DT_N       AS LN_LTST_MNTN_DT_N 
       ,LN_CUSTD_OPUN_COD       AS LN_CUSTD_OPUN_COD 
       ,LN_CUSTD_OPR_COD        AS LN_CUSTD_OPR_COD 
       ,LN_VAR_DT_N             AS LN_VAR_DT_N 
       ,LN_BVAR_CUSTD_OPUN_COD  AS LN_BVAR_CUSTD_OPUN_COD 
       ,LN_BVAR_CUSTD_OPR_NO    AS LN_BVAR_CUSTD_OPR_NO 
       ,LN_CRNT_DAY_LST_TX_SQ_NO        AS LN_CRNT_DAY_LST_TX_SQ_NO 
       ,LN_BELONG_INSTN_COD     AS LN_BELONG_INSTN_COD 
       ,LN_ASS_OPUN_NO          AS LN_ASS_OPUN_NO 
       ,LN_LTST_MNTN_OPUN_NO    AS LN_LTST_MNTN_OPUN_NO 
       ,LN_FLST_OPUN_NO         AS LN_FLST_OPUN_NO 
       ,LN_DB_PART_ID           AS LN_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_LN_CBOD_LNWRRWRR A                                   --担保品资料档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_CBOD_LNWRRWRR_INNTMP1 = sqlContext.sql(sql)
F_LN_CBOD_LNWRRWRR_INNTMP1.registerTempTable("F_LN_CBOD_LNWRRWRR_INNTMP1")

#F_LN_CBOD_LNWRRWRR = sqlContext.read.parquet(hdfs+'/F_LN_CBOD_LNWRRWRR/*')
#F_LN_CBOD_LNWRRWRR.registerTempTable("F_LN_CBOD_LNWRRWRR")
sql = """
 SELECT DST.ETLDT                                               --源系统日期:src.ETLDT
       ,DST.LN_CLTRL_NO                                        --担保品编号:src.LN_CLTRL_NO
       ,DST.LN_FINL_VRSN_N                                     --帐务版次:src.LN_FINL_VRSN_N
       ,DST.LN_CUST_NO                                         --客户编号:src.LN_CUST_NO
       ,DST.LN_CUST_NAME                                       --客户名称:src.LN_CUST_NAME
       ,DST.LN_CLTRL_STS                                       --担保品状态:src.LN_CLTRL_STS
       ,DST.LN_INDBTR_RGHT                                     --债务人权利:src.LN_INDBTR_RGHT
       ,DST.LN_CLTRL_FLG                                       --抵质押标志:src.LN_CLTRL_FLG
       ,DST.LN_CLTRL_TYP                                       --担保品种类:src.LN_CLTRL_TYP
       ,DST.LN_CLTRL_NAME                                      --担保品名称:src.LN_CLTRL_NAME
       ,DST.LN_CLTRL_ADDR                                      --担保品存放地址:src.LN_CLTRL_ADDR
       ,DST.LN_TDP_ACCT_NO                                     --定存单帐号:src.LN_TDP_ACCT_NO
       ,DST.LN_HYPO_SQ_NO                                      --质借序号:src.LN_HYPO_SQ_NO
       ,DST.LN_INTER_ACCT_NOS                                  --本行质押存款账户串:src.LN_INTER_ACCT_NOS
       ,DST.LN_OUTER_ACCT_NOS                                  --他行质押存款账户串:src.LN_OUTER_ACCT_NOS
       ,DST.LN_ESCO_COD                                        --估价公司代号:src.LN_ESCO_COD
       ,DST.LN_ESCO_NAME                                       --估价公司名称:src.LN_ESCO_NAME
       ,DST.LN_ESDT_N                                          --估价日期:src.LN_ESDT_N
       ,DST.LN_FACE_VAL                                        --COMP3帐面价值:src.LN_FACE_VAL
       ,DST.LN_APPRC_VAL                                       --COMP3评估价值:src.LN_APPRC_VAL
       ,DST.LN_APPRC_CURR_COD                                  --评估价值币别:src.LN_APPRC_CURR_COD
       ,DST.LN_UDTK_AMT                                        --COMP3担保金额:src.LN_UDTK_AMT
       ,DST.LN_CVRT_EQB_AMT                                    --COMP3折价金额:src.LN_CVRT_EQB_AMT
       ,DST.LN_CVRT_EQB_CURR_COD                               --折价金额币别:src.LN_CVRT_EQB_CURR_COD
       ,DST.LN_RESOLD_AMT                                      --COMP3变卖金额:src.LN_RESOLD_AMT
       ,DST.LN_UDTK_CTRT_NO                                    --担保合同编号:src.LN_UDTK_CTRT_NO
       ,DST.LN_INS_PL_NO                                       --保单号:src.LN_INS_PL_NO
       ,DST.LN_DOCTTL_ORG_COD                                  --权利证书登记机关:src.LN_DOCTTL_ORG_COD
       ,DST.LN_DOCTTL_NO                                       --权利证书号码:src.LN_DOCTTL_NO
       ,DST.LN_DOCTTL_NAME                                     --权利证书名称:src.LN_DOCTTL_NAME
       ,DST.LN_RFN_TYP                                         --归还方式:src.LN_RFN_TYP
       ,DST.LN_RMRK                                            --备注:src.LN_RMRK
       ,DST.LN_OBTN_RGHT_DT_N                                  --取得权利日期:src.LN_OBTN_RGHT_DT_N
       ,DST.LN_RTRN_DT_N                                       --退回日期:src.LN_RTRN_DT_N
       ,DST.LN_INV_STRT_DT_N                                   --起始日期4:src.LN_INV_STRT_DT_N
       ,DST.LN_INV_END_DT_N                                    --存续迄日:src.LN_INV_END_DT_N
       ,DST.LN_COLL_DT_N                                       --收妥日期:src.LN_COLL_DT_N
       ,DST.LN_TRNO_DT_N                                       --转出日期:src.LN_TRNO_DT_N
       ,DST.LN_APRV_DOC_NO                                     --核准文件号:src.LN_APRV_DOC_NO
       ,DST.LN_APRV_DT_N                                       --核准日期:src.LN_APRV_DT_N
       ,DST.LN_APRV_PIC_NO                                     --核准主管代号:src.LN_APRV_PIC_NO
       ,DST.LN_LTST_MNTN_OPR_NO                                --最近维护操作员号:src.LN_LTST_MNTN_OPR_NO
       ,DST.LN_LTST_MNTN_DT_N                                  --最近维护日期:src.LN_LTST_MNTN_DT_N
       ,DST.LN_CUSTD_OPUN_COD                                  --保管机构:src.LN_CUSTD_OPUN_COD
       ,DST.LN_CUSTD_OPR_COD                                   --保管柜员号:src.LN_CUSTD_OPR_COD
       ,DST.LN_VAR_DT_N                                        --变更日期:src.LN_VAR_DT_N
       ,DST.LN_BVAR_CUSTD_OPUN_COD                             --转交前保管机构:src.LN_BVAR_CUSTD_OPUN_COD
       ,DST.LN_BVAR_CUSTD_OPR_NO                               --转交前保管柜员号:src.LN_BVAR_CUSTD_OPR_NO
       ,DST.LN_CRNT_DAY_LST_TX_SQ_NO                           --本日上笔交易流水号:src.LN_CRNT_DAY_LST_TX_SQ_NO
       ,DST.LN_BELONG_INSTN_COD                                --账户归属机构:src.LN_BELONG_INSTN_COD
       ,DST.LN_ASS_OPUN_NO                                     --考核机构号:src.LN_ASS_OPUN_NO
       ,DST.LN_LTST_MNTN_OPUN_NO                               --最近维护交易机构:src.LN_LTST_MNTN_OPUN_NO
       ,DST.LN_FLST_OPUN_NO                                    --开户机构编号:src.LN_FLST_OPUN_NO
       ,DST.LN_DB_PART_ID                                      --分区键:src.LN_DB_PART_ID
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_LN_CBOD_LNWRRWRR DST 
   LEFT JOIN F_LN_CBOD_LNWRRWRR_INNTMP1 SRC 
     ON SRC.LN_CLTRL_NO         = DST.LN_CLTRL_NO 
  WHERE SRC.LN_CLTRL_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_CBOD_LNWRRWRR_INNTMP2 = sqlContext.sql(sql)
dfn="F_LN_CBOD_LNWRRWRR/"+V_DT+".parquet"
UNION=F_LN_CBOD_LNWRRWRR_INNTMP2.unionAll(F_LN_CBOD_LNWRRWRR_INNTMP1)
F_LN_CBOD_LNWRRWRR_INNTMP1.cache()
F_LN_CBOD_LNWRRWRR_INNTMP2.cache()
nrowsi = F_LN_CBOD_LNWRRWRR_INNTMP1.count()
nrowsa = F_LN_CBOD_LNWRRWRR_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_LN_CBOD_LNWRRWRR_INNTMP1.unpersist()
F_LN_CBOD_LNWRRWRR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_CBOD_LNWRRWRR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_LN_CBOD_LNWRRWRR/"+V_DT_LD+".parquet /"+dbname+"/F_LN_CBOD_LNWRRWRR_BK/")
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_CBOD_LNWRRWRR/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_CBOD_LNWRRWRR_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_LN_CBOD_LNWRRWRR/"+V_DT+".parquet /"+dbname+"/F_LN_CBOD_LNWRRWRR_BK/")
