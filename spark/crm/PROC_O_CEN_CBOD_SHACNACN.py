#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_SHACNACN').setMaster(sys.argv[2])
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

O_CI_CBOD_SHACNACN = sqlContext.read.parquet(hdfs+'/O_CI_CBOD_SHACNACN/*')
O_CI_CBOD_SHACNACN.registerTempTable("O_CI_CBOD_SHACNACN")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CI_CBOD_SHACNACN = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_SHACNACN_BK/'+V_DT_LD+'.parquet/*')
F_CI_CBOD_SHACNACN.registerTempTable("F_CI_CBOD_SHACNACN")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,SH_ACCT_NO              AS SH_ACCT_NO 
       ,SH_ACCT_CHAR            AS SH_ACCT_CHAR 
       ,SH_PDP_CODE             AS SH_PDP_CODE 
       ,SH_AVAL_DT              AS SH_AVAL_DT 
       ,SH_CUST_NO              AS SH_CUST_NO 
       ,SH_CUST_NAME            AS SH_CUST_NAME 
       ,SH_DRW_TYP              AS SH_DRW_TYP 
       ,SH_PRDS_INSTN_DPDW_FLG  AS SH_PRDS_INSTN_DPDW_FLG 
       ,SH_DPDW_RANG            AS SH_DPDW_RANG 
       ,SH_INTP_FLG             AS SH_INTP_FLG 
       ,SH_VIP_ACCT_FLG         AS SH_VIP_ACCT_FLG 
       ,SH_AVAL_TRN_DT          AS SH_AVAL_TRN_DT 
       ,SH_DDP_ACCT_STS         AS SH_DDP_ACCT_STS 
       ,SH_DDP_ACCT_CTL_STS     AS SH_DDP_ACCT_CTL_STS 
       ,SH_PSBK_NO              AS SH_PSBK_NO 
       ,SH_PSBK_DL_DT           AS SH_PSBK_DL_DT 
       ,SH_PSBK_PRT_NO          AS SH_PSBK_PRT_NO 
       ,SH_PSBK_STS             AS SH_PSBK_STS 
       ,SH_DET_ITEM_1           AS SH_DET_ITEM_1 
       ,SH_DET_ITEM_2           AS SH_DET_ITEM_2 
       ,SH_DET_ITEM_3           AS SH_DET_ITEM_3 
       ,SH_PGLN_TOTL_1_PG       AS SH_PGLN_TOTL_1_PG 
       ,SH_PGLN_TOTL_1_LN       AS SH_PGLN_TOTL_1_LN 
       ,SH_PGLN_TOTL_2_PG       AS SH_PGLN_TOTL_2_PG 
       ,SH_PGLN_TOTL_2_LN       AS SH_PGLN_TOTL_2_LN 
       ,SH_PGLN_TOTL_3_PG       AS SH_PGLN_TOTL_3_PG 
       ,SH_PGLN_TOTL_3_LN       AS SH_PGLN_TOTL_3_LN 
       ,SH_AENTR_DET_TOTL_1     AS SH_AENTR_DET_TOTL_1 
       ,SH_AENTR_DET_TOTL_2     AS SH_AENTR_DET_TOTL_2 
       ,SH_AENTR_DET_TOTL_3     AS SH_AENTR_DET_TOTL_3 
       ,SH_PRINTED_MAX_NO_1     AS SH_PRINTED_MAX_NO_1 
       ,SH_PRINTED_MAX_NO_2     AS SH_PRINTED_MAX_NO_2 
       ,SH_PRINTED_MAX_NO_3     AS SH_PRINTED_MAX_NO_3 
       ,SH_INT_ACCT_NO          AS SH_INT_ACCT_NO 
       ,SH_ZG_LTM_ACCT_BAL      AS SH_ZG_LTM_ACCT_BAL 
       ,SH_ZG_LTM_DDP_PDT       AS SH_ZG_LTM_DDP_PDT 
       ,SH_TZ_LTM_ACCT_BAL      AS SH_TZ_LTM_ACCT_BAL 
       ,SH_TZ_LTM_DDP_PDT       AS SH_TZ_LTM_DDP_PDT 
       ,SH_LTM_DDP_DT           AS SH_LTM_DDP_DT 
       ,SH_LTM_REC_DT           AS SH_LTM_REC_DT 
       ,SH_PSWD_ERR_TIMES       AS SH_PSWD_ERR_TIMES 
       ,SH_QPSWD                AS SH_QPSWD 
       ,SH_PSWD_ERR_DT          AS SH_PSWD_ERR_DT 
       ,SH_ENC_TYP              AS SH_ENC_TYP 
       ,SH_ENCKEY_VER           AS SH_ENCKEY_VER 
       ,SH_PSWD_DL_DT           AS SH_PSWD_DL_DT 
       ,SH_PSWD_STS             AS SH_PSWD_STS 
       ,SH_LEGAL_INSTN_NO       AS SH_LEGAL_INSTN_NO 
       ,SH_OPAC_INSTN_NO        AS SH_OPAC_INSTN_NO 
       ,SH_OPAC_DT              AS SH_OPAC_DT 
       ,SH_OPAC_TLR_NO          AS SH_OPAC_TLR_NO 
       ,SH_CACCT_TLR_NO         AS SH_CACCT_TLR_NO 
       ,SH_CACCT_INSTN_NO       AS SH_CACCT_INSTN_NO 
       ,SH_CACCT_DT             AS SH_CACCT_DT 
       ,SH_OPAC_PERM_NO         AS SH_OPAC_PERM_NO 
       ,SH_CERT_TYP             AS SH_CERT_TYP 
       ,SH_CERT_ID              AS SH_CERT_ID 
       ,SH_1LVL_BRH_ID          AS SH_1LVL_BRH_ID 
       ,SH_DB_PART_ID           AS SH_DB_PART_ID 
       ,SH_CRPT_PIN             AS SH_CRPT_PIN 
       ,SH_ACN_MEMO             AS SH_ACN_MEMO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CI_CBOD_SHACNACN A                                   --股金主档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_SHACNACN_INNTMP1 = sqlContext.sql(sql)
F_CI_CBOD_SHACNACN_INNTMP1.registerTempTable("F_CI_CBOD_SHACNACN_INNTMP1")

#F_CI_CBOD_SHACNACN = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_SHACNACN/*')
#F_CI_CBOD_SHACNACN.registerTempTable("F_CI_CBOD_SHACNACN")
sql = """
 SELECT DST.ETLDT                                               --源系统日期:src.ETLDT
       ,DST.SH_ACCT_NO                                         --帐号:src.SH_ACCT_NO
       ,DST.SH_ACCT_CHAR                                       --股东性质:src.SH_ACCT_CHAR
       ,DST.SH_PDP_CODE                                        --产品代码PDP:src.SH_PDP_CODE
       ,DST.SH_AVAL_DT                                         --起用日期:src.SH_AVAL_DT
       ,DST.SH_CUST_NO                                         --客户编号:src.SH_CUST_NO
       ,DST.SH_CUST_NAME                                       --客户名称:src.SH_CUST_NAME
       ,DST.SH_DRW_TYP                                         --支取方式:src.SH_DRW_TYP
       ,DST.SH_PRDS_INSTN_DPDW_FLG                             --约定机构存取标志:src.SH_PRDS_INSTN_DPDW_FLG
       ,DST.SH_DPDW_RANG                                       --通兑范围:src.SH_DPDW_RANG
       ,DST.SH_INTP_FLG                                        --股金红利计息标志:src.SH_INTP_FLG
       ,DST.SH_VIP_ACCT_FLG                                    --重要帐户标志:src.SH_VIP_ACCT_FLG
       ,DST.SH_AVAL_TRN_DT                                     --起始可转让日期:src.SH_AVAL_TRN_DT
       ,DST.SH_DDP_ACCT_STS                                    --活存帐户状态:src.SH_DDP_ACCT_STS
       ,DST.SH_DDP_ACCT_CTL_STS                                --账户控制状态:src.SH_DDP_ACCT_CTL_STS
       ,DST.SH_PSBK_NO                                         --存折册号:src.SH_PSBK_NO
       ,DST.SH_PSBK_DL_DT                                      --存折挂失日期:src.SH_PSBK_DL_DT
       ,DST.SH_PSBK_PRT_NO                                     --存折印刷号:src.SH_PSBK_PRT_NO
       ,DST.SH_PSBK_STS                                        --存折状态:src.SH_PSBK_STS
       ,DST.SH_DET_ITEM_1                                      --投资股交易记录编号:src.SH_DET_ITEM_1
       ,DST.SH_DET_ITEM_2                                      --资格股交易记录编号-2:src.SH_DET_ITEM_2
       ,DST.SH_DET_ITEM_3                                      --分红交易记录编号:src.SH_DET_ITEM_3
       ,DST.SH_PGLN_TOTL_1_PG                                  --投资股页数-1:src.SH_PGLN_TOTL_1_PG
       ,DST.SH_PGLN_TOTL_1_LN                                  --投资股页行数:src.SH_PGLN_TOTL_1_LN
       ,DST.SH_PGLN_TOTL_2_PG                                  --资格股页数-22:src.SH_PGLN_TOTL_2_PG
       ,DST.SH_PGLN_TOTL_2_LN                                  --资格股行数-22:src.SH_PGLN_TOTL_2_LN
       ,DST.SH_PGLN_TOTL_3_PG                                  --分红记录页数-3:src.SH_PGLN_TOTL_3_PG
       ,DST.SH_PGLN_TOTL_3_LN                                  --分红记录行数-3:src.SH_PGLN_TOTL_3_LN
       ,DST.SH_AENTR_DET_TOTL_1                                --投资股未登折明细数:src.SH_AENTR_DET_TOTL_1
       ,DST.SH_AENTR_DET_TOTL_2                                --资格股未登折明细数-2:src.SH_AENTR_DET_TOTL_2
       ,DST.SH_AENTR_DET_TOTL_3                                --资格股未登折明细数-3:src.SH_AENTR_DET_TOTL_3
       ,DST.SH_PRINTED_MAX_NO_1                                --投资股已打印最大序号-3:src.SH_PRINTED_MAX_NO_1
       ,DST.SH_PRINTED_MAX_NO_2                                --资格股已打印最大序号:src.SH_PRINTED_MAX_NO_2
       ,DST.SH_PRINTED_MAX_NO_3                                --分红记录已打印最大序号:src.SH_PRINTED_MAX_NO_3
       ,DST.SH_INT_ACCT_NO                                     --利息帐号:src.SH_INT_ACCT_NO
       ,DST.SH_ZG_LTM_ACCT_BAL                                 --资格股上年度股金股数:src.SH_ZG_LTM_ACCT_BAL
       ,DST.SH_ZG_LTM_DDP_PDT                                  --资格股上年度分红积数:src.SH_ZG_LTM_DDP_PDT
       ,DST.SH_TZ_LTM_ACCT_BAL                                 --投资股上年度股金股数:src.SH_TZ_LTM_ACCT_BAL
       ,DST.SH_TZ_LTM_DDP_PDT                                  --投资股上年度分红积数:src.SH_TZ_LTM_DDP_PDT
       ,DST.SH_LTM_DDP_DT                                      --上次分红日期:src.SH_LTM_DDP_DT
       ,DST.SH_LTM_REC_DT                                      --上次股权登记日期:src.SH_LTM_REC_DT
       ,DST.SH_PSWD_ERR_TIMES                                  --密码出错次数:src.SH_PSWD_ERR_TIMES
       ,DST.SH_QPSWD                                           --查询密码:src.SH_QPSWD
       ,DST.SH_PSWD_ERR_DT                                     --密码错误日期:src.SH_PSWD_ERR_DT
       ,DST.SH_ENC_TYP                                         --密码加密方式:src.SH_ENC_TYP
       ,DST.SH_ENCKEY_VER                                      --密钥版本:src.SH_ENCKEY_VER
       ,DST.SH_PSWD_DL_DT                                      --密码挂失日期:src.SH_PSWD_DL_DT
       ,DST.SH_PSWD_STS                                        --密码状态:src.SH_PSWD_STS
       ,DST.SH_LEGAL_INSTN_NO                                  --法人机构号:src.SH_LEGAL_INSTN_NO
       ,DST.SH_OPAC_INSTN_NO                                   --开户机构号:src.SH_OPAC_INSTN_NO
       ,DST.SH_OPAC_DT                                         --开户日期:src.SH_OPAC_DT
       ,DST.SH_OPAC_TLR_NO                                     --开户柜员号:src.SH_OPAC_TLR_NO
       ,DST.SH_CACCT_TLR_NO                                    --销户柜员号:src.SH_CACCT_TLR_NO
       ,DST.SH_CACCT_INSTN_NO                                  --销户机构号:src.SH_CACCT_INSTN_NO
       ,DST.SH_CACCT_DT                                        --销户日期:src.SH_CACCT_DT
       ,DST.SH_OPAC_PERM_NO                                    --开户许可证号:src.SH_OPAC_PERM_NO
       ,DST.SH_CERT_TYP                                        --证件种类:src.SH_CERT_TYP
       ,DST.SH_CERT_ID                                         --证件号码:src.SH_CERT_ID
       ,DST.SH_1LVL_BRH_ID                                     --省分行号:src.SH_1LVL_BRH_ID
       ,DST.SH_DB_PART_ID                                      --分区键:src.SH_DB_PART_ID
       ,DST.SH_CRPT_PIN                                        --加密后交易密码:src.SH_CRPT_PIN
       ,DST.SH_ACN_MEMO                                        --股金扩展字段:src.SH_ACN_MEMO
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_CI_CBOD_SHACNACN DST 
   LEFT JOIN F_CI_CBOD_SHACNACN_INNTMP1 SRC 
     ON SRC.SH_ACCT_NO          = DST.SH_ACCT_NO 
  WHERE SRC.SH_ACCT_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_SHACNACN_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_CBOD_SHACNACN/"+V_DT+".parquet"
UNION=F_CI_CBOD_SHACNACN_INNTMP2.unionAll(F_CI_CBOD_SHACNACN_INNTMP1)
F_CI_CBOD_SHACNACN_INNTMP1.cache()
F_CI_CBOD_SHACNACN_INNTMP2.cache()
nrowsi = F_CI_CBOD_SHACNACN_INNTMP1.count()
nrowsa = F_CI_CBOD_SHACNACN_INNTMP2.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_SHACNACN/*.parquet")
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_CBOD_SHACNACN_INNTMP1.unpersist()
F_CI_CBOD_SHACNACN_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CBOD_SHACNACN lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_CBOD_SHACNACN/"+V_DT_LD+".parquet /"+dbname+"/F_CI_CBOD_SHACNACN_BK/")

#备份

ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_SHACNACN_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CI_CBOD_SHACNACN/"+V_DT+".parquet /"+dbname+"/F_CI_CBOD_SHACNACN_BK/")
