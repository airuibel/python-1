#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CICIECIF').setMaster(sys.argv[2])
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

O_CI_CBOD_CICIECIF = sqlContext.read.parquet(hdfs+'/O_CI_CBOD_CICIECIF/*')
O_CI_CBOD_CICIECIF.registerTempTable("O_CI_CBOD_CICIECIF")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CI_CBOD_CICIECIF = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIECIF/*')
F_CI_CBOD_CICIECIF.registerTempTable("F_CI_CBOD_CICIECIF")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,CI_CUST_NO              AS CI_CUST_NO 
       ,CI_CUST_TYP             AS CI_CUST_TYP 
       ,CI_CHN_NAME             AS CI_CHN_NAME 
       ,CI_REG_BIRDY_N          AS CI_REG_BIRDY_N 
       ,CI_ENG_SPL_NAME_1       AS CI_ENG_SPL_NAME_1 
       ,CI_ENG_SPL_NAME_2       AS CI_ENG_SPL_NAME_2 
       ,CI_RESCNTY              AS CI_RESCNTY 
       ,CI_RES_CNTY             AS CI_RES_CNTY 
       ,CI_FLST_TLR_NO          AS CI_FLST_TLR_NO 
       ,CI_FLST_DT_N            AS CI_FLST_DT_N 
       ,CI_LTST_MNTN_OPR_NO     AS CI_LTST_MNTN_OPR_NO 
       ,CI_LTST_MNTN_DT_N       AS CI_LTST_MNTN_DT_N 
       ,CI_LTST_AWBK_1          AS CI_LTST_AWBK_1 
       ,CI_LTST_AWBK_2          AS CI_LTST_AWBK_2 
       ,CI_LTST_AWBK_3          AS CI_LTST_AWBK_3 
       ,CI_LTST_AWBK_4          AS CI_LTST_AWBK_4 
       ,CI_UNIT_CHN_INIL        AS CI_UNIT_CHN_INIL 
       ,CI_TABS_REG_NO          AS CI_TABS_REG_NO 
       ,CI_TAXR_SCAL            AS CI_TAXR_SCAL 
       ,CI_ADMN_TYP             AS CI_ADMN_TYP 
       ,CI_BUSNTP               AS CI_BUSNTP 
       ,CI_BLNG_SYS             AS CI_BLNG_SYS 
       ,CI_ECON_CHAR            AS CI_ECON_CHAR 
       ,CI_MANG_DEPT            AS CI_MANG_DEPT 
       ,CI_REG_CAP              AS CI_REG_CAP 
       ,CI_CAP_INHD             AS CI_CAP_INHD 
       ,CI_ENTP_SCAL            AS CI_ENTP_SCAL 
       ,CI_ENTP_QUAL_COD        AS CI_ENTP_QUAL_COD 
       ,CI_FCURR_BAL_CONTR      AS CI_FCURR_BAL_CONTR 
       ,CI_APRV_LN_CRLMT        AS CI_APRV_LN_CRLMT 
       ,CI_APRV_YR              AS CI_APRV_YR 
       ,CI_APRV_FILE_NO         AS CI_APRV_FILE_NO 
       ,CI_FUND_PIC_DEPT        AS CI_FUND_PIC_DEPT 
       ,CI_LEDG_PDT             AS CI_LEDG_PDT 
       ,CI_OPAC_PERM_NO         AS CI_OPAC_PERM_NO 
       ,CI_CRLMT_NO_MAX         AS CI_CRLMT_NO_MAX 
       ,CI_REG_CAP_CURR_COD     AS CI_REG_CAP_CURR_COD 
       ,CI_CON_CUST_FLG         AS CI_CON_CUST_FLG 
       ,CI_CARD_CRLMT_NO        AS CI_CARD_CRLMT_NO 
       ,CI_CRT_SYS              AS CI_CRT_SYS 
       ,CI_CRT_SCT_N            AS CI_CRT_SCT_N 
       ,CI_CRT_ORG              AS CI_CRT_ORG 
       ,CI_UPD_SYS              AS CI_UPD_SYS 
       ,CI_UPD_ORG              AS CI_UPD_ORG 
       ,CI_DB_PART_ID           AS CI_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CI_CBOD_CICIECIF A                                   --单位客户信息档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_CICIECIF_INNTMP1 = sqlContext.sql(sql)
F_CI_CBOD_CICIECIF_INNTMP1.registerTempTable("F_CI_CBOD_CICIECIF_INNTMP1")

#F_CI_CBOD_CICIECIF = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIECIF/*')
#F_CI_CBOD_CICIECIF.registerTempTable("F_CI_CBOD_CICIECIF")
sql = """
 SELECT DST.ETLDT                                               --源系统日期:src.ETLDT
       ,DST.CI_CUST_NO                                         --客户编号:src.CI_CUST_NO
       ,DST.CI_CUST_TYP                                        --客户类别:src.CI_CUST_TYP
       ,DST.CI_CHN_NAME                                        --中文名称:src.CI_CHN_NAME
       ,DST.CI_REG_BIRDY_N                                     --注册/出生日期:src.CI_REG_BIRDY_N
       ,DST.CI_ENG_SPL_NAME_1                                  --英文/拼音名称1:src.CI_ENG_SPL_NAME_1
       ,DST.CI_ENG_SPL_NAME_2                                  --英文/拼音名称2:src.CI_ENG_SPL_NAME_2
       ,DST.CI_RESCNTY                                         --居住国:src.CI_RESCNTY
       ,DST.CI_RES_CNTY                                        --所在国:src.CI_RES_CNTY
       ,DST.CI_FLST_TLR_NO                                     --建档柜员代号:src.CI_FLST_TLR_NO
       ,DST.CI_FLST_DT_N                                       --建档日期:src.CI_FLST_DT_N
       ,DST.CI_LTST_MNTN_OPR_NO                                --最近维护操作员号:src.CI_LTST_MNTN_OPR_NO
       ,DST.CI_LTST_MNTN_DT_N                                  --最近维护日期(LTST):src.CI_LTST_MNTN_DT_N
       ,DST.CI_LTST_AWBK_1                                     --最近往来银行1:src.CI_LTST_AWBK_1
       ,DST.CI_LTST_AWBK_2                                     --最近往来银行2:src.CI_LTST_AWBK_2
       ,DST.CI_LTST_AWBK_3                                     --最近往来银行3:src.CI_LTST_AWBK_3
       ,DST.CI_LTST_AWBK_4                                     --最近往来银行4:src.CI_LTST_AWBK_4
       ,DST.CI_UNIT_CHN_INIL                                   --单位中文简称:src.CI_UNIT_CHN_INIL
       ,DST.CI_TABS_REG_NO                                     --税务登记编号:src.CI_TABS_REG_NO
       ,DST.CI_TAXR_SCAL                                       --纳税人规模:src.CI_TAXR_SCAL
       ,DST.CI_ADMN_TYP                                        --经营种类:src.CI_ADMN_TYP
       ,DST.CI_BUSNTP                                          --行业别:src.CI_BUSNTP
       ,DST.CI_BLNG_SYS                                        --所属系统:src.CI_BLNG_SYS
       ,DST.CI_ECON_CHAR                                       --经济性质:src.CI_ECON_CHAR
       ,DST.CI_MANG_DEPT                                       --上级主管部门:src.CI_MANG_DEPT
       ,DST.CI_REG_CAP                                         --注册资金:src.CI_REG_CAP
       ,DST.CI_CAP_INHD                                        --实收资本:src.CI_CAP_INHD
       ,DST.CI_ENTP_SCAL                                       --企业规模:src.CI_ENTP_SCAL
       ,DST.CI_ENTP_QUAL_COD                                   --企业资质等级:src.CI_ENTP_QUAL_COD
       ,DST.CI_FCURR_BAL_CONTR                                 --外币余额限制:src.CI_FCURR_BAL_CONTR
       ,DST.CI_APRV_LN_CRLMT                                   --批准贷款额度:src.CI_APRV_LN_CRLMT
       ,DST.CI_APRV_YR                                         --批准年度:src.CI_APRV_YR
       ,DST.CI_APRV_FILE_NO                                    --批准文号:src.CI_APRV_FILE_NO
       ,DST.CI_FUND_PIC_DEPT                                   --资金主管部门:src.CI_FUND_PIC_DEPT
       ,DST.CI_LEDG_PDT                                        --主导产品:src.CI_LEDG_PDT
       ,DST.CI_OPAC_PERM_NO                                    --开户许可证号:src.CI_OPAC_PERM_NO
       ,DST.CI_CRLMT_NO_MAX                                    --贷款额度最大编号:src.CI_CRLMT_NO_MAX
       ,DST.CI_REG_CAP_CURR_COD                                --注册资金币别:src.CI_REG_CAP_CURR_COD
       ,DST.CI_CON_CUST_FLG                                    --关注客户标志:src.CI_CON_CUST_FLG
       ,DST.CI_CARD_CRLMT_NO                                   --信用卡额度编号:src.CI_CARD_CRLMT_NO
       ,DST.CI_CRT_SYS                                         --创建系统:src.CI_CRT_SYS
       ,DST.CI_CRT_SCT_N                                       --创建时间:src.CI_CRT_SCT_N
       ,DST.CI_CRT_ORG                                         --创建机构:src.CI_CRT_ORG
       ,DST.CI_UPD_SYS                                         --更新系统:src.CI_UPD_SYS
       ,DST.CI_UPD_ORG                                         --更新机构:src.CI_UPD_ORG
       ,DST.CI_DB_PART_ID                                      --分区键:src.CI_DB_PART_ID
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_CI_CBOD_CICIECIF DST 
   LEFT JOIN F_CI_CBOD_CICIECIF_INNTMP1 SRC 
     ON SRC.CI_CUST_NO          = DST.CI_CUST_NO 
  WHERE SRC.CI_CUST_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_CICIECIF_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_CBOD_CICIECIF/"+V_DT+".parquet"
F_CI_CBOD_CICIECIF_INNTMP2=F_CI_CBOD_CICIECIF_INNTMP2.unionAll(F_CI_CBOD_CICIECIF_INNTMP1)
F_CI_CBOD_CICIECIF_INNTMP1.cache()
F_CI_CBOD_CICIECIF_INNTMP2.cache()
nrowsi = F_CI_CBOD_CICIECIF_INNTMP1.count()
nrowsa = F_CI_CBOD_CICIECIF_INNTMP2.count()
F_CI_CBOD_CICIECIF_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_CBOD_CICIECIF_INNTMP1.unpersist()
F_CI_CBOD_CICIECIF_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CBOD_CICIECIF lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_CBOD_CICIECIF/"+V_DT_LD+".parquet /"+dbname+"/F_CI_CBOD_CICIECIF_BK/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_CICIECIF/"+V_DT_LD+".parquet")