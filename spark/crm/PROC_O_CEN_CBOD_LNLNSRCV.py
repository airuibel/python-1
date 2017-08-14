#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_LNLNSRCV').setMaster(sys.argv[2])
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

O_LN_CBOD_LNLNSRCV = sqlContext.read.parquet(hdfs+'/O_LN_CBOD_LNLNSRCV/*')
O_LN_CBOD_LNLNSRCV.registerTempTable("O_LN_CBOD_LNLNSRCV")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_LN_CBOD_LNLNSRCV = sqlContext.read.parquet(hdfs+'/F_LN_CBOD_LNLNSRCV_BK/'+V_DT_LD+'.parquet/*')
F_LN_CBOD_LNLNSRCV.registerTempTable("F_LN_CBOD_LNLNSRCV")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,FK_LNLNS_KEY            AS FK_LNLNS_KEY 
       ,LN_EVT_COD              AS LN_EVT_COD 
       ,LN_EVT_SQ_NO_N          AS LN_EVT_SQ_NO_N 
       ,LN_EVT_DT               AS LN_EVT_DT 
       ,LN_EVT_TM               AS LN_EVT_TM 
       ,LN_OPR_NO               AS LN_OPR_NO 
       ,LN_RMRK                 AS LN_RMRK 
       ,LN_EVT_STS              AS LN_EVT_STS 
       ,LN_BUSN_TYP             AS LN_BUSN_TYP 
       ,LN_DUE_DT_N             AS LN_DUE_DT_N 
       ,LN_EXTN_CTRT_NO         AS LN_EXTN_CTRT_NO 
       ,LN_NINTR_ACOR_STY       AS LN_NINTR_ACOR_STY 
       ,LN_NINTR_NEGO_COD       AS LN_NINTR_NEGO_COD 
       ,LN_NINTR_NEGO_SYMB      AS LN_NINTR_NEGO_SYMB 
       ,LN_INTR_TYP             AS LN_INTR_TYP 
       ,LN_INTR_ADJ_STRT_DT_N   AS LN_INTR_ADJ_STRT_DT_N 
       ,LN_DOC_NO               AS LN_DOC_NO 
       ,LN_DOC_TYP              AS LN_DOC_TYP 
       ,LN_DSCRP_COD            AS LN_DSCRP_COD 
       ,LN_OLN_DUE_DT_N         AS LN_OLN_DUE_DT_N 
       ,LN_APPL_DUE_DT_N        AS LN_APPL_DUE_DT_N 
       ,LN_BELONG_INSTN_COD     AS LN_BELONG_INSTN_COD 
       ,LN_ASS_OPUN_NO          AS LN_ASS_OPUN_NO 
       ,LN_FLST_OPUN_NO         AS LN_FLST_OPUN_NO 
       ,LN_DB_PART_ID           AS LN_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_LN_CBOD_LNLNSRCV A                                   --贷款展期事故档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_CBOD_LNLNSRCV_INNTMP1 = sqlContext.sql(sql)
F_LN_CBOD_LNLNSRCV_INNTMP1.registerTempTable("F_LN_CBOD_LNLNSRCV_INNTMP1")

#F_LN_CBOD_LNLNSRCV = sqlContext.read.parquet(hdfs+'/F_LN_CBOD_LNLNSRCV/*')
#F_LN_CBOD_LNLNSRCV.registerTempTable("F_LN_CBOD_LNLNSRCV")
sql = """
 SELECT DST.ETLDT                                               --源系统日期:src.ETLDT
       ,DST.FK_LNLNS_KEY                                       --FK_LNLNS_KEY:src.FK_LNLNS_KEY
       ,DST.LN_EVT_COD                                         --事故代码:src.LN_EVT_COD
       ,DST.LN_EVT_SQ_NO_N                                     --事故序号:src.LN_EVT_SQ_NO_N
       ,DST.LN_EVT_DT                                          --事故日期:src.LN_EVT_DT
       ,DST.LN_EVT_TM                                          --事故时间:src.LN_EVT_TM
       ,DST.LN_OPR_NO                                          --操作员号:src.LN_OPR_NO
       ,DST.LN_RMRK                                            --备注:src.LN_RMRK
       ,DST.LN_EVT_STS                                         --事故状态:src.LN_EVT_STS
       ,DST.LN_BUSN_TYP                                        --业务别:src.LN_BUSN_TYP
       ,DST.LN_DUE_DT_N                                        --到期日期:src.LN_DUE_DT_N
       ,DST.LN_EXTN_CTRT_NO                                    --展期合同编号:src.LN_EXTN_CTRT_NO
       ,DST.LN_NINTR_ACOR_STY                                  --新利率依据方式:src.LN_NINTR_ACOR_STY
       ,DST.LN_NINTR_NEGO_COD                                  --新利率加减码:src.LN_NINTR_NEGO_COD
       ,DST.LN_NINTR_NEGO_SYMB                                 --新利率加减符号:src.LN_NINTR_NEGO_SYMB
       ,DST.LN_INTR_TYP                                        --利率种类:src.LN_INTR_TYP
       ,DST.LN_INTR_ADJ_STRT_DT_N                              --利率调整起始日期:src.LN_INTR_ADJ_STRT_DT_N
       ,DST.LN_DOC_NO                                          --凭证号码:src.LN_DOC_NO
       ,DST.LN_DOC_TYP                                         --凭证种类:src.LN_DOC_TYP
       ,DST.LN_DSCRP_COD                                       --摘要代码:src.LN_DSCRP_COD
       ,DST.LN_OLN_DUE_DT_N                                    --原到期日期:src.LN_OLN_DUE_DT_N
       ,DST.LN_APPL_DUE_DT_N                                   --借款到期日:src.LN_APPL_DUE_DT_N
       ,DST.LN_BELONG_INSTN_COD                                --账户归属机构:src.LN_BELONG_INSTN_COD
       ,DST.LN_ASS_OPUN_NO                                     --考核机构号:src.LN_ASS_OPUN_NO
       ,DST.LN_FLST_OPUN_NO                                    --开户机构编号:src.LN_FLST_OPUN_NO
       ,DST.LN_DB_PART_ID                                      --分区键:src.LN_DB_PART_ID
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_LN_CBOD_LNLNSRCV DST 
   LEFT JOIN F_LN_CBOD_LNLNSRCV_INNTMP1 SRC 
     ON SRC.ETLDT               = DST.ETLDT 
    AND SRC.FK_LNLNS_KEY        = DST.FK_LNLNS_KEY 
    AND SRC.LN_EVT_COD          = DST.LN_EVT_COD 
    AND SRC.LN_EVT_SQ_NO_N      = DST.LN_EVT_SQ_NO_N 
  WHERE SRC.ETLDT IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_CBOD_LNLNSRCV_INNTMP2 = sqlContext.sql(sql)
dfn="F_LN_CBOD_LNLNSRCV/"+V_DT+".parquet"
UNION=F_LN_CBOD_LNLNSRCV_INNTMP2.unionAll(F_LN_CBOD_LNLNSRCV_INNTMP1)
F_LN_CBOD_LNLNSRCV_INNTMP1.cache()
F_LN_CBOD_LNLNSRCV_INNTMP2.cache()
nrowsi = F_LN_CBOD_LNLNSRCV_INNTMP1.count()
nrowsa = F_LN_CBOD_LNLNSRCV_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_LN_CBOD_LNLNSRCV_INNTMP1.unpersist()
F_LN_CBOD_LNLNSRCV_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_CBOD_LNLNSRCV lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_LN_CBOD_LNLNSRCV/"+V_DT_LD+".parquet /"+dbname+"/F_LN_CBOD_LNLNSRCV_BK/")
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_CBOD_LNLNSRCV/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_CBOD_LNLNSRCV_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_LN_CBOD_LNLNSRCV/"+V_DT+".parquet /"+dbname+"/F_LN_CBOD_LNLNSRCV_BK/")