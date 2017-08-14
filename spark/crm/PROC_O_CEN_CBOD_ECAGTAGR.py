#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_ECAGTAGR').setMaster(sys.argv[2])
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
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_ECAGTAGR/"+V_DT_LD+".parquet")
O_CI_CBOD_ECAGTAGR = sqlContext.read.parquet(hdfs+'/O_CI_CBOD_ECAGTAGR/*')
O_CI_CBOD_ECAGTAGR.registerTempTable("O_CI_CBOD_ECAGTAGR")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CI_CBOD_ECAGTAGR = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECAGTAGR_BK/'+V_DT_LD+'.parquet')
F_CI_CBOD_ECAGTAGR.registerTempTable("F_CI_CBOD_ECAGTAGR")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,EC_BNK_NO               AS EC_BNK_NO 
       ,EC_SEQ_NO               AS EC_SEQ_NO 
       ,EC_CUST_ID              AS EC_CUST_ID 
       ,EC_CUST_NO              AS EC_CUST_NO 
       ,EC_CUST_TYP             AS EC_CUST_TYP 
       ,EC_AGR_NO               AS EC_AGR_NO 
       ,EC_ACCT_1               AS EC_ACCT_1 
       ,EC_TRD_CURR_COD         AS EC_TRD_CURR_COD 
       ,EC_AGR_TYP              AS EC_AGR_TYP 
       ,EC_AGR_SUBTYP           AS EC_AGR_SUBTYP 
       ,EC_PDT_COD              AS EC_PDT_COD 
       ,EC_AGR_SGNDAT_N         AS EC_AGR_SGNDAT_N 
       ,EC_OPAC_ORG             AS EC_OPAC_ORG 
       ,EC_SGN_TLR_NO           AS EC_SGN_TLR_NO 
       ,EC_AGR_STS              AS EC_AGR_STS 
       ,EC_CRT_SYS              AS EC_CRT_SYS 
       ,EC_CRT_ORG              AS EC_CRT_ORG 
       ,EC_CRT_SCT_N            AS EC_CRT_SCT_N 
       ,EC_CRT_OPR              AS EC_CRT_OPR 
       ,EC_UPD_SYS              AS EC_UPD_SYS 
       ,EC_UPD_OPR              AS EC_UPD_OPR 
       ,EC_UPD_ORG              AS EC_UPD_ORG 
       ,EC_DB_PART_ID           AS EC_DB_PART_ID 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CI_CBOD_ECAGTAGR A                                   --客户协议信息档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_ECAGTAGR_INNTMP1 = sqlContext.sql(sql)
F_CI_CBOD_ECAGTAGR_INNTMP1.registerTempTable("F_CI_CBOD_ECAGTAGR_INNTMP1")

#F_CI_CBOD_ECAGTAGR = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECAGTAGR/*')
#F_CI_CBOD_ECAGTAGR.registerTempTable("F_CI_CBOD_ECAGTAGR")
sql = """
 SELECT DST.ETLDT                                               --平台日期:src.ETLDT
       ,DST.EC_BNK_NO                                          --法人银行号:src.EC_BNK_NO
       ,DST.EC_SEQ_NO                                          --ECIF顺序号:src.EC_SEQ_NO
       ,DST.EC_CUST_ID                                         --开户公司单位代码(20位):src.EC_CUST_ID
       ,DST.EC_CUST_NO                                         --客户编号:src.EC_CUST_NO
       ,DST.EC_CUST_TYP                                        --客户类别:src.EC_CUST_TYP
       ,DST.EC_AGR_NO                                          --协议编号(AGR):src.EC_AGR_NO
       ,DST.EC_ACCT_1                                          --帐号1(CB):src.EC_ACCT_1
       ,DST.EC_TRD_CURR_COD                                    --交易币种:src.EC_TRD_CURR_COD
       ,DST.EC_AGR_TYP                                         --协议类型:src.EC_AGR_TYP
       ,DST.EC_AGR_SUBTYP                                      --协议子类型(4位):src.EC_AGR_SUBTYP
       ,DST.EC_PDT_COD                                         --贵金属产品代码2:src.EC_PDT_COD
       ,DST.EC_AGR_SGNDAT_N                                    --签订日期2:src.EC_AGR_SGNDAT_N
       ,DST.EC_OPAC_ORG                                        --开户机构/签约机构:src.EC_OPAC_ORG
       ,DST.EC_SGN_TLR_NO                                      --经办柜员2:src.EC_SGN_TLR_NO
       ,DST.EC_AGR_STS                                         --协议状态:src.EC_AGR_STS
       ,DST.EC_CRT_SYS                                         --创建系统:src.EC_CRT_SYS
       ,DST.EC_CRT_ORG                                         --设置机构:src.EC_CRT_ORG
       ,DST.EC_CRT_SCT_N                                       --创建时间(8位):src.EC_CRT_SCT_N
       ,DST.EC_CRT_OPR                                         --创建人:src.EC_CRT_OPR
       ,DST.EC_UPD_SYS                                         --更新系统:src.EC_UPD_SYS
       ,DST.EC_UPD_OPR                                         --更新人:src.EC_UPD_OPR
       ,DST.EC_UPD_ORG                                         --更新机构:src.EC_UPD_ORG
       ,DST.EC_DB_PART_ID                                      --分区键:src.EC_DB_PART_ID
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_CI_CBOD_ECAGTAGR DST 
   LEFT JOIN F_CI_CBOD_ECAGTAGR_INNTMP1 SRC 
     ON SRC.EC_BNK_NO           = DST.EC_BNK_NO 
    AND SRC.EC_SEQ_NO           = DST.EC_SEQ_NO 
  WHERE SRC.EC_BNK_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_ECAGTAGR_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_CBOD_ECAGTAGR/"+V_DT+".parquet"
F_CI_CBOD_ECAGTAGR_INNTMP2=F_CI_CBOD_ECAGTAGR_INNTMP2.unionAll(F_CI_CBOD_ECAGTAGR_INNTMP1)
F_CI_CBOD_ECAGTAGR_INNTMP1.cache()
F_CI_CBOD_ECAGTAGR_INNTMP2.cache()
nrowsi = F_CI_CBOD_ECAGTAGR_INNTMP1.count()
nrowsa = F_CI_CBOD_ECAGTAGR_INNTMP2.count()
F_CI_CBOD_ECAGTAGR_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_CBOD_ECAGTAGR_INNTMP1.unpersist()
F_CI_CBOD_ECAGTAGR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CBOD_ECAGTAGR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_ECAGTAGR_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_CI_CBOD_ECAGTAGR/"+V_DT+".parquet /"+dbname+"/F_CI_CBOD_ECAGTAGR_BK/")
