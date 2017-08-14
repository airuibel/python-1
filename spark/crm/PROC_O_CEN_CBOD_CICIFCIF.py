#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_CICIFCIF').setMaster(sys.argv[2])
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

O_CI_CBOD_CICIFCIF = sqlContext.read.parquet(hdfs+'/O_CI_CBOD_CICIFCIF/*')
O_CI_CBOD_CICIFCIF.registerTempTable("O_CI_CBOD_CICIFCIF")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CI_CBOD_CICIFCIF = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIFCIF/*')
F_CI_CBOD_CICIFCIF.registerTempTable("F_CI_CBOD_CICIFCIF")

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
       ,CI_MRG_COND             AS CI_MRG_COND 
       ,CI_CUST_TTL             AS CI_CUST_TTL 
       ,CI_SEX                  AS CI_SEX 
       ,CI_PPL_COD              AS CI_PPL_COD 
       ,CI_MAX_EDUC_LVL_COD     AS CI_MAX_EDUC_LVL_COD 
       ,CI_OCCUP_COD            AS CI_OCCUP_COD 
       ,CI_POSN                 AS CI_POSN 
       ,CI_TITL                 AS CI_TITL 
       ,CI_WOKG_UNIT_NAME       AS CI_WOKG_UNIT_NAME 
       ,CI_DEPEND_NO_N          AS CI_DEPEND_NO_N 
       ,CI_PECON_RESUR          AS CI_PECON_RESUR 
       ,CI_OTH_ECON_RSUR        AS CI_OTH_ECON_RSUR 
       ,CI_MN_INCOM             AS CI_MN_INCOM 
       ,CI_CRD_UDTK_TM_N        AS CI_CRD_UDTK_TM_N 
       ,CI_CF_UNIT_TYP          AS CI_CF_UNIT_TYP 
       ,CI_HAVE_WORK_YEAR       AS CI_HAVE_WORK_YEAR 
       ,CI_CON_CUST_FLG         AS CI_CON_CUST_FLG 
       ,CI_ECON_TYP             AS CI_ECON_TYP 
       ,CI_HOUSE_TYPE           AS CI_HOUSE_TYPE 
       ,CI_REGISTER             AS CI_REGISTER 
       ,CI_CRT_SCT_N            AS CI_CRT_SCT_N 
       ,CI_UNIT_CHN_INIL        AS CI_UNIT_CHN_INIL 
       ,CI_MANG_DEPT            AS CI_MANG_DEPT 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_CI_CBOD_CICIFCIF A                                   --对私客户信息主档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_CICIFCIF_INNTMP1 = sqlContext.sql(sql)
F_CI_CBOD_CICIFCIF_INNTMP1.registerTempTable("F_CI_CBOD_CICIFCIF_INNTMP1")

#F_CI_CBOD_CICIFCIF = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_CICIFCIF/*')
#F_CI_CBOD_CICIFCIF.registerTempTable("F_CI_CBOD_CICIFCIF")
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
       ,DST.CI_MRG_COND                                        --婚姻状况:src.CI_MRG_COND
       ,DST.CI_CUST_TTL                                        --客户称呼:src.CI_CUST_TTL
       ,DST.CI_SEX                                             --性别:src.CI_SEX
       ,DST.CI_PPL_COD                                         --民族代码:src.CI_PPL_COD
       ,DST.CI_MAX_EDUC_LVL_COD                                --最高学历代码:src.CI_MAX_EDUC_LVL_COD
       ,DST.CI_OCCUP_COD                                       --职业代码:src.CI_OCCUP_COD
       ,DST.CI_POSN                                            --职务:src.CI_POSN
       ,DST.CI_TITL                                            --职称:src.CI_TITL
       ,DST.CI_WOKG_UNIT_NAME                                  --工作单位名称:src.CI_WOKG_UNIT_NAME
       ,DST.CI_DEPEND_NO_N                                     --供养人数:src.CI_DEPEND_NO_N
       ,DST.CI_PECON_RESUR                                     --主要经济来源:src.CI_PECON_RESUR
       ,DST.CI_OTH_ECON_RSUR                                   --其他经济来源:src.CI_OTH_ECON_RSUR
       ,DST.CI_MN_INCOM                                        --月收入:src.CI_MN_INCOM
       ,DST.CI_CRD_UDTK_TM_N                                   --信用卡担保次数:src.CI_CRD_UDTK_TM_N
       ,DST.CI_CF_UNIT_TYP                                     --单位性质:src.CI_CF_UNIT_TYP
       ,DST.CI_HAVE_WORK_YEAR                                  --参加工作年份:src.CI_HAVE_WORK_YEAR
       ,DST.CI_CON_CUST_FLG                                    --关注客户标志:src.CI_CON_CUST_FLG
       ,DST.CI_ECON_TYP                                        --经济类型:src.CI_ECON_TYP
       ,DST.CI_HOUSE_TYPE                                      --住宅类型:src.CI_HOUSE_TYPE
       ,DST.CI_REGISTER                                        --户籍:src.CI_REGISTER
       ,DST.CI_CRT_SCT_N                                       --创建时间:src.CI_CRT_SCT_N
       ,DST.CI_UNIT_CHN_INIL                                   --办公电话:src.CI_UNIT_CHN_INIL
       ,DST.CI_MANG_DEPT                                       --总负债:src.CI_MANG_DEPT
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_CI_CBOD_CICIFCIF DST 
   LEFT JOIN F_CI_CBOD_CICIFCIF_INNTMP1 SRC 
     ON SRC.CI_CUST_NO          = DST.CI_CUST_NO 
  WHERE SRC.CI_CUST_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_CBOD_CICIFCIF_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_CBOD_CICIFCIF/"+V_DT+".parquet"
F_CI_CBOD_CICIFCIF_INNTMP2=F_CI_CBOD_CICIFCIF_INNTMP2.unionAll(F_CI_CBOD_CICIFCIF_INNTMP1)
F_CI_CBOD_CICIFCIF_INNTMP1.cache()
F_CI_CBOD_CICIFCIF_INNTMP2.cache()
nrowsi = F_CI_CBOD_CICIFCIF_INNTMP1.count()
nrowsa = F_CI_CBOD_CICIFCIF_INNTMP2.count()
F_CI_CBOD_CICIFCIF_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_CBOD_CICIFCIF_INNTMP1.unpersist()
F_CI_CBOD_CICIFCIF_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_CBOD_CICIFCIF lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_CBOD_CICIFCIF/"+V_DT_LD+".parquet /"+dbname+"/F_CI_CBOD_CICIFCIF_BK/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_CBOD_CICIFCIF/"+V_DT_LD+".parquet")