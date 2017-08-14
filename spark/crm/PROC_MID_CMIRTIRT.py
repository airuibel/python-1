#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MID_CMIRTIRT').setMaster(sys.argv[2])
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

F_CM_CBOD_PDPRTTDP = sqlContext.read.parquet(hdfs+'/F_CM_CBOD_PDPRTTDP/*')
F_CM_CBOD_PDPRTTDP.registerTempTable("F_CM_CBOD_PDPRTTDP")
F_CM_CBOD_CMBCTBCT = sqlContext.read.parquet(hdfs+'/F_CM_CBOD_CMBCTBCT/*')
F_CM_CBOD_CMBCTBCT.registerTempTable("F_CM_CBOD_CMBCTBCT")
V_LOOKUP_CODE = sqlContext.read.parquet(hdfs+'/V_LOOKUP_CODE/*')
V_LOOKUP_CODE.registerTempTable("V_LOOKUP_CODE")
F_CM_CBOD_PDPRTSAP = sqlContext.read.parquet(hdfs+'/F_CM_CBOD_PDPRTSAP/*')
F_CM_CBOD_PDPRTSAP.registerTempTable("F_CM_CBOD_PDPRTSAP")
MID_CMIRTIRT_2 = sqlContext.read.parquet(hdfs+'/MID_CMIRTIRT_2/*')
MID_CMIRTIRT_2.registerTempTable("MID_CMIRTIRT_2")
F_CI_CBOD_PDPDPPDP = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_PDPDPPDP/*')
F_CI_CBOD_PDPDPPDP.registerTempTable("F_CI_CBOD_PDPDPPDP")
ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")

#任务[12] 001-05::
V_STEP = V_STEP + 1

MID_CMIRTIRT = sqlContext.read.parquet(hdfs+'/MID_CMIRTIRT/*')
MID_CMIRTIRT.registerTempTable("MID_CMIRTIRT")

sql = """
 SELECT CM_INTR1                AS CM_INTR1 
       ,CM_OPUN_COD             AS CM_OPUN_COD 
       ,CM_TYP                  AS CM_TYP 
       ,CM_AVL_DT               AS CM_AVL_DT 
       ,CM_CURR_COD             AS CM_CURR_COD 
       ,PD_CODE                 AS PD_CODE 
       ,'ST13'                  AS PD_DEP_PRD 
       ,FR_ID                   AS FR_ID 
   FROM MID_CMIRTIRT A                                         --
  WHERE PD_CODE                 = '999TD000200' 
    AND CM_TYP                  = '07' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_CMIRTIRT_INNTMP1 = sqlContext.sql(sql)
MID_CMIRTIRT_INNTMP1.registerTempTable("MID_CMIRTIRT_INNTMP1")

#MID_CMIRTIRT = sqlContext.read.parquet(hdfs+'/MID_CMIRTIRT/*')
#MID_CMIRTIRT.registerTempTable("MID_CMIRTIRT")
sql = """
 SELECT DST.CM_INTR1                                            --利率:src.CM_INTR1
       ,DST.CM_OPUN_COD                                        --机构号:src.CM_OPUN_COD
       ,DST.CM_TYP                                             --利率类型:src.CM_TYP
       ,DST.CM_AVL_DT                                          --利率日期:src.CM_AVL_DT
       ,DST.CM_CURR_COD                                        --币种:src.CM_CURR_COD
       ,DST.PD_CODE                                            --产品代码:src.PD_CODE
       ,DST.PD_DEP_PRD                                         --存期:src.PD_DEP_PRD
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM MID_CMIRTIRT DST 
   LEFT JOIN MID_CMIRTIRT_INNTMP1 SRC 
     ON SRC.CM_OPUN_COD         = DST.CM_OPUN_COD 
    AND SRC.CM_TYP              = DST.CM_TYP 
    AND SRC.CM_AVL_DT           = DST.CM_AVL_DT 
    AND SRC.CM_CURR_COD         = DST.CM_CURR_COD 
    AND SRC.PD_CODE             = DST.PD_CODE 
    AND SRC.FR_ID               = DST.FR_ID 
  WHERE SRC.CM_OPUN_COD IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_CMIRTIRT_INNTMP2 = sqlContext.sql(sql)
dfn="MID_CMIRTIRT/"+V_DT+".parquet"
MID_CMIRTIRT_INNTMP2.unionAll(MID_CMIRTIRT_INNTMP1)
MID_CMIRTIRT_INNTMP1.cache()
MID_CMIRTIRT_INNTMP2.cache()
nrowsi = MID_CMIRTIRT_INNTMP1.count()
nrowsa = MID_CMIRTIRT_INNTMP2.count()
MID_CMIRTIRT_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
MID_CMIRTIRT_INNTMP1.unpersist()
MID_CMIRTIRT_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_CMIRTIRT lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/MID_CMIRTIRT/"+V_DT_LD+".parquet /"+dbname+"/MID_CMIRTIRT_BK/")

#任务[11] 001-04::
V_STEP = V_STEP + 1

MID_CMIRTIRT = sqlContext.read.parquet(hdfs+'/MID_CMIRTIRT/*')
MID_CMIRTIRT.registerTempTable("MID_CMIRTIRT")

sql = """
 SELECT A.CM_INTR1                AS CM_INTR1 
       ,B.CM_OPUN_COD           AS CM_OPUN_COD 
       ,A.CM_TYP                  AS CM_TYP 
       ,A.CM_AVL_DT               AS CM_AVL_DT 
       ,A.CM_CURR_COD             AS CM_CURR_COD 
       ,A.PD_CODE               AS PD_CODE 
       ,''                      AS PD_DEP_PRD 
       ,COALESCE(G.FR_ID, 'UNK')                       AS FR_ID 
   FROM F_CM_CBOD_CMBCTBCT B                                   --营业单位档
   LEFT JOIN MID_CMIRTIRT C                                    --利率中间表1
     ON B.CM_OPUN_COD           = C.CM_OPUN_COD 
   LEFT JOIN ADMIN_AUTH_ORG G                                  --机构表
     ON B.CM_OPUN_COD           = G.ORG_ID 
  INNER JOIN MID_CMIRTIRT A                                    --利率中间表1
     ON A.CM_OPUN_COD           = '999999999' 
    AND A.CM_TYP                = C.CM_TYP 
    AND A.CM_CURR_COD           = C.CM_CURR_COD 
    AND A.PD_CODE               = C.PD_CODE 
  WHERE C.CM_CURR_COD IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_CMIRTIRT = sqlContext.sql(sql)
MID_CMIRTIRT.registerTempTable("MID_CMIRTIRT")
dfn="MID_CMIRTIRT/"+V_DT+".parquet"
MID_CMIRTIRT.cache()
nrows = MID_CMIRTIRT.count()
MID_CMIRTIRT.write.save(path=hdfs + '/' + dfn, mode='append')
MID_CMIRTIRT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_CMIRTIRT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-03::
V_STEP = V_STEP + 1

MID_CMIRTIRT = sqlContext.read.parquet(hdfs+'/MID_CMIRTIRT/*')
MID_CMIRTIRT.registerTempTable("MID_CMIRTIRT")

sql = """
 SELECT A.CM_INTR1              AS CM_INTR1 
       ,B.CM_OPUN_COD           AS CM_OPUN_COD 
       ,A.CM_TYP                AS CM_TYP 
       ,A.CM_AVL_DT             AS CM_AVL_DT 
       ,A.CM_CURR_COD           AS CM_CURR_COD 
       ,A.PD_CODE               AS PD_CODE 
       ,A.PD_DEP_PRD            AS PD_DEP_PRD 
       ,SUBSTR(B.CM_1LVL_BRH_ID, 1, 3)                       AS FR_ID 
   FROM F_CM_CBOD_CMBCTBCT B                                   --营业单位档
  INNER JOIN MID_CMIRTIRT A                                    --利率中间表1
     ON A.CM_OPUN_COD           = CONCAT(B.CM_1LVL_BRH_ID, '999999') 
   LEFT JOIN MID_CMIRTIRT C                                    --利率中间表1
     ON A.CM_CURR_COD           = C.CM_CURR_COD 
    AND A.PD_CODE               = C.PD_CODE 
    AND B.CM_OPUN_COD           = C.CM_OPUN_COD 
    AND A.PD_DEP_PRD            = C.PD_DEP_PRD 
  WHERE C.CM_CURR_COD IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_CMIRTIRT = sqlContext.sql(sql)
MID_CMIRTIRT.registerTempTable("MID_CMIRTIRT")
dfn="MID_CMIRTIRT/"+V_DT+".parquet"
MID_CMIRTIRT.cache()
nrows = MID_CMIRTIRT.count()
MID_CMIRTIRT.write.save(path=hdfs + '/' + dfn, mode='append')
MID_CMIRTIRT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_CMIRTIRT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[11] 001-02::
V_STEP = V_STEP + 1

MID_CMIRTIRT = sqlContext.read.parquet(hdfs+'/MID_CMIRTIRT/*')
MID_CMIRTIRT.registerTempTable("MID_CMIRTIRT")

sql = """
 SELECT C.CM_INTR1              AS CM_INTR1 
       ,C.CM_OPUN_COD           AS CM_OPUN_COD 
       ,C.CM_INTR_TYP           AS CM_TYP 
       ,C.CM_AVL_DT             AS CM_AVL_DT 
       ,C.CM_CURR_COD           AS CM_CURR_COD 
       ,D.PD_CODE               AS PD_CODE 
       ,CASE B.PD_DEP_PRD WHEN '00000' THEN 'ST01' WHEN '00001' THEN 'ST02' WHEN '00007' THEN 'ST03' WHEN '00030' THEN 'ST04' WHEN '00090' THEN 'ST05' WHEN '00180' THEN 'ST06' WHEN '00360' THEN 'ST07' WHEN '00720' THEN 'ST08' WHEN '01080' THEN 'ST09' WHEN '01800' THEN 'ST10' WHEN '02160' THEN 'ST11' WHEN '02700' THEN 'ST12' WHEN '02880' THEN 'ST13' ELSE B.PD_DEP_PRD END                     AS PD_DEP_PRD 
       ,SUBSTR(C.CM_OPUN_COD, 1, 3)                       AS FR_ID 
   FROM MID_CMIRTIRT_2 C                                       --利率中间表2
  INNER JOIN F_CM_CBOD_PDPRTTDP B                              --活期利息部件
     ON B.PD_WORD_BANK_COD      = '888' 
    AND B.PD_WORD_PROV_COD      = '999' 
    AND B.PD_CURR_IDEN          = '0' 
    AND B.PD_INTR_TYP           = C.CM_INTR_TYP 
    AND B.PD_CUR_COD            = C.CM_CURR_COD 
  INNER JOIN F_CI_CBOD_PDPDPPDP D                              --核心产品表
     ON B.PD_WORD_CLSFN         = SUBSTR(D.PD_LINK_PRT1, 1, 3) 
    AND B.PD_PD_SEQ             = SUBSTR(D.PD_LINK_PRT1, 4, LENGTH(D.PD_LINK_PRT1)) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_CMIRTIRT = sqlContext.sql(sql)
MID_CMIRTIRT.registerTempTable("MID_CMIRTIRT")
dfn="MID_CMIRTIRT/"+V_DT+".parquet"
MID_CMIRTIRT.cache()
nrows = MID_CMIRTIRT.count()
MID_CMIRTIRT.write.save(path=hdfs + '/' + dfn, mode='append')
MID_CMIRTIRT.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_CMIRTIRT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-01::
V_STEP = V_STEP + 1

MID_CMIRTIRT = sqlContext.read.parquet(hdfs+'/MID_CMIRTIRT/*')
MID_CMIRTIRT.registerTempTable("MID_CMIRTIRT")

sql = """
 SELECT C.CM_INTR1              AS CM_INTR1 
       ,C.CM_OPUN_COD           AS CM_OPUN_COD 
       ,CM_INTR_TYP             AS CM_TYP 
       ,CM_AVL_DT               AS CM_AVL_DT 
       ,CM_CURR_COD             AS CM_CURR_COD 
       ,D.PD_CODE               AS PD_CODE 
       ,''                      AS PD_DEP_PRD 
       ,SUBSTR(C.CM_OPUN_COD, 1, 3)                       AS FR_ID 
   FROM MID_CMIRTIRT_2 C                                       --利率中间表2
  INNER JOIN F_CM_CBOD_PDPRTSAP F                              --活期利息部件
     ON F.PD_INTR_TYP           = C.CM_INTR_TYP 
  INNER JOIN F_CI_CBOD_PDPDPPDP D                              --核心产品表
     ON F.PD_WORD_CLSFN         = SUBSTR(D.PD_LINK_PRT3, 1, 3) 
    AND F.PD_PD_SEQ             = SUBSTR(D.PD_LINK_PRT3, 4, LENGTH(D.PD_LINK_PRT3)) 
  INNER JOIN V_LOOKUP_CODE E                                   --码值表
     ON E.TARGET_VALUE          = C.CM_CURR_COD 
    AND E.SOURCE_VALUE          = F.PD_CUR_COD 
  WHERE F.PD_WORD_BANK_COD      = '888' 
    AND F.PD_WORD_PROV_COD      = '999' 
    AND F.PD_CURR_IDEN          = '0' 
    AND E.TYP_ID                = 'CBOD_CURR_CODE_0020' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_CMIRTIRT = sqlContext.sql(sql)
MID_CMIRTIRT.registerTempTable("MID_CMIRTIRT")
dfn="MID_CMIRTIRT/"+V_DT+".parquet"
MID_CMIRTIRT.cache()
nrows = MID_CMIRTIRT.count()
MID_CMIRTIRT.write.save(path=hdfs + '/' + dfn, mode='append')
MID_CMIRTIRT.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_CMIRTIRT/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_CMIRTIRT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
