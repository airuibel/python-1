#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_MID_CMIRTIRT_2').setMaster(sys.argv[2])
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

#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
OCRM_F_FID = sqlContext.read.parquet(hdfs+'/OCRM_F_FID/*')
OCRM_F_FID.registerTempTable("OCRM_F_FID")

#目标表：
#F_CM_CBOD_CMIRTIRT 增该表 从BK读取前一天数据
F_CM_CBOD_CMIRTIRT = sqlContext.read.parquet(hdfs+'/F_CM_CBOD_CMIRTIRT_BK/'+V_DT_LD+'.parquet')
F_CM_CBOD_CMIRTIRT.registerTempTable("F_CM_CBOD_CMIRTIRT")
#MID_CMIRTIRT_2 全量


#任务[21] 001-01::清除不在法人机构内的法人
V_STEP = V_STEP + 1

sql = """
 SELECT A.ETLDT                 AS ETLDT 
       ,A.CM_OPUN_COD           AS CM_OPUN_COD 
       ,A.CM_BUSN_TYP           AS CM_BUSN_TYP 
       ,A.CM_CURR_COD           AS CM_CURR_COD 
       ,A.CM_INTR_TYP           AS CM_INTR_TYP 
       ,A.CM_MASS_AMT_RANG      AS CM_MASS_AMT_RANG 
       ,A.CM_AVL_DT             AS CM_AVL_DT 
       ,A.CM_INTR1              AS CM_INTR1 
       ,A.CM_INTR2              AS CM_INTR2 
       ,A.CM_DOC_NO_CM          AS CM_DOC_NO_CM 
       ,A.CM_INTR_DSCRP         AS CM_INTR_DSCRP 
       ,A.CM_IRT_STS            AS CM_IRT_STS 
       ,A.FR_ID                 AS FR_ID 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
   FROM F_CM_CBOD_CMIRTIRT A                                   --利率主档
   LEFT JOIN OCRM_F_FID B                                      --法人表
     ON SUBSTR(A.CM_OPUN_COD, 1, 3)                       = B.FR_ID 
  WHERE B.FR_ID IS 
    NOT NULL 
    AND SUBSTR(A.CM_OPUN_COD, 1, 3) < '134' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CM_CBOD_CMIRTIRT = sqlContext.sql(sql)
F_CM_CBOD_CMIRTIRT.registerTempTable("F_CM_CBOD_CMIRTIRT")
dfn="F_CM_CBOD_CMIRTIRT/"+V_DT+".parquet"
F_CM_CBOD_CMIRTIRT.cache()
nrows = F_CM_CBOD_CMIRTIRT.count()
F_CM_CBOD_CMIRTIRT.write.save(path=hdfs + '/' + dfn, mode='overwrite')
F_CM_CBOD_CMIRTIRT.unpersist()
#增改表,保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_CMIRTIRT/"+V_DT_LD+".parquet")
#然后复制当天数据进BK
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CM_CBOD_CMIRTIRT_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CM_CBOD_CMIRTIRT/"+V_DT+".parquet /"+dbname+"/F_CM_CBOD_CMIRTIRT_BK/"+V_DT+".parquet")


et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CM_CBOD_CMIRTIRT lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


#任务[21] 001-02::插入汇率最大日期的数据
V_STEP = V_STEP + 1
F_CM_CBOD_CMIRTIRT = sqlContext.read.parquet(hdfs+'/F_CM_CBOD_CMIRTIRT/*')
F_CM_CBOD_CMIRTIRT.registerTempTable("F_CM_CBOD_CMIRTIRT")
sql = """
 SELECT A.ETLDT                 AS ETLDT 
       ,CAST(''                      AS DECIMAL(4))                       AS CMIRT_LL 
       ,A.CM_OPUN_COD           AS CM_OPUN_COD 
       ,A.CM_BUSN_TYP           AS CM_BUSN_TYP 
       ,A.CM_CURR_COD           AS CM_CURR_COD 
       ,A.CM_INTR_TYP           AS CM_INTR_TYP 
       ,A.CM_MASS_AMT_RANG      AS CM_MASS_AMT_RANG 
       ,CAST(''                      AS DECIMAL(15))                       AS CMIRT_DB_TIMESTAMP 
       ,A.CM_AVL_DT             AS CM_AVL_DT 
       ,CAST(A.CM_INTR1              AS DECIMAL(8, 5))                       AS CM_INTR1 
       ,CAST(A.CM_INTR2              AS DECIMAL(8, 5))                       AS CM_INTR2 
       ,A.CM_DOC_NO_CM          AS CM_DOC_NO_CM 
       ,A.CM_INTR_DSCRP         AS CM_INTR_DSCRP 
       ,A.CM_IRT_STS            AS CM_IRT_STS 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
   FROM F_CM_CBOD_CMIRTIRT A                                   --利率主档
  INNER JOIN(
         SELECT MAX(CM_AVL_DT)                       AS CM_AVL_DT 
               ,CM_OPUN_COD 
               ,CM_CURR_COD 
               ,CM_INTR_TYP 
           FROM F_CM_CBOD_CMIRTIRT 
          GROUP BY CM_OPUN_COD 
               ,CM_CURR_COD 
               ,CM_INTR_TYP) B                                 --
     ON A.CM_AVL_DT             = B.CM_AVL_DT 
    AND A.CM_OPUN_COD           = B.CM_OPUN_COD 
    AND A.CM_CURR_COD           = B.CM_CURR_COD 
    AND A.CM_INTR_TYP           = B.CM_INTR_TYP 
  WHERE A.CM_BUSN_TYP           = 'XXX' 
    AND A.CM_MASS_AMT_RANG      = '00' 
    AND A.CM_IRT_STS            = '0' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
MID_CMIRTIRT_2 = sqlContext.sql(sql)
MID_CMIRTIRT_2.registerTempTable("MID_CMIRTIRT_2")
dfn="MID_CMIRTIRT_2/"+V_DT+".parquet"
MID_CMIRTIRT_2.cache()
nrows = MID_CMIRTIRT_2.count()
MID_CMIRTIRT_2.write.save(path=hdfs + '/' + dfn, mode='overwrite')
MID_CMIRTIRT_2.unpersist()
#全量表保存后需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/MID_CMIRTIRT_2/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert MID_CMIRTIRT_2 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)


