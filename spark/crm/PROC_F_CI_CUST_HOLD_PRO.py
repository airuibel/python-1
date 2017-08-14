#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUST_HOLD_PRO').setMaster(sys.argv[2])
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


#清除数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO/*.parquet")
#恢复数据到今日数据文件
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO/"+V_DT+".parquet")


OCRM_F_DP_CARD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_DP_CARD_INFO/*')
OCRM_F_DP_CARD_INFO.registerTempTable("OCRM_F_DP_CARD_INFO")
ACRM_F_CI_ASSET_BUSI_PROTO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_ASSET_BUSI_PROTO/*')
ACRM_F_CI_ASSET_BUSI_PROTO.registerTempTable("ACRM_F_CI_ASSET_BUSI_PROTO")
F_CI_WSYH_ECCIFPRODSET = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFPRODSET/*')
F_CI_WSYH_ECCIFPRODSET.registerTempTable("F_CI_WSYH_ECCIFPRODSET")
OCRM_F_PD_PROD_CATL = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_CATL/*')
OCRM_F_PD_PROD_CATL.registerTempTable("OCRM_F_PD_PROD_CATL")
ACRM_F_CI_CERT_INFO = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CERT_INFO/*')
ACRM_F_CI_CERT_INFO.registerTempTable("ACRM_F_CI_CERT_INFO")
OCRM_F_PD_PROD_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_INFO/*')
OCRM_F_PD_PROD_INFO.registerTempTable("OCRM_F_PD_PROD_INFO")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
ACRM_F_DP_PAYROLL = sqlContext.read.parquet(hdfs+'/ACRM_F_DP_PAYROLL/*')
ACRM_F_DP_PAYROLL.registerTempTable("ACRM_F_DP_PAYROLL")
OCRM_F_PD_PROD_CATL_VIEW = sqlContext.read.parquet(hdfs+'/OCRM_F_PD_PROD_CATL_VIEW/*')
OCRM_F_PD_PROD_CATL_VIEW.registerTempTable("OCRM_F_PD_PROD_CATL_VIEW")
F_CI_SBK_CARDINFO = sqlContext.read.parquet(hdfs+'/F_CI_SBK_CARDINFO/*')
F_CI_SBK_CARDINFO.registerTempTable("F_CI_SBK_CARDINFO")

#任务[21] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.PRODUCT_ID            AS PRODUCT_ID 
       ,'1'                     AS FLAG 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_PD_PROD_INFO A                                  --产品表
  INNER JOIN OCRM_F_PD_PROD_CATL_VIEW B                        --产品类别表
     ON A.CATL_CODE             = B.CATL_CODE 
  INNER JOIN OCRM_F_PD_PROD_CATL_VIEW C                        --
     ON LOCATE(C.CATLSEQ, B.CATLSEQ) > 0 
    AND C.CATL_CODE             = '1080' 
  INNER JOIN OCRM_F_PD_PROD_CATL D                             --
     ON C.CATL_CODE             = D.CATL_CODE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_MID_HOLD_PRO = sqlContext.sql(sql)
OCRM_MID_HOLD_PRO.registerTempTable("OCRM_MID_HOLD_PRO")
dfn="OCRM_MID_HOLD_PRO/"+V_DT+".parquet"
OCRM_MID_HOLD_PRO.cache()
nrows = OCRM_MID_HOLD_PRO.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_MID_HOLD_PRO/*.parquet")
OCRM_MID_HOLD_PRO.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_MID_HOLD_PRO.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_MID_HOLD_PRO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-02::
V_STEP = V_STEP + 1

sql = """
 SELECT A.PRODUCT_ID            AS PRODUCT_ID 
       ,'2'                     AS FLAG 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_PD_PROD_INFO A                                  --产品表
  INNER JOIN OCRM_F_PD_PROD_CATL_VIEW B                        --产品类别表
     ON A.CATL_CODE             = B.CATL_CODE 
  INNER JOIN OCRM_F_PD_PROD_CATL_VIEW C                        --
     ON LOCATE(C.CATLSEQ, B.CATLSEQ) > 0 
    AND C.CATL_CODE             = '1020' 
  INNER JOIN OCRM_F_PD_PROD_CATL D                             --
     ON C.CATL_CODE             = D.CATL_CODE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_MID_HOLD_PRO = sqlContext.sql(sql)
OCRM_MID_HOLD_PRO.registerTempTable("OCRM_MID_HOLD_PRO")
dfn="OCRM_MID_HOLD_PRO/"+V_DT+".parquet"
OCRM_MID_HOLD_PRO.cache()
nrows = OCRM_MID_HOLD_PRO.count()
OCRM_MID_HOLD_PRO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_MID_HOLD_PRO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_MID_HOLD_PRO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_MID_HOLD_PRO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-03::
V_STEP = V_STEP + 1

sql = """
 SELECT A.PRODUCT_ID            AS PRODUCT_ID 
       ,'3'                     AS FLAG 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_PD_PROD_INFO A                                  --产品表
  INNER JOIN OCRM_F_PD_PROD_CATL_VIEW B                        --产品类别表
     ON A.CATL_CODE             = B.CATL_CODE 
  INNER JOIN OCRM_F_PD_PROD_CATL_VIEW C                        --
     ON LOCATE(C.CATLSEQ, B.CATLSEQ) > 0 
    AND C.CATL_CODE             = '1160' 
  INNER JOIN OCRM_F_PD_PROD_CATL D                             --
     ON C.CATL_CODE             = D.CATL_CODE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_MID_HOLD_PRO = sqlContext.sql(sql)
OCRM_MID_HOLD_PRO.registerTempTable("OCRM_MID_HOLD_PRO")
dfn="OCRM_MID_HOLD_PRO/"+V_DT+".parquet"
OCRM_MID_HOLD_PRO.cache()
nrows = OCRM_MID_HOLD_PRO.count()
OCRM_MID_HOLD_PRO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_MID_HOLD_PRO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_MID_HOLD_PRO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_MID_HOLD_PRO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-04::
V_STEP = V_STEP + 1

sql = """
 SELECT A.PRODUCT_ID            AS PRODUCT_ID 
       ,'4'                     AS FLAG 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_PD_PROD_INFO A                                  --产品表
  INNER JOIN OCRM_F_PD_PROD_CATL_VIEW B                        --产品类别表
     ON A.CATL_CODE             = B.CATL_CODE 
  INNER JOIN OCRM_F_PD_PROD_CATL_VIEW C                        --
     ON LOCATE(C.CATLSEQ, B.CATLSEQ) > 0 
    AND C.CATL_CODE IN('1100', '1110', '1140') 
  INNER JOIN OCRM_F_PD_PROD_CATL D                             --
     ON C.CATL_CODE             = D.CATL_CODE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_MID_HOLD_PRO = sqlContext.sql(sql)
OCRM_MID_HOLD_PRO.registerTempTable("OCRM_MID_HOLD_PRO")
dfn="OCRM_MID_HOLD_PRO/"+V_DT+".parquet"
OCRM_MID_HOLD_PRO.cache()
nrows = OCRM_MID_HOLD_PRO.count()
OCRM_MID_HOLD_PRO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_MID_HOLD_PRO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_MID_HOLD_PRO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_MID_HOLD_PRO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-05::
V_STEP = V_STEP + 1

sql = """
 SELECT A.PRODUCT_ID            AS PRODUCT_ID 
       ,'5'                     AS FLAG 
       ,A.FR_ID                 AS FR_ID 
   FROM OCRM_F_PD_PROD_INFO A                                  --产品表
  INNER JOIN OCRM_F_PD_PROD_CATL_VIEW B                        --产品类别表
     ON A.CATL_CODE             = B.CATL_CODE 
  INNER JOIN OCRM_F_PD_PROD_CATL_VIEW C                        --
     ON LOCATE(C.CATLSEQ, B.CATLSEQ) > 0 
    AND C.CATL_CODE             = '1130' 
  INNER JOIN OCRM_F_PD_PROD_CATL D                             --
     ON C.CATL_CODE             = D.CATL_CODE """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_MID_HOLD_PRO = sqlContext.sql(sql)
OCRM_MID_HOLD_PRO.registerTempTable("OCRM_MID_HOLD_PRO")
dfn="OCRM_MID_HOLD_PRO/"+V_DT+".parquet"
OCRM_MID_HOLD_PRO.cache()
nrows = OCRM_MID_HOLD_PRO.count()
OCRM_MID_HOLD_PRO.write.save(path=hdfs + '/' + dfn, mode='append')
OCRM_MID_HOLD_PRO.unpersist()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_MID_HOLD_PRO/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_MID_HOLD_PRO lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[21] 001-06::
V_STEP = V_STEP + 1

sql = """
 SELECT C.CUST_ID               AS CUST_ID 
       ,CASE WHEN B.CR_CRD_STS IS NULL THEN '1' ELSE '0' END                     AS IF_RL 
       ,CASE WHEN B.CR_CRD_STS            = '2' THEN '1' ELSE '0' END                     AS IF_JH 
       ,A.FR_ID                 AS FR_ID 
   FROM F_CI_SBK_CARDINFO A                                    --社保卡表
  INNER JOIN ACRM_F_CI_CERT_INFO C                             --证件信息表
     ON C.FR_ID                 = A.FR_ID 
    AND A.IDNO                  = C.CERT_NO 
   LEFT JOIN OCRM_F_DP_CARD_INFO B                             --借记卡表
     ON A.CARDNO                = B.CR_CRD_NO 
    AND B.CR_CRD_STS            = '2' 
    AND B.CR_PDP_CODE IN('999CR000053', '999CR000022') 
  WHERE A.ODS_ST_DATE           = V_DT 
  GROUP BY C.CUST_ID 
       ,B.CR_CRD_STS 
       ,A.FR_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_02 = sqlContext.sql(sql)
OCRM_F_CI_CUST_HOLD_PRO_02.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO_02")
dfn="OCRM_F_CI_CUST_HOLD_PRO_02/"+V_DT+".parquet"
OCRM_F_CI_CUST_HOLD_PRO_02.cache()
nrows = OCRM_F_CI_CUST_HOLD_PRO_02.count()
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_02/*.parquet")
OCRM_F_CI_CUST_HOLD_PRO_02.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_HOLD_PRO_02.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_HOLD_PRO_02 lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-07::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")

OCRM_MID_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_MID_HOLD_PRO/*')
OCRM_MID_HOLD_PRO.registerTempTable("OCRM_MID_HOLD_PRO")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,CASE WHEN C.PROD_FLAG IS NOT NULL THEN CONCAT(SUBSTR(C.PROD_FLAG, 1, 6) , '1' , SUBSTR(C.PROD_FLAG, 8, 5)) ELSE '000000100000' END                     AS PROD_FLAG 
       ,'2'                     AS CUST_TYPE 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN OCRM_MID_HOLD_PRO B                               --持有产品中间表
     ON A.PRODUCT_ID            = B.PRODUCT_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.FLAG                  = '1' 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO C                         --客户持有产品临时表
     ON C.FR_ID                 = A.FR_ID 
    AND A.CUST_ID               = C.CUST_ID 
  WHERE A.CUST_TYP              = '2' 
    AND BEGIN_DATE              = regexp_replace(V_DT, '-','') 
    AND A.BAL_RMB > 0 
  GROUP BY A.CUST_ID,A.FR_ID,C.PROD_FLAG """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO_INNTMP1")

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.PROD_FLAG                                          --产品标识:src.PROD_FLAG
       ,DST.CUST_TYPE                                          --客户类型:src.CUST_TYPE
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_CUST_HOLD_PRO DST 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_HOLD_PRO/"+V_DT+".parquet"
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2=OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unionAll(OCRM_F_CI_CUST_HOLD_PRO_INNTMP1)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.cache()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.count()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.unpersist()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_HOLD_PRO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_BK/")

#任务[12] 001-08::
V_STEP = V_STEP + 1
OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,CASE WHEN C.PROD_FLAG IS NOT NULL THEN CONCAT(SUBSTR(C.PROD_FLAG, 1, 7) , '1' , SUBSTR(C.PROD_FLAG, 9)) ELSE '000000010000' END                     AS PROD_FLAG 
       ,'2'                     AS CUST_TYPE 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN OCRM_MID_HOLD_PRO B                               --持有产品中间表
     ON A.PRODUCT_ID            = B.PRODUCT_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.FLAG                  = '2' 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO C                         --客户持有产品临时表
     ON C.FR_ID                 = A.FR_ID 
    AND A.CUST_ID               = C.CUST_ID 
  WHERE A.CUST_TYP              = '2' 
    AND BEGIN_DATE              = regexp_replace(V_DT, '-','') 
    AND A.BAL_RMB > 0 
  GROUP BY A.CUST_ID,A.FR_ID,C.PROD_FLAG """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO_INNTMP1")

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.PROD_FLAG                                          --产品标识:src.PROD_FLAG
       ,DST.CUST_TYPE                                          --客户类型:src.CUST_TYPE
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_CUST_HOLD_PRO DST 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_HOLD_PRO/"+V_DT+".parquet"
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2=OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unionAll(OCRM_F_CI_CUST_HOLD_PRO_INNTMP1)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.cache()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.count()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.unpersist()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_HOLD_PRO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_BK/")

#任务[12] 001-09::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,CASE WHEN C.PROD_FLAG IS NOT NULL THEN CONCAT(SUBSTR(C.PROD_FLAG, 1, 8) , '1' , SUBSTR(C.PROD_FLAG, 10)) ELSE '000000001000' END                     AS PROD_FLAG 
       ,'2'                     AS CUST_TYPE 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN OCRM_MID_HOLD_PRO B                               --持有产品中间表
     ON A.PRODUCT_ID            = B.PRODUCT_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.FLAG                  = '3' 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO C                         --客户持有产品临时表
     ON C.FR_ID                 = A.FR_ID 
    AND A.CUST_ID               = C.CUST_ID 
  WHERE A.CUST_TYP              = '2' 
    AND BEGIN_DATE              = regexp_replace(V_DT, '-','') 
    AND A.BAL_RMB > 0 
  GROUP BY A.CUST_ID,A.FR_ID,C.PROD_FLAG """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO_INNTMP1")

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.PROD_FLAG                                          --产品标识:src.PROD_FLAG
       ,DST.CUST_TYPE                                          --客户类型:src.CUST_TYPE
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_CUST_HOLD_PRO DST 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_HOLD_PRO/"+V_DT+".parquet"
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2=OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unionAll(OCRM_F_CI_CUST_HOLD_PRO_INNTMP1)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.cache()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.count()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.unpersist()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_HOLD_PRO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_BK/")

#任务[12] 001-10::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,CASE WHEN C.PROD_FLAG IS 
    NOT NULL THEN CONCAT(SUBSTR(C.PROD_FLAG, 1, 9) , '1' , SUBSTR(C.PROD_FLAG, 11)) ELSE '000000000100' END                     AS PROD_FLAG 
       ,'2'                     AS CUST_TYPE 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN OCRM_MID_HOLD_PRO B                               --持有产品中间表
     ON A.PRODUCT_ID            = B.PRODUCT_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.FLAG                  = '4' 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO C                         --客户持有产品临时表
     ON C.FR_ID                 = A.FR_ID 
    AND A.CUST_ID               = C.CUST_ID 
  WHERE A.CUST_TYP              = '2' 
    AND BEGIN_DATE              = regexp_replace(V_DT, '-','') 
    AND A.BAL_RMB > 0 
  GROUP BY A.CUST_ID,A.FR_ID,C.PROD_FLAG """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO_INNTMP1")

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.PROD_FLAG                                          --产品标识:src.PROD_FLAG
       ,DST.CUST_TYPE                                          --客户类型:src.CUST_TYPE
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_CUST_HOLD_PRO DST 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_HOLD_PRO/"+V_DT+".parquet"
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2=OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unionAll(OCRM_F_CI_CUST_HOLD_PRO_INNTMP1)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.cache()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.count()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.unpersist()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_HOLD_PRO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_BK/")

#任务[12] 001-11::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,CASE WHEN C.PROD_FLAG IS NOT NULL THEN CONCAT(SUBSTR(C.PROD_FLAG, 1, 10) , '1' , SUBSTR(C.PROD_FLAG, 12)) ELSE '000000000010' END                     AS PROD_FLAG 
       ,'2'                     AS CUST_TYPE 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_CI_ASSET_BUSI_PROTO A                           --资产协议表
  INNER JOIN OCRM_MID_HOLD_PRO B                               --持有产品中间表
     ON A.PRODUCT_ID            = B.PRODUCT_ID 
    AND B.FR_ID                 = A.FR_ID 
    AND B.FLAG                  = '5' 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO C                         --客户持有产品临时表
     ON C.FR_ID                 = A.FR_ID 
    AND A.CUST_ID               = C.CUST_ID 
  WHERE A.CUST_TYP              = '2' 
    AND BEGIN_DATE              = regexp_replace(V_DT, '-','')  
    AND A.BAL_RMB > 0 
  GROUP BY A.CUST_ID,A.FR_ID,C.PROD_FLAG """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO_INNTMP1")

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.PROD_FLAG                                          --产品标识:src.PROD_FLAG
       ,DST.CUST_TYPE                                          --客户类型:src.CUST_TYPE
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_CUST_HOLD_PRO DST 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_HOLD_PRO/"+V_DT+".parquet"
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2=OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unionAll(OCRM_F_CI_CUST_HOLD_PRO_INNTMP1)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.cache()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.count()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.unpersist()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_HOLD_PRO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_BK/")

#任务[12] 001-12::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,CASE WHEN B.PROD_FLAG IS NOT NULL THEN CONCAT(SUBSTR(B.PROD_FLAG, 1, 11) , '1') ELSE '000000000001' END AS PROD_FLAG 
       ,'2'                     AS CUST_TYPE 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM ACRM_F_DP_PAYROLL A                                    --代发工资
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO B                         --客户持有产品临时表
     ON B.FR_ID                 = A.FR_ID 
    AND A.CUST_ID               = B.CUST_ID 
  GROUP BY A.CUST_ID,A.FR_ID,B.PROD_FLAG """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO_INNTMP1")

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.PROD_FLAG                                          --产品标识:src.PROD_FLAG
       ,DST.CUST_TYPE                                          --客户类型:src.CUST_TYPE
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_CUST_HOLD_PRO DST 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_HOLD_PRO/"+V_DT+".parquet"
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2=OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unionAll(OCRM_F_CI_CUST_HOLD_PRO_INNTMP1)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.cache()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.count()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.unpersist()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_HOLD_PRO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_BK/")

#任务[12] 001-13::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")

sql = """
 SELECT A.ODS_CUST_ID           AS CUST_ID 
       ,CASE WHEN C.PROD_FLAG IS NOT NULL THEN CONCAT(SUBSTR(C.PROD_FLAG, 1, 11) , '1') ELSE '000000000001' END AS PROD_FLAG 
       ,'2'                     AS CUST_TYPE 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM (SELECT A.FR_ID,A.ODS_CUST_ID,
                ROW_NUMBER() OVER(PARTITION BY A.ODS_CUST_ID ORDER BY A.ODS_CUST_ID DESC) RN
           FROM OCRM_F_CI_SYS_RESOURCE A                               --系统来源中间表
           JOIN F_CI_WSYH_ECCIFPRODSET B ON A.FR_ID = B.FR_ID AND A.SOURCE_CUST_ID = B.CIFSEQ 
                                        AND B.PRDSETID = '财务管理' AND B.ODS_ST_DATE = V_DT
          WHERE A.ODS_SYS_ID = 'IBK' AND A.ODS_CUST_TYPE = '2' ) A
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO C                         --客户持有产品临时表
     ON A.FR_ID                 = C.FR_ID 
    AND A.ODS_CUST_ID           = C.CUST_ID 
   WHERE A.RN = '1'
  GROUP BY A.ODS_CUST_ID,A.FR_ID,C.PROD_FLAG """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO_INNTMP1")

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.PROD_FLAG                                          --产品标识:src.PROD_FLAG
       ,DST.CUST_TYPE                                          --客户类型:src.CUST_TYPE
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_CUST_HOLD_PRO DST 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_HOLD_PRO/"+V_DT+".parquet"
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2=OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unionAll(OCRM_F_CI_CUST_HOLD_PRO_INNTMP1)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.cache()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.count()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.unpersist()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_HOLD_PRO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_BK/")

#任务[12] 001-14::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_HOLD_PRO_02 = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO_02/*')
OCRM_F_CI_CUST_HOLD_PRO_02.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO_02")

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,CASE WHEN B.PROD_FLAG IS NOT NULL 
           THEN CONCAT(SUBSTR(B.PROD_FLAG, 1, 12) , IF_RL , IF_JH) 
          ELSE CONCAT('000000000000' , IF_RL , IF_JH) 
         END AS PROD_FLAG 
       ,'2'                     AS CUST_TYPE 
       ,V_DT                    AS ETL_DATE 
       ,A.FR_ID                 AS FR_ID 
   FROM (SELECT CUST_ID,IF_RL,IF_JH,FR_ID,
                ROW_NUMBER() OVER(PARTITION BY FR_ID,CUST_ID ORDER BY IF_JH DESC,IF_RL DESC) RN
           FROM OCRM_F_CI_CUST_HOLD_PRO_02) A                           --客户持有产品临时表02
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO B                         --客户持有产品临时表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID
   WHERE A.RN = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO_INNTMP1")

OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")
sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.PROD_FLAG                                          --产品标识:src.PROD_FLAG
       ,DST.CUST_TYPE                                          --客户类型:src.CUST_TYPE
       ,DST.ETL_DATE                                           --数据日期:src.ETL_DATE
       ,DST.FR_ID                                              --法人号:src.FR_ID
   FROM OCRM_F_CI_CUST_HOLD_PRO DST 
   LEFT JOIN OCRM_F_CI_CUST_HOLD_PRO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_HOLD_PRO/"+V_DT+".parquet"
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2=OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unionAll(OCRM_F_CI_CUST_HOLD_PRO_INNTMP1)
OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.cache()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.count()

#装载数据
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_BK/"+V_DT+".parquet")


OCRM_F_CI_CUST_HOLD_PRO_INNTMP1.unpersist()
OCRM_F_CI_CUST_HOLD_PRO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_HOLD_PRO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_HOLD_PRO_BK/")
