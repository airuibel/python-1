#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_SYS_RESOURCE').setMaster(sys.argv[2])
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


F_CI_ZFB_FT_USER_PRO_SIGNINFO = sqlContext.read.parquet(hdfs+'/F_CI_ZFB_FT_USER_PRO_SIGNINFO/*')
F_CI_ZFB_FT_USER_PRO_SIGNINFO.registerTempTable("F_CI_ZFB_FT_USER_PRO_SIGNINFO")
F_CI_CBOD_ECCIFIDI = sqlContext.read.parquet(hdfs+'/F_CI_CBOD_ECCIFIDI/*')
F_CI_CBOD_ECCIFIDI.registerTempTable("F_CI_CBOD_ECCIFIDI")
F_TX_WSYH_ECCIF = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIF/*')
F_TX_WSYH_ECCIF.registerTempTable("F_TX_WSYH_ECCIF")
F_CI_WSYH_ECCIFID = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECCIFID/*')
F_CI_WSYH_ECCIFID.registerTempTable("F_CI_WSYH_ECCIFID")
V_LOOKUP_CODE = sqlContext.read.parquet(hdfs+'/V_LOOKUP_CODE/*')
V_LOOKUP_CODE.registerTempTable("V_LOOKUP_CODE")
F_TX_FIN_CUSTINFO = sqlContext.read.parquet(hdfs+'/F_TX_FIN_CUSTINFO/*')
F_TX_FIN_CUSTINFO.registerTempTable("F_TX_FIN_CUSTINFO")
OCRM_F_CI_SYS_RESOURCE_BEFORE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE_BEFORE/*')
OCRM_F_CI_SYS_RESOURCE_BEFORE.registerTempTable("OCRM_F_CI_SYS_RESOURCE_BEFORE")
F_CI_CUSTR = sqlContext.read.parquet(hdfs+'/F_CI_CUSTR/*')
F_CI_CUSTR.registerTempTable("F_CI_CUSTR")
F_TX_WSYH_ECCIFMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIFMCH/*')
F_TX_WSYH_ECCIFMCH.registerTempTable("F_TX_WSYH_ECCIFMCH")
F_CI_ZDH_ZZDH_SHOP = sqlContext.read.parquet(hdfs+'/F_CI_ZDH_ZZDH_SHOP/*')
F_CI_ZDH_ZZDH_SHOP.registerTempTable("F_CI_ZDH_ZZDH_SHOP")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet")

OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
#核心客户特殊处理
sql = """
 SELECT A.ID                    AS ID 
       ,NVL(A.ODS_CUST_ID,B.ODS_CUST_ID )          AS ODS_CUST_ID 
       ,NVL(A.ODS_CUST_NAME,B.ODS_CUST_NAME) AS ODS_CUST_NAME
	   ,NVL(A.SOURCE_CUST_ID,B.SOURCE_CUST_ID)           AS SOURCE_CUST_ID 
       ,NVL(A.SOURCE_CUST_NAME,B.SOURCE_CUST_NAME)       AS SOURCE_CUST_NAME 
       ,NVL(A.CERT_TYPE,B.CERT_TYPE)            AS CERT_TYPE 
       ,NVL(A.CERT_NO,B.CERT_NO)          AS CERT_NO 
       ,'CEN'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,NVL(A.ODS_CUST_TYPE,B.ODS_CUST_TYPE)          AS ODS_CUST_TYPE 
       ,'1'                       AS CUST_STAT 
       ,B.BEGIN_DATE          AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,B.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,B.FR_ID          AS FR_ID 
       ,B.CERT_FLAG        AS CERT_FLAG 
   FROM (SELECT 
					     A.ODS_CUST_ID,
                         A.ODS_CUST_ID AS SOURCE_CUST_ID,
                         B.EC_FULL_NAM  AS ODS_CUST_NAME,
                         B.EC_FULL_NAM AS SOURCE_CUST_NAME,
                         B.EC_CER_TYP   AS  CERT_TYPE,
                         B.EC_CER_NO AS             CERT_NO,
                         'CEN'  AS  ODS_SYS_ID,-- 核心
                         B.EC_CUST_TYP     AS    ODS_CUST_TYPE,
						 B.EC_CRT_SCT_N AS BEGIN_DATE,
						 B.EC_PRI_CER_FLG AS CERT_FLAG,
						 A.END_DATE, -- 结束日期放置永久
						 A.SYS_ID_FLAG,        --系统标识字符串
						 A.FR_ID AS FR_ID	       
					FROM OCRM_F_CI_SYS_RESOURCE_BEFORE A
					JOIN F_CI_CBOD_ECCIFIDI B ON A.ODS_CUST_ID=B.EC_CUST_NO AND A.FR_ID = B.FR_ID
					WHERE A.ODS_ST_DATE=V_DT) B
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE A                          --系统来源中间表
     ON A.SOURCE_CUST_NAME = B.SOURCE_CUST_NAME 
	      AND A.CERT_TYPE = B.CERT_TYPE 
	      AND A.CERT_NO = B.CERT_NO 
	      AND A.ODS_SYS_ID = 'CEN' 
	      AND A.FR_ID = B.FR_ID
        AND (A.SOURCE_CUST_ID=B.SOURCE_CUST_ID OR (A.SOURCE_CUST_ID IS NULL AND B.SOURCE_CUST_ID IS NULL))
  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.ODS_CUST_ID         = DST.ODS_CUST_ID
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.SOURCE_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/")

#任务[12] 001-02::
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
#网银+手机银行客户（对公）
sql = """
 SELECT A.ID                    AS ID 
       ,NVL(A.ODS_CUST_ID,B.ODS_CUST_ID )          AS ODS_CUST_ID 
       ,NVL(A.ODS_CUST_NAME,B.ODS_CUST_NAME) AS ODS_CUST_NAME
	   ,NVL(A.SOURCE_CUST_ID,B.SOURCE_CUST_ID)           AS SOURCE_CUST_ID 
       ,NVL(A.SOURCE_CUST_NAME,B.SOURCE_CUST_NAME)       AS SOURCE_CUST_NAME 
       ,NVL(A.CERT_TYPE,B.CERT_TYPE)            AS CERT_TYPE 
       ,NVL(A.CERT_NO,B.CERT_NO)          AS CERT_NO 
       ,B.ODS_SYS_ID                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,NVL(A.ODS_CUST_TYPE,B.ODS_CUST_TYPE)          AS ODS_CUST_TYPE 
       ,'1'                       AS CUST_STAT 
       ,B.BEGIN_DATE          AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,B.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,B.FR_ID          AS FR_ID 
       ,A.CERT_FLAG        AS CERT_FLAG 
	FROM (SELECT DISTINCT
						    '' AS ODS_CUST_ID            ,
						    A.CIFSEQ AS SOURCE_CUST_ID       ,
						    A.CIFNAME  AS ODS_CUST_NAME       ,
						    A.CIFNAME  AS SOURCE_CUST_NAME    ,
						    B.IDTYPE AS CERT_TYPE         ,
						    B.IDNO AS CERT_NO             ,
						    CASE C.MCHANNELID WHEN 'EIBS' THEN 'IBK'  --网银
						         ELSE 'MBK'  --手机银行
						       END AS ODS_SYS_ID            ,-- 网银
						    '2' AS ODS_CUST_TYPE           ,-- 对公
						    '' AS BEGIN_DATE ,
						    '' AS END_DATE ,
						    CASE C.MCHANNELID WHEN 'EIBS' THEN '101000000000'  --网银
						         ELSE '100000000010'   --手机银行
						         END SYS_ID_FLAG,       --系统标识字符串
						     A.FR_ID 
                  FROM F_TX_WSYH_ECCIF A,F_CI_WSYH_ECCIFID B,F_TX_WSYH_ECCIFMCH C
                      WHERE A.CIFSEQ = B.CIFSEQ AND a.CIFSTATE = 'N'
                      AND A.FR_ID = B.FR_ID AND A.FR_ID = C.FR_ID 
                      AND A.CIFSEQ = C.CIFSEQ AND C.MCHANNELID IN ('EIBS','EMBS')) B
   
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE A                          --系统来源中间表
     ON A.SOURCE_CUST_NAME = B.SOURCE_CUST_NAME 
	      AND A.CERT_TYPE = B.CERT_TYPE 
	      AND A.CERT_NO = B.CERT_NO 
	      AND A.FR_ID = B.FR_ID 
	      AND A.ODS_SYS_ID = B.ODS_SYS_ID 
        AND (A.SOURCE_CUST_ID=B.SOURCE_CUST_ID OR (A.SOURCE_CUST_ID IS NULL AND B.SOURCE_CUST_ID IS NULL))    """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.SOURCE_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/")

#任务[12] 001-03::
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
#贷记卡客户
sql = """
 SELECT A.ID                    AS ID 
       ,NVL(A.ODS_CUST_ID,B.ODS_CUST_ID )          AS ODS_CUST_ID 
       ,NVL(A.ODS_CUST_NAME,B.ODS_CUST_NAME) AS ODS_CUST_NAME
	   ,NVL(A.SOURCE_CUST_ID,B.SOURCE_CUST_ID)           AS SOURCE_CUST_ID 
       ,NVL(A.SOURCE_CUST_NAME,B.SOURCE_CUST_NAME)       AS SOURCE_CUST_NAME 
       ,NVL(A.CERT_TYPE,B.CERT_TYPE)            AS CERT_TYPE 
       ,NVL(A.CERT_NO,B.CERT_NO)          AS CERT_NO 
       ,B.ODS_SYS_ID                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,NVL(A.ODS_CUST_TYPE,B.ODS_CUST_TYPE)          AS ODS_CUST_TYPE 
       ,'1'                       AS CUST_STAT 
       ,B.BEGIN_DATE          AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,B.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,B.FR_ID          AS FR_ID 
       ,A.CERT_FLAG        AS CERT_FLAG
    FROM (SELECT DISTINCT
						 '' AS ODS_CUST_ID            ,
						 A.CUSTR_NBR AS SOURCE_CUST_ID    ,
						 A.SURNAME AS ODS_CUST_NAME       ,
                         A.SURNAME AS SOURCE_CUST_NAME    ,
                         A.RACE_CODE AS CERT_TYPE         ,
                         A.CUSTR_NBR AS CERT_NO           ,
                         'CRE' AS ODS_SYS_ID            ,
                         '1' AS ODS_CUST_TYPE           ,-- 个人
						  '' AS BEGIN_DATE             ,
						  '' AS END_DATE               ,
						  '100001000000'         as  sys_id_flag ,           --系统标识字符串
						  A.FR_ID AS FR_ID
				FROM F_CI_CUSTR A) B    --客户资料
		LEFT JOIN OCRM_F_CI_SYS_RESOURCE A    --系统来源中间表
		ON A.FR_ID =B.FR_ID
				AND A.SOURCE_CUST_NAME = B.SOURCE_CUST_NAME 
	      AND A.CERT_TYPE = B.CERT_TYPE 
	      AND A.CERT_NO = B.CERT_NO 
	      AND A.ODS_SYS_ID = B.ODS_SYS_ID 
        AND (A.SOURCE_CUST_ID=B.SOURCE_CUST_ID OR (A.SOURCE_CUST_ID IS NULL AND B.SOURCE_CUST_ID IS NULL))     """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.SOURCE_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/")

#任务[12] 001-04::
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
#金融进村客户
sql = """
 SELECT A.ID                    AS ID 
       ,NVL(A.ODS_CUST_ID,B.ODS_CUST_ID )          AS ODS_CUST_ID 
       ,NVL(A.ODS_CUST_NAME,B.ODS_CUST_NAME) AS ODS_CUST_NAME
	   ,NVL(A.SOURCE_CUST_ID,B.SOURCE_CUST_ID)           AS SOURCE_CUST_ID 
       ,NVL(A.SOURCE_CUST_NAME,B.SOURCE_CUST_NAME)       AS SOURCE_CUST_NAME 
       ,NVL(A.CERT_TYPE,B.CERT_TYPE)            AS CERT_TYPE 
       ,NVL(A.CERT_NO,B.CERT_NO)          AS CERT_NO 
       ,B.ODS_SYS_ID                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,NVL(A.ODS_CUST_TYPE,B.ODS_CUST_TYPE)          AS ODS_CUST_TYPE 
       ,'1'                       AS CUST_STAT 
       ,B.BEGIN_DATE          AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,B.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,B.FR_ID          AS FR_ID 
       ,A.CERT_FLAG        AS CERT_FLAG
   FROM (SELECT DISTINCT
						 '' AS ODS_CUST_ID            ,
						 SHOP_NO AS SOURCE_CUST_ID      ,
						 SHOP_NAME AS ODS_CUST_NAME     ,
                         SHOP_NAME AS SOURCE_CUST_NAME  ,
                         SHOP_KIND AS CERT_TYPE         ,
                         BUS_AREA AS CERT_NO       ,
                         'ZDH' AS ODS_SYS_ID            ,-- 金融进村
                         '1' AS ODS_CUST_TYPE           ,-- 零售客户
                         CASE WHEN LENGTH(TRIM(SIGN_DATE)) = 8 THEN concat(SUBSTR(SIGN_DATE,1,4),'-',SUBSTR(SIGN_DATE,5,2),'-',SUBSTR(SIGN_DATE,7,2)) ELSE '' END AS BEGIN_DATE,
                         CASE WHEN LENGTH(TRIM(UNSIGN_DATE)) = 8 THEN concat(SUBSTR(UNSIGN_DATE,1,4),'-',SUBSTR(UNSIGN_DATE,5,2),'-',SUBSTR(UNSIGN_DATE,7,2)) ELSE '' END AS END_DATE ,
                         '100000010000'         as  sys_id_flag        ,    --系统标识字符串
                          COALESCE(A.FR_ID,'UNK') AS FR_ID 
				FROM F_CI_ZDH_ZZDH_SHOP A
				WHERE ODS_ST_DATE = V_DT) B                                   --商户信息表
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE A                          --系统来源中间表
     ON A.FR_ID = B.FR_ID  
			  AND A.SOURCE_CUST_NAME = B.SOURCE_CUST_NAME 
	      AND A.CERT_TYPE = B.CERT_TYPE 
	      AND A.CERT_NO = B.CERT_NO 
	      AND A.ODS_SYS_ID = 'ZDH' 
        AND (A.SOURCE_CUST_ID=B.SOURCE_CUST_ID OR (A.SOURCE_CUST_ID IS NULL AND B.SOURCE_CUST_ID IS NULL))   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.SOURCE_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/")

#任务[12] 001-05::
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
#理财平台客户
sql = """
 SELECT A.ID                    AS ID 
       ,NVL(A.ODS_CUST_ID,B.ODS_CUST_ID )          AS ODS_CUST_ID 
       ,NVL(A.ODS_CUST_NAME,B.ODS_CUST_NAME) AS ODS_CUST_NAME
	   ,NVL(A.SOURCE_CUST_ID,B.SOURCE_CUST_ID)           AS SOURCE_CUST_ID 
       ,NVL(A.SOURCE_CUST_NAME,B.SOURCE_CUST_NAME)       AS SOURCE_CUST_NAME 
       ,NVL(A.CERT_TYPE,B.CERT_TYPE)            AS CERT_TYPE 
       ,NVL(A.CERT_NO,B.CERT_NO)          AS CERT_NO 
       ,B.ODS_SYS_ID                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,NVL(A.ODS_CUST_TYPE,B.ODS_CUST_TYPE)          AS ODS_CUST_TYPE 
       ,'1'                       AS CUST_STAT 
       ,B.BEGIN_DATE          AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,B.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,B.FR_ID          AS FR_ID 
       ,A.CERT_FLAG        AS CERT_FLAG
   FROM (SELECT DISTINCT
						 '' AS ODS_CUST_ID            , 
						 ZONENO AS SOURCE_CUST_ID       ,
                         ACCTNAME AS ODS_CUST_NAME      ,
						 ACCTNAME AS SOURCE_CUST_NAME   ,
                         B.TARGET_VALUE AS  CERT_TYPE           ,
                         IDNO AS CERT_NO                ,
                         'FIN' AS ODS_SYS_ID            ,
                         CASE WHEN CUSTTYPE ='1' THEN '2' ELSE '1' END AS ODS_CUST_TYPE      ,
                         '' AS BEGIN_DATE          ,
                         '' AS END_DATE   ,
                         '100000000100'         as  sys_id_flag  ,          --系统标识字符串
                         ZONENO    AS FR_ID       --法人号
				FROM F_TX_FIN_CUSTINFO A,V_LOOKUP_CODE B
					WHERE ODS_ST_DATE = V_DT
					AND A.IDTYPE=B.SOURCE_VALUE
					AND B.TYP_ID = 'FIN_IDTYPE')B
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE A            --系统来源中间表
     ON A.SOURCE_CUST_NAME = B.SOURCE_CUST_NAME 
	      AND A.CERT_TYPE = B.CERT_TYPE 
	      AND A.CERT_NO = B.CERT_NO 
	      AND A.ODS_SYS_ID = 'FIN'
	      AND A.FR_ID = B.FR_ID
        AND (A.SOURCE_CUST_ID=B.SOURCE_CUST_ID OR (A.SOURCE_CUST_ID IS NULL AND B.SOURCE_CUST_ID IS NULL))    """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.SOURCE_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/")

#任务[12] 001-06::
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
#支付宝客户
sql = """
 SELECT A.ID                    AS ID 
       ,NVL(A.ODS_CUST_ID,B.ODS_CUST_ID )          AS ODS_CUST_ID 
       ,NVL(A.ODS_CUST_NAME,B.ODS_CUST_NAME) AS ODS_CUST_NAME
	   ,NVL(A.SOURCE_CUST_ID,B.SOURCE_CUST_ID)           AS SOURCE_CUST_ID 
       ,NVL(A.SOURCE_CUST_NAME,B.SOURCE_CUST_NAME)       AS SOURCE_CUST_NAME 
       ,NVL(A.CERT_TYPE,B.CERT_TYPE)            AS CERT_TYPE 
       ,NVL(A.CERT_NO,B.CERT_NO)          AS CERT_NO 
       ,B.ODS_SYS_ID                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,NVL(A.ODS_CUST_TYPE,B.ODS_CUST_TYPE)          AS ODS_CUST_TYPE 
       ,'1'                       AS CUST_STAT 
       ,B.BEGIN_DATE          AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,B.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,B.FR_ID          AS FR_ID 
       ,A.CERT_FLAG        AS CERT_FLAG
   FROM (SELECT DISTINCT
						     	CASE WHEN LENGTH(TRIM(USER_CIFNO)) = 12 
						     	     THEN USER_CIFNO 
						     	     ELSE ''
						     	END  AS ODS_CUST_ID                    , 
						     	USER_CIFNO AS SOURCE_CUST_ID           ,
                                USERTC_CLIENTNAME AS ODS_CUST_NAME     ,
						     	USERTC_CLIENTNAME AS SOURCE_CUST_NAME  ,
                                USERTC_CRENDENTYPE AS  CERT_TYPE       ,
                                USERTC_CRENDENID AS CERT_NO            ,
                                 'APY' AS ODS_SYS_ID                    ,-- 支付宝
                                 '1' AS ODS_CUST_TYPE                   ,-- 个人客户
                                 concat(SUBSTR(USERTC_OPENDATE,1,4),'-',SUBSTR(USERTC_OPENDATE,5,2),'-',SUBSTR(USERTC_OPENDATE,7,2)) AS BEGIN_DATE    ,
                                 concat(SUBSTR(USERTC_CLOSEDATE,1,4),'-',SUBSTR(USERTC_CLOSEDATE,5,2),'-',SUBSTR(USERTC_CLOSEDATE,7,2)) AS END_DATE    ,
                                 '100000000001'         as  sys_id_flag     ,       --系统标识字符串
                                 COALESCE(A.FR_ID,'UNK') AS FR_ID
						      FROM F_CI_ZFB_FT_USER_PRO_SIGNINFO  A   
						      WHERE ODS_ST_DATE =V_DT 
						        AND EXISTS (SELECT 1 FROM F_CI_ZFB_FT_USER_PRO_SIGNINFO B 
						     	            WHERE A.USER_CIFNO=B.USER_CIFNO
						     	              AND B.FR_ID = A.FR_ID
						     			        GROUP BY B.USER_CIFNO
						     			        HAVING COUNT(1) =1
						     			       ))B
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE A                          --
     ON A.SOURCE_CUST_NAME = B.SOURCE_CUST_NAME 
	      AND A.CERT_TYPE = B.CERT_TYPE 
	      AND A.CERT_NO = B.CERT_NO 
	      AND A.ODS_SYS_ID = 'APY' 
	      AND A.FR_ID = B.FR_ID
        AND (A.SOURCE_CUST_ID=B.SOURCE_CUST_ID OR (A.SOURCE_CUST_ID IS NULL AND B.SOURCE_CUST_ID IS NULL))"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.SOURCE_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/")

#任务[12] 001-07::
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
sql = """
 SELECT A.ID                    AS ID 
       ,NVL(A.ODS_CUST_ID,B.ODS_CUST_ID )          AS ODS_CUST_ID 
       ,NVL(A.ODS_CUST_NAME,B.ODS_CUST_NAME) AS ODS_CUST_NAME
	   ,NVL(A.SOURCE_CUST_ID,B.SOURCE_CUST_ID)           AS SOURCE_CUST_ID 
       ,NVL(A.SOURCE_CUST_NAME,B.SOURCE_CUST_NAME)       AS SOURCE_CUST_NAME 
       ,NVL(A.CERT_TYPE,B.CERT_TYPE)            AS CERT_TYPE 
       ,NVL(A.CERT_NO,B.CERT_NO)          AS CERT_NO 
       ,B.ODS_SYS_ID                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,NVL(A.ODS_CUST_TYPE,B.ODS_CUST_TYPE)          AS ODS_CUST_TYPE 
       ,'1'                       AS CUST_STAT 
       ,B.BEGIN_DATE          AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,B.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,B.FR_ID          AS FR_ID 
       ,A.CERT_FLAG        AS CERT_FLAG
   FROM (SELECT DISTINCT 									
					     CASE WHEN LENGTH(TRIM(USER_CIFNO)) = 12 
					  	  	 THEN USER_CIFNO 
					  	  	 ELSE ''
					     END  AS ODS_CUST_ID                    , 
					     USER_CIFNO AS SOURCE_CUST_ID           ,
                         USERTC_CLIENTNAME AS ODS_CUST_NAME     ,
						 USERTC_CLIENTNAME AS SOURCE_CUST_NAME  ,
                         USERTC_CRENDENTYPE AS  CERT_TYPE       ,
                         USERTC_CRENDENID AS CERT_NO            ,
                         'APY' AS ODS_SYS_ID                    ,-- 支付宝
                         '1' AS ODS_CUST_TYPE                   ,-- 个人客户
                         MAX(concat(SUBSTR(USERTC_OPENDATE,1,4),'-',SUBSTR(USERTC_OPENDATE,5,2),'-',SUBSTR(USERTC_OPENDATE,7,2))) AS BEGIN_DATE    ,
                         '' AS END_DATE    ,
                         '100000000001' as sys_id_flag      ,     --系统标识字符串
                        COALESCE(A.FR_ID,'UNK') AS FR_ID
		 FROM F_CI_ZFB_FT_USER_PRO_SIGNINFO  A 
		 WHERE ODS_ST_DATE =V_DT
		   AND EXISTS (SELECT 1 FROM F_CI_ZFB_FT_USER_PRO_SIGNINFO B 
		                WHERE A.USER_CIFNO=B.USER_CIFNO 
		                  AND B.FR_ID = A.FR_ID
					        GROUP BY B.USER_CIFNO
					        HAVING COUNT(1) >1
					       )
			AND EXISTS (SELECT 1 FROM F_CI_ZFB_FT_USER_PRO_SIGNINFO B 
			            WHERE A.USER_CIFNO=B.USER_CIFNO 
			              AND B.UserTc_CloseDate IS NULL 
			              AND B.FR_ID = A.FR_ID
					       )
		 GROUP BY A.FR_ID,A.USER_CIFNO,A.USERTC_CLIENTNAME,A.USERTC_CRENDENTYPE,A.USERTC_CRENDENID )B
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE A                          --
     ON A.FR_ID = B.FR_ID
		AND A.SOURCE_CUST_NAME = B.SOURCE_CUST_NAME 
	      AND A.CERT_TYPE = B.CERT_TYPE 
	      AND A.CERT_NO = B.CERT_NO 
	      AND A.ODS_SYS_ID = 'APY'
        AND (A.SOURCE_CUST_ID=B.SOURCE_CUST_ID OR (A.SOURCE_CUST_ID IS NULL AND B.SOURCE_CUST_ID IS NULL)) """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.SOURCE_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/")
#任务[12] 001-08::
V_STEP = V_STEP + 1
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
sql = """
 SELECT A.ID                    AS ID 
       ,NVL(A.ODS_CUST_ID,B.ODS_CUST_ID )          AS ODS_CUST_ID 
       ,NVL(A.ODS_CUST_NAME,B.ODS_CUST_NAME) AS ODS_CUST_NAME
	   ,NVL(A.SOURCE_CUST_ID,B.SOURCE_CUST_ID)           AS SOURCE_CUST_ID 
       ,NVL(A.SOURCE_CUST_NAME,B.SOURCE_CUST_NAME)       AS SOURCE_CUST_NAME 
       ,NVL(A.CERT_TYPE,B.CERT_TYPE)            AS CERT_TYPE 
       ,NVL(A.CERT_NO,B.CERT_NO)          AS CERT_NO 
       ,B.ODS_SYS_ID                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,NVL(A.ODS_CUST_TYPE,B.ODS_CUST_TYPE)          AS ODS_CUST_TYPE 
       ,'1'                       AS CUST_STAT 
       ,B.BEGIN_DATE          AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,B.SYS_ID_FLAG           AS SYS_ID_FLAG 
       ,B.FR_ID          AS FR_ID 
       ,A.CERT_FLAG        AS CERT_FLAG
   FROM (SELECT DISTINCT 									
						 CASE WHEN LENGTH(TRIM(USER_CIFNO)) = 12 
						      THEN USER_CIFNO 
						      ELSE ''
						 END  AS ODS_CUST_ID                    , 
						 USER_CIFNO AS SOURCE_CUST_ID           ,
                         USERTC_CLIENTNAME AS ODS_CUST_NAME     ,
						 USERTC_CLIENTNAME AS SOURCE_CUST_NAME  ,
                         USERTC_CRENDENTYPE AS  CERT_TYPE       ,
                         USERTC_CRENDENID AS CERT_NO            ,
                         'APY' AS ODS_SYS_ID                    ,-- 支付宝
                         '1' AS ODS_CUST_TYPE                   ,-- 个人客户
                         MAX(concat(SUBSTR(USERTC_OPENDATE,1,4),'-',SUBSTR(USERTC_OPENDATE,5,2),'-',SUBSTR(USERTC_OPENDATE,7,2))) AS BEGIN_DATE    ,
                         '' AS END_DATE    ,
                         '100000000001' as sys_id_flag      ,     --系统标识字符串
                        COALESCE(A.FR_ID,'UNK') AS FR_ID
			 FROM F_CI_ZFB_FT_USER_PRO_SIGNINFO  A 
			 WHERE ODS_ST_DATE =V_DT
			   AND EXISTS (SELECT 1 FROM F_CI_ZFB_FT_USER_PRO_SIGNINFO B 
			                WHERE A.USER_CIFNO=B.USER_CIFNO 
			                  AND B.FR_ID = A.FR_ID
						        GROUP BY B.USER_CIFNO
						        HAVING COUNT(1) >1
						       )
				AND EXISTS (SELECT 1 FROM F_CI_ZFB_FT_USER_PRO_SIGNINFO B 
				            WHERE A.USER_CIFNO=B.USER_CIFNO 
				              AND B.UserTc_CloseDate IS NULL 
				              AND B.FR_ID = A.FR_ID
						       )
			 GROUP BY A.FR_ID,A.USER_CIFNO,A.USERTC_CLIENTNAME,A.USERTC_CRENDENTYPE,A.USERTC_CRENDENID )B
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE A                          --
     ON A.FR_ID = B.FR_ID
		AND A.SOURCE_CUST_NAME = B.SOURCE_CUST_NAME 
	      AND A.CERT_TYPE = B.CERT_TYPE 
	      AND A.CERT_NO = B.CERT_NO 
	      AND A.ODS_SYS_ID = 'APY'
        AND (A.SOURCE_CUST_ID=B.SOURCE_CUST_ID OR (A.SOURCE_CUST_ID IS NULL AND B.SOURCE_CUST_ID IS NULL))"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")


sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.ODS_CUST_ID                                        --核心客户号:src.ODS_CUST_ID
       ,DST.ODS_CUST_NAME                                      --核心客户名称:src.ODS_CUST_NAME
       ,DST.SOURCE_CUST_ID                                     --源系统客户号:src.SOURCE_CUST_ID
       ,DST.SOURCE_CUST_NAME                                   --院系统客户名称:src.SOURCE_CUST_NAME
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_CUST_TYPE                                      --客户类型:src.ODS_CUST_TYPE
       ,DST.CUST_STAT                                          --客户状态，1正式客户，2潜在客户:src.CUST_STAT
       ,DST.BEGIN_DATE                                         --开始日期:src.BEGIN_DATE
       ,DST.END_DATE                                           --结束日期:src.END_DATE
       ,DST.SYS_ID_FLAG                                        --系统标志位:src.SYS_ID_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CERT_FLAG                                          --主证件标志:src.CERT_FLAG
   FROM OCRM_F_CI_SYS_RESOURCE DST 
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE_INNTMP1 SRC 
     ON SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
    AND SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_ID      = DST.SOURCE_CUST_ID 
  WHERE SRC.SOURCE_CUST_NAME IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_SYS_RESOURCE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/")

#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/"+V_DT+".parquet")
