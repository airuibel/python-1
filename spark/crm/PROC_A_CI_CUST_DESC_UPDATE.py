#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_CI_CUST_DESC_UPDATE').setMaster(sys.argv[2])
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

OCRM_F_CI_GRADE_APPLY = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_GRADE_APPLY/*')
OCRM_F_CI_GRADE_APPLY.registerTempTable("OCRM_F_CI_GRADE_APPLY")
ACRM_F_CI_CUST_GRADE = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_GRADE/*')
ACRM_F_CI_CUST_GRADE.registerTempTable("ACRM_F_CI_CUST_GRADE")

#任务[12] 001-01::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC_BK/'+V_DT+'.parquet/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")

sql = """
 SELECT ID                      AS ID 
       ,CUST_ID                 AS CUST_ID 
       ,CUST_ZH_NAME            AS CUST_ZH_NAME 
       ,CUST_EN_NAME            AS CUST_EN_NAME 
       ,CUST_ABBR               AS CUST_ABBR 
       ,CUST_STAT               AS CUST_STAT 
       ,CUST_TYP                AS CUST_TYP 
       ,CERT_TYPE               AS CERT_TYPE 
       ,CERT_NUM                AS CERT_NUM 
       ,CUST_CRELVL             AS CUST_CRELVL 
       ,CUST_EVALUATEDATE       AS CUST_EVALUATEDATE 
       ,CUST_EVALUATEOVERDATE   AS CUST_EVALUATEOVERDATE 
       ,ODS_SYS_ID              AS ODS_SYS_ID 
       ,CRM_DT                  AS CRM_DT 
       ,IF_SHAREHOLDER          AS IF_SHAREHOLDER 
       ,IF_STRATEGY             AS IF_STRATEGY 
       ,OBJ_RATING              AS OBJ_RATING 
       ,OBJ_DATE                AS OBJ_DATE 
       ,OBJ_RATING              AS OLD_OBJ_RATING 
       ,SUB_RATING              AS SUB_RATING 
       ,SUB_DATE                AS SUB_DATE 
       ,OLD_SUB_RATING          AS OLD_SUB_RATING 
       ,IS_STAFF                AS IS_STAFF 
       ,CUST_LEV                AS CUST_LEV 
       ,HB_FLAG                 AS HB_FLAG 
       ,BILL_REPLACE_FLAG       AS BILL_REPLACE_FLAG 
       ,OBLIGATION_FLAG         AS OBLIGATION_FLAG 
       ,WAGE_ARREAR_FALG        AS WAGE_ARREAR_FALG 
       ,TAXES_FLAG              AS TAXES_FLAG 
       ,USURY_FLAG              AS USURY_FLAG 
       ,OPERATION_SAFE_FALG     AS OPERATION_SAFE_FALG 
       ,OBJ_NAME                AS OBJ_NAME 
       ,OBJ_NAME                AS OLD_OBJ_NAME 
       ,GRADE_HAND_NAME         AS GRADE_HAND_NAME 
       ,IS_MODIFY               AS IS_MODIFY 
       ,IS_CRE                  AS IS_CRE 
       ,FR_ID                   AS FR_ID 
       ,UN_NULL_RATE            AS UN_NULL_RATE 
       ,HOLD_PRO_FLAG           AS HOLD_PRO_FLAG 
       ,GPS                     AS GPS 
       ,LONGITUDE               AS LONGITUDE 
       ,LATITUDE                AS LATITUDE 
       ,LINK_TEL                AS LINK_TEL 
       ,LINK_MAN                AS LINK_MAN 
       ,CUST_ADDR               AS CUST_ADDR 
       ,VALID_FLG               AS VALID_FLG 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC_INNTMP1.registerTempTable("OCRM_F_CI_CUST_DESC_INNTMP1")

#OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
#OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_ZH_NAME                                       --客户（中文）名称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名称:src.CUST_EN_NAME
       ,DST.CUST_ABBR                                          --:src.CUST_ABBR
       ,DST.CUST_STAT                                          --客户状态:src.CUST_STAT
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_CRELVL                                        --:src.CUST_CRELVL
       ,DST.CUST_EVALUATEDATE                                  --:src.CUST_EVALUATEDATE
       ,DST.CUST_EVALUATEOVERDATE                              --:src.CUST_EVALUATEOVERDATE
       ,DST.ODS_SYS_ID                                         --系统编号:src.ODS_SYS_ID
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.IF_SHAREHOLDER                                     --是否股东:src.IF_SHAREHOLDER
       ,DST.IF_STRATEGY                                        --:src.IF_STRATEGY
       ,DST.OBJ_RATING                                         --客户客观评级:src.OBJ_RATING
       ,DST.OBJ_DATE                                           --评级日期:src.OBJ_DATE
       ,DST.OLD_OBJ_RATING                                     --上次评级:src.OLD_OBJ_RATING
       ,DST.SUB_RATING                                         --客户主观评级:src.SUB_RATING
       ,DST.SUB_DATE                                           --:src.SUB_DATE
       ,DST.OLD_SUB_RATING                                     --:src.OLD_SUB_RATING
       ,DST.IS_STAFF                                           --是否本行员工:src.IS_STAFF
       ,DST.CUST_LEV                                           --客户级别:src.CUST_LEV
       ,DST.HB_FLAG                                            --合并标志:src.HB_FLAG
       ,DST.BILL_REPLACE_FLAG                                  --:src.BILL_REPLACE_FLAG
       ,DST.OBLIGATION_FLAG                                    --:src.OBLIGATION_FLAG
       ,DST.WAGE_ARREAR_FALG                                   --:src.WAGE_ARREAR_FALG
       ,DST.TAXES_FLAG                                         --:src.TAXES_FLAG
       ,DST.USURY_FLAG                                         --:src.USURY_FLAG
       ,DST.OPERATION_SAFE_FALG                                --:src.OPERATION_SAFE_FALG
       ,DST.OBJ_NAME                                           --客观评级名称:src.OBJ_NAME
       ,DST.OLD_OBJ_NAME                                       --上次客观评级名称:src.OLD_OBJ_NAME
       ,DST.GRADE_HAND_NAME                                    --主观评级名称:src.GRADE_HAND_NAME
       ,DST.IS_MODIFY                                          --是否前台修改:src.IS_MODIFY
       ,DST.IS_CRE                                             --是否有贷户:src.IS_CRE
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.UN_NULL_RATE                                       --客户信息完整度:src.UN_NULL_RATE
       ,DST.HOLD_PRO_FLAG                                      --持有产品标识:src.HOLD_PRO_FLAG
       ,DST.GPS                                                --GPS:src.GPS
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.LINK_MAN                                           --联系人:src.LINK_MAN
       ,DST.CUST_ADDR                                          --客户地址:src.CUST_ADDR
       ,DST.VALID_FLG                                          --有效标志:src.VALID_FLG
   FROM OCRM_F_CI_CUST_DESC DST 
   LEFT JOIN OCRM_F_CI_CUST_DESC_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_DESC/"+V_DT+".parquet"
UNION=OCRM_F_CI_CUST_DESC_INNTMP2.unionAll(OCRM_F_CI_CUST_DESC_INNTMP1)
OCRM_F_CI_CUST_DESC_INNTMP1.cache()
OCRM_F_CI_CUST_DESC_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_DESC_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_DESC_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_DESC_INNTMP1.unpersist()
OCRM_F_CI_CUST_DESC_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/")

#任务[12] 001-02::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_ABBR             AS CUST_ABBR 
       ,A.CUST_STAT             AS CUST_STAT 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.CERT_TYPE             AS CERT_TYPE 
       ,A.CERT_NUM              AS CERT_NUM 
       ,A.CUST_CRELVL           AS CUST_CRELVL 
       ,A.CUST_EVALUATEDATE     AS CUST_EVALUATEDATE 
       ,A.CUST_EVALUATEOVERDATE AS CUST_EVALUATEOVERDATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.CRM_DT                AS CRM_DT 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_STRATEGY           AS IF_STRATEGY 
       ,B.GRADE_LEVEL           AS OBJ_RATING 
       ,V_DT                    AS OBJ_DATE 
       ,A.OBJ_RATING            AS OLD_OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,A.SUB_DATE              AS SUB_DATE 
       ,A.OLD_SUB_RATING        AS OLD_SUB_RATING 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUST_LEV              AS CUST_LEV 
       ,A.HB_FLAG               AS HB_FLAG 
       ,A.BILL_REPLACE_FLAG     AS BILL_REPLACE_FLAG 
       ,A.OBLIGATION_FLAG       AS OBLIGATION_FLAG 
       ,A.WAGE_ARREAR_FALG      AS WAGE_ARREAR_FALG 
       ,A.TAXES_FLAG            AS TAXES_FLAG 
       ,A.USURY_FLAG            AS USURY_FLAG 
       ,A.OPERATION_SAFE_FALG   AS OPERATION_SAFE_FALG 
       ,A.OBJ_NAME              AS OBJ_NAME 
       ,A.OBJ_NAME              AS OLD_OBJ_NAME 
       ,A.GRADE_HAND_NAME       AS GRADE_HAND_NAME 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.IS_CRE                AS IS_CRE 
       ,A.FR_ID                 AS FR_ID 
       ,A.UN_NULL_RATE          AS UN_NULL_RATE 
       ,A.HOLD_PRO_FLAG         AS HOLD_PRO_FLAG 
       ,A.GPS                   AS GPS 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.LINK_MAN              AS LINK_MAN 
       ,A.CUST_ADDR             AS CUST_ADDR 
       ,A.VALID_FLG             AS VALID_FLG 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN(
         SELECT FR_ID 
               ,CUST_ID 
               ,MAX(GRADE_LEVEL)                       AS GRADE_LEVEL 
           FROM ACRM_F_CI_CUST_GRADE 
          WHERE ETL_DT                  = V_DT 
          GROUP BY FR_ID 
               ,CUST_ID) B                                     --客户评级
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC_INNTMP1.registerTempTable("OCRM_F_CI_CUST_DESC_INNTMP1")

#OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
#OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_ZH_NAME                                       --客户（中文）名称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名称:src.CUST_EN_NAME
       ,DST.CUST_ABBR                                          --:src.CUST_ABBR
       ,DST.CUST_STAT                                          --客户状态:src.CUST_STAT
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_CRELVL                                        --:src.CUST_CRELVL
       ,DST.CUST_EVALUATEDATE                                  --:src.CUST_EVALUATEDATE
       ,DST.CUST_EVALUATEOVERDATE                              --:src.CUST_EVALUATEOVERDATE
       ,DST.ODS_SYS_ID                                         --系统编号:src.ODS_SYS_ID
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.IF_SHAREHOLDER                                     --是否股东:src.IF_SHAREHOLDER
       ,DST.IF_STRATEGY                                        --:src.IF_STRATEGY
       ,DST.OBJ_RATING                                         --客户客观评级:src.OBJ_RATING
       ,DST.OBJ_DATE                                           --评级日期:src.OBJ_DATE
       ,DST.OLD_OBJ_RATING                                     --上次评级:src.OLD_OBJ_RATING
       ,DST.SUB_RATING                                         --客户主观评级:src.SUB_RATING
       ,DST.SUB_DATE                                           --:src.SUB_DATE
       ,DST.OLD_SUB_RATING                                     --:src.OLD_SUB_RATING
       ,DST.IS_STAFF                                           --是否本行员工:src.IS_STAFF
       ,DST.CUST_LEV                                           --客户级别:src.CUST_LEV
       ,DST.HB_FLAG                                            --合并标志:src.HB_FLAG
       ,DST.BILL_REPLACE_FLAG                                  --:src.BILL_REPLACE_FLAG
       ,DST.OBLIGATION_FLAG                                    --:src.OBLIGATION_FLAG
       ,DST.WAGE_ARREAR_FALG                                   --:src.WAGE_ARREAR_FALG
       ,DST.TAXES_FLAG                                         --:src.TAXES_FLAG
       ,DST.USURY_FLAG                                         --:src.USURY_FLAG
       ,DST.OPERATION_SAFE_FALG                                --:src.OPERATION_SAFE_FALG
       ,DST.OBJ_NAME                                           --客观评级名称:src.OBJ_NAME
       ,DST.OLD_OBJ_NAME                                       --上次客观评级名称:src.OLD_OBJ_NAME
       ,DST.GRADE_HAND_NAME                                    --主观评级名称:src.GRADE_HAND_NAME
       ,DST.IS_MODIFY                                          --是否前台修改:src.IS_MODIFY
       ,DST.IS_CRE                                             --是否有贷户:src.IS_CRE
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.UN_NULL_RATE                                       --客户信息完整度:src.UN_NULL_RATE
       ,DST.HOLD_PRO_FLAG                                      --持有产品标识:src.HOLD_PRO_FLAG
       ,DST.GPS                                                --GPS:src.GPS
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.LINK_MAN                                           --联系人:src.LINK_MAN
       ,DST.CUST_ADDR                                          --客户地址:src.CUST_ADDR
       ,DST.VALID_FLG                                          --有效标志:src.VALID_FLG
   FROM OCRM_F_CI_CUST_DESC DST 
   LEFT JOIN OCRM_F_CI_CUST_DESC_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_DESC/"+V_DT+".parquet"
UNION=OCRM_F_CI_CUST_DESC_INNTMP2.unionAll(OCRM_F_CI_CUST_DESC_INNTMP1)
OCRM_F_CI_CUST_DESC_INNTMP1.cache()
OCRM_F_CI_CUST_DESC_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_DESC_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_DESC_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_DESC_INNTMP1.unpersist()
OCRM_F_CI_CUST_DESC_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/")



#任务[12] 001-03::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_ABBR             AS CUST_ABBR 
       ,A.CUST_STAT             AS CUST_STAT 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.CERT_TYPE             AS CERT_TYPE 
       ,A.CERT_NUM              AS CERT_NUM 
       ,A.CUST_CRELVL           AS CUST_CRELVL 
       ,A.CUST_EVALUATEDATE     AS CUST_EVALUATEDATE 
       ,A.CUST_EVALUATEOVERDATE AS CUST_EVALUATEOVERDATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.CRM_DT                AS CRM_DT 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_STRATEGY           AS IF_STRATEGY 
       ,CASE WHEN A.CUST_TYP              = '1' THEN '17' ELSE '27' END                     AS OBJ_RATING 
       ,A.OBJ_DATE              AS OBJ_DATE 
       ,A.OLD_OBJ_RATING        AS OLD_OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,A.SUB_DATE              AS SUB_DATE 
       ,A.OLD_SUB_RATING        AS OLD_SUB_RATING 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUST_LEV              AS CUST_LEV 
       ,A.HB_FLAG               AS HB_FLAG 
       ,A.BILL_REPLACE_FLAG     AS BILL_REPLACE_FLAG 
       ,A.OBLIGATION_FLAG       AS OBLIGATION_FLAG 
       ,A.WAGE_ARREAR_FALG      AS WAGE_ARREAR_FALG 
       ,A.TAXES_FLAG            AS TAXES_FLAG 
       ,A.USURY_FLAG            AS USURY_FLAG 
       ,A.OPERATION_SAFE_FALG   AS OPERATION_SAFE_FALG 
       ,A.OBJ_NAME              AS OBJ_NAME 
       ,A.OLD_OBJ_NAME          AS OLD_OBJ_NAME 
       ,A.GRADE_HAND_NAME       AS GRADE_HAND_NAME 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.IS_CRE                AS IS_CRE 
       ,A.FR_ID                 AS FR_ID 
       ,A.UN_NULL_RATE          AS UN_NULL_RATE 
       ,A.HOLD_PRO_FLAG         AS HOLD_PRO_FLAG 
       ,A.GPS                   AS GPS 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.LINK_MAN              AS LINK_MAN 
       ,A.CUST_ADDR             AS CUST_ADDR 
       ,A.VALID_FLG             AS VALID_FLG 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
   LEFT JOIN ACRM_F_CI_CUST_GRADE B                            --评级表
     ON A.FR_ID                 = B.FR_ID 
    AND A.CUST_ID               = B.CUST_ID 
    AND B.ETL_DT                = V_DT 
  WHERE B.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC_INNTMP1.registerTempTable("OCRM_F_CI_CUST_DESC_INNTMP1")

#OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
#OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_ZH_NAME                                       --客户（中文）名称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名称:src.CUST_EN_NAME
       ,DST.CUST_ABBR                                          --:src.CUST_ABBR
       ,DST.CUST_STAT                                          --客户状态:src.CUST_STAT
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_CRELVL                                        --:src.CUST_CRELVL
       ,DST.CUST_EVALUATEDATE                                  --:src.CUST_EVALUATEDATE
       ,DST.CUST_EVALUATEOVERDATE                              --:src.CUST_EVALUATEOVERDATE
       ,DST.ODS_SYS_ID                                         --系统编号:src.ODS_SYS_ID
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.IF_SHAREHOLDER                                     --是否股东:src.IF_SHAREHOLDER
       ,DST.IF_STRATEGY                                        --:src.IF_STRATEGY
       ,DST.OBJ_RATING                                         --客户客观评级:src.OBJ_RATING
       ,DST.OBJ_DATE                                           --评级日期:src.OBJ_DATE
       ,DST.OLD_OBJ_RATING                                     --上次评级:src.OLD_OBJ_RATING
       ,DST.SUB_RATING                                         --客户主观评级:src.SUB_RATING
       ,DST.SUB_DATE                                           --:src.SUB_DATE
       ,DST.OLD_SUB_RATING                                     --:src.OLD_SUB_RATING
       ,DST.IS_STAFF                                           --是否本行员工:src.IS_STAFF
       ,DST.CUST_LEV                                           --客户级别:src.CUST_LEV
       ,DST.HB_FLAG                                            --合并标志:src.HB_FLAG
       ,DST.BILL_REPLACE_FLAG                                  --:src.BILL_REPLACE_FLAG
       ,DST.OBLIGATION_FLAG                                    --:src.OBLIGATION_FLAG
       ,DST.WAGE_ARREAR_FALG                                   --:src.WAGE_ARREAR_FALG
       ,DST.TAXES_FLAG                                         --:src.TAXES_FLAG
       ,DST.USURY_FLAG                                         --:src.USURY_FLAG
       ,DST.OPERATION_SAFE_FALG                                --:src.OPERATION_SAFE_FALG
       ,DST.OBJ_NAME                                           --客观评级名称:src.OBJ_NAME
       ,DST.OLD_OBJ_NAME                                       --上次客观评级名称:src.OLD_OBJ_NAME
       ,DST.GRADE_HAND_NAME                                    --主观评级名称:src.GRADE_HAND_NAME
       ,DST.IS_MODIFY                                          --是否前台修改:src.IS_MODIFY
       ,DST.IS_CRE                                             --是否有贷户:src.IS_CRE
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.UN_NULL_RATE                                       --客户信息完整度:src.UN_NULL_RATE
       ,DST.HOLD_PRO_FLAG                                      --持有产品标识:src.HOLD_PRO_FLAG
       ,DST.GPS                                                --GPS:src.GPS
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.LINK_MAN                                           --联系人:src.LINK_MAN
       ,DST.CUST_ADDR                                          --客户地址:src.CUST_ADDR
       ,DST.VALID_FLG                                          --有效标志:src.VALID_FLG
   FROM OCRM_F_CI_CUST_DESC DST 
   LEFT JOIN OCRM_F_CI_CUST_DESC_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_DESC/"+V_DT+".parquet"
UNION=OCRM_F_CI_CUST_DESC_INNTMP2.unionAll(OCRM_F_CI_CUST_DESC_INNTMP1)
OCRM_F_CI_CUST_DESC_INNTMP1.cache()
OCRM_F_CI_CUST_DESC_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_DESC_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_DESC_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_DESC_INNTMP1.unpersist()
OCRM_F_CI_CUST_DESC_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/")


#任务[12] 001-04::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")

sql = """
 SELECT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,A.CUST_EN_NAME          AS CUST_EN_NAME 
       ,A.CUST_ABBR             AS CUST_ABBR 
       ,A.CUST_STAT             AS CUST_STAT 
       ,A.CUST_TYP              AS CUST_TYP 
       ,A.CERT_TYPE             AS CERT_TYPE 
       ,A.CERT_NUM              AS CERT_NUM 
       ,A.CUST_CRELVL           AS CUST_CRELVL 
       ,A.CUST_EVALUATEDATE     AS CUST_EVALUATEDATE 
       ,A.CUST_EVALUATEOVERDATE AS CUST_EVALUATEOVERDATE 
       ,A.ODS_SYS_ID            AS ODS_SYS_ID 
       ,A.CRM_DT                AS CRM_DT 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_STRATEGY           AS IF_STRATEGY 
       ,B.TO_GRADE              AS OBJ_RATING 
       ,A.OBJ_DATE              AS OBJ_DATE 
       ,A.OLD_OBJ_RATING        AS OLD_OBJ_RATING 
       ,A.SUB_RATING            AS SUB_RATING 
       ,A.SUB_DATE              AS SUB_DATE 
       ,A.OLD_SUB_RATING        AS OLD_SUB_RATING 
       ,A.IS_STAFF              AS IS_STAFF 
       ,A.CUST_LEV              AS CUST_LEV 
       ,A.HB_FLAG               AS HB_FLAG 
       ,A.BILL_REPLACE_FLAG     AS BILL_REPLACE_FLAG 
       ,A.OBLIGATION_FLAG       AS OBLIGATION_FLAG 
       ,A.WAGE_ARREAR_FALG      AS WAGE_ARREAR_FALG 
       ,A.TAXES_FLAG            AS TAXES_FLAG 
       ,A.USURY_FLAG            AS USURY_FLAG 
       ,A.OPERATION_SAFE_FALG   AS OPERATION_SAFE_FALG 
       ,A.OBJ_NAME              AS OBJ_NAME 
       ,A.OLD_OBJ_NAME          AS OLD_OBJ_NAME 
       ,A.GRADE_HAND_NAME       AS GRADE_HAND_NAME 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.IS_CRE                AS IS_CRE 
       ,A.FR_ID                 AS FR_ID 
       ,A.UN_NULL_RATE          AS UN_NULL_RATE 
       ,A.HOLD_PRO_FLAG         AS HOLD_PRO_FLAG 
       ,A.GPS                   AS GPS 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.LINK_MAN              AS LINK_MAN 
       ,A.CUST_ADDR             AS CUST_ADDR 
       ,A.VALID_FLG             AS VALID_FLG 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN(
         SELECT FR_ID 
               ,CUST_ID 
               ,TO_GRADE 
               ,ROW_NUMBER() OVER(
              PARTITION BY CUST_ID 
                  ORDER BY APPLY_DATE DESC) RN 
           FROM OCRM_F_CI_GRADE_APPLY 
          WHERE STATUS                  = '2') B               --评级1
     ON A.CUST_ID               = B.CUST_ID 
    AND A.FR_ID                 = B.FR_ID 
    AND B.RN                    = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC_INNTMP1.registerTempTable("OCRM_F_CI_CUST_DESC_INNTMP1")

#OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
#OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_ZH_NAME                                       --客户（中文）名称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名称:src.CUST_EN_NAME
       ,DST.CUST_ABBR                                          --:src.CUST_ABBR
       ,DST.CUST_STAT                                          --客户状态:src.CUST_STAT
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_CRELVL                                        --:src.CUST_CRELVL
       ,DST.CUST_EVALUATEDATE                                  --:src.CUST_EVALUATEDATE
       ,DST.CUST_EVALUATEOVERDATE                              --:src.CUST_EVALUATEOVERDATE
       ,DST.ODS_SYS_ID                                         --系统编号:src.ODS_SYS_ID
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.IF_SHAREHOLDER                                     --是否股东:src.IF_SHAREHOLDER
       ,DST.IF_STRATEGY                                        --:src.IF_STRATEGY
       ,DST.OBJ_RATING                                         --客户客观评级:src.OBJ_RATING
       ,DST.OBJ_DATE                                           --评级日期:src.OBJ_DATE
       ,DST.OLD_OBJ_RATING                                     --上次评级:src.OLD_OBJ_RATING
       ,DST.SUB_RATING                                         --客户主观评级:src.SUB_RATING
       ,DST.SUB_DATE                                           --:src.SUB_DATE
       ,DST.OLD_SUB_RATING                                     --:src.OLD_SUB_RATING
       ,DST.IS_STAFF                                           --是否本行员工:src.IS_STAFF
       ,DST.CUST_LEV                                           --客户级别:src.CUST_LEV
       ,DST.HB_FLAG                                            --合并标志:src.HB_FLAG
       ,DST.BILL_REPLACE_FLAG                                  --:src.BILL_REPLACE_FLAG
       ,DST.OBLIGATION_FLAG                                    --:src.OBLIGATION_FLAG
       ,DST.WAGE_ARREAR_FALG                                   --:src.WAGE_ARREAR_FALG
       ,DST.TAXES_FLAG                                         --:src.TAXES_FLAG
       ,DST.USURY_FLAG                                         --:src.USURY_FLAG
       ,DST.OPERATION_SAFE_FALG                                --:src.OPERATION_SAFE_FALG
       ,DST.OBJ_NAME                                           --客观评级名称:src.OBJ_NAME
       ,DST.OLD_OBJ_NAME                                       --上次客观评级名称:src.OLD_OBJ_NAME
       ,DST.GRADE_HAND_NAME                                    --主观评级名称:src.GRADE_HAND_NAME
       ,DST.IS_MODIFY                                          --是否前台修改:src.IS_MODIFY
       ,DST.IS_CRE                                             --是否有贷户:src.IS_CRE
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.UN_NULL_RATE                                       --客户信息完整度:src.UN_NULL_RATE
       ,DST.HOLD_PRO_FLAG                                      --持有产品标识:src.HOLD_PRO_FLAG
       ,DST.GPS                                                --GPS:src.GPS
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.LINK_MAN                                           --联系人:src.LINK_MAN
       ,DST.CUST_ADDR                                          --客户地址:src.CUST_ADDR
       ,DST.VALID_FLG                                          --有效标志:src.VALID_FLG
   FROM OCRM_F_CI_CUST_DESC DST 
   LEFT JOIN OCRM_F_CI_CUST_DESC_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_DESC/"+V_DT+".parquet"
UNION=OCRM_F_CI_CUST_DESC_INNTMP2.unionAll(OCRM_F_CI_CUST_DESC_INNTMP1)
OCRM_F_CI_CUST_DESC_INNTMP1.cache()
OCRM_F_CI_CUST_DESC_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_DESC_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_DESC_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_DESC_INNTMP1.unpersist()
OCRM_F_CI_CUST_DESC_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/")