#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_CUST_FLAG').setMaster(sys.argv[2])
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
#删除当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT+".parquet")
#从备份表拷贝当天的数据，依赖PROC_F_CI_CUST_DESC
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT+".parquet")

OCRM_F_CI_CUST_SIGN = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_SIGN/*')
OCRM_F_CI_CUST_SIGN.registerTempTable("OCRM_F_CI_CUST_SIGN")
OCRM_F_CI_CUST_HOLD_PRO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_HOLD_PRO/*')
OCRM_F_CI_CUST_HOLD_PRO.registerTempTable("OCRM_F_CI_CUST_HOLD_PRO")
OCRM_F_CI_CUST_DESC = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_DESC/*')
OCRM_F_CI_CUST_DESC.registerTempTable("OCRM_F_CI_CUST_DESC")
#OCRM_F_CI_CUST_FLAG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_FLAG/*')
#OCRM_F_CI_CUST_FLAG.registerTempTable("OCRM_F_CI_CUST_FLAG")


#任务[11] 001-01::
V_STEP = V_STEP + 1

sql = """
 SELECT A.CUST_ID               AS CUST_ID 
       ,concat(SUBSTR(A.ODS_SYS_ID, 5, 2) , SUBSTR(A.ODS_SYS_ID, 10, 1) , SUBSTR(A.ODS_SYS_ID, 12, 1))                       AS SIGN_1 
       ,B.SIGN_FLAG             AS SIGN_2 
       ,''          AS SIGN_3 
       ,''                      AS SIGN_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS CRM_DT 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN OCRM_F_CI_CUST_SIGN B                             --客户签约表
     ON B.FR_ID                 = A.FR_ID 
    AND B.ST_DATE               = V_DT 
    AND A.CUST_ID               = B.CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_FLAG = sqlContext.sql(sql)
OCRM_F_CI_CUST_FLAG.registerTempTable("OCRM_F_CI_CUST_FLAG")
dfn="OCRM_F_CI_CUST_FLAG/"+V_DT+".parquet"
OCRM_F_CI_CUST_FLAG.cache()
nrows = OCRM_F_CI_CUST_FLAG.count()

#清除数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_FLAG/*.parquet")
OCRM_F_CI_CUST_FLAG.write.save(path=hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_FLAG.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_FLAG lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

#任务[12] 001-02::
V_STEP = V_STEP + 1
OCRM_F_CI_CUST_FLAG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_FLAG/*')
OCRM_F_CI_CUST_FLAG.registerTempTable("OCRM_F_CI_CUST_FLAG")
sql = """
 SELECT M.CUST_ID               AS CUST_ID 
       ,CASE WHEN N.CUST_ID IS NULL THEN M.SIGN_1 ELSE N.SIGN_2    END                AS SIGN_1 
       ,CASE WHEN N.CUST_ID IS NULL THEN '' ELSE N.SIGN_2    END      AS SIGN_2 
       ,M.PROD_FLAG             AS SIGN_3 
       ,CASE WHEN N.CUST_ID IS NULL THEN '' ELSE N.SIGN_FLAG    END     AS SIGN_FLAG 
       ,M.FR_ID                 AS FR_ID 
       ,V_DT                    AS CRM_DT 
   FROM (SELECT A.CUST_ID,
                    CONCAT(SUBSTR(A.ODS_SYS_ID,5,2),SUBSTR(A.ODS_SYS_ID,10,1),SUBSTR(A.ODS_SYS_ID,12,1)) AS SIGN_1,
                    PROD_FLAG ,
					A.FR_ID
         FROM OCRM_F_CI_CUST_DESC A           --统一客户信息表
         JOIN OCRM_F_CI_CUST_HOLD_PRO B       --客户持有产品临时表
		ON B.FR_ID = A.FR_ID AND B.ETL_DATE = V_DT AND A.CUST_ID = B.CUST_ID ) M
	LEFT JOIN OCRM_F_CI_CUST_FLAG N            --客户签约-持有产品临时表
	ON M.CUST_ID = N.CUST_ID AND M.FR_ID = N.FR_ID
   """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_FLAG_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_FLAG_INNTMP1.registerTempTable("OCRM_F_CI_CUST_FLAG_INNTMP1")


sql = """
 SELECT DST.CUST_ID                                             --客户编号:src.CUST_ID
       ,DST.SIGN_1                                             --系统标志:src.SIGN_1
       ,DST.SIGN_2                                             --签约渠道(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_2
       ,DST.SIGN_3                                             --持有产品标志:src.SIGN_3
       ,DST.SIGN_FLAG                                          --汇总签约标志:src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --平台数据日期:src.CRM_DT
   FROM OCRM_F_CI_CUST_FLAG DST 
   LEFT JOIN OCRM_F_CI_CUST_FLAG_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.CUST_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_FLAG_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_FLAG/"+V_DT+".parquet"
OCRM_F_CI_CUST_FLAG_INNTMP2=OCRM_F_CI_CUST_FLAG_INNTMP2.unionAll(OCRM_F_CI_CUST_FLAG_INNTMP1)
OCRM_F_CI_CUST_FLAG_INNTMP1.cache()
OCRM_F_CI_CUST_FLAG_INNTMP2.cache()
print(OCRM_F_CI_CUST_FLAG_INNTMP1.count())
print(OCRM_F_CI_CUST_FLAG_INNTMP2.count())
nrowsi = OCRM_F_CI_CUST_FLAG_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_FLAG_INNTMP2.count()
OCRM_F_CI_CUST_FLAG_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_FLAG_INNTMP1.unpersist()
OCRM_F_CI_CUST_FLAG_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_FLAG lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-03::
V_STEP = V_STEP + 1


OCRM_F_CI_CUST_FLAG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_FLAG/*')
OCRM_F_CI_CUST_FLAG.registerTempTable("OCRM_F_CI_CUST_FLAG")

sql = """
 SELECT CUST_ID                 AS CUST_ID 
       ,SIGN_1                  AS SIGN_1 
       ,SIGN_2                  AS SIGN_2 
       ,SIGN_3                  AS SIGN_3 
       ,CONCAT(SIGN_1 , SIGN_2 , SIGN_3)                  AS SIGN_FLAG 
       ,FR_ID                   AS FR_ID 
       ,CRM_DT                  AS CRM_DT 
   FROM OCRM_F_CI_CUST_FLAG A                                  --客户签约-持有产品临时表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_FLAG_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_FLAG_INNTMP1.registerTempTable("OCRM_F_CI_CUST_FLAG_INNTMP1")


sql = """
 SELECT DST.CUST_ID                                             --客户号:src.CUST_ID
       ,DST.SIGN_1                                             --系统标志:src.SIGN_1
       ,DST.SIGN_2                                             --签约渠道(网银-手机银行-短信-电费-水费-燃气-广电-电信):src.SIGN_2
       ,DST.SIGN_3                                             --持有产品标志:src.SIGN_3
       ,DST.SIGN_FLAG                                          --汇总签约标志:src.SIGN_FLAG
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.CRM_DT                                             --ETL日期:src.CRM_DT
   FROM OCRM_F_CI_CUST_FLAG DST 
   LEFT JOIN OCRM_F_CI_CUST_FLAG_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_FLAG_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_CUST_FLAG/"+V_DT+".parquet"
OCRM_F_CI_CUST_FLAG_INNTMP2=OCRM_F_CI_CUST_FLAG_INNTMP2.unionAll(OCRM_F_CI_CUST_FLAG_INNTMP1)
OCRM_F_CI_CUST_FLAG_INNTMP1.cache()
OCRM_F_CI_CUST_FLAG_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_FLAG_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_FLAG_INNTMP2.count()
OCRM_F_CI_CUST_FLAG_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_CUST_FLAG_INNTMP1.unpersist()
OCRM_F_CI_CUST_FLAG_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_FLAG lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)


#任务[12] 001-04::
V_STEP = V_STEP + 1

OCRM_F_CI_CUST_FLAG = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_CUST_FLAG/*')
OCRM_F_CI_CUST_FLAG.registerTempTable("OCRM_F_CI_CUST_FLAG")

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
       ,V_DT                    AS CRM_DT 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_STRATEGY           AS IF_STRATEGY 
       ,A.OBJ_RATING            AS OBJ_RATING 
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
       ,B.SIGN_FLAG             AS HOLD_PRO_FLAG 
       ,A.GPS                   AS GPS 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.LINK_MAN              AS LINK_MAN 
       ,A.CUST_ADDR             AS CUST_ADDR 
       ,A.VALID_FLG             AS VALID_FLG 
   FROM OCRM_F_CI_CUST_DESC A                                  --统一客户信息表
  INNER JOIN (SELECT CUST_ID,FR_ID,SIGN_FLAG,ROW_NUMBER() OVER(PARTITION BY CUST_ID,FR_ID ORDER BY SIGN_FLAG DESC ) RN
               FROM OCRM_F_CI_CUST_FLAG) B                             --客户签约-持有产品临时表
     ON B.FR_ID                 = A.FR_ID 
    AND A.CUST_ID               = B.CUST_ID 
	AND B.RN = 1 """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_CUST_DESC_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_CUST_DESC_INNTMP1.registerTempTable("OCRM_F_CI_CUST_DESC_INNTMP1")

sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_ZH_NAME                                       --客户（中文）名称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名称:src.CUST_EN_NAME
       ,DST.CUST_ABBR                                          --0:src.CUST_ABBR
       ,DST.CUST_STAT                                          --客户状态:src.CUST_STAT
       ,DST.CUST_TYP                                           --客户类型:src.CUST_TYP
       ,DST.CERT_TYPE                                          --证件类型:src.CERT_TYPE
       ,DST.CERT_NUM                                           --证件号码:src.CERT_NUM
       ,DST.CUST_CRELVL                                        --0:src.CUST_CRELVL
       ,DST.CUST_EVALUATEDATE                                  --0:src.CUST_EVALUATEDATE
       ,DST.CUST_EVALUATEOVERDATE                              --0:src.CUST_EVALUATEOVERDATE
       ,DST.ODS_SYS_ID                                         --系统编号:src.ODS_SYS_ID
       ,DST.CRM_DT                                             --平台日期:src.CRM_DT
       ,DST.IF_SHAREHOLDER                                     --是否股东:src.IF_SHAREHOLDER
       ,DST.IF_STRATEGY                                        --0:src.IF_STRATEGY
       ,DST.OBJ_RATING                                         --客户客观评级:src.OBJ_RATING
       ,DST.OBJ_DATE                                           --评级日期:src.OBJ_DATE
       ,DST.OLD_OBJ_RATING                                     --上次评级:src.OLD_OBJ_RATING
       ,DST.SUB_RATING                                         --客户主观评级:src.SUB_RATING
       ,DST.SUB_DATE                                           --0:src.SUB_DATE
       ,DST.OLD_SUB_RATING                                     --0:src.OLD_SUB_RATING
       ,DST.IS_STAFF                                           --是否本行员工:src.IS_STAFF
       ,DST.CUST_LEV                                           --客户级别:src.CUST_LEV
       ,DST.HB_FLAG                                            --合并标志:src.HB_FLAG
       ,DST.BILL_REPLACE_FLAG                                  --0:src.BILL_REPLACE_FLAG
       ,DST.OBLIGATION_FLAG                                    --0:src.OBLIGATION_FLAG
       ,DST.WAGE_ARREAR_FALG                                   --0:src.WAGE_ARREAR_FALG
       ,DST.TAXES_FLAG                                         --0:src.TAXES_FLAG
       ,DST.USURY_FLAG                                         --0:src.USURY_FLAG
       ,DST.OPERATION_SAFE_FALG                                --0:src.OPERATION_SAFE_FALG
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
OCRM_F_CI_CUST_DESC_INNTMP2=OCRM_F_CI_CUST_DESC_INNTMP2.unionAll(OCRM_F_CI_CUST_DESC_INNTMP1)
OCRM_F_CI_CUST_DESC_INNTMP1.cache()
OCRM_F_CI_CUST_DESC_INNTMP2.cache()
nrowsi = OCRM_F_CI_CUST_DESC_INNTMP1.count()
nrowsa = OCRM_F_CI_CUST_DESC_INNTMP2.count()
OCRM_F_CI_CUST_DESC_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#删除
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/"+V_DT+".parquet ")
#备份最新数据
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/"+V_DT+".parquet")

OCRM_F_CI_CUST_DESC_INNTMP1.unpersist()
OCRM_F_CI_CUST_DESC_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert OCRM_F_CI_CUST_DESC lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/OCRM_F_CI_CUST_DESC/"+V_DT_LD+".parquet /"+dbname+"/OCRM_F_CI_CUST_DESC_BK/")
