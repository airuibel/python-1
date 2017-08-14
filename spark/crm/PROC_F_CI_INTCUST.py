#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_F_CI_INTCUST').setMaster(sys.argv[2])
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
F_CM_GJJS_PTC = sqlContext.read.parquet(hdfs+'/F_CM_GJJS_PTC/*')
F_CM_GJJS_PTC.registerTempTable("F_CM_GJJS_PTC")
F_CI_GJJS_PTY = sqlContext.read.parquet(hdfs+'/F_CI_GJJS_PTY/*')
F_CI_GJJS_PTY.registerTempTable("F_CI_GJJS_PTY")
F_CI_GJJS_ADR = sqlContext.read.parquet(hdfs+'/F_CI_GJJS_ADR/*')
F_CI_GJJS_ADR.registerTempTable("F_CI_GJJS_ADR")
F_CI_GJJS_PTY = sqlContext.read.parquet(hdfs+'/F_CI_GJJS_PTY/*')
F_CI_GJJS_PTY.registerTempTable("F_CI_GJJS_PTY")

#目标表：
#OCRM_F_CI_SYS_RESOURCE 增改表 多个PY 非第一个 ,拿BK目录今天日期数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/*")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet")
OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

#TMP_INTCUST_01 全量表 ，临时表

#OCRM_F_CI_PER_CUST_INFO 增改表 多个PY 非第一个 ,拿BK目录今天日期数据
#ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_PER_CUST_INFO/*")
#ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_PER_CUST_INFO_BK/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_PER_CUST_INFO/"+V_DT+".parquet")
OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO_BK/'+V_DT+'*')
OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")

#OCRM_F_CI_COM_CUST_INFO 增改表 多个PY 非第一个 ,拿BK目录今天日期数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/*")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet")
OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")

sql = """
 SELECT DISTINCT COALESCE(B.ID, CAST(MONOTONICALLY_INCREASING_ID()                       AS BIGINT))                       AS ID 
       ,COALESCE(B.ODS_CUST_ID, A.CLIENTID)                       AS ODS_CUST_ID 
       ,NAM                     AS ODS_CUST_NAME 
       ,COALESCE(B.SOURCE_CUST_ID, A.EXTKEY)                       AS SOURCE_CUST_ID 
       ,NAM                     AS SOURCE_CUST_NAME 
       ,CRIT                    AS CERT_TYPE 
       ,CRID                    AS CERT_NO 
       ,'INT'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,COALESCE(B.ODS_CUST_TYPE, CASE WHEN LENGTH(CRID) >= 15 THEN '1' ELSE '2' END)                       AS ODS_CUST_TYPE 
       ,'1'                     AS CUST_STAT 
       ,B.BEGIN_DATE            AS BEGIN_DATE 
       ,B.END_DATE              AS END_DATE 
       ,'100010000000'          AS SYS_ID_FLAG 
       ,A.FR_ID                 AS FR_ID 
       ,B.CERT_FLAG             AS CERT_FLAG 
   FROM F_CI_GJJS_PTY A                                        --客户资料表
   LEFT JOIN OCRM_F_CI_SYS_RESOURCE B                          --中间表
     ON A.FR_ID                 = B.FR_ID 
    AND A.EXTKEY                = B.SOURCE_CUST_NAME 
    AND A.CRIT                  = B.CERT_TYPE 
    AND A.CRID                  = B.CERT_NO 
    AND B.ODS_SYS_ID            = 'INT' 
  WHERE CLIENTID IS 
    NOT NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.registerTempTable("OCRM_F_CI_SYS_RESOURCE_INNTMP1")

#OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
#OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")
sql = """
 SELECT DISTINCT DST.ID                                                  --ID:src.ID
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
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.SOURCE_CUST_NAME    = DST.SOURCE_CUST_NAME 
    AND SRC.CERT_TYPE           = DST.CERT_TYPE 
    AND SRC.CERT_NO             = DST.CERT_NO 
    AND SRC.ODS_SYS_ID          = DST.ODS_SYS_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_SYS_RESOURCE_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet"
OCRM_F_CI_SYS_RESOURCE_INNTMP2=OCRM_F_CI_SYS_RESOURCE_INNTMP2.unionAll(OCRM_F_CI_SYS_RESOURCE_INNTMP1)
OCRM_F_CI_SYS_RESOURCE_INNTMP1.cache()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.cache()
nrowsi = OCRM_F_CI_SYS_RESOURCE_INNTMP1.count()
nrowsa = OCRM_F_CI_SYS_RESOURCE_INNTMP2.count()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
#OCRM_F_CI_SYS_RESOURCE_INNTMP1.unpersist()
OCRM_F_CI_SYS_RESOURCE_INNTMP2.unpersist()
#增改表，保存后更新BK目录文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_SYS_RESOURCE/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_SYS_RESOURCE_BK/"+V_DT+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
#任务[21] 001-02::
V_STEP = V_STEP + 1

OCRM_F_CI_SYS_RESOURCE = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_SYS_RESOURCE/*')
OCRM_F_CI_SYS_RESOURCE.registerTempTable("OCRM_F_CI_SYS_RESOURCE")

sql = """
 SELECT A.NAM          AS CUST_NAME 
       ,D.ODS_CUST_ID           AS ODS_CUST_ID 
       ,A.NAM1                  AS NAM1 
       ,A.BIDDAT                AS BIDDAT 
       ,B.LOCCTY                AS LOCCTY 
       ,B.LOCTXT                AS LOCTXT 
       ,C.TELOFF                AS TELOFF 
       ,C.DEP                   AS DEP 
       ,C.NAM                   AS NAM4 
       ,B.FAX1                  AS FAX1 
       ,B.NAM                   AS NAM 
       ,B.LOCZIP                AS LOCZIP 
       ,B.TEL1                  AS TEL1 
       ,C.TELMOB                AS TELMOB 
       ,B.EML                   AS EML 
       ,A.IDTYPE                AS IDTYPE 
       ,A.IDCODE                AS IDCODE 
       ,A.FR_ID                 AS FR_ID 
   FROM (SELECT A.*,ROW_NUMBER() OVER(PARTITION BY FR_ID,EXTKEY ORDER BY INR DESC) RN FROM F_CI_GJJS_PTY A WHERE EXTKEY<>'RCBJCNBJXXX') A                                                   --客户资料表
  INNER JOIN OCRM_F_CI_SYS_RESOURCE D                          --中间表
     ON A.FR_ID                 = D.FR_ID 
    AND A.NAM                   = D.SOURCE_CUST_NAME 
    AND A.CRIT                  = D.CERT_TYPE 
    AND A.CRID                  = D.CERT_NO 
    AND D.ODS_SYS_ID            = 'INT' 
   LEFT JOIN F_CI_GJJS_ADR B                                   --地址表
     ON A.EXTKEY                = B.EXTKEY 
    AND B.FR_ID                 = A.FR_ID 
   LEFT JOIN F_CM_GJJS_PTC C                                   --利息中间表
     ON C.PTYINR                = A.INR 
    AND A.FR_ID                 = B.FR_ID 
  WHERE A.RN                    = '1' """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
TMP_INTCUST_01 = sqlContext.sql(sql)
TMP_INTCUST_01.registerTempTable("TMP_INTCUST_01")
dfn="TMP_INTCUST_01/"+V_DT+".parquet"
#TMP_INTCUST_01.cache()
#nrows = TMP_INTCUST_01.count()
TMP_INTCUST_01.write.save(path=hdfs + '/' + dfn, mode='overwrite')
TMP_INTCUST_01.unpersist()
#全需要删除前一天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/TMP_INTCUST_01/"+V_DT_LD+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
#任务[12] 001-03::
V_STEP = V_STEP + 1



sql = """
 SELECT D.ID                    AS ID 
       ,D.CUST_ID               AS CUST_ID 
       ,COALESCE(D.CUST_NAME, E.CUST_NAME)                       AS CUST_NAME 
       ,COALESCE(D.CUST_ENAME, E.NAM1)                       AS CUST_ENAME 
       ,D.CUST_ENAME1           AS CUST_ENAME1 
       ,COALESCE(E.BIDDAT, CUST_BIR)                       AS CUST_BIR 
       ,COALESCE(E.LOCCTY, CUST_RESCNTY)                       AS CUST_RESCNTY 
       ,D.CUST_MRG              AS CUST_MRG 
       ,D.CUST_SEX              AS CUST_SEX 
       ,D.CUST_NATION           AS CUST_NATION 
       ,D.CUST_REGISTER         AS CUST_REGISTER 
       ,D.CUST_REGTYP           AS CUST_REGTYP 
       ,D.CUST_REGADDR          AS CUST_REGADDR 
       ,D.CUST_CITISHIP         AS CUST_CITISHIP 
       ,D.CUST_USED_NAME        AS CUST_USED_NAME 
       ,D.CUST_TTL              AS CUST_TTL 
       ,D.CUST_GROUP            AS CUST_GROUP 
       ,D.CUST_EVADATE          AS CUST_EVADATE 
       ,D.IS_STAFF              AS IS_STAFF 
       ,D.CUS_TYPE              AS CUS_TYPE 
       ,D.IS_GOODMAN            AS IS_GOODMAN 
       ,D.IS_TECH               AS IS_TECH 
       ,D.IS_GUDONG             AS IS_GUDONG 
       ,D.IS_SY                 AS IS_SY 
       ,D.IS_BUSSMA             AS IS_BUSSMA 
       ,D.CUST_FAMSTATUS        AS CUST_FAMSTATUS 
       ,D.CUST_HEALTH           AS CUST_HEALTH 
       ,D.CUST_LANGUAGE         AS CUST_LANGUAGE 
       ,D.CUST_RELIGION         AS CUST_RELIGION 
       ,D.CUST_POLIFACE         AS CUST_POLIFACE 
       ,D.CUST_RES_CNTY         AS CUST_RES_CNTY 
       ,D.CITY_COD              AS CITY_COD 
       ,D.CUST_LOCALYEAR        AS CUST_LOCALYEAR 
       ,D.ID_BLACKLIST          AS ID_BLACKLIST 
       ,D.CUST_ATT_FMDAT        AS CUST_ATT_FMDAT 
       ,D.CUST_ATT_DESC         AS CUST_ATT_DESC 
       ,D.FIN_RISK_ASS          AS FIN_RISK_ASS 
       ,D.RISK_APPETITE         AS RISK_APPETITE 
       ,D.EVA_DATE              AS EVA_DATE 
       ,D.FIN_WARN              AS FIN_WARN 
       ,D.CUST_CRT_SCT          AS CUST_CRT_SCT 
       ,D.CUST_HEIGHT           AS CUST_HEIGHT 
       ,D.CUST_WEIGHT           AS CUST_WEIGHT 
       ,D.CUST_BLOTYP           AS CUST_BLOTYP 
       ,D.TEMP_RESIDENCE        AS TEMP_RESIDENCE 
       ,D.BK_RELASHIP           AS BK_RELASHIP 
       ,D.CUST_EDU_LVL_COD      AS CUST_EDU_LVL_COD 
       ,D.GIHHEST_DEGREE        AS GIHHEST_DEGREE 
       ,D.GRADUATION            AS GRADUATION 
       ,D.GRADUATE_NAME         AS GRADUATE_NAME 
       ,D.CUST_OCCUP_COD        AS CUST_OCCUP_COD 
       ,D.CUST_OCCUP_COD1       AS CUST_OCCUP_COD1 
       ,D.OCCP_STATE            AS OCCP_STATE 
       ,D.WORK_STATE            AS WORK_STATE 
       ,D.CUST_POSN             AS CUST_POSN 
       ,D.CUST_TITL             AS CUST_TITL 
       ,D.TERM_OF_CON           AS TERM_OF_CON 
       ,D.WORK_YEAR             AS WORK_YEAR 
       ,D.CUST_WORK_YEAR        AS CUST_WORK_YEAR 
       ,D.CUST_WORK_UNIT_NAME   AS CUST_WORK_UNIT_NAME 
       ,COALESCE(E.TELOFF, CUST_UTELL)                       AS CUST_UTELL 
       ,D.CUST_UTYP             AS CUST_UTYP 
       ,COALESCE(E.DEP, DEPT)                       AS DEPT 
       ,D.IS_CONTROLLER         AS IS_CONTROLLER 
       ,D.SOCIAL_DUTY           AS SOCIAL_DUTY 
       ,D.PER_DESCRIB           AS PER_DESCRIB 
       ,D.PER_RESUME            AS PER_RESUME 
       ,D.IS_FARMER_FLG         AS IS_FARMER_FLG 
       ,D.HOUHOLD_CLASS         AS HOUHOLD_CLASS 
       ,D.CUST_TEAMNAME         AS CUST_TEAMNAME 
       ,D.VILLAGE_NAME          AS VILLAGE_NAME 
       ,D.IS_VILLAGECADRE       AS IS_VILLAGECADRE 
       ,D.IS_MEDICARE           AS IS_MEDICARE 
       ,D.IS_POORISNO           AS IS_POORISNO 
       ,D.MAKUP                 AS MAKUP 
       ,D.INDUSTRYTYPE          AS INDUSTRYTYPE 
       ,D.MOSTBUSINESS          AS MOSTBUSINESS 
       ,D.BUSINESSADD           AS BUSINESSADD 
       ,D.LICENSENO             AS LICENSENO 
       ,D.LICENSEDATE           AS LICENSEDATE 
       ,D.TAXNO                 AS TAXNO 
       ,D.TAXNO1                AS TAXNO1 
       ,D.MAINPROORINCOME       AS MAINPROORINCOME 
       ,D.ADMIN_LVL             AS ADMIN_LVL 
       ,D.WORK_PERMIT           AS WORK_PERMIT 
       ,COALESCE(E.NAM4, LINKMAN)                       AS LINKMAN 
       ,D.OFF_ADDR              AS OFF_ADDR 
       ,D.OFF_ZIP               AS OFF_ZIP 
       ,D.MICRO_BLOG            AS MICRO_BLOG 
       ,COALESCE(E.FAX1, FAX)                       AS FAX 
       ,D.MSN                   AS MSN 
       ,D.OTHER_CONTACT         AS OTHER_CONTACT 
       ,D.CUST_REG_ADDR2        AS CUST_REG_ADDR2 
       ,COALESCE(E.NAM, CI_ADDR)                       AS CI_ADDR 
       ,COALESCE(E.LOCZIP, CUST_POSTCOD)                       AS CUST_POSTCOD 
       ,COALESCE(E.TEL1, CUST_YEL_NO)                       AS CUST_YEL_NO 
       ,D.CUST_AREA_COD         AS CUST_AREA_COD 
       ,COALESCE(E.TELMOB, CUST_MBTELNO)                       AS CUST_MBTELNO 
       ,COALESCE(E.EML, CUST_EMAIL)                       AS CUST_EMAIL 
       ,D.CUST_SUB_TEL          AS CUST_SUB_TEL 
       ,D.CUST_WEBSITE          AS CUST_WEBSITE 
       ,D.CUST_WORKADDR         AS CUST_WORKADDR 
       ,COALESCE(E.NAM, CUST_COMMADD)                       AS CUST_COMMADD 
       ,COALESCE(E.LOCZIP, CUST_COMZIP)                       AS CUST_COMZIP 
       ,D.CUST_WORKZIP          AS CUST_WORKZIP 
       ,D.CUST_RELIGNLISM       AS CUST_RELIGNLISM 
       ,D.CUST_EFFSTATUS        AS CUST_EFFSTATUS 
       ,D.CUST_ARREA            AS CUST_ARREA 
       ,D.CUST_FAMADDR          AS CUST_FAMADDR 
       ,D.CUST_VILLAGENO        AS CUST_VILLAGENO 
       ,D.NET_ADDR              AS NET_ADDR 
       ,D.CUST_CRE_TYP          AS CUST_CRE_TYP 
       ,D.CUST_CER_NO           AS CUST_CER_NO 
       ,D.CUST_EXPD_DT          AS CUST_EXPD_DT 
       ,D.CUST_CHK_FLG          AS CUST_CHK_FLG 
       ,D.CUST_CER_STS          AS CUST_CER_STS 
       ,D.CUST_SONO             AS CUST_SONO 
       ,D.CUST_PECON_RESUR      AS CUST_PECON_RESUR 
       ,D.CUST_MN_INCO          AS CUST_MN_INCO 
       ,D.CUST_ANNUAL_INCOME    AS CUST_ANNUAL_INCOME 
       ,D.PER_INCURR_YCODE      AS PER_INCURR_YCODE 
       ,D.PER_IN_ANOUNT         AS PER_IN_ANOUNT 
       ,D.PER_INCURR_MCODE      AS PER_INCURR_MCODE 
       ,D.PER_INCURR_FAMCODE    AS PER_INCURR_FAMCODE 
       ,D.FAM_INCOMEACC         AS FAM_INCOMEACC 
       ,D.OTH_FAMINCOME         AS OTH_FAMINCOME 
       ,D.CUST_TOT_ASS          AS CUST_TOT_ASS 
       ,D.CUST_TOT_DEBT         AS CUST_TOT_DEBT 
       ,D.CUST_FAM_NUM          AS CUST_FAM_NUM 
       ,D.CUST_DEPEND_NO        AS CUST_DEPEND_NO 
       ,D.CUST_OT_INCO          AS CUST_OT_INCO 
       ,D.CUST_HOUSE_TYP        AS CUST_HOUSE_TYP 
       ,D.WAGES_ACCOUNT         AS WAGES_ACCOUNT 
       ,D.OPEN_BANK             AS OPEN_BANK 
       ,D.CRE_RECORD            AS CRE_RECORD 
       ,D.HAVE_YDTCARD          AS HAVE_YDTCARD 
       ,D.IS_LIFSUR             AS IS_LIFSUR 
       ,D.IS_ILLSUR             AS IS_ILLSUR 
       ,D.IS_ENDOSUR            AS IS_ENDOSUR 
       ,D.HAVE_CAR              AS HAVE_CAR 
       ,D.AVG_ASS               AS AVG_ASS 
       ,D.REMARK                AS REMARK 
       ,D.REC_UPSYS             AS REC_UPSYS 
       ,D.REC_UPMEC             AS REC_UPMEC 
       ,D.REC_UODATE            AS REC_UODATE 
       ,D.REC_UPMAN             AS REC_UPMAN 
       ,D.SOUR_SYS              AS SOUR_SYS 
       ,D.PLAT_DATE             AS PLAT_DATE 
       ,D.COMBIN_FLG            AS COMBIN_FLG 
       ,D.DATE_SOU              AS DATE_SOU 
       ,D.INPUTDATE             AS INPUTDATE 
       ,D.CUST_FANID            AS CUST_FANID 
       ,D.HZ_CERTYPE            AS HZ_CERTYPE 
       ,D.HZ_CERTID             AS HZ_CERTID 
       ,D.CUST_TEAMNO           AS CUST_TEAMNO 
       ,D.HZ_NAME               AS HZ_NAME 
       ,D.CUST_FAMNUM           AS CUST_FAMNUM 
       ,D.CUST_WEANAME          AS CUST_WEANAME 
       ,D.CUST_WEAVALUE         AS CUST_WEAVALUE 
       ,D.CONLANDAREA           AS CONLANDAREA 
       ,D.CONPOOLAREA           AS CONPOOLAREA 
       ,D.CUST_CHILDREN         AS CUST_CHILDREN 
       ,D.NUM_OF_CHILD          AS NUM_OF_CHILD 
       ,D.TOTINCO_OF_CH         AS TOTINCO_OF_CH 
       ,D.CUST_HOUSE            AS CUST_HOUSE 
       ,D.CUST_HOUSECOUNT       AS CUST_HOUSECOUNT 
       ,D.CUST_HOUSEAREA        AS CUST_HOUSEAREA 
       ,D.CUST_PRIVATECAR       AS CUST_PRIVATECAR 
       ,D.CAR_NUM_DESC          AS CAR_NUM_DESC 
       ,D.CUST_FAMILYSORT       AS CUST_FAMILYSORT 
       ,D.CUST_OTHMEG           AS CUST_OTHMEG 
       ,D.CUST_SUMJF            AS CUST_SUMJF 
       ,D.CUST_ZCJF             AS CUST_ZCJF 
       ,D.CUST_FZJF             AS CUST_FZJF 
       ,D.CUST_CONSUMEJF        AS CUST_CONSUMEJF 
       ,D.CUST_CHANNELJF        AS CUST_CHANNELJF 
       ,D.CUST_MIDBUSJF         AS CUST_MIDBUSJF 
       ,D.CRECARD_POINTS        AS CRECARD_POINTS 
       ,D.QQ                    AS QQ 
       ,D.MICRO_MSG             AS MICRO_MSG 
       ,V_DT                    AS ODS_ST_DATE 
       ,D.FAMILY_SAFE           AS FAMILY_SAFE 
       ,D.BAD_HABIT_FLAG        AS BAD_HABIT_FLAG 
       ,D.IS_MODIFY             AS IS_MODIFY 
       ,D.FR_ID                 AS FR_ID 
       ,D.LONGITUDE             AS LONGITUDE 
       ,D.LATITUDE              AS LATITUDE 
       ,D.GPS                   AS GPS 
   FROM OCRM_F_CI_PER_CUST_INFO D                              --对私客户信息
  INNER JOIN TMP_INTCUST_01 E                                  --国结更新对私客户临时表01
     ON D.FR_ID                 = E.FR_ID 
    AND D.CUST_ID               = E.ODS_CUST_ID """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_PER_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_PER_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_PER_CUST_INFO_INNTMP1")

#OCRM_F_CI_PER_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_PER_CUST_INFO/*')
#OCRM_F_CI_PER_CUST_INFO.registerTempTable("OCRM_F_CI_PER_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --ID:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --中文名称:src.CUST_NAME
       ,DST.CUST_ENAME                                         --英文/拼音名称1:src.CUST_ENAME
       ,DST.CUST_ENAME1                                        --英文/拼音名称2:src.CUST_ENAME1
       ,DST.CUST_BIR                                           --注册/出生日期:src.CUST_BIR
       ,DST.CUST_RESCNTY                                       --居住国:src.CUST_RESCNTY
       ,DST.CUST_MRG                                           --婚姻状况:src.CUST_MRG
       ,DST.CUST_SEX                                           --性别:src.CUST_SEX
       ,DST.CUST_NATION                                        --民族:src.CUST_NATION
       ,DST.CUST_REGISTER                                      --户籍:src.CUST_REGISTER
       ,DST.CUST_REGTYP                                        --户籍性质:src.CUST_REGTYP
       ,DST.CUST_REGADDR                                       --户籍地址:src.CUST_REGADDR
       ,DST.CUST_CITISHIP                                      --国籍:src.CUST_CITISHIP
       ,DST.CUST_USED_NAME                                     --曾用名:src.CUST_USED_NAME
       ,DST.CUST_TTL                                           --客户称呼:src.CUST_TTL
       ,DST.CUST_GROUP                                         --客户所属群:src.CUST_GROUP
       ,DST.CUST_EVADATE                                       --本行评估日期:src.CUST_EVADATE
       ,DST.IS_STAFF                                           --员工标志:src.IS_STAFF
       ,DST.CUS_TYPE                                           --客户类型:src.CUS_TYPE
       ,DST.IS_GOODMAN                                         --不良嗜好:src.IS_GOODMAN
       ,DST.IS_TECH                                            --有无技术特长:src.IS_TECH
       ,DST.IS_GUDONG                                          --是否本社股东:src.IS_GUDONG
       ,DST.IS_SY                                              --是否社员:src.IS_SY
       ,DST.IS_BUSSMA                                          --是否个体工商户:src.IS_BUSSMA
       ,DST.CUST_FAMSTATUS                                     --家庭状况:src.CUST_FAMSTATUS
       ,DST.CUST_HEALTH                                        --健康状况:src.CUST_HEALTH
       ,DST.CUST_LANGUAGE                                      --语言习惯:src.CUST_LANGUAGE
       ,DST.CUST_RELIGION                                      --宗教信仰:src.CUST_RELIGION
       ,DST.CUST_POLIFACE                                      --政治面貌:src.CUST_POLIFACE
       ,DST.CUST_RES_CNTY                                      --所在国:src.CUST_RES_CNTY
       ,DST.CITY_COD                                           --所在城市:src.CITY_COD
       ,DST.CUST_LOCALYEAR                                     --所在城市入住时间:src.CUST_LOCALYEAR
       ,DST.ID_BLACKLIST                                       --上本行黑名单标志:src.ID_BLACKLIST
       ,DST.CUST_ATT_FMDAT                                     --上黑名单日期:src.CUST_ATT_FMDAT
       ,DST.CUST_ATT_DESC                                      --上黑名单原因:src.CUST_ATT_DESC
       ,DST.FIN_RISK_ASS                                       --最新理财风险评估:src.FIN_RISK_ASS
       ,DST.RISK_APPETITE                                      --风险偏好:src.RISK_APPETITE
       ,DST.EVA_DATE                                           --评估时间:src.EVA_DATE
       ,DST.FIN_WARN                                           --最新理财预警标准:src.FIN_WARN
       ,DST.CUST_CRT_SCT                                       --开户日期:src.CUST_CRT_SCT
       ,DST.CUST_HEIGHT                                        --身高(厘米):src.CUST_HEIGHT
       ,DST.CUST_WEIGHT                                        --体重(公斤):src.CUST_WEIGHT
       ,DST.CUST_BLOTYP                                        --血型:src.CUST_BLOTYP
       ,DST.TEMP_RESIDENCE                                     --暂时居留标志:src.TEMP_RESIDENCE
       ,DST.BK_RELASHIP                                        --本行关系人:src.BK_RELASHIP
       ,DST.CUST_EDU_LVL_COD                                   --最高学历代码:src.CUST_EDU_LVL_COD
       ,DST.GIHHEST_DEGREE                                     --最高学位:src.GIHHEST_DEGREE
       ,DST.GRADUATION                                         --毕业年份:src.GRADUATION
       ,DST.GRADUATE_NAME                                      --毕业学校:src.GRADUATE_NAME
       ,DST.CUST_OCCUP_COD                                     --职业代码:src.CUST_OCCUP_COD
       ,DST.CUST_OCCUP_COD1                                    --职业（人行）:src.CUST_OCCUP_COD1
       ,DST.OCCP_STATE                                         --职业状态:src.OCCP_STATE
       ,DST.WORK_STATE                                         --就业状态:src.WORK_STATE
       ,DST.CUST_POSN                                          --职务:src.CUST_POSN
       ,DST.CUST_TITL                                          --职称:src.CUST_TITL
       ,DST.TERM_OF_CON                                        --劳动合同期限:src.TERM_OF_CON
       ,DST.WORK_YEAR                                          --从事现职年份:src.WORK_YEAR
       ,DST.CUST_WORK_YEAR                                     --参加工作年份:src.CUST_WORK_YEAR
       ,DST.CUST_WORK_UNIT_NAME                                --工作单位名称:src.CUST_WORK_UNIT_NAME
       ,DST.CUST_UTELL                                         --单位电话:src.CUST_UTELL
       ,DST.CUST_UTYP                                          --单位性质:src.CUST_UTYP
       ,DST.DEPT                                               --单位部门:src.DEPT
       ,DST.IS_CONTROLLER                                      --是否单位控制人:src.IS_CONTROLLER
       ,DST.SOCIAL_DUTY                                        --社会职务:src.SOCIAL_DUTY
       ,DST.PER_DESCRIB                                        --个人描述:src.PER_DESCRIB
       ,DST.PER_RESUME                                         --个人简历:src.PER_RESUME
       ,DST.IS_FARMER_FLG                                      --农户标识:src.IS_FARMER_FLG
       ,DST.HOUHOLD_CLASS                                      --农户分类:src.HOUHOLD_CLASS
       ,DST.CUST_TEAMNAME                                      --所属行政乡(镇)名:src.CUST_TEAMNAME
       ,DST.VILLAGE_NAME                                       --所属行政村名:src.VILLAGE_NAME
       ,DST.IS_VILLAGECADRE                                    --是否村组干部:src.IS_VILLAGECADRE
       ,DST.IS_MEDICARE                                        --是否参加农村新型合作医疗保险:src.IS_MEDICARE
       ,DST.IS_POORISNO                                        --是否扶贫户:src.IS_POORISNO
       ,DST.MAKUP                                              --经营模式:src.MAKUP
       ,DST.INDUSTRYTYPE                                       --经营行业:src.INDUSTRYTYPE
       ,DST.MOSTBUSINESS                                       --经营范围及方式:src.MOSTBUSINESS
       ,DST.BUSINESSADD                                        --经营场所:src.BUSINESSADD
       ,DST.LICENSENO                                          --营业执照号码:src.LICENSENO
       ,DST.LICENSEDATE                                        --营业执照登记时间:src.LICENSEDATE
       ,DST.TAXNO                                              --税务登记证号码（国税）:src.TAXNO
       ,DST.TAXNO1                                             --税务登记证号码（地税）:src.TAXNO1
       ,DST.MAINPROORINCOME                                    --主要经营项目及收入来源:src.MAINPROORINCOME
       ,DST.ADMIN_LVL                                          --行政级别:src.ADMIN_LVL
       ,DST.WORK_PERMIT                                        --工作证号:src.WORK_PERMIT
       ,DST.LINKMAN                                            --联系人:src.LINKMAN
       ,DST.OFF_ADDR                                           --办公地址:src.OFF_ADDR
       ,DST.OFF_ZIP                                            --办公地址邮编:src.OFF_ZIP
       ,DST.MICRO_BLOG                                         --微博:src.MICRO_BLOG
       ,DST.FAX                                                --传真:src.FAX
       ,DST.MSN                                                --MSN:src.MSN
       ,DST.OTHER_CONTACT                                      --其他联系方式:src.OTHER_CONTACT
       ,DST.CUST_REG_ADDR2                                     --户籍地址:src.CUST_REG_ADDR2
       ,DST.CI_ADDR                                            --居住地址:src.CI_ADDR
       ,DST.CUST_POSTCOD                                       --邮政编码:src.CUST_POSTCOD
       ,DST.CUST_YEL_NO                                        --电话号码:src.CUST_YEL_NO
       ,DST.CUST_AREA_COD                                      --区号:src.CUST_AREA_COD
       ,DST.CUST_MBTELNO                                       --手机号:src.CUST_MBTELNO
       ,DST.CUST_EMAIL                                         --电子邮箱:src.CUST_EMAIL
       ,DST.CUST_SUB_TEL                                       --分机:src.CUST_SUB_TEL
       ,DST.CUST_WEBSITE                                       --网址:src.CUST_WEBSITE
       ,DST.CUST_WORKADDR                                      --工作地址:src.CUST_WORKADDR
       ,DST.CUST_COMMADD                                       --通讯地址:src.CUST_COMMADD
       ,DST.CUST_COMZIP                                        --通讯地址邮政编码:src.CUST_COMZIP
       ,DST.CUST_WORKZIP                                       --单位地址邮政编码:src.CUST_WORKZIP
       ,DST.CUST_RELIGNLISM                                    --所属行政区域:src.CUST_RELIGNLISM
       ,DST.CUST_EFFSTATUS                                     --客户有效标识:src.CUST_EFFSTATUS
       ,DST.CUST_ARREA                                         --区域ID:src.CUST_ARREA
       ,DST.CUST_FAMADDR                                       --常住地址:src.CUST_FAMADDR
       ,DST.CUST_VILLAGENO                                     --行政村代码:src.CUST_VILLAGENO
       ,DST.NET_ADDR                                           --网络地址:src.NET_ADDR
       ,DST.CUST_CRE_TYP                                       --证件类型:src.CUST_CRE_TYP
       ,DST.CUST_CER_NO                                        --证件号码:src.CUST_CER_NO
       ,DST.CUST_EXPD_DT                                       --证件有效期:src.CUST_EXPD_DT
       ,DST.CUST_CHK_FLG                                       --身份核查标记:src.CUST_CHK_FLG
       ,DST.CUST_CER_STS                                       --证件状态:src.CUST_CER_STS
       ,DST.CUST_SONO                                          --社会保险号:src.CUST_SONO
       ,DST.CUST_PECON_RESUR                                   --主要经济来源:src.CUST_PECON_RESUR
       ,DST.CUST_MN_INCO                                       --月收入:src.CUST_MN_INCO
       ,DST.CUST_ANNUAL_INCOME                                 --年纯收入(万元):src.CUST_ANNUAL_INCOME
       ,DST.PER_INCURR_YCODE                                   --个人税前年收入币种码值代码:src.PER_INCURR_YCODE
       ,DST.PER_IN_ANOUNT                                      --个人税前年收入金额:src.PER_IN_ANOUNT
       ,DST.PER_INCURR_MCODE                                   --个人月工资收入币种码值代码:src.PER_INCURR_MCODE
       ,DST.PER_INCURR_FAMCODE                                 --家庭年收入币种码值代码:src.PER_INCURR_FAMCODE
       ,DST.FAM_INCOMEACC                                      --家庭年收入金额:src.FAM_INCOMEACC
       ,DST.OTH_FAMINCOME                                      --家庭其他年收入:src.OTH_FAMINCOME
       ,DST.CUST_TOT_ASS                                       --总资产(万元):src.CUST_TOT_ASS
       ,DST.CUST_TOT_DEBT                                      --总负债(万元):src.CUST_TOT_DEBT
       ,DST.CUST_FAM_NUM                                       --家庭总人口:src.CUST_FAM_NUM
       ,DST.CUST_DEPEND_NO                                     --供养人数:src.CUST_DEPEND_NO
       ,DST.CUST_OT_INCO                                       --其他经济来源:src.CUST_OT_INCO
       ,DST.CUST_HOUSE_TYP                                     --住宅类型:src.CUST_HOUSE_TYP
       ,DST.WAGES_ACCOUNT                                      --工资账号:src.WAGES_ACCOUNT
       ,DST.OPEN_BANK                                          --工资帐号开户银行:src.OPEN_BANK
       ,DST.CRE_RECORD                                         --信用记录:src.CRE_RECORD
       ,DST.HAVE_YDTCARD                                       --易贷通卡号:src.HAVE_YDTCARD
       ,DST.IS_LIFSUR                                          --是否参加人寿保险:src.IS_LIFSUR
       ,DST.IS_ILLSUR                                          --是否参加大病保险:src.IS_ILLSUR
       ,DST.IS_ENDOSUR                                         --是否参加养老保险:src.IS_ENDOSUR
       ,DST.HAVE_CAR                                           --是否拥有车辆:src.HAVE_CAR
       ,DST.AVG_ASS                                            --日均资产:src.AVG_ASS
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.REC_UPSYS                                          --最近更新系统:src.REC_UPSYS
       ,DST.REC_UPMEC                                          --最近更新机构:src.REC_UPMEC
       ,DST.REC_UODATE                                         --最近更新日期:src.REC_UODATE
       ,DST.REC_UPMAN                                          --最近更新人:src.REC_UPMAN
       ,DST.SOUR_SYS                                           --来源系统:src.SOUR_SYS
       ,DST.PLAT_DATE                                          --平台日期:src.PLAT_DATE
       ,DST.COMBIN_FLG                                         --合并标志:src.COMBIN_FLG
       ,DST.DATE_SOU                                           --数据来源:src.DATE_SOU
       ,DST.INPUTDATE                                          --登记时间:src.INPUTDATE
       ,DST.CUST_FANID                                         --户口簿编号:src.CUST_FANID
       ,DST.HZ_CERTYPE                                         --户主证件类型:src.HZ_CERTYPE
       ,DST.HZ_CERTID                                          --户主证件号码:src.HZ_CERTID
       ,DST.CUST_TEAMNO                                        --小组编号:src.CUST_TEAMNO
       ,DST.HZ_NAME                                            --户主姓名:src.HZ_NAME
       ,DST.CUST_FAMNUM                                        --家庭劳动力:src.CUST_FAMNUM
       ,DST.CUST_WEANAME                                       --家庭财产名称:src.CUST_WEANAME
       ,DST.CUST_WEAVALUE                                      --家庭财产估价:src.CUST_WEAVALUE
       ,DST.CONLANDAREA                                        --承包土地面积:src.CONLANDAREA
       ,DST.CONPOOLAREA                                        --承包水塘面积:src.CONPOOLAREA
       ,DST.CUST_CHILDREN                                      --是否有子女:src.CUST_CHILDREN
       ,DST.NUM_OF_CHILD                                       --子女数量:src.NUM_OF_CHILD
       ,DST.TOTINCO_OF_CH                                      --子女总月收入:src.TOTINCO_OF_CH
       ,DST.CUST_HOUSE                                         --是否有房产:src.CUST_HOUSE
       ,DST.CUST_HOUSECOUNT                                    --房产数量:src.CUST_HOUSECOUNT
       ,DST.CUST_HOUSEAREA                                     --房产总面积:src.CUST_HOUSEAREA
       ,DST.CUST_PRIVATECAR                                    --是否有私家车:src.CUST_PRIVATECAR
       ,DST.CAR_NUM_DESC                                       --私家车数量及描述:src.CAR_NUM_DESC
       ,DST.CUST_FAMILYSORT                                    --家庭分类:src.CUST_FAMILYSORT
       ,DST.CUST_OTHMEG                                        --其他信息:src.CUST_OTHMEG
       ,DST.CUST_SUMJF                                         --总积分:src.CUST_SUMJF
       ,DST.CUST_ZCJF                                          --资产积分:src.CUST_ZCJF
       ,DST.CUST_FZJF                                          --负债积分:src.CUST_FZJF
       ,DST.CUST_CONSUMEJF                                     --消费积分:src.CUST_CONSUMEJF
       ,DST.CUST_CHANNELJF                                     --渠道积分:src.CUST_CHANNELJF
       ,DST.CUST_MIDBUSJF                                      --中间业务积分:src.CUST_MIDBUSJF
       ,DST.CRECARD_POINTS                                     --信用卡消费积分:src.CRECARD_POINTS
       ,DST.QQ                                                 --QQ号码:src.QQ
       ,DST.MICRO_MSG                                          --微信:src.MICRO_MSG
       ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
       ,DST.FAMILY_SAFE                                        --:src.FAMILY_SAFE
       ,DST.BAD_HABIT_FLAG                                     --:src.BAD_HABIT_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_PER_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_PER_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_PER_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_PER_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_PER_CUST_INFO_INNTMP2=OCRM_F_CI_PER_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_PER_CUST_INFO_INNTMP1)
OCRM_F_CI_PER_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_PER_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_PER_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_PER_CUST_INFO_INNTMP2.count()
OCRM_F_CI_PER_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_PER_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_PER_CUST_INFO_INNTMP2.unpersist()
#增改表，保存后更新BK目录文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_PER_CUST_INFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_PER_CUST_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_PER_CUST_INFO_BK/"+V_DT+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

#任务[12] 001-04::
V_STEP = V_STEP + 1



sql = """
 SELECT DISTINCT A.ID                    AS ID 
       ,A.CUST_ID               AS CUST_ID 
       ,COALESCE(A.CUST_NAME, B.NAM1)                       AS CUST_NAME 
       ,A.CUST_ZH_NAME          AS CUST_ZH_NAME 
       ,COALESCE(A.CUST_EN_NAME, B.NAM)                       AS CUST_EN_NAME 
       ,A.CUST_EN_NAME2         AS CUST_EN_NAME2 
       ,A.CERT_TYP              AS CERT_TYP 
       ,A.CERT_NO               AS CERT_NO 
       ,A.COM_SCALE             AS COM_SCALE 
       ,A.COM_START_DATE        AS COM_START_DATE 
       ,A.COM_BELONG            AS COM_BELONG 
       ,A.HOLDING_TYP           AS HOLDING_TYP 
       ,COALESCE(B.CTVC, A.INDUS_CALSS_MAIN)                       AS INDUS_CALSS_MAIN 
       ,A.INDUS_CLAS_DEPUTY     AS INDUS_CLAS_DEPUTY 
       ,A.BUS_TYP               AS BUS_TYP 
       ,A.ORG_TYP               AS ORG_TYP 
       ,A.ECO_TYP               AS ECO_TYP 
       ,A.COM_TYP               AS COM_TYP 
       ,A.COM_LEVEL             AS COM_LEVEL 
       ,A.OTHER_NAME            AS OTHER_NAME 
       ,A.OBJECT_RATE           AS OBJECT_RATE 
       ,A.SUBJECT_RATE          AS SUBJECT_RATE 
       ,A.EFF_DATE              AS EFF_DATE 
       ,A.RATE_DATE             AS RATE_DATE 
       ,A.CREDIT_LEVEL          AS CREDIT_LEVEL 
       ,A.LISTING_CORP_TYP      AS LISTING_CORP_TYP 
       ,A.IF_AGRICULTRUE        AS IF_AGRICULTRUE 
       ,A.IF_BANK_SIGNING       AS IF_BANK_SIGNING 
       ,A.IF_SHAREHOLDER        AS IF_SHAREHOLDER 
       ,A.IF_SHARE_CUST         AS IF_SHARE_CUST 
       ,A.IF_CREDIT_CUST        AS IF_CREDIT_CUST 
       ,A.IF_BASIC              AS IF_BASIC 
       ,A.IF_ESTATE             AS IF_ESTATE 
       ,A.IF_HIGH_TECH          AS IF_HIGH_TECH 
       ,A.IF_SMALL              AS IF_SMALL 
       ,A.IF_IBK                AS IF_IBK 
       ,A.PLICY_TYP             AS PLICY_TYP 
       ,A.IF_EXPESS             AS IF_EXPESS 
       ,A.IF_MONITER            AS IF_MONITER 
       ,A.IF_FANACING           AS IF_FANACING 
       ,CASE EXTKEY WHEN NULL THEN '2' ELSE '1' END                     AS IF_INT 
       ,A.IF_GROUP              AS IF_GROUP 
       ,A.RIGHT_FLAG            AS RIGHT_FLAG 
       ,A.RATE_RESULT_OUTER     AS RATE_RESULT_OUTER 
       ,A.RATE_DATE_OUTER       AS RATE_DATE_OUTER 
       ,A.RATE_ORG_NAME         AS RATE_ORG_NAME 
       ,A.ENT_QUA_LEVEL         AS ENT_QUA_LEVEL 
       ,A.SYN_FLAG              AS SYN_FLAG 
       ,A.FOR_BAL_LIMIT         AS FOR_BAL_LIMIT 
       ,A.LINCENSE_NO           AS LINCENSE_NO 
       ,A.ADJUST_TYP            AS ADJUST_TYP 
       ,A.UPGRADE_FLAG          AS UPGRADE_FLAG 
       ,A.EMERGING_TYP          AS EMERGING_TYP 
       ,A.ESTATE_QUALIFICATION  AS ESTATE_QUALIFICATION 
       ,A.AREA_ID               AS AREA_ID 
       ,A.UNION_FLAG            AS UNION_FLAG 
       ,A.BLACKLIST_FLAG        AS BLACKLIST_FLAG 
       ,A.AUTH_ORG              AS AUTH_ORG 
       ,A.OPEN_ORG1             AS OPEN_ORG1 
       ,A.FIRST_OPEN_TYP        AS FIRST_OPEN_TYP 
       ,A.OTHER_BANK_ORG        AS OTHER_BANK_ORG 
       ,A.IF_EFFICT_LOANCARD    AS IF_EFFICT_LOANCARD 
       ,A.LOAN_CARDNO           AS LOAN_CARDNO 
       ,A.LOAN_CARD_DATE        AS LOAN_CARD_DATE 
       ,A.FIRST_OPEN_DATE       AS FIRST_OPEN_DATE 
       ,A.FIRST_LOAN_DATE       AS FIRST_LOAN_DATE 
       ,A.LOAN_RATE             AS LOAN_RATE 
       ,A.DEP_RATE              AS DEP_RATE 
       ,A.DEP_RATIO             AS DEP_RATIO 
       ,A.SETTLE_RATIO          AS SETTLE_RATIO 
       ,A.BASIC_ACCT            AS BASIC_ACCT 
       ,COALESCE(B.CRNM, A.LEGAL_NAME)                       AS LEGAL_NAME 
       ,COALESCE(B.CRID, A.LEGAL_CERT_NO)                       AS LEGAL_CERT_NO 
       ,A.LINK_MOBILE           AS LINK_MOBILE 
       ,A.FAX_NO                AS FAX_NO 
       ,A.LINK_TEL_FIN          AS LINK_TEL_FIN 
       ,A.REGISTER_ADDRESS      AS REGISTER_ADDRESS 
       ,A.REGISTER_ZIP          AS REGISTER_ZIP 
       ,A.COUNTRY               AS COUNTRY 
       ,A.PROVINCE              AS PROVINCE 
       ,A.WORK_ADDRESS          AS WORK_ADDRESS 
       ,A.E_MAIL                AS E_MAIL 
       ,A.WEB_ADDRESS           AS WEB_ADDRESS 
       ,A.CONTROLLER_NAME       AS CONTROLLER_NAME 
       ,A.CONTROLLER_CERT_TYP   AS CONTROLLER_CERT_TYP 
       ,A.CONTROLLER_CERT_NO    AS CONTROLLER_CERT_NO 
       ,A.LINK_TEL              AS LINK_TEL 
       ,A.OPEN_ORG2             AS OPEN_ORG2 
       ,A.OPEN_DATE             AS OPEN_DATE 
       ,A.REG_CCY               AS REG_CCY 
       ,COALESCE(B.RGCP, A.REG_CAPITAL)                       AS REG_CAPITAL 
       ,A.BUSINESS              AS BUSINESS 
       ,A.EMPLOYEE_NUM          AS EMPLOYEE_NUM 
       ,A.TOTAL_ASSET           AS TOTAL_ASSET 
       ,A.SALE_ASSET            AS SALE_ASSET 
       ,A.TAX_NO                AS TAX_NO 
       ,A.RENT_NO               AS RENT_NO 
       ,A.LAST_DATE             AS LAST_DATE 
       ,A.BOND_FLAG             AS BOND_FLAG 
       ,A.BUS_AREA              AS BUS_AREA 
       ,A.BUS_OWNER             AS BUS_OWNER 
       ,A.BUS_STAT              AS BUS_STAT 
       ,A.INCOME_CCY            AS INCOME_CCY 
       ,A.INCOME_SETTLE         AS INCOME_SETTLE 
       ,A.TAXPAYER_SCALE        AS TAXPAYER_SCALE 
       ,A.MERGE_SYS_ID          AS MERGE_SYS_ID 
       ,A.BELONG_SYS_ID         AS BELONG_SYS_ID 
       ,A.MERGE_ORG             AS MERGE_ORG 
       ,A.MERGE_DATE            AS MERGE_DATE 
       ,A.MERGE_OFFICER         AS MERGE_OFFICER 
       ,A.REMARK1               AS REMARK1 
       ,A.BIRTH_DATE            AS BIRTH_DATE 
       ,A.KEY_CERT_NO           AS KEY_CERT_NO 
       ,COALESCE(B.CRIT, A.KEY_CERT_TYP)                       AS KEY_CERT_TYP 
       ,COALESCE(B.CRID, A.KEY_CUST_ID)                       AS KEY_CUST_ID 
       ,COALESCE(B.CRNM, A.KEY_PEOPLE_NAME)                       AS KEY_PEOPLE_NAME 
       ,A.EDU_LEVEL             AS EDU_LEVEL 
       ,A.WORK_YEAR             AS WORK_YEAR 
       ,A.HOUSE_ADDRESS         AS HOUSE_ADDRESS 
       ,A.HOUSE_ZIP             AS HOUSE_ZIP 
       ,A.DUTY_TIME             AS DUTY_TIME 
       ,A.SHARE_HOLDING         AS SHARE_HOLDING 
       ,A.DUTY                  AS DUTY 
       ,A.REMARK2               AS REMARK2 
       ,A.SEX                   AS SEX 
       ,A.SOCIAL_INSURE_NO      AS SOCIAL_INSURE_NO 
       ,A.ODS_ST_DATE           AS ODS_ST_DATE 
       ,A.GREEN_FLAG            AS GREEN_FLAG 
       ,A.IS_MODIFY             AS IS_MODIFY 
       ,A.FR_ID                 AS FR_ID 
       ,A.LONGITUDE             AS LONGITUDE 
       ,A.LATITUDE              AS LATITUDE 
       ,A.GPS                   AS GPS 
   FROM OCRM_F_CI_COM_CUST_INFO A                              --对公客户信息表
  INNER JOIN OCRM_F_CI_SYS_RESOURCE C                          --
     ON A.FR_ID                 = C.FR_ID 
    AND A.CUST_ID               = C.ODS_CUST_ID 
    AND C.ODS_SYS_ID            = 'INT' 
  INNER JOIN (SELECT A.*,ROW_NUMBER() OVER(PARTITION BY FR_ID,EXTKEY ORDER BY INR DESC) RN FROM F_CI_GJJS_PTY A WHERE EXTKEY<>'RCBJCNBJXXX') B                                              --
     ON A.FR_ID                 = B.FR_ID
     AND B.EXTKEY=C.SOURCE_CUST_ID 
    AND B.RN                    = '1'  """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1 = sqlContext.sql(sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.registerTempTable("OCRM_F_CI_COM_CUST_INFO_INNTMP1")

#OCRM_F_CI_COM_CUST_INFO = sqlContext.read.parquet(hdfs+'/OCRM_F_CI_COM_CUST_INFO/*')
#OCRM_F_CI_COM_CUST_INFO.registerTempTable("OCRM_F_CI_COM_CUST_INFO")
sql = """
 SELECT DST.ID                                                  --主键:src.ID
       ,DST.CUST_ID                                            --客户编号:src.CUST_ID
       ,DST.CUST_NAME                                          --客户名称:src.CUST_NAME
       ,DST.CUST_ZH_NAME                                       --单位中文简称:src.CUST_ZH_NAME
       ,DST.CUST_EN_NAME                                       --客户英文名:src.CUST_EN_NAME
       ,DST.CUST_EN_NAME2                                      --英文/拼音名称2:src.CUST_EN_NAME2
       ,DST.CERT_TYP                                           --证件类型:src.CERT_TYP
       ,DST.CERT_NO                                            --证件号码:src.CERT_NO
       ,DST.COM_SCALE                                          --企业规模:src.COM_SCALE
       ,DST.COM_START_DATE                                     --企业成立日期:src.COM_START_DATE
       ,DST.COM_BELONG                                         --企业隶属关系:src.COM_BELONG
       ,DST.HOLDING_TYP                                        --客户控股类型:src.HOLDING_TYP
       ,DST.INDUS_CALSS_MAIN                                   --行业分类（主营):src.INDUS_CALSS_MAIN
       ,DST.INDUS_CLAS_DEPUTY                                  --行业分类（副营):src.INDUS_CLAS_DEPUTY
       ,DST.BUS_TYP                                            --客户业务类型:src.BUS_TYP
       ,DST.ORG_TYP                                            --客户性质:src.ORG_TYP
       ,DST.ECO_TYP                                            --经济性质:src.ECO_TYP
       ,DST.COM_TYP                                            --企业类型:src.COM_TYP
       ,DST.COM_LEVEL                                          --农业产业化企业级别:src.COM_LEVEL
       ,DST.OTHER_NAME                                         --其他名称:src.OTHER_NAME
       ,DST.OBJECT_RATE                                        --客观评级:src.OBJECT_RATE
       ,DST.SUBJECT_RATE                                       --主观评级:src.SUBJECT_RATE
       ,DST.EFF_DATE                                           --客户等级即期评级有效期:src.EFF_DATE
       ,DST.RATE_DATE                                          --即期评级时间:src.RATE_DATE
       ,DST.CREDIT_LEVEL                                       --即期信用等级:src.CREDIT_LEVEL
       ,DST.LISTING_CORP_TYP                                   --上市公司类型:src.LISTING_CORP_TYP
       ,DST.IF_AGRICULTRUE                                     --是否涉农企业:src.IF_AGRICULTRUE
       ,DST.IF_BANK_SIGNING                                    --是否银企签约:src.IF_BANK_SIGNING
       ,DST.IF_SHAREHOLDER                                     --是否本行/社股东:src.IF_SHAREHOLDER
       ,DST.IF_SHARE_CUST                                      --是否我行关联方客户:src.IF_SHARE_CUST
       ,DST.IF_CREDIT_CUST                                     --是否我行授信客户:src.IF_CREDIT_CUST
       ,DST.IF_BASIC                                           --是否在我行开立基本户:src.IF_BASIC
       ,DST.IF_ESTATE                                          --是否从事房地产开发:src.IF_ESTATE
       ,DST.IF_HIGH_TECH                                       --是否高新技术企业:src.IF_HIGH_TECH
       ,DST.IF_SMALL                                           --是否小企业:src.IF_SMALL
       ,DST.IF_IBK                                             --是否网银签约客户:src.IF_IBK
       ,DST.PLICY_TYP                                          --产业政策分类:src.PLICY_TYP
       ,DST.IF_EXPESS                                          --是否为过剩行业:src.IF_EXPESS
       ,DST.IF_MONITER                                         --是否重点监控行业:src.IF_MONITER
       ,DST.IF_FANACING                                        --是否属于政府融资平台:src.IF_FANACING
       ,DST.IF_INT                                             --是否国结客户:src.IF_INT
       ,DST.IF_GROUP                                           --是否集团客户:src.IF_GROUP
       ,DST.RIGHT_FLAG                                         --有无进出口经营权:src.RIGHT_FLAG
       ,DST.RATE_RESULT_OUTER                                  --外部机构评级结果:src.RATE_RESULT_OUTER
       ,DST.RATE_DATE_OUTER                                    --外部机构评级日期:src.RATE_DATE_OUTER
       ,DST.RATE_ORG_NAME                                      --外部评级机构名称:src.RATE_ORG_NAME
       ,DST.ENT_QUA_LEVEL                                      --企业资质等级:src.ENT_QUA_LEVEL
       ,DST.SYN_FLAG                                           --银团标识:src.SYN_FLAG
       ,DST.FOR_BAL_LIMIT                                      --外币余额限制:src.FOR_BAL_LIMIT
       ,DST.LINCENSE_NO                                        --开户许可证号:src.LINCENSE_NO
       ,DST.ADJUST_TYP                                         --产业结构调整类型:src.ADJUST_TYP
       ,DST.UPGRADE_FLAG                                       --工业转型升级标识:src.UPGRADE_FLAG
       ,DST.EMERGING_TYP                                       --战略新兴产业类型:src.EMERGING_TYP
       ,DST.ESTATE_QUALIFICATION                               --房地产开发资质:src.ESTATE_QUALIFICATION
       ,DST.AREA_ID                                            --区域ID:src.AREA_ID
       ,DST.UNION_FLAG                                         --合并标志:src.UNION_FLAG
       ,DST.BLACKLIST_FLAG                                     --黑名单标识:src.BLACKLIST_FLAG
       ,DST.AUTH_ORG                                           --上级主管部门名称:src.AUTH_ORG
       ,DST.OPEN_ORG1                                          --我行开户行:src.OPEN_ORG1
       ,DST.FIRST_OPEN_TYP                                     --首次开户账户类型:src.FIRST_OPEN_TYP
       ,DST.OTHER_BANK_ORG                                     --他行开户行:src.OTHER_BANK_ORG
       ,DST.IF_EFFICT_LOANCARD                                 --贷款卡是否有效:src.IF_EFFICT_LOANCARD
       ,DST.LOAN_CARDNO                                        --贷款卡号:src.LOAN_CARDNO
       ,DST.LOAN_CARD_DATE                                     --贷款卡最新年审年份:src.LOAN_CARD_DATE
       ,DST.FIRST_OPEN_DATE                                    --在本行/社首次开立账户时间:src.FIRST_OPEN_DATE
       ,DST.FIRST_LOAN_DATE                                    --与本行/社建立信贷关系时间:src.FIRST_LOAN_DATE
       ,DST.LOAN_RATE                                          --贷款加权平均利率(%):src.LOAN_RATE
       ,DST.DEP_RATE                                           --存款加权平均利率(%):src.DEP_RATE
       ,DST.DEP_RATIO                                          --授信客户存贷比:src.DEP_RATIO
       ,DST.SETTLE_RATIO                                       --授信客户结算比:src.SETTLE_RATIO
       ,DST.BASIC_ACCT                                         --基本账户号:src.BASIC_ACCT
       ,DST.LEGAL_NAME                                         --法人代表姓名:src.LEGAL_NAME
       ,DST.LEGAL_CERT_NO                                      --法定代表人身份证号码:src.LEGAL_CERT_NO
       ,DST.LINK_MOBILE                                        --联系电话(短信通知号码):src.LINK_MOBILE
       ,DST.FAX_NO                                             --传真电话:src.FAX_NO
       ,DST.LINK_TEL_FIN                                       --财务部联系电话:src.LINK_TEL_FIN
       ,DST.REGISTER_ADDRESS                                   --注册地址:src.REGISTER_ADDRESS
       ,DST.REGISTER_ZIP                                       --注册地址邮政编码:src.REGISTER_ZIP
       ,DST.COUNTRY                                            --所在国家(地区):src.COUNTRY
       ,DST.PROVINCE                                           --省份、直辖市、自治区:src.PROVINCE
       ,DST.WORK_ADDRESS                                       --办公地址:src.WORK_ADDRESS
       ,DST.E_MAIL                                             --公司E－Mail:src.E_MAIL
       ,DST.WEB_ADDRESS                                        --公司网址:src.WEB_ADDRESS
       ,DST.CONTROLLER_NAME                                    --实际控制人姓名:src.CONTROLLER_NAME
       ,DST.CONTROLLER_CERT_TYP                                --实际控制人证件类型:src.CONTROLLER_CERT_TYP
       ,DST.CONTROLLER_CERT_NO                                 --实际控制人证件号码:src.CONTROLLER_CERT_NO
       ,DST.LINK_TEL                                           --联系电话:src.LINK_TEL
       ,DST.OPEN_ORG2                                          --开户机构:src.OPEN_ORG2
       ,DST.OPEN_DATE                                          --开户日期:src.OPEN_DATE
       ,DST.REG_CCY                                            --注册资本币种:src.REG_CCY
       ,DST.REG_CAPITAL                                        --注册资本:src.REG_CAPITAL
       ,DST.BUSINESS                                           --经营范围:src.BUSINESS
       ,DST.EMPLOYEE_NUM                                       --员工人数:src.EMPLOYEE_NUM
       ,DST.TOTAL_ASSET                                        --资产总额:src.TOTAL_ASSET
       ,DST.SALE_ASSET                                         --销售额:src.SALE_ASSET
       ,DST.TAX_NO                                             --税务登记证号(国税):src.TAX_NO
       ,DST.RENT_NO                                            --税务登记证号(地税):src.RENT_NO
       ,DST.LAST_DATE                                          --分期筹资的最后一期的时间:src.LAST_DATE
       ,DST.BOND_FLAG                                          --有无董事会:src.BOND_FLAG
       ,DST.BUS_AREA                                           --经营场地面积:src.BUS_AREA
       ,DST.BUS_OWNER                                          --经营场地所有权:src.BUS_OWNER
       ,DST.BUS_STAT                                           --经营状况:src.BUS_STAT
       ,DST.INCOME_CCY                                         --实收资本币种:src.INCOME_CCY
       ,DST.INCOME_SETTLE                                      --实收资本:src.INCOME_SETTLE
       ,DST.TAXPAYER_SCALE                                     --纳税人规模:src.TAXPAYER_SCALE
       ,DST.MERGE_SYS_ID                                       --最近更新系统:src.MERGE_SYS_ID
       ,DST.BELONG_SYS_ID                                      --所属系统:src.BELONG_SYS_ID
       ,DST.MERGE_ORG                                          --最近更新机构:src.MERGE_ORG
       ,DST.MERGE_DATE                                         --最近更新日期:src.MERGE_DATE
       ,DST.MERGE_OFFICER                                      --最近更新人:src.MERGE_OFFICER
       ,DST.REMARK1                                            --备注:src.REMARK1
       ,DST.BIRTH_DATE                                         --出生日期:src.BIRTH_DATE
       ,DST.KEY_CERT_NO                                        --证件号码:src.KEY_CERT_NO
       ,DST.KEY_CERT_TYP                                       --关键人证件:src.KEY_CERT_TYP
       ,DST.KEY_CUST_ID                                        --关键人客户编号:src.KEY_CUST_ID
       ,DST.KEY_PEOPLE_NAME                                    --关键人名称:src.KEY_PEOPLE_NAME
       ,DST.EDU_LEVEL                                          --学历:src.EDU_LEVEL
       ,DST.WORK_YEAR                                          --相关行业从业年限:src.WORK_YEAR
       ,DST.HOUSE_ADDRESS                                      --家庭住址:src.HOUSE_ADDRESS
       ,DST.HOUSE_ZIP                                          --住址邮编:src.HOUSE_ZIP
       ,DST.DUTY_TIME                                          --担任该职务时间:src.DUTY_TIME
       ,DST.SHARE_HOLDING                                      --持股情况:src.SHARE_HOLDING
       ,DST.DUTY                                               --担任职务:src.DUTY
       ,DST.REMARK2                                            --备注:src.REMARK2
       ,DST.SEX                                                --性别:src.SEX
       ,DST.SOCIAL_INSURE_NO                                   --社会保险号码:src.SOCIAL_INSURE_NO
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.GREEN_FLAG                                         --:src.GREEN_FLAG
       ,DST.IS_MODIFY                                          --是否修改:src.IS_MODIFY
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.LONGITUDE                                          --经度:src.LONGITUDE
       ,DST.LATITUDE                                           --纬度:src.LATITUDE
       ,DST.GPS                                                --GPS:src.GPS
   FROM OCRM_F_CI_COM_CUST_INFO DST 
   LEFT JOIN OCRM_F_CI_COM_CUST_INFO_INNTMP1 SRC 
     ON SRC.FR_ID               = DST.FR_ID 
    AND SRC.CUST_ID             = DST.CUST_ID 
  WHERE SRC.FR_ID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
OCRM_F_CI_COM_CUST_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet"
OCRM_F_CI_COM_CUST_INFO_INNTMP2=OCRM_F_CI_COM_CUST_INFO_INNTMP2.unionAll(OCRM_F_CI_COM_CUST_INFO_INNTMP1)
OCRM_F_CI_COM_CUST_INFO_INNTMP1.cache()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.cache()
nrowsi = OCRM_F_CI_COM_CUST_INFO_INNTMP1.count()
nrowsa = OCRM_F_CI_COM_CUST_INFO_INNTMP2.count()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
OCRM_F_CI_COM_CUST_INFO_INNTMP1.unpersist()
OCRM_F_CI_COM_CUST_INFO_INNTMP2.unpersist()
#增改表，保存后更新BK目录文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/OCRM_F_CI_COM_CUST_INFO/"+V_DT+".parquet /"+dbname+"/OCRM_F_CI_COM_CUST_INFO_BK/"+V_DT+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)
