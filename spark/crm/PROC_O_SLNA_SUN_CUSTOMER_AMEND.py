#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_SLNA_SUN_CUSTOMER_AMEND').setMaster(sys.argv[2])
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

O_CI_SUN_CUSTOMER_AMEND = sqlContext.read.parquet(hdfs+'/O_CI_SUN_CUSTOMER_AMEND/*')
O_CI_SUN_CUSTOMER_AMEND.registerTempTable("O_CI_SUN_CUSTOMER_AMEND")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_SUN_CUSTOMER_AMEND/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_SUN_CUSTOMER_AMEND_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_SUN_CUSTOMER_AMEND/"+V_DT+".parquet")


F_CI_SUN_CUSTOMER_AMEND = sqlContext.read.parquet(hdfs+'/F_CI_SUN_CUSTOMER_AMEND/*')
F_CI_SUN_CUSTOMER_AMEND.registerTempTable("F_CI_SUN_CUSTOMER_AMEND")

sql = """
 SELECT A.CUSTOMERID            AS CUSTOMERID 
       ,A.FULLNAME              AS FULLNAME 
       ,A.CUSTOMERTYPE          AS CUSTOMERTYPE 
       ,A.CERTTYPE              AS CERTTYPE 
       ,A.CERTID                AS CERTID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.UPDATEUSERID          AS UPDATEUSERID 
       ,A.UPDATEORGID           AS UPDATEORGID 
       ,A.FARMILYID             AS FARMILYID 
       ,A.FAMILYROLE            AS FAMILYROLE 
       ,A.ISHZ                  AS ISHZ 
       ,A.WORKINGCAPITAL        AS WORKINGCAPITAL 
       ,A.CAPITALASSETS         AS CAPITALASSETS 
       ,A.FAMILYAVERAGEINCOME   AS FAMILYAVERAGEINCOME 
       ,A.FAMILYALLINCOME       AS FAMILYALLINCOME 
       ,A.FAMILYALLOUT          AS FAMILYALLOUT 
       ,A.FAMILYPUREINCOME      AS FAMILYPUREINCOME 
       ,A.TOTALASSETS           AS TOTALASSETS 
       ,A.TOTALINDEBTEDNESS     AS TOTALINDEBTEDNESS 
       ,A.FAMILYPUREASSET       AS FAMILYPUREASSET 
       ,A.LANDSIZE              AS LANDSIZE 
       ,A.LANDNO                AS LANDNO 
       ,A.YEAROUTCOME           AS YEAROUTCOME 
       ,A.BUSINESSADDRESS       AS BUSINESSADDRESS 
       ,A.ALLGUARANTYADDRESS    AS ALLGUARANTYADDRESS 
       ,A.ALLGUARANTYTEL        AS ALLGUARANTYTEL 
       ,A.CREDITDATE            AS CREDITDATE 
       ,A.INFRINGEMENTTIMES     AS INFRINGEMENTTIMES 
       ,A.AVERAGEDEPOSIT        AS AVERAGEDEPOSIT 
       ,A.PROJECTNO             AS PROJECTNO 
       ,A.MAINPROSCOPE          AS MAINPROSCOPE 
       ,A.MANAGEUSERID          AS MANAGEUSERID 
       ,A.MANAGEORGID           AS MANAGEORGID 
       ,A.ORDERDEPOSIT          AS ORDERDEPOSIT 
       ,A.MHOUSESTRUCTURE       AS MHOUSESTRUCTURE 
       ,A.MHOUSENO              AS MHOUSENO 
       ,A.ACTUALEVALUATE        AS ACTUALEVALUATE 
       ,A.OHOUSESTRUCTURE       AS OHOUSESTRUCTURE 
       ,A.OHOUSENO              AS OHOUSENO 
       ,A.OACTUALEVALUATE       AS OACTUALEVALUATE 
       ,A.MACHINENAME           AS MACHINENAME 
       ,A.MACHINEVALUE          AS MACHINEVALUE 
       ,A.OTHERASSET            AS OTHERASSET 
       ,A.HOUSEAREANAME         AS HOUSEAREANAME 
       ,A.HOUSEID               AS HOUSEID 
       ,A.HOUSEAREANO           AS HOUSEAREANO 
       ,A.YEARINCOME            AS YEARINCOME 
       ,A.CORPORATEORGID        AS CORPORATEORGID 
       ,A.PHASETYPE             AS PHASETYPE 
       ,A.PHASEUSERID           AS PHASEUSERID 
       ,A.PHASETIME             AS PHASETIME 
       ,A.PHASEACTION           AS PHASEACTION 
       ,A.SERIALNO              AS SERIALNO 
       ,A.FAMILYMONTHINCOME     AS FAMILYMONTHINCOME 
       ,A.MAINPROORINCOME       AS MAINPROORINCOME 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'SLNA'                  AS ODS_SYS_ID 
   FROM O_CI_SUN_CUSTOMER_AMEND A                              --阳光信贷农户信息修改表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_SUN_CUSTOMER_AMEND_INNTMP1 = sqlContext.sql(sql)
F_CI_SUN_CUSTOMER_AMEND_INNTMP1.registerTempTable("F_CI_SUN_CUSTOMER_AMEND_INNTMP1")

#F_CI_SUN_CUSTOMER_AMEND = sqlContext.read.parquet(hdfs+'/F_CI_SUN_CUSTOMER_AMEND/*')
#F_CI_SUN_CUSTOMER_AMEND.registerTempTable("F_CI_SUN_CUSTOMER_AMEND")
sql = """
 SELECT DST.CUSTOMERID                                          --客户编号:src.CUSTOMERID
       ,DST.FULLNAME                                           --姓名:src.FULLNAME
       ,DST.CUSTOMERTYPE                                       --客户类型:src.CUSTOMERTYPE
       ,DST.CERTTYPE                                           --证件类型:src.CERTTYPE
       ,DST.CERTID                                             --证件号码:src.CERTID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTDATE                                          --登记日期:src.INPUTDATE
       ,DST.UPDATEUSERID                                       --更新人员:src.UPDATEUSERID
       ,DST.UPDATEORGID                                        --更新机构:src.UPDATEORGID
       ,DST.FARMILYID                                          --户籍编号(户号代码):src.FARMILYID
       ,DST.FAMILYROLE                                         --家庭角色:src.FAMILYROLE
       ,DST.ISHZ                                               --是否为户主:src.ISHZ
       ,DST.WORKINGCAPITAL                                     --流动资金:src.WORKINGCAPITAL
       ,DST.CAPITALASSETS                                      --固定资产:src.CAPITALASSETS
       ,DST.FAMILYAVERAGEINCOME                                --家庭人均收入:src.FAMILYAVERAGEINCOME
       ,DST.FAMILYALLINCOME                                    --家庭年收入:src.FAMILYALLINCOME
       ,DST.FAMILYALLOUT                                       --家庭总支出:src.FAMILYALLOUT
       ,DST.FAMILYPUREINCOME                                   --家庭净收入:src.FAMILYPUREINCOME
       ,DST.TOTALASSETS                                        --家庭总资产:src.TOTALASSETS
       ,DST.TOTALINDEBTEDNESS                                  --家庭总负债:src.TOTALINDEBTEDNESS
       ,DST.FAMILYPUREASSET                                    --家庭净资产:src.FAMILYPUREASSET
       ,DST.LANDSIZE                                           --承包土地面积:src.LANDSIZE
       ,DST.LANDNO                                             --地址码:src.LANDNO
       ,DST.YEAROUTCOME                                        --年支出:src.YEAROUTCOME
       ,DST.BUSINESSADDRESS                                    --经营地址:src.BUSINESSADDRESS
       ,DST.ALLGUARANTYADDRESS                                 --所有抵质押、担保住址:src.ALLGUARANTYADDRESS
       ,DST.ALLGUARANTYTEL                                     --所有抵质押、担保联系号码:src.ALLGUARANTYTEL
       ,DST.CREDITDATE                                         --与我行首次建立信贷关系时间:src.CREDITDATE
       ,DST.INFRINGEMENTTIMES                                  --违约次数:src.INFRINGEMENTTIMES
       ,DST.AVERAGEDEPOSIT                                     --一年日均存款金额:src.AVERAGEDEPOSIT
       ,DST.PROJECTNO                                          --经营项目编号:src.PROJECTNO
       ,DST.MAINPROSCOPE                                       --经营规模:src.MAINPROSCOPE
       ,DST.MANAGEUSERID                                       --管户人:src.MANAGEUSERID
       ,DST.MANAGEORGID                                        --管户机构:src.MANAGEORGID
       ,DST.ORDERDEPOSIT                                       --预约存款:src.ORDERDEPOSIT
       ,DST.MHOUSESTRUCTURE                                    --主屋结构:src.MHOUSESTRUCTURE
       ,DST.MHOUSENO                                           --主屋间数:src.MHOUSENO
       ,DST.ACTUALEVALUATE                                     --实际估价:src.ACTUALEVALUATE
       ,DST.OHOUSESTRUCTURE                                    --边屋结构:src.OHOUSESTRUCTURE
       ,DST.OHOUSENO                                           --边屋间数:src.OHOUSENO
       ,DST.OACTUALEVALUATE                                    --边屋估价:src.OACTUALEVALUATE
       ,DST.MACHINENAME                                        --机械名称:src.MACHINENAME
       ,DST.MACHINEVALUE                                       --机械价值:src.MACHINEVALUE
       ,DST.OTHERASSET                                         --其他资产:src.OTHERASSET
       ,DST.HOUSEAREANAME                                      --房屋小区名:src.HOUSEAREANAME
       ,DST.HOUSEID                                            --房号:src.HOUSEID
       ,DST.HOUSEAREANO                                        --面积:src.HOUSEAREANO
       ,DST.YEARINCOME                                         --家庭年收入:src.YEARINCOME
       ,DST.CORPORATEORGID                                     --法人机构号:src.CORPORATEORGID
       ,DST.PHASETYPE                                          --阶段类型:src.PHASETYPE
       ,DST.PHASEUSERID                                        --审批人:src.PHASEUSERID
       ,DST.PHASETIME                                          --审批时间:src.PHASETIME
       ,DST.PHASEACTION                                        --审批动作:src.PHASEACTION
       ,DST.SERIALNO                                           --申请流水号:src.SERIALNO
       ,DST.FAMILYMONTHINCOME                                  --家庭人均收入:src.FAMILYMONTHINCOME
       ,DST.MAINPROORINCOME                                    --主要经营项目及收入来源:src.MAINPROORINCOME
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_CI_SUN_CUSTOMER_AMEND DST 
   LEFT JOIN F_CI_SUN_CUSTOMER_AMEND_INNTMP1 SRC 
     ON SRC.SERIALNO            = DST.SERIALNO 
  WHERE SRC.SERIALNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_SUN_CUSTOMER_AMEND_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_SUN_CUSTOMER_AMEND/"+V_DT+".parquet"
F_CI_SUN_CUSTOMER_AMEND_INNTMP2=F_CI_SUN_CUSTOMER_AMEND_INNTMP2.unionAll(F_CI_SUN_CUSTOMER_AMEND_INNTMP1)
F_CI_SUN_CUSTOMER_AMEND_INNTMP1.cache()
F_CI_SUN_CUSTOMER_AMEND_INNTMP2.cache()
nrowsi = F_CI_SUN_CUSTOMER_AMEND_INNTMP1.count()
nrowsa = F_CI_SUN_CUSTOMER_AMEND_INNTMP2.count()
F_CI_SUN_CUSTOMER_AMEND_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_SUN_CUSTOMER_AMEND_INNTMP1.unpersist()
F_CI_SUN_CUSTOMER_AMEND_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_SUN_CUSTOMER_AMEND lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_SUN_CUSTOMER_AMEND/"+V_DT_LD+".parquet /"+dbname+"/F_CI_SUN_CUSTOMER_AMEND_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_SUN_CUSTOMER_AMEND_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_SUN_CUSTOMER_AMEND/"+V_DT+".parquet /"+dbname+"/F_CI_SUN_CUSTOMER_AMEND_BK/"+V_DT+".parquet")
