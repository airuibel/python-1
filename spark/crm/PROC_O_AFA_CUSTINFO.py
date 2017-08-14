#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_AFA_CUSTINFO').setMaster(sys.argv[2])
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

O_CI_AFA_CUSTINFO = sqlContext.read.parquet(hdfs+'/O_CI_AFA_CUSTINFO/*')
O_CI_AFA_CUSTINFO.registerTempTable("O_CI_AFA_CUSTINFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_CI_AFA_CUSTINFO = sqlContext.read.parquet(hdfs+'/F_CI_AFA_CUSTINFO/*')
F_CI_AFA_CUSTINFO.registerTempTable("F_CI_AFA_CUSTINFO")

sql = """
 SELECT SYSID                   AS SYSID 
       ,SUBSYSID                AS SUBSYSID 
       ,PACTNO                  AS PACTNO 
       ,BUSINO                  AS BUSINO 
       ,SUBBUSINO               AS SUBBUSINO 
       ,CHANNELNO               AS CHANNELNO 
       ,SIGNEDTYPE              AS SIGNEDTYPE 
       ,CUSTNO                  AS CUSTNO 
       ,CUSTTYPE                AS CUSTTYPE 
       ,CUSTNAME                AS CUSTNAME 
       ,ACCOUNT                 AS ACCOUNT 
       ,AMOUNT                  AS AMOUNT 
       ,ACCSTATUS               AS ACCSTATUS 
       ,ZONENO                  AS ZONENO 
       ,ACCNAME                 AS ACCNAME 
       ,IDNO                    AS IDNO 
       ,IDTYP                   AS IDTYP 
       ,ADDRESS                 AS ADDRESS 
       ,TELPHNO                 AS TELPHNO 
       ,PHONENO                 AS PHONENO 
       ,DEPUTYNAME              AS DEPUTYNAME 
       ,DEPUTYIDNO              AS DEPUTYIDNO 
       ,DEPUTYIDTYP             AS DEPUTYIDTYP 
       ,DEPUTYADDRESS           AS DEPUTYADDRESS 
       ,DEPUTYTELPHNO           AS DEPUTYTELPHNO 
       ,DEPUTYPHONENO           AS DEPUTYPHONENO 
       ,STARTDATE               AS STARTDATE 
       ,ENDDATE                 AS ENDDATE 
       ,REGBRNO                 AS REGBRNO 
       ,REGTELLERNO             AS REGTELLERNO 
       ,REGDATE                 AS REGDATE 
       ,REGTIME                 AS REGTIME 
       ,SIGNSTATE               AS SIGNSTATE 
       ,DATAORI                 AS DATAORI 
       ,NOTE1                   AS NOTE1 
       ,NOTE2                   AS NOTE2 
       ,NOTE3                   AS NOTE3 
       ,NOTE4                   AS NOTE4 
       ,NOTE5                   AS NOTE5 
       ,FR_ID                   AS FR_ID 
       ,'AFA'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
   FROM O_CI_AFA_CUSTINFO A                                    --代理单位客户签约信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_AFA_CUSTINFO_INNTMP1 = sqlContext.sql(sql)
F_CI_AFA_CUSTINFO_INNTMP1.registerTempTable("F_CI_AFA_CUSTINFO_INNTMP1")

#F_CI_AFA_CUSTINFO = sqlContext.read.parquet(hdfs+'/F_CI_AFA_CUSTINFO/*')
#F_CI_AFA_CUSTINFO.registerTempTable("F_CI_AFA_CUSTINFO")
sql = """
 SELECT DST.SYSID                                               --系统标识:src.SYSID
       ,DST.SUBSYSID                                           --子系统标识:src.SUBSYSID
       ,DST.PACTNO                                             --协议号:src.PACTNO
       ,DST.BUSINO                                             --业务大类:src.BUSINO
       ,DST.SUBBUSINO                                          --业务子类:src.SUBBUSINO
       ,DST.CHANNELNO                                          --签约渠道:src.CHANNELNO
       ,DST.SIGNEDTYPE                                         --签约类型:src.SIGNEDTYPE
       ,DST.CUSTNO                                             --客户号:src.CUSTNO
       ,DST.CUSTTYPE                                           --客户类型:src.CUSTTYPE
       ,DST.CUSTNAME                                           --客户全称:src.CUSTNAME
       ,DST.ACCOUNT                                            --帐号:src.ACCOUNT
       ,DST.AMOUNT                                             --帐户金额:src.AMOUNT
       ,DST.ACCSTATUS                                          --帐户状态:src.ACCSTATUS
       ,DST.ZONENO                                             --法人号:src.ZONENO
       ,DST.ACCNAME                                            --账户户名:src.ACCNAME
       ,DST.IDNO                                               --证件号码:src.IDNO
       ,DST.IDTYP                                              --证件类型:src.IDTYP
       ,DST.ADDRESS                                            --通讯地址:src.ADDRESS
       ,DST.TELPHNO                                            --固定电话:src.TELPHNO
       ,DST.PHONENO                                            --手机号码:src.PHONENO
       ,DST.DEPUTYNAME                                         --代理人名称:src.DEPUTYNAME
       ,DST.DEPUTYIDNO                                         --代理人证件类型:src.DEPUTYIDNO
       ,DST.DEPUTYIDTYP                                        --代理人证件号码:src.DEPUTYIDTYP
       ,DST.DEPUTYADDRESS                                      --代理人通讯地址:src.DEPUTYADDRESS
       ,DST.DEPUTYTELPHNO                                      --代理人联系电话（固定电话）:src.DEPUTYTELPHNO
       ,DST.DEPUTYPHONENO                                      --代理人手机号码:src.DEPUTYPHONENO
       ,DST.STARTDATE                                          --委托起日:src.STARTDATE
       ,DST.ENDDATE                                            --委托止日:src.ENDDATE
       ,DST.REGBRNO                                            --登记机构:src.REGBRNO
       ,DST.REGTELLERNO                                        --登记柜员:src.REGTELLERNO
       ,DST.REGDATE                                            --登记日期:src.REGDATE
       ,DST.REGTIME                                            --登记时间:src.REGTIME
       ,DST.SIGNSTATE                                          --有效标志 0:已签约 1:已注销:src.SIGNSTATE
       ,DST.DATAORI                                            --数据来源 0:单笔 1:批量:src.DATAORI
       ,DST.NOTE1                                              --备用1:src.NOTE1
       ,DST.NOTE2                                              --备用2:src.NOTE2
       ,DST.NOTE3                                              --备用3:src.NOTE3
       ,DST.NOTE4                                              --备用4:src.NOTE4
       ,DST.NOTE5                                              --备用5:src.NOTE5
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_SYS_ID                                         --系统来源标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --跑批日期:src.ODS_ST_DATE
   FROM F_CI_AFA_CUSTINFO DST 
   LEFT JOIN F_CI_AFA_CUSTINFO_INNTMP1 SRC 
     ON SRC.SYSID               = DST.SYSID 
    AND SRC.SUBSYSID            = DST.SUBSYSID 
    AND SRC.PACTNO              = DST.PACTNO 
  WHERE SRC.SYSID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_AFA_CUSTINFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_AFA_CUSTINFO/"+V_DT+".parquet"
F_CI_AFA_CUSTINFO_INNTMP2=F_CI_AFA_CUSTINFO_INNTMP2.unionAll(F_CI_AFA_CUSTINFO_INNTMP1)
F_CI_AFA_CUSTINFO_INNTMP1.cache()
F_CI_AFA_CUSTINFO_INNTMP2.cache()
nrowsi = F_CI_AFA_CUSTINFO_INNTMP1.count()
nrowsa = F_CI_AFA_CUSTINFO_INNTMP2.count()
F_CI_AFA_CUSTINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_AFA_CUSTINFO_INNTMP1.unpersist()
F_CI_AFA_CUSTINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_AFA_CUSTINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_AFA_CUSTINFO/"+V_DT_LD+".parquet /"+dbname+"/F_CI_AFA_CUSTINFO_BK/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_AFA_CUSTINFO/"+V_DT_LD+".parquet")
