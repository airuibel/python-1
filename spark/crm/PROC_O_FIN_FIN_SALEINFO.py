#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_FIN_FIN_SALEINFO').setMaster(sys.argv[2])
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
#----------来源表---------------
O_TX_FIN_SALEINFO = sqlContext.read.parquet(hdfs+'/O_TX_FIN_SALEINFO/*')
O_TX_FIN_SALEINFO.registerTempTable("O_TX_FIN_SALEINFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#----------目标表---------------
F_TX_FIN_SALEINFO = sqlContext.read.parquet(hdfs+'/F_TX_FIN_SALEINFO_BK/'+V_DT_LD+'.parquet/*')
F_TX_FIN_SALEINFO.registerTempTable("F_TX_FIN_SALEINFO")

sql = """
 SELECT A.CUSTTP                AS CUSTTP 
       ,A.SALECHL               AS SALECHL 
       ,A.SALETP                AS SALETP 
       ,A.CURRENCY              AS CURRENCY 
       ,A.PRODCODE              AS PRODCODE 
       ,A.PRODNAME              AS PRODNAME 
       ,A.PRODZONENO            AS PRODZONENO 
       ,A.PRODSDATE             AS PRODSDATE 
       ,A.PRODEDATE             AS PRODEDATE 
       ,A.ESTCUSTLYR            AS ESTCUSTLYR 
       ,A.ESTCUSTHYR            AS ESTCUSTHYR 
       ,A.BACKFLAG              AS BACKFLAG 
       ,A.REMVFLAG              AS REMVFLAG 
       ,A.PLEDFLAG              AS PLEDFLAG 
       ,A.CUSTSTATE             AS CUSTSTATE 
       ,A.PRINTFLAG             AS PRINTFLAG 
       ,A.ACCTNO                AS ACCTNO 
       ,A.ACCTNAME              AS ACCTNAME 
       ,A.AGENTSERIALNO         AS AGENTSERIALNO 
       ,A.REMVSERIALNO          AS REMVSERIALNO 
       ,A.REMVDATE              AS REMVDATE 
       ,A.BACKSERIALNO          AS BACKSERIALNO 
       ,A.BACKDATE              AS BACKDATE 
       ,A.PLEDSERIALNO          AS PLEDSERIALNO 
       ,A.PLEDDATE              AS PLEDDATE 
       ,A.UNPLEDSERIALNO        AS UNPLEDSERIALNO 
       ,A.UNPLEDDATE            AS UNPLEDDATE 
       ,A.UPTRDATE              AS UPTRDATE 
       ,A.IDTYPE                AS IDTYPE 
       ,A.IDNO                  AS IDNO 
       ,A.APPLPER               AS APPLPER 
       ,A.APPLAMT               AS APPLAMT 
       ,A.CUSTMANAGE            AS CUSTMANAGE 
       ,A.PRODRISKLEV           AS PRODRISKLEV 
       ,A.CUSTRISKLEV           AS CUSTRISKLEV 
       ,A.ORIPRODCODE           AS ORIPRODCODE 
       ,A.OAGENTSERIALNO        AS OAGENTSERIALNO 
       ,A.LOADSERIALNO          AS LOADSERIALNO 
       ,A.LOADAMOUNT            AS LOADAMOUNT 
       ,A.LOADDATE              AS LOADDATE 
       ,A.LOADSEQNO             AS LOADSEQNO 
       ,A.ALPAYAMT              AS ALPAYAMT 
       ,A.LEFTAMT               AS LEFTAMT 
       ,A.ALBENEAMT             AS ALBENEAMT 
       ,A.FREEZEAMT             AS FREEZEAMT 
       ,A.BENEAMT               AS BENEAMT 
       ,A.PUNAMT                AS PUNAMT 
       ,A.NEWPRODCODE           AS NEWPRODCODE 
       ,A.NEWAGENTSERIALNO      AS NEWAGENTSERIALNO 
       ,A.SALEZONENO            AS SALEZONENO 
       ,A.SALESUBBKNO           AS SALESUBBKNO 
       ,A.SALEBRNO              AS SALEBRNO 
       ,A.SALETELLERNO          AS SALETELLERNO 
       ,A.SALEDATE              AS SALEDATE 
       ,A.SALETIME              AS SALETIME 
       ,A.AMTCHKCTL             AS AMTCHKCTL 
       ,A.NOTE1                 AS NOTE1 
       ,A.NOTE2                 AS NOTE2 
       ,A.NOTE3                 AS NOTE3 
       ,A.NOTE4                 AS NOTE4 
       ,A.NOTE5                 AS NOTE5 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'FIN'                   AS ODS_SYS_ID 
   FROM O_TX_FIN_SALEINFO A                                    --认购明细表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_FIN_SALEINFO_INNTMP1 = sqlContext.sql(sql)
F_TX_FIN_SALEINFO_INNTMP1.registerTempTable("F_TX_FIN_SALEINFO_INNTMP1")

#F_TX_FIN_SALEINFO = sqlContext.read.parquet(hdfs+'/F_TX_FIN_SALEINFO/*')
#F_TX_FIN_SALEINFO.registerTempTable("F_TX_FIN_SALEINFO")
sql = """
 SELECT DST.CUSTTP                                              --账户类型:src.CUSTTP
       ,DST.SALECHL                                            --认购渠道:src.SALECHL
       ,DST.SALETP                                             --认购类型:src.SALETP
       ,DST.CURRENCY                                           --币种:src.CURRENCY
       ,DST.PRODCODE                                           --产品代码:src.PRODCODE
       ,DST.PRODNAME                                           --产品名称:src.PRODNAME
       ,DST.PRODZONENO                                         --产品归属法人:src.PRODZONENO
       ,DST.PRODSDATE                                          --起息日:src.PRODSDATE
       ,DST.PRODEDATE                                          --到期日:src.PRODEDATE
       ,DST.ESTCUSTLYR                                         --预期最低收益率:src.ESTCUSTLYR
       ,DST.ESTCUSTHYR                                         --预期最高收益率:src.ESTCUSTHYR
       ,DST.BACKFLAG                                           --赎回标志:src.BACKFLAG
       ,DST.REMVFLAG                                           --撤单标志:src.REMVFLAG
       ,DST.PLEDFLAG                                           --质押标志:src.PLEDFLAG
       ,DST.CUSTSTATE                                          --客户交易状态:src.CUSTSTATE
       ,DST.PRINTFLAG                                          --补打标志:src.PRINTFLAG
       ,DST.ACCTNO                                             --理财账户:src.ACCTNO
       ,DST.ACCTNAME                                           --户名:src.ACCTNAME
       ,DST.AGENTSERIALNO                                      --认购前置流水:src.AGENTSERIALNO
       ,DST.REMVSERIALNO                                       --撤单前置流水:src.REMVSERIALNO
       ,DST.REMVDATE                                           --撤单前置日期:src.REMVDATE
       ,DST.BACKSERIALNO                                       --赎回前置流水:src.BACKSERIALNO
       ,DST.BACKDATE                                           --赎回前置日期:src.BACKDATE
       ,DST.PLEDSERIALNO                                       --质押前置流水:src.PLEDSERIALNO
       ,DST.PLEDDATE                                           --质押前置日期:src.PLEDDATE
       ,DST.UNPLEDSERIALNO                                     --解质押前置流水:src.UNPLEDSERIALNO
       ,DST.UNPLEDDATE                                         --解质押前置日期:src.UNPLEDDATE
       ,DST.UPTRDATE                                           --上划前置日期:src.UPTRDATE
       ,DST.IDTYPE                                             --证件类型:src.IDTYPE
       ,DST.IDNO                                               --证件号码:src.IDNO
       ,DST.APPLPER                                            --申请份数:src.APPLPER
       ,DST.APPLAMT                                            --申请金额:src.APPLAMT
       ,DST.CUSTMANAGE                                         --销售客户经理:src.CUSTMANAGE
       ,DST.PRODRISKLEV                                        --产品风险等级:src.PRODRISKLEV
       ,DST.CUSTRISKLEV                                        --客户风险等级:src.CUSTRISKLEV
       ,DST.ORIPRODCODE                                        --原产品代码:src.ORIPRODCODE
       ,DST.OAGENTSERIALNO                                     --原认购前置流水号:src.OAGENTSERIALNO
       ,DST.LOADSERIALNO                                       --止付前置流水号:src.LOADSERIALNO
       ,DST.LOADAMOUNT                                         --止付金额:src.LOADAMOUNT
       ,DST.LOADDATE                                           --止付日期:src.LOADDATE
       ,DST.LOADSEQNO                                          --止付序号:src.LOADSEQNO
       ,DST.ALPAYAMT                                           --已付本金总额:src.ALPAYAMT
       ,DST.LEFTAMT                                            --剩余本金:src.LEFTAMT
       ,DST.ALBENEAMT                                          --已付利息总额:src.ALBENEAMT
       ,DST.FREEZEAMT                                          --申请赎回金额:src.FREEZEAMT
       ,DST.BENEAMT                                            --赎回利息:src.BENEAMT
       ,DST.PUNAMT                                             --赎回罚金:src.PUNAMT
       ,DST.NEWPRODCODE                                        --预约产品代码:src.NEWPRODCODE
       ,DST.NEWAGENTSERIALNO                                   --预约认购前置流水号:src.NEWAGENTSERIALNO
       ,DST.SALEZONENO                                         --认购所属法人:src.SALEZONENO
       ,DST.SALESUBBKNO                                        --认购所属分行:src.SALESUBBKNO
       ,DST.SALEBRNO                                           --认购机构:src.SALEBRNO
       ,DST.SALETELLERNO                                       --认购柜员:src.SALETELLERNO
       ,DST.SALEDATE                                           --认购日期:src.SALEDATE
       ,DST.SALETIME                                           --认购时间:src.SALETIME
       ,DST.AMTCHKCTL                                          --额度检查标志:src.AMTCHKCTL
       ,DST.NOTE1                                              --备用1:src.NOTE1
       ,DST.NOTE2                                              --备用2:src.NOTE2
       ,DST.NOTE3                                              --备用3:src.NOTE3
       ,DST.NOTE4                                              --备用4:src.NOTE4
       ,DST.NOTE5                                              --备用5:src.NOTE5
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_TX_FIN_SALEINFO DST 
   LEFT JOIN F_TX_FIN_SALEINFO_INNTMP1 SRC 
     ON SRC.PRODCODE            = DST.PRODCODE 
    AND SRC.AGENTSERIALNO       = DST.AGENTSERIALNO 
    AND SRC.SALEDATE            = DST.SALEDATE 
  WHERE SRC.PRODCODE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_FIN_SALEINFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_TX_FIN_SALEINFO/"+V_DT+".parquet"
UNION=F_TX_FIN_SALEINFO_INNTMP2.unionAll(F_TX_FIN_SALEINFO_INNTMP1)
F_TX_FIN_SALEINFO_INNTMP1.cache()
F_TX_FIN_SALEINFO_INNTMP2.cache()
nrowsi = F_TX_FIN_SALEINFO_INNTMP1.count()
nrowsa = F_TX_FIN_SALEINFO_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_TX_FIN_SALEINFO_INNTMP1.unpersist()
F_TX_FIN_SALEINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_FIN_SALEINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_TX_FIN_SALEINFO/"+V_DT_LD+".parquet /"+dbname+"/F_TX_FIN_SALEINFO_BK/")

#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_FIN_SALEINFO/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_FIN_SALEINFO_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_TX_FIN_SALEINFO/"+V_DT+".parquet /"+dbname+"/F_TX_FIN_SALEINFO_BK/")