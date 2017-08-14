#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_AFA_BATTXNINFO').setMaster(sys.argv[2])
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

O_TX_AFA_BATTXNINFO = sqlContext.read.parquet(hdfs+'/O_TX_AFA_BATTXNINFO/*')
O_TX_AFA_BATTXNINFO.registerTempTable("O_TX_AFA_BATTXNINFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_TX_AFA_BATTXNINFO = sqlContext.read.parquet(hdfs+'/F_TX_AFA_BATTXNINFO/*')
F_TX_AFA_BATTXNINFO.registerTempTable("F_TX_AFA_BATTXNINFO")

sql = """
 SELECT WORKDATE                AS WORKDATE 
       ,BATNO                   AS BATNO 
       ,SEQNO                   AS SEQNO 
       ,AGENTSERIALNO           AS AGENTSERIALNO 
       ,SENDBANK                AS SENDBANK 
       ,PAYERACC                AS PAYERACC 
       ,PAYERACCTYP             AS PAYERACCTYP 
       ,CHECKDSFLAG             AS CHECKDSFLAG 
       ,PAYERNAME               AS PAYERNAME 
       ,CURRENCY                AS CURRENCY 
       ,AMOUNT                  AS AMOUNT 
       ,SMT                     AS SMT 
       ,RECVBANK                AS RECVBANK 
       ,PAYEEACC                AS PAYEEACC 
       ,PAYEEACCTYP             AS PAYEEACCTYP 
       ,PAYEENAME               AS PAYEENAME 
       ,VOUCHTYPE               AS VOUCHTYPE 
       ,VOUCHDATE               AS VOUCHDATE 
       ,VOUCHNO                 AS VOUCHNO 
       ,TRADESTATUS             AS TRADESTATUS 
       ,BANKSYSSEQ              AS BANKSYSSEQ 
       ,ERRORCODE               AS ERRORCODE 
       ,ERRORMSG                AS ERRORMSG 
       ,CUSID                   AS CUSID 
       ,DCFLAG                  AS DCFLAG 
       ,GSFLAG                  AS GSFLAG 
       ,INSURSYSTYP             AS INSURSYSTYP 
       ,POLICY                  AS POLICY 
       ,INSURNODE               AS INSURNODE 
       ,THIRDDATE               AS THIRDDATE 
       ,BANKAMOUNT              AS BANKAMOUNT 
       ,THIRDFILE               AS THIRDFILE 
       ,FR_ID                   AS FR_ID 
       ,'AFA'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
   FROM O_TX_AFA_BATTXNINFO A                                  --代收付数据明细登记薄
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_AFA_BATTXNINFO_INNTMP1 = sqlContext.sql(sql)
F_TX_AFA_BATTXNINFO_INNTMP1.registerTempTable("F_TX_AFA_BATTXNINFO_INNTMP1")

#F_TX_AFA_BATTXNINFO = sqlContext.read.parquet(hdfs+'/F_TX_AFA_BATTXNINFO/*')
#F_TX_AFA_BATTXNINFO.registerTempTable("F_TX_AFA_BATTXNINFO")
sql = """
 SELECT DST.WORKDATE                                            --平台日期:src.WORKDATE
       ,DST.BATNO                                              --批次号:src.BATNO
       ,DST.SEQNO                                              --序号:src.SEQNO
       ,DST.AGENTSERIALNO                                      --平台业务流水号:src.AGENTSERIALNO
       ,DST.SENDBANK                                           --付款行行号:src.SENDBANK
       ,DST.PAYERACC                                           --付款人账号:src.PAYERACC
       ,DST.PAYERACCTYP                                        --付款账号卡折标志:src.PAYERACCTYP
       ,DST.CHECKDSFLAG                                        --验证代收客户签约标志:src.CHECKDSFLAG
       ,DST.PAYERNAME                                          --付款人名称:src.PAYERNAME
       ,DST.CURRENCY                                           --币种:src.CURRENCY
       ,DST.AMOUNT                                             --交易金额:src.AMOUNT
       ,DST.SMT                                                --摘要附言:src.SMT
       ,DST.RECVBANK                                           --收款行行号:src.RECVBANK
       ,DST.PAYEEACC                                           --收款账号:src.PAYEEACC
       ,DST.PAYEEACCTYP                                        --收款账号卡折标志:src.PAYEEACCTYP
       ,DST.PAYEENAME                                          --收款户名:src.PAYEENAME
       ,DST.VOUCHTYPE                                          --凭证种类:src.VOUCHTYPE
       ,DST.VOUCHDATE                                          --凭证日期:src.VOUCHDATE
       ,DST.VOUCHNO                                            --凭证号码:src.VOUCHNO
       ,DST.TRADESTATUS                                        --主机处理标志:src.TRADESTATUS
       ,DST.BANKSYSSEQ                                         --主机流水号:src.BANKSYSSEQ
       ,DST.ERRORCODE                                          --响应码:src.ERRORCODE
       ,DST.ERRORMSG                                           --响应信息:src.ERRORMSG
       ,DST.CUSID                                              --第三方用户名:src.CUSID
       ,DST.DCFLAG                                             --银保通 扣付标记:src.DCFLAG
       ,DST.GSFLAG                                             --银保通 集团股份标志:src.GSFLAG
       ,DST.INSURSYSTYP                                        --银保通 保险公司内部系统:src.INSURSYSTYP
       ,DST.POLICY                                             --银保通 保险单号:src.POLICY
       ,DST.INSURNODE                                          --银保通 保险公司营业网点:src.INSURNODE
       ,DST.THIRDDATE                                          --银保通 生成日期:src.THIRDDATE
       ,DST.BANKAMOUNT                                         --交易金额:src.BANKAMOUNT
       ,DST.THIRDFILE                                          --第三方源文件:src.THIRDFILE
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_SYS_ID                                         --源系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --平台日期:src.ODS_ST_DATE
   FROM F_TX_AFA_BATTXNINFO DST 
   LEFT JOIN F_TX_AFA_BATTXNINFO_INNTMP1 SRC 
     ON SRC.WORKDATE            = DST.WORKDATE 
    AND SRC.BATNO               = DST.BATNO 
    AND SRC.SEQNO               = DST.SEQNO 
  WHERE SRC.WORKDATE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_AFA_BATTXNINFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_TX_AFA_BATTXNINFO/"+V_DT+".parquet"
F_TX_AFA_BATTXNINFO_INNTMP2=F_TX_AFA_BATTXNINFO_INNTMP2.unionAll(F_TX_AFA_BATTXNINFO_INNTMP1)
F_TX_AFA_BATTXNINFO_INNTMP1.cache()
F_TX_AFA_BATTXNINFO_INNTMP2.cache()
nrowsi = F_TX_AFA_BATTXNINFO_INNTMP1.count()
nrowsa = F_TX_AFA_BATTXNINFO_INNTMP2.count()
F_TX_AFA_BATTXNINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_TX_AFA_BATTXNINFO_INNTMP1.unpersist()
F_TX_AFA_BATTXNINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_AFA_BATTXNINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_TX_AFA_BATTXNINFO/"+V_DT_LD+".parquet /"+dbname+"/F_TX_AFA_BATTXNINFO_BK/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_AFA_BATTXNINFO/"+V_DT_LD+".parquet")
