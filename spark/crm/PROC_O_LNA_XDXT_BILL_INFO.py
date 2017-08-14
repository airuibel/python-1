#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_LNA_XDXT_BILL_INFO').setMaster(sys.argv[2])
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

O_LN_XDXT_BILL_INFO = sqlContext.read.parquet(hdfs+'/O_LN_XDXT_BILL_INFO/*')
O_LN_XDXT_BILL_INFO.registerTempTable("O_LN_XDXT_BILL_INFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_BILL_INFO/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_BILL_INFO_BK/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_BILL_INFO/"+V_DT+".parquet")


F_LN_XDXT_BILL_INFO = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BILL_INFO/*')
F_LN_XDXT_BILL_INFO.registerTempTable("F_LN_XDXT_BILL_INFO")

sql = """
 SELECT A.OBJECTTYPE            AS OBJECTTYPE 
       ,A.OBJECTNO              AS OBJECTNO 
       ,A.SERIALNO              AS SERIALNO 
       ,A.BILLNO                AS BILLNO 
       ,A.BILLTYPE              AS BILLTYPE 
       ,A.BILLATTRIBUTE         AS BILLATTRIBUTE 
       ,A.BILLSTATUS            AS BILLSTATUS 
       ,A.PURPOSE               AS PURPOSE 
       ,A.WRITERID              AS WRITERID 
       ,A.WRITER                AS WRITER 
       ,A.HOLDERID              AS HOLDERID 
       ,A.HOLDER                AS HOLDER 
       ,A.BENEFICIARYID         AS BENEFICIARYID 
       ,A.BENEFICIARY           AS BENEFICIARY 
       ,A.ENDORSETIMES          AS ENDORSETIMES 
       ,A.LASTBENEFICIARY       AS LASTBENEFICIARY 
       ,A.ACCEPTORID            AS ACCEPTORID 
       ,A.ACCEPTOR              AS ACCEPTOR 
       ,A.ACCEPTORLEVEL         AS ACCEPTORLEVEL 
       ,A.ACCEPTORREGION        AS ACCEPTORREGION 
       ,A.TERM                  AS TERM 
       ,A.WRITEDATE             AS WRITEDATE 
       ,A.MATURITY              AS MATURITY 
       ,A.LCCURRENCY            AS LCCURRENCY 
       ,A.BILLCOUNT             AS BILLCOUNT 
       ,A.BILLSUM               AS BILLSUM 
       ,A.FINISHDATE            AS FINISHDATE 
       ,A.INPUTORGID            AS INPUTORGID 
       ,A.INPUTUSERID           AS INPUTUSERID 
       ,A.INPUTDATE             AS INPUTDATE 
       ,A.REMARK                AS REMARK 
       ,A.ACTUALSUM             AS ACTUALSUM 
       ,A.ACTUALINT             AS ACTUALINT 
       ,A.RATE                  AS RATE 
       ,A.INTTYPE               AS INTTYPE 
       ,A.SCALE                 AS SCALE 
       ,A.GATHERINGNAME         AS GATHERINGNAME 
       ,A.ACCOUNTNO             AS ACCOUNTNO 
       ,A.ABOUTBANKNAME         AS ABOUTBANKNAME 
       ,A.INTERSERIALNO         AS INTERSERIALNO 
       ,A.ABOUTBANKID           AS ABOUTBANKID 
       ,A.RELATIVEPUTOUTNO      AS RELATIVEPUTOUTNO 
       ,A.SENDFLAG              AS SENDFLAG 
       ,A.PERPUTOUTNO           AS PERPUTOUTNO 
       ,A.CHECKREPLYDATE        AS CHECKREPLYDATE 
       ,A.KEEPBILLORGID         AS KEEPBILLORGID 
       ,A.ADJDAY                AS ADJDAY 
       ,A.APPLYBANKACCOUNT      AS APPLYBANKACCOUNT 
       ,A.ACCEPTORACCOUNT       AS ACCEPTORACCOUNT 
       ,A.ACCEPTORBANK          AS ACCEPTORBANK 
       ,A.PAYACCOUNT            AS PAYACCOUNT 
       ,A.REMITTERBANKNAME      AS REMITTERBANKNAME 
       ,A.REMITTERACCOUNT       AS REMITTERACCOUNT 
       ,A.REMITTERBANK          AS REMITTERBANK 
       ,A.DISCOUNTFLAG          AS DISCOUNTFLAG 
       ,A.DISCOUNTACCOUNT       AS DISCOUNTACCOUNT 
       ,A.BUYRATE               AS BUYRATE 
       ,A.DUEBILLNO             AS DUEBILLNO 
       ,A.MIGRATEFLAG           AS MIGRATEFLAG 
       ,A.TERMDAY               AS TERMDAY 
       ,A.APLYFLAG              AS APLYFLAG 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'LNA'                   AS ODS_SYS_ID 
       ,A.SUBBILLTYPE           AS SUBBILLTYPE 
       ,A.RELATIVEAGREEMENT     AS RELATIVEAGREEMENT 
   FROM O_LN_XDXT_BILL_INFO A                                  --票据信息
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_BILL_INFO_INNTMP1 = sqlContext.sql(sql)
F_LN_XDXT_BILL_INFO_INNTMP1.registerTempTable("F_LN_XDXT_BILL_INFO_INNTMP1")

#F_LN_XDXT_BILL_INFO = sqlContext.read.parquet(hdfs+'/F_LN_XDXT_BILL_INFO/*')
#F_LN_XDXT_BILL_INFO.registerTempTable("F_LN_XDXT_BILL_INFO")
sql = """
 SELECT DST.OBJECTTYPE                                          --对象类型:src.OBJECTTYPE
       ,DST.OBJECTNO                                           --对象编号:src.OBJECTNO
       ,DST.SERIALNO                                           --票据信息:src.SERIALNO
       ,DST.BILLNO                                             --票据编号:src.BILLNO
       ,DST.BILLTYPE                                           --票据类型:src.BILLTYPE
       ,DST.BILLATTRIBUTE                                      --票据属性:src.BILLATTRIBUTE
       ,DST.BILLSTATUS                                         --票据状态:src.BILLSTATUS
       ,DST.PURPOSE                                            --用途:src.PURPOSE
       ,DST.WRITERID                                           --出票人代码:src.WRITERID
       ,DST.WRITER                                             --出票人:src.WRITER
       ,DST.HOLDERID                                           --持票人代码:src.HOLDERID
       ,DST.HOLDER                                             --持票人:src.HOLDER
       ,DST.BENEFICIARYID                                      --受益人代码:src.BENEFICIARYID
       ,DST.BENEFICIARY                                        --受益人:src.BENEFICIARY
       ,DST.ENDORSETIMES                                       --背书次数:src.ENDORSETIMES
       ,DST.LASTBENEFICIARY                                    --上次背书人:src.LASTBENEFICIARY
       ,DST.ACCEPTORID                                         --承兑人代码:src.ACCEPTORID
       ,DST.ACCEPTOR                                           --承兑人:src.ACCEPTOR
       ,DST.ACCEPTORLEVEL                                      --承兑人级别:src.ACCEPTORLEVEL
       ,DST.ACCEPTORREGION                                     --承兑人所在行政区域:src.ACCEPTORREGION
       ,DST.TERM                                               --期限:src.TERM
       ,DST.WRITEDATE                                          --出票日期:src.WRITEDATE
       ,DST.MATURITY                                           --到期日:src.MATURITY
       ,DST.LCCURRENCY                                         --币种:src.LCCURRENCY
       ,DST.BILLCOUNT                                          --票据数量:src.BILLCOUNT
       ,DST.BILLSUM                                            --票面金额:src.BILLSUM
       ,DST.FINISHDATE                                         --完成日期:src.FINISHDATE
       ,DST.INPUTORGID                                         --登记机构:src.INPUTORGID
       ,DST.INPUTUSERID                                        --登记人:src.INPUTUSERID
       ,DST.INPUTDATE                                          --登记日期:src.INPUTDATE
       ,DST.REMARK                                             --备注:src.REMARK
       ,DST.ACTUALSUM                                          --实付金额:src.ACTUALSUM
       ,DST.ACTUALINT                                          --实收利息:src.ACTUALINT
       ,DST.RATE                                               --贴现利率:src.RATE
       ,DST.INTTYPE                                            --贴现付息方式:src.INTTYPE
       ,DST.SCALE                                              --协议付息所占比例:src.SCALE
       ,DST.GATHERINGNAME                                      --收款人户名:src.GATHERINGNAME
       ,DST.ACCOUNTNO                                          --收款人帐号:src.ACCOUNTNO
       ,DST.ABOUTBANKNAME                                      --收款行行名:src.ABOUTBANKNAME
       ,DST.INTERSERIALNO                                      --承兑汇票组内序号:src.INTERSERIALNO
       ,DST.ABOUTBANKID                                        --收款行行号:src.ABOUTBANKID
       ,DST.RELATIVEPUTOUTNO                                   --相关出帐号:src.RELATIVEPUTOUTNO
       ,DST.SENDFLAG                                           --发送标志:src.SENDFLAG
       ,DST.PERPUTOUTNO                                        --相关出帐号1:src.PERPUTOUTNO
       ,DST.CHECKREPLYDATE                                     --查复日期:src.CHECKREPLYDATE
       ,DST.KEEPBILLORGID                                      --票据存放机构:src.KEEPBILLORGID
       ,DST.ADJDAY                                             --调整天数:src.ADJDAY
       ,DST.APPLYBANKACCOUNT                                   --申请人开户行号:src.APPLYBANKACCOUNT
       ,DST.ACCEPTORACCOUNT                                    --付款/承兑人账号:src.ACCEPTORACCOUNT
       ,DST.ACCEPTORBANK                                       --付款/承兑人开户银行:src.ACCEPTORBANK
       ,DST.PAYACCOUNT                                         --付款/承兑人开户行号:src.PAYACCOUNT
       ,DST.REMITTERBANKNAME                                   --出票银行名称:src.REMITTERBANKNAME
       ,DST.REMITTERACCOUNT                                    --出票人账号:src.REMITTERACCOUNT
       ,DST.REMITTERBANK                                       --出票银行号:src.REMITTERBANK
       ,DST.DISCOUNTFLAG                                       --贴现买方付息标记:src.DISCOUNTFLAG
       ,DST.DISCOUNTACCOUNT                                    --贴现买方付息账号:src.DISCOUNTACCOUNT
       ,DST.BUYRATE                                            --买方贴现付息比率:src.BUYRATE
       ,DST.DUEBILLNO                                          --借据号:src.DUEBILLNO
       ,DST.MIGRATEFLAG                                        --移植标志:src.MIGRATEFLAG
       ,DST.TERMDAY                                            --贴现天数:src.TERMDAY
       ,DST.APLYFLAG                                           --是否考虑对公节假日:src.APLYFLAG
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
       ,DST.SUBBILLTYPE                                        --票据种类:src.SUBBILLTYPE
       ,DST.RELATIVEAGREEMENT                                  --买方付息人是否在我行开户:src.RELATIVEAGREEMENT
   FROM F_LN_XDXT_BILL_INFO DST 
   LEFT JOIN F_LN_XDXT_BILL_INFO_INNTMP1 SRC 
     ON SRC.OBJECTTYPE          = DST.OBJECTTYPE 
    AND SRC.OBJECTNO            = DST.OBJECTNO 
    AND SRC.SERIALNO            = DST.SERIALNO 
  WHERE SRC.OBJECTTYPE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_LN_XDXT_BILL_INFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_LN_XDXT_BILL_INFO/"+V_DT+".parquet"
F_LN_XDXT_BILL_INFO_INNTMP2=F_LN_XDXT_BILL_INFO_INNTMP2.unionAll(F_LN_XDXT_BILL_INFO_INNTMP1)
F_LN_XDXT_BILL_INFO_INNTMP1.cache()
F_LN_XDXT_BILL_INFO_INNTMP2.cache()
nrowsi = F_LN_XDXT_BILL_INFO_INNTMP1.count()
nrowsa = F_LN_XDXT_BILL_INFO_INNTMP2.count()
F_LN_XDXT_BILL_INFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_LN_XDXT_BILL_INFO_INNTMP1.unpersist()
F_LN_XDXT_BILL_INFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_LN_XDXT_BILL_INFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_LN_XDXT_BILL_INFO/"+V_DT_LD+".parquet /"+dbname+"/F_LN_XDXT_BILL_INFO_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_LN_XDXT_BILL_INFO_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_LN_XDXT_BILL_INFO/"+V_DT+".parquet /"+dbname+"/F_LN_XDXT_BILL_INFO_BK/"+V_DT+".parquet")
