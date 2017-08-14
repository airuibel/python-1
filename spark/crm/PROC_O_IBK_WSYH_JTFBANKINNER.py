#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_JTFBANKINNER').setMaster(sys.argv[2])
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

O_TX_WSYH_JTFBANKINNER = sqlContext.read.parquet(hdfs+'/O_TX_WSYH_JTFBANKINNER/*')
O_TX_WSYH_JTFBANKINNER.registerTempTable("O_TX_WSYH_JTFBANKINNER")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_JTFBANKINNER/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_TX_WSYH_JTFBANKINNER_BK/"+V_DT_LD+".parquet /"+dbname+"/F_TX_WSYH_JTFBANKINNER/"+V_DT+".parquet")


F_TX_WSYH_JTFBANKINNER = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_JTFBANKINNER/*')
F_TX_WSYH_JTFBANKINNER.registerTempTable("F_TX_WSYH_JTFBANKINNER")

sql = """
 SELECT A.JNLNO                 AS JNLNO 
       ,A.SCHEDULEFLAG          AS SCHEDULEFLAG 
       ,A.SCHEDULERULE          AS SCHEDULERULE 
       ,A.PROCESSSTATE          AS PROCESSSTATE 
       ,A.PAYERBANKACTYP        AS PAYERBANKACTYP 
       ,A.PAYERACNO             AS PAYERACNO 
       ,A.CORECIFNO             AS CORECIFNO 
       ,A.PAYERSUBACNO          AS PAYERSUBACNO 
       ,A.PAYERSUBACTYPE        AS PAYERSUBACTYPE 
       ,A.PAYERSUBACSEQ         AS PAYERSUBACSEQ 
       ,A.PAYERCURRENCYCODE     AS PAYERCURRENCYCODE 
       ,A.PAYERCRFLAG           AS PAYERCRFLAG 
       ,A.PAYEEDEPTSEQ          AS PAYEEDEPTSEQ 
       ,A.PAYEECIFTYPE          AS PAYEECIFTYPE 
       ,A.PAYEEACNAME           AS PAYEEACNAME 
       ,A.PAYEEBANKACTYPE       AS PAYEEBANKACTYPE 
       ,A.PAYEEACNO             AS PAYEEACNO 
       ,A.PAYEECURRENCYCODE     AS PAYEECURRENCYCODE 
       ,A.PAYEECRFLAG           AS PAYEECRFLAG 
       ,A.TRSCURRENCYCODE       AS TRSCURRENCYCODE 
       ,A.TRSCRFLAG             AS TRSCRFLAG 
       ,A.TRSAMOUNT             AS TRSAMOUNT 
       ,A.FUNDUSAGE             AS FUNDUSAGE 
       ,A.ATTACHINFO            AS ATTACHINFO 
       ,A.TRSPASSWORD           AS TRSPASSWORD 
       ,A.SVCID                 AS SVCID 
       ,A.ISADDTOPAYEEBOOK      AS ISADDTOPAYEEBOOK 
       ,A.PAYEEACID             AS PAYEEACID 
       ,A.NOTICE                AS NOTICE 
       ,A.PROCESSTIME           AS PROCESSTIME 
       ,A.PARENTJNLNO           AS PARENTJNLNO 
       ,A.PAYERACNAME           AS PAYERACNAME 
       ,A.BATCHJNLNO            AS BATCHJNLNO 
       ,A.TRSTYPEFLAG           AS TRSTYPEFLAG 
       ,A.TRANSFERPOLICYTYPE    AS TRANSFERPOLICYTYPE 
       ,A.MOBILE                AS MOBILE 
       ,A.ORDERID               AS ORDERID 
       ,A.TRANSDATE             AS TRANSDATE 
       ,A.FEECODE               AS FEECODE 
       ,A.FEEAMOUNT             AS FEEAMOUNT 
       ,A.PAYEEMOBILE           AS PAYEEMOBILE 
       ,A.PAYEEEMAIL            AS PAYEEEMAIL 
       ,A.NOTICEPAYEEFLAG       AS NOTICEPAYEEFLAG 
       ,A.BATCHNOTICESEQ        AS BATCHNOTICESEQ 
       ,A.BATCHNOTICEFLAG       AS BATCHNOTICEFLAG 
       ,A.FR_ID                 AS FR_ID 
       ,'IBK'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
   FROM O_TX_WSYH_JTFBANKINNER A                               --行内转账交易流水表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_JTFBANKINNER_INNTMP1 = sqlContext.sql(sql)
F_TX_WSYH_JTFBANKINNER_INNTMP1.registerTempTable("F_TX_WSYH_JTFBANKINNER_INNTMP1")

#F_TX_WSYH_JTFBANKINNER = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_JTFBANKINNER/*')
#F_TX_WSYH_JTFBANKINNER.registerTempTable("F_TX_WSYH_JTFBANKINNER")
sql = """
 SELECT DST.JNLNO                                               --流水号:src.JNLNO
       ,DST.SCHEDULEFLAG                                       --预约标志:src.SCHEDULEFLAG
       ,DST.SCHEDULERULE                                       --预约指令:src.SCHEDULERULE
       ,DST.PROCESSSTATE                                       --处理状态:src.PROCESSSTATE
       ,DST.PAYERBANKACTYP                                     --转出账户类型:src.PAYERBANKACTYP
       ,DST.PAYERACNO                                          --转出账号:src.PAYERACNO
       ,DST.CORECIFNO                                          --转出账户核心客户号:src.CORECIFNO
       ,DST.PAYERSUBACNO                                       --转出子账号:src.PAYERSUBACNO
       ,DST.PAYERSUBACTYPE                                     --转出子账户类型:src.PAYERSUBACTYPE
       ,DST.PAYERSUBACSEQ                                      --转出子账户序号:src.PAYERSUBACSEQ
       ,DST.PAYERCURRENCYCODE                                  --转出币种代码:src.PAYERCURRENCYCODE
       ,DST.PAYERCRFLAG                                        --转出币种钞汇标志:src.PAYERCRFLAG
       ,DST.PAYEEDEPTSEQ                                       --转入帐号所属机构号:src.PAYEEDEPTSEQ
       ,DST.PAYEECIFTYPE                                       --转入客户类型:src.PAYEECIFTYPE
       ,DST.PAYEEACNAME                                        --转入方户名:src.PAYEEACNAME
       ,DST.PAYEEBANKACTYPE                                    --转入账户类型:src.PAYEEBANKACTYPE
       ,DST.PAYEEACNO                                          --转入账号:src.PAYEEACNO
       ,DST.PAYEECURRENCYCODE                                  --转入币种代码:src.PAYEECURRENCYCODE
       ,DST.PAYEECRFLAG                                        --转入币种钞汇标志:src.PAYEECRFLAG
       ,DST.TRSCURRENCYCODE                                    --交易币种代码:src.TRSCURRENCYCODE
       ,DST.TRSCRFLAG                                          --交易币种钞汇标志:src.TRSCRFLAG
       ,DST.TRSAMOUNT                                          --交易金额:src.TRSAMOUNT
       ,DST.FUNDUSAGE                                          --付款用途:src.FUNDUSAGE
       ,DST.ATTACHINFO                                         --附言:src.ATTACHINFO
       ,DST.TRSPASSWORD                                        --交易密码:src.TRSPASSWORD
       ,DST.SVCID                                              --服务ID:src.SVCID
       ,DST.ISADDTOPAYEEBOOK                                   --是否添加收款人:src.ISADDTOPAYEEBOOK
       ,DST.PAYEEACID                                          --收款单位编号:src.PAYEEACID
       ,DST.NOTICE                                             --通知人:src.NOTICE
       ,DST.PROCESSTIME                                        --最后处理时间:src.PROCESSTIME
       ,DST.PARENTJNLNO                                        --关联流水号:src.PARENTJNLNO
       ,DST.PAYERACNAME                                        --转出帐户名称:src.PAYERACNAME
       ,DST.BATCHJNLNO                                         --批次号:src.BATCHJNLNO
       ,DST.TRSTYPEFLAG                                        --对公对私转帐标志:src.TRSTYPEFLAG
       ,DST.TRANSFERPOLICYTYPE                                 --转帐类型:src.TRANSFERPOLICYTYPE
       ,DST.MOBILE                                             --短信接收手机号:src.MOBILE
       ,DST.ORDERID                                            --交易识别码:src.ORDERID
       ,DST.TRANSDATE                                          --交易时间:src.TRANSDATE
       ,DST.FEECODE                                            --费种代码:src.FEECODE
       ,DST.FEEAMOUNT                                          --手续费:src.FEEAMOUNT
       ,DST.PAYEEMOBILE                                        --收款人手机号码:src.PAYEEMOBILE
       ,DST.PAYEEEMAIL                                         --收款人邮箱:src.PAYEEEMAIL
       ,DST.NOTICEPAYEEFLAG                                    --是否通知收款人:src.NOTICEPAYEEFLAG
       ,DST.BATCHNOTICESEQ                                     --批量通知序号:src.BATCHNOTICESEQ
       ,DST.BATCHNOTICEFLAG                                    --是否批量通知:src.BATCHNOTICEFLAG
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --平台日期:src.ODS_ST_DATE
   FROM F_TX_WSYH_JTFBANKINNER DST 
   LEFT JOIN F_TX_WSYH_JTFBANKINNER_INNTMP1 SRC 
     ON SRC.JNLNO               = DST.JNLNO 
  WHERE SRC.JNLNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_JTFBANKINNER_INNTMP2 = sqlContext.sql(sql)
dfn="F_TX_WSYH_JTFBANKINNER/"+V_DT+".parquet"
F_TX_WSYH_JTFBANKINNER_INNTMP2=F_TX_WSYH_JTFBANKINNER_INNTMP2.unionAll(F_TX_WSYH_JTFBANKINNER_INNTMP1)
F_TX_WSYH_JTFBANKINNER_INNTMP1.cache()
F_TX_WSYH_JTFBANKINNER_INNTMP2.cache()
nrowsi = F_TX_WSYH_JTFBANKINNER_INNTMP1.count()
nrowsa = F_TX_WSYH_JTFBANKINNER_INNTMP2.count()
F_TX_WSYH_JTFBANKINNER_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_TX_WSYH_JTFBANKINNER_INNTMP1.unpersist()
F_TX_WSYH_JTFBANKINNER_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_WSYH_JTFBANKINNER lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_TX_WSYH_JTFBANKINNER/"+V_DT_LD+".parquet /"+dbname+"/F_TX_WSYH_JTFBANKINNER_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_JTFBANKINNER_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_TX_WSYH_JTFBANKINNER/"+V_DT+".parquet /"+dbname+"/F_TX_WSYH_JTFBANKINNER_BK/"+V_DT+".parquet")
