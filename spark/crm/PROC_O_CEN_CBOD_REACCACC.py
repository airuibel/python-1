#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CEN_CBOD_REACCACC').setMaster(sys.argv[2])
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

O_RE_CBOD_REACCACC = sqlContext.read.parquet(hdfs+'/O_RE_CBOD_REACCACC/*')
O_RE_CBOD_REACCACC.registerTempTable("O_RE_CBOD_REACCACC")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_RE_CBOD_REACCACC = sqlContext.read.parquet(hdfs+'/F_RE_CBOD_REACCACC_BK/'+V_DT_LD+'.parquet/*')
F_RE_CBOD_REACCACC.registerTempTable("F_RE_CBOD_REACCACC")

sql = """
 SELECT ETLDT                   AS ETLDT 
       ,RE_DOC_TYP              AS RE_DOC_TYP 
       ,RE_DD_NO                AS RE_DD_NO 
       ,RE_ACCP_APPLN_ACCT_NO   AS RE_ACCP_APPLN_ACCT_NO 
       ,RE_DP_AMT               AS RE_DP_AMT 
       ,RE_RMT_AMT              AS RE_RMT_AMT 
       ,RE_DD_STS               AS RE_DD_STS 
       ,RE_DD_DUEDT             AS RE_DD_DUEDT 
       ,RE_CUST_NO              AS RE_CUST_NO 
       ,RE_BA_STS               AS RE_BA_STS 
       ,RE_OPUN_COD             AS RE_OPUN_COD 
       ,RE_TX_SEQ_NO            AS RE_TX_SEQ_NO 
       ,RE_DL_STS               AS RE_DL_STS 
       ,RE_DL_DT                AS RE_DL_DT 
       ,RE_DL_SVC               AS RE_DL_SVC 
       ,RE_LST_TX_DT            AS RE_LST_TX_DT 
       ,RE_ACCP_LN_AMT          AS RE_ACCP_LN_AMT 
       ,RE_CRLMT_NO             AS RE_CRLMT_NO 
       ,RE_DL_DUE_DT            AS RE_DL_DUE_DT 
       ,RE_ACCP_APPLN_NAME      AS RE_ACCP_APPLN_NAME 
       ,RE_PAYEE_AWBK_NAME      AS RE_PAYEE_AWBK_NAME 
       ,RE_DP_ACCT_NO           AS RE_DP_ACCT_NO 
       ,RE_PAYEE_AWBK_NO_FL     AS RE_PAYEE_AWBK_NO_FL 
       ,RE_PAYEE_AWBK_NAME_FL   AS RE_PAYEE_AWBK_NAME_FL 
       ,RE_HOLDER_NAME          AS RE_HOLDER_NAME 
       ,RE_HOLDER_ACCT_NO       AS RE_HOLDER_ACCT_NO 
       ,RE_SVC                  AS RE_SVC 
       ,RE_APPL_BRH_STD         AS RE_APPL_BRH_STD 
       ,RE_SIG_DT               AS RE_SIG_DT 
       ,RE_GUAR_RATE            AS RE_GUAR_RATE 
       ,RE_BILL_BANK_NO         AS RE_BILL_BANK_NO 
       ,RE_BILL_TELLER_NO       AS RE_BILL_TELLER_NO 
       ,RE_PAY_TELLER_NO        AS RE_PAY_TELLER_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CEN'                   AS ODS_SYS_ID 
   FROM O_RE_CBOD_REACCACC A                                   --银行承兑汇票主档
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_RE_CBOD_REACCACC_INNTMP1 = sqlContext.sql(sql)
F_RE_CBOD_REACCACC_INNTMP1.registerTempTable("F_RE_CBOD_REACCACC_INNTMP1")

#F_RE_CBOD_REACCACC = sqlContext.read.parquet(hdfs+'/F_RE_CBOD_REACCACC_BK/'+V_DT_LD+'.parquet/*')
#F_RE_CBOD_REACCACC.registerTempTable("F_RE_CBOD_REACCACC")
sql = """
 SELECT DST.ETLDT                                               --平台日期:src.ETLDT
       ,DST.RE_DOC_TYP                                         --凭证种类(DOC):src.RE_DOC_TYP
       ,DST.RE_DD_NO                                           --汇票号码:src.RE_DD_NO
       ,DST.RE_ACCP_APPLN_ACCT_NO                              --出票人帐号:src.RE_ACCP_APPLN_ACCT_NO
       ,DST.RE_DP_AMT                                          --保证金金额(dp):src.RE_DP_AMT
       ,DST.RE_RMT_AMT                                         --汇款金额:src.RE_RMT_AMT
       ,DST.RE_DD_STS                                          --票据状态(DD):src.RE_DD_STS
       ,DST.RE_DD_DUEDT                                        --汇票到期日:src.RE_DD_DUEDT
       ,DST.RE_CUST_NO                                         --客户编号:src.RE_CUST_NO
       ,DST.RE_BA_STS                                          --承兑汇票状态:src.RE_BA_STS
       ,DST.RE_OPUN_COD                                        --营业单位代码:src.RE_OPUN_COD
       ,DST.RE_TX_SEQ_NO                                       --交易序号:src.RE_TX_SEQ_NO
       ,DST.RE_DL_STS                                          --挂失状态:src.RE_DL_STS
       ,DST.RE_DL_DT                                           --挂失日期:src.RE_DL_DT
       ,DST.RE_DL_SVC                                          --挂失手续费:src.RE_DL_SVC
       ,DST.RE_LST_TX_DT                                       --最后交易日期:src.RE_LST_TX_DT
       ,DST.RE_ACCP_LN_AMT                                     --承兑垫款:src.RE_ACCP_LN_AMT
       ,DST.RE_CRLMT_NO                                        --贷款额度编号:src.RE_CRLMT_NO
       ,DST.RE_DL_DUE_DT                                       --挂失止付止期:src.RE_DL_DUE_DT
       ,DST.RE_ACCP_APPLN_NAME                                 --出票人名称:src.RE_ACCP_APPLN_NAME
       ,DST.RE_PAYEE_AWBK_NAME                                 --收款人开户行名称:src.RE_PAYEE_AWBK_NAME
       ,DST.RE_DP_ACCT_NO                                      --保证金帐号(dp):src.RE_DP_ACCT_NO
       ,DST.RE_PAYEE_AWBK_NO_FL                                --收款行行号:src.RE_PAYEE_AWBK_NO_FL
       ,DST.RE_PAYEE_AWBK_NAME_FL                              --收款人开户银行名称FL:src.RE_PAYEE_AWBK_NAME_FL
       ,DST.RE_HOLDER_NAME                                     --持票人名称(60位):src.RE_HOLDER_NAME
       ,DST.RE_HOLDER_ACCT_NO                                  --持票人帐号:src.RE_HOLDER_ACCT_NO
       ,DST.RE_SVC                                             --手续费:src.RE_SVC
       ,DST.RE_APPL_BRH_STD                                    --申请行机构号:src.RE_APPL_BRH_STD
       ,DST.RE_SIG_DT                                          --出票日期(合同建立):src.RE_SIG_DT
       ,DST.RE_GUAR_RATE                                       --保证金利率:src.RE_GUAR_RATE
       ,DST.RE_BILL_BANK_NO                                    --承兑行行号:src.RE_BILL_BANK_NO
       ,DST.RE_BILL_TELLER_NO                                  --签发柜员号:src.RE_BILL_TELLER_NO
       ,DST.RE_PAY_TELLER_NO                                   --解付柜员号:src.RE_PAY_TELLER_NO
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统代码:src.ODS_SYS_ID
   FROM F_RE_CBOD_REACCACC DST 
   LEFT JOIN F_RE_CBOD_REACCACC_INNTMP1 SRC 
     ON SRC.RE_DD_NO            = DST.RE_DD_NO 
  WHERE SRC.RE_DD_NO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_RE_CBOD_REACCACC_INNTMP2 = sqlContext.sql(sql)
dfn="F_RE_CBOD_REACCACC/"+V_DT+".parquet"
UNION=F_RE_CBOD_REACCACC_INNTMP2.unionAll(F_RE_CBOD_REACCACC_INNTMP1)
F_RE_CBOD_REACCACC_INNTMP1.cache()
F_RE_CBOD_REACCACC_INNTMP2.cache()
nrowsi = F_RE_CBOD_REACCACC_INNTMP1.count()
nrowsa = F_RE_CBOD_REACCACC_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_RE_CBOD_REACCACC_INNTMP1.unpersist()
F_RE_CBOD_REACCACC_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_RE_CBOD_REACCACC lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_RE_CBOD_REACCACC/"+V_DT_LD+".parquet /"+dbname+"/F_RE_CBOD_REACCACC_BK/")
#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_RE_CBOD_REACCACC/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_RE_CBOD_REACCACC_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_RE_CBOD_REACCACC/"+V_DT+".parquet /"+dbname+"/F_RE_CBOD_REACCACC_BK/")