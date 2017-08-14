#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_CRE_MPUR').setMaster(sys.argv[2])
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

O_TX_MPUR = sqlContext.read.parquet(hdfs+'/O_TX_MPUR/*')
O_TX_MPUR.registerTempTable("O_TX_MPUR")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_TX_MPUR = sqlContext.read.parquet(hdfs+'/F_TX_MPUR_BK/'+V_DT_LD+'.parquet/*')
F_TX_MPUR.registerTempTable("F_TX_MPUR")

sql = """
 SELECT BANK                    AS BANK 
       ,XACCOUNT                AS XACCOUNT 
       ,CARD_NBR                AS CARD_NBR 
       ,MP_NUMBER               AS MP_NUMBER 
       ,INP_DAY                 AS INP_DAY 
       ,INP_TIME                AS INP_TIME 
       ,AUTH_CODE               AS AUTH_CODE 
       ,MERCHANT                AS MERCHANT 
       ,MICRO_REF               AS MICRO_REF 
       ,PRODUCT                 AS PRODUCT 
       ,PURCH_DAY               AS PURCH_DAY 
       ,REACT_RESN              AS REACT_RESN 
       ,PROD_ID                 AS PROD_ID 
       ,INT_RATE                AS INT_RATE 
       ,ACCEL_INT               AS ACCEL_INT 
       ,FIRST_INT               AS FIRST_INT 
       ,MTH_INSTL               AS MTH_INSTL 
       ,INSTL_CNT               AS INSTL_CNT 
       ,LST_INSTDY              AS LST_INSTDY 
       ,MTH_INT                 AS MTH_INT 
       ,MTH_PPL                 AS MTH_PPL 
       ,MTH_FEE                 AS MTH_FEE 
       ,NBR_MTHS                AS NBR_MTHS 
       ,ORIG_PURCH              AS ORIG_PURCH 
       ,REM_PPL                 AS REM_PPL 
       ,ORIG_INT                AS ORIG_INT 
       ,TOT_INT_CG              AS TOT_INT_CG 
       ,REM_INT                 AS REM_INT 
       ,FEE_FLAG                AS FEE_FLAG 
       ,ORIG_FEE                AS ORIG_FEE 
       ,REM_FEE                 AS REM_FEE 
       ,TRANS_SRC               AS TRANS_SRC 
       ,STATUS_PRE              AS STATUS_PRE 
       ,STATUS                  AS STATUS 
       ,STATUS_DAY              AS STATUS_DAY 
       ,EMPLOYEE                AS EMPLOYEE 
       ,APP_SEQ                 AS APP_SEQ 
       ,INP_SRC                 AS INP_SRC 
       ,MP_TYPE                 AS MP_TYPE 
       ,ACCE_FEE                AS ACCE_FEE 
       ,ACCE_INT                AS ACCE_INT 
       ,ACCE_PFEE               AS ACCE_PFEE 
       ,ACCE_PPL                AS ACCE_PPL 
       ,INTER_MTHS              AS INTER_MTHS 
       ,RES_PPL                 AS RES_PPL 
       ,ADDR2                   AS ADDR2 
       ,PAYCF_DAY               AS PAYCF_DAY 
       ,FEE_DATE                AS FEE_DATE 
       ,REMARK                  AS REMARK 
       ,CURR_NUM                AS CURR_NUM 
       ,ORG_NO                  AS ORG_NO 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'CRE'                   AS ODS_SYS_ID 
   FROM O_TX_MPUR A                                            --分期付款交易记录
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_MPUR_INNTMP1 = sqlContext.sql(sql)
F_TX_MPUR_INNTMP1.registerTempTable("F_TX_MPUR_INNTMP1")

#F_TX_MPUR = sqlContext.read.parquet(hdfs+'/F_TX_MPUR/*')
#F_TX_MPUR.registerTempTable("F_TX_MPUR")
sql = """
 SELECT DST.BANK                                                --银行号:src.BANK
       ,DST.XACCOUNT                                           --账号:src.XACCOUNT
       ,DST.CARD_NBR                                           --卡号:src.CARD_NBR
       ,DST.MP_NUMBER                                          --分期付款序号:src.MP_NUMBER
       ,DST.INP_DAY                                            --记录日期:src.INP_DAY
       ,DST.INP_TIME                                           --记录时间:src.INP_TIME
       ,DST.AUTH_CODE                                          --授权号:src.AUTH_CODE
       ,DST.MERCHANT                                           --商户号:src.MERCHANT
       ,DST.MICRO_REF                                          --微索引编号:src.MICRO_REF
       ,DST.PRODUCT                                            --卡片产品号:src.PRODUCT
       ,DST.PURCH_DAY                                          --消费日期:src.PURCH_DAY
       ,DST.REACT_RESN                                         --激活原因描述:src.REACT_RESN
       ,DST.PROD_ID                                            --分期付款商品号:src.PROD_ID
       ,DST.INT_RATE                                           --分期付款利率:src.INT_RATE
       ,DST.ACCEL_INT                                          --罚息金额:src.ACCEL_INT
       ,DST.FIRST_INT                                          --首笔利息金额:src.FIRST_INT
       ,DST.MTH_INSTL                                          --每月摊消分期付款金额:src.MTH_INSTL
       ,DST.INSTL_CNT                                          --已分期摊消期数:src.INSTL_CNT
       ,DST.LST_INSTDY                                         --上次分期摊消日期:src.LST_INSTDY
       ,DST.MTH_INT                                            --上月分期摊消利息金额:src.MTH_INT
       ,DST.MTH_PPL                                            --上月分期摊消本金金额:src.MTH_PPL
       ,DST.MTH_FEE                                            --上月分期摊消费用金额:src.MTH_FEE
       ,DST.NBR_MTHS                                           --总分期月数:src.NBR_MTHS
       ,DST.ORIG_PURCH                                         --总产品金额（本金总金额）:src.ORIG_PURCH
       ,DST.REM_PPL                                            --剩余未还本金:src.REM_PPL
       ,DST.ORIG_INT                                           --总利息金额:src.ORIG_INT
       ,DST.TOT_INT_CG                                         --累计已摊消利息:src.TOT_INT_CG
       ,DST.REM_INT                                            --剩余未还利息:src.REM_INT
       ,DST.FEE_FLAG                                           --收费标志:src.FEE_FLAG
       ,DST.ORIG_FEE                                           --总费用:src.ORIG_FEE
       ,DST.REM_FEE                                            --剩余未还费用:src.REM_FEE
       ,DST.TRANS_SRC                                          --交易来源:src.TRANS_SRC
       ,DST.STATUS_PRE                                         --前一状态:src.STATUS_PRE
       ,DST.STATUS                                             --分期付款状态:src.STATUS
       ,DST.STATUS_DAY                                         --分期付款状态改变日期:src.STATUS_DAY
       ,DST.EMPLOYEE                                           --状态变动操作员:src.EMPLOYEE
       ,DST.APP_SEQ                                            --申请件编号:src.APP_SEQ
       ,DST.INP_SRC                                            --交易来源:src.INP_SRC
       ,DST.MP_TYPE                                            --交易类型:src.MP_TYPE
       ,DST.ACCE_FEE                                           --提前还款费用:src.ACCE_FEE
       ,DST.ACCE_INT                                           --提前还款利息:src.ACCE_INT
       ,DST.ACCE_PFEE                                          --提前还款手续费:src.ACCE_PFEE
       ,DST.ACCE_PPL                                           --提前还款本金:src.ACCE_PPL
       ,DST.INTER_MTHS                                         --分摊间隔期数:src.INTER_MTHS
       ,DST.RES_PPL                                            --已摊销暂不入账本金:src.RES_PPL
       ,DST.ADDR2                                              --交易描述2:src.ADDR2
       ,DST.PAYCF_DAY                                          --随心分到期日:src.PAYCF_DAY
       ,DST.FEE_DATE                                           --随心分费率:src.FEE_DATE
       ,DST.REMARK                                             --分期用途:src.REMARK
       ,DST.CURR_NUM                                           --币种:src.CURR_NUM
       ,DST.ORG_NO                                             --机构编号:src.ORG_NO
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --ETL日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --来源系统:src.ODS_SYS_ID
   FROM F_TX_MPUR DST 
   LEFT JOIN F_TX_MPUR_INNTMP1 SRC 
     ON SRC.BANK                = DST.BANK 
    AND SRC.CARD_NBR            = DST.CARD_NBR 
    AND SRC.MP_NUMBER           = DST.MP_NUMBER 
  WHERE SRC.BANK IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_MPUR_INNTMP2 = sqlContext.sql(sql)
dfn="F_TX_MPUR/"+V_DT+".parquet"
UNION=F_TX_MPUR_INNTMP2.unionAll(F_TX_MPUR_INNTMP1)
F_TX_MPUR_INNTMP1.cache()
F_TX_MPUR_INNTMP2.cache()
nrowsi = F_TX_MPUR_INNTMP1.count()
nrowsa = F_TX_MPUR_INNTMP2.count()
UNION.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_TX_MPUR_INNTMP1.unpersist()
F_TX_MPUR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_MPUR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
#ret = os.system("hdfs dfs -mv /"+dbname+"/F_TX_MPUR/"+V_DT_LD+".parquet /"+dbname+"/F_TX_MPUR_BK/")


#备份
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_MPUR/"+V_DT_LD+".parquet ")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_MPUR_BK/"+V_DT+".parquet ")
ret = os.system("hdfs dfs -cp /"+dbname+"/F_TX_MPUR/"+V_DT+".parquet /"+dbname+"/F_TX_MPUR_BK/")
