#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_ECCIFMCH').setMaster(sys.argv[2])
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

O_TX_WSYH_ECCIFMCH = sqlContext.read.parquet(hdfs+'/O_TX_WSYH_ECCIFMCH/*')
O_TX_WSYH_ECCIFMCH.registerTempTable("O_TX_WSYH_ECCIFMCH")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_ECCIFMCH/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_TX_WSYH_ECCIFMCH_BK/"+V_DT_LD+".parquet /"+dbname+"/F_TX_WSYH_ECCIFMCH/"+V_DT+".parquet")


F_TX_WSYH_ECCIFMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIFMCH/*')
F_TX_WSYH_ECCIFMCH.registerTempTable("F_TX_WSYH_ECCIFMCH")

sql = """
 SELECT A.CIFSEQ                AS CIFSEQ 
       ,A.MCHANNELID            AS MCHANNELID 
       ,A.FEETIME               AS FEETIME 
       ,A.AGREEMENTNO           AS AGREEMENTNO 
       ,A.AGREEDATE             AS AGREEDATE 
       ,A.AGREEDETPTSEQ         AS AGREEDETPTSEQ 
       ,A.ACNO                  AS ACNO 
       ,A.CMCHANNESTATE         AS CMCHANNESTATE 
       ,A.CRMNO                 AS CRMNO 
       ,A.CRMNAME               AS CRMNAME 
       ,A.CRMTEL                AS CRMTEL 
       ,A.CRMDEPTNO             AS CRMDEPTNO 
       ,A.RERERRERNAME          AS RERERRERNAME 
       ,A.RERERRERTYPE          AS RERERRERTYPE 
       ,A.REFERRERNO            AS REFERRERNO 
       ,A.REFERRERDEPTSEQ       AS REFERRERDEPTSEQ 
       ,A.RERERRERNO            AS RERERRERNO 
       ,A.RERERRERCIFNO         AS RERERRERCIFNO 
       ,A.LOCKSTATE             AS LOCKSTATE 
       ,A.LOCKBEGINDATE         AS LOCKBEGINDATE 
       ,A.LOCKENDDATE           AS LOCKENDDATE 
       ,A.LOCKENDTIME           AS LOCKENDTIME 
       ,A.LOCKBEGINTIME         AS LOCKBEGINTIME 
       ,A.OPENTIME              AS OPENTIME 
       ,A.CLOSETIME             AS CLOSETIME 
       ,A.OPENUSERSEQ           AS OPENUSERSEQ 
       ,A.CLOSEUSERSEQ          AS CLOSEUSERSEQ 
       ,A.CLOSEREASON           AS CLOSEREASON 
       ,A.SELFREGFLAG           AS SELFREGFLAG 
       ,A.REOPENFLAG            AS REOPENFLAG 
       ,A.BATCHFLAG             AS BATCHFLAG 
       ,A.USEDATE               AS USEDATE 
       ,A.USERNO                AS USERNO 
       ,A.USERNAME              AS USERNAME 
       ,A.USERMOBILE            AS USERMOBILE 
       ,A.USERTEL               AS USERTEL 
       ,A.USERDEPTNO            AS USERDEPTNO 
       ,A.OPENCITY              AS OPENCITY 
       ,A.OPENDEPTSEQ           AS OPENDEPTSEQ 
       ,A.CHECKUSERSEQ          AS CHECKUSERSEQ 
       ,A.CHECKDEPTSEQ          AS CHECKDEPTSEQ 
       ,A.CREATEUSERSEQ         AS CREATEUSERSEQ 
       ,A.CREATEDEPTSEQ         AS CREATEDEPTSEQ 
       ,A.CREATETIME            AS CREATETIME 
       ,A.UPDATEUSERSEQ         AS UPDATEUSERSEQ 
       ,A.UPDATEDEPTSEQ         AS UPDATEDEPTSEQ 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.CLOSEJNLNO            AS CLOSEJNLNO 
       ,A.FR_ID                 AS FR_ID 
       ,'IBK'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
   FROM O_TX_WSYH_ECCIFMCH A                                   --客户渠道表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_ECCIFMCH_INNTMP1 = sqlContext.sql(sql)
F_TX_WSYH_ECCIFMCH_INNTMP1.registerTempTable("F_TX_WSYH_ECCIFMCH_INNTMP1")

#F_TX_WSYH_ECCIFMCH = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIFMCH/*')
#F_TX_WSYH_ECCIFMCH.registerTempTable("F_TX_WSYH_ECCIFMCH")
sql = """
 SELECT DST.CIFSEQ                                              --客户顺序号:src.CIFSEQ
       ,DST.MCHANNELID                                         --模块渠道代号:src.MCHANNELID
       ,DST.FEETIME                                            --扣费成功时间:src.FEETIME
       ,DST.AGREEMENTNO                                        --协议编号:src.AGREEMENTNO
       ,DST.AGREEDATE                                          --协议签订日期:src.AGREEDATE
       ,DST.AGREEDETPTSEQ                                      --签约机构顺序号:src.AGREEDETPTSEQ
       ,DST.ACNO                                               --主帐号:src.ACNO
       ,DST.CMCHANNESTATE                                      --状态:src.CMCHANNESTATE
       ,DST.CRMNO                                              --客户经理工号:src.CRMNO
       ,DST.CRMNAME                                            --客户经理姓名:src.CRMNAME
       ,DST.CRMTEL                                             --客户经理电话:src.CRMTEL
       ,DST.CRMDEPTNO                                          --客户经理所属网点:src.CRMDEPTNO
       ,DST.RERERRERNAME                                       --推荐人姓名:src.RERERRERNAME
       ,DST.RERERRERTYPE                                       --推荐人类型:src.RERERRERTYPE
       ,DST.REFERRERNO                                         --推荐人:src.REFERRERNO
       ,DST.REFERRERDEPTSEQ                                    --推荐人所属网点:src.REFERRERDEPTSEQ
       ,DST.RERERRERNO                                         --推荐人工号:src.RERERRERNO
       ,DST.RERERRERCIFNO                                      --推荐人客户号:src.RERERRERCIFNO
       ,DST.LOCKSTATE                                          --冻结状态:src.LOCKSTATE
       ,DST.LOCKBEGINDATE                                      --冻结开始时间:src.LOCKBEGINDATE
       ,DST.LOCKENDDATE                                        --冻结结束时间:src.LOCKENDDATE
       ,DST.LOCKENDTIME                                        --冻结开始:src.LOCKENDTIME
       ,DST.LOCKBEGINTIME                                      --冻结结束:src.LOCKBEGINTIME
       ,DST.OPENTIME                                           --开户时间:src.OPENTIME
       ,DST.CLOSETIME                                          --销户时间:src.CLOSETIME
       ,DST.OPENUSERSEQ                                        --开户柜员:src.OPENUSERSEQ
       ,DST.CLOSEUSERSEQ                                       --销户柜员:src.CLOSEUSERSEQ
       ,DST.CLOSEREASON                                        --销户原因:src.CLOSEREASON
       ,DST.SELFREGFLAG                                        --自助注册标志:src.SELFREGFLAG
       ,DST.REOPENFLAG                                         --销户重开标志:src.REOPENFLAG
       ,DST.BATCHFLAG                                          --批量签约标志:src.BATCHFLAG
       ,DST.USEDATE                                            --启用日期:src.USEDATE
       ,DST.USERNO                                             --经办人工号:src.USERNO
       ,DST.USERNAME                                           --经办员名称:src.USERNAME
       ,DST.USERMOBILE                                         --经办人手机:src.USERMOBILE
       ,DST.USERTEL                                            --经办人电话:src.USERTEL
       ,DST.USERDEPTNO                                         --经办人单位号:src.USERDEPTNO
       ,DST.OPENCITY                                           --开户地:src.OPENCITY
       ,DST.OPENDEPTSEQ                                        --开户机构顺序号:src.OPENDEPTSEQ
       ,DST.CHECKUSERSEQ                                       --审核开户柜员:src.CHECKUSERSEQ
       ,DST.CHECKDEPTSEQ                                       --审核开户机构顺序号:src.CHECKDEPTSEQ
       ,DST.CREATEUSERSEQ                                      --创建用户顺序号:src.CREATEUSERSEQ
       ,DST.CREATEDEPTSEQ                                      --创建机构顺序号:src.CREATEDEPTSEQ
       ,DST.CREATETIME                                         --创建时间:src.CREATETIME
       ,DST.UPDATEUSERSEQ                                      --更新用户顺序号:src.UPDATEUSERSEQ
       ,DST.UPDATEDEPTSEQ                                      --更新机构顺序号:src.UPDATEDEPTSEQ
       ,DST.UPDATETIME                                         --更新时间:src.UPDATETIME
       ,DST.CLOSEJNLNO                                         --失效流水号:src.CLOSEJNLNO
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
   FROM F_TX_WSYH_ECCIFMCH DST 
   LEFT JOIN F_TX_WSYH_ECCIFMCH_INNTMP1 SRC 
     ON SRC.CIFSEQ              = DST.CIFSEQ 
    AND SRC.MCHANNELID          = DST.MCHANNELID 
  WHERE SRC.CIFSEQ IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_ECCIFMCH_INNTMP2 = sqlContext.sql(sql)
dfn="F_TX_WSYH_ECCIFMCH/"+V_DT+".parquet"
F_TX_WSYH_ECCIFMCH_INNTMP2=F_TX_WSYH_ECCIFMCH_INNTMP2.unionAll(F_TX_WSYH_ECCIFMCH_INNTMP1)
F_TX_WSYH_ECCIFMCH_INNTMP1.cache()
F_TX_WSYH_ECCIFMCH_INNTMP2.cache()
nrowsi = F_TX_WSYH_ECCIFMCH_INNTMP1.count()
nrowsa = F_TX_WSYH_ECCIFMCH_INNTMP2.count()
F_TX_WSYH_ECCIFMCH_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_TX_WSYH_ECCIFMCH_INNTMP1.unpersist()
F_TX_WSYH_ECCIFMCH_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_WSYH_ECCIFMCH lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_TX_WSYH_ECCIFMCH/"+V_DT_LD+".parquet /"+dbname+"/F_TX_WSYH_ECCIFMCH_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_ECCIFMCH_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_TX_WSYH_ECCIFMCH/"+V_DT+".parquet /"+dbname+"/F_TX_WSYH_ECCIFMCH_BK/"+V_DT+".parquet")
