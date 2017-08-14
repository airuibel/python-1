#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_ECACCT').setMaster(sys.argv[2])
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

O_CI_WSYH_ECACCT = sqlContext.read.parquet(hdfs+'/O_CI_WSYH_ECACCT/*')
O_CI_WSYH_ECACCT.registerTempTable("O_CI_WSYH_ECACCT")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECACCT/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_WSYH_ECACCT_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_WSYH_ECACCT/"+V_DT+".parquet")

F_CI_WSYH_ECACCT = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECACCT/*')
F_CI_WSYH_ECACCT.registerTempTable("F_CI_WSYH_ECACCT")

sql = """
 SELECT A.CIFSEQ                AS CIFSEQ 
       ,A.ACSEQ                 AS ACSEQ 
       ,A.DEPTSEQ               AS DEPTSEQ 
       ,A.BANKACTYPE            AS BANKACTYPE 
       ,A.BANKACSUBTYPE         AS BANKACSUBTYPE 
       ,A.ACNO                  AS ACNO 
       ,A.ACNAME                AS ACNAME 
       ,A.ACORDER               AS ACORDER 
       ,A.CURRENCY              AS CURRENCY 
       ,A.CRFLAG                AS CRFLAG 
       ,A.ASSOCIFSEQ            AS ASSOCIFSEQ 
       ,A.ASSOCIFACFLAG         AS ASSOCIFACFLAG 
       ,A.ASSOCIFLEVEL          AS ASSOCIFLEVEL 
       ,A.CORECIFNO             AS CORECIFNO 
       ,A.ACALIAS               AS ACALIAS 
       ,A.ACSTATE               AS ACSTATE 
       ,A.CREATEUSERSEQ         AS CREATEUSERSEQ 
       ,A.CREATEDEPTSEQ         AS CREATEDEPTSEQ 
       ,A.CREATETIME            AS CREATETIME 
       ,A.UPDATEUSERSEQ         AS UPDATEUSERSEQ 
       ,A.UPDATEDEPTSEQ         AS UPDATEDEPTSEQ 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'IBK'                   AS ODS_SYS_ID 
   FROM O_CI_WSYH_ECACCT A                                     --客户账户表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECACCT_INNTMP1 = sqlContext.sql(sql)
F_CI_WSYH_ECACCT_INNTMP1.registerTempTable("F_CI_WSYH_ECACCT_INNTMP1")

#F_CI_WSYH_ECACCT = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECACCT/*')
#F_CI_WSYH_ECACCT.registerTempTable("F_CI_WSYH_ECACCT")
sql = """
 SELECT DST.CIFSEQ                                              --客户顺序号:src.CIFSEQ
       ,DST.ACSEQ                                              --账号顺序号:src.ACSEQ
       ,DST.DEPTSEQ                                            --账户开户机构:src.DEPTSEQ
       ,DST.BANKACTYPE                                         --银行账户类型:src.BANKACTYPE
       ,DST.BANKACSUBTYPE                                      --银行账户性质:src.BANKACSUBTYPE
       ,DST.ACNO                                               --账号:src.ACNO
       ,DST.ACNAME                                             --账户名称:src.ACNAME
       ,DST.ACORDER                                            --显示顺序号:src.ACORDER
       ,DST.CURRENCY                                           --币种:src.CURRENCY
       ,DST.CRFLAG                                             --钞汇标志:src.CRFLAG
       ,DST.ASSOCIFSEQ                                         --:src.ASSOCIFSEQ
       ,DST.ASSOCIFACFLAG                                      --关联企业账号标志:src.ASSOCIFACFLAG
       ,DST.ASSOCIFLEVEL                                       --关联企业级别:src.ASSOCIFLEVEL
       ,DST.CORECIFNO                                          --帐号所属核心客户号:src.CORECIFNO
       ,DST.ACALIAS                                            --账户别名:src.ACALIAS
       ,DST.ACSTATE                                            --状态:src.ACSTATE
       ,DST.CREATEUSERSEQ                                      --创建用户顺序号:src.CREATEUSERSEQ
       ,DST.CREATEDEPTSEQ                                      --创建机构顺序号:src.CREATEDEPTSEQ
       ,DST.CREATETIME                                         --创建时间:src.CREATETIME
       ,DST.UPDATEUSERSEQ                                      --更新用户顺序号:src.UPDATEUSERSEQ
       ,DST.UPDATEDEPTSEQ                                      --更新机构顺序号:src.UPDATEDEPTSEQ
       ,DST.UPDATETIME                                         --更新时间:src.UPDATETIME
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
   FROM F_CI_WSYH_ECACCT DST 
   LEFT JOIN F_CI_WSYH_ECACCT_INNTMP1 SRC 
     ON SRC.ACSEQ               = DST.ACSEQ 
  WHERE SRC.ACSEQ IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECACCT_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_WSYH_ECACCT/"+V_DT+".parquet"
F_CI_WSYH_ECACCT_INNTMP2=F_CI_WSYH_ECACCT_INNTMP2.unionAll(F_CI_WSYH_ECACCT_INNTMP1)
F_CI_WSYH_ECACCT_INNTMP1.cache()
F_CI_WSYH_ECACCT_INNTMP2.cache()
nrowsi = F_CI_WSYH_ECACCT_INNTMP1.count()
nrowsa = F_CI_WSYH_ECACCT_INNTMP2.count()
F_CI_WSYH_ECACCT_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_WSYH_ECACCT_INNTMP1.unpersist()
F_CI_WSYH_ECACCT_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_WSYH_ECACCT lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_WSYH_ECACCT/"+V_DT_LD+".parquet /"+dbname+"/F_CI_WSYH_ECACCT_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECACCT_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_WSYH_ECACCT/"+V_DT+".parquet /"+dbname+"/F_CI_WSYH_ECACCT_BK/"+V_DT+".parquet")
