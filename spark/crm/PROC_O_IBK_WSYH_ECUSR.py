#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_ECUSR').setMaster(sys.argv[2])
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

O_CI_WSYH_ECUSR = sqlContext.read.parquet(hdfs+'/O_CI_WSYH_ECUSR/*')
O_CI_WSYH_ECUSR.registerTempTable("O_CI_WSYH_ECUSR")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECUSR/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_WSYH_ECUSR_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_WSYH_ECUSR/"+V_DT+".parquet")


F_CI_WSYH_ECUSR = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECUSR/*')
F_CI_WSYH_ECUSR.registerTempTable("F_CI_WSYH_ECUSR")

sql = """
 SELECT A.USERSEQ               AS USERSEQ 
       ,A.DIVSEQ                AS DIVSEQ 
       ,A.CIFSEQ                AS CIFSEQ 
       ,A.USERNAME              AS USERNAME 
       ,A.USERID                AS USERID 
       ,A.PASSWORD              AS PASSWORD 
       ,A.IDTYPE                AS IDTYPE 
       ,A.IDNO                  AS IDNO 
       ,A.EMAIL                 AS EMAIL 
       ,A.PHONE                 AS PHONE 
       ,A.USERSTATE             AS USERSTATE 
       ,A.GENDER                AS GENDER 
       ,A.TITLE                 AS TITLE 
       ,A.NATIONALITY           AS NATIONALITY 
       ,A.MOBILEPHONE           AS MOBILEPHONE 
       ,A.CIFACRIGHT            AS CIFACRIGHT 
       ,A.OPENDATE              AS OPENDATE 
       ,A.CLOSEDATE             AS CLOSEDATE 
       ,A.USERDUTY              AS USERDUTY 
       ,A.USERPLACE             AS USERPLACE 
       ,A.OUTOFOFFICEFLAG       AS OUTOFOFFICEFLAG 
       ,A.AUTHGROUP             AS AUTHGROUP 
       ,A.ADMINUSER             AS ADMINUSER 
       ,A.OLDUSERID             AS OLDUSERID 
       ,A.AUTHLEVEL             AS AUTHLEVEL 
       ,A.CREATEUSERSEQ         AS CREATEUSERSEQ 
       ,A.CREATEDEPTSEQ         AS CREATEDEPTSEQ 
       ,A.CREATETIME            AS CREATETIME 
       ,A.UPDATEUSERSEQ         AS UPDATEUSERSEQ 
       ,A.UPDATEDEPTSEQ         AS UPDATEDEPTSEQ 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'IBK'                   AS ODS_SYS_ID 
   FROM O_CI_WSYH_ECUSR A                                      --电子银行用户表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECUSR_INNTMP1 = sqlContext.sql(sql)
F_CI_WSYH_ECUSR_INNTMP1.registerTempTable("F_CI_WSYH_ECUSR_INNTMP1")

#F_CI_WSYH_ECUSR = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECUSR/*')
#F_CI_WSYH_ECUSR.registerTempTable("F_CI_WSYH_ECUSR")
sql = """
 SELECT DST.USERSEQ                                             --用户顺序号:src.USERSEQ
       ,DST.DIVSEQ                                             --部门顺序号:src.DIVSEQ
       ,DST.CIFSEQ                                             --客户顺序号:src.CIFSEQ
       ,DST.USERNAME                                           --用户名称:src.USERNAME
       ,DST.USERID                                             --电子银行登录号:src.USERID
       ,DST.PASSWORD                                           --电子银行登录密码:src.PASSWORD
       ,DST.IDTYPE                                             --证件类型:src.IDTYPE
       ,DST.IDNO                                               --证件号:src.IDNO
       ,DST.EMAIL                                              --EMAIL:src.EMAIL
       ,DST.PHONE                                              --固定电话:src.PHONE
       ,DST.USERSTATE                                          --状态:src.USERSTATE
       ,DST.GENDER                                             --性别:src.GENDER
       ,DST.TITLE                                              --称谓:src.TITLE
       ,DST.NATIONALITY                                        --国籍:src.NATIONALITY
       ,DST.MOBILEPHONE                                        --手机号码:src.MOBILEPHONE
       ,DST.CIFACRIGHT                                         --是否客户级所有账号:src.CIFACRIGHT
       ,DST.OPENDATE                                           --用户开户日期:src.OPENDATE
       ,DST.CLOSEDATE                                          --用户销户日期:src.CLOSEDATE
       ,DST.USERDUTY                                           --用户职务:src.USERDUTY
       ,DST.USERPLACE                                          --区域:src.USERPLACE
       ,DST.OUTOFOFFICEFLAG                                    --不在岗标志:src.OUTOFOFFICEFLAG
       ,DST.AUTHGROUP                                          --授权组别:src.AUTHGROUP
       ,DST.ADMINUSER                                          --管理员标志:src.ADMINUSER
       ,DST.OLDUSERID                                          --:src.OLDUSERID
       ,DST.AUTHLEVEL                                          --:src.AUTHLEVEL
       ,DST.CREATEUSERSEQ                                      --创建用户顺序号:src.CREATEUSERSEQ
       ,DST.CREATEDEPTSEQ                                      --创建机构顺序号:src.CREATEDEPTSEQ
       ,DST.CREATETIME                                         --创建时间:src.CREATETIME
       ,DST.UPDATEUSERSEQ                                      --更新用户顺序号:src.UPDATEUSERSEQ
       ,DST.UPDATEDEPTSEQ                                      --更新机构顺序号:src.UPDATEDEPTSEQ
       ,DST.UPDATETIME                                         --更新时间:src.UPDATETIME
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
   FROM F_CI_WSYH_ECUSR DST 
   LEFT JOIN F_CI_WSYH_ECUSR_INNTMP1 SRC 
     ON SRC.USERSEQ             = DST.USERSEQ 
  WHERE SRC.USERSEQ IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECUSR_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_WSYH_ECUSR/"+V_DT+".parquet"
F_CI_WSYH_ECUSR_INNTMP2=F_CI_WSYH_ECUSR_INNTMP2.unionAll(F_CI_WSYH_ECUSR_INNTMP1)
F_CI_WSYH_ECUSR_INNTMP1.cache()
F_CI_WSYH_ECUSR_INNTMP2.cache()
nrowsi = F_CI_WSYH_ECUSR_INNTMP1.count()
nrowsa = F_CI_WSYH_ECUSR_INNTMP2.count()
F_CI_WSYH_ECUSR_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_WSYH_ECUSR_INNTMP1.unpersist()
F_CI_WSYH_ECUSR_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_WSYH_ECUSR lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_WSYH_ECUSR/"+V_DT_LD+".parquet /"+dbname+"/F_CI_WSYH_ECUSR_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECUSR_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_WSYH_ECUSR/"+V_DT+".parquet /"+dbname+"/F_CI_WSYH_ECUSR_BK/"+V_DT+".parquet")
