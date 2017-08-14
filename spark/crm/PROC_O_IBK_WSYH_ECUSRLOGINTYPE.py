#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_ECUSRLOGINTYPE').setMaster(sys.argv[2])
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

O_CI_WSYH_ECUSRLOGINTYPE = sqlContext.read.parquet(hdfs+'/O_CI_WSYH_ECUSRLOGINTYPE/*')
O_CI_WSYH_ECUSRLOGINTYPE.registerTempTable("O_CI_WSYH_ECUSRLOGINTYPE")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECUSRLOGINTYPE/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_WSYH_ECUSRLOGINTYPE_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_WSYH_ECUSRLOGINTYPE/"+V_DT+".parquet")


F_CI_WSYH_ECUSRLOGINTYPE = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECUSRLOGINTYPE/*')
F_CI_WSYH_ECUSRLOGINTYPE.registerTempTable("F_CI_WSYH_ECUSRLOGINTYPE")

sql = """
 SELECT A.USERSEQ               AS USERSEQ 
       ,A.MCHANNELID            AS MCHANNELID 
       ,A.LOGINTYPE             AS LOGINTYPE 
       ,A.USERID                AS USERID 
       ,A.PASSWORD              AS PASSWORD 
       ,A.LOGINTYPESTATE        AS LOGINTYPESTATE 
       ,A.UPDATEPASSWORDDATE    AS UPDATEPASSWORDDATE 
       ,A.WRONGPASSCOUNT        AS WRONGPASSCOUNT 
       ,A.UNLOCKDATE            AS UNLOCKDATE 
       ,A.FIRSTLOGINTIME        AS FIRSTLOGINTIME 
       ,A.LASTLOGINTIME         AS LASTLOGINTIME 
       ,A.LASTLOGINADDR         AS LASTLOGINADDR 
       ,A.CREATEUSERSEQ         AS CREATEUSERSEQ 
       ,A.CREATEDEPTSEQ         AS CREATEDEPTSEQ 
       ,A.CREATETIME            AS CREATETIME 
       ,A.UPDATEUSERSEQ         AS UPDATEUSERSEQ 
       ,A.UPDATEDEPTSEQ         AS UPDATEDEPTSEQ 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'IBK'                   AS ODS_SYS_ID 
   FROM O_CI_WSYH_ECUSRLOGINTYPE A                             --电子银行用户认证信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECUSRLOGINTYPE_INNTMP1 = sqlContext.sql(sql)
F_CI_WSYH_ECUSRLOGINTYPE_INNTMP1.registerTempTable("F_CI_WSYH_ECUSRLOGINTYPE_INNTMP1")

#F_CI_WSYH_ECUSRLOGINTYPE = sqlContext.read.parquet(hdfs+'/F_CI_WSYH_ECUSRLOGINTYPE/*')
#F_CI_WSYH_ECUSRLOGINTYPE.registerTempTable("F_CI_WSYH_ECUSRLOGINTYPE")
sql = """
 SELECT DST.USERSEQ                                             --用户顺序号:src.USERSEQ
       ,DST.MCHANNELID                                         --模块渠道代号:src.MCHANNELID
       ,DST.LOGINTYPE                                          --登录类型:src.LOGINTYPE
       ,DST.USERID                                             --用户登录号:src.USERID
       ,DST.PASSWORD                                           --用户登录密码:src.PASSWORD
       ,DST.LOGINTYPESTATE                                     --开通状态:src.LOGINTYPESTATE
       ,DST.UPDATEPASSWORDDATE                                 --最近密码修改时间:src.UPDATEPASSWORDDATE
       ,DST.WRONGPASSCOUNT                                     --密码错误次数:src.WRONGPASSCOUNT
       ,DST.UNLOCKDATE                                         --最后一次解锁日期时间:src.UNLOCKDATE
       ,DST.FIRSTLOGINTIME                                     --首次登录时间:src.FIRSTLOGINTIME
       ,DST.LASTLOGINTIME                                      --最后登录时间:src.LASTLOGINTIME
       ,DST.LASTLOGINADDR                                      --最后一次登录地址:src.LASTLOGINADDR
       ,DST.CREATEUSERSEQ                                      --创建用户顺序号:src.CREATEUSERSEQ
       ,DST.CREATEDEPTSEQ                                      --创建机构顺序号:src.CREATEDEPTSEQ
       ,DST.CREATETIME                                         --创建时间:src.CREATETIME
       ,DST.UPDATEUSERSEQ                                      --更新用户顺序号:src.UPDATEUSERSEQ
       ,DST.UPDATEDEPTSEQ                                      --更新机构顺序号:src.UPDATEDEPTSEQ
       ,DST.UPDATETIME                                         --更新时间:src.UPDATETIME
       ,DST.FR_ID                                              --法人号:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
   FROM F_CI_WSYH_ECUSRLOGINTYPE DST 
   LEFT JOIN F_CI_WSYH_ECUSRLOGINTYPE_INNTMP1 SRC 
     ON SRC.USERSEQ             = DST.USERSEQ 
    AND SRC.MCHANNELID          = DST.MCHANNELID 
    AND SRC.LOGINTYPE           = DST.LOGINTYPE 
  WHERE SRC.USERSEQ IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_WSYH_ECUSRLOGINTYPE_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_WSYH_ECUSRLOGINTYPE/"+V_DT+".parquet"
F_CI_WSYH_ECUSRLOGINTYPE_INNTMP2=F_CI_WSYH_ECUSRLOGINTYPE_INNTMP2.unionAll(F_CI_WSYH_ECUSRLOGINTYPE_INNTMP1)
F_CI_WSYH_ECUSRLOGINTYPE_INNTMP1.cache()
F_CI_WSYH_ECUSRLOGINTYPE_INNTMP2.cache()
nrowsi = F_CI_WSYH_ECUSRLOGINTYPE_INNTMP1.count()
nrowsa = F_CI_WSYH_ECUSRLOGINTYPE_INNTMP2.count()
F_CI_WSYH_ECUSRLOGINTYPE_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_WSYH_ECUSRLOGINTYPE_INNTMP1.unpersist()
F_CI_WSYH_ECUSRLOGINTYPE_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_WSYH_ECUSRLOGINTYPE lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_WSYH_ECUSRLOGINTYPE/"+V_DT_LD+".parquet /"+dbname+"/F_CI_WSYH_ECUSRLOGINTYPE_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_WSYH_ECUSRLOGINTYPE_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_WSYH_ECUSRLOGINTYPE/"+V_DT+".parquet /"+dbname+"/F_CI_WSYH_ECUSRLOGINTYPE_BK/"+V_DT+".parquet")
