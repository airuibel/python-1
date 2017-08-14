#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_IBK_WSYH_ECCIF').setMaster(sys.argv[2])
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

O_TX_WSYH_ECCIF = sqlContext.read.parquet(hdfs+'/O_TX_WSYH_ECCIF/*')
O_TX_WSYH_ECCIF.registerTempTable("O_TX_WSYH_ECCIF")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_ECCIF/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_TX_WSYH_ECCIF_BK/"+V_DT_LD+".parquet /"+dbname+"/F_TX_WSYH_ECCIF/"+V_DT+".parquet")

F_TX_WSYH_ECCIF = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIF/*')
F_TX_WSYH_ECCIF.registerTempTable("F_TX_WSYH_ECCIF")

sql = """
 SELECT A.CIFSEQ                AS CIFSEQ 
       ,A.MODULEID              AS MODULEID 
       ,A.CIFENGNAME            AS CIFENGNAME 
       ,A.CIFNAMEPY             AS CIFNAMEPY 
       ,A.CIFNAME               AS CIFNAME 
       ,A.CIFLEVEL              AS CIFLEVEL 
       ,A.CORECIFLEVEL          AS CORECIFLEVEL 
       ,A.COREDEPTSEQ           AS COREDEPTSEQ 
       ,A.CIFTYPE               AS CIFTYPE 
       ,A.CIFCONTROL            AS CIFCONTROL 
       ,A.CIFMONITOR            AS CIFMONITOR 
       ,A.CIFEXEMPT             AS CIFEXEMPT 
       ,A.CIFLOANFLG            AS CIFLOANFLG 
       ,A.CIFFINVIPFLG          AS CIFFINVIPFLG 
       ,A.CCQUERYPWD            AS CCQUERYPWD 
       ,A.TRANSFERTYPE          AS TRANSFERTYPE 
       ,A.CIFDEPTSEQ            AS CIFDEPTSEQ 
       ,A.CIFSTATE              AS CIFSTATE 
       ,A.CREATEUSERSEQ         AS CREATEUSERSEQ 
       ,A.CREATEDEPTSEQ         AS CREATEDEPTSEQ 
       ,A.CREATETIME            AS CREATETIME 
       ,A.UPDATEUSERSEQ         AS UPDATEUSERSEQ 
       ,A.UPDATEDEPTSEQ         AS UPDATEDEPTSEQ 
       ,A.UPDATETIME            AS UPDATETIME 
       ,A.UPDATEMCHANNEL        AS UPDATEMCHANNEL 
       ,A.UPDATEJNLNO           AS UPDATEJNLNO 
       ,A.UPDATECIFSEQ          AS UPDATECIFSEQ 
       ,A.FR_ID                 AS FR_ID 
       ,'IBK'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
   FROM O_TX_WSYH_ECCIF A                                      --电子银行参与方信息表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_ECCIF_INNTMP1 = sqlContext.sql(sql)
F_TX_WSYH_ECCIF_INNTMP1.registerTempTable("F_TX_WSYH_ECCIF_INNTMP1")

#F_TX_WSYH_ECCIF = sqlContext.read.parquet(hdfs+'/F_TX_WSYH_ECCIF/*')
#F_TX_WSYH_ECCIF.registerTempTable("F_TX_WSYH_ECCIF")
sql = """
 SELECT DST.CIFSEQ                                              --客户顺序号:src.CIFSEQ
       ,DST.MODULEID                                           --模块代号:src.MODULEID
       ,DST.CIFENGNAME                                         --客户英文名称:src.CIFENGNAME
       ,DST.CIFNAMEPY                                          --客户名称拼音:src.CIFNAMEPY
       ,DST.CIFNAME                                            --客户名称:src.CIFNAME
       ,DST.CIFLEVEL                                           --电子银行等级:src.CIFLEVEL
       ,DST.CORECIFLEVEL                                       --核心系统银行等级:src.CORECIFLEVEL
       ,DST.COREDEPTSEQ                                        --核心客户所属机构顺序号号:src.COREDEPTSEQ
       ,DST.CIFTYPE                                            --客户类别:src.CIFTYPE
       ,DST.CIFCONTROL                                         --是否受控客户:src.CIFCONTROL
       ,DST.CIFMONITOR                                         --是否受监视客户:src.CIFMONITOR
       ,DST.CIFEXEMPT                                          --是否豁免客户:src.CIFEXEMPT
       ,DST.CIFLOANFLG                                         --是否贷款户:src.CIFLOANFLG
       ,DST.CIFFINVIPFLG                                       --是否理财VIP客户:src.CIFFINVIPFLG
       ,DST.CCQUERYPWD                                         --信用卡查询密码:src.CCQUERYPWD
       ,DST.TRANSFERTYPE                                       --对外转帐属性:src.TRANSFERTYPE
       ,DST.CIFDEPTSEQ                                         --客户归属机构:src.CIFDEPTSEQ
       ,DST.CIFSTATE                                           --状态:src.CIFSTATE
       ,DST.CREATEUSERSEQ                                      --创建用户顺序号:src.CREATEUSERSEQ
       ,DST.CREATEDEPTSEQ                                      --创建机构顺序号:src.CREATEDEPTSEQ
       ,DST.CREATETIME                                         --创建时间:src.CREATETIME
       ,DST.UPDATEUSERSEQ                                      --更新用户顺序号:src.UPDATEUSERSEQ
       ,DST.UPDATEDEPTSEQ                                      --更新机构顺序号:src.UPDATEDEPTSEQ
       ,DST.UPDATETIME                                         --更新时间:src.UPDATETIME
       ,DST.UPDATEMCHANNEL                                     --维护模块渠道:src.UPDATEMCHANNEL
       ,DST.UPDATEJNLNO                                        --维护流水号:src.UPDATEJNLNO
       ,DST.UPDATECIFSEQ                                       --维护客户号:src.UPDATECIFSEQ
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --系统平台日期:src.ODS_ST_DATE
   FROM F_TX_WSYH_ECCIF DST 
   LEFT JOIN F_TX_WSYH_ECCIF_INNTMP1 SRC 
     ON SRC.CIFSEQ              = DST.CIFSEQ 
  WHERE SRC.CIFSEQ IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_WSYH_ECCIF_INNTMP2 = sqlContext.sql(sql)
dfn="F_TX_WSYH_ECCIF/"+V_DT+".parquet"
F_TX_WSYH_ECCIF_INNTMP2=F_TX_WSYH_ECCIF_INNTMP2.unionAll(F_TX_WSYH_ECCIF_INNTMP1)
F_TX_WSYH_ECCIF_INNTMP1.cache()
F_TX_WSYH_ECCIF_INNTMP2.cache()
nrowsi = F_TX_WSYH_ECCIF_INNTMP1.count()
nrowsa = F_TX_WSYH_ECCIF_INNTMP2.count()
F_TX_WSYH_ECCIF_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_TX_WSYH_ECCIF_INNTMP1.unpersist()
F_TX_WSYH_ECCIF_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_WSYH_ECCIF lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_TX_WSYH_ECCIF/"+V_DT_LD+".parquet /"+dbname+"/F_TX_WSYH_ECCIF_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_WSYH_ECCIF_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_TX_WSYH_ECCIF/"+V_DT+".parquet /"+dbname+"/F_TX_WSYH_ECCIF_BK/"+V_DT+".parquet")
