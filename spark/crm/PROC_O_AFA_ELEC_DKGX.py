#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_AFA_ELEC_DKGX').setMaster(sys.argv[2])
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

O_NI_AFA_ELEC_DKGX = sqlContext.read.parquet(hdfs+'/O_NI_AFA_ELEC_DKGX/*')
O_NI_AFA_ELEC_DKGX.registerTempTable("O_NI_AFA_ELEC_DKGX")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_NI_AFA_ELEC_DKGX = sqlContext.read.parquet(hdfs+'/F_NI_AFA_ELEC_DKGX/*')
F_NI_AFA_ELEC_DKGX.registerTempTable("F_NI_AFA_ELEC_DKGX")

sql = """
 SELECT SYSID                   AS SYSID 
       ,ZHH                     AS ZHH 
       ,QYLX                    AS QYLX 
       ,HM                      AS HM 
       ,DZ                      AS DZ 
       ,QYRQ                    AS QYRQ 
       ,BGRQ                    AS BGRQ 
       ,JGM                     AS JGM 
       ,UPJGM                   AS UPJGM 
       ,KHYH                    AS KHYH 
       ,KZBZ                    AS KZBZ 
       ,YHZH                    AS YHZH 
       ,ZHHM                    AS ZHHM 
       ,TEL                     AS TEL 
       ,DSYBZ                   AS DSYBZ 
       ,ZT                      AS ZT 
       ,TELLERNO                AS TELLERNO 
       ,NOTE1                   AS NOTE1 
       ,NOTE2                   AS NOTE2 
       ,NOTE3                   AS NOTE3 
       ,FR_ID                   AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'AFA'                   AS ODS_SYS_ID 
   FROM O_NI_AFA_ELEC_DKGX A                                   --省级电费代扣关系表
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_NI_AFA_ELEC_DKGX_INNTMP1 = sqlContext.sql(sql)
F_NI_AFA_ELEC_DKGX_INNTMP1.registerTempTable("F_NI_AFA_ELEC_DKGX_INNTMP1")

#F_NI_AFA_ELEC_DKGX = sqlContext.read.parquet(hdfs+'/F_NI_AFA_ELEC_DKGX/*')
#F_NI_AFA_ELEC_DKGX.registerTempTable("F_NI_AFA_ELEC_DKGX")
sql = """
 SELECT DST.SYSID                                               --系统标识:src.SYSID
       ,DST.ZHH                                                --总用户号:src.ZHH
       ,DST.QYLX                                               --签约类型:src.QYLX
       ,DST.HM                                                 --户名:src.HM
       ,DST.DZ                                                 --地址:src.DZ
       ,DST.QYRQ                                               --签约日期:src.QYRQ
       ,DST.BGRQ                                               --变更日期:src.BGRQ
       ,DST.JGM                                                --网点机构码:src.JGM
       ,DST.UPJGM                                              --清算机构码:src.UPJGM
       ,DST.KHYH                                               --银行代码:src.KHYH
       ,DST.KZBZ                                               --卡折标志:src.KZBZ
       ,DST.YHZH                                               --代扣账号:src.YHZH
       ,DST.ZHHM                                               --账号户名:src.ZHHM
       ,DST.TEL                                                --联系电话:src.TEL
       ,DST.DSYBZ                                              --单双月标志:src.DSYBZ
       ,DST.ZT                                                 --状态:src.ZT
       ,DST.TELLERNO                                           --柜员号:src.TELLERNO
       ,DST.NOTE1                                              --备注1:src.NOTE1
       ,DST.NOTE2                                              --备注2:src.NOTE2
       ,DST.NOTE3                                              --备注3:src.NOTE3
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --平台数据日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --源系统代码:src.ODS_SYS_ID
   FROM F_NI_AFA_ELEC_DKGX DST 
   LEFT JOIN F_NI_AFA_ELEC_DKGX_INNTMP1 SRC 
     ON SRC.SYSID               = DST.SYSID 
    AND SRC.ZHH                 = DST.ZHH 
  WHERE SRC.SYSID IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_NI_AFA_ELEC_DKGX_INNTMP2 = sqlContext.sql(sql)
dfn="F_NI_AFA_ELEC_DKGX/"+V_DT+".parquet"
F_NI_AFA_ELEC_DKGX_INNTMP2=F_NI_AFA_ELEC_DKGX_INNTMP2.unionAll(F_NI_AFA_ELEC_DKGX_INNTMP1)
F_NI_AFA_ELEC_DKGX_INNTMP1.cache()
F_NI_AFA_ELEC_DKGX_INNTMP2.cache()
nrowsi = F_NI_AFA_ELEC_DKGX_INNTMP1.count()
nrowsa = F_NI_AFA_ELEC_DKGX_INNTMP2.count()
F_NI_AFA_ELEC_DKGX_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_NI_AFA_ELEC_DKGX_INNTMP1.unpersist()
F_NI_AFA_ELEC_DKGX_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_NI_AFA_ELEC_DKGX lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_NI_AFA_ELEC_DKGX/"+V_DT_LD+".parquet /"+dbname+"/F_NI_AFA_ELEC_DKGX_BK/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_NI_AFA_ELEC_DKGX/"+V_DT_LD+".parquet")