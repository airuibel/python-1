#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_SBK_CARDINFO').setMaster(sys.argv[2])
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

O_CI_SBK_CARDINFO = sqlContext.read.parquet(hdfs+'/O_CI_SBK_CARDINFO/*')
O_CI_SBK_CARDINFO.registerTempTable("O_CI_SBK_CARDINFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1
#先删除原表所有数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_SBK_CARDINFO/*.parquet")
#从昨天备表复制一份全量过来
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_SBK_CARDINFO_BK/"+V_DT_LD+".parquet /"+dbname+"/F_CI_SBK_CARDINFO/"+V_DT+".parquet")


F_CI_SBK_CARDINFO = sqlContext.read.parquet(hdfs+'/F_CI_SBK_CARDINFO/*')
F_CI_SBK_CARDINFO.registerTempTable("F_CI_SBK_CARDINFO")

sql = """
 SELECT A.WORKDATE              AS WORKDATE 
       ,A.BATNO                 AS BATNO 
       ,A.CUSTNO                AS CUSTNO 
       ,A.PEOPID                AS PEOPID 
       ,A.CARDNO                AS CARDNO 
       ,A.SYSID                 AS SYSID 
       ,A.CARDNAKER             AS CARDNAKER 
       ,A.SJPCH                 AS SJPCH 
       ,A.CJJGPCH               AS CJJGPCH 
       ,A.NAME                  AS NAME 
       ,A.IDTYPE                AS IDTYPE 
       ,A.IDNO                  AS IDNO 
       ,A.SEX                   AS SEX 
       ,A.TELEPHONE             AS TELEPHONE 
       ,A.COUNTRY               AS COUNTRY 
       ,A.AREACODE              AS AREACODE 
       ,A.NETFLAG               AS NETFLAG 
       ,A.ADRESS                AS ADRESS 
       ,A.ZY                    AS ZY 
       ,A.XYLB                  AS XYLB 
       ,A.ZJYXQ                 AS ZJYXQ 
       ,A.DWMC                  AS DWMC 
       ,A.JHRMC                 AS JHRMC 
       ,A.JHRXB                 AS JHRXB 
       ,A.JHRZJLX               AS JHRZJLX 
       ,A.JHRZJHM               AS JHRZJHM 
       ,A.JHRDZ                 AS JHRDZ 
       ,A.JHRLXDH               AS JHRLXDH 
       ,A.BHKBZ                 AS BHKBZ 
       ,A.JKYHKH                AS JKYHKH 
       ,A.JIHUO                 AS JIHUO 
       ,A.YZKBZ                 AS YZKBZ 
       ,A.FKRQ                  AS FKRQ 
       ,A.APPAREACODE           AS APPAREACODE 
       ,A.NOTE1                 AS NOTE1 
       ,A.NOTE2                 AS NOTE2 
       ,A.NOTE3                 AS NOTE3 
       ,A.SYSTYPE               AS SYSTYPE 
       ,A.SUBSYSID              AS SUBSYSID 
       ,A.JKCARDNO              AS JKCARDNO 
       ,A.ZONENO                AS ZONENO 
       ,A.BRNO                  AS BRNO 
       ,A.BIRTHDAY              AS BIRTHDAY 
       ,A.ORISMKNO              AS ORISMKNO 
       ,A.ORIJKNO               AS ORIJKNO 
       ,A.FR_ID                 AS FR_ID 
       ,V_DT                    AS ODS_ST_DATE 
       ,'SBK'                   AS ODS_SYS_ID 
   FROM O_CI_SBK_CARDINFO A                                    --社保卡
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_SBK_CARDINFO_INNTMP1 = sqlContext.sql(sql)
F_CI_SBK_CARDINFO_INNTMP1.registerTempTable("F_CI_SBK_CARDINFO_INNTMP1")

#F_CI_SBK_CARDINFO = sqlContext.read.parquet(hdfs+'/F_CI_SBK_CARDINFO/*')
#F_CI_SBK_CARDINFO.registerTempTable("F_CI_SBK_CARDINFO")
sql = """
 SELECT DST.WORKDATE                                            --平台受理日期:src.WORKDATE
       ,DST.BATNO                                              --社保批次号:src.BATNO
       ,DST.CUSTNO                                             --客户号:src.CUSTNO
       ,DST.PEOPID                                             --省个人识别号:src.PEOPID
       ,DST.CARDNO                                             --银行卡号:src.CARDNO
       ,DST.SYSID                                              --系统标识号:src.SYSID
       ,DST.CARDNAKER                                          --卡商代码:src.CARDNAKER
       ,DST.SJPCH                                              --市级批次号:src.SJPCH
       ,DST.CJJGPCH                                            --采集机构批次号:src.CJJGPCH
       ,DST.NAME                                               --姓名:src.NAME
       ,DST.IDTYPE                                             --证件类型:src.IDTYPE
       ,DST.IDNO                                               --证件号码:src.IDNO
       ,DST.SEX                                                --性别:src.SEX
       ,DST.TELEPHONE                                          --联系电话:src.TELEPHONE
       ,DST.COUNTRY                                            --国籍:src.COUNTRY
       ,DST.AREACODE                                           --行政区划代码:src.AREACODE
       ,DST.NETFLAG                                            --网内往外标志:src.NETFLAG
       ,DST.ADRESS                                             --地址:src.ADRESS
       ,DST.ZY                                                 --职业:src.ZY
       ,DST.XYLB                                               --行业类别:src.XYLB
       ,DST.ZJYXQ                                              --证件有效期:src.ZJYXQ
       ,DST.DWMC                                               --单位名称:src.DWMC
       ,DST.JHRMC                                              --监护人名称:src.JHRMC
       ,DST.JHRXB                                              --监护人性别:src.JHRXB
       ,DST.JHRZJLX                                            --监护人证件类型:src.JHRZJLX
       ,DST.JHRZJHM                                            --监护人证件号码:src.JHRZJHM
       ,DST.JHRDZ                                              --监护人地址:src.JHRDZ
       ,DST.JHRLXDH                                            --监护人联系电话:src.JHRLXDH
       ,DST.BHKBZ                                              --补换卡标志:src.BHKBZ
       ,DST.JKYHKH                                             --旧卡银行卡号:src.JKYHKH
       ,DST.JIHUO                                              --激活标志:src.JIHUO
       ,DST.YZKBZ                                              --预制卡标志:src.YZKBZ
       ,DST.FKRQ                                               --发卡日期:src.FKRQ
       ,DST.APPAREACODE                                        --申领地行政区划代码:src.APPAREACODE
       ,DST.NOTE1                                              --备注1:src.NOTE1
       ,DST.NOTE2                                              --备注2:src.NOTE2
       ,DST.NOTE3                                              --备注3:src.NOTE3
       ,DST.SYSTYPE                                            --系统类型:src.SYSTYPE
       ,DST.SUBSYSID                                           --子系统号:src.SUBSYSID
       ,DST.JKCARDNO                                           --健康卡号:src.JKCARDNO
       ,DST.ZONENO                                             --法人号:src.ZONENO
       ,DST.BRNO                                               --机构:src.BRNO
       ,DST.BIRTHDAY                                           --出生年月:src.BIRTHDAY
       ,DST.ORISMKNO                                           --旧(市民卡号/个人识别号):src.ORISMKNO
       ,DST.ORIJKNO                                            --ORIJKNO:src.ORIJKNO
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_ST_DATE                                        --系统日期:src.ODS_ST_DATE
       ,DST.ODS_SYS_ID                                         --系统标志:src.ODS_SYS_ID
   FROM F_CI_SBK_CARDINFO DST 
   LEFT JOIN F_CI_SBK_CARDINFO_INNTMP1 SRC 
     ON SRC.CARDNO              = DST.CARDNO 
  WHERE SRC.CARDNO IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_CI_SBK_CARDINFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_CI_SBK_CARDINFO/"+V_DT+".parquet"
F_CI_SBK_CARDINFO_INNTMP2=F_CI_SBK_CARDINFO_INNTMP2.unionAll(F_CI_SBK_CARDINFO_INNTMP1)
F_CI_SBK_CARDINFO_INNTMP1.cache()
F_CI_SBK_CARDINFO_INNTMP2.cache()
nrowsi = F_CI_SBK_CARDINFO_INNTMP1.count()
nrowsa = F_CI_SBK_CARDINFO_INNTMP2.count()
F_CI_SBK_CARDINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_CI_SBK_CARDINFO_INNTMP1.unpersist()
F_CI_SBK_CARDINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_CI_SBK_CARDINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_CI_SBK_CARDINFO/"+V_DT_LD+".parquet /"+dbname+"/F_CI_SBK_CARDINFO_BK/")
#先删除备表当天数据
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_CI_SBK_CARDINFO_BK/"+V_DT+".parquet")
#从当天原表复制一份全量到备表
ret = os.system("hdfs dfs -cp -f /"+dbname+"/F_CI_SBK_CARDINFO/"+V_DT+".parquet /"+dbname+"/F_CI_SBK_CARDINFO_BK/"+V_DT+".parquet")
