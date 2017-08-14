#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_O_AFA_BATINFO').setMaster(sys.argv[2])
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

O_TX_AFA_BATINFO = sqlContext.read.parquet(hdfs+'/O_TX_AFA_BATINFO/*')
O_TX_AFA_BATINFO.registerTempTable("O_TX_AFA_BATINFO")

#任务[12] 001-01::
V_STEP = V_STEP + 1

F_TX_AFA_BATINFO = sqlContext.read.parquet(hdfs+'/F_TX_AFA_BATINFO/*')
F_TX_AFA_BATINFO.registerTempTable("F_TX_AFA_BATINFO")

sql = """
 SELECT SYSID                   AS SYSID 
       ,WORKDATE                AS WORKDATE 
       ,BATNO                   AS BATNO 
       ,WORKTIME                AS WORKTIME 
       ,CRPCOD                  AS CRPCOD 
       ,ITMCOD                  AS ITMCOD 
       ,BATTYP                  AS BATTYP 
       ,SUBBATTYP               AS SUBBATTYP 
       ,CTLFLG                  AS CTLFLG 
       ,ACCTYP                  AS ACCTYP 
       ,SAVTIM                  AS SAVTIM 
       ,RESAVTIM                AS RESAVTIM 
       ,TOTALCOUNT              AS TOTALCOUNT 
       ,TOTALAMOUNT             AS TOTALAMOUNT 
       ,SUCCESSCOUNT            AS SUCCESSCOUNT 
       ,SUCCESSAMOUNT           AS SUCCESSAMOUNT 
       ,BATSTAT                 AS BATSTAT 
       ,FILENAME                AS FILENAME 
       ,FILELEN                 AS FILELEN 
       ,FILETYPE                AS FILETYPE 
       ,PRODUCTCODE             AS PRODUCTCODE 
       ,FILESEQ                 AS FILESEQ 
       ,RELEASE                 AS RELEASE 
       ,BANKFILESTATE           AS BANKFILESTATE 
       ,BANKSQNO                AS BANKSQNO 
       ,BANKQUEUENO             AS BANKQUEUENO 
       ,DATASOURCE              AS DATASOURCE 
       ,ZONENO                  AS ZONENO 
       ,BRNO                    AS BRNO 
       ,TELLERNO                AS TELLERNO 
       ,CMTDAT                  AS CMTDAT 
       ,CMTBRNO                 AS CMTBRNO 
       ,CMTTELLERNO             AS CMTTELLERNO 
       ,CHANNELCODE             AS CHANNELCODE 
       ,CHANNELSEQ              AS CHANNELSEQ 
       ,CHANNELDATE             AS CHANNELDATE 
       ,THIRDFILE               AS THIRDFILE 
       ,DOWNFILENAME            AS DOWNFILENAME 
       ,ERRORCODE               AS ERRORCODE 
       ,ERRORMSG                AS ERRORMSG 
       ,NOTE1                   AS NOTE1 
       ,NOTE2                   AS NOTE2 
       ,FR_ID                   AS FR_ID 
       ,'AFA'                   AS ODS_SYS_ID 
       ,V_DT                    AS ODS_ST_DATE 
   FROM O_TX_AFA_BATINFO A                                     --批次信息汇总登记薄
"""

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_AFA_BATINFO_INNTMP1 = sqlContext.sql(sql)
F_TX_AFA_BATINFO_INNTMP1.registerTempTable("F_TX_AFA_BATINFO_INNTMP1")

#F_TX_AFA_BATINFO = sqlContext.read.parquet(hdfs+'/F_TX_AFA_BATINFO/*')
#F_TX_AFA_BATINFO.registerTempTable("F_TX_AFA_BATINFO")
sql = """
 SELECT DST.SYSID                                               --子系统号:src.SYSID
       ,DST.WORKDATE                                           --平台受理日期:src.WORKDATE
       ,DST.BATNO                                              --批次号:src.BATNO
       ,DST.WORKTIME                                           --平台时间:src.WORKTIME
       ,DST.CRPCOD                                             --单位编号:src.CRPCOD
       ,DST.ITMCOD                                             --项目编号:src.ITMCOD
       ,DST.BATTYP                                             --批量类型:src.BATTYP
       ,DST.SUBBATTYP                                          --子批量类型:src.SUBBATTYP
       ,DST.CTLFLG                                             --控制标志位:src.CTLFLG
       ,DST.ACCTYP                                             --入帐模式:src.ACCTYP
       ,DST.SAVTIM                                             --存期:src.SAVTIM
       ,DST.RESAVTIM                                           --自动转存期限:src.RESAVTIM
       ,DST.TOTALCOUNT                                         --总笔数:src.TOTALCOUNT
       ,DST.TOTALAMOUNT                                        --总金额:src.TOTALAMOUNT
       ,DST.SUCCESSCOUNT                                       --成功总笔数:src.SUCCESSCOUNT
       ,DST.SUCCESSAMOUNT                                      --成功总金额:src.SUCCESSAMOUNT
       ,DST.BATSTAT                                            --批次状态:src.BATSTAT
       ,DST.FILENAME                                           --上送主机文件名:src.FILENAME
       ,DST.FILELEN                                            --文件大小:src.FILELEN
       ,DST.FILETYPE                                           --上送主机文件业务类型:src.FILETYPE
       ,DST.PRODUCTCODE                                        --开户产品代码:src.PRODUCTCODE
       ,DST.FILESEQ                                            --上送主机文件编号:src.FILESEQ
       ,DST.RELEASE                                            --文件重传次数:src.RELEASE
       ,DST.BANKFILESTATE                                      --主机文件状态:src.BANKFILESTATE
       ,DST.BANKSQNO                                           --下载报表序列号:src.BANKSQNO
       ,DST.BANKQUEUENO                                        --下载报表队列号:src.BANKQUEUENO
       ,DST.DATASOURCE                                         --数据来源:src.DATASOURCE
       ,DST.ZONENO                                             --地区号:src.ZONENO
       ,DST.BRNO                                               --交易网点编号:src.BRNO
       ,DST.TELLERNO                                           --创建柜员:src.TELLERNO
       ,DST.CMTDAT                                             --提交日期:src.CMTDAT
       ,DST.CMTBRNO                                            --提交机构号:src.CMTBRNO
       ,DST.CMTTELLERNO                                        --提交柜员:src.CMTTELLERNO
       ,DST.CHANNELCODE                                        --发起渠道:src.CHANNELCODE
       ,DST.CHANNELSEQ                                         --发起渠道流水号:src.CHANNELSEQ
       ,DST.CHANNELDATE                                        --发起渠道日期:src.CHANNELDATE
       ,DST.THIRDFILE                                          --第三方源文件:src.THIRDFILE
       ,DST.DOWNFILENAME                                       --下载文件名:src.DOWNFILENAME
       ,DST.ERRORCODE                                          --响应码:src.ERRORCODE
       ,DST.ERRORMSG                                           --响应信息:src.ERRORMSG
       ,DST.NOTE1                                              --备用1:src.NOTE1
       ,DST.NOTE2                                              --备用2:src.NOTE2
       ,DST.FR_ID                                              --法人代码:src.FR_ID
       ,DST.ODS_SYS_ID                                         --源系统标志:src.ODS_SYS_ID
       ,DST.ODS_ST_DATE                                        --平台日期:src.ODS_ST_DATE
   FROM F_TX_AFA_BATINFO DST 
   LEFT JOIN F_TX_AFA_BATINFO_INNTMP1 SRC 
     ON SRC.WORKDATE            = DST.WORKDATE 
    AND SRC.BATNO               = DST.BATNO 
  WHERE SRC.WORKDATE IS NULL """

sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
F_TX_AFA_BATINFO_INNTMP2 = sqlContext.sql(sql)
dfn="F_TX_AFA_BATINFO/"+V_DT+".parquet"
F_TX_AFA_BATINFO_INNTMP2=F_TX_AFA_BATINFO_INNTMP2.unionAll(F_TX_AFA_BATINFO_INNTMP1)
F_TX_AFA_BATINFO_INNTMP1.cache()
F_TX_AFA_BATINFO_INNTMP2.cache()
nrowsi = F_TX_AFA_BATINFO_INNTMP1.count()
nrowsa = F_TX_AFA_BATINFO_INNTMP2.count()
F_TX_AFA_BATINFO_INNTMP2.write.save(path = hdfs + '/' + dfn, mode='overwrite')
F_TX_AFA_BATINFO_INNTMP1.unpersist()
F_TX_AFA_BATINFO_INNTMP2.unpersist()
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds, insert F_TX_AFA_BATINFO lines %d, all lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrowsi, nrowsa)
ret = os.system("hdfs dfs -mv /"+dbname+"/F_TX_AFA_BATINFO/"+V_DT_LD+".parquet /"+dbname+"/F_TX_AFA_BATINFO_BK/")
ret = os.system("hdfs dfs -rm -r /"+dbname+"/F_TX_AFA_BATINFO/"+V_DT_LD+".parquet")
