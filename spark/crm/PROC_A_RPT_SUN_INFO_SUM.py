#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_RPT_SUN_INFO_SUM').setMaster(sys.argv[2])
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


#----------------------------------------------业务逻辑开始----------------------------------------------------------
#源表
ACRM_A_PROD_CHANNEL_INFO = sqlContext.read.parquet(hdfs+'/ACRM_A_PROD_CHANNEL_INFO/*')
ACRM_A_PROD_CHANNEL_INFO.registerTempTable("ACRM_A_PROD_CHANNEL_INFO")
#目标表：
#RPT_SUN_INFO_DETAIL 增改表 多个PY 非第一个
#处理过程：先删除运算目录所有数据文件，然后复制BK目录当天数据到运算目录，作为今天数据文件
ret = os.system("hdfs dfs -rm -r /"+dbname+"/RPT_SUN_INFO_DETAIL/*.parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/RPT_SUN_INFO_DETAIL_BK/"+V_DT+".parquet /"+dbname+"/RPT_SUN_INFO_DETAIL/"+V_DT+".parquet")

RPT_SUN_INFO_DETAIL = sqlContext.read.parquet(hdfs+'/RPT_SUN_INFO_DETAIL/*')
RPT_SUN_INFO_DETAIL.registerTempTable("RPT_SUN_INFO_DETAIL")

#任务[21] 001-01::
V_STEP = V_STEP + 1

#MERGE操作
sql="""
		select 
			 T1.ADDR     						--地址
			,count(T1.CUST_ID) as COUNT1    		--总户数
			,(count(T1.CUST_ID)- sum(case when T1.CUST_ID LIKE '3%' then 1 else 0 end )) as COUNT2	--存量客户数
			,sum(case when T1.CUST_ID LIKE '3%' then 1 else 0 end ) as COUNT3	--潜在客户数
			,0 as COUNT4						--家庭预授信总户数
			,0 as COUNT5						--家庭签约授信总户数
			,0 as COUNT6						--家庭签约用信总户数
			,sum(case when T2.IS_ELEC='Y' then 1 else 0 end ) as COUNT7	 --电费签约总户数
			,sum(case when T2.IS_WATER='Y' then 1 else 0 end ) as COUNT8 --水费签约总户数
			,T1.FR_ID					--法人号
			,T1.ADDR_DESC				--详细地址
		from RPT_SUN_INFO_DETAIL T1
		left join ACRM_A_PROD_CHANNEL_INFO T2 on T1.CUST_ID = T2.CUST_ID  AND T1.FR_ID = T2.FR_ID
		group by T1.FR_ID,T1.ADDR,T1.ADDR_DESC
"""


sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
RPT_SUN_INFO_SUM = sqlContext.sql(sql)
dfn="RPT_SUN_INFO_DETAIL/"+V_DT+".parquet"
RPT_SUN_INFO_SUM.cache()
nrows = RPT_SUN_INFO_SUM.count()
RPT_SUN_INFO_SUM.write.save(path=hdfs + '/' + dfn, mode='overwrite')
RPT_SUN_INFO_SUM.unpersist()
#增改表保存后，复制当天数据进BK：先删除BK当天日期，然后复制
ret = os.system("hdfs dfs -rm -r /"+dbname+"/RPT_SUN_INFO_DETAIL_BK/"+V_DT+".parquet")
ret = os.system("hdfs dfs -cp -f /"+dbname+"/RPT_SUN_INFO_DETAIL/"+V_DT+".parquet /"+dbname+"/RPT_SUN_INFO_DETAIL_BK/"+V_DT+".parquet")
et = datetime.now()
print("Step %d start[%s] end[%s] use %d seconds") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds)

