#coding=UTF-8
from pyspark import SparkContext, SparkConf, SQLContext, Row, HiveContext
from pyspark.sql.types import *
from datetime import date, datetime, timedelta
import sys, re, os
import sys, re, os , calendar

st = datetime.now()
conf = SparkConf().setAppName('PROC_A_R_CONT_TOP').setMaster(sys.argv[2])
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
#本月月末日期（10位）
monthRange = calendar.monthrange(int(etl_date[0:3]),int(etl_date[4:6]))#得到本月的天数   
V_LAST_DAY = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(str(monthRange[1])))).strftime("%Y-%m-%d") 
#上月末日期
V_DT_LMD = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1) + timedelta(-1)).strftime("%Y%m%d")
#10位日期
V_DT10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), int(etl_date[6:8]))).strftime("%Y-%m-%d")
#上月末日期10
V_DT_LMD10 = (date(int(etl_date[0:4]), int(etl_date[4:6]), 1) + timedelta(-1)).strftime("%Y-%m-%d")
V_STEP = 0

#判断是否月末
if V_DT10 == V_LAST_DAY:

	ADMIN_AUTH_ORG = sqlContext.read.parquet(hdfs+'/ADMIN_AUTH_ORG/*')
	ADMIN_AUTH_ORG.registerTempTable("ADMIN_AUTH_ORG")
	ACRM_F_CI_CUST_CONTRIBUTION = sqlContext.read.parquet(hdfs+'/ACRM_F_CI_CUST_CONTRIBUTION/*')
	ACRM_F_CI_CUST_CONTRIBUTION.registerTempTable("ACRM_F_CI_CUST_CONTRIBUTION")

	#任务[21] 001-01::
	V_STEP = V_STEP + 1

	sql = """
	 SELECT CAST('000000' AS INTEGER)     AS ID 
		   ,T.ORG_ID                  AS ORG_ID 
		   ,T.ORG_NAME               AS ORG_NAME 
		   ,T.CUST_ID                 AS CUST_ID 
		   ,T.CUST_NAME               AS CUST_NAME 
		   ,T.CUST_CONTRIBUTION       AS CONTRIBUTION_CUST 
		   ,V_DT                AS ODS_DATE 
		   ,T.CUST_TYP              AS CUST_TYP 
	   FROM ( select row_number() over(PARTITION by b.org_id ,A.CUST_TYP order by COALESCE(a.CUST_CONTRIBUTION,0) desc ) as n  --根据机构分组，排序
			,b.ORG_ID                                --机构编号
			,b.ORG_NAME                              --机构名称
			,a.CUST_ID                               --客户编号
			,a.CUST_NAME                             --客户名称
			,A.CUST_TYP                              --客户类型
			,COALESCE(a.CUST_CONTRIBUTION,0) AS CUST_CONTRIBUTION        --客户贡献度
			from  ACRM_F_CI_CUST_CONTRIBUTION a
			inner join ADMIN_AUTH_ORG b on a.org_id=b.org_id
			 WHERE   A.CUST_ID <> 'X9999999999999999999'
			 AND A.ODS_DATE = V_DT_LMD
	   )
	   T
	   WHERE  T.n <=10
		  
							  --
	"""

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	sql = re.sub(r"\bV_DT_LMD\b", "'"+V_DT_LMD10+"'", sql)
	ACRM_A_CONT_TOP = sqlContext.sql(sql)
	ACRM_A_CONT_TOP.registerTempTable("ACRM_A_CONT_TOP")
	dfn="ACRM_A_CONT_TOP/"+V_DT+".parquet"
	ACRM_A_CONT_TOP.cache()
	nrows = ACRM_A_CONT_TOP.count()
	ACRM_A_CONT_TOP.write.save(path=hdfs + '/' + dfn, mode='overwrite')
	ACRM_A_CONT_TOP.unpersist()
	ret = os.system("hdfs dfs -rm -r /"+dbname+"/ACRM_A_CONT_TOP/"+V_DT_LD+".parquet")
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CONT_TOP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)

	#任务[11] 001-02::
	V_STEP = V_STEP + 1

	sql = """
	 SELECT CAST('000000' AS INTEGER)    AS ID 
		   ,BB.ORG_ID               AS ORG_ID 
		   ,BB.ORG_NAME             AS ORG_NAME 
		   ,AA.CUST_ID              AS CUST_ID 
		   ,AA.CUST_NAME            AS CUST_NAME 
		   ,AA.CONTRIBUTION_CUST    AS CONTRIBUTION_CUST 
		   ,V_DT                AS ODS_DATE 
		   ,AA.CUST_TYP             AS CUST_TYP 
	   FROM 
	   (SELECT            
							FR_ID                    --机构编号   
						   ,FR_NAME                  --机构名称   
						   ,CUST_ID                  --客户编号   
						   ,CUST_NAME                --客户名称
						   ,t.CUST_TYP                 --客户类型   
						   ,CONTRIBUTION_CUST        --客户贡献度   
					  FROM 
						   (
							 SELECT 
									  row_number() over(PARTITION by b.FR_ID,A.CUST_TYP order by sum(COALESCE(a.CUST_CONTRIBUTION,0)) desc ) as n  --根据法人分组，排序
									 ,b.FR_ID                                                --法人编号
									 ,b.FR_NAME                                              --法人名称
									 ,a.CUST_ID                                              --客户编号
									 ,a.CUST_NAME                                            --客户名称
									 ,a.CUST_TYP                                             --客户类型
									 ,sum(COALESCE(a.CUST_CONTRIBUTION,0)) as CONTRIBUTION_CUST          --客户贡献度
							   FROM ACRM_F_CI_CUST_CONTRIBUTION  A 
						 INNER JOIN ADMIN_AUTH_ORG b 
													 on a.org_id=b.org_id
							  WHERE A.CUST_ID <> 'X9999999999999999999'
								AND A.ODS_DATE = V_DT_LMD
						   GROUP BY  b.FR_ID                               --法人编号
									,b.FR_NAME                             --法人名称
									,a.CUST_ID                            --客户编号
									,A.CUST_NAME                           --客户名称  
									,a.CUST_TYP   
							  ) t
							   where t.n <=10  --前10条      
							   
					   )  AA
	  INNER JOIN ADMIN_AUTH_ORG BB                                 --
		 ON AA.FR_ID = BB.FR_ID 
		AND BB.UP_ORG_ID            = '320000000' """

	sql = re.sub(r"\bV_DT\b", "'"+V_DT10+"'", sql)
	sql = re.sub(r"\bV_DT_LMD\b", "'"+V_DT_LMD10+"'", sql)
	ACRM_A_CONT_TOP = sqlContext.sql(sql)
	ACRM_A_CONT_TOP.registerTempTable("ACRM_A_CONT_TOP")
	dfn="ACRM_A_CONT_TOP/"+V_DT+".parquet"
	ACRM_A_CONT_TOP.cache()
	nrows = ACRM_A_CONT_TOP.count()
	ACRM_A_CONT_TOP.write.save(path=hdfs + '/' + dfn, mode='append')
	ACRM_A_CONT_TOP.unpersist()
	ADMIN_AUTH_ORG.unpersist()
	ACRM_F_CI_CUST_CONTRIBUTION.unpersist()
	et = datetime.now()
	print("Step %d start[%s] end[%s] use %d seconds, insert ACRM_A_CONT_TOP lines %d") % (V_STEP, st.strftime("%H:%M:%S"), et.strftime("%H:%M:%S"), (et-st).seconds, nrows)
