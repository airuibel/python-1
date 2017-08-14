TRUNCATE table TEMP_ACCT_INFO IMMEDIATE;--
    insert into TEMP_ACCT_INFO
    select T2.SA_CUST_NO,
    max(case when (T2.SA_ACCT_CHAR='1100') then T2.SA_OPAC_DT end),
    max(case when (T2.SA_ACCT_CHAR<>'1100') then T2.SA_OPAC_DT end),
    sum(case when (T2.SA_ACCT_CHAR='1100' and T1.SA_TX_DT >= to_char(add_months(to_date(p_etl_date, 'yyyymmdd'),-6),'yyyymmdd')) then T1.SA_DR_AMT end),
    sum(case when (T2.SA_ACCT_CHAR<>'1100' and T1.SA_TX_DT >= to_char(add_months(to_date(p_etl_date, 'yyyymmdd'),-6),'yyyymmdd')) then T1.SA_DR_AMT end),
    sum(case when (T2.SA_ACCT_CHAR='1100' and T1.SA_TX_DT like substr(p_etl_date,1,6)||'%') then T1.SA_DR_AMT end),
    sum(case when (T2.SA_ACCT_CHAR<>'1100' and T1.SA_TX_DT like substr(p_etl_date,1,6)||'%') then T1.SA_DR_AMT end),
    --sum(case when (SA_RMRK LIKE '%水费%' or SA_RMRK LIKE '%电费%' OR SA_DSCRP_COD IN ('0701', '0702', '0021', '0022','1056','8121','1140')) and sa_dr_amt>0 then SA_TX_AMT end)
    sum(case when (SA_RMRK LIKE '%水费%' or SA_DSCRP_COD IN ('0021','0701','1056')) and sa_dr_amt>0 then SA_TX_AMT end),
    sum(case when (SA_RMRK LIKE '%电费%' or SA_DSCRP_COD IN ('0022','0702','1040','8141')) and sa_dr_amt>0 then SA_TX_AMT end),
    sum(case when (SA_RMRK LIKE '%工资%' or SA_DSCRP_COD IN ('0002', '0003', '0004', '1138')) and sa_dr_amt>0 then SA_TX_AMT end)
    from F_CORE_SAACNTXN T1 
    inner join F_CORE_SAACNACN T2 on (T1.FK_SAACN_KEY = T2.SA_ACCT_NO)
    where T1.ETL_DATE=p_etl_date 
    and exists (select null 
                from F_MIS_CUSTOMER_INFO CI 
                where CI.CustomerType like '01%' 
                and CI.MFCustomerID=T2.SA_CUST_NO 
                and CI.CRT_NO=V_ORG_ID) 
    and T1.CRT_NO=V_ORG_ID 
    and T2.CRT_NO=V_ORG_ID
    group by T2.SA_CUST_NO with ur ;--

    commit;--
    SET v_point = '9902';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);        --

    --账户最近360天单日最大资金流出量
    TRUNCATE table TEMP_ACCT_INFO_01 IMMEDIATE;--
    
    insert into TEMP_ACCT_INFO_01
    select FS.SA_CUST_NO,LF.SA_TX_DT,sum(LF.SA_CR_AMT) 
    from F_CORE_SAACNTXN LF 
    inner join F_CORE_SAACNACN FS on (LF.FK_SAACN_KEY=FS.SA_ACCT_NO)
    where LF.SA_TX_DT >= to_char(add_months(to_date(p_etl_date, 'yyyymmdd'),-12),'yyyymmdd')
    and exists (select null 
                from F_MIS_CUSTOMER_INFO CI 
                where CI.CustomerType like '01%' 
                and CI.MFCustomerID=FS.SA_CUST_NO 
                and CI.CRT_NO=V_ORG_ID) 
    and LF.ETL_DATE=p_etl_date 
    and LF.CRT_NO=V_ORG_ID 
    and FS.CRT_NO=V_ORG_ID
    group by FS.SA_CUST_NO,LF.SA_TX_DT with ur;--

    commit;--
    SET v_point = '9903';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);     --

    TRUNCATE table TEMP_ACCT_INFO_04 IMMEDIATE;--
    
    insert into TEMP_ACCT_INFO_04
    select T2.SA_CUST_NO,sum(T1.SA_ACCT_BAL) 
    from F_CORE_SAACNAMT T1 
    inner join F_CORE_SAACNACN T2 on (T1.FK_SAACN_KEY = T2.SA_ACCT_NO)
    where T1.ETL_DATE=p_etl_date 
    and T1.CRT_NO=V_ORG_ID 
    and T2.CRT_NO=V_ORG_ID
    and exists (select null 
                from F_MIS_CUSTOMER_INFO CI 
                where CI.CustomerType like '01%' 
                and CI.MFCustomerID=T2.SA_CUST_NO 
                and CI.CRT_NO=V_ORG_ID) 
    group by T2.SA_CUST_NO with ur ;--

    commit;--
    SET v_point = '9905';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
        

    TRUNCATE table TEMP_ACCT_INFO_09 IMMEDIATE;--
    
    insert into TEMP_ACCT_INFO_09
    select T1.CUST_ID,max(T1.SA_CR_AMT) 
    from TEMP_ACCT_INFO_01 T1
    group by T1.CUST_ID with ur ;--

    commit;--
    SET v_point = '9910';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);      --

    --清空当月的增量数据
    delete from RWMS_ACCT_INFO 
    where MON=substr(p_etl_date,1,6)  
    and CUST_ID in (select SA_CUST_NO 
                    from F_CORE_SAACNACN T1 
                    where T1.ETL_DATE=p_etl_date 
                    and T1.CRT_NO=V_ORG_ID) with ur;--

    commit;--
    SET v_point = '9911';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);   --
    
    --插入增量数据
    INSERT INTO RWMS_ACCT_INFO
    (
    CORE_CUST_ID,            --核心客户号
    INPUTDATE,               --数据日期
    MON,                     --月份  
    CUST_ID,                 --客户号
    BFMY_OPEN_ACCT_DT,       --基本户开户日期
    NON_BFMY_OPEN_ACCT_DT,   --非基本户开户日期
    DPSIT_BAL,               --存款余额
    RCNT_6MON_BFMY_CNT_AMOT, --最近6个月基本户结算量
    RCNT_6MON_NON_BFMY_CNT_AMOT, --最近6个月非基本户结算量
    MON_BFMY_CNT_AMOT,           --本月基本户结算量
    MON_NON_BFMY_CNT_AMOT,       --本月非基本户结算量
    RCNT_360DAY_MAX_CAP_FLOW,    --账户最近360天单日最大资金流出量
    WATER_COST,                  --水费
    ELECTRIC_COST,               --电费
    AGENT_DISTR_SALRY_AMT,       --代发工资
    UPDATEDATE,                  --ETL更新时间
    CRT_NO
    )
    SELECT 
    distinct T1.SA_CUST_NO,
    to_date(p_etl_date, 'yyyymmdd'),
    substr(p_etl_date,1,6),
    T1.SA_CUST_NO,
    T.col2,
    T.col3,
    T4.SA_ACCT_BAL,
    T.col5,
    T.col6,
    T.col7,
    T.col8,
    T9.SA_CR_AMT,
    T.col9,
    T.col10,
    T.col11,
    to_date(p_etl_date, 'yyyymmdd'),
    T1.CRT_NO
    from F_CORE_SAACNACN T1
    left join TEMP_ACCT_INFO T on (T1.SA_CUST_NO=T.CORE_CUST_ID)
    left join TEMP_ACCT_INFO_04 T4 on (T1.SA_CUST_NO=T4.CORE_CUST_ID)
    left join TEMP_ACCT_INFO_09 T9 on (T1.SA_CUST_NO=T9.CORE_CUST_ID)
    where T1.ETL_DATE=p_etl_date 
    and T1.CRT_NO=V_ORG_ID
    and exists (select null 
                from F_MIS_CUSTOMER_INFO CI 
                where CI.CustomerType like '01%' 
                and CI.MFCustomerID=T1.SA_CUST_NO 
                and CI.CRT_NO=V_ORG_ID) 
    with ur;--
    commit;--

    

SET V_INT=V_INT+1;--
end WHILE;--
close cursor_orgid;--

    -- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--

END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@


CREATE PROCEDURE "RWUSER"."PRO_M_APPLY_INFO" (IN p_etl_date VARCHAR(20),out
result_code integer) 
/******************************************************/
/***     程序名称 ：模型层-申请信息表              ***/
/***     创建日期 ：2015-06-21                      ***/
/***     创建人员 ：xnhu                            ***/
/***     功能描述 ：模型层申请信息表抽取           ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150621151318900
BEGIN
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值

    DECLARE V_ORG_ID VARCHAR(32);
    DECLARE V_INT INT DEFAULT 0;
    DECLARE V_INT1 INT DEFAULT 0;
    DECLARE cursor_orgid CURSOR with hold FOR 
    select DISTINCT FR_ID from ADMIN_AUTH_ORG with ur;

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning';
        SET v_sql_code = substr(v_proc_stat,1,4);
        SET v_msg = v_proc_stat||' v_point:'||v_point;
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);
    END;
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning';
        SET v_sql_code = substr(v_proc_stat,1,4);
        SET v_point  = '找不到视图';
        SET v_msg = v_proc_stat||' v_point:'||v_point;
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';
        SET result_code = 1;
        ROLLBACK;
        SET v_sql_code = substr(v_proc_stat,1,11);
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);
    END;

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';      -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_APPLY_INFO';              -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

    SET v_point = '9901';
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);
    
    open cursor_orgid;
    select count(DISTINCT FR_ID) into V_INT1 from ADMIN_AUTH_ORG with ur;
    while V_INT < V_INT1 do
    fetch cursor_orgid into V_ORG_ID;


    --删除当天的数据
    delete from RWMS_APPLY_INFO 
    where SEQ_ID in (select BA.SERIALNO 
                     from F_MIS_BUSINESS_APPLY BA 
                     where ETL_DATE=p_etl_date 
                     and CRT_NO=V_ORG_ID) with ur;
    commit;


    --插入增量数据     
    INSERT INTO RWMS_APPLY_INFO     
    (INPUTDATE, --数据日期     
    HAPP_DT, --发生日期     
    CUST_ID, --客户编号     
    BIZ_BREED, --业务品种     
    HAPP_TYPE, --发生类型     
    CUR_TYPE, --币种     
    PROMIS_LOAN_AMT, --承贷金额     
    CENTRL_GUAR_MODE, --主要担保方式     
    SEQ_ID,   --流水号
    EXPOS_RATIO,--敞口比例
    FLOW_RESULT, --审批状态
    FLOW_DATE, --审批时间
    CRT_NO
    )
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,    
    BA.OCCURDATE,     
    BA.CUSTOMERID,    
    BA.BUSINESSTYPE,     	
    BA.OCCURTYPE,     
    BA.BUSINESSCURRENCY,     
    BA.BUSINESSSUM,    
    BA.VOUCHTYPE,     
    BA.SERIALNO,
    1,
    FO.PhaseNo,
    FO.InputDate,
    CRT_NO
    from F_MIS_BUSINESS_APPLY BA 
    inner join F_MIS_FLOW_OBJECT FO on (BA.SERIALNO=FO.ObjectNo)
    where BA.ETL_DATE=p_etl_date 
    and CRT_NO=V_ORG_ID
    and FO.ObjectType='CreditApply'
    and exists (select 1 
                from F_MIS_CUSTOMER_INFO CI 
                where CI.CustomerType like '01%' 
                and CI.CustomerID=BA.CUSTOMERID 
                and CI.CRT_NO=V_ORG_ID) with ur;
    commit;  

    SET V_INT=V_INT+1;
    end WHILE;
    close cursor_orgid;

    -- 过程正常结束
    SET v_point = '9999';
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);
    SET result_code = 0;
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_CONTR_INFO" (IN p_etl_date VARCHAR(20),IN
V_ORG_ID VARCHAR(20),out
result_code integer) 
/******************************************************/
/***     程序名称 ：模型层-合同信息表              ***/
/***     创建日期 ：2015-06-21                      ***/
/***     创建人员 ：xnhu                            ***/
/***     功能描述 ：模型层合同信息表抽取           ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150621151628800
BEGIN
    DECLARE V_INI_FLAG VARCHAR(1);
    
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_CONTR_INFO';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    --删除当天的数据
    delete from RWMS_CONTR_INFO where CONTR_ID in (select BC.SERIALNO from F_MIS_BUSINESS_CONTRACT BC 
    where ETL_DATE=p_etl_date and CRT_NO=V_ORG_ID) and CRT_NO=V_ORG_ID;
    commit;

    select INI_FLAG into V_INI_FLAG from ETL_DATE_TODAY with ur;

    SET v_point = '9902';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    
    if(V_INI_FLAG='0') then
    --插入增量数据     
    INSERT INTO RWMS_CONTR_INFO     
    (
    BIZ_BREED, --业务品种                         
    CONTR_ID, --合同号             
    CRDT_AMT, --授信金额                         
    CUST_ID, --客户号                          
    GUAR_MODE,--担保方式             
    HAPP_TYPE,--发生类型                        
    INPUTDATE,--数据日期                         
    MATURE_DT,--到期日期                                    
    STAR_DT, --起始日期
    CUR_BAL, --当前余额
    IF_LRISK, --是否低风险
    EXPOS_BAL, --敞口余额
    TR_CONST_FLG, --额度有效标志
    OPER_ORG,
    OPERR,
    CRT_NO
    )
    SELECT     
    BC.BUSINESSTYPE,             
    BC.SERIALNO,             
    BC.BUSINESSSUM,            
    BC.CUSTOMERID,             
    case when BC.VOUCHTYPE='D' then '005' when BC.VOUCHTYPE='C' then '010' 
          when BC.VOUCHTYPE='C05' then '01010' when BC.VOUCHTYPE='C06' then '01020'
          when BC.VOUCHTYPE='C07' then '01030' when BC.VOUCHTYPE='C13' then '01040'
          when BC.VOUCHTYPE='B' then '020' when BC.VOUCHTYPE='B02' then '02010'
          when BC.VOUCHTYPE='B03' then '02020' when BC.VOUCHTYPE='B04' then '02030'
          when BC.VOUCHTYPE='B05' then '02040' when BC.VOUCHTYPE='B06' then '02050'
          when BC.VOUCHTYPE='B07' then '02060' when BC.VOUCHTYPE='B08' then '02072'
          when BC.VOUCHTYPE='B09' then '02074' when BC.VOUCHTYPE='B10' then '02080'
          when BC.VOUCHTYPE='B11' then '02083' when BC.VOUCHTYPE='B99' then '02085'
          when BC.VOUCHTYPE='A' then '040' when BC.VOUCHTYPE='A01' then '04005'
          when BC.VOUCHTYPE='A02' then '04007' when BC.VOUCHTYPE='A03' then '04010'
          when BC.VOUCHTYPE='A04' then '04015' when BC.VOUCHTYPE='A05' then '04020'
          when BC.VOUCHTYPE='A06' then '04030' when BC.VOUCHTYPE='A07' then '04035'
          when BC.VOUCHTYPE='A08' then '04040' when BC.VOUCHTYPE='A09' then '04050'
          when BC.VOUCHTYPE='A10' then '04060' when BC.VOUCHTYPE='A11' then '04070'
          when BC.VOUCHTYPE='A12' then '04075' when BC.VOUCHTYPE='A13' then '04080'
          when BC.VOUCHTYPE='A14' then '04085' when BC.VOUCHTYPE='A15' then '04090'
          when BC.VOUCHTYPE='H' then '050' when BC.VOUCHTYPE='0C01' then '060'
          when BC.VOUCHTYPE='G01' then '080' when BC.VOUCHTYPE='G02' then '090' ELSE BC.VOUCHTYPE  END,
    BC.OCCURTYPE,             
    to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,             
    BC.MATURITY,             
    BC.PUTOUTDATE,
    BC.BALANCE,
    '2',
    BC.BALANCE,
    BC.FLAG5,
    BC.MANAGEORGID,
    BC.MANAGEUSERID,
    BC.CRT_NO
    from F_MIS_BUSINESS_CONTRACT BC 
    inner join F_MIS_ENT_INFO EI on (EI.CUSTOMERID=BC.CUSTOMERID)
    where BC.ETL_DATE=p_etl_date and BC.CRT_NO=V_ORG_ID with ur
    ;
    commit;  
    else
    --初始化
    INSERT INTO RWMS_CONTR_INFO     
    (
    BIZ_BREED, --业务品种                         
    CONTR_ID, --合同号             
    CRDT_AMT, --授信金额                         
    CUST_ID, --客户号                          
    GUAR_MODE,--担保方式             
    HAPP_TYPE,--发生类型                        
    INPUTDATE,--数据日期                         
    MATURE_DT,--到期日期                                    
    STAR_DT, --起始日期
    CUR_BAL, --当前余额
    IF_LRISK, --是否低风险
    EXPOS_BAL, --敞口余额
    TR_CONST_FLG, --额度有效标志
    OPER_ORG,
    OPERR,
    CRT_NO
    )
    SELECT     
    BC.BUSINESSTYPE,             
    BC.SERIALNO,             
    BC.BUSINESSSUM,            
    BC.CUSTOMERID,             
    case when BC.VOUCHTYPE='D' then '005' when BC.VOUCHTYPE='C' then '010' 
          when BC.VOUCHTYPE='C05' then '01010' when BC.VOUCHTYPE='C06' then '01020'
          when BC.VOUCHTYPE='C07' then '01030' when BC.VOUCHTYPE='C13' then '01040'
          when BC.VOUCHTYPE='B' then '020' when BC.VOUCHTYPE='B02' then '02010'
          when BC.VOUCHTYPE='B03' then '02020' when BC.VOUCHTYPE='B04' then '02030'
          when BC.VOUCHTYPE='B05' then '02040' when BC.VOUCHTYPE='B06' then '02050'
          when BC.VOUCHTYPE='B07' then '02060' when BC.VOUCHTYPE='B08' then '02072'
          when BC.VOUCHTYPE='B09' then '02074' when BC.VOUCHTYPE='B10' then '02080'
          when BC.VOUCHTYPE='B11' then '02083' when BC.VOUCHTYPE='B99' then '02085'
          when BC.VOUCHTYPE='A' then '040' when BC.VOUCHTYPE='A01' then '04005'
          when BC.VOUCHTYPE='A02' then '04007' when BC.VOUCHTYPE='A03' then '04010'
          when BC.VOUCHTYPE='A04' then '04015' when BC.VOUCHTYPE='A05' then '04020'
          when BC.VOUCHTYPE='A06' then '04030' when BC.VOUCHTYPE='A07' then '04035'
          when BC.VOUCHTYPE='A08' then '04040' when BC.VOUCHTYPE='A09' then '04050'
          when BC.VOUCHTYPE='A10' then '04060' when BC.VOUCHTYPE='A11' then '04070'
          when BC.VOUCHTYPE='A12' then '04075' when BC.VOUCHTYPE='A13' then '04080'
          when BC.VOUCHTYPE='A14' then '04085' when BC.VOUCHTYPE='A15' then '04090'
          when BC.VOUCHTYPE='H' then '050' when BC.VOUCHTYPE='0C01' then '060'
          when BC.VOUCHTYPE='G01' then '080' when BC.VOUCHTYPE='G02' then '090' ELSE BC.VOUCHTYPE  END,
    BC.OCCURTYPE,             
    to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,             
    BC.MATURITY,             
    BC.PUTOUTDATE,
    BC.BALANCE,
    '2',
    BC.BALANCE,
    BC.FLAG5,
    BC.MANAGEORGID,
    BC.MANAGEUSERID,
    BC.CRT_NO
    from F_MIS_BUSINESS_CONTRACT BC 
    inner join F_MIS_ENT_INFO EI on (EI.CUSTOMERID=BC.CUSTOMERID)
    where BC.ETL_DATE=p_etl_date and BC.CRT_NO=V_ORG_ID and (BC.BALANCE is null or BC.BALANCE>0) with ur
    ;
    commit;  
    end if;

    -- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_CORP_CURT" (IN p_etl_date VARCHAR(20),out
result_code integer)
/******************************************************
程序名称 ：模型层-对公活期              
创建日期 ：2015-06-22                      
创建人员 ：jzhu                            
功能描述 ：模型层资金交易信息抽取           
******************************************************/
LANGUAGE SQL
SPECIFIC SQL150622153441000
BEGIN
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值

    DECLARE V_ORG_ID VARCHAR(32);--
    DECLARE V_INT INT DEFAULT 0;--
    DECLARE V_INT1 INT DEFAULT 0;--
    DECLARE cursor_orgid CURSOR with hold FOR 
    select DISTINCT FR_ID from ADMIN_AUTH_ORG with ur for read only;--

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK;--
        SET v_sql_code = substr(v_proc_stat,1,11);--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_CORP_CURT';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

    --插入到上月的交易总额指标
    if (substr(p_etl_date,7,2)='01') then
        insert into RWMS_CORP_CURT_M
           (core_cust_id,
            mon,
            amount)
        select 
        rcc.core_cust_id,
        substr(to_char(to_date(p_etl_date,'yyyymmdd')-1 day,'yyyymmdd'),1,6),
        sum(rcc.tx_amt)-(select sum(LN_TOTL_LN_AMT_HYPO_AMT) from F_CORE_LNLNSLNS where LN_CUST_NO=rcc.core_cust_id and LN_FRST_ALFD_DT_N like substr(to_char(to_date(p_etl_date,'yyyymmdd')-1 day,'yyyymmdd'),1,6)||'%')
        from RWMS_CORP_CURT rcc
        where rcc.tx_dt like substr(to_char(to_date(p_etl_date,'yyyymmdd')-1 day,'yyyymmdd'),1,6)||'%'
        group by rcc.core_cust_id with ur
        ;        --

        update RWMS_CORP_CURT_M set amount=0 where amount<0;
        commit;
    end if;--

    delete from F_CORE_SAACNTXN where ETL_DATE = to_char(to_date(p_etl_date,'yyyymmdd')-1 month,'yyyymmdd');
    commit;

    open cursor_orgid;--
    SELECT count(distinct FR_ID) 
    into V_INT1 
    from ADMIN_AUTH_ORG with ur;--
    



    --清空数据
    delete from  TEMP_CORP_CURT_03 ;--
    delete from  TEMP_CORP_CURT_01 ;--
    delete from  TEMP_CORP_CURT_02 ;--
    commit;

    while V_INT < V_INT1 do
    fetch cursor_orgid into V_ORG_ID;--

    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    INSERT INTO TEMP_CORP_CURT_03     
    (
    ACCT_ID,                 --帐号
    CUST_ACCT_ID_SSEQ_ID
    )     
    SELECT 
    T1.FK_SAACN_KEY,
    T1.SA_DDP_ACCT_NO_DET_N 
    from F_CORE_SAACNTXN T1 
    inner join F_CORE_SAACNACN T2 on (T1.FK_SAACN_KEY = T2.SA_ACCT_NO)
    where T1.ETL_DATE = p_etl_date 
    and T1.CRT_NO=V_ORG_ID 
    and T2.CRT_NO=V_ORG_ID
    and exists (select null 
                from F_MIS_CUSTOMER_INFO CI 
                where CI.CustomerType like '01%' 
                and CI.MFCustomerID=T2.SA_CUST_NO 
                and CI.CRT_NO=V_ORG_ID)
    with ur;--
    commit;--
    
    SET v_point = '9902';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    insert into TEMP_CORP_CURT_01 
    select T1.ACCT_ID,sum(T2.SA_ACCT_BAL)
    from TEMP_CORP_CURT_03 T1 
         left join F_CORE_SAACNAMT T2 on (T1.ACCT_ID = T2.FK_SAACN_KEY)
    where T2.CRT_NO=V_ORG_ID
    group by T1.ACCT_ID
    with ur;--

    commit;--
    SET v_point = '9903';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--


    insert into TEMP_CORP_CURT_02 
    select T1.ACCT_ID,T2.SA_FRZ_STS
    from TEMP_CORP_CURT_03 T1 
         inner join F_CORE_SAACNAMT T2 on (T1.ACCT_ID = T2.FK_SAACN_KEY)
    where T2.CRT_NO=V_ORG_ID 
    with ur
    ;--

    commit;--
    SET v_point = '9904';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    --清空数据
    delete from RWMS_CORP_CURT 
    where CRT_NO=V_ORG_ID 
    and exists (select null 
                from F_CORE_SAACNTXN T1 
                where ACCT_ID=T1.FK_SAACN_KEY 
                and CUST_ACCT_ID_SSEQ_ID=T1.SA_DDP_ACCT_NO_DET_N) with ur;--
    commit;--
    
    SET v_point = '9905';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    
    --插入全量数据     
    INSERT INTO RWMS_CORP_CURT     
    (             
    INPUTDATE              , --数据日期
    ACCT_ID                , --帐号
    CUST_ACCT_ID_SSEQ_ID,
    CUST_ACCT_ID           , --客户帐号
    DC_FLAG                , --借贷标记
    HOST_DT                , --主机日期
    OPP_ACCT_ID            , --对方帐号
    OPP_ACCT_NAME          , --对方户名
    TX_AMT                 , --交易金额
    TX_DT                  , --交易日期
    TX_TM                  , --交易时间
    MEMO                   , --摘要代码
    CORE_CUST_ID           , --核心客户号
    BIZ_CD_ID              , --业务代号
    CUST_ID                , --客户号
    ACCO_BAL               ,
    REC_STS,
    CRT_NO
    )     
    SELECT     
    to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,--数据日期
    LF.FK_SAACN_KEY,                            --账号
    LF.SA_DDP_ACCT_NO_DET_N,                    --
    LF.FK_SAACN_KEY,                            --客户账号
    case when LF.SA_DR_AMT>0 then '0' else '1' end,--借贷标记
    LF.SA_TX_DT,                                --主机日期
    LF.SA_OP_ACCT_NO_32,                        --对方账号
    LF.SA_OP_CUST_NAME,                         --对方户名
    LF.SA_TX_AMT,                               --交易金额
    LF.SA_TX_DT,                                --交易日期
    LF.SA_TX_TM,                                --交易时间
    LF.SA_DSCRP_COD,                            --摘要代码
    LS.SA_CUST_NO,                              --核心客户号
    '',                                         --业务代号
    LS.SA_CUST_NO,                              --客户号
    T1.SA_ACCT_BAL,
    T2.REC_STS,
    LF.CRT_NO
    from F_CORE_SAACNTXN LF 
    inner join F_CORE_SAACNACN LS on (LF.FK_SAACN_KEY=LS.SA_ACCT_NO)
    left join TEMP_CORP_CURT_01 T1 on (LF.FK_SAACN_KEY=T1.ACCT_ID)
    left join TEMP_CORP_CURT_02 T2 on (LF.FK_SAACN_KEY=T2.ACCT_ID)
    where LF.ETL_DATE = p_etl_date 
    and LF.CRT_NO=V_ORG_ID 
    and LS.CRT_NO=V_ORG_ID 
    and exists (select null 
                from F_MIS_CUSTOMER_INFO CI 
                where CI.CustomerType like '01%' 
                and CI.MFCustomerID=LS.SA_CUST_NO 
                and CI.CRT_NO=V_ORG_ID)
    with ur;--
    commit;--

SET V_INT=V_INT+1;--
end WHILE;--
close cursor_orgid;--

    /*
    --帐户余额
    update RWMS_CORP_CURT CC set CC.ACCO_BAL = (select LD.SA_ACCT_BAL from F_AGM_LNA_DESPOIT LD
                             where CC.ACCT_ID = LD.SA_ACCT_NO) 
    where INPUTDATE = to_date(p_etl_date, 'yyyymmdd')
    ;--
    commit;*/

    --记录状态 冻结-2 止付-3
    /*update RWMS_CORP_CURT CC set CC.REC_STS = (select case when SA_FRZ_STS='Y' then '2' when SA_STP_STS='Y' then '3' end from F_AGM_LNA_DESPOIT LD
                             where CC.ACCT_ID = LD.SA_ACCT_NO) 
    where INPUTDATE = to_date(p_etl_date, 'yyyymmdd')
    ;--
    commit;*/

    --过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@


CREATE PROCEDURE "RWUSER"."PRO_M_CUST_INFO" (IN p_etl_date VARCHAR(20),OUT
result_code INTEGER)

/******************************************************/
/***     程序名称 ：模型层-客户信息表                ***/
/***     创建日期 ：2015-06-15                      ***/
/***     创建人员 ：jzhu                            ***/
/***     功能描述 ：模型层客户信息表抽取             ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150720151238600
BEGIN   
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值

    DECLARE V_ORG_ID VARCHAR(32);--
    DECLARE V_INT INT DEFAULT 0;--
    DECLARE V_INT1 INT DEFAULT 0;--
    DECLARE cursor_orgid CURSOR with hold FOR 
    select DISTINCT FR_ID from ADMIN_AUTH_ORG with ur for read only;--

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_CUST_INFO';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

open cursor_orgid;--
SELECT count(distinct FR_ID) into V_INT1 from ADMIN_AUTH_ORG with ur ;--
while V_INT < V_INT1 do
  fetch cursor_orgid into V_ORG_ID;--

    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    
    MERGE INTO RWMS_CUST_INFO RCI
		USING
        (SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    CI.CUSTOMERID,
    CI.MFCUSTOMERID,
    EI.ENTERPRISENAME as CUSTOMERNAME,
    EI.LISTINGCORPORNOT,
    EI.ORGTYPE,
    EI.INDUSTRYTYPE,
    CI.MANAGERORGID,
    (case when EI.SCOPE='CS01' then '2' when EI.SCOPE='CS02' then '3' when EI.SCOPE='CS03' then '4' 
    when EI.SCOPE='CS04' then '5' when EI.SCOPE='CS05' then '9' else EI.SCOPE end) as ENT_SIZE,
    EI.LOANCARDNO,
    --EI.INDUSTRYTYPE,
    EI.CREDITLEVEL,
    --CI.MANAGERORGID,
    CI.MANAGERUSERID,
    EI.LICENSENO,
    CI.CERTID,
    (case when CI.CERTTYPE='105' then 'Ent01' when CI.CERTTYPE='2001' then 'Ent02' when CI.CERTTYPE='56' then 'Ent03' 
    when CI.CERTTYPE='0' then 'Ind01' when CI.CERTTYPE='1' then 'Ind02' when CI.CERTTYPE='3' then 'Ind04' 
    when CI.CERTTYPE='4' then 'Ind05' when CI.CERTTYPE='5' then 'Ind06' when CI.CERTTYPE='6' then 'Ind07' 
    when CI.CERTTYPE='7' then 'Ind08' when CI.CERTTYPE='8' then 'Ind09' when CI.CERTTYPE='9' then 'Ind10' 
    when CI.CERTTYPE='53' then 'Ind11' when CI.CERTTYPE='57' then 'Ind12' when CI.CERTTYPE='54' then 'Ind13' 
    when CI.CERTTYPE='55' then 'Ind14' when CI.CERTTYPE='X' then 'Ind15' else CI.CERTTYPE end) as CERTTYPE,
    --CI.CERTID,
    CI.CUSTOMERTYPE,
    --to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    --EI.ORGTYPE,
    EI.REGISTERCAPITAL,
    CI.CRT_NO,
    CI.SRC_DT,
    CI.ETL_DT,
     (case when EI.ORGNATURE='06' then '0101' when EI.ORGNATURE='07' then '0102' when EI.ORGNATURE='04' then '0103' 
    when EI.ORGNATURE='08' then '0104' when EI.ORGNATURE='09' then '0105' when EI.ORGNATURE='10' then '0106' 
    when EI.ORGNATURE='05' then '0199' else EI.ORGNATURE end) as Org_Nature,
    '0' as IF_GROUP_CUST,
    EI.FinanceBelong,
    case when EI.ENTERPRISENAME like '%担保%' then '1' else '2' end as IF_BON_GUAR_CORP
    from F_MIS_CUSTOMER_INFO CI 
    inner join F_MIS_ENT_INFO EI on (CI.CUSTOMERID=EI.CUSTOMERID)
    where CI.ETL_DATE=p_etl_date and CI.CRT_NO=V_ORG_ID and CI.CustomerType like '01%' and EI.IdentifyFlag is null
    and exists (select null from F_CORE_LNLNSLNS T 
    where T.LN_CUST_NO=CI.MFCUSTOMERID and T.CRT_NO=V_ORG_ID
    --有贷款余额
    and T.LN_LN_BAL>0 and (LN_APCL_FLG<>'Y' or LN_APCL_FLG is null))
union 
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    CI.CUSTOMERID,
    CI.MFCUSTOMERID,
    EI.ENTERPRISENAME as CUSTOMERNAME,
    EI.LISTINGCORPORNOT,
    EI.ORGTYPE,
    EI.INDUSTRYTYPE,
    CI.MANAGERORGID,
    (case when EI.SCOPE='CS01' then '2' when EI.SCOPE='CS02' then '3' when EI.SCOPE='CS03' then '4' 
    when EI.SCOPE='CS04' then '5' when EI.SCOPE='CS05' then '9' else EI.SCOPE end) as ENT_SIZE,
    EI.LOANCARDNO,
    --EI.INDUSTRYTYPE,
    EI.CREDITLEVEL,
    --CI.MANAGERORGID,
    CI.MANAGERUSERID,
    EI.LICENSENO,
    CI.CERTID,
    (case when CI.CERTTYPE='105' then 'Ent01' when CI.CERTTYPE='2001' then 'Ent02' when CI.CERTTYPE='56' then 'Ent03' 
    when CI.CERTTYPE='0' then 'Ind01' when CI.CERTTYPE='1' then 'Ind02' when CI.CERTTYPE='3' then 'Ind04' 
    when CI.CERTTYPE='4' then 'Ind05' when CI.CERTTYPE='5' then 'Ind06' when CI.CERTTYPE='6' then 'Ind07' 
    when CI.CERTTYPE='7' then 'Ind08' when CI.CERTTYPE='8' then 'Ind09' when CI.CERTTYPE='9' then 'Ind10' 
    when CI.CERTTYPE='53' then 'Ind11' when CI.CERTTYPE='57' then 'Ind12' when CI.CERTTYPE='54' then 'Ind13' 
    when CI.CERTTYPE='55' then 'Ind14' when CI.CERTTYPE='X' then 'Ind15' else CI.CERTTYPE end) as CERTTYPE,
    --CI.CERTID,
    CI.CUSTOMERTYPE,
    --to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    --EI.ORGTYPE,
    EI.REGISTERCAPITAL,
    CI.CRT_NO,
    CI.SRC_DT,
    CI.ETL_DT,
     (case when EI.ORGNATURE='06' then '0101' when EI.ORGNATURE='07' then '0102' when EI.ORGNATURE='04' then '0103' 
    when EI.ORGNATURE='08' then '0104' when EI.ORGNATURE='09' then '0105' when EI.ORGNATURE='10' then '0106' 
    when EI.ORGNATURE='05' then '0199' else EI.ORGNATURE end) as Org_Nature,
    '0' as IF_GROUP_CUST,
    EI.FinanceBelong,
    case when EI.ENTERPRISENAME like '%担保%' then '1' else '2' end as IF_BON_GUAR_CORP
    from F_MIS_CUSTOMER_INFO CI 
    inner join F_MIS_ENT_INFO EI on (CI.CUSTOMERID=EI.CUSTOMERID)
    where CI.ETL_DATE=p_etl_date and CI.CRT_NO=V_ORG_ID and CI.CustomerType like '01%' and EI.IdentifyFlag is null
    and exists (select null from F_MIS_BUSINESS_CONTRACT BC where BC.CUSTOMERID=CI.CUSTOMERID and BC.CRT_NO=V_ORG_ID
    --有有效授信
    and BC.FLAG5='1000' and BC.BUSINESSTYPE like '3010%')
union 
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    CI.CUSTOMERID,
    CI.MFCUSTOMERID,
    EI.ENTERPRISENAME as CUSTOMERNAME,
    EI.LISTINGCORPORNOT,
    EI.ORGTYPE,
    EI.INDUSTRYTYPE,
    CI.MANAGERORGID,
    (case when EI.SCOPE='CS01' then '2' when EI.SCOPE='CS02' then '3' when EI.SCOPE='CS03' then '4' 
    when EI.SCOPE='CS04' then '5' when EI.SCOPE='CS05' then '9' else EI.SCOPE end) as ENT_SIZE,
    EI.LOANCARDNO,
    --EI.INDUSTRYTYPE,
    EI.CREDITLEVEL,
    --CI.MANAGERORGID,
    CI.MANAGERUSERID,
    EI.LICENSENO,
    CI.CERTID,
    (case when CI.CERTTYPE='105' then 'Ent01' when CI.CERTTYPE='2001' then 'Ent02' when CI.CERTTYPE='56' then 'Ent03' 
    when CI.CERTTYPE='0' then 'Ind01' when CI.CERTTYPE='1' then 'Ind02' when CI.CERTTYPE='3' then 'Ind04' 
    when CI.CERTTYPE='4' then 'Ind05' when CI.CERTTYPE='5' then 'Ind06' when CI.CERTTYPE='6' then 'Ind07' 
    when CI.CERTTYPE='7' then 'Ind08' when CI.CERTTYPE='8' then 'Ind09' when CI.CERTTYPE='9' then 'Ind10' 
    when CI.CERTTYPE='53' then 'Ind11' when CI.CERTTYPE='57' then 'Ind12' when CI.CERTTYPE='54' then 'Ind13' 
    when CI.CERTTYPE='55' then 'Ind14' when CI.CERTTYPE='X' then 'Ind15' else CI.CERTTYPE end) as CERTTYPE,
    --CI.CERTID,
    CI.CUSTOMERTYPE,
    --to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    --EI.ORGTYPE,
    EI.REGISTERCAPITAL,
    CI.CRT_NO,
    CI.SRC_DT,
    CI.ETL_DT,
     (case when EI.ORGNATURE='06' then '0101' when EI.ORGNATURE='07' then '0102' when EI.ORGNATURE='04' then '0103' 
    when EI.ORGNATURE='08' then '0104' when EI.ORGNATURE='09' then '0105' when EI.ORGNATURE='10' then '0106' 
    when EI.ORGNATURE='05' then '0199' else EI.ORGNATURE end) as Org_Nature,
    '0' as IF_GROUP_CUST,
    EI.FinanceBelong,
    case when EI.ENTERPRISENAME like '%担保%' then '1' else '2' end as IF_BON_GUAR_CORP
    from F_MIS_CUSTOMER_INFO CI 
    inner join F_MIS_ENT_INFO EI on (CI.CUSTOMERID=EI.CUSTOMERID)
    where CI.ETL_DATE=p_etl_date and CI.CRT_NO=V_ORG_ID and EI.IdentifyFlag is null
    --在途业务
    and exists (select 1 from F_MIS_BUSINESS_APPLY BA inner join F_MIS_FLOW_OBJECT FO on (BA.SERIALNO=FO.OBJECTNO and FO.OBJECTTYPE='CreditApply') 
    where BA.CUSTOMERID=EI.CUSTOMERID and BA.CRT_NO=V_ORG_ID and FO.PHASENO not in ('1000','8000','0010','3000'))
union
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    CI.CUSTOMERID,
    CI.MFCUSTOMERID,
    EI.ENTERPRISENAME as CUSTOMERNAME,
    EI.LISTINGCORPORNOT,
    EI.ORGTYPE,
    EI.INDUSTRYTYPE,
    CI.MANAGERORGID,
    (case when EI.SCOPE='CS01' then '2' when EI.SCOPE='CS02' then '3' when EI.SCOPE='CS03' then '4' 
    when EI.SCOPE='CS04' then '5' when EI.SCOPE='CS05' then '9' else EI.SCOPE end) as ENT_SIZE,
    EI.LOANCARDNO,
    --EI.INDUSTRYTYPE,
    EI.CREDITLEVEL,
    --CI.MANAGERORGID,
    CI.MANAGERUSERID,
    EI.LICENSENO,
    CI.CERTID,
    (case when CI.CERTTYPE='105' then 'Ent01' when CI.CERTTYPE='2001' then 'Ent02' when CI.CERTTYPE='56' then 'Ent03' 
    when CI.CERTTYPE='0' then 'Ind01' when CI.CERTTYPE='1' then 'Ind02' when CI.CERTTYPE='3' then 'Ind04' 
    when CI.CERTTYPE='4' then 'Ind05' when CI.CERTTYPE='5' then 'Ind06' when CI.CERTTYPE='6' then 'Ind07' 
    when CI.CERTTYPE='7' then 'Ind08' when CI.CERTTYPE='8' then 'Ind09' when CI.CERTTYPE='9' then 'Ind10' 
    when CI.CERTTYPE='53' then 'Ind11' when CI.CERTTYPE='57' then 'Ind12' when CI.CERTTYPE='54' then 'Ind13' 
    when CI.CERTTYPE='55' then 'Ind14' when CI.CERTTYPE='X' then 'Ind15' else CI.CERTTYPE end) as CERTTYPE,
    --CI.CERTID,
    CI.CUSTOMERTYPE,
    --to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    --EI.ORGTYPE,
    EI.REGISTERCAPITAL,
    CI.CRT_NO,
    CI.SRC_DT,
    CI.ETL_DT,
     (case when EI.ORGNATURE='06' then '0101' when EI.ORGNATURE='07' then '0102' when EI.ORGNATURE='04' then '0103' 
    when EI.ORGNATURE='08' then '0104' when EI.ORGNATURE='09' then '0105' when EI.ORGNATURE='10' then '0106' 
    when EI.ORGNATURE='05' then '0199' else EI.ORGNATURE end) as Org_Nature,
    '0' as IF_GROUP_CUST,
    EI.FinanceBelong,
    case when EI.ENTERPRISENAME like '%担保%' then '1' else '2' end as IF_BON_GUAR_CORP
    from F_MIS_CUSTOMER_INFO CI 
    inner join F_MIS_ENT_INFO EI on (CI.CUSTOMERID=EI.CUSTOMERID)
    where CI.ETL_DATE=p_etl_date and CI.CRT_NO=V_ORG_ID and EI.IdentifyFlag is null
    --当天新增客户
    and EI.SRC_DT=p_etl_date
union
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    EI.CUSTOMERID,
    null as MFCUSTOMERID,
    EI.ENTERPRISENAME as CUSTOMERNAME,
    EI.LISTINGCORPORNOT,
    EI.ORGTYPE,
    EI.INDUSTRYTYPE,
    EI.InputOrgID,
    (case when EI.SCOPE='CS01' then '2' when EI.SCOPE='CS02' then '3' when EI.SCOPE='CS03' then '4' 
    when EI.SCOPE='CS04' then '5' when EI.SCOPE='CS05' then '9' else EI.SCOPE end) as ENT_SIZE,
    EI.LOANCARDNO,
    --EI.INDUSTRYTYPE,
    EI.CREDITLEVEL,
    --nvl(EI.INPUTORGID,CI.MANAGERORGID),
    nvl(EI.INPUTUSERID,CI.MANAGERUSERID) as MANAGERUSERID,
    EI.LICENSENO,
    null as CERTID,
    null as CERTTYPE,
    --null as CERTID,
    '02' as CUSTOMERTYPE,
    --to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    --EI.ORGTYPE,
    EI.REGISTERCAPITAL,
    EI.CRT_NO,
    EI.SRC_DT,
    EI.ETL_DT,
    '02' as Org_Nature,
    '0' as IF_GROUP_CUST,
    EI.FinanceBelong,
    case when EI.ENTERPRISENAME like '%担保%' then '1' else '2' end as IF_BON_GUAR_CORP
    from F_MIS_ENT_INFO EI 
    inner join F_MIS_CUSTOMER_INFO CI on (CI.CustomerID=EI.CustomerID)
    where CI.ETL_DATE=p_etl_date and EI.CRT_NO=V_ORG_ID and CI.CRT_NO=V_ORG_ID
    --集团客户
    and EI.IdentifyFlag = '2'
union
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    CI.CUSTOMERID,
    CI.MFCUSTOMERID,
    CI.CUSTOMERNAME,
    null as LISTINGCORPORNOT,
    null as ORGTYPE,
    null as INDUSTRYTYPE,
    CI.MANAGERORGID,
    null as ENT_SIZE,
    null as LOANCARDNO,
    --null as INDUSTRYTYPE,
    null as CREDITLEVEL,
    --CI.MANAGERORGID,
    CI.MANAGERUSERID,
    null as LICENSENO,
    CI.CERTID,
    (case when CI.CERTTYPE='105' then 'Ent01' when CI.CERTTYPE='2001' then 'Ent02' when CI.CERTTYPE='56' then 'Ent03' 
    when CI.CERTTYPE='0' then 'Ind01' when CI.CERTTYPE='1' then 'Ind02' when CI.CERTTYPE='3' then 'Ind04' 
    when CI.CERTTYPE='4' then 'Ind05' when CI.CERTTYPE='5' then 'Ind06' when CI.CERTTYPE='6' then 'Ind07' 
    when CI.CERTTYPE='7' then 'Ind08' when CI.CERTTYPE='8' then 'Ind09' when CI.CERTTYPE='9' then 'Ind10' 
    when CI.CERTTYPE='53' then 'Ind11' when CI.CERTTYPE='57' then 'Ind12' when CI.CERTTYPE='54' then 'Ind13' 
    when CI.CERTTYPE='55' then 'Ind14' when CI.CERTTYPE='X' then 'Ind15' else CI.CERTTYPE end) as CERTTYPE,
    --CI.CERTID,
    CI.CUSTOMERTYPE,
    --to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    --null as ORGTYPE,
    null as REGISTERCAPITAL,
    CI.CRT_NO,
    CI.SRC_DT,
    CI.ETL_DT,
    '0302' as Org_Nature,
    '0' as IF_GROUP_CUST,
    null as FinanceBelong,
    '2' as IF_BON_GUAR_CORP
    from F_MIS_CUSTOMER_INFO CI 
    where CI.ETL_DATE=p_etl_date and CI.CRT_NO=V_ORG_ID and
    --个人经营性客户
    CI.CUSTOMERTYPE like '03%' and exists (select null from F_MIS_BUSINESS_CONTRACT BC 
    where BC.CUSTOMERID=CI.CUSTOMERID and BC.BALANCE>0 and BC.BUSINESSTYPE like '2050%' and BC.CRT_NO=V_ORG_ID)) B
            ON  RCI.CUST_ID = B.CUSTOMERID
           WHEN MATCHED THEN UPDATE SET (RCI.INPUTDATE,RCI.CORE_CUST_ID,RCI.CUST_NAME,RCI.IF_GO_PUB_CORP,RCI.ECON_ORG_COMPNT,RCI.INDUS_TYPE,RCI.MAIN_LINE,RCI.ENT_SIZE,RCI.LOAN_CARD_ID,RCI.NEW_INL_STD_INDUS_CLASS,RCI.CRDT_RATING_RESLT,RCI.BELONG_ORG,RCI.MANAGE_CUST_MGR,RCI.BIZ_LICS_ID,RCI.LP_ORG_CD,RCI.CERT_TYPE,RCI.CERT_ID,RCI.CUST_TYPE,RCI.UPDATEDATE,RCI.ORG_ECON_COMPNT,RCI.RGST_CAP,RCI.CRT_NO,RCI.SRC_DT,RCI.ETL_DT,RCI.ORG_NATURE,RCI.IF_GROUP_CUST,RCI.FinanceBelong,RCI.IF_BON_GUAR_CORP)
                                      = (B.Etl_Tx_Date,B.MFCUSTOMERID,B.CUSTOMERNAME,B.LISTINGCORPORNOT,B.ORGTYPE,B.INDUSTRYTYPE,B.MANAGERORGID,B.ENT_SIZE,B.LOANCARDNO,B.INDUSTRYTYPE,B.CREDITLEVEL,B.MANAGERORGID,B.MANAGERUSERID,B.LICENSENO,B.CERTID,B.CERTTYPE,B.CERTID,B.CUSTOMERTYPE,B.Etl_Tx_Date,B.ORGTYPE,B.REGISTERCAPITAL,B.CRT_NO,B.SRC_DT,B.ETL_DT,B.Org_Nature,B.IF_GROUP_CUST,B.FinanceBelong,B.IF_BON_GUAR_CORP)
           WHEN NOT MATCHED THEN 
           INSERT  
           (RCI.INPUTDATE,RCI.CUST_ID,RCI.CORE_CUST_ID,RCI.CUST_NAME,RCI.IF_GO_PUB_CORP,RCI.ECON_ORG_COMPNT,RCI.INDUS_TYPE,RCI.MAIN_LINE,RCI.ENT_SIZE,RCI.LOAN_CARD_ID,RCI.NEW_INL_STD_INDUS_CLASS,RCI.CRDT_RATING_RESLT,RCI.BELONG_ORG,RCI.MANAGE_CUST_MGR,RCI.BIZ_LICS_ID,RCI.LP_ORG_CD,RCI.CERT_TYPE,RCI.CERT_ID,RCI.CUST_TYPE,RCI.UPDATEDATE,RCI.ORG_ECON_COMPNT,RCI.RGST_CAP,RCI.CRT_NO,RCI.SRC_DT,RCI.ETL_DT,RCI.ORG_NATURE,RCI.IF_GROUP_CUST,RCI.FinanceBelong,RCI.IF_BON_GUAR_CORP) 
        VALUES
            (B.Etl_Tx_Date,B.CustomerID,B.MFCUSTOMERID,B.CUSTOMERNAME,B.LISTINGCORPORNOT,B.ORGTYPE,B.INDUSTRYTYPE,B.MANAGERORGID,B.ENT_SIZE,B.LOANCARDNO,B.INDUSTRYTYPE,B.CREDITLEVEL,B.MANAGERORGID,B.MANAGERUSERID,B.LICENSENO,B.CERTID,B.CERTTYPE,B.CERTID,B.CUSTOMERTYPE,B.Etl_Tx_Date,B.ORGTYPE,B.REGISTERCAPITAL,B.CRT_NO,B.SRC_DT,B.ETL_DT,B.Org_Nature,B.IF_GROUP_CUST,B.FinanceBelong,B.IF_BON_GUAR_CORP);
    
    SET v_point = '9903';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    --是否集团客户
    update RWMS_CUST_INFO CI set IF_GROUP_CUST = '1',ORG_NATURE='02'
       where CUST_TYPE like '02%' and ETL_DT=p_etl_date and crt_no=V_ORG_ID;--
    commit;--
/*
    SET v_point = '9904';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    update RWMS_CUST_INFO CI set IF_GROUP_CUST = '0'
       where IF_GROUP_CUST is null and ETL_DT=p_etl_date and crt_no=V_ORG_ID;--
    commit;--
*/
    SET v_point = '9905';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    --是否政府融资平台
    update RWMS_CUST_INFO CI set IF_GOV_PLATFM = '1'
       where exists (select 1 from F_MIS_BUSINESS_CONTRACT BC
                             where BC.CUSTOMERID = CI.CUST_ID and BC.ISONEQ = '1' and BC.BALANCE>0 and BC.CRT_NO=V_ORG_ID)
    and CUST_TYPE like '01%' and ETL_DT=p_etl_date and CRT_NO=V_ORG_ID;--
    commit;--

    SET v_point = '9906';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    --法定代表人
    update RWMS_CUST_INFO CI set LEGAL_REP = (select RT.RelativeID from F_MIS_CUSTOMER_RELATIVE RT
                             where RT.CustomerID = CI.CUST_ID and CI.CRT_NO=V_ORG_ID and RT.RelationShip = '0100' FETCH FIRST 1 ROWS ONLY) 
       where exists (select 1 from F_MIS_CUSTOMER_RELATIVE RT1
                             where RT1.CustomerID = CI.CUST_ID and CI.CRT_NO=V_ORG_ID and RT1.RelationShip = '0100')
    and CUST_TYPE like '01%' and ETL_DT=p_etl_date and CI.CRT_NO=V_ORG_ID;--
    commit;--

    SET v_point = '9907';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    --第一大控股股东
    update RWMS_CUST_INFO CI set LEGAL_REP = (select RT.RelativeID from F_MIS_CUSTOMER_RELATIVE RT
                             where RT.CustomerID = CI.CUST_ID and CI.CRT_NO=V_ORG_ID and RT.RelationShip like '02%' order by RT.RelativeID desc FETCH FIRST 1 ROWS ONLY) 
       where exists (select 1 from F_MIS_CUSTOMER_RELATIVE RT1 where 
                              RT1.CustomerID = CI.CUST_ID and CI.CRT_NO=V_ORG_ID and RT1.RelationShip like '02%')
    and CUST_TYPE like '01%' and ETL_DT=p_etl_date and CI.CRT_NO=V_ORG_ID;--

    commit;--
    SET v_point = '9908';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    --债项余额之和/敞口余额/授信总额/不良贷款总额
    TRUNCATE table TEMP_CUST_INFO_01 IMMEDIATE;--
    insert into TEMP_CUST_INFO_01
    select BD.CUSTOMERID,sum(T.LN_LN_BAL),sum(T.LN_LN_BAL),sum(T.LN_TOTL_LN_AMT_HYPO_AMT),
    sum(case when BD.CLASSIFYRESULT in ('03','04','05') then T.LN_LN_BAL else 0 end) 
    from F_CORE_LNLNSLNS T inner join F_MIS_BUSINESS_DUEBILL BD on (T.LN_LN_ACCT_NO=BD.SERIALNO)
    where T.LN_LN_BAL>0 and T.CRT_NO=V_ORG_ID and BD.CRT_NO=V_ORG_ID
    group by BD.CUSTOMERID
    ;--
    commit;--
    
    update RWMS_CUST_INFO CI set (DEBT_BAL_SUM,EXPOS_BAL,CRDT_TOTAL_AMT,BAD_LOAN_TOTAL_AMT) = 
    (select LN_LN_BAL1,LN_LN_BAL2,LN_TOTL_LN_AMT_HYPO_AMT,LN_LN_BAL3 
    from TEMP_CUST_INFO_01 
    where CUSTOMERID = CI.CUST_ID) where CI.CRT_NO=V_ORG_ID ;--

    commit;--
    SET v_point = '9909';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    TRUNCATE table TEMP_CUST_INFO_02 IMMEDIATE;--
    insert into TEMP_CUST_INFO_02
    SELECT ER.ObjectNo,ER.CognResult,ER.AccountMonth,
    ROW_NUMBER() OVER(PARTITION BY ER.ObjectNo ORDER BY ER.AccountMonth DESC ) RN
    FROM F_MIS_EVALUATE_RECORD ER inner join RWMS_CUST_INFO CI on (ER.OBJECTNO=CI.CUST_ID) 
    where ER.ObjectType='Customer' and CI.CRT_NO=V_ORG_ID;--
    commit;--
    
    --更新客户等级
    update RWMS_CUST_INFO CI set CI.CRDT_RATING_RESLT=(select ER.CognResult from TEMP_CUST_INFO_02 ER 
    where ER.ObjectNo=CI.CUST_ID and ER.RowNumber='1')
    where CI.CRT_NO=V_ORG_ID;--
    commit;--

    update RWMS_CUST_INFO CI set CI.LAST_RATING_RESLT=(select ER.CognResult from TEMP_CUST_INFO_02 ER 
    where ER.ObjectNo=CI.CUST_ID and ER.RowNumber='2')
    where CI.CRT_NO=V_ORG_ID;--
    commit;--
    
    update RWMS_CUST_INFO CI set CI.LAST_TWICE_RATING_RESLT=(select ER.CognResult from TEMP_CUST_INFO_02 ER 
    where ER.ObjectNo=CI.CUST_ID and ER.RowNumber='3')
    where CI.CRT_NO=V_ORG_ID;--
    commit;--

SET V_INT=V_INT+1;--
end WHILE;--
close cursor_orgid;--
   
/*
    SET v_point = '9910';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    
    --最近一次申请否决日期
    update RWMS_CUST_INFO CI set RCNT_ONC_APPLY_NON_DT = (select FO.INPUTDATE from F_MIS_BUSINESS_APPLY BA inner join F_MIS_FLOW_OBJECT FO 
                            on (BA.SERIALNO=FO.OBJECTNO and FO.OBJECTTYPE='CreditApply')
                             where BA.CUSTOMERID = CI.CUST_ID and FO.PHASENO='8000' order by FO.INPUTDATE desc FETCH FIRST 1 ROWS ONLY) 
       where exists (select 1 from F_MIS_BUSINESS_APPLY BA inner join F_MIS_FLOW_OBJECT FO 
                             on (BA.SERIALNO=FO.OBJECTNO and FO.OBJECTTYPE='CreditApply')
                             where BA.CUSTOMERID = CI.CUST_ID and FO.PHASENO='8000');--

    commit;--
    SET v_point = '9911';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    --最近一次申请通过日期
    update RWMS_CUST_INFO CI set RCNT_ONC_APPLY_NON_DT = (select FO.INPUTDATE from F_MIS_BUSINESS_APPLY BA inner join F_MIS_FLOW_OBJECT FO 
                            on (BA.SERIALNO=FO.OBJECTNO and FO.OBJECTTYPE='CreditApply')
                             where BA.CUSTOMERID = CI.CUST_ID and FO.PHASENO='1000' order by FO.INPUTDATE desc FETCH FIRST 1 ROWS ONLY) 
       where exists (select 1 from F_MIS_BUSINESS_APPLY BA inner join F_MIS_FLOW_OBJECT FO 
                             on (BA.SERIALNO=FO.OBJECTNO and FO.OBJECTTYPE='CreditApply')
                             where BA.CUSTOMERID = CI.CUST_ID and FO.PHASENO='1000');--
    commit;--
*/
    -- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_DUBIL_INFO" (IN p_etl_date VARCHAR(20),IN
V_ORG_ID VARCHAR(20),out result_code integer) LANGUAGE SQL
/******************************************************/
/***     程序名称 ：模型层-借据信息表              ***/
/***     创建日期 ：2015-06-21                      ***/
/***     创建人员 ：xnhu                            ***/
/***     功能描述 ：模型层借据信息表抽取           ***/
/******************************************************/
SPECIFIC SQL150621154454000
BEGIN
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值
    DECLARE V_INI_FLAG varchar(10); 

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_DUBIL_INFO';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    select INI_FLAG into V_INI_FLAG from ETL_DATE_TODAY with ur;

    delete from RWMS_DUBIL_INFO where DUBIL_ID in (select BD.LN_LN_ACCT_NO from F_CORE_LNLNSLNS BD 
    where ETL_DATE=p_etl_date and CRT_NO=V_ORG_ID) and CRT_NO=V_ORG_ID with ur;
    commit;

    if(V_INI_FLAG='0') then
    --插入全量数据     
    INSERT INTO RWMS_DUBIL_INFO     
    (       
    INPUTDATE,--数据日期 
    RELA_CONTR_SEQ_ID,--相关合同流水号
    CRDT_BREED, --授信品种                         
    DISTR_DT, --发放日期                                      
    CORE_CUST_ID, --核心客户号                            
    ADV_MONEY_FLAG, --垫款标志             
    FIVE_LVL_CLASS_RESLT,--五级分类结果                                                
    ACT_MATURE_DT,--实际到期日                                                
    OUT_ACCT_AMT,--金额  
    PAY_MODE,--支付方式                                                
    CUST_ID,--客户号                                                 
    DUBIL_BAL,--借据余额 
    DUBIL_ID,--借据号                                                
    LOAN_ACCT_ID,--贷款账号
    REPAY_ACCT_ID,--还款账号                                                
    OVDUE_TM,--逾期时间                                                
    PAYOFF_DT,  --结清日期
    CRT_NO
    )     
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,     
    T.RELATIVESERIALNO2,             
    T.BUSINESSTYPE,             
    BD.LN_FRST_ALFD_DT_N,            
    BD.LN_CUST_NO,
    T.ADVANCEFLAG,
    T.CLASSIFYRESULT,                         
    BD.LN_DUE_DT_N, 
    BD.LN_TOTL_LN_AMT_HYPO_AMT, 
    BD.LN_RFN_STY, 
    T.CUSTOMERID, 
    BD.LN_LN_BAL, 
    BD.LN_LN_ACCT_NO, 
    BD.LN_DEP_ACCT_NO,
    BD.LN_AUTO_DEDU_DEP_ACCT_NO_1,
    BD.LN_DLAY_LN_DT_N,
    BD.LN_CLSD_DT_N, 
    BD.CRT_NO
    from F_CORE_LNLNSLNS BD 
    inner join F_MIS_BUSINESS_DUEBILL T on (BD.LN_LN_ACCT_NO = T.SERIALNO)
    inner join F_MIS_ENT_INFO EI on (EI.CUSTOMERID=T.CUSTOMERID)
    where BD.ETL_DATE=p_etl_date and BD.CRT_NO=V_ORG_ID and T.CRT_NO=V_ORG_ID
    --and exists (select 1 from F_MIS_CUSTOMER_INFO CI where CI.CustomerType like '01%' and CI.MFCustomerID=T.CUSTOMERID and CI.CRT_NO=V_ORG_ID)
    with ur
    ;
    commit;
    else
    INSERT INTO RWMS_DUBIL_INFO     
    (       
    INPUTDATE,--数据日期 
    RELA_CONTR_SEQ_ID,--相关合同流水号
    CRDT_BREED, --授信品种                         
    DISTR_DT, --发放日期                                      
    CORE_CUST_ID, --核心客户号                            
    ADV_MONEY_FLAG, --垫款标志             
    FIVE_LVL_CLASS_RESLT,--五级分类结果                                                
    ACT_MATURE_DT,--实际到期日                                                
    OUT_ACCT_AMT,--金额  
    PAY_MODE,--支付方式                                                
    CUST_ID,--客户号                                                 
    DUBIL_BAL,--借据余额 
    DUBIL_ID,--借据号                                                
    LOAN_ACCT_ID,--贷款账号
    REPAY_ACCT_ID,--还款账号                                                
    OVDUE_TM,--逾期时间                                                
    PAYOFF_DT,  --结清日期
    CRT_NO
    )     
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,     
    T.RELATIVESERIALNO2,             
    T.BUSINESSTYPE,             
    BD.LN_FRST_ALFD_DT_N,            
    BD.LN_CUST_NO,
    T.ADVANCEFLAG,
    T.CLASSIFYRESULT,                         
    BD.LN_DUE_DT_N, 
    BD.LN_TOTL_LN_AMT_HYPO_AMT, 
    BD.LN_RFN_STY, 
    T.CUSTOMERID, 
    BD.LN_LN_BAL, 
    BD.LN_LN_ACCT_NO, 
    BD.LN_DEP_ACCT_NO,
    BD.LN_AUTO_DEDU_DEP_ACCT_NO_1,
    BD.LN_DLAY_LN_DT_N,
    BD.LN_CLSD_DT_N,
    BD.CRT_NO
    from F_CORE_LNLNSLNS BD 
    inner join F_MIS_BUSINESS_DUEBILL T on (BD.LN_LN_ACCT_NO = T.SERIALNO)
    inner join F_MIS_ENT_INFO EI on (EI.CUSTOMERID=T.CUSTOMERID)
    where BD.ETL_DATE=p_etl_date and BD.CRT_NO=V_ORG_ID and T.CRT_NO=V_ORG_ID and BD.LN_LN_BAL>0
    --and exists (select 1 from F_MIS_CUSTOMER_INFO CI where CI.CustomerType like '01%' and CI.MFCustomerID=T.CUSTOMERID and CI.CRT_NO=V_ORG_ID)
    with ur
    ;
    commit;
    end if;

    -- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--  
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_FIN_RPT_DATA_INDEX" (IN p_etl_date VARCHAR(20),IN
V_ORG_ID VARCHAR(20),out result_code integer)
/******************************************************/
/***     程序名称 ：模型层-财务报表数据指标           ***/
/***     创建日期 ：2015-06-15                      ***/
/***     创建人员 ：jzhu                            ***/
/***     功能描述 ：模型层财务报表数据指标抽取        ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150615162600900
BEGIN
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_FIN_RPT_DATA_INDEX';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    /*truncate table temp_fin_data_index immediate;*/
--If(V_ORG_ID in ('011','061')) then
    DELETE FROM RWMS_FIN_RPT_DATA_INDEX 
    WHERE (CUST_ID,RPT_TERM_ID,RPT_PRD_CD,RPT_CALI_CD) 
    IN (SELECT CUSTOMERID,REPORTDATE,REPORTPERIOD,REPORTSCOPE FROM RWMSUSER.CUSTOMER_FSRECORD WHERE ETL_DATE=p_etl_date and crt_no=V_ORG_ID)
    and crt_no=V_ORG_ID with ur;--
    COMMIT;--
    
    --插入增量数据
    INSERT INTO RWMS_FIN_RPT_DATA_INDEX
    (INPUTDATE                      ,--数据日期                        
    CUST_ID                        ,--客户编号                        
    RPT_TERM_ID                    ,--报表期次                        
    RPT_PRD_CD                     ,--报表周期代码                    
    RPT_CALI_CD                    ,--报表口径代码                    
    RPT_STS                        ,--报表状态                        
    AUDIT_FLAG                     ,--审计标志                        
    ASSET_LIAB_RATIO               ,--资产负债率                      
    CONTGT_LIAB_RATE               ,--或有负债比率                    
    OPER_CASH_FLOW_QTTY            ,--经营性现金流入量                
    NET_CASH_FLOW_QTTY             ,--净现金流量                      
    INT_GUAR_MULTI                 ,--EBIT利息保障倍数                
    MON_CASH_FLOW_QTTY             ,--考核月的现金流量                
    LIQD_RATE                      ,--流动比率                        
    QCK_RATIO                      ,--速动比率                        
    QCK_RATIO_W_R_TRADE            ,--速动比率（批发零售业）          
    QCK_RATIO_OTH_INDUS            ,--速动比率（其他行业）            
    AUDIT_UNIT                     ,--审计单位                        
    AUDIT_OPIN                     ,--审计意见                        
    OWNER_RIGHT_INTER_SUM          ,--所有者权益合计                  
    LONG_SHR_RIGHT_INVEST          ,--长期股权投资                    
    LONG_CLAIM_INVEST              ,--长期债权投资                    
    INVISIBLE_ASSET                ,--无形资产                        
    ASSET_SUM                      ,--资产合计                        
    NET_MARG                       ,--净利润                          
    SUM_ASSET_PRFT_RATE            ,--总资产收益率                    
    NET_ASSET_PRFT_RATE            ,--净资产收益率                    
    CAP_PRFT_RATE                  ,--资本收益率                      
    MAIN_BIZ_INCOME_GROW_RAT       ,--主营收入增长率                  
    NET_MARG_GROW_RAT              ,--净利润增长率                    
    MARG_TOTAL_AMT_GROW_RAT        ,--利润总额增长率                  
    SUM_ASSET_PAYBACK              ,--总资产报酬率                    
    MARG_TOTAL_AMT                 ,--利润总额                        
    MAIN_BIZ_BIZ_INCOME            ,--主营业务收入                    
    MAIN_BIZ_BIZ_INCOME_CASH_RATE  ,--主营业务收入现金率              
    INVTRY_TURN_DAY_W_R_TRADE      ,--存货周转天数（批发业、零售业）  
    INVTRY_TURN_DAY_OTH            ,--存货周转天数（其他行业）        
    OTH_COLL_MONEY                 ,--其他应收款                      
    LIQD_ASSET                     ,--流动资产                        
    OTH_ACCT_PAYA                  ,--其他应付款                      
    LIQD_LIAB                      ,--流动负债                        
    RECVBL_DEBT                    ,--应收账款                        
    SUM_ASSET                      ,--总资产                          
    RECVBL_DEBT_TURN_DAY_W_R_TRADE ,--应收账款周转天数（批发零售业）  
    RECVBL_DEBT_TURN_DAY_OTH       ,--应收账款周转天数（其他行业）    
    FIX_ASSET                      ,--固定资产                        
    CUR_CAP                        ,--货币资金                        
    INVTRY                         ,--存货                            
    SUM_LIAB                       ,--总负债                          
    SHORT_TM_LOAN                  ,--短期借款                        
    PAYBL_BILL                     ,--应付票据                        
    LONG_LOAN                      ,--长期借款                        
    NET_ASSET_SUM                  ,--净资产合计                      
    SELL_INCOME                    ,--销售收入                        
    OPER_CASH                      ,--经营活动净现金流                
    INVEST_CASH                    ,--投资活动净现金流                
    FUNDRS_CASH                    ,--筹资活动净现金流                
    RECVBL_DEBT_TURNOV_RAT         ,--应收账款周转率                  
    INVTRY_TURNOV_RAT              ,--存货周转率                      
    SELL_INT_RATE                  ,--销售毛利率                      
    HOLD_TO_MATURE_INVEST          ,--持有至到期投资                                    
    TX_FIN_ASSET                   ,--交易性金融资产                  
    COLL_BILL                      ,--应收票据                        
    COLL_COMP_MONEY                ,--应收代偿款                      
    ENTR_LOAN                      ,--委托贷款                        
    OUT_GUAR_MARGIN                ,--存出担保保证金                  
    PAYBL_SHARE_PRVSN              ,--应付分担保账款                  
    PAYBL_COMM_FEE_COMM            ,--应付手续费及佣金                
    SAVE_GUAR_MARGIN               ,--存入担保保证金                  
    GUAR_COMP_PREP                 ,--担保赔偿准备                    
    NOT_MATURE_PREP                ,--未到期责任准备                  
    LONG_LIAB_SUM                  ,--长期负债合计                    
    RECV_CAP                       ,--实收资本                        
    CAP_SPLS                       ,--资本公积                        
    COMN_RISK_PREP                 ,--一般风险准备                    
    GUAR_SPT_FUND                  ,--担保扶持基金                    
    GUAR_BIZ_INCOME                ,--担保业务收入                    
    GUAR_BIZ_MARG                  ,--担保业务利润                    
    BIZ_COST                       ,--营业费用                        
    BIZ_MARG                       ,--营业利润                        
    MAIN_BIZ_MARG_RATE             ,--主营业务利润率                  
    NET_MARG_RATE,                  --净利润率       
    CRT_NO
    )
    select to_date(p_etl_date, 'yyyymmdd') as Etl_Tx_Date, 
    T1.CUSTOMERID,
    T1.REPORTDATE,
    T1.REPORTPERIOD,
    T1.REPORTSCOPE,
    T1.REPORTSTATUS,
    T1.AUDITFLAG,
    T2.ASSET_LIAB_RATIO ,
    T2.CONTGT_LIAB_RATE ,
    T2.OPER_CASH_FLOW_QTTY ,
    T2.NET_CASH_FLOW_QTTY ,
    T2.INT_GUAR_MULTI ,
    T2.MON_CASH_FLOW_QTTY ,
    T2.LIQD_RATE ,
    T2.QCK_RATIO ,
    T2.QCK_RATIO_W_R_TRADE ,
    T2.QCK_RATIO_OTH_INDUS ,
    T1.AUDITOFFICE ,
    null ,
    T2.OWNER_RIGHT_INTER_SUM ,
    T2.LONG_SHR_RIGHT_INVEST ,
    T2.LONG_CLAIM_INVEST ,
    T2.INVISIBLE_ASSET ,
    T2.ASSET_SUM ,
    T2.NET_MARG ,
    T2.SUM_ASSET_PRFT_RATE ,
    T2.NET_ASSET_PRFT_RATE ,
    T2.CAP_PRFT_RATE ,
    T2.MAIN_BIZ_INCOME_GROW_RAT ,
    T2.NET_MARG_GROW_RAT ,
    T2.MARG_TOTAL_AMT_GROW_RAT ,
    T2.SUM_ASSET_PAYBACK ,
    T2.MARG_TOTAL_AMT ,
    T2.MAIN_BIZ_BIZ_INCOME ,
    T2.MAIN_BIZ_BIZ_INCOME_CASH_RATE ,
    T2.INVTRY_TURN_DAY_W_R_TRADE ,
    T2.INVTRY_TURN_DAY_OTH ,
    T2.OTH_COLL_MONEY ,
    T2.LIQD_ASSET ,
    T2.OTH_ACCT_PAYA ,
    T2.LIQD_LIAB ,
    T2.RECVBL_DEBT ,
    T2.SUM_ASSET ,
    T2.RECVBL_DEBT_TURN_DAY_W_R_TRADE ,
    T2.RECVBL_DEBT_TURN_DAY_OTH ,
    T2.FIX_ASSET ,
    T2.CUR_CAP ,
    T2.INVTRY ,
    T2.SUM_LIAB ,
    T2.SHORT_TM_LOAN ,
    T2.PAYBL_BILL ,
    T2.LONG_LOAN ,
    T2.NET_ASSET_SUM ,
    T2.SELL_INCOME ,
    T2.OPER_CASH ,
    T2.INVEST_CASH ,
    T2.FUNDRS_CASH ,
    T2.RECVBL_DEBT_TURNOV_RAT ,
    T2.INVTRY_TURNOV_RAT ,
    T2.SELL_INT_RATE ,
    T2.HOLD_TO_MATURE_INVEST ,
    T2.TX_FIN_ASSET ,
    T2.COLL_BILL ,
    T2.COLL_COMP_MONEY ,
    T2.ENTR_LOAN ,
    T2.OUT_GUAR_MARGIN ,
    T2.PAYBL_SHARE_PRVSN ,
    T2.PAYBL_COMM_FEE_COMM ,
    T2.SAVE_GUAR_MARGIN ,
    T2.GUAR_COMP_PREP ,
    T2.NOT_MATURE_PREP ,
    T2.LONG_LIAB_SUM ,
    T2.RECV_CAP ,
    T2.CAP_SPLS ,
    T2.COMN_RISK_PREP ,
    T2.GUAR_SPT_FUND ,
    T2.GUAR_BIZ_INCOME ,
    T2.GUAR_BIZ_MARG ,
    T2.BIZ_COST ,
    T2.BIZ_MARG ,
    T2.MAIN_BIZ_MARG_RATE ,
    T2.NET_MARG_RATE,
    T1.CRT_NO
    from (select CUSTOMERID,REPORTDATE,REPORTPERIOD,REPORTSCOPE,max(REPORTSTATUS) as REPORTSTATUS,max(AUDITFLAG) as AUDITFLAG,
   max(AUDITOFFICE) as AUDITOFFICE,max(CRT_NO) as CRT_NO,max(InputDate) as InputDate 
   from rwmsuser.CUSTOMER_FSRECORD 
   where InputDate>'20131231' and ReportStatus in ('02','03') and CRT_NO=V_ORG_ID
   and customerid not like 'PI%' and CUSTOMERID in (select CustomerID from F_MIS_ENT_INFO)
   group by CUSTOMERID,REPORTDATE,REPORTPERIOD,REPORTSCOPE) T1 inner JOIN (
    SELECT CF.CUSTOMERID,CF.REPORTDATE,CF.REPORTPERIOD,CF.REPORTSCOPE,
    max(CASE WHEN RD.ROWSUBJECT in ('612') THEN RD.COL2VALUE END) AS ASSET_LIAB_RATIO ,                --资产负债率                      
    max(CASE WHEN RD.ROWSUBJECT in ('922') THEN RD.COL2VALUE END) AS CONTGT_LIAB_RATE ,                --或有负债比率                    
    max(CASE WHEN RD.ROWSUBJECT in ('406') and REPORTNAME='现金流量表(自动生成)' THEN RD.COL2VALUE END) AS OPER_CASH_FLOW_QTTY ,             --经营性现金流入量                
    max(CASE WHEN RD.ROWSUBJECT in ('418') and REPORTNAME='现金流量表(自动生成)' THEN RD.COL2VALUE END) AS NET_CASH_FLOW_QTTY ,              --净现金流量                      
    max(CASE WHEN RD.ROWSUBJECT in ('610') THEN RD.COL2VALUE END) AS INT_GUAR_MULTI ,                  --EBIT利息保障倍数                
    max(CASE WHEN RD.ROWSUBJECT in ('905') THEN RD.COL2VALUE END) AS MON_CASH_FLOW_QTTY ,              --考核月的现金流量                
    max(CASE WHEN RD.ROWSUBJECT in ('600') THEN RD.COL2VALUE END) AS LIQD_RATE ,                       --流动比率                        
    max(CASE WHEN RD.ROWSUBJECT in ('602') THEN RD.COL2VALUE END) AS QCK_RATIO ,                       --速动比率                        
    max(CASE WHEN RD.ROWSUBJECT in ('602') THEN RD.COL2VALUE END) AS QCK_RATIO_W_R_TRADE ,             --速动比率（批发零售业）          
    max(CASE WHEN RD.ROWSUBJECT in ('602') THEN RD.COL2VALUE END) AS QCK_RATIO_OTH_INDUS ,             --速动比率（其他行业）                                   
    max(CASE WHEN RD.ROWSUBJECT in ('266') THEN RD.COL2VALUE END) AS OWNER_RIGHT_INTER_SUM ,           --所有者权益合计                  
    max(CASE WHEN RD.ROWSUBJECT in ('133') THEN RD.COL2VALUE END) AS LONG_SHR_RIGHT_INVEST ,           --长期股权投资                    
    max(CASE WHEN RD.ROWSUBJECT in ('134') THEN RD.COL2VALUE END) AS LONG_CLAIM_INVEST ,               --长期债权投资                    
    max(CASE WHEN RD.ROWSUBJECT in ('149') THEN RD.COL2VALUE END) AS INVISIBLE_ASSET ,                 --无形资产                        
    max(CASE WHEN RD.ROWSUBJECT in ('165') THEN RD.COL2VALUE END) AS ASSET_SUM ,                       --资产合计                        
    max(CASE WHEN RD.ROWSUBJECT in ('333') THEN RD.COL2VALUE END) AS NET_MARG ,                        --净利润                          
    max(CASE WHEN RD.ROWSUBJECT in ('646') THEN RD.COL2VALUE END) AS SUM_ASSET_PRFT_RATE ,             --总资产收益率                    
    max(CASE WHEN RD.ROWSUBJECT in ('648') THEN RD.COL2VALUE END) AS NET_ASSET_PRFT_RATE ,             --净资产收益率                    
    0.000000 AS CAP_PRFT_RATE ,                   --资本收益率                      
    max(CASE WHEN RD.ROWSUBJECT in ('908') THEN RD.COL2VALUE END) AS MAIN_BIZ_INCOME_GROW_RAT ,        --主营收入增长率                  
    max(CASE WHEN RD.ROWSUBJECT in ('918') THEN RD.COL2VALUE END) AS NET_MARG_GROW_RAT ,               --净利润增长率                    
    0.000000 AS MARG_TOTAL_AMT_GROW_RAT ,         --利润总额增长率                  
    max(CASE WHEN RD.ROWSUBJECT in ('935') THEN RD.COL2VALUE END) AS SUM_ASSET_PAYBACK ,               --总资产报酬率                    
    max(CASE WHEN RD.ROWSUBJECT in ('329') THEN RD.COL2VALUE END) AS MARG_TOTAL_AMT ,                  --利润总额                        
    max(CASE WHEN RD.ROWSUBJECT in ('301') THEN RD.COL2VALUE END) AS MAIN_BIZ_BIZ_INCOME ,             --主营业务收入                    
    max(CASE WHEN RD.ROWSUBJECT in ('933') THEN RD.COL2VALUE END) AS MAIN_BIZ_BIZ_INCOME_CASH_RATE ,   --主营业务收入现金率              
    0.000000 AS INVTRY_TURN_DAY_W_R_TRADE ,       --存货周转天数（批发业、零售业）  
    0.000000 AS INVTRY_TURN_DAY_OTH ,             --存货周转天数（其他行业）        
    max(CASE WHEN RD.ROWSUBJECT in ('115') THEN RD.COL2VALUE END) AS OTH_COLL_MONEY ,                  --其他应收款                      
    max(CASE WHEN RD.ROWSUBJECT in ('123') THEN RD.COL2VALUE END) AS LIQD_ASSET ,                      --流动资产                        
    max(CASE WHEN RD.ROWSUBJECT in ('218') THEN RD.COL2VALUE END) AS OTH_ACCT_PAYA ,                   --其他应付款                      
    max(CASE WHEN RD.ROWSUBJECT in ('224') THEN RD.COL2VALUE END) AS LIQD_LIAB ,                       --流动负债                        
    max(CASE WHEN RD.ROWSUBJECT in ('107') THEN RD.COL2VALUE END) AS RECVBL_DEBT ,                     --应收账款                        
    max(CASE WHEN RD.ROWSUBJECT in ('165') THEN RD.COL2VALUE END) AS SUM_ASSET ,                       --总资产                          
    0.0 AS RECVBL_DEBT_TURN_DAY_W_R_TRADE ,  --应收账款周转天数 
    0.0 AS RECVBL_DEBT_TURN_DAY_OTH ,        --应收账款周转天数（其他行业）    
    max(CASE WHEN ((RD.ROWSUBJECT in ('137') and RD.RowNo in ('0190')) or (RD.ROWSUBJECT in ('154') and RD.RowNo in ('0380'))) THEN RD.COL2VALUE END) AS FIX_ASSET ,                       --固定资产                        
    max(CASE WHEN RD.ROWSUBJECT in ('101') THEN RD.COL2VALUE END) AS CUR_CAP ,                         --货币资金                        
    max(CASE WHEN RD.ROWSUBJECT in ('117') THEN RD.COL2VALUE END) AS INVTRY ,                          --存货                            
    max(CASE WHEN RD.ROWSUBJECT in ('246') THEN RD.COL2VALUE END) AS SUM_LIAB ,                        --总负债                          
    max(CASE WHEN RD.ROWSUBJECT in ('200') THEN RD.COL2VALUE END) AS SHORT_TM_LOAN ,                   --短期借款                        
    max(CASE WHEN RD.ROWSUBJECT in ('204') THEN RD.COL2VALUE END) AS PAYBL_BILL ,                      --应付票据                        
    max(CASE WHEN RD.ROWSUBJECT in ('228') THEN RD.COL2VALUE END) AS LONG_LOAN ,                       --长期借款                        
    max(CASE WHEN RD.ROWSUBJECT in ('290') THEN RD.COL2VALUE END) AS NET_ASSET_SUM ,                   --净资产合计                      
    max(CASE WHEN RD.ROWSUBJECT in ('400') and REPORTNAME='现金流量表(自动生成)' THEN RD.COL2VALUE END) AS SELL_INCOME ,                     --销售收入                        
    max(CASE WHEN RD.ROWSUBJECT in ('418') and REPORTNAME='现金流量表(自动生成)' THEN RD.COL2VALUE END) AS OPER_CASH ,                       --经营活动净现金流                
    max(CASE WHEN RD.ROWSUBJECT in ('442') and REPORTNAME='现金流量表(自动生成)' THEN RD.COL2VALUE END) AS INVEST_CASH ,                     --投资活动净现金流                
    max(CASE WHEN RD.ROWSUBJECT in ('460') and REPORTNAME='现金流量表(自动生成)' THEN RD.COL2VALUE END) AS FUNDRS_CASH ,                     --筹资活动净现金流                
    max(CASE WHEN RD.ROWSUBJECT in ('650') THEN RD.COL2VALUE END) AS RECVBL_DEBT_TURNOV_RAT ,          --应收账款周转率                  
    max(CASE WHEN RD.ROWSUBJECT in ('652') THEN RD.COL2VALUE END) AS INVTRY_TURNOV_RAT ,               --存货周转率                      
    max(CASE WHEN RD.ROWSUBJECT in ('636') THEN RD.COL2VALUE END) AS SELL_INT_RATE ,                   --销售毛利率                      
    max(CASE WHEN RD.ROWSUBJECT in ('129') THEN RD.COL2VALUE END) AS HOLD_TO_MATURE_INVEST ,           --持有至到期投资                                   
    max(CASE WHEN RD.ROWSUBJECT in ('103') THEN RD.COL2VALUE END) AS TX_FIN_ASSET ,                    --交易性金融资产                  
    max(CASE WHEN RD.ROWSUBJECT in ('105') THEN RD.COL2VALUE END) AS COLL_BILL ,                       --应收票据                        
    0.000000 AS COLL_COMP_MONEY ,                 --应收代偿款                      
    0.000000 AS ENTR_LOAN ,                       --委托贷款                        
    0.000000 AS OUT_GUAR_MARGIN ,                 --存出担保保证金                  
    0.000000 AS PAYBL_SHARE_PRVSN ,               --应付分担保账款                  
    0.000000 AS PAYBL_COMM_FEE_COMM ,             --应付手续费及佣金                
    0.000000 AS SAVE_GUAR_MARGIN ,                --存入担保保证金                  
    0.000000 AS GUAR_COMP_PREP ,                  --担保赔偿准备                    
    0.000000 AS NOT_MATURE_PREP ,                 --未到期责任准备                  
    max(CASE WHEN RD.ROWSUBJECT in ('237') THEN RD.COL2VALUE END) AS LONG_LIAB_SUM ,                   --长期负债合计                    
    max(CASE WHEN RD.ROWSUBJECT in ('250') THEN RD.COL2VALUE END) AS RECV_CAP ,                        --实收资本                        
    max(CASE WHEN RD.ROWSUBJECT in ('252') THEN RD.COL2VALUE END) AS CAP_SPLS ,                        --资本公积                        
    max(CASE WHEN RD.ROWSUBJECT in ('670') THEN RD.COL2VALUE END) AS COMN_RISK_PREP ,                  --一般风险准备                    
    0.000000 AS GUAR_SPT_FUND ,                   --担保扶持基金                    
    0.000000 AS GUAR_BIZ_INCOME ,                 --担保业务收入                    
    0.000000 AS GUAR_BIZ_MARG ,                   --担保业务利润                    
    max(CASE WHEN RD.ROWSUBJECT in ('314') THEN RD.COL2VALUE END) AS BIZ_COST ,                        --营业费用                        
    max(CASE WHEN RD.ROWSUBJECT in ('321') THEN RD.COL2VALUE END) AS BIZ_MARG ,                        --营业利润                        
    max(CASE WHEN RD.ROWSUBJECT in ('308') THEN RD.COL2VALUE END) AS MAIN_BIZ_MARG_RATE ,              --主营业务利润率                  
    max(CASE WHEN RD.ROWSUBJECT in ('915') THEN RD.COL2VALUE END) AS NET_MARG_RATE                     --净利润率 
    FROM rwmsuser.REPORT_DATA RD 
    inner join rwmsuser.REPORT_RECORD RR on (RR.REPORTNO = RD.REPORTNO)
    inner join rwmsuser.CUSTOMER_FSRECORD CF on (CF.RECORDNO = RR.OBJECTNO)
    inner join F_MIS_ENT_INFO EI on (EI.CUSTOMERID=CF.CUSTOMERID)
    where CF.ETL_DATE = p_etl_date and CF.ReportStatus in ('02','03')
    and CF.CRT_NO=V_ORG_ID and RR.CRT_NO=V_ORG_ID and EI.CRT_NO=V_ORG_ID
    /*and T1.CUSTOMERID in (select CustomerID from F_MIS_CUSTOMER_INFO where crt_no in ('011','061'))*/
    GROUP BY CF.CUSTOMERID,CF.REPORTDATE,CF.REPORTPERIOD,CF.REPORTSCOPE 
    ) T2 ON (T1.CUSTOMERID=T2.CUSTOMERID and T1.REPORTDATE=T2.REPORTDATE and T1.REPORTPERIOD=T2.REPORTPERIOD and T1.REPORTSCOPE=T2.REPORTSCOPE)
    WHERE T1.InputDate>'20131231' and T1.ReportStatus in ('02','03') --and T2.ASSET_SUM is not null
    and T1.CRT_NO=V_ORG_ID
    and T1.customerid not like 'PI%' and T1.CUSTOMERID in (select CustomerID from F_MIS_ENT_INFO)
    with ur;--
    commit;--
--end if;--
    

    -- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_GUAR_INFO" (IN p_etl_date VARCHAR(20),IN
V_ORG_ID VARCHAR(20),out result_code integer)
/******************************************************/
/***     程序名称 ：模型层-担保信息表                ***/
/***     创建日期 ：2015-06-19                      ***/
/***     创建人员 ：xnhu                            ***/
/***     功能描述 ：模型层担保信息表抽取             ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150619154453600
BEGIN   
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_GUAR_INFO';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    delete from RWMS_GUAR_INFO where GUAR_CONTR_ID in (select SERIALNO from F_MIS_GUARANTY_CONTRACT
    where ETL_DATE=p_etl_date and CRT_NO=V_ORG_ID) and CRT_NO=V_ORG_ID;

    commit;
    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    --插入全量数据
    INSERT INTO RWMS_GUAR_INFO
    (INPUTDATE, --数据日期
    GUAR_CONTR_ID, --担保合同号
    REL_CONTR_ID,--关联合同号
    GUAR_AMT, --担保金额
    GUAR_CONTR_MATURE_DT, --担保合同到期日
    GUAR_CONTR_TYPE, --担保合同类型
    GUAR_CONTR_STS, --担保合同状态
    Guar_Antor_ID,--借款人
    Customer_ID,--担保人
    GUARANTOR_NAME,
    CERT_TYPE,
    CERT_ID,
    CRT_NO
    )
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    GC.SERIALNO,
    CR.SERIALNO,
    GC.GUARANTYVALUE,
    GC.ENDDATE,
    GC.GUARANTYTYPE,
    GC.CONTRACTSTATUS,
    GC.CustomerID,
    GC.GuarantorID,
    GC.GUARANTORNAME,
    (case when GC.CERTTYPE='105' then 'Ent01' when GC.CERTTYPE='2001' then 'Ent02' when GC.CERTTYPE='56' then 'Ent03' 
    when GC.CERTTYPE='0' then 'Ind01' when GC.CERTTYPE='1' then 'Ind02' when GC.CERTTYPE='3' then 'Ind04' 
    when GC.CERTTYPE='4' then 'Ind05' when GC.CERTTYPE='5' then 'Ind06' when GC.CERTTYPE='6' then 'Ind07' 
    when GC.CERTTYPE='7' then 'Ind08' when GC.CERTTYPE='8' then 'Ind09' when GC.CERTTYPE='9' then 'Ind10' 
    when GC.CERTTYPE='53' then 'Ind11' when GC.CERTTYPE='57' then 'Ind12' when GC.CERTTYPE='54' then 'Ind13' 
    when GC.CERTTYPE='55' then 'Ind14' when GC.CERTTYPE='X' then 'Ind15' else GC.CERTTYPE end) as CERTTYPE,
    GC.CERTID,
    GC.CRT_NO
    from F_MIS_GUARANTY_CONTRACT GC 
    inner join F_MIS_CONTRACT_RELATIVE CR on (GC.SERIALNO=CR.OBJECTNO)
    inner join F_MIS_ENT_INFO EI on (GC.CustomerID=EI.CustomerID)
    where GC.ETL_DATE=p_etl_date and CR.OBJECTTYPE='GuarantyContract' 
    and GC.ContractStatus='020' and GC.CRT_NO=V_ORG_ID 
    with ur;
    commit;

    -- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_GUARTY_INFO" (IN p_etl_date VARCHAR(20),IN
V_ORG_ID VARCHAR(20),out result_code integer)
/******************************************************/
/***     程序名称 ：模型层-担保品信息表              ***/
/***     创建日期 ：2015-06-19                      ***/
/***     创建人员 ：xnhu                            ***/
/***     功能描述 ：模型层担保品信息表抽取           ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150619145521100
BEGIN
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_GUARTY_INFO';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

--If(V_ORG_ID in ('011','061')) then
    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    delete from RWMS_GUARTY_INFO where GUARTY_ID in (select GI.GUARANTYID from F_MIS_GUARANTY_INFO GI 
    where ETL_DATE=p_etl_date and CRT_NO=V_ORG_ID) with ur;--

    commit;--
    SET v_point = '9902';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    
    --插入全量数据
    INSERT INTO RWMS_GUARTY_INFO
    (INPUTDATE, --数据日期
    GUARTY_ID, --担保物编号
    GUARTY_TYPE, --担保物类型
    GUARTY_STS, --担保物状态
    ATTR_ID, --权属人编号
    INTO_WHS_TM, --入库时间
    GUAR_CONTR_ID, --担保合同号
    OBJ_TYPE,--对象类型
    PLDG_IMPA_CONTR_VAL, --抵质押品合同价值
    PLDG_IMPA_RATIO, --抵质押率
    PLDG_IMPA_CFM_VAL, --抵质押品贷时确认价值
    obj_id,
    GUARANTY_RIGHTID, -- 权证号
    OTHER_GUARANTY_RIGHT, --他项权证号
    GUARANTY_NAME, --担保物名称
    CRT_NO
    )
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    GI.GUARANTYID,
    GI.GUARANTYTYPE,
    GI.GUARANTYSTATUS,
    GI.OWNERID,
    T.LN_OBTN_RGHT_DT_N,
    GR.CONTRACTNO,
    GR.OBJECTTYPE,
    GI.CONFIRMVALUE,
    GI.GUARANTYRATE,
    GI.CONFIRMVALUE,
    GR.ObjectNo,
    GI.GUARANTYRIGHTID,
    GI.OTHERGUARANTYRIGHT,
    GI.GUARANTYNAME,
    GI.CRT_NO
    from F_MIS_GUARANTY_INFO GI 
    inner join F_CORE_LNWRRWRR T on (GI.GUARANTYID=T.LN_CLTRL_NO)
    inner join F_MIS_GUARANTY_RELATIVE GR on (GR.GUARANTYID=GI.GUARANTYID)
    where GI.ETL_DATE=p_etl_date and GR.OBJECTTYPE='BusinessContract' 
    and GI.CRT_NO=V_ORG_ID and T.CRT_NO=V_ORG_ID 
    and exists (select null from F_MIS_GUARANTY_CONTRACT GC where GR.CONTRACTNO=GC.SERIALNO and GC.CRT_NO=V_ORG_ID
    and exists (select null from F_MIS_ENT_INFO EI where GC.CUSTOMERID=EI.CUSTOMERID))
    with ur
    ;--
/*
    MERGE INTO RWMS_GUARTY_INFO RGI 
        USING (SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
        GI.GUARANTYID,
        GI.GUARANTYTYPE,
        GI.GUARANTYSTATUS,
        GI.OWNERID,
        T.LN_OBTN_RGHT_DT_N,
        GR.CONTRACTNO,
        GR.OBJECTTYPE,
        GI.CONFIRMVALUE,
        GI.GUARANTYRATE,
        GR.ObjectNo,
        GI.GUARANTYRIGHTID,
        GI.OTHERGUARANTYRIGHT,
        GI.GUARANTYNAME,
        GI.CRT_NO
        from F_MIS_GUARANTY_INFO GI 
        inner join F_CORE_LNWRRWRR T on (GI.GUARANTYID=T.LN_CLTRL_NO)
        inner join F_MIS_GUARANTY_RELATIVE GR on (GR.GUARANTYID=GI.GUARANTYID)
        where GI.ETL_DATE=p_etl_date and GR.OBJECTTYPE='BusinessContract' and GI.CRT_NO=V_ORG_ID and T.CRT_NO=V_ORG_ID
             ) T       
    ON T.CRT_NO=RGI.CRT_NO AND T.GUARANTYID=RGI.GUARTY_ID AND T.CRT_NO=V_ORG_ID
    WHEN MATCHED THEN UPDATE SET (RGI.INPUTDATE, --数据日期
    RGI.GUARTY_TYPE, --担保物类型
    RGI.GUARTY_STS, --担保物状态
    RGI.ATTR_ID, --权属人编号
    RGI.INTO_WHS_TM, --入库时间
    RGI.GUAR_CONTR_ID, --担保合同号
    RGI.OBJ_TYPE,--对象类型
    RGI.PLDG_IMPA_CONTR_VAL, --抵质押品合同价值
    RGI.PLDG_IMPA_RATIO, --抵质押率
    RGI.PLDG_IMPA_CFM_VAL, --抵质押品贷时确认价值
    RGI.obj_id,
    RGI.GUARANTY_RIGHTID, -- 权证号
    RGI.OTHER_GUARANTY_RIGHT, --他项权证号
    RGI.GUARANTY_NAME --担保物名称
    )
     =(T.Etl_Tx_Date,
    T.GUARANTYTYPE,
    T.GUARANTYSTATUS,
    T.OWNERID,
    T.LN_OBTN_RGHT_DT_N,
    T.CONTRACTNO,
    T.OBJECTTYPE,
    T.CONFIRMVALUE,
    T.GUARANTYRATE,
    T.CONFIRMVALUE,
    T.ObjectNo,
    T.GUARANTYRIGHTID,
    T.OTHERGUARANTYRIGHT,
    T.GUARANTYNAME)
    WHEN NOT MATCHED THEN INSERT(RGI.INPUTDATE, RGI.GUARTY_ID, RGI.GUARTY_TYPE, RGI.GUARTY_STS, RGI.ATTR_ID, RGI.INTO_WHS_TM, RGI.GUAR_CONTR_ID, RGI.OBJ_TYPE,RGI.PLDG_IMPA_CONTR_VAL, RGI.PLDG_IMPA_RATIO, RGI.PLDG_IMPA_CFM_VAL, RGI.obj_id,RGI.GUARANTY_RIGHTID, RGI.OTHER_GUARANTY_RIGHT, RGI.GUARANTY_NAME, RGI.CRT_NO)
    values (T.Etl_Tx_Date,T.GUARANTYID,T.GUARANTYTYPE,T.GUARANTYSTATUS,T.OWNERID,T.LN_OBTN_RGHT_DT_N,T.CONTRACTNO,T.OBJECTTYPE,T.CONFIRMVALUE,T.GUARANTYRATE,T.CONFIRMVALUE,T.ObjectNo,T.GUARANTYRIGHTID,T.OTHERGUARANTYRIGHT,T.GUARANTYNAME,T.CRT_NO)
    ;*/
    commit;--
    
--end if;--

    -- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_IC_BASIC_C" (IN p_etl_date VARCHAR(20),out
result_code integer)
/******************************************************/
/***     程序名称 ：模型层-企业照面信息表              ***/
/***     创建日期 ：2015-06-19                      ***/
/***     创建人员 ：xnhu                            ***/
/***     功能描述 ：模型层企业照面信息表抽取           ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150619155738400
BEGIN
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                        -- 数据日期date型
    DECLARE v_proc_stat varchar(100);              -- 存储过程执行状态
    DECLARE v_point varchar(10);                   -- 自定义断点变量
    DECLARE v_proc_name varchar(100);              -- 存储过程名称
    DECLARE v_sql_code  char(6);                   -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                    -- 日志信息内容：所有的返回信息值

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_IC_BASIC_C';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--   

    TRUNCATE table RWMS_IC_BASIC_C IMMEDIATE;
    --插入全量数据
    INSERT INTO RWMS_IC_BASIC_C
    (INPUTDATE, --数据日期
    RGST_ID, --注册号
    CUST_NAME, --企业名称
    LP_NAME, --法定代表人姓名
    RGST_CAP, --注册资本(万元)
    RGST_CAP_CUR_CD, --币种
    OPBUSI_DT, --开业日期YYYY-MM-DD
    OPER_STAR_DT, --经营期限自YYYY-MM-DD
    OPER_STOP_DT, --经营期限至YYYY-MM-DD
    CUST_TYPE_NAME, --企业(机构)类型
    CUST_OPER_STS_CD, --经营状态
    ACT_OFFICE_ADDR, --住址
    PERM_OPER_ITEM, --许可经营项目
    COMN_OPER_ITEM, --一般经营项目
    OPER_SCOPE, --经营(业务)范围
    OPER_SCOPE_MODE, --经营(业务)范围及方式
    RGST_ORG, --登记机关
    FINAL_YCHECK_YEAR, --最后年检年度YYYY
    INDUS_CD, --行业代码
    WRTOFF_DT --注销日期YYYY-MM-DD
    )
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    CI.REGNO,
    CI.ENTNAME,
    CI.FRNAME,
    CI.REGCAP,
    CI.REGCAPCUR,
    CI.ESDATE,
    CI.OPFROM,
    CI.OPTO,
    CI.ENTTYPE,
    CI.ENTSTATUS,
    CI.DOM,
    CI.ABUITEM,
    CI.CBUITEM,
    CI.OPSCOPE,
    CI.OPSCOANDFORM,
    CI.REGORG,
    CI.ANCHEYEAR,
    CI.INDUSTRYPHYCODE,
    CI.CANDATE
    from F_GS_ENTBASE CI where ETL_DATE=p_etl_date
    ;
    commit;

    -- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_IC_CLT_BREAK_INFO_C" (IN p_etl_date VARCHAR(20),out
result_code integer)

/******************************************************/
/***     程序名称 ：模型层-失信被执表              ***/
/***     创建日期 ：2015-06-19                      ***/
/***     创建人员 ：xnhu                            ***/
/***     功能描述 ：模型层失信被执表抽取           ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150619165921200
BEGIN 
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_IC_CLT_BREAK_INFO_C';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--  
    TRUNCATE table RWMS_IC_CLT_BREAK_INFO_C IMMEDIATE;
    --插入全量数据
    INSERT INTO RWMS_IC_CLT_BREAK_INFO_C
    (ID,
    CASE_NO, --案号
    BY_TRANSACTOR_NAME, --被执行人姓名
    GENDER, --性别
    AGE, --年龄
    IDTTY_IC_ID, --身份证件号/工商注册号
    ID_CARD_ADDRESS, --身份证原始发证地
    REGISTER_TM, --立案时间
    EXEC_COURT, --执行法院
    PROV, --省份
    CONC_CNT --关注次数
    )
    SELECT CBI.PBSEQ,
    CBI.CASECODE,
    CBI.INAMECLEAN,
    CBI.SEXYCLEAN,
    CBI.AGECLEAN,
    CBI.CARDNUM,
    CBI.YSFZD,
    CBI.REGDATECLEAN,
    CBI.COURTNAME,
    CBI.AREANAMECLEAN,
    CBI.FOCUSNUMBER
    from F_GS_PUNISHBREAK CBI where ETL_DATE=p_etl_date
    ;
    commit;
    -- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_IC_ENTINV_C" (IN p_etl_date VARCHAR(20),OUT
result_code integer)
/*************************************************************/
/***     程序名称 ：模型层-企业对外投资信息表                ***/
/***     创建日期 ：2015-06-21                             ***/
/***     创建人员 ：xnhu                                   ***/
/***     功能描述 ：模型层企业对外投资信息表抽取             ***/
/************************************************************/
LANGUAGE SQL
SPECIFIC SQL150621094657200
BEGIN   
-- 开始定义异常处理变量
DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
DECLARE v_data_dt date;                      -- 数据日期date型
DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
DECLARE v_point varchar(10);                 -- 自定义断点变量
DECLARE v_proc_name varchar(100);            -- 存储过程名称
DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值 

-- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_IC_ENTINV_C';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型
    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
   TRUNCATE table RWMS_IC_ENTINV_C IMMEDIATE;
    --插入全量数据
    INSERT INTO RWMS_IC_ENTINV_C
    (
      INPUTDATE,--数据日期
      SYS_MAKE_ID,--ID
      MAIN_RGST_ID ,--主注册号
      CUST_NAME,--企业(机构)名称
      RGST_ID,--注册号
      CUST_TYPE_NAME,--企业(机构)类型
      RGST_CAP,--注册资本(万元)
      RGST_CAP_CUR_CD,--币种
      CUST_OPER_STS_CD,--企业状态
      WRTOFF_DT,--注销日期YYYY-MM-DD
      RGST_ORG,--登记机关
      CONTRI_AMT,--认缴出资额(万元)
      CUR_CD,--币种
      HOLD_STCK_RATIO,--出资比例（如7.27%）
      OPBUSI_DT--开业日期YYYY-MM-DD
    )
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    CE.EISEQ,    
    F1.REGNO,   
    F2.ENTNAME,
    F2.REGNO,      
    F2.ENTTYPE,    
    F2.REGCAP,     
    F2.REGCAPCUR,  
    F2.ENTSTATUS,  
    F2.CANDATE,    
    F2.REGORG,     
    CE.SUBCONAM,   
    CE.CONGROCUR,   
    CE.FUNDEDRATIO,
    F2.ESDATE     
   from F_GS_ENTINV CE inner join F_GS_ENTBASE F1 on (CE.ENTSEQ1=F1.ENTSEQ) 
   inner join F_GS_ENTBASE F2 on (CE.ENTSEQ2=F2.ENTSEQ) 
   --where ETL_DATE=p_etl_date
    ;
    commit;

-- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_IC_LPINV_C" (IN p_etl_date VARCHAR(20),OUT
result_code integer)
/*************************************************************/
/***     程序名称 ：模型层-法定代表人对外投资表                ***/
/***     创建日期 ：2015-06-21                             ***/
/***     创建人员 ：xnhu                                   ***/
/***     功能描述 ：模型层法定代表人对外投资表抽取             ***/
/************************************************************/
LANGUAGE SQL
SPECIFIC SQL150621102103500
BEGIN  
-- 开始定义异常处理变量
DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
DECLARE v_data_dt date;                      -- 数据日期date型
DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
DECLARE v_point varchar(10);                 -- 自定义断点变量
DECLARE v_proc_name varchar(100);            -- 存储过程名称
DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值 

-- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_IC_LPINV_C';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型
    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);-- 

    TRUNCATE table RWMS_IC_LPINV_C IMMEDIATE;
    --插入全量数据
    INSERT INTO RWMS_IC_LPINV_C
    (
      INPUTDATE,--数据日期
      SYS_MAKE_ID,--ID
      MAIN_RGST_ID,--主注册号
      CUST_NAME,--企业(机构)名称
      RGST_ID,--注册号
      CUST_TYPE_NAME,--企业(机构)类型
      RGST_CAP,--注册资本(万元)
      RGST_CAP_CUR_CD,--币种
      CUST_OPER_STS_CD,--企业状态
      WRTOFF_DT,--注销日期YYYY-MM-DD
      RGST_ORG,--登记机关
      CONTRI_AMT,--认缴出资额(万元)
      CUR_CD,--币种
      HOLD_STCK_RATIO,--出资比例（如7.27%）
      OPBUSI_DT--开业日期YYYY-MM-DD
    )
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    CL.EFISEQ,    
    F1.REGNO,
    F2.ENTNAME,    
    F2.REGNO,      
    F2.ENTTYPE,    
    F2.REGCAP,     
    F2.REGCAPCUR,  
    F2.ENTSTATUS,  
    F2.CANDATE,    
    F2.REGORG,     
    CL.SUBCONAM,   
    CL.CURRENCY,   
    CL.FUNDEDRATIO,
    F1.ESDATE     
   from F_GS_ENTFRINV CL inner join F_GS_ENTBASE F1 on (F1.ENTSEQ=Cl.ENTSEQ1)
   inner join F_GS_ENTBASE F2 on (F2.ENTSEQ=Cl.ENTSEQ2)
    ;
    commit;

-- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_IC_MGR_C" (IN p_etl_date VARCHAR(20),OUT
result_code integer)

/******************************************************/
/***     程序名称 ：模型层-任职信息信息表 ***/
/***     创建日期 ：2015-06-21                      ***/
/***     创建人员 ：xxli1                            ***/
/***     功能描述 ：模型层任职信息信息表抽取 ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150621102939500
BEGIN   
-- 开始定义异常处理变量
DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
DECLARE v_data_dt date;                      -- 数据日期date型
DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
DECLARE v_point varchar(10);                 -- 自定义断点变量
DECLARE v_proc_name varchar(100);            -- 存储过程名称
DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值 

-- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_IC_MGR_C';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型
    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    TRUNCATE table RWMS_IC_MGR_C IMMEDIATE;
    --插入全量数据
    INSERT INTO RWMS_IC_MGR_C
    (
      INPUTDATE,    --数据日期      
     SYS_MAKE_ID,--
    MAIN_RGST_ID,
     MGR_NAME,     --人员姓名       
     POS         --职务            
    )
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    FC.EPSEQ,
    F1.REGNO,
    FC.PERNAME,
    FC.POSITION
    from F_GS_ENTPERSON FC inner join F_GS_ENTBASE F1 on (F1.ENTSEQ=FC.ENTSEQ)
    ;
    
    
    commit;

-- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_IC_SHAR_C" (IN p_etl_date VARCHAR(20),OUT
result_code integer)

/******************************************************/
/***     程序名称 ：模型层-股东及出资信息信息表 ***/
/***     创建日期 ：2015-06-21                      ***/
/***     创建人员 ：xxli1                            ***/
/***     功能描述 ：模型层股东及出资信息信息表抽取 ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150621105300900
BEGIN  
-- 开始定义异常处理变量
DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
DECLARE v_data_dt date;                      -- 数据日期date型
DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
DECLARE v_point varchar(10);                 -- 自定义断点变量
DECLARE v_proc_name varchar(100);            -- 存储过程名称
DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值 

-- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_IC_SHAR_C';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型
    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);-- 
    
    TRUNCATE table RWMS_IC_SHAR_C IMMEDIATE;
    --插入全量数据
    INSERT INTO RWMS_IC_SHAR_C
    (
   INPUTDATE,--数据日期
   SYS_MAKE_ID,
   MAIN_RGST_ID,
   SHAR_NAME,--股东名称
   CONTRI_AMT,--认缴出资额
   HOLD_STCK_RATIO,--出资比例（如7.27%）
   CUR_CD,--币种
   CONTRI_DT,--出资日期YYYY-MM-DD
   CTY_CD--国家
    )
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    FC.SHSEQ,
    F1.REGNO,
   FC.SHANAME,
   FC.SUBCONAM,
   FC.FUNDEDRATIO,
   FC.REGCAPCUR,
   FC.CONDATE,
   FC.COUNTRY
   from F_GS_SHAREHOLDER FC inner join F_GS_ENTBASE F1 on (F1.ENTSEQ=FC.ENTSEQ)
    ;
   commit;

-- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_OPP_LIST" (IN p_etl_date VARCHAR(20),out
result_code integer)
/******************************************************/
/***     程序名称 ：模型层-交易对手名单信息表 ***/
/***     创建日期 ：2015-06-21                      ***/
/***     创建人员 ：xxli1                            ***/
/***     功能描述 ：模型层交易对手名单信息表抽取 ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150621112649500
BEGIN   
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值

    DECLARE V_ORG_ID VARCHAR(32);
    DECLARE V_INT INT DEFAULT 0;
    DECLARE V_INT1 INT DEFAULT 0;
--    DECLARE cursor_orgid CURSOR with hold FOR 
--select DISTINCT FR_ID from ADMIN_AUTH_ORG with ur;

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_OPP_LIST';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

--open cursor_orgid;
--select count(DISTINCT FR_ID) into V_INT1 from ADMIN_AUTH_ORG with ur;
--while V_INT < V_INT1 do
--  fetch cursor_orgid into V_ORG_ID;

    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    --delete from RWMS_RWMS_OPP_LIST 
    --where SEQ_ID in (select TRANSACTIONKEY from F_T47_TRANSACTION FC where ETL_DATE=p_etl_date);
    TRUNCATE table RWMS_RWMS_OPP_LIST IMMEDIATE;

    commit;
    SET v_point = '9902';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    
    --插入增量数据
    INSERT INTO RWMS_RWMS_OPP_LIST
    (
   INPUTDATE,--数据日期
   CUST_NAME,--客户名称
   ACCT_ID,--账号
   OPP_CUST,--对方客户号
   OPP_NAME,--交易对手姓名
   UPDATEDATE,--ETL更新时间
   TX_DT,--交易日期
   SEQ_ID,--流水号
   ORIG_CUR_TYPE_AMT,--原币种金额
   CUST_ID,--客户号
   MAIN_CUST,--主客户号
   OPP_ACCT_ID --交易对手账号
    )
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
   FC.PARTY_CHN_NAME,
   FC.ACCT_NUM,
   FC.OPP_PARTY_ID,
   FC.OPP_NAME,
   to_date(FC.SRC_DT, 'yyyymmdd'),
   FC.SRC_DT,
   FC.TRANSACTIONKEY,
   FC.CNY_AMT,
   FC.PARTY_ID,
   FC.HOST_CUST_ID,
   FC.OPP_ACCT_NUM
    from F_T47_TRANSACTION FC
    inner join F_T47_TRANSACTION_UC T1 on (FC.CB_PK=T1.CB_PK)
    where FC.ETL_DATE=p_etl_date with ur
    ;
    commit;

--SET V_INT=V_INT+1;
--end WHILE;
--close cursor_orgid;

-- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_REL_TAB" (IN p_etl_date VARCHAR(20),out
result_code integer)

/******************************************************/
/***     程序名称 ：模型层-关联关系信息表 ***/
/***     创建日期 ：2015-06-21                      ***/
/***     创建人员 ：xxli1                            ***/
/***     功能描述 ：模型层关联关系信息表抽取 ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150621112205600
BEGIN   
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值
    
    DECLARE V_ORG_ID VARCHAR(32);
    DECLARE V_INT INT DEFAULT 0;
    DECLARE V_INT1 INT DEFAULT 0;
    DECLARE cursor_orgid CURSOR with hold FOR 
    select DISTINCT FR_ID from ADMIN_AUTH_ORG with ur for read only;

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_REL_TAB';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

open cursor_orgid;

    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

TRUNCATE table RWMS_REL_TAB IMMEDIATE;

SELECT count(distinct FR_ID) into V_INT1 from ADMIN_AUTH_ORG with ur ;
while V_INT < V_INT1 do
  fetch cursor_orgid into V_ORG_ID;

    --插入全量数据
    INSERT INTO RWMS_REL_TAB
    (
   INPUTDATE,--数据日期
   CUST_ID,--客户ID
   REL_PERS_ID,--关联人ID
   REL_PERS_NAME,--关联人名称
   REL_REL,--关联关系
   CERT_TYPE,--证件类型
   CERT_ID,--证件号码
   CRT_NO,
   INVEST_SUM,
   INVEST_RATIO
    )
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
   CUSTOMERID,
   RELATIVEID,
   CUSTOMERNAME,
   RELATIONSHIP,
   (case when CERTTYPE='105' then 'Ent01' when CERTTYPE='2001' then 'Ent02' when CERTTYPE='56' then 'Ent03' 
    when CERTTYPE='0' then 'Ind01' when CERTTYPE='1' then 'Ind02' when CERTTYPE='3' then 'Ind04' 
    when CERTTYPE='4' then 'Ind05' when CERTTYPE='5' then 'Ind06' when CERTTYPE='6' then 'Ind07' 
    when CERTTYPE='7' then 'Ind08' when CERTTYPE='8' then 'Ind09' when CERTTYPE='9' then 'Ind10' 
    when CERTTYPE='53' then 'Ind11' when CERTTYPE='57' then 'Ind12' when CERTTYPE='54' then 'Ind13' 
    when CERTTYPE='55' then 'Ind14' when CERTTYPE='X' then 'Ind15' else CERTTYPE end) as CERTTYPE,
   CERTID,
   CRT_NO,
   INVESTMENTSUM,
   INVESTMENTPROP
    from F_MIS_CUSTOMER_RELATIVE where CRT_NO=V_ORG_ID
union 
    select to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    GR.CUSTOMERID,
    GR.RELATIVEID,
    null,
    '5401',
    CI.CERTTYPE,
    CI.CERTID,
    GR.CRT_NO,
    null,
    null
    from F_MIS_GROUP_RELATIVE GR 
    inner join F_MIS_CUSTOMER_INFO CI on (CI.CustomerID=GR.RelativeID and CI.CRT_NO=V_ORG_ID)
    where GR.CRT_NO=V_ORG_ID with ur;

    commit;

SET V_INT=V_INT+1;
end WHILE;
close cursor_orgid;

    -- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."PRO_M_WTOFF_LIST" (IN p_etl_date VARCHAR(20),out
result_code integer)

/******************************************************/
/***     程序名称 ：模型层-核销名单                  ***/
/***     创建日期 ：2015-06-15                      ***/
/***     创建人员 ：jzhu                            ***/
/***     功能描述 ：模型层核销名单抽取               ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150615090837100
BEGIN   
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'PRO_M_WTOFF_LIST';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型

    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    --插入增量数据
    INSERT INTO RWMS_WTOFF_LIST
    (INPUTDATE, --数据日期
    CUST_ID, --客户号
    CORE_CUST_ID, --核心客户号
    REC_STS, --记录状态
    UPDATEDATE, --ETL更新时间
    CRT_NO, --法人代码
    SRC_DT, --源系统日期
    ETL_DT --平台日期
    )
    SELECT to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    T.CUSTOMERID,
    BD.LN_CUST_NO,
    '2',
    to_date(p_etl_date, 'yyyymmdd') Etl_Tx_Date,
    BD.CRT_NO,
    BD.SRC_DT,
    BD.ETL_DT
    from F_CORE_LNLNSLNS BD 
    inner join F_MIS_BUSINESS_DUEBILL T on (T.SERIALNO=BD.LN_LN_ACCT_NO)
    where BD.LN_APCL_FLG='Y' and BD.ETL_DATE = p_etl_date
    and not exists (select null from RWMS_WTOFF_LIST where RWMS_WTOFF_LIST.CUST_ID=T.CUSTOMERID)
    with ur;
    commit;
    -- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."SP_A_INITIAL_RULE" (IN p_etl_date VARCHAR(20),OUT
result_code integer)
/******************************************************/
/***     程序名称 ：应用层-初始化RWMS_RULE表，导入授信客户，集团客户，个人经营贷客户***/
/***     创建日期 ：2015-06-21                      ***/
/***     创建人员 ：jzhu                            ***/
/***     功能描述 ：                                ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150621095853100
BEGIN
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值 

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'SP_A_INITIAL_RULE';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型
    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);-- 

    --清空RWMS_RULE表
    TRUNCATE table RWMS_RULE IMMEDIATE;

    SET v_point = '9902';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);-- 


--导入授信客户

    INSERT INTO RWMS_RULE
      (CustomerId,
      REGISTERCAPITAL,
      BelongOrgName,
      GROUP_BUSINESS,
      SUM,
      ORGTYPE,
      BALANCE,
      --LICENSEDATE,
      businesshistory,
      businesssum_TY,
      ISCERTIFIABLEGUARANTOR,
      BUSINESSSUM,
      refusedate,
      accecptdate,
      SCOPE,
      CRT_NO
      )select
        distinct ci.CUST_ID,
        RGST_CAP,
        MAIN_LINE,
        CRDT_TOTAL_AMT,
        DEBT_BAL_SUM,
        ECON_ORG_COMPNT,
        ci.EXPOS_BAL,
        --BIZ_LICS_INVA_DT,
        H_YEAR_CRDT_AMT,
        ANNUAL_CUST_CRDT_SIZE,
        case when ci.cust_name like '%担保%' then '1' else '0' end  as IF_BON_GUAR_CORP,--IF_BON_GUAR_CORP
        CRDT_TOTAL_AMT,
        RCNT_ONC_APPLY_NON_DT,
        RCNT_ONC_APPLY_PASS_DT,
        NEW_ENT_SIZE,
        ci.CRT_NO
        from RWMS_CUST_INFO ci 
        inner join F_MIS_ENT_INFO EI on (ci.cust_id=EI.CustomerID)
        where exists (select 1 
                from RWMS_DUBIL_INFO BD 
                where BD.cust_id=ci.cust_id 
                and BD.DUBIL_BAL>0) with ur
    --inner join RWMS_CONTR_INFO bc on (ci.cust_id=bc.cust_id and bc.CUR_BAL>0)
;
    COMMIT;

    SET v_point = '9903';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);-- 

--导入第三方客户
INSERT INTO RWMS_RULE
(CustomerId,crt_no) select customerid,'999' from rwmsuser.PARTNER_INFO ;
COMMIT;

-- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--

END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."SP_A_UPDATE_01" (IN p_etl_date VARCHAR(20),IN
V_ORG_ID VARCHAR(20),OUT result_code integer)
/******************************************************/
/***     程序名称 ：应用层-初始化rwms_rule表部分其余字段 (财务指标等。)***/
/***     创建日期 ：2015-06-21                      ***/
/***     创建人员 ：jzhu                            ***/
/***     功能描述 ：                                ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150621102955900
BEGIN   
    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值 

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        --CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'SP_A_UPDATE_01';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型
    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

    --审批机构数量(5年之内，年报)
/*    update rwms_rule rr
    set AUDITOFFICE=(SELECT COUNT(distinct audit_unit)
                   FROM RWMS_FIN_RPT_DATA_INDEX
                   WHERE cust_id=rr.customerid
                   AND rpt_term_id LIKE '%12'
                   AND rpt_term_id
                   BETWEEN to_char(to_date(p_etl_date,'yyyymmdd')-60 month,'yyyy/mm') AND to_char(to_date(p_etl_date,'yyyymmdd'),'yyyy/mm')
                   AND AUDIT_FLAG='1' and crt_no=V_ORG_ID
                   ) where crt_no=V_ORG_ID with ur;
    commit;
*/     
    SET v_point = '9902';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

--更新本期财务数据  LONG_LOAN(净资产合计后面的字段添加到哪里了？NET_ASSET_SUM)
      update rwms_rule rr
      set (REPORTDATE,
           REPORTPERIOD,
           AUDITFLAG,
           REPORTSCOPE,
           REPORTSTATUS,
           UPDATEDATE,
           ZCFZL,
           HYFZL,
           JXJLL,
           XJLRL,
           LDBL,
           SDBL,
           LXBZBS,
           XJLL,
           SDBLPF,
           SDBLQT,
           --AUDITOPINION,
           SYZQY,
           CQGQTZ,
           CQZQTZ,
           CYZDQTZ,
           WXZC,
           --AUDITOFFICE,
           JLR,
           ZZCSYL,
           JZCSYL,
           ZBSYL,
           ZYSRZZL,
           JLRZZL,
           LRZEZZL,
           ZZCBCL,
           LRZE,
           ZYYWSRXJL,
           PFCHZZTS,
           QTCHZZTS,
           QTYSK,
           LDZC,
           QTYFK,
           LDFZ,
           YSZK,
           ZZC,
           PFYSZKZZTS,
           QTYSZKZZTS,
           GDZC,
           DQJK,
           YFPJ,
           CQJK)= (select RPT_TERM_ID,
                          RPT_PRD_CD,
                          AUDIT_FLAG,
                          RPT_CALI_CD,
                          RPT_STS,
                          INPUT_DT,
                          ASSET_LIAB_RATIO,
                          CONTGT_LIAB_RATE,
                          NET_CASH_FLOW_QTTY,
                          OPER_CASH_FLOW_QTTY,
                          LIQD_RATE,
                          QCK_RATIO,
                          INT_GUAR_MULTI,
                          MON_CASH_FLOW_QTTY,
                          QCK_RATIO_W_R_TRADE,
                          QCK_RATIO_OTH_INDUS,
                          --AUDIT_OPIN,
                          OWNER_RIGHT_INTER_SUM,
                          nvl(LONG_SHR_RIGHT_INVEST,0)+nvl(LONG_CLAIM_INVEST,0),
                          nvl(LONG_SHR_RIGHT_INVEST,0)+nvl(LONG_CLAIM_INVEST,0),
                          HOLD_TO_MATURE_INVEST,
                          INVISIBLE_ASSET,
                          --AUDIT_UNIT,
                          NET_MARG,
                          SUM_ASSET_PRFT_RATE,
                          NET_ASSET_PRFT_RATE,
                          CAP_PRFT_RATE,
                          MAIN_BIZ_INCOME_GROW_RAT,
                          NET_MARG_GROW_RAT,
                          MARG_TOTAL_AMT_GROW_RAT,
                          SUM_ASSET_PAYBACK,
                          MARG_TOTAL_AMT,
                          MAIN_BIZ_BIZ_INCOME_CASH_RATE,
                          INVTRY_TURN_DAY_W_R_TRADE,
                          INVTRY_TURN_DAY_OTH,
                          OTH_COLL_MONEY,
                          LIQD_ASSET,
                          OTH_ACCT_PAYA,
                          LIQD_LIAB,
                          RECVBL_DEBT,
                          SUM_ASSET,
                          RECVBL_DEBT_TURN_DAY_W_R_TRADE,
                          RECVBL_DEBT_TURN_DAY_OTH,
                          FIX_ASSET,
                          SHORT_TM_LOAN,
                          PAYBL_BILL,
                          LONG_LOAN from RWMS_FIN_RPT_DATA_INDEX where CUST_ID=rr.customerid and crt_no=V_ORG_ID
and RPT_TERM_ID=(select max(RPT_TERM_ID) from RWMS_FIN_RPT_DATA_INDEX where cust_id=rr.customerid and crt_no=V_ORG_ID) fetch first 1 row only)
where crt_no=V_ORG_ID with ur;
commit;

    SET v_point = '9903';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

        --更新行业财务信息。
        update rwms_rule rr
        set (MODELCLASS,
             WHPJZCFZL,
             QGPJZCFZL,
             WHPJLDBL,
             QGPJLDBL,
             WHPJSDBL,
             QGPJZSDBL,
             WHPJLXBZBS,
             QGPJLXBZBS,
             WHPJZZCSYL,
             QGPJZZCSYL,
             WHPJZBSYL,
             QGPJZBSYL,
             WHPJZZCBCL,
             WHPJJZCSYL,
             QGPJZZCBCL,
             QGPJJZCSYL)=(select RPT_TYPE,
                                 BON_INDUS_AVG,
                                 CTY_INDUS_AVG,
                                 UBNK_INDUS_LIQD_RATE_AVG,
                                 CTY_INDUS_LIQD_RATE_AVG,
                                 QCK_RATIO_UBNK,
                                 CTY_QCK_RATIO,
                                 INT_GUAR_MULTI_BON,
                                 INT_GUAR_MULTI_CTY,
                                 SUM_ASSET_PRFT_RATE_BON,
                                 SUM_ASSET_PRFT_RATE_CTY,
                                 CAP_PRFT_RATE_BON,
                                 CAP_PRFT_RATE_CTY,
                                 SUM_ASSET_PAYBACK_UBNK,
                                 NET_ASSET_PRFT_RATE_UBNK,
                                 SUM_ASSET_PAYBACK_CTY,
                                 NET_ASSET_PRFT_RATE_CTY
                          from RWMS_FIN_INDUS_DATA_INDEX where CUST_ID=rr.customerid and crt_no=V_ORG_ID 
and RPT_TERM_ID=(select max(RPT_TERM_ID) 
from RWMS_FIN_INDUS_DATA_INDEX where cust_id=rr.customerid and crt_no=V_ORG_ID) fetch first 1 row only)
where crt_no=V_ORG_ID with ur;

    SET v_point = '9903';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

        --更新上月财务报表信息。
          update rwms_rule rr
          set (ZCFZL_LM,
               XJLRL_LM,
               LXBZBS_LM,
               SDBL_LM,
               JLR_LM,
               ZZCSYL_LM,
               JZCSYL_LM,
               ZYSRZZL_LM,
               JLRZZL_LM,
               LRZEZZL_LM,
               ZZCBCL_LM,
               LRZE_LM,
               YSZK_LM,
               ZZC_LM,
               GDZC_LM
               )= (select ASSET_LIAB_RATIO,
                                 OPER_CASH_FLOW_QTTY,
                                 INT_GUAR_MULTI,
                                 QCK_RATIO,
                                 NET_MARG,
                                 SUM_ASSET_PRFT_RATE,
                                 NET_ASSET_PRFT_RATE,
                                 MAIN_BIZ_INCOME_GROW_RAT,
                                 NET_MARG_GROW_RAT,
                                 MARG_TOTAL_AMT_GROW_RAT,
                                 SUM_ASSET_PAYBACK,
                                 MARG_TOTAL_AMT,
                                 RECVBL_DEBT,
                                 SUM_ASSET,
                                 FIX_ASSET
                                 from RWMS_FIN_RPT_DATA_INDEX where cust_id=rr.customerid and crt_no=V_ORG_ID
and rpt_term_id=(case when reportperiod='01' then to_char(to_date(rr.reportdate,'yyyy/MM')-1 month,'yyyy/MM')
when reportperiod='02' then to_char(to_date(rr.reportdate,'yyyy/MM')-3 month,'yyyy/MM')
when reportperiod='03' then to_char(to_date(rr.reportdate,'yyyy/MM')-6 month,'yyyy/MM')
when reportperiod='04' then to_char(to_date(rr.reportdate,'yyyy/MM')-12 month,'yyyy/MM') end)
and rpt_prd_cd=rr.reportperiod
fetch first 1 row only) where crt_no=V_ORG_ID with ur;
COMMIT;

-- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."SP_A_FINA" (IN p_etl_date VARCHAR(20),IN V_ORG_ID
VARCHAR(20),OUT result_code integer)
/******************************************************/
/***     程序名称 ：应用层-初始化rwms_rule表部分其余字段 (财务指标等。)***/
/***     创建日期 ：2015-06-21                      ***/
/***     创建人员 ：xxli1                           ***/
/***     功能描述 ：                                ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150624140318100
BEGIN   

    -- 开始定义异常处理变量
    DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
    DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
    DECLARE v_data_dt date;                      -- 数据日期date型
    DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
    DECLARE v_point varchar(10);                 -- 自定义断点变量
    DECLARE v_proc_name varchar(100);            -- 存储过程名称
    DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
    DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值 

    -- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        --CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'SP_A_FINA';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型
    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

--最近三个月的现金流量均值
update rwms_rule rr
   set XJLL_AVG =
       (select avg(MON_CASH_FLOW_QTTY)
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid and crt_no=V_ORG_ID
           and rpt_term_id between
               to_char(to_date(p_etl_date, 'yyyymmdd')-3 month,'yyyy/mm') and
               to_char(to_date(p_etl_date, 'yyyymmdd')-1 month,'yyyy/mm')
           and rpt_prd_cd='01') where crt_no=V_ORG_ID with ur;
commit;

    SET v_point = '9902';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

--上年同期流动比率，上年同期净利润，上年同期主营业务收入现金率
update rwms_rule rr
   set (LDBL_LY, JLR_LY, ZYYWSRXJL_LY) =
       (select LIQD_RATE, NET_MARG, MAIN_BIZ_BIZ_INCOME_CASH_RATE
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid and crt_no=V_ORG_ID
           and rpt_term_id =
               to_char(add_months(to_date(rr.reportdate, 'yyyy/MM'), -12),'yyyy/MM') 
           and rpt_prd_cd=rr.REPORTPERIOD
fetch first 1 row only) where crt_no=V_ORG_ID with ur;
commit;

    SET v_point = '9903';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

--上两期EBIT利息保障倍数，上两期总资产报酬率，上两期净资产收益率，上两期利润总额
update rwms_rule rr
   set (LXBZBS_L2M, ZZCBCL_L2M, JZCSYL_L2M, LRZE_L2M) =
       (select INT_GUAR_MULTI,
               SUM_ASSET_PAYBACK,
               NET_ASSET_PRFT_RATE,
               MARG_TOTAL_AMT
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid and crt_no=V_ORG_ID
           and rpt_term_id=(case when reportperiod='01' then to_char(to_date(rr.reportdate,'yyyy/MM')-2 month,'yyyy/MM')
when reportperiod='02' then to_char(to_date(rr.reportdate,'yyyy/MM')-6 month,'yyyy/MM')
when reportperiod='03' then to_char(to_date(rr.reportdate,'yyyy/MM')-12 month,'yyyy/MM')
when reportperiod='04' then to_char(to_date(rr.reportdate,'yyyy/MM')-24 month,'yyyy/MM') end)
and rpt_prd_cd=rr.reportperiod 
fetch first 1 row only) where crt_no=V_ORG_ID with ur;
commit;

    SET v_point = '9904';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

--上三期利润总额
update rwms_rule rr
   set LRZE_L3M =
       (select MARG_TOTAL_AMT
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid and crt_no=V_ORG_ID
           and rpt_term_id =
               (case when reportperiod='01' then to_char(to_date(rr.reportdate,'yyyy/MM')-3 month,'yyyy/MM')
when reportperiod='02' then to_char(to_date(rr.reportdate,'yyyy/MM')-9 month,'yyyy/MM')
when reportperiod='03' then to_char(to_date(rr.reportdate,'yyyy/MM')-18 month,'yyyy/MM')
when reportperiod='04' then to_char(to_date(rr.reportdate,'yyyy/MM')-36 month,'yyyy/MM') end) 
and rpt_prd_cd=rr.reportperiod 
fetch first 1 row only) where crt_no=V_ORG_ID with ur;
commit;

/*--年初利润总额(上年全年利润总额)
update rwms_rule rr
   set LRZE_NCZ =
       (select MARG_TOTAL_AMT
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid
           and RPT_PRD_CD = '04'
           and rpt_term_id =
               (to_char(add_months(to_date(p_etl_date, 'yyyymmdd'), -12),
                        'yyyy') || '12') fetch first 1 row only);
commit;*/

    SET v_point = '9905';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

--当期年报利润总额，当期年报主营业务收入
update rwms_rule rr
   set (LRZE_TY, ZYYWSR_TY) =
       (select MARG_TOTAL_AMT, MAIN_BIZ_BIZ_INCOME
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid and crt_no=V_ORG_ID
           and RPT_PRD_CD = '04'
           and rpt_term_id =
               to_char(add_months(to_date(p_etl_date, 'yyyymmdd'), -12),
                       'yyyy') || '/12' fetch first 1 row only) where crt_no=V_ORG_ID with ur;
commit;

    SET v_point = '9906';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

--上期年报利润总额，上期年报主营业务收入
update rwms_rule rr
   set (LRZE_LY, ZYYWSR_LY) =
       (select MARG_TOTAL_AMT, MAIN_BIZ_BIZ_INCOME
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid and crt_no=V_ORG_ID
           and RPT_PRD_CD = '04'
           and rpt_term_id =
               to_char(add_months(to_date(p_etl_date, 'yyyymmdd'), -24),
                       'yyyy') || '/12' fetch first 1 row only) where crt_no=V_ORG_ID with ur;
commit;

    SET v_point = '9907';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

--上两期年报利润总额，上两期年报主营业务收入
update rwms_rule rr
   set (LRZE_L2Y, ZYYWSR_L2Y) =
       (select MARG_TOTAL_AMT, MAIN_BIZ_BIZ_INCOME
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid and crt_no=V_ORG_ID
           and RPT_PRD_CD = '04'
           and rpt_term_id =
               to_char(add_months(to_date(p_etl_date, 'yyyymmdd'), -36),
                       'yyyy') || '/12' fetch first 1 row only) where crt_no=V_ORG_ID with ur;
commit;

    SET v_point = '9908';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--

--上期经营性净现金流量（年）
update rwms_rule rr
   set (JXJLL_LY) =
       (select NET_CASH_FLOW_QTTY
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid and crt_no=V_ORG_ID
           and RPT_PRD_CD = '04'
           and rpt_term_id =
               to_char(add_months(to_date(p_etl_date, 'yyyymmdd'), -24),
                       'yyyy') || '/12' fetch first 1 row only) where crt_no=V_ORG_ID with ur;
commit;

/*--上两期经营性净现金流量（年）
update rwms_rule rr
   set (JXJLL_L2Y) =
       (select NET_CASH_FLOW_QTTY
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid
           and RPT_PRD_CD = '04'
           and rpt_term_id =
               to_char(add_months(to_date(rr.reportdate, 'yyyyMM'), -36),
                       'yyyy') || 12);
commit;

--上期经营性净现金流量（季度）
update rwms_rule rr
   set (JXJLL_LQ) =
       (select NET_CASH_FLOW_QTTY
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid
           and RPT_PRD_CD = '02'
           and rpt_term_id =
              to_char(add_months(to_date(rr.reportdate, 'yyyyMM'), -3),'yyyyMM'));
commit;

--上两期经营性净现金流量（季度）
update rwms_rule rr
   set (JXJLL_L2Q) =
       (select NET_CASH_FLOW_QTTY
          from RWMS_FIN_RPT_DATA_INDEX
         where cust_id = rr.customerid
           and RPT_PRD_CD = '02'
           and rpt_term_id =
               to_char(add_months(to_date(rr.reportdate, 'yyyyMM'), -3),'yyyyMM'));
commit;*/
-- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."SP_A_INITIAL_RWMSTEST" (IN p_etl_date VARCHAR(20),OUT
result_code integer)
/******************************************************/
/***     程序名称 ：SP_D29_INITIAL_rwmsuser            ***/
/***     创建日期 ：2015-06-22                      ***/
/***     创建人员 ：xnhu                            ***/
/***     功能描述 ：                                ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150622143006900
BEGIN  

-- 开始定义异常处理变量
DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
DECLARE v_data_dt date;                      -- 数据日期date型
DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
DECLARE v_point varchar(10);                 -- 自定义断点变量
DECLARE v_proc_name varchar(100);            -- 存储过程名称
DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值 

-- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        --CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'SP_A_INITIAL_RWMSTEST';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型
    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);-- 
      --初始化 rwmsuser.ent_info 表
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE rwmsuser.ENT_INFO';
/*
delete from rwms_rule rr
where rr.customerid in (
                       select cust_id from rwms_dubil_info
                       where (FIVE_LVL_CLASS_RESLT like '03%'
                       or FIVE_LVL_CLASS_RESLT like '04%'
                       or FIVE_LVL_CLASS_RESLT like '05%')
                       and dubil_bal>0 );
commit;

delete from rwms_rule rr where rr.overdate>90;
commit;
*/

/*
insert into rwmsuser.ent_info(
          CUSTOMERID,
          ENTERPRISENAME,
          ORGNATURE,
          INDUSTRYTYPE,
          INDUSTRYTYPE2,
          ECONOMYTYPE,
          REGISTERCAPITAL,
          SCOPE,
          CREDITDATE,
          FICTITIOUSPERSON,
          CERTTYPE,
          CORPID,
          LISTINGCORPORNOT,
          LOANCARDNO,
          CREDITLEVEL,
          MANAGEORGID,
          MANAGEUSERID
            )
    select  cust_id,
            cust_name,
            CUST_TYPE,
            new_inl_std_indus_class ,
            OWN_INDUS ,
            econ_org_compnt ,
            rgst_cap,
            new_ent_size,
            fst_setup_crdt_co_tm ,
            legal_rep ,
            CERT_TYPE,
            CERT_ID,
            if_go_pub_corp ,
            loan_card_id ,
            crdt_rating_reslt ,
            substr(main_line,1,4) ,
            manage_cust_mgr 
    from  rwms_cust_info ;
    commit;
*/
    /*以下为界面需要表，部分是否需要修改。*/
 /*
            update rwmsuser.ent_info  set GROUPFLAG='1' where customerid in (select rel_pers_id from RWMS_REL_TAB rr where REL_REL='5401');
     commit;
*/
 --初始化 rwmsuser.havecredit_custlist 表

    --EXECUTE IMMEDIATE 'TRUNCATE TABLE rwmsuser.HAVECREDIT_CUSTLIST';
TRUNCATE table rwmsuser.HAVECREDIT_CUSTLIST IMMEDIATE;

 --界面展示是否需要授信客户？
insert into rwmsuser.havecredit_custlist(
          CUSTOMERID,
          CUSTOMERNAME,
          MFCUSTOMERID,
          BALANCE,
          MANAGEORGID,
          SCOPE1,
          INDUSTRYTYPE,
          INDUSTRYTYPE1,
          INDUSTRYTYPE2,
          CREDITLEVEL,
          MANAGEUSERID,
          OTHERCUSTOMRID,
          REGADDRESS,
          CERTTYPE,
          CERTID,
          CUSTTYPE,
          CREDITBALANCE,
          ORGNATURE,
           SOURCETYPE)
    select ci.cust_id,
            cust_name,
            core_cust_id ,
            ci.EXPOS_BAL ,
            MAIN_LINE ,
            NEW_ENT_SIZE ,
            NEW_INL_STD_INDUS_CLASS ,
            substr(NEW_INL_STD_INDUS_CLASS,1,1),
            substr(NEW_INL_STD_INDUS_CLASS,1,3),
            CRDT_RATING_RESLT ,
            MANAGE_CUST_MGR ,
            LP_ORG_CD ,
            RGST_ADDR ,
            CERT_TYPE ,
            CERT_ID,
            substr(CERT_TYPE,1,1),
            ci.EXPOS_BAL,
            ORG_NATURE,'0'
            from rwms_cust_info ci;

commit;
-- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

CREATE PROCEDURE "RWUSER"."SP_A_JS_RELATIVE_INFO" (IN p_etl_date VARCHAR(20),OUT
result_code integer)
/******************************************************/
/***     程序名称 ：应用层-初始化rwms_rule表部分其余字段 (财务指标等。)***/
/***     创建日期 ：2015-06-21                      ***/
/***     创建人员 ：xxli1                           ***/
/***     功能描述 ：                                ***/
/***     备注 ： 所有工商征信中关联人只取关联人名称，关联人ID、类型均为空  add by zqian 2017-2-15 11:58:29 沈才胜确认                                ***/
/******************************************************/
LANGUAGE SQL
SPECIFIC SQL150622143040400
BEGIN  

-- 开始定义异常处理变量
DECLARE sqlcode int default 0 ;              -- SQLCODE返回代码 默认0-成功
DECLARE sqlstate char(5) default '00000';    -- SQLSTATE返回代码 默认'00000'-成功
DECLARE v_data_dt date;                      -- 数据日期date型
DECLARE v_proc_stat varchar(100);            -- 存储过程执行状态
DECLARE v_point varchar(10);                 -- 自定义断点变量
DECLARE v_proc_name varchar(100);            -- 存储过程名称
DECLARE v_sql_code  char(6);                 -- 错误码日志信息SQLCODE
DECLARE v_msg varchar(200);                  -- 日志信息内容：所有的返回信息值 

-- 异常处理
    -- 02000 ，找不到进行 FETCH,UPDATE ,DELETE 操作的行。或者查询的结果是个空行
    DECLARE CONTINUE HANDLER FOR sqlstate '02000'
    BEGIN
        SET v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    -- 42704 ，找不到视图。
    DECLARE CONTINUE HANDLER FOR sqlstate '42704'
    BEGIN
        set v_proc_stat = char(sqlcode)||sqlstate||'Warning' ;--
        SET v_sql_code = substr(v_proc_stat,1,4);--
        SET v_point  = '找不到视图';--
        SET v_msg = v_proc_stat||' v_point:'||v_point  ;    --
        CALL proc_dblog(v_data_dt,v_proc_name ,'WARN' ,v_sql_code,v_msg);--
    END;--
    --SQL产生的非上述异常的处理
    DECLARE EXIT HANDLER FOR sqlexception 
    BEGIN
        SET v_proc_stat = char(sqlcode) || sqlstate || ' Failed';--
        SET result_code = 1;--
        ROLLBACK ;    --
        SET v_sql_code = substr(v_proc_stat,1,11) ;--
        SET v_msg = v_proc_stat||'v_POINT:'||v_POINT;--
        CALL proc_dblog(v_data_dt,v_proc_name ,'ERROR' ,v_sql_code,v_msg);--
    END;--

    -- 初始化变量
    SET v_proc_stat = char(0) || '00000 Success';                -- 设置SQL处理状态初始值 
    SET v_proc_name = 'SP_A_JS_RELATIVE_INFO';                  -- 存储过程名
    SET v_data_dt = to_date(p_etl_date, 'yyyymmdd');   -- ETL日期转为日期型
    SET v_point = '9901';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);-- 
--1、给js_relative_info_tmp 临时表赋值

EXECUTE IMMEDIATE 'alter table js_relative_info_tmp activate not logged initially with empty table';--

    SET v_point = '9902';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);-- 

insert into js_relative_info_tmp
(CUSTOMERID  , --客户号
CUSTOMERNAME  ,--客户名称
CERTTYPE  ,    --证件类型
CERTID  ,      --证件号码
RELACUSTOMERID  ,--关联客户编号
RELACUSTOMERNAME  ,--关联客户名称
RELACERTTYPE  ,--关联证件类型
RELACERTID  ,  --关联证件号码
RELATIONSHIP  ,--关联关系大类
INVESTMENTPROP  ,   ---股权占比(是否都是百分比)
INVESTMENTSUM,         --出资金额
ETL_INPUTDATE  ,    --ETL加工时间
ETL_UPDATEDATE  ,   --ETL更新时间
FROM_SYSTEM  ,      --系统来源
--ETL_FLAG  ,         --ETL标志
--SCHEDULE_CD,        --调度号
ITEM1 ,    --01
ITEM2 ,    --关系明细
SYSTEMFROM --1
)

--R02,A是B的股东
--取工商中的企业对外投资信息
select c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       '',
       t.cust_name,
       '',
       '',
       'R12' as relationship,    --工商的 对外投资 划分到 企业投资关联大类中； add by zqian 2017-1-23 15:42:14
       T.HOLD_STCK_RATIO,
       t.CONTRI_AMT,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'IC',
     --  I_ETL_FLAG,
   --    I_SCHEDULE_CD,
       '01',
       '12',  --修改为对应码值  add by zqian 2017-1-24 10:10:01
       '1'
  from rwuser.RWMS_IC_ENTINV_C t
  inner join rwuser.RWMS_CUST_INFO c
    on t.main_rgst_id = c.BIZ_LICS_ID --营业执照号
  left join rwuser.RWMS_CUST_INFO u
    on t.cust_name = u.cust_name --企业(机构)名称
 where c.cust_id is not null
--where   t.cust_name is not null  and u.cert_id is not null  --and c.cert_id is not null

UNION ALL
 -- 修改记录  ffwang 2014-12-29  出资信息现在取企业征信中的数据
/* 股东及出资信息 */
--由于原系统（外部数据采集平台）当中的出资人证件类型存放为证件号码，
--故无法使用此证件类型所以只取行内出资人信息。

/*可以取到行外出资人信息的
SELECT
       c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       u.cust_id,
       t.CONTRI_PER_NAME,
       t.cert_type,
       t.cert_id,
       'R02' as relationship,
       to_char(T.CONTRI_PCT*100) || '.00%',
       t.CONTRI_AMT,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'CL',
       '02',
       '出资人',
       '1'
  FROM RWMS_CL_CRDT_CONTRI_PER_C T
  left join rwuser.RWMS_CUST_INFO c
    on t.LOAN_CARD_ID = c.LOAN_CARD_ID
  left join rwuser.RWMS_CUST_INFO u
    on t.cert_id = u.cert_id
   where c.cust_id is not null and t.cert_id is not null 
	UNION ALL

   --只取行内出资人信息

-- 征信先只取关联关系那张表，这块先注释  add by zqian 2017-1-24 10:27:11
SELECT
       c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       u.cust_id,
       t.CONTRI_PER_NAME,
       u.cert_type,
       u.cert_id,
       'R02' as relationship,
       to_char(T.CONTRI_PCT*100) || '.00%',
       t.CONTRI_AMT,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'CL',
     --  I_ETL_FLAG,
     --  I_SCHEDULE_CD,
       '02',
       '出资人',   
       '1'
  FROM RWMS_CL_CRDT_CONTRI_PER_C T
  left join rwuser.RWMS_CUST_INFO c
    on t.LOAN_CARD_ID = c.LOAN_CARD_ID
  left join rwuser.RWMS_CUST_INFO u
    on t.cert_id = u.cert_id
   where c.cust_id is not null and u.cert_id is not null
  
  UNION ALL
--t.cert_id 在RWMS_CL_CRDT_FGUAR_DTL_C表中存放的是贷款卡编码
 select
  U.CUST_ID,
  T.BY_GUAR,
  U.CERT_TYPE,
  U.CERT_ID, --被担保人
  c.cust_id,
  c.cust_name,
  c.cert_type,
  c.cert_id, --担保人信息
  'R03' as relationship,
  NULL,
  NULL,
  to_char(CURRENT DATE,'yyyymmdd'),
  NULL,
  'CL',
--  I_ETL_FLAG,
--  I_SCHEDULE_CD,
  '03',
  '担保人',
  '2'
   from RWMS_CL_CRDT_FGUAR_DTL_C t
   LEFT JOIN rwuser.RWMS_CUST_INFO c
     ON T.LOAN_CARD_ID = C.LOAN_CARD_ID
   LEFT JOIN rwuser.RWMS_CUST_INFO u
     on u.loan_card_id = t.cert_id
  where u.cust_id is not null
union all

--信贷中的担保人关系 分为 “被担保”和“对外担保”两种关系，原有逻辑注释，新逻辑在下面 add by zqian 2017-2-7 11:00:17
select GC.Customer_ID,
       c.customername,
       (case when c.CERTTYPE='105' then 'Ent01' when c.CERTTYPE='2001' then 'Ent02' when c.CERTTYPE='56' then 'Ent03' 
    when c.CERTTYPE='0' then 'Ind01' when c.CERTTYPE='1' then 'Ind02' when c.CERTTYPE='3' then 'Ind04' 
    when c.CERTTYPE='4' then 'Ind05' when c.CERTTYPE='5' then 'Ind06' when c.CERTTYPE='6' then 'Ind07' 
    when c.CERTTYPE='7' then 'Ind08' when c.CERTTYPE='8' then 'Ind09' when c.CERTTYPE='9' then 'Ind10' 
    when c.CERTTYPE='53' then 'Ind11' when c.CERTTYPE='57' then 'Ind12' when c.CERTTYPE='54' then 'Ind13' 
    when c.CERTTYPE='55' then 'Ind14' when c.CERTTYPE='X' then 'X' else c.CERTTYPE end) as certtype,
       c.certid,
       BD.cust_id,
       u.customername,
       (case when u.CERTTYPE='105' then 'Ent01' when u.CERTTYPE='2001' then 'Ent02' when u.CERTTYPE='56' then 'Ent03' 
    when u.CERTTYPE='0' then 'Ind01' when u.CERTTYPE='1' then 'Ind02' when u.CERTTYPE='3' then 'Ind04' 
    when u.CERTTYPE='4' then 'Ind05' when u.CERTTYPE='5' then 'Ind06' when u.CERTTYPE='6' then 'Ind07' 
    when u.CERTTYPE='7' then 'Ind08' when u.CERTTYPE='8' then 'Ind09' when u.CERTTYPE='9' then 'Ind10' 
    when u.CERTTYPE='53' then 'Ind11' when u.CERTTYPE='57' then 'Ind12' when u.CERTTYPE='54' then 'Ind13' 
    when u.CERTTYPE='55' then 'Ind14' when u.CERTTYPE='X' then 'X' else u.CERTTYPE end) as certtype ,
       u.certid,
       'R03' as relationship,
       null,
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'XG',
       '04',
       '担保人',
       '0'
  from rwuser.RWMS_GUAR_INFO GC
 inner join rwuser.RWMS_DUBIL_INFO BD
    ON BD.RELA_CONTR_SEQ_ID=GC.REL_CONTR_ID 
  left join rwuser.F_MIS_CUSTOMER_INFO c
    on GC.Customer_ID = c.customerid
  left join rwuser.F_MIS_CUSTOMER_INFO u
    on BD.cust_id = U.customerid
 where BD.DUBIL_BAL > 0 and U.customerid<>c.customerid
 */
 /*将原有的信贷中“担保人”关系，分为“被担保”和“对外担保”两种关系，且借款人和担保人均在预警系统客户中存在； add by zqian 2017-2-7 11:01:27 */
  select BD.cust_id,
       u.cust_name,
       u.cert_type,
       u.cert_id,
       c.cust_id,
       GC.GUARANTOR_NAME,
       c.cert_type,
       c.cert_id,
       'R03' as relationship,
       null,
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'XG',
       '04',
       '0309',
       '0'
  from rwuser.RWMS_GUAR_INFO GC
 inner join rwuser.RWMS_DUBIL_INFO BD
    ON BD.RELA_CONTR_SEQ_ID=GC.REL_CONTR_ID 
  left join rwuser.rwms_cust_info c
    on GC.Customer_ID = c.cust_id
  inner join rwuser.rwms_cust_info u
    on BD.cust_id = U.cust_id
 where BD.DUBIL_BAL > 0 
union all 
select GC.Customer_ID,
       u.cust_name,
       u.cert_type,
       u.cert_id,
       GC.Guar_Antor_ID,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       'R03' as relationship,
       null,
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'XG',
       '04',
       '0310',
       '0'
  from rwuser.RWMS_GUAR_INFO GC
 inner join rwuser.RWMS_DUBIL_INFO BD
    ON BD.RELA_CONTR_SEQ_ID=GC.REL_CONTR_ID 
  inner join rwuser.rwms_cust_info u
    on GC.Customer_ID = u.cust_id
  left join rwuser.rwms_cust_info c
    on BD.cust_id = c.cust_id
 where BD.DUBIL_BAL > 0
 
 /*将原有的信贷中“担保人”关系，分为“被担保”和“对外担保”两种关系，且借款人和担保人均在预警系统客户中存在； end by zqian 2017-2-7 11:01:27 */
   
union all
	SELECT t.CUST_ID,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       t.REL_PERS_ID,
       t.rel_pers_name,
       T.cert_type,
       T.cert_id,
       CASE
         WHEN T.REL_REL LIKE '01%' THEN
          'R01' --高管
         WHEN T.REL_REL LIKE '08%' THEN
          'R01' --集团高管
         WHEN T.REL_REL LIKE '52%' THEN
          'R02' ---股东
         WHEN T.REL_REL LIKE '02%' THEN
          'R12' --企业关联投资
         WHEN T.REL_REL LIKE '51%' THEN
          'R01' --高管
       end as relationship, ---需要核验AB前后的顺序，是否需要拆分开
       NULL,
       NULL,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'XG',
   --    I_ETL_FLAG,
   --    I_SCHEDULE_CD,
       '06',
       CASE
         WHEN T.REL_REL LIKE '01%' THEN
          '01' --高管      --修改为对应码值  add by zqian 2017-1-24 10:10:01
         WHEN T.REL_REL LIKE '08%' THEN
          '01' --集团高管  --修改为对应码值  add by zqian 2017-1-24 10:10:01
         WHEN T.REL_REL LIKE '52%' THEN
          '02' ---股东     --修改为对应码值  add by zqian 2017-1-24 10:10:01
         WHEN T.REL_REL LIKE '02%' THEN
          '12' --股东      --修改为对应码值  add by zqian 2017-1-24 10:10:01
         WHEN T.REL_REL LIKE '51%' THEN
          '01' --高管      --修改为对应码值  add by zqian 2017-1-24 10:10:01
       end,
       '0'
  FROM RWMS_REL_TAB T
  inner join rwuser.RWMS_CUST_INFO c
    on t.cust_id = c.cust_id
 where (t.REL_REL LIKE '01%' or T.REL_REL LIKE '08%' or
       T.REL_REL LIKE '52%' or T.REL_REL LIKE '02%' or
       T.REL_REL LIKE '51%')
   and c.cust_name is not null
union all
SELECT c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id, --R05 表示资金往来
       u.cust_id,
       t.opp_name,
       u.cert_type,
       u.cert_id, --交易对手不在我行
       'R05',
       null,
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'FXQ',
    --   I_ETL_FLAG,
   --    I_SCHEDULE_CD,
       '07',
       '05',   --修改为对应码值  add by zqian 2017-1-24 10:10:01
       '0'
  FROM RWMS_RWMS_OPP_LIST T
  inner JOIN rwuser.RWMS_CUST_INFO c
    ON T.Main_Cust = C.Core_Cust_Id
  inner join rwuser.RWMS_CUST_INFO u
    on T.OPP_NAME = U.CUST_NAME
 where c.cust_name<>t.opp_name and length(t.opp_name)>8
/*
 union all
  --企业征信中的股权关联

SELECT
       c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       u.cust_id,
       t.corp_name,
       u.cert_type,
       u.cert_id,
       'R02' as relationship,
       '',
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'CL',
    --   I_ETL_FLAG,
    --   I_SCHEDULE_CD,
       '09',
        CASE
         WHEN rel like '%家族企业%' THEN
          '家族企业'
         WHEN rel like '%母子公司%' THEN
          '母子公司'
         WHEN rel like '%投资关联%' THEN
          '投资关联'
         WHEN rel like '%出资人关联%' THEN
          '出资人关联'
       end,
       '2'
  FROM RWMS_CL_CRD_REL_ENT_C T
  left join rwuser.RWMS_CUST_INFO c
    on t.loan_card_encode = c.loan_card_id
  left join rwuser.RWMS_CUST_INFO u
    on t.corp_name = u.cust_name

     where (rel like '%家族企业%'
    or rel like '%母子公司%'
    or rel like '%投资关联%'
    or rel like '%出资人关联%')
      and c.cust_name is not null
      and u.cert_id is not null

    --企业征信中的高管关联
union all
SELECT
       c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       u.cust_id,
       t.corp_name,
       u.cert_type,
       u.cert_id,
       'R01' as relationship,
       '',
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'CL',
   --    I_ETL_FLAG,
    --   I_SCHEDULE_CD,
       '09',
       '高管人员关联',
       '2'
  FROM RWMS_CL_CRD_REL_ENT_C T
  left join rwuser.RWMS_CUST_INFO c
    on t.loan_card_encode = c.loan_card_id
  left join rwuser.RWMS_CUST_INFO u
    on t.corp_name = u.cust_name

     where (rel like '%高管人员关联%')
      and c.cust_name is not null
      and u.cert_id is not null
*/
     --企业征信中的担保关联
/*
--  征信关联关系 取 新33类的关联关系   add  by zqian 2017-1-24 10:32:02
union all
SELECT
       c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       u.cust_id,
       t.corp_name,
       u.cert_type,
       u.cert_id,
       'R03' as relationship,
       '',
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'CL',
    --   I_ETL_FLAG,
    --   I_SCHEDULE_CD,
       '09',
       CASE
         WHEN rel like '%相互担保%' THEN
          '相互担保'
         WHEN rel like '%担保人关联%' THEN
          '担保人关联'
       end,
       '2'
  FROM RWMS_CL_CRD_REL_ENT_C T
  left join rwuser.RWMS_CUST_INFO c
    on t.loan_card_encode = c.loan_card_id
  left join rwuser.RWMS_CUST_INFO u
    on t.corp_name = u.cust_name

     where (rel like '%相互担保%'
    or rel like '%担保人关联%' )
      and c.cust_name is not null
      and u.cert_id is not null
   */   
union all
 select
       rci1.customerid,
       rci1.customername,
       (case when rci1.CERTTYPE='105' then 'Ent01' when rci1.CERTTYPE='2001' then 'Ent02' when rci1.CERTTYPE='56' then 'Ent03' 
    when rci1.CERTTYPE='0' then 'Ind01' when rci1.CERTTYPE='1' then 'Ind02' when rci1.CERTTYPE='3' then 'Ind04' 
    when rci1.CERTTYPE='4' then 'Ind05' when rci1.CERTTYPE='5' then 'Ind06' when rci1.CERTTYPE='6' then 'Ind07' 
    when rci1.CERTTYPE='7' then 'Ind08' when rci1.CERTTYPE='8' then 'Ind09' when rci1.CERTTYPE='9' then 'Ind10' 
    when rci1.CERTTYPE='53' then 'Ind11' when rci1.CERTTYPE='57' then 'Ind12' when rci1.CERTTYPE='54' then 'Ind13' 
    when rci1.CERTTYPE='55' then 'Ind14' when rci1.CERTTYPE='X' then 'X' else rci1.CERTTYPE end) as certtype1,
       nvl(rci1.certid,rci1.customerid), 
       rci2.customerid,
       rci2.customername,
       (case when rci2.CERTTYPE='105' then 'Ent01' when rci2.CERTTYPE='2001' then 'Ent02' when rci2.CERTTYPE='56' then 'Ent03' 
    when rci2.CERTTYPE='0' then 'Ind01' when rci2.CERTTYPE='1' then 'Ind02' when rci2.CERTTYPE='3' then 'Ind04' 
    when rci2.CERTTYPE='4' then 'Ind05' when rci2.CERTTYPE='5' then 'Ind06' when rci2.CERTTYPE='6' then 'Ind07' 
    when rci2.CERTTYPE='7' then 'Ind08' when rci2.CERTTYPE='8' then 'Ind09' when rci2.CERTTYPE='9' then 'Ind10' 
    when rci2.CERTTYPE='53' then 'Ind11' when rci2.CERTTYPE='57' then 'Ind12' when rci2.CERTTYPE='54' then 'Ind13' 
    when rci2.CERTTYPE='55' then 'Ind14' when rci2.CERTTYPE='X' then 'X' else rci2.CERTTYPE end) as certtype2,
       nvl(rci2.certid,rci2.customerid),
       'R10',
       null,
       null,
       p_etl_date,
       NULL,
       'XG',
       '08',
       '10',
       '1' 
    from F_MIS_GROUP_RELATIVE gr
    inner join f_mis_customer_info rci1 on gr.customerid=rci1.customerid
    inner join f_mis_customer_info rci2 on gr.relativeid=rci2.customerid
union all
    select
       rci2.customerid,
       rci2.customername,
       (case when rci2.CERTTYPE='105' then 'Ent01' when rci2.CERTTYPE='2001' then 'Ent02' when rci2.CERTTYPE='56' then 'Ent03' 
    when rci2.CERTTYPE='0' then 'Ind01' when rci2.CERTTYPE='1' then 'Ind02' when rci2.CERTTYPE='3' then 'Ind04' 
    when rci2.CERTTYPE='4' then 'Ind05' when rci2.CERTTYPE='5' then 'Ind06' when rci2.CERTTYPE='6' then 'Ind07' 
    when rci2.CERTTYPE='7' then 'Ind08' when rci2.CERTTYPE='8' then 'Ind09' when rci2.CERTTYPE='9' then 'Ind10' 
    when rci2.CERTTYPE='53' then 'Ind11' when rci2.CERTTYPE='57' then 'Ind12' when rci2.CERTTYPE='54' then 'Ind13' 
    when rci2.CERTTYPE='55' then 'Ind14' when rci2.CERTTYPE='X' then 'X' else rci2.CERTTYPE end) as certtype2,
       nvl(rci2.certid,rci2.customerid), 
       rci1.customerid,
       rci1.customername,
       (case when rci1.CERTTYPE='105' then 'Ent01' when rci1.CERTTYPE='2001' then 'Ent02' when rci1.CERTTYPE='56' then 'Ent03' 
    when rci1.CERTTYPE='0' then 'Ind01' when rci1.CERTTYPE='1' then 'Ind02' when rci1.CERTTYPE='3' then 'Ind04' 
    when rci1.CERTTYPE='4' then 'Ind05' when rci1.CERTTYPE='5' then 'Ind06' when rci1.CERTTYPE='6' then 'Ind07' 
    when rci1.CERTTYPE='7' then 'Ind08' when rci1.CERTTYPE='8' then 'Ind09' when rci1.CERTTYPE='9' then 'Ind10' 
    when rci1.CERTTYPE='53' then 'Ind11' when rci1.CERTTYPE='57' then 'Ind12' when rci1.CERTTYPE='54' then 'Ind13' 
    when rci1.CERTTYPE='55' then 'Ind14' when rci1.CERTTYPE='X' then 'X' else rci1.CERTTYPE end) as certtype1,
       nvl(rci1.certid,rci1.customerid),
       'R11',
       null,
       null,
       p_etl_date,
       NULL,
       'XG',
       '09',
       '11',
       '1' 
    from F_MIS_GROUP_RELATIVE gr
    inner join f_mis_customer_info rci1 on gr.customerid=rci1.customerid
    inner join f_mis_customer_info rci2 on gr.relativeid=rci2.customerid

/* 新增 关联关系 工商数据解析 add by zqian 2017-1-22 19:44:59 */
 --股权关联
 union all
	select c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       '',
       gs.SHANAME,
       '',
       '',
       'R02' as relationship,   
       null,
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'IC',
     --  I_ETL_FLAG,
   --    I_SCHEDULE_CD,
       '01',
       '02',  
       '1'
from rwuser.F_GS_ENTBASE GE 
inner join rwuser.F_GS_SHAREHOLDER GS on GE.ENTSEQ=GS.ENTSEQ 
inner join rwuser.RWMS_CUST_INFO c on c.cust_name=ge.entName
left join rwuser.RWMS_CUST_INFO u on u.cust_name=gs.SHANAME
where c.cust_id is not null
 
 --高管关联：企业主要人员任职信息 F_GS_ENTPERSON
 union all
 select c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       '',
       gp.pername,
       '',
       '',
       'R01' as relationship,   
       null,
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'IC',
     --  I_ETL_FLAG,
   --    I_SCHEDULE_CD,
       '01',
       '01', 
       '1'
from rwuser.F_GS_ENTBASE GE 
inner join rwuser.F_GS_ENTPERSON GP on GE.ENTSEQ=GP.ENTSEQ 
inner join rwuser.RWMS_CUST_INFO c on c.cust_name=ge.entName
left join rwuser.RWMS_CUST_INFO u on u.cust_name=gp.pername
where c.cust_id is not null

--高管关联： 法人代表对外投资信息 F_GS_ENTFRINV
 union all
 select c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       '',
       T2.EntName,
       '',
       '',
       'R01' as relationship,   
       null,
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'IC',
     --  I_ETL_FLAG,
   --    I_SCHEDULE_CD,
       '01',
       '0116',  --修改为对应码值 
       '1'
from rwuser.F_GS_ENTBASE T1
inner join rwuser.F_GS_ENTFRINV GT  on T1.ENTSEQ=GT.ENTSEQ1
inner join rwuser.F_GS_ENTBASE T2 on T2.ENTSEQ=GT.ENTSEQ2
inner join rwuser.RWMS_CUST_INFO c on c.cust_name=T1.entName
left join rwuser.RWMS_CUST_INFO u on u.cust_name=T2.entName
where T1.ENTNAME <> T2.ENTNAME and T1.ENTNAME is not null
 
--高管关联： 法人代表其他公司任职信息 F_GS_ENTFRPOSITION
 union all
 select c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       '',
       T2.EntName,
       '',
       '',
       'R01' as relationship,   
       null,
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'IC',
     --  I_ETL_FLAG,
   	 --  I_SCHEDULE_CD,
       '01',
       '0117',  
       '1'
from rwuser.F_GS_ENTBASE T1
inner join rwuser.F_GS_ENTFRPOSITION GF  on T1.ENTSEQ=GF.ENTSEQ1
inner join rwuser.F_GS_ENTBASE T2 on T2.ENTSEQ=GF.ENTSEQ2
inner join rwuser.RWMS_CUST_INFO c on c.cust_name=T1.entName
left join rwuser.RWMS_CUST_INFO u on u.cust_name=T2.entName
where T1.ENTNAME <> T2.ENTNAME and T1.ENTNAME is not null
 
/* 新增 关联关系 工商数据解析 end by zqian 2017-1-22 19:44:59 */


/* 新增 关联关系 解析征信报告33小类 add zqian 2017-1-22 19:44:59*/      
  union all
	SELECT
       c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       '',
       t.corp_name,
       '',
       '',
       'R10' as relationship,
       '',
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'CL',
    --   I_ETL_FLAG,
    --   I_SCHEDULE_CD,
       '01',
      CASE
       WHEN rel like '%集团企业关联家族企业%' THEN
         '1001'
       WHEN rel like '%集团企业关联母子关系%' THEN
         '1002'
       end,
       '2'
  FROM RWMS_CL_CRD_REL_ENT_C T
  inner join rwuser.RWMS_CUST_INFO c
    on t.loan_card_encode = c.loan_card_id
  left join rwuser.RWMS_CUST_INFO u
    on t.corp_name = u.cust_name
     where rel like '%集团企业关联%'
      and c.cust_name is not null
      and u.cert_id is not null     

	union all
	SELECT
       c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       '',
       t.corp_name,
       '',
       '',
       'R12' as relationship,
       '',
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'CL',
    --   I_ETL_FLAG,
    --   I_SCHEDULE_CD,
       '01',
      CASE
       WHEN rel like '%企业投资关联对外投资%' THEN
         '1201'
       WHEN rel like '%企业投资关联被投资%' THEN
         '1202'
       WHEN rel like '%企业投资关联相互投资%' THEN
         '1203'
       end,
       '2'
  FROM RWMS_CL_CRD_REL_ENT_C T
  inner join rwuser.RWMS_CUST_INFO c
    on t.loan_card_encode = c.loan_card_id
  left join rwuser.RWMS_CUST_INFO u
    on t.corp_name = u.cust_name
     where rel like '%企业投资关联%'
      and c.cust_name is not null
      and u.cert_id is not null 
 union all
	SELECT
       c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       '',
       t.corp_name,
       '',
       '',
       'R03' as relationship,
       '',
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'CL',
    --   I_ETL_FLAG,
    --   I_SCHEDULE_CD,
       '01',
      CASE
       WHEN rel like '%企业担保关联相互担保%' THEN
         '0301'
       WHEN rel like '%企业担保关联被担保%' THEN
         '0302'
       WHEN rel like '%企业担保关联对外担保%' THEN
         '0303'
       WHEN rel like '%企业担保人关联兼法人代表%' THEN
 				 '0304'
       WHEN rel like '%企业担保人关联兼总经理%' THEN
			   '0305'
       WHEN rel like '%企业担保人关联兼财务负责人%' THEN
 			   '0306'
       WHEN rel like '%企业担保人关联兼个人出资%' THEN
 				 '0307'
       WHEN rel like '%企业担保人关联兼个人担保%' THEN
 				'0308'  
       end,
       '2'
  FROM RWMS_CL_CRD_REL_ENT_C T
  inner join rwuser.RWMS_CUST_INFO c
    on t.loan_card_encode = c.loan_card_id
  left join rwuser.RWMS_CUST_INFO u
    on t.corp_name = u.cust_name
     where rel like '%企业担保人关联%'
      and c.cust_name is not null
      and u.cert_id is not null    

union all
	SELECT
       c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       '',
       t.corp_name,
       '',
       '',
       'R01' as relationship,
       '',
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'CL',
    --   I_ETL_FLAG,
    --   I_SCHEDULE_CD,
       '01',
      CASE
       WHEN rel like '%法人代表关联兼法人代表%' THEN
         '0101'
       WHEN rel like '%法人代表关联兼总经理%' THEN
         '0102'
       WHEN rel like '%法人代表关联兼财务负责人%' THEN
         '0103'
       WHEN rel like '%法人代表关联兼个人出资%' THEN
 				 '0104'
       WHEN rel like '%法人代表关联兼个人担保%' THEN
			   '0105'
       WHEN rel like '%总经理关联兼法人代表%' THEN
 			   '0106'
       WHEN rel like '%总经理关联兼总经理%' THEN
 				 '0107'
       WHEN rel like '%总经理关联兼财务报表%' THEN
 				'0108'  
 			 WHEN rel like '%总经理关联兼个人出资%' THEN
         '0109'
       WHEN rel like '%总经理关联兼个人担保%' THEN
         '0110'
       WHEN rel like '%财务负责人关联兼法人代表%' THEN
         '0111'
       WHEN rel like '%财务负责人关联兼总经理%' THEN
 				 '0112'
       WHEN rel like '%财务负责人关联兼财务负责人%' THEN
			   '0113'
       WHEN rel like '%财务负责人关联兼个人出资%' THEN
 			   '0114'
       WHEN rel like '%财务负责人关联兼个人担保%' THEN
 				 '0115'  				
       end,
       '2'
  FROM RWMS_CL_CRD_REL_ENT_C T
  inner join rwuser.RWMS_CUST_INFO c
    on t.loan_card_encode = c.loan_card_id
  left join rwuser.RWMS_CUST_INFO u
    on t.corp_name = u.cust_name
     where rel like '%财务负责人关联%'
      and c.cust_name is not null
      and u.cert_id is not null         
union all
	SELECT
       c.cust_id,
       c.cust_name,
       c.cert_type,
       c.cert_id,
       '',
       t.corp_name,
       '',
       '',
       'R02' as relationship,
       '',
       null,
       to_char(CURRENT DATE,'yyyymmdd'),
       NULL,
       'CL',
    --   I_ETL_FLAG,
    --   I_SCHEDULE_CD,
       '01',
      CASE
       WHEN rel like '%出资人关联兼法人代表%' THEN
         '0201'
       WHEN rel like '%出资人关联兼总经理%' THEN
         '0202'
       WHEN rel like '%出资人关联兼财务负责人%' THEN
         '0203'
       WHEN rel like '%出资人关联兼个人出资%' THEN
 				 '0204'
       WHEN rel like '%出资人关联兼个人担保%' THEN
			   '0205'
       end,
       '2'
  FROM RWMS_CL_CRD_REL_ENT_C T
  inner join rwuser.RWMS_CUST_INFO c
    on t.loan_card_encode = c.loan_card_id
  left join rwuser.RWMS_CUST_INFO u
    on t.corp_name = u.cust_name
     where rel like '%出资人关联兼%'
      and c.cust_name is not null
      and u.cert_id is not null          
   with ur ;
                
 /* 新增 关联关系 end by zqian 2017-1-22 20:44:59*/          

  COMMIT;--
  ---去重插入目标表

EXECUTE IMMEDIATE 'alter table js_relative_info activate not logged initially with empty table';--


insert into js_relative_info
(CUSTOMERID  ,		--客户号
CUSTOMERNAME  ,		--客户名称
CERTTYPE  ,				--证件类型
CERTID  ,					--证件号码
RELACUSTOMERID  ,	--关联客户编号
RELACUSTOMERNAME ,--关联客户名称
RELACERTTYPE  ,		--关联证件类型
RELACERTID  ,			--关联证件号码
RELATIONSHIP  ,		--关联关系大类
INVESTMENTPROP  , --股权占比(是否都是百分比)
INVESTMENTSUM,		--出资金额
ETL_INPUTDATE  , 	--ETL加工时间
ETL_UPDATEDATE  ,	--ETL更新时间
FROM_SYSTEM  ,		--系统来源
ETL_FLAG  ,				--ETL标志
SCHEDULE_CD ,			--调度号
ITEM1 ,
ITEM2 ,						--关系明细
SYSTEMFROM
)
SELECT CUSTOMERID  ,
CUSTOMERNAME  ,
CERTTYPE  ,
CERTID  ,
RELACUSTOMERID  ,
RELACUSTOMERNAME  ,
RELACERTTYPE  ,
RELACERTID  ,
RELATIONSHIP  ,
INVESTMENTPROP  ,   ---是否都是百分比
INVESTMENTSUM,
ETL_INPUTDATE  ,
ETL_UPDATEDATE  ,
FROM_SYSTEM  ,
ETL_FLAG  ,
SCHEDULE_CD ,
ITEM1 ,
ITEM2 ,
SYSTEMFROM
 from (
SELECT CUSTOMERID  ,
CUSTOMERNAME  ,
CERTTYPE  ,
CERTID  ,
RELACUSTOMERID  ,
RELACUSTOMERNAME  ,
RELACERTTYPE  ,
RELACERTID  ,
RELATIONSHIP  ,
INVESTMENTPROP  ,   ---是否都是百分比
INVESTMENTSUM,
ETL_INPUTDATE  ,
ETL_UPDATEDATE  ,
FROM_SYSTEM  ,
ETL_FLAG  ,
SCHEDULE_CD ,
ITEM1 ,ITEM2 ,SYSTEMFROM,
row_number() over (partition by t.customerid,t.customername,RELACUSTOMERID,RELACUSTOMERNAME,RELATIONSHIP,ITEM2 order by item1 desc) as rn  --添加判断小类是否相同  add by zqian 
FROM js_relative_info_TMP T)
where rn=1;--

commit;--
  ----更新五个属性字段值
 update js_relative_info t
  set t.colortype=(case when t.relationship='R01' THEN '0x030103'
                       when t.relationship='R02' THEN '0x5ca70a'
                       when t.relationship='R03' THEN '0xff0000'
                       when t.relationship='R05' THEN '0xb802c3'
                       when t.relationship='R10' THEN '0x0066ff' 
                       when t.relationship='R11' THEN '0xfad333'
                       when t.relationship='R12' THEN '0x01FFF0'
                       END ),
      T.SOURCETYPE=(CASE WHEN T.CUSTOMERID IS NOT NULL THEN '0'
                           ELSE T.SYSTEMFROM END),
      T.RELASOURCETYPE=(CASE WHEN T.RELACUSTOMERID IS NOT NULL THEN '0'
                           ELSE T.SYSTEMFROM END),

      T.CUSTOMERTYPE=(CASE WHEN T.CERTTYPE like 'Ind%'  THEN 'P'
                           WHEN (T.CERTTYPE like 'Ent%' or T.CERTTYPE is null or T.CERTTYPE='') THEN 'C'
                           ELSE NULL END),
      T.RELACUSTOMERTYPE=(CASE WHEN T.RELACERTTYPE like 'Ind%'  THEN 'P'
                           WHEN (T.RELACERTTYPE like 'Ent%' or T.RELACERTTYPE is null  or T.RELACERTTYPE='') THEN 'C'
                           ELSE NULL END);--
   COMMIT;--
 EXECUTE IMMEDIATE 'alter table RWMSUSER.RELATIVE_XMLONLY_INFO activate not logged initially with empty table';--

  INSERT INTO RWMSUSER.RELATIVE_XMLONLY_INFO
      (CUSTOMERID,				--客户号
       CUSTOMERNAME,			--客户名称
       CERTTYPE,					--证件类型
       CERTID,						--证件号码
       RELACUSTOMERID,		--关联客户编号
       RELACUSTOMERNAME,	--关联客户名称
       RELACERTTYPE,			--关联证件类型
       RELACERTNAME,			--关联证件号码

       RELATIONSHIP,			--关联关系大类
       INVESTMENTSUM,			--出资金额
       INVESTMENTPROP,		--股权占比(是否都是百分比)
       FROM_SYSTEM,				--系统来源
       COLORTYPE,
       SOURCETYPE,
       RELASOURCETYPE,		
       CUSTOMERTYPE,
       RELATIONDETIAL,		--关系明细
       RELACUSTOMERTYPE,
       ORDERDESCNUM)
      SELECT CUSTOMERID,
              CUSTOMERNAME,
              CERTTYPE,
              CERTID,
              RELACUSTOMERID,
              RELACUSTOMERNAME,
              RELACERTTYPE,
              RELACERTID,

              RELATIONSHIP,
              INVESTMENTSUM,
              INVESTMENTPROP,
              FROM_SYSTEM,
              COLORTYPE,
              SOURCETYPE,
              RELASOURCETYPE,
              CUSTOMERTYPE,
              RELATIONDETIAL,
              RELACUSTOMERTYPE,
              ORDERDESCNUM
         FROM (SELECT DISTINCT CUSTOMERID,
                               CUSTOMERNAME,
                               CERTTYPE,
                               CERTID,
                               RELACUSTOMERID,
                               RELACUSTOMERNAME,
                               RELACERTTYPE,
                               RELACERTID,

                               RELATIONSHIP,
                               INVESTMENTSUM,
                               INVESTMENTPROP,
                               FROM_SYSTEM,
                               COLORTYPE,
                               SOURCETYPE,
                               RELASOURCETYPE,
                               CUSTOMERTYPE,
                               ITEM2 as RELATIONDETIAL,
                               RELACUSTOMERTYPE,
                               CASE
                                 WHEN RELATIONSHIP = 'R02' THEN
                                  ROW_NUMBER()
                                  OVER(PARTITION BY CUSTOMERID,
                                       RELATIONSHIP ORDER BY INVESTMENTPROP DESC,
                                       INVESTMENTSUM DESC)
                                 ELSE
                                  1
                               END AS ORDERDESCNUM
                 FROM js_relative_info RI
                WHERE (RI.RELATIONSHIP IN
                      ('R01',  'R02', 'R03', 'R05','R10','R11','R12')
                      and ri.Relacerttype is not null)
                      or ( RI.RELATIONSHIP ='R02'
                      and ri.Relacerttype is not null))
        WHERE ORDERDESCNUM <= 10;--
         commit;--
-- 过程正常结束
    SET v_point = '9999';--
    CALL proc_dblog(v_data_dt,v_proc_name ,'info' ,v_sql_code,v_point);--
    SET result_code = 0 ;--
    CALL PROC_RUN_LOG(p_etl_date,v_proc_name,v_data_dt,result_code);--
END@
SET CURRENT SCHEMA = "RWUSER  "@
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","RWUSER"@

