/* 电销激活资金 */

select sum(pmec_net_value_sub+pmec_net_in_sub) from info_silver.ods_crm_transfer_record
where  to_char(submit_time,'yyyymm')=201703
and valid=1 and process in(5,6)
and bgroup_id in(111);


/* 电销交易额*/

select sum(b.contqty)
from info_silver.ods_crm_transfer_record a
  join info_silver.ods_history_deal b
  on a.firm_id=b.firmid
  and a.submit_time < b.trade_time
  and b.fdate between 20170301 and 20170331
where  to_char(a.submit_time,'yyyymm') between 201701 and 201703
and a.valid=1 and a.process in(5,6)
and a.bgroup_id in(1,7,8);


select * from INFO_SILVER.ODS_CRM_TRANSFER_RECORD a
where to_char(a.SUBMIT_TIME,'yyyymmdd') between 20160901 and 20170431;







select DISTINCT FIA_ID FROM info_silver.ods_crm_transfer_record where  to_char(submit_time,'yyyymm')= 201703  AND fgroup_id in (2,3,4,5,6,9,10,11,12,105) and process in (5,6)

/* 16年留底资金投顾交易额*/

select sum(b.contqty)
from info_silver.ods_crm_transfer_record a
  join info_silver.ods_history_deal b
  on a.firm_id=b.firmid
where  to_char(a.submit_time,'yyyymm') < 201701
and a.valid=1 and a.process in(5,6)
and  a.submit_time < b.trade_time
and  b.fdate between 20170301 and 20170331

/* 16年留底资金互联网交易额*/

select sum(b.contqty)
from info_silver.ods_crm_transfer_record a
  join info_silver.ods_history_deal b
  on a.firm_id=b.firmid
where  to_char(a.submit_time,'yyyymm') < 201701
and a.valid=1 and a.process in(5,6)
and  a.submit_time < b.trade_time
and  b.fdate between 20170301 and 20170331



/*2016年有入金且1月前未开单用户1月交易额*/
select sum(b.contqty)
  from
    (SELECT
       firmid,
       min(CASE WHEN inorout = 'A'
         THEN realdate END) AS mindate
     FROM silver_njs.history_transfer@silver_std
     WHERE partnerid = 'pmec'
      GROUP BY firmid
    )a
    join info_silver.ods_history_deal b
    on a.firmid=b.firmid
where
  to_char(a.mindate, 'yyyymm') < 201701 /*首次入金时间*/
   and  b.fdate between 20170301 and 20170331 /*交易时间*/
AND
    a.firmid NOT IN
     (SELECT firm_id
      FROM info_silver.ods_crm_transfer_record
      WHERE process IN (5, 6) AND valid = 1
            AND to_char(submit_time, 'yyyymm' ) <= 201703) /*流转时间*/




/*2016年有入金且2017年1月开单用户流转前交易额*/
select sum(b.contqty)
  from
    (SELECT
       firmid,
       min(CASE WHEN inorout = 'A'
         THEN realdate END) AS mindate
     FROM silver_njs.history_transfer@silver_std
     WHERE partnerid = 'pmec'
      GROUP BY firmid
    )a
    join info_silver.ods_history_deal b
    on a.firmid=b.firmid

    join info_silver.ods_crm_transfer_record c
      on a.firmid=c.firm_id

where
  to_char(a.mindate, 'yyyymm') < 201701 /*首次入金时间*/
    and b.fdate between 20170301 and 20170331 /*交易时间*/
    and to_char(c.submit_time,'yyyymm') = 201703 /*流转时间*/
and c.submit_time > b.trade_time




/*1月有入金且2017年1月开单用户流转前交易额*/
select sum(b.contqty)
  from
    (SELECT
       firmid,
       min(CASE WHEN inorout = 'A'
         THEN realdate END) AS mindate
     FROM silver_njs.history_transfer@silver_std
     WHERE partnerid = 'pmec'
      GROUP BY firmid
    )a
    join info_silver.ods_history_deal b
    on a.firmid=b.firmid

    join info_silver.ods_crm_transfer_record c
      on a.firmid=c.firm_id

where
  to_char(a.mindate, 'yyyymm') = 201703 /*首次入金时间*/
    and b.fdate between 20170301 and 20170331 /*交易时间*/
    and to_char(c.submit_time,'yyyymm') = 201703  /*流转时间*/
and c.submit_time > b.trade_time



/*1月有入金且1月未开单用户1月交易额*/
select sum(b.contqty)
  from
    (SELECT
       firmid,
       min(CASE WHEN inorout = 'A'
         THEN realdate END) AS mindate
     FROM silver_njs.history_transfer@silver_std
     WHERE partnerid = 'pmec'
      GROUP BY firmid
    )a
    join info_silver.ods_history_deal b
    on a.firmid=b.firmid
where
  to_char(a.mindate, 'yyyymm') = 201703 /*首次入金时间*/
   and  b.fdate between 20170301 and 20170331/*交易时间*/
AND
    a.firmid NOT IN
     (SELECT firm_id
      FROM info_silver.ods_crm_transfer_record
      WHERE process IN (5, 6) AND valid = 1
            AND to_char(submit_time, 'yyyymm' ) between 201703 and 201703) /*流转时间*/