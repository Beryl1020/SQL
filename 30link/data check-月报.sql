SELECT
  partner_id, sum(CONTQTY)                                                                     --各平台交易额
FROM info_silver.ods_history_deal
WHERE fdate BETWEEN 20170401 AND 20170430
GROUP BY partner_id


select
  sum (case when aa.fgroup_id in (2,3,4,5,6,9,10,11,12,105) then bb.contqty end),                             --电销、微销交易额
  sum(case when aa.fgroup_id in (106,112,113,114) then bb.contqty end)
  from
(select firm_id,fia_id,fgroup_id,submit_time from info_silver.ods_crm_transfer_record
where to_char(submit_time,'yyyymmdd') <= 20170430 and process in(5,6) and valid=1) aa
    join info_silver.ods_history_deal bb
  on aa.firm_id=bb.firmid
where bb.fdate between 20170401 and 20170430
and aa.submit_time < bb.trade_time



/*
select sum(bb.contqty)
   from
(select firm_id,fia_id,fgroup_id,submit_time from info_silver.ods_crm_transfer_record
where to_char(submit_time,'yyyymmdd') <= 20170331 and process in(5,6) and valid=1) aa
    join info_silver.ods_history_deal bb
  on aa.firm_id=bb.firmid
where bb.fdate between 20170301 and 20170331
and aa.submit_time < bb.trade_time
*/



select   4 as subid,
   sum(case
       when deal.wareid = 'GDAG' then deal.contnum*8
       when deal.wareid = 'GDPD' then deal.contnum*1000*0.48
       when deal.wareid = 'GDPT' then deal.contnum*1000*0.5 end) as money                           -- 点差
  from info_silver.ods_history_deal deal
  where deal.partner_id ='pmec'
  and deal.operation_src = 'open'
  and deal.fdate between 20170401 and 20170430
  group by 4


select 5 as subid, sum(case when flow.changetype=8 then (-1)*flow.AMOUNT end) as money
     from silver_njs.pmec_zj_flow@silver_std flow
     where to_char(flow.fdate,'yyyymmdd') between 20170401 and 20170430                           -- 滞纳金
     group by 5


select 6 as subid, sum(case when flow.changetype in (9,10) then (-1)*flow.amount end) as money  -- 头寸+点差
     from silver_njs.pmec_zj_flow@silver_std flow
      where to_char(flow.fdate,'yyyymmdd') between 20170401 and 20170430
      group by 6

select    3 as subid,sum(deal.CONTQTY)*0.00065 as money                      -- 手续费
   from info_silver.ods_history_deal deal
   where deal.partner_id='pmec' and deal.fdate between 20170401 and 20170430
   group by 3


select
  count(distinct deal.firmid),
  sum(deal.contqty)                                                                     --5万以上用户交易额
from
  info_silver.ods_history_deal deal
JOIN
  (
    SELECT
      firmid,
      max(net_assets) AS assets
    FROM silver_njs.tb_silver_data_center@silver_std
    WHERE hdate BETWEEN 20170401 and 20170430
    group by firmid
    ) ass
on deal.firmid=ass.firmid
where ass.assets>=50000
      and deal.partner_id='pmec'
and deal.fdate between 20170401 and 20170430



select sum(case when partnerid='njs'  and inorout='A' then inoutmoney                   ----广贵、南交净入金
            when partnerid='njs'  and inorout='B' then (-1)*inoutmoney end),
  sum(case when partnerid='pmec'  and inorout='A' then inoutmoney
      when partnerid='pmec'  and inorout='B' then (-1)*inoutmoney end)
  from silver_njs.history_transfer@silver_std
where fdate between 20170401 and 20170430


select
  count(distinct aa.user_id),
  count(distinct case when aa.partner_id='njs' then aa.firmid end),                         --月新交易用户数
  count(distinct case when aa.partner_id='pmec' then aa.firmid end)
  from
    (
      SELECT
        user_id,
        firmid,
        partner_id,
        MIN(fdate) AS mindate
      FROM info_silver.ods_history_deal
      GROUP BY firmid, partner_id,user_id
    ) aa
    join
    (select firmid,sum(case when inorout='A' then inoutmoney end) as inoutmoney
       from silver_njs.history_transfer@silver_std
    group by firmid) io
    on aa.firmid = io.firmid and io.inoutmoney>0
where aa.mindate between 20170401 and 20170430



select
  count(distinct user_id),
  count(distinct case when partner_id='njs' then firmid end),                               --月交易用户总数
  count(distinct case when partner_id='pmec' then firmid end)
from
  info_silver.ods_history_deal
where fdate between 20170401 and 20170430






select sum(aa.id)/19,sum(aa.idn)/19,sum(aa.idp)/19                                         --日均交易用户总数
  from
    (
      SELECT
        fdate,
        count(distinct user_id) AS id,
        count(distinct CASE WHEN partner_id = 'njs'
          THEN firmid END)      AS idn,
        count(distinct CASE WHEN partner_id = 'pmec'
          THEN firmid END)      AS idp
      FROM
        info_silver.ods_history_deal
      WHERE fdate BETWEEN 20170401 and 20170430
      group by fdate
    ) aa





select count(firmid),
  count(case when partner_id='njs' then firmid end),                                           --月订单总数
  count(case when partner_id='pmec' then firmid end)
  from info_silver.ods_history_deal
where fdate between 20170401 and 20170430


SELECT
  count(DISTINCT CASE
                 WHEN to_char(bbb.date1, 'yyyymmdd') BETWEEN 20170401 and 20170430            --有效入金数
                 and his.refer_1_type='Internal Channel'
                   THEN bbb.id END)/19,
  count(DISTINCT CASE
                 WHEN to_char(bbb.date1, 'yyyymmdd') BETWEEN 20170401 and 20170430
                 and his.refer_1_type='External Channel'
                   THEN bbb.id END)/19,
  count(DISTINCT CASE
                 WHEN to_char(bbb.date1, 'yyyymmdd') BETWEEN 20170401 and 20170430
                 and his.refer_1_type='Others'
                   THEN bbb.id END)/19
FROM
  (SELECT
     aaa.firm_id          id,
     min(aaa.realdate) AS date1
   FROM
     (SELECT
        suba.firmid        AS firm_id,
        suba.realdate      AS realdate,
        sum(subb.summoney) AS money
      FROM
        (SELECT
           firmid,
           realdate
         FROM silver_njs.history_transfer@silver_std
         WHERE partnerid = 'pmec') suba
        LEFT JOIN
        (SELECT
           firmid,
           (CASE WHEN inorout = 'A'
             THEN inoutmoney
            WHEN inorout = 'B'
              THEN (-1) * inoutmoney END) AS summoney,
           realdate
         FROM silver_njs.history_transfer@silver_std
        ) subb
          ON suba.firmid = subb.firmid AND suba.realdate >= subb.realdate

      GROUP BY suba.firmid, suba.realdate) aaa
   WHERE aaa.money >= 50
   GROUP BY aaa.firm_id) bbb
  join info_silver.ods_history_user his
  on bbb.id=his.firm_id













select name,group_id from info_silver.tb_crm_ia where group_id in (112,113,114)
select * from info_silver.ods_history_deal where partner_id='pmec'
select firm_id,refer_1_type from info_silver.ods_history_user