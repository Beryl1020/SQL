SELECT
  partner_id, sum(CONTQTY)                                                                  --各平台交易额
FROM ods_history_deal@silver_stat_urs_30_link
WHERE fdate BETWEEN 20170201 AND 20170228
GROUP BY partner_id


select
  sum (case when aa.fgroup_id in (2,3,4,5,6,9,10,11,12,105) then bb.contqty end),           --各销售方式交易额
  sum(case when aa.fgroup_id in (106,112,113,114) then bb.contqty end)
  from
(select firm_id,fia_id,fgroup_id from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link
where to_char(submit_time,'yyyymmdd') <= 20170228) aa
    join ods_history_deal@silver_stat_urs_30_link bb
  on aa.firm_id=bb.firmid
where bb.fdate between 20170201 and 20170228


select /*+driving_site(deal)*/ 4 as subid,
   sum(case
       when deal.wareid = 'GDAG' then deal.contnum*8
       when deal.wareid = 'GDPD' then deal.contnum*1000*0.48
       when deal.wareid = 'GDPT' then deal.contnum*1000*0.5 end) as money               -- 点差
  from ods_history_deal@SILVER_STAT_URS_30_LINK deal
  where deal.partner_id ='pmec'
  and deal.operation_src = 'open'
  and deal.fdate between 20170318 and 20170324
  group by 4                           -- 点差

select 5 as subid, sum(case when flow.changetype=8 then (-1)*flow.AMOUNT end) as money
     from silver_njs.pmec_zj_flow flow
     where to_char(flow.fdate,'yyyymmdd') between 20170318 and 20170324                   -- 滞纳金
     group by 5

select 6 as subid, sum(case when flow.changetype in (9,10) then (-1)*flow.amount end) as money -- 头寸+点差
     from silver_njs.pmec_zj_flow flow
      where to_char(flow.fdate,'yyyymmdd') between 20170318 and 20170324
      group by 6

select  /*+driving_site(deal)*/ 3 as subid,sum(deal.CONTQTY)*0.00065 as money                    -- 手续费
   from ods_history_deal@silver_stat_urs_30_link deal
   where deal.partner_id='pmec' and deal.fdate between 20170318 and 20170324
   group by 3


select /*driving_site(deal)*/
  count(deal.firmid),
  sum(deal.contqty)                                                                     --10万以上用户交易额
from
  ods_history_deal@silver_stat_urs_30_link deal
JOIN
  (
    SELECT
      firmid,
      max(net_assets) AS assets
    FROM silver_njs.tb_silver_data_center
    WHERE hdate BETWEEN 20170201 AND 20170228
    group by firmid
    ) ass
on deal.firmid=ass.firmid
where ass.assets>=100000
      and deal.partner_id='pmec'
and deal.fdate between 20170201 and 20170228



select sum(case when partnerid='pmec'  and inorout='A' then inoutmoney             ----广贵、南交净入金
            when partnerid='pmec'  and inorout='B' then (-1)*inoutmoney end),
  sum(case when partnerid='njs'  and inorout='A' then inoutmoney
      when partnerid='njs'  and inorout='B' then (-1)*inoutmoney end)
  from silver_njs.history_transfer
where fdate between 20170201 and 20170228


select
  count(case when aa.firmid)
    (
  SELECT firmid, partner_id, MIN (trade_time) AS mintime
  FROM ods_history_deal@silver_stat_urs_30_link
  GROUP BY firmid, partner_id
    ) aa








select * from ods_history_deal@silver_stat_urs_30_link