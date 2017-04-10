SELECT sum(case when PARTNER_ID = 'njs' and fdate between 20170101 and 20170131 then CONTQTY END),        --pmec/njs/平台交易额
  sum(case when PARTNER_ID = 'pmec' and fdate between 20170101 and 20170131 then CONTQTY END),
  sum(case when fdate between 20170101 and 20170131 then CONTQTY END)
  from info_silver.ods_history_deal






select sum(trans.pmec_net_value_sub+trans.pmec_net_in_sub) as 激活资金,
  count(distinct trans.firm_id) as 流转单数,
  count(distinct trans.fia_id)
from info_silver.ods_crm_transfer_record trans
where to_char(trans.submit_time,'yyyymmdd') between 20170101 and 20170131
and trans.process in(5,6) and trans.valid=1


select '微销前端开单',count(distinct user_id),sum(pmec_net_value_sub+pmec_net_in_sub)
from info_silver.ods_crm_transfer_record
where process in (5,6) and valid=1
      and to_char(submit_time,'yyyymmdd') between 20170101 and 20170131
      and fgroup_id in (112,113,114,106)                                                                                  --微销前端开单

select 2 as subid,sum(case
                      when io.inorout='A' and io.partnerid='pmec'
  then inoutmoney
                      when io.inorout='B' and io.partnerid='pmec'
                        then (-1)*inoutmoney end ) as moneypmec,
  sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money                        --pmec、平台净入金
   from silver_njs.history_transfer@silver_std io
   where  io.fdate between 20170101 and 20170131
   group by 2





select '微销前端净入金',sum(pmec_net_value_sub+pmec_net_in_sub)
from info_silver.ods_crm_transfer_record
where process in (5,6) and valid=1
      and to_char(submit_time,'yyyymmdd') between 20170101 and 20170131
      and fgroup_id in (112,113,114,106)



select '电销前端净入金',sum(pmec_net_value_sub+pmec_net_in_sub)
from info_silver.ods_crm_transfer_record
where process in (5,6) and valid=1
      and to_char(submit_time,'yyyymmdd') between 20170101 and 20170131
      and fgroup_id in (2,3,4,5,6,9,10,11,12,105)







select sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money   -- 后端电销净入金
from info_silver.ods_crm_transfer_record trans
  join silver_njs.history_transfer@silver_std io
  on trans.firm_id=io.firmid
where trans.process in (5,6) and trans.valid=1
and io.fdate between 20170101 and 20170131
  and trans.cur_bgroup_id in(1,7,8)
and io.realdate > trans.submit_time


select sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money   -- 后端微销净入金
from info_silver.ods_crm_transfer_record trans
  join silver_njs.history_transfer@silver_std io
  on trans.firm_id=io.firmid
where trans.process in (5,6) and trans.valid=1
and io.fdate between 20170101 and 20170131
  and trans.cur_bgroup_id in(111)
and io.realdate > trans.submit_time



select count(distinct fia_id) from info_silver.ods_crm_transfer_record
where process in (5,6) and valid=1 and to_char(submit_time,'yyyymmdd') between 20170101 and 20170131









select count (distinct case when partner_id ='pmec' and
                                 TO_CHAR(OPEN_ACCOUNT_TIME,'yyyymmdd')                      --开户用户
between 20170101 and 20170131 then firm_id end )
    from tb_silver_user_stat@silver_std


select
  count (case when aa.pid='pmec' and aa.mindate between 20170101 and 20170131 then aa.firmid END )                         --新入金用户
    from
  (select trans.firmid, min(trans.fdate) as mindate,trans.partnerid as pid
   from silver_njs.history_transfer@silver_std trans
   where inorout='A'
   group by firmid,partnerid) aa


select count(distinct case when to_char(bbb.date1,'yyyymmdd') between 20170101 and 20170131 then bbb.id end), 0 from   --有效新入金用户
  (select aaa.firm_id id, min(aaa.realdate) as date1
  from
  (select suba.firmid as firm_id, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer@silver_std where partnerid='pmec') suba
  left JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate
  from silver_njs.history_transfer@silver_std where partnerid='pmec') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate

  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firm_id) bbb group by 0





    select                                                                                  --交易用户
         count (distinct case
                         when deal.partner_id='pmec' and deal.fdate between 20170201 and 20170228 then deal.firmid end) as cnt1,
         count (distinct case
                         when deal.fdate between 20170201 and 20170228 then deal.firmid end) as cnt2
       from info_silver.ods_history_deal deal
      where deal.fdate between 20170201 and 20170228
      --   and deal.ordersty<>151 --非强平
