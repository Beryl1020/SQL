SELECT sum(case when PARTNER_ID = 'njs' and fdate between 20170401 and 20170430 then CONTQTY END),        --pmec/njs/平台交易额
  sum(case when PARTNER_ID = 'pmec' and fdate between 20170401 and 20170430 then CONTQTY END),
  sum(case when PARTNER_ID = 'hht' and fdate between 20170401 and 20170430 then CONTQTY END),
  sum(case when fdate between 20170401 and 20170430 then CONTQTY END)
  from info_silver.ods_history_deal

select * from info_silver.ods_crm_transfer_record where to_char(submit_time,'yyyymmdd')>=20170426
select * from tb_silver_user_stat@silver_std where firm_id = 163000000434471
select * from silver_consult.tb_crm_transfer_record@consul_std trans
where to_char(trans.submit_time,'yyyymmdd') between 20170401 and 20170431
and trans.process in(5,6) and trans.valid=1 and bia_group_id = 115





select sum(case when partnerid = 'pmec' then  trans.pmec_net_value_sub+trans.pmec_net_in_sub
  when partnerid='hht' then trans.hht_net_value_sub+trans.hht_net_in_sub END ) as 激活资金,
  count(distinct trans.user_id) as 流转单数,
  count(distinct trans.fia_id)
from silver_consult.tb_crm_transfer_record@consul_std trans
where to_char(trans.submit_time,'yyyymmdd') between 20170401 and 20170431
and trans.process in(5,6) and trans.valid=1
and bia_group_id in (111)


select '微销前端开单',count(distinct user_id),sum(pmec_net_value_sub+pmec_net_in_sub)
from info_silver.ods_crm_transfer_record
where process in (5,6) and valid=1
      and to_char(submit_time,'yyyymmdd') between 20170101 and 20170131
      and fgroup_id in (112,113,114,106)                                                                                  --微销前端开单

select 2 as subid,sum(case
                      when io.inorout='A' and io.partnerid='hht'
  then inoutmoney
                      when io.inorout='B' and io.partnerid='hht'
                        then (-1)*inoutmoney end ) as moneypmec,
  sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money                        --pmec、平台净入金
   from silver_njs.history_transfer@silver_std io
   where  io.fdate between 20170401 and 20170431
   group by 2





select '微销前端净入金',
  sum(case when partnerid = 'pmec' then  trans.pmec_net_value_sub+trans.pmec_net_in_sub
  when partnerid='hht' then trans.hht_net_value_sub+trans.hht_net_in_sub END )
from silver_consult.tb_crm_transfer_record@consul_std trans
where process in (5,6) and valid=1
      and to_char(submit_time,'yyyymmdd') between 20170401 and 20170431
      and bia_group_id in (1,7,8)



select '电销前端净入金',sum(pmec_net_value_sub+pmec_net_in_sub)
from info_silver.ods_crm_transfer_record
where process in (5,6) and valid=1
      and to_char(submit_time,'yyyymmdd') between 20170401 and 20170431
      and fgroup_id in (2,3,4,5,6,9,10,11,12,105)




select sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money
  from  silver_njs.history_transfer@silver_std io
where io.fdate between 20170401 and 20170431
     and io.partnerid ='pmec'


select * from silver_consult.tb_crm_transfer_record@consul_std

select sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money   -- 后端电销净入金
from silver_consult.tb_crm_transfer_record@consul_std trans
  join info_silver.dw_user_account u
  on trans.user_id = u.crm_user_id
  and u.partner_id in ('pmec','hht')
  join silver_njs.history_transfer@silver_std io
  on u.firm_id=io.firmid
where trans.process in (5,6) and trans.valid=1
and io.fdate between 20170401 and 20170431
  and trans.bia_group_id in(111)
and io.realdate > trans.submit_time
and io.partnerid in ('pmec','hht')


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









select count (distinct case when partner_id ='hht' and
                                 TO_CHAR(OPEN_ACCOUNT_TIME,'yyyymmdd')                      --开户用户
between 20170401 and 20170431  then firm_id end )
    from tb_silver_user_stat@silver_std


select
  count (case when aa.pid='hht' and aa.mindate between 20170401 and 20170431 then aa.firmid END )                         --新入金用户
    from
  (select trans.firmid, min(trans.fdate) as mindate,trans.partnerid as pid
   from silver_njs.history_transfer@silver_std trans
   where inorout='A'
   group by firmid,partnerid) aa







select count(distinct case when to_char(bbb.date1,'yyyymmdd') between 20170401 and 20170431 then bbb.id end), 0 from   --有效新入金用户
  (select aaa.firm_id id, min(aaa.realdate) as date1
  from
  (select suba.firmid as firm_id, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer@silver_std where partnerid='hht') suba
  left JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate
  from silver_njs.history_transfer@silver_std where partnerid='hht') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate

  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firm_id) bbb group by 0





    select                                                                                  --交易用户
         count (distinct case
                         when deal.partner_id='pmec' and deal.fdate between 20170401 and 20170430 then deal.firmid end) as cnt1,
         count (distinct case
                         when deal.partner_id='hht' and deal.fdate between 20170401 and 20170430 then deal.firmid end) as cnt2
       from info_silver.ods_history_deal deal
      where deal.fdate between 20170401 and 20170430
      --   and deal.ordersty<>151 --非强平





select sub1.money,sub2.money,sub3.money, sub3.money*0.00065+sub4.money+sub5.money,sub3.money*0.00065+sub5.money+sub6.money
    FROM

  (select 1 as subid,sum(io.inoutmoney) as money -- 总入金
    from silver_njs.history_transfer@silver_std io
    where io.inorout = 'A' and io.partnerid='pmec' and io.fdate between 20170401 and 20170431
    group by 1) sub1
  left join
  (select 2 as subid,sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money --净入金
   from silver_njs.history_transfer@silver_std io
   where io.partnerid='pmec' and io.fdate between 20170401 and 20170431
   group by 2) sub2
  on sub1.subid<>sub2.subid
  left join
  (select    3 as subid,sum(deal.CONTQTY) as money -- 总交易额
   from info_silver.ods_history_deal deal
   where deal.partner_id='pmec' and deal.fdate between 20170401 and 20170431

   group by 3) sub3
  on sub1.subid<>sub3.subid
  left join
   (select   4 as subid,
   sum(case
       when deal.wareid = 'GDAG' then deal.contnum*8
       when deal.wareid = 'GDPD' then deal.contnum*1000*0.48
       when deal.wareid = 'GDPT' then deal.contnum*1000*0.5 end) as money -- 点差
  from info_silver.ods_history_deal deal
  where deal.partner_id ='pmec'
  and deal.operation_src = 'open'
  and deal.fdate between 20170401 and 20170431
  group by 4) sub4
    ON sub1.subid<>sub4.subid
  left join
    (select 5 as subid, sum(case when flow.changetype=8 then (-1)*flow.AMOUNT end) as money -- 滞纳金
     from silver_njs.pmec_zj_flow@silver_std flow
     where to_char(flow.fdate,'yyyymmdd') between 20170401 and 20170431
     group by 5) sub5
    ON sub1.subid<>sub5.subid
  left JOIN
    (select 6 as subid, sum(case when flow.changetype in (9,10) then (-1)*flow.amount end) as money -- 头寸+点差
     from silver_njs.pmec_zj_flow@silver_std flow
      where to_char(flow.fdate,'yyyymmdd') between 20170401 and 20170431
      group by 6) sub6
    on sub1.subid<>sub6.subid



select type,sum(amount) from NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT
where to_char(create_time-0.25,'yyyymmdd') between 20170401 and 20170430
group by type
order by type



select sum(weight)*8 from info_silver.tb_nsip_t_filled_order
where to_char(trade_time-0.25,'yyyymmdd') between 20170401 and 20170431 and
 ((POSITION_DIRECTION = 1 AND POSITION_OPERATION=0) OR (POSITION_DIRECTION = 2 AND POSITION_OPERATION=1))

-- 7 滞纳金 5.6 盈亏

SELECT sum(trade_price*weight)*0.00065
FROM info_silver.tb_nsip_t_filled_order
where to_char(trade_Date,'yyyymmdd')  between 20170401 and 20170431
