-- hht pmec, njs 开户数
select partner_id,count(distinct firm_id) from tb_silver_user_stat@silver_std
where to_char(open_account_time, 'yyyymmdd')=to_char(sysdate-1,'yyyymmdd')
group by partner_id;

select count(distinct user_id) from tb_silver_user_stat@silver_std
where to_char(open_account_time, 'yyyymmdd')=to_char(sysdate-1,'yyyymmdd')



-- njs pmec 新入金数
select count (case when aa.pid='pmec' and aa.mindate=to_char(sysdate-1,'yyyymmdd') then aa.firmid end),
  count (case when aa.pid='njs' and aa.mindate=to_char(sysdate-1,'yyyymmdd')  then aa.firmid end ),
  count (case when aa.pid='hht' and aa.mindate=to_char(sysdate-1,'yyyymmdd')  then aa.firmid end )
    from
  (select trans.firmid, min(trans.fdate) as mindate,trans.partnerid as pid
   from silver_njs.history_transfer@silver_std trans
   where inorout='A'
   group by firmid,partnerid) aa;

select count (distinct case when  aa.mindate=to_char(sysdate-1,'yyyymmdd') then bb.user_id end)
    from
  (select trans.firmid, min(trans.fdate) as mindate,trans.partnerid as pid
   from silver_njs.history_transfer@silver_std trans
   where inorout='A'
   group by firmid,partnerid) aa
join tb_silver_user_stat@silver_std bb
      on aa.firmid = bb.firm_id



-- hht 新入金数
select count(case when to_char(aa.mindate,'yyyymmdd')=to_char(sysdate-1,'yyyymmdd') then aa.fund_id end)
  FROM
    (
select fund_id, min(trade_date) as mindate
  from NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT
where charge_amount>0 AND ORDER_STATUS = 3 AND RECONC_STATUS = 2
group by fund_id) aa





-- njs pmec hht 净入金
select io.partnerid,
  sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money --净入金
   from silver_njs.history_transfer@silver_std io
   where io.fdate = to_char(sysdate-1,'yyyymmdd')
group by io.partnerid

select io.partnerid,
  sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money --净入金
   from silver_njs.history_transfer@silver_std io
     join tb_silver_user_stat@silver_std bb
      on io.firmid = bb.firm_id
   where io.fdate = to_char(sysdate-1,'yyyymmdd')
group by io.partnerid



-- njs pmec hht 交易额
select deal.partner_id,sum(deal.CONTQTY) as money -- 总交易额
   from info_silver.ods_history_deal deal
   where  deal.fdate = to_char(sysdate-1,'yyyymmdd')
group by deal.partner_id


-- njs pmec hht 交易人数
select deal.partner_id,count(distinct deal.firmid) as 交易人数 -- 总交易额
   from info_silver.ods_history_deal deal
   where  deal.fdate = to_char(sysdate-1,'yyyymmdd') --and deal.partner_id='njs'
     and contqty > 0
group by deal.partner_id;

select count(distinct deal.useR_id) as 交易人数 -- 总交易额
   from info_silver.ods_history_deal deal
   where  deal.fdate = to_char(sysdate-1,'yyyymmdd') --and deal.partner_id='njs'
     and contqty > 0

select count(distinct firmid) from silver_njs.history_deal@silveronline_link where fdate=20170426 and partner_id='njs'





SELECT sum(trade_price*weight)
FROM info_silver.tb_nsip_t_filled_order
where to_char(trade_Date,'yyyymmdd')= to_char(sysdate-1,'yyyymmdd') ;



select max(trade_Date) from info_silver.tb_nsip_t_filled_order
where to_char(trade_Date)= to_char(sysdate-1,'yyyymmdd')

