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

select * from info_silver.tb_nsip_t_filled_order a



---------------------------------------------------------------------------------------

/* HHT昨日开户 */
select partner_id,count(distinct firm_id) from tb_silver_user_stat@silver_std
where to_char(open_account_time, 'yyyymmdd') between
  and partner_id = 'hht'
group by partner_id;

/* HHT昨日入金*/
select
  count (case when aa.pid='hht' and aa.mindate=to_char(sysdate-1,'yyyymmdd')  then aa.firmid end )
    from
  (select trans.firmid, min(trans.fdate) as mindate,trans.partnerid as pid
   from silver_njs.history_transfer@silver_std trans
   where inorout='A'
   group by firmid,partnerid) aa;

/* HHT昨日有效新入金*/


SELECT ccc.refer_1_type,
  count(DISTINCT CASE WHEN to_char(bbb.date1, 'yyyymmdd') = to_char(sysdate - 1, 'yyyymmdd')
  THEN bbb.id END)
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
         WHERE partnerid = 'hht') suba
        LEFT JOIN
        (SELECT
           firmid,
           (CASE WHEN inorout = 'A'
             THEN inoutmoney
            WHEN inorout = 'B'
              THEN (-1) * inoutmoney END) AS summoney,
           realdate
         FROM silver_njs.history_transfer@silver_std
         WHERE partnerid = 'hht') subb
          ON suba.firmid = subb.firmid AND suba.realdate >= subb.realdate

      GROUP BY suba.firmid, suba.realdate) aaa
   WHERE aaa.money >= 50
   GROUP BY aaa.firm_id) bbb
left join info_silver.dw_user_account ccc
  on bbb.id= ccc.firm_id
group by ccc.refer_1_type

/* HHT昨日净入金*/
select
sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money --净入金
   from silver_njs.history_transfer@silver_std io
   where io.partnerid='hht' and io.fdate = to_char(sysdate - 1, 'yyyymmdd')
/* HHT昨日交易额*/
select
sum(deal.CONTQTY) as money -- 总交易额
   from info_silver.ods_history_deal deal
   where deal.partner_id='hht' and deal.fdate = to_char(sysdate - 1, 'yyyymmdd')

/* HHT昨日点差*/
select
sum( deal.contnum*8) as money -- 点差
  from info_silver.ods_history_deal deal
  where deal.partner_id ='hht'
  and deal.fdate = to_char(sysdate - 1, 'yyyymmdd')

SELECT sum(weight*8)
FROM info_silver.tb_nsip_t_filled_order
where to_char(trade_date,'yyyymmdd') = to_char(sysdate - 1, 'yyyymmdd')
and status =1




SELECT FEE + CLOSE_PL + TODAY_SETTLE_PL + CURRENT_DELAY_FEE AS HHTPROFIT,
       FEE_TB.FEE AS HHT_FEE,
       SPOTGAIN AS HHT_POINT,
       FEE_TB.CLOSE_PL - SPOTGAIN AS HHT_CLOSE_PL ,
       HOLD_TB.TODAY_SETTLE_PL AS HHT_SETTLE_PL,
       CURRENT_DELAY_FEE HHT_OVERNIGHT
  FROM (SELECT TRADE_DATE,
               SUM(POINT * COMMODITY_FACTOR * QUANTITY) AS SPOTGAIN
          FROM (SELECT TRADE_DATE, COMMODITY_ID, WEIGHT, QUANTITY
                  FROM NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE T1
                 WHERE T1.MEMBER_ID = '001170223'
                   AND ((POSITION_DIRECTION = 1 AND POSITION_OPERATION=0) OR (POSITION_DIRECTION = 2 AND POSITION_OPERATION=1))
                   AND to_char(TRADE_DATE,'yyyymmdd')=TO_CHAR(sysdate-1,'YYYYMMDD')
                   ) T1
          LEFT JOIN NSIP_MARKET.TB_NSIP_Q_COMMODITY_DETAIL@LINK_NSIP_MARKET T2
            ON T1.COMMODITY_ID = T2.COMMODITY_ID
         GROUP BY TRADE_DATE) POINT_TB
  LEFT JOIN (SELECT TRADE_DATE,
                    SUM(FEE-(ROUND(FEE*0.1875,2))) AS FEE,
                    SUM(CLOSE_PL) * -1 AS CLOSE_PL
               FROM NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE
              WHERE MEMBER_ID = '001170223'
                AND  to_char(TRADE_DATE,'yyyymmdd')=TO_CHAR(sysdate-1,'YYYYMMDD')
              GROUP BY TRADE_DATE) FEE_TB
    ON POINT_TB.TRADE_DATE = FEE_TB.TRADE_DATE
  LEFT JOIN (SELECT TRADE_DATE,
                    SUM(TODAY_SETTLE_PL) * -1 AS TODAY_SETTLE_PL,
                    SUM(CURRENT_DELAY_FEE) AS CURRENT_DELAY_FEE
               FROM NSIP_TRADE.TB_NSIP_T_POSITION_DETAIL_H@LINK_NSIP_TRADE
              WHERE MEMBER_ID = '001170223'
               AND STATUS=1
                AND to_char(TRADE_DATE,'yyyymmdd')=TO_CHAR(sysdate-1,'YYYYMMDD')
              GROUP BY TRADE_DATE) HOLD_TB
    ON POINT_TB.TRADE_DATE = HOLD_TB.TRADE_DATE


select * from NSIP_TRADE.TB_NSIP_T_POSITION_DETAIL_H@LINK_NSIP_TRADE


---------------------------------------------------------------------------------------
/*投顾短信-当日开单*/
select count(distinct user_id) from silver_consult.tb_crm_transfer_record@consul_std
where to_char(submit_time,'yyyymmdd') = to_char(sysdate-1,'yyyymmdd')
and process in (5,6) and valid = 1 and bia_group_id in (1,7,8,111)
/*投顾短信-当周开单*/
select count(distinct user_id) from silver_consult.tb_crm_transfer_record@consul_std
where to_char(submit_time,'yyyymmdd') between 20170501 and 20170504
and process in (5,6) and valid = 1 and bia_group_id in (1,7,8,111)

/*激活资金*/
select sum(nvl(aa.money1,0)+nvl(aa.money2,0))
  from
(
select a.user_id,avg(case when c.net_zcmoney is not null then c.net_zcmoney  end) as money1,
                 sum(case when d.inoutmoney is not null and d.inorout='A' then d.inoutmoney
                     when d.inoutmoney is not null and d.inorout='B' then -d.inoutmoney end) as money2
from silver_consult.tb_crm_transfer_record@consul_std a
join info_silver.dw_user_account b
  on a.user_id=b.crm_user_id
  and b.partner_id = 'hht'
left join info_silver.ods_order_zcmoney c
  on b.firm_id = c.firm_id
  and c.fdate = to_char(a.submit_time-3,'yyyymmdd')
  left join silver_njs.history_transfer@silver_std d
  on b.firm_id = d.firmid
  and d.fdate = to_char(a.submit_time,'yyyymmdd')
  and d.realdate < a.submit_time
where
 a.process in (5,6) and a.valid = 1
and a.bia_group_id in (1,7,8,111)
  and to_char(a.submit_time,'yyyymmdd') between '20170501' and '20170501'
group by a.user_id) aa


select * from silver_consult.tb_crm_transfer_record@consul_std where user_id =1000533062
select * from info_silver.ods_order_zcmoney where firm_id = '163170425136383'
select firm_id from info_silver.dw_user_account where crm_user_id = '1000533062'


/*后端净入金*/
select sum(case when c.inorout='A' then inoutmoney when c.inorout='B' then -inoutmoney end)
from silver_consult.tb_crm_transfer_record@consul_std a
join info_silver.dw_user_account b
  on a.user_id=b.crm_user_id
  and b.partner_id = 'hht'
join silver_njs.history_transfer@silver_std c
  on b.firm_id = c.firmid
and c.realdate > a.submit_time
where a.process in(5,6) and a.valid =1 and a.bia_group_id in (1,7,8,111)
and c.fdate between 20170501 and 20170504
and b.group_id in (1,7,8,111)

/*后端总维护用户数*/

select count(distinct a.user_id)
from silver_consult.tb_crm_transfer_record@consul_std a
join info_silver.dw_user_account b
  on a.user_id=b.crm_user_id
  and b.partner_id = 'hht'
where a.process in(5,6) and a.valid =1 and a.bia_group_id in (1,7,8,111)
and b.group_id in (1,7,8,111)

/*后端总维护用户交易额*/
select sum(c.contqty)
from silver_consult.tb_crm_transfer_record@consul_std a
join info_silver.dw_user_account b
  on a.user_id=b.crm_user_id
 and b.partner_id = 'hht'
  join info_silver.ods_history_deal c
  on b.firm_id = c.firmid
where a.process in(5,6) and a.valid =1 and a.bia_group_id in (1,7,8,111)
and b.group_id in (1,7,8,111)
and c.fdate = 20170504



/*后端维护过用户数*/

select count(distinct a.user_id)
from silver_consult.tb_crm_transfer_record@consul_std a
join info_silver.dw_user_account b
  on a.user_id=b.crm_user_id
  and b.partner_id = 'hht'
where a.process in(5,6) and a.valid =1
      and a.bia_group_id in (1,7,8,111)

/*后端维护过用户交易额*/

select sum(c.contqty)
from silver_consult.tb_crm_transfer_record@consul_std a
join info_silver.dw_user_account b
  on a.user_id=b.crm_user_id
 and b.partner_id = 'hht'
  join info_silver.ods_history_deal c
  on b.firm_id = c.firmid
where a.process in(5,6) and a.valid =1 and a.bia_group_id in (1,7,8,111)
and c.fdate = 20170504



/*后端资金量*/

select sum(c.net_zcmoney)
from silver_consult.tb_crm_transfer_record@consul_std a
join info_silver.dw_user_account b
  on a.user_id=b.crm_user_id
 and b.partner_id = 'hht'
  join info_silver.ods_order_zcmoney c
  on b.firm_id = c.firm_id
where a.process in(5,6) and a.valid =1 and a.bia_group_id in (1,7,8,111)
and b.group_id in (1,7,8,111)
and c.fdate = 20170504




select * from info_silver.SMS_SILVER_CONSULT where to_char(stat_date,'yyyymm')=201705

select * from silver_consult.tb_crm_transfer_record@consul_std where to_char(submit_time,'yyyymmdd') between 20170501 and 20170504


select sum(a.net_zcmoney) from info_silver.ods_order_zcmoney a
join info_silver.dw_user_account b on a.firm_id = b.firm_id where a.fdate=20170504 and a.partner_id='hht' and b.partner_id = 'hht';






