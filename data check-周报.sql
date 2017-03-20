--  Part 1. 当周交易额

(SELECT sum(case when PARTNER_ID = 'njs' and fdate between 20170225 and 20170303 then CONTQTY END) as n1,
  sum(case when PARTNER_ID = 'njs' and fdate between 20170201 and 20170303 then CONTQTY END) as n2,
  sum(case when PARTNER_ID = 'njs' and fdate between 20170101 and 20170303 then CONTQTY END) as n3,
  sum(case when PARTNER_ID = 'njs' and fdate between 20170101 and 20170303 then CONTQTY END) as n4
  from ods_history_deal@silver_stat_urs_30_link) --njs 周、月、季度、年总交易额
union

(SELECT sum(case when PARTNER_ID = 'pmec' and fdate between 20170225 and 20170303 then CONTQTY END) as p1,
  sum(case when PARTNER_ID = 'pmec' and fdate between 20170201 and 20170303 then CONTQTY END) as p2,
  sum(case when PARTNER_ID = 'pmec' and fdate between 20170101 and 20170303 then CONTQTY END) as p3,
  sum(case when PARTNER_ID = 'pmec' and fdate between 20170101 and 20170303 then CONTQTY END) as p4
  from ods_history_deal@silver_stat_urs_30_link) --pmec 周、月、季度、年总交易额




-- Part 2. 平台基础数据
  SELECT sum(case when PARTNER_ID = 'pmec' and fdate between 20170225 and 20170303 then CONTQTY END)/5,
  sum(case when fdate between 20170225 and 20170303 then CONTQTY END)/5
  from ods_history_deal@silver_stat_urs_30_link -- pmec日均交易额，平台日均交易额

union all

  select sum(case when partnerid='pmec' and fdate between 20170225 and 20170303 and inorout='A' then inoutmoney
            when partnerid='pmec' and fdate between 20170225 and 20170303 and inorout='B' then (-1)*inoutmoney end),
  sum(case when fdate between 20170225 and 20170303 and inorout='A' then inoutmoney
      when fdate between 20170225 and 20170303 and inorout='B' then (-1)*inoutmoney end)
  from silver_njs.history_transfer -- pmec净入金、平台净入金

UNION all

  select sum(case when PARTNER_ID = 'pmec' then NET_ASSETS END),
  sum(NET_ASSETS)
  from silver_njs.tb_silver_data_center
  where hdate=20170303 -- pmec净资产、平台净资产

union ALL

  select sum(a.cnt1)/5,sum(a.cnt2)/5
    from
      (select /*+driving_site(deal)*/  deal.fdate,
         count (distinct case when deal.partner_id='pmec' and deal.fdate between 20170225 and 20170303 then deal.firmid end) as cnt1,
         count (distinct case when deal.fdate between 20170225 and 20170303 then deal.firmid end) as cnt2
       from ods_history_deal@silver_stat_urs_30_link deal
      where deal.fdate between 20170225 and 20170303
         and deal.ordersty<>151
      group by deal.fdate) a -- pmec日均交易用户，平台日均交易用户

union ALL

  select count (distinct case when partner_id ='pmec' and TO_CHAR(OPEN_ACCOUNT_TIME,'yyyymmdd') between 20170225 and 20170303 then firm_id end )/5,
    count (distinct case when TO_CHAR(OPEN_ACCOUNT_TIME,'yyyymmdd') between 20170225 and 20170303 then firm_id end )/5
    from tb_silver_user_stat -- pmec日均入金用户，平台日均入金用户

union ALL

  select count (case when aa.pid='pmec' and aa.mindate between 20170225 and 20170303 then aa.firmid end)/5,
  count (case when aa.mindate between 20170225 and 20170303 then aa.firmid END )/5
    from
  (select trans.firmid, min(trans.fdate) as mindate,trans.partnerid as pid
   from silver_njs.history_transfer trans
   where inorout='A'
   group by firmid,partnerid) aa -- pmec日均首次入金用户，平台日均首次入金用户

union ALL
  select count(distinct case when to_char(bbb.date1,'yyyymmdd') between 20170225 and 20170303 then bbb.id end)/5, 0 from
  (select aaa.firm_id id, min(aaa.realdate) as date1
  from
  (select suba.firmid as firm_id, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer where partnerid='pmec') suba
  left JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate
  from silver_njs.history_transfer where partnerid='pmec') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate

  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firm_id) bbb group by 0 --pmec新增有效净入金用户，第二列为空

-- Part 4. 广贵所数据
  select sub1.money,sub2.money,sub3.money, sub3.money*0.00065+sub4.money+sub5.money,sub3.money*0.00065+sub5.money+sub6.money
    FROM

  (select 1 as subid,sum(io.inoutmoney) as money -- 总入金
    from silver_njs.history_transfer io
    where io.inorout = 'A' and io.partnerid='pmec' and io.fdate between 20170225 and 20170303
    group by 1) sub1
  left join
  (select 2 as subid,sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money --净入金
   from silver_njs.history_transfer io
   where io.partnerid='pmec' and io.fdate between 20170225 and 20170303
   group by 2) sub2
  on sub1.subid<>sub2.subid
  left join
  (select  /*+driving_site(deal)*/ 3 as subid,sum(deal.CONTQTY) as money -- 总交易额
   from ods_history_deal@silver_stat_urs_30_link deal
   where deal.partner_id='pmec' and deal.fdate between 20170225 and 20170303

   group by 3) sub3
  on sub1.subid<>sub3.subid
  left join
   (select /*+driving_site(deal)*/ 4 as subid,
   sum(case
       when deal.wareid = 'GDAG' then deal.contnum*8
       when deal.wareid = 'GDPD' then deal.contnum*1000*0.48
       when deal.wareid = 'GDPT' then deal.contnum*1000*0.5 end) as money -- 点差
  from ods_history_deal@SILVER_STAT_URS_30_LINK deal
  where deal.partner_id ='pmec'
  and deal.operation_src = 'open'
  and deal.fdate between 20170225 and 20170303
  group by 4) sub4
    ON sub1.subid<>sub4.subid
  left join
    (select 5 as subid, sum(case when flow.changetype=8 then (-1)*flow.AMOUNT end) as money -- 滞纳金
     from silver_njs.pmec_zj_flow flow
     where to_char(flow.fdate,'yyyymmdd') between 20170225 and 20170303
     group by 5) sub5
    ON sub1.subid<>sub5.subid
  left JOIN
    (select 6 as subid, sum(case when flow.changetype in (9,10) then (-1)*flow.amount end) as money -- 头寸+点差
     from silver_njs.pmec_zj_flow flow
      where to_char(flow.fdate,'yyyymmdd') between 20170225 and 20170303
      group by 6) sub6
    on sub1.subid<>sub6.subid

-- Part 9. 南交、广贵内外推折算交易额
  select sum( case when deal.partner_id='njs' and refer.refer_1_type='Internal Channel' then deal.CONTQTY end) as njs内推,
    sum( case when deal.partner_id='njs' and refer.refer_1_type='External Channel' then deal.CONTQTY end) as njs外推,
    sum( case when deal.partner_id='njs' and (refer.refer_1_type not in ('Internal Channel','External Channel') or refer.refer_1_type is null) then deal.CONTQTY end) as njs其他,
  sum( case when deal.partner_id='pmec' and refer.refer_1_type='Internal Channel' then deal.CONTQTY end) as pmec内推,
    sum( case when deal.partner_id='pmec' and refer.refer_1_type='External Channel' then deal.CONTQTY end) as pmec外推,
      sum( case when deal.partner_id='pmec' and (refer.refer_1_type not in ('Internal Channel','External Channel') or refer.refer_1_type is null) then deal.CONTQTY end) as pmec其他
  from ods_history_deal@silver_stat_urs_30_link deal
left join ods_history_user@silver_stat_urs_30_link refer
    on deal.firmid=refer.firm_id
    where deal.fdate between 20170225 and 20170303

-- Part 6 每周电销资源转化情况


select '电销前端开单',count(distinct user_id)
from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link
where process in (5,6) and valid=1
      and to_char(submit_time,'yyyymmdd') between 20170225 and 20170303
      and fgroup_id in (2,3,4,5,6,9,10,11,12,105) --电销前端开单

union all

SELECT
  '电销当周接单当周有效开仓',count (aa.id)
FROM
  (SELECT /*+driving_site(deal)*/
     distinct trans.firm_id                                         AS id,
     max(trans.PMEC_NET_VALUE_SUB + trans.PMEC_NET_IN_SUB) AS num1,
     sum(CASE WHEN deal.wareid = 'GDAG'
       THEN deal.contnum
         WHEN deal.wareid = 'GDPT'
           THEN deal.contnum * 56
         WHEN deal.wareid = 'GDPD'
           THEN deal.contnum * 30 END)                     AS num2
   FROM info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
     LEFT JOIN ods_history_deal@silver_stat_urs_30_link deal
       ON trans.firm_id = deal.firmid
   WHERE to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170225 AND 20170303
         AND trans.fgroup_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105)
         AND trans.process IN (5, 6) AND trans.valid = 1
         AND (deal.trade_time > trans.submit_time)
         AND deal.fdate <=20170303
   GROUP BY trans.firm_id) aa
where (aa.num1 < 100000 AND aa.num2 >= 30)
       or (aa.num1 < 200000 AND aa.num2 >= 60)
        or (aa.num1 < 300000 AND aa.num2 >= 120)
        or (aa.num1 < 500000 AND aa.num2 >= 180)
         or (aa.num1 < 1000000 AND aa.num2 >= 240)
         or (aa.num1 < 2000000 AND aa.num2 >= 480)
          or (aa.num1 >= 2000000 AND aa.num2 >= 720)              -- 有效开仓

