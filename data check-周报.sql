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
      --   and deal.ordersty<>151 --非强平
      group by deal.fdate) a -- pmec日均交易用户，平台日均交易用户


union all

  select count (distinct case when partner_id ='pmec' and TO_CHAR(OPEN_ACCOUNT_TIME,'yyyymmdd') between 20170225 and 20170303 then firm_id end )/5,
    count (distinct case when TO_CHAR(OPEN_ACCOUNT_TIME,'yyyymmdd') between 20170225 and 20170303 then firm_id end )/5
    from tb_silver_user_stat -- pmec日均开户用户，平台日均开户用户

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


----Part 3 投顾新单数据
select sum(trans.pmec_net_value_sub+trans.pmec_net_in_sub) as 激活资金,
  count(distinct trans.firm_id) as 流转单数
from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
where to_char(trans.submit_time,'yyyymmdd') between 20170225 and 20170303
and trans.process in(5,6) and trans.valid=1





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





----Part 5 投顾近5周交易额、交易人数、净入金、收入及其在广贵所占比

select aaa.投顾广贵交易额,aaa.投顾广贵交易人数,bbb.后端用户平台收入,ccc.后端用户净入金
  from
    (SELECT
       /*+driving_site(trans)*/
       1               AS id,
       sum(广贵交易额)      AS 投顾广贵交易额,                                --后端投顾的广贵交易人数，广贵交易额
       sum(广贵交易人数) / 5 AS 投顾广贵交易人数
     FROM
       (
         SELECT
           /*+driving_site(deal)*/
           sum(deal.contqty)           AS 广贵交易额,
           count(DISTINCT deal.firmid) AS 广贵交易人数,
           deal.fdate
         FROM ods_history_deal@silver_stat_urs_30_link deal
           JOIN info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
             ON deal.firmid = trans.firm_id
         WHERE trans.cur_bgroup_id IN (1, 7, 8, 111)
               AND deal.fdate BETWEEN 20170225 AND 20170303
               AND deal.trade_time > trans.submit_time
               AND trans.process IN (5, 6) AND trans.valid = 1
               AND deal.partner_id = 'pmec'
         GROUP BY deal.fdate
       )
    ) aaa
join

    (
      SELECT
        /*+driving_site(trans)*/                                             -- 后端投顾维护用户产出的平台收入
        2                                  AS ID,
        sum(CASE WHEN flow.changetype IN (1, 3)
          THEN flow.amount / 8 * 6.5 * (-1)
            WHEN flow.changetype IN (8)
              THEN flow.amount * (-1)
            WHEN flow.changetype IN (9, 10)
              THEN flow.amount * (-1) END) AS 后端用户平台收入
      /*+driving_site(trans)*/
      FROM silver_njs.pmec_zj_flow flow
        JOIN info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
          ON flow.loginaccount = trans.firm_id
      WHERE trans.cur_bgroup_id IN (1, 7, 8, 111)
            and  trans.process IN (5, 6) AND trans.valid = 1
            AND trans.submit_time < flow.createdate
            AND to_char(flow.fdate, 'yyyymmdd') BETWEEN '20170225' AND '20170303'
    ) bbb
on aaa.id<>bbb.id

join
      (
        SELECT
          /*+driving_site(trans)*/                                                  -- 后端投顾维护用户净入金
          3                                       AS ID,
          sum(CASE WHEN inout.inorout = 'A'
            THEN inout.inoutmoney
              WHEN inout.inorout = 'B'
                THEN inout.inoutmoney * (-1) END) AS 后端用户净入金
        FROM silver_njs.history_transfer inout
          JOIN info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
            ON inout.firmid = trans.firm_id
        WHERE trans.cur_bgroup_id IN (1, 7, 8, 111)
              AND trans.process IN (5, 6) AND trans.valid = 1
              AND inout.partnerid = 'pmec'
              AND inout.fdate BETWEEN 20170311 AND 20170317
      )ccc
on aaa.id<>ccc.id




-- Part 6 每周电销资源转化情况


select '电销前端开单',count(distinct user_id)
from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link
where process in (5,6) and valid=1
      and to_char(submit_time,'yyyymmdd') between 20170225 and 20170303
      and fgroup_id in (2,3,4,5,6,9,10,11,12,105) --电销前端开单

union all

SELECT
  '电销当周开单当周有效开仓',count (aa.id)
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
          or (aa.num1 >= 2000000 AND aa.num2 >= 720)                            ---  有效开仓

-- Part 7.  每周微销资源转化情况

select '微销前端开单',count(distinct user_id)
from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link
where process in (5,6) and valid=1
      and to_char(submit_time,'yyyymmdd') between 20170225 and 20170303
      and fgroup_id in (112,113,114,106) --微销前端开单

union all

SELECT
  '微销当周开单当周有效开仓',count (aa.id)
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
         AND trans.fgroup_id IN (112,113,114,106)
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
          or (aa.num1 >= 2000000 AND aa.num2 >= 720)                            ---  有效开仓






-- Part 8.  后端维护用户资金、出入金及交易情况

select b1.id,b1.总接手资金,b1.本周接收资金,b1.服务用户数,b1.本周新增服务用户数,
b2.总入金量,b2.总出金量,b3.交易用户数,b4.有效开仓用户数,b5.维护间隔
from
  (
    SELECT
      /*+driving_site(trans)*/
      trans.cur_bgroup_id                                          AS id,
      sum(trans.pmec_net_value_sub + trans.pmec_net_in_sub)        AS 总接手资金,
      sum(CASE WHEN to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170225 AND 20170303
        THEN trans.pmec_net_value_sub + trans.pmec_net_in_sub END) AS 本周接收资金,
      count(DISTINCT trans.firm_id)                                AS 服务用户数,
      count(DISTINCT CASE WHEN to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170225 AND 20170303
        THEN trans.firm_id END)                                    AS 本周新增服务用户数
    FROM info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
    WHERE trans.cur_bgroup_id IN (1, 7, 8, 111)
          AND trans.process IN (5, 6) AND trans.valid = 1
    GROUP BY trans.cur_bgroup_id
  ) b1
JOIN
  (
    SELECT
      /*+driving_site(trans)*/
      trans.cur_bgroup_id as id,
      sum(CASE WHEN inout.inorout = 'A'
        THEN inout.inoutmoney END) AS 总入金量,
      sum(CASE WHEN inout.inorout = 'B'
        THEN inout.inoutmoney END) AS 总出金量
    FROM info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
      JOIN silver_njs.history_transfer inout
        ON trans.firm_id = inout.firmid
    WHERE trans.cur_bgroup_id IN (1, 7, 8, 111)
          AND trans.process IN (5, 6) AND trans.valid = 1
          AND inout.fdate BETWEEN 20170225 AND 20170303
          AND trans.submit_time < inout.realdate
    GROUP BY trans.cur_bgroup_id
    ) b2
  on b1.id=b2.id
join
    (
      SELECT
        /*+driving_site(trans)*/
        /*+driving_site(deal)*/
        trans.cur_bgroup_id as id,
        count(DISTINCT trans.firm_id) AS 交易用户数
      FROM info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
        JOIN ods_history_deal@silver_stat_urs_30_link deal
          ON trans.firm_id = deal.firmid
      WHERE trans.cur_bgroup_id IN (1, 7, 8, 111)
            AND trans.process IN (5, 6) AND trans.valid = 1
            AND trans.submit_time < deal.trade_time
            AND deal.fdate BETWEEN 20170318 AND 20170323
      GROUP BY trans.cur_bgroup_id
      ) b3
    on b1.id=b3.id
JOIN
    (
      SELECT
        /*+driving_site(trans)*/
        /*+driving_site(deal)*/
        aaa.cur_bgroup_id as id,
        count(DISTINCT aaa.firm_id) AS 有效开仓用户数
      FROM
        (
          SELECT
            /*+driving_site(deal)*/
            aa.firm_id                        AS firm_id,
            aa.cur_bgroup_id                  AS cur_bgroup_id,
            aa.fmoney                         AS fmoney,
            sum(CASE WHEN deal.wareid = 'GDAG'
              THEN deal.contnum
                WHEN deal.wareid = 'GDPT'
                  THEN deal.contnum * 56
                WHEN deal.wareid = 'GDPD'
                  THEN deal.contnum * 30 END) AS contnum
          FROM
            (
              SELECT DISTINCT
                /*+driving_site(trans)*/
                trans.firm_id                                    AS firm_id,
                trans.cur_bgroup_id                              AS cur_bgroup_id,
                trans.pmec_net_value_sub + trans.pmec_net_in_sub AS fmoney,
                trans.submit_time                                AS submit_time
              FROM info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
              WHERE trans.cur_bgroup_id IN (1, 7, 8, 111)
                    AND trans.process IN (5, 6) AND trans.valid = 1
            ) aa
            JOIN ods_history_deal@silver_stat_urs_30_link deal
              ON aa.firm_id = deal.firmid
                 AND aa.submit_time < deal.trade_time
          --WHERE deal.fdate <= 20170303
          GROUP BY aa.firm_id, aa.fmoney, aa.cur_bgroup_id
        ) aaa
      WHERE (aaa.fmoney < 100000 AND aaa.contnum >= 30)
            OR (aaa.fmoney < 200000 AND aaa.contnum >= 60)
            OR (aaa.fmoney < 300000 AND aaa.contnum >= 120)
            OR (aaa.fmoney < 500000 AND aaa.contnum >= 180)
            OR (aaa.fmoney < 1000000 AND aaa.contnum >= 240)
            OR (aaa.fmoney < 2000000 AND aaa.contnum >= 480)
            OR (aaa.fmoney >= 2000000 AND aaa.contnum >= 720)
      GROUP BY aaa.cur_bgroup_id
      ) b4
    on b1.id=b4.id
JOIN
    (
      SELECT                                                                               -- 维护间隔
        a.cur_bgroup_id as id,
        avg(a.delta) as 维护间隔
      FROM
        (
          SELECT
            /*+driving_site(trans)*/
            trans.firm_id,
            trans.cur_bgroup_id                           AS cur_bgroup_id,
            min(tel.create_time) - min(trans.submit_time) AS delta
          FROM info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
            JOIN tb_crm_tel_record@silver_stat_urs_30_link tel
              ON tel.user_id = trans.user_id
          WHERE tel.create_time > trans.submit_time
                AND to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170225 AND 20170303
                AND to_char(tel.create_time, 'yyyymmdd') <= 20170303
                AND trans.cur_bgroup_id IN (1, 7, 8, 111)
                AND trans.process IN (5, 6) AND trans.valid = 1
          GROUP BY trans.firm_id, trans.cur_bgroup_id
        ) a
      WHERE a.delta IS NOT NULL AND a.delta > 0
      GROUP BY a.cur_bgroup_id
      ) b5
    on b1.id=b5.id



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




