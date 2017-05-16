SELECT
  a.trader_id,
  b.certno_city,
  b.real_name,
  b.bank_name,
  b.card_type,
  b.refer_2_type,
  b.ia_id,
  b.group_id,
  b.ia_name,
  c.bank_ct_city,
  sum(d.amount)            AS 总盈亏1,
  sum(nvl(f.net_zcmoney,0)-nvl(e.净入金,0)) as 总盈亏2,
  a.买入建仓,a.卖出平仓,a.买入平仓,a.卖出建仓,a.买入建仓时间,a.卖出平仓时间
  
FROM 
  (select trader_id, 
     sum(CASE WHEN position_direction = 1 AND position_operation = 0
    THEN weight END)     AS 买入建仓,
  sum(CASE WHEN position_direction = 2 AND position_operation = 1
    THEN weight END)     AS 卖出平仓,
  sum(CASE WHEN position_direction = 1 AND position_operation = 1
    THEN weight END)     AS 买入平仓,
  sum(CASE WHEN position_direction = 2 AND position_operation = 0
    THEN weight END)     AS 卖出建仓,
  max(CASE WHEN position_direction = 1 AND position_operation = 0
    THEN trade_time END) AS 买入建仓时间,
  max(CASE WHEN position_direction = 2 AND position_operation = 1
    THEN trade_time END) AS 卖出平仓时间
   from NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE where status =1 and to_char(trade_time, 'yyyymmdd hh24miss') BETWEEN '20170516 030000' AND '20170516 080000'
  group by trader_id) a
  JOIN info_silver.dw_user_account b
    ON a.trader_id = b.firm_id
  join info_silver.dw_user_account v
    on b.user_id = v.user_id and v.partner_id = 'pmec'
  LEFT JOIN info_silver.ods_history_user c
   ON v.firm_id = c.firm_id
  LEFT JOIN
  (SELECT
     loginaccount as fund_id,
     sum(CASE WHEN changeTYPE IN (1,3,8,9,10)
       THEN amount
         END) AS amount
   FROM info_silver.pmec_zj_flow
   GROUP BY loginaccount) d
    ON v.firm_id = d.fund_id
  left JOIN (SELECT
          firmid,
          sum(CASE WHEN inorout = 'A'
            THEN inoutmoney
          WHEN inorout = 'B'
            THEN -inoutmoney END) as 净入金
        FROM silver_njs.history_transfer@silver_std
        WHERE partnerid = 'pmec'
            group by firmid) e
  on v.firm_id = e.firmid
  left join
  (select firm_id, net_zcmoney from info_silver.ods_order_zcmoney where fdate = '20170521' and partner_id = 'hht') f
  on v.firm_id = f.firm_id
GROUP BY A.trader_id, b.certno_city, b.real_name, b.bank_name, b.card_type, b.refer_2_type, b.ia_id, b.group_id, b.ia_name, C.bank_ct_city,
  a.买入建仓,a.卖出平仓,a.买入平仓,a.卖出建仓,a.买入建仓时间,a.卖出平仓时间

SELECT *
FROM NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE

select * from silver_njs.history_transfer@silver_std where firmid= '163170424487052'

select * from info_silver.ods_order_zcmoney where firm_id = '163170424487052'

select * from NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE where trader_id = '163170424487052'

select * from info_silver.dw_user_account where firm_id ='163170428049945'
select * from info_silver.dw_user_account where user_id ='151213609'
select sum(amount) from info_silver.pmec_zj_flow where loginaccount='163000000511472' and changetype in (1,3,8,9,10)

select * from info_silver.ods_history_deal where firmid ='163000000016754'
SELECT *
FROM info_silver.ods_crm_transfer_record   where to_char(submit_time,'yyyymmdd')=20170515



select aa.主站id,aa.用户姓名,aa.组别,aa.投顾姓名,aa.昨日净资产,bb.本月流转后净入金,cc.本月流转后手续费
from
(select b.user_id as 主站id,
  b.real_name as 用户姓名,
  b.ia_name as 投顾姓名,
  b.group_id as 组别,
  sum(c.net_zcmoney) as 昨日净资产
from info_silver.dw_user_account b
  join info_silver.ods_order_zcmoney c
  on b.firm_id = c.firm_id
    and b.partner_id = 'hht'
  join info_silver.ods_crm_transfer_record a
  on a.firm_id = b.firm_id
where b.group_id in (1,7,8,111)
and c.fdate = to_char(sysdate-1,'yyyymmdd') and a.process in (5,6) and a.valid =1
group by b.user_id,b.real_name,b.ia_name,b.group_id

) aa

left join


(
select b.user_id as 主站id,
  b.real_name as 用户姓名,
  b.ia_name as 投顾姓名,
  b.group_id as 组别,
  sum(case when c.inorout='A' then inoutmoney when c.inorout='B' then -inoutmoney end) as 本月流转后净入金
from silver_njs.history_transfer@silver_std c
join info_silver.dw_user_account b
  on c.firmid=b.firm_id
  and b.partner_id = 'hht'
  join info_silver.ods_crm_transfer_record a
  on a.firm_id = b.firm_id
where  substr(c.fdate,1,6) =to_char(sysdate-1,'yyyymm')
and b.group_id in (1,7,8,111)
  and a.submit_time < c.realdate and a.process in (5,6) and a.valid =1
group by b.user_id,b.real_name,b.ia_name,b.group_id
) bb
on aa.主站id = bb.主站id
left join

(
SELECT b.user_id as 主站id,
  b.real_name as 用户姓名,
  b.ia_name as 投顾姓名,
  b.group_id as 组别,
  sum(a.trade_price*a.weight)*0.00065 as 本月流转后手续费
FROM info_silver.dw_user_account b
  left join info_silver.tb_nsip_t_filled_order a
  on a.trader_id = b.firm_id
  join info_silver.ods_crm_transfer_record c
  on b.firm_id = c.firm_id
where to_char(a.trade_Date,'yyyymm') = to_char(sysdate-1,'yyyymm')
  and group_id in (1,7,8,111)
  and a.trade_date > c.submit_time and c.process in (5,6) and c.valid =1
  group by b.user_id,b.real_name,b.ia_name,b.group_id
) cc
on aa.主站id=cc.主站id




SELECT
  a1.group_id,
  a1.总接手资金,
  a2.本周激活资金,
  a3.当日流转当日达标率,
  a4.当日流转当日开仓率,
  a5.本周资金利用率,
  a6.本月资金利用率,
  a7.本周流转后净入金,
  sum(a2.本周流转单) AS 流转单数
FROM
  (
    SELECT
      a.group_id,
      sum(CASE WHEN b.partner_id = 'pmec'
        THEN b.pmec_net_in_sub + b.pmec_net_value_sub
          WHEN b.partner_id = 'hht'
            THEN b.hht_net_in_sub + b.hht_net_in_sub END) AS 总接手资金,
      count(DISTINCT a.user_id)                           AS num
    FROM info_silver.dw_user_account a
      JOIN info_silver.ods_crm_transfer_record b
        ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
    WHERE a.group_id IN (1, 7, 8, 111) AND b.valid = 1 AND b.process IN (5, 6)
    GROUP BY a.group_id
  ) a1
  LEFT JOIN

  (SELECT
     a.group_id,
     sum(CASE WHEN b.partner_id = 'pmec'
       THEN b.pmec_net_in_sub + b.pmec_net_value_sub
         WHEN b.partner_id = 'hht'
           THEN b.hht_net_in_sub + b.hht_net_in_sub END) AS 本周激活资金,
    count(distinct b.user_id) as 本周流转单
   FROM info_silver.dw_user_account a
     JOIN info_silver.ods_crm_transfer_record b
       ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
   WHERE a.group_id IN (1, 7, 8, 111) AND
         to_char(b.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
             sysdate - 1, 'yyyymmdd')
         AND b.valid = 1 AND b.process IN (5, 6)
   GROUP BY  a.group_id
  ) a2
    ON a1.group_id = a2.group_id
  LEFT JOIN

  (SELECT
     bbb.group_id,
     round(sum(nvl(aaa.当日达标用户数, 0)) / sum(nvl(bbb.当日流转用户数,0)), 4) AS 当日流转当日达标率
   FROM
     (SELECT
        a.ia_id,
        a.group_id,
        a.ia_name,
        count(b.user_id) AS 当日流转用户数
      FROM
        info_silver.dw_user_account a
        JOIN info_silver.ods_crm_transfer_record b
          ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
      WHERE a.group_id IN (1, 7, 8, 111) AND
            to_char(b.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
                sysdate - 1, 'yyyymmdd')
            AND b.valid = 1 AND b.process IN (5, 6)
      GROUP BY a.ia_id, a.group_id, a.ia_name) bbb
     LEFT JOIN
     (SELECT
        aa.ia_id,aa.ia_name,
        aa.group_id,
        count(DISTINCT aa.user_id) AS 当日达标用户数
      FROM
        (SELECT
           trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name,
           max(trans.HHT_NET_VALUE_SUB + trans.HHT_NET_IN_SUB) AS num1,
           sum(CASE WHEN deal.wareid = 'LSAG100g'
             THEN deal.contnum END)                            AS num2
         FROM info_silver.ods_crm_transfer_record trans
           JOIN info_silver.dw_user_account a
             ON trans.user_id = a.crm_user_id
                AND a.partner_id = trans.partner_id
           JOIN (SELECT
                   wareid,
                   contnum,
                   fdate,
                   trade_time,
                   firmid
                 FROM info_silver.ods_history_deal
                 WHERE fdate >= '20160901' AND partner_id = 'hht') deal
             ON trans.firm_id = deal.firmid
         WHERE
           to_char(trans.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
               sysdate - 1, 'yyyymmdd')
           AND a.group_id IN (1, 7, 8, 111)
           AND trans.process IN (5, 6) AND trans.valid = 1
           AND (deal.trade_time > trans.submit_time)
           AND deal.fdate = to_char(trans.submit_time, 'yyyymmdd')
         GROUP BY trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name) aa
      WHERE (aa.num1 < 100000 AND aa.num2 >= 100)
            OR (aa.num1 < 200000 AND aa.num2 >= 200)
            OR (aa.num1 < 300000 AND aa.num2 >= 500)
            OR (aa.num1 < 500000 AND aa.num2 >= 1000)
            OR (aa.num1 < 1000000 AND aa.num2 >= 1500)
            OR (aa.num1 >= 1000000 AND aa.num2 >= 2000)

      GROUP BY aa.ia_id,aa.ia_name,
        aa.group_id) aaa
       ON aaa.ia_id = bbb.ia_id
    group by bbb.group_id
  ) a3
    ON a1.group_id = a3.group_id
  LEFT JOIN
  (SELECT
     bbb.group_id,
     round(sum(nvl(aaa.当日流转开仓用户数, 0)) /sum( nvl(bbb.当日流转用户数,0)), 4) AS 当日流转当日开仓率
   FROM
     (SELECT
        a.ia_id,
        a.group_id,
        a.ia_name,
        count(b.user_id) AS 当日流转用户数
      FROM
        info_silver.dw_user_account a
        JOIN info_silver.ods_crm_transfer_record b
          ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
      WHERE a.group_id IN (1, 7, 8, 111) AND
            to_char(b.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
                sysdate - 1, 'yyyymmdd')
            AND b.valid = 1 AND b.process IN (5, 6)
      GROUP BY a.ia_id, a.group_id, a.ia_name) bbb
     LEFT JOIN
     (SELECT
        aa.group_id,
        aa.ia_id,
        aa.ia_name,
        count(DISTINCT aa.user_id) AS 当日流转开仓用户数
      FROM
        (SELECT
           trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name,
           max(trans.HHT_NET_VALUE_SUB + trans.HHT_NET_IN_SUB) AS num1,
           sum(CASE WHEN deal.wareid = 'LSAG100g'
             THEN deal.contnum END)                            AS num2
         FROM info_silver.ods_crm_transfer_record trans
           JOIN info_silver.dw_user_account a
             ON trans.user_id = a.crm_user_id
                AND a.partner_id = trans.partner_id
           JOIN (SELECT
                   wareid,
                   contnum,
                   fdate,
                   trade_time,
                   firmid
                 FROM info_silver.ods_history_deal
                 WHERE fdate >= '20160901' AND partner_id = 'hht') deal
             ON trans.firm_id = deal.firmid
         WHERE
           to_char(trans.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
               sysdate - 1, 'yyyymmdd')
           AND a.group_id IN (1, 7, 8, 111)
           AND trans.process IN (5, 6) AND trans.valid = 1
           AND (deal.trade_time > trans.submit_time)
           AND deal.fdate = to_char(trans.submit_time, 'yyyymmdd')
         GROUP BY trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name) aa
      WHERE (aa.num1 < 100000 AND aa.num2 >= 30)
            OR (aa.num1 < 200000 AND aa.num2 >= 60)
            OR (aa.num1 < 300000 AND aa.num2 >= 120)
            OR (aa.num1 < 500000 AND aa.num2 >= 180)
            OR (aa.num1 < 1000000 AND aa.num2 >= 240)
            OR (aa.num1 < 2000000 AND aa.num2 >= 480)
            OR (aa.num1 >= 2000000 AND aa.num2 >= 720)
      GROUP BY aa.group_id,
        aa.ia_id,
        aa.ia_name) aaa
       ON aaa.ia_id = bbb.ia_id
    group by bbb.group_id
  ) a4
    ON a1.group_id  = a4.group_id

  LEFT JOIN

  (SELECT
     bb.group_id,
     round(sum(nvl(cc.流转后交易额, 0)) / (sum(aa.hht_net_value_sub + aa.hht_net_in_sub) + sum(nvl(dd.流转后净入金, 0))),
           2) AS 本周资金利用率
   FROM
     (SELECT *
      FROM info_silver.ods_crm_transfer_record
      WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
          sysdate - 1, 'yyyymmdd')
            AND PROCESS IN (5, 6) AND valid = 1) aa
     JOIN info_silver.dw_user_account bb
       ON aa.user_id = bb.crm_user_id AND aa.partner_id = bb.partner_id
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(A.contqty) AS 流转后交易额
      FROM info_silver.ods_history_deal A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
             sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.trade_time > b.submit_time
      GROUP BY A.firmid) cc
       ON aa.firm_id = cc.firmid
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(CASE WHEN A.inorout = 'A'
          THEN A.inoutmoney
            WHEN A.inorout = 'B'
              THEN -A.inoutmoney END) AS 流转后净入金
      FROM silver_njs.history_transfer@silver_std A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
             sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.realdate > b.submit_time
      GROUP BY a.firmid) dd
       ON aa.firm_id = dd.firmid
   WHERE bb.group_id IN (1, 7, 8, 111)
   GROUP BY bb.group_id
  ) a5
    ON a1.group_id  = a5.group_id
  LEFT JOIN

  (SELECT
     bb.group_id,
     round(sum(nvl(cc.本月流转后交易额, 0)) / (sum(nvl(ee.上月底净资产, 0)) + sum(nvl(dd.本月净入金, 0))), 2) AS 本月资金利用率
   FROM
     (SELECT *
      FROM info_silver.ods_crm_transfer_record
      WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(add_months(sysdate - 1, -1), 'mm'),
                                                             'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
            AND PROCESS IN (5, 6) AND valid = 1) aa
     JOIN info_silver.dw_user_account bb
       ON aa.user_id = bb.crm_user_id
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(A.contqty) AS 本月流转后交易额
      FROM info_silver.ods_history_deal A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(add_months(sysdate - 1, -1), 'mm'),
                                                                'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'mm'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.trade_time > b.submit_time
      GROUP BY A.firmid) cc
       ON aa.firm_id = cc.firmid
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(CASE WHEN A.inorout = 'A'
          THEN A.inoutmoney
            WHEN A.inorout = 'B'
              THEN -A.inoutmoney END) AS 本月净入金
      FROM silver_njs.history_transfer@silver_std A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(add_months(sysdate - 1, -1), 'mm'),
                                                                'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'mm'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.realdate > b.submit_time
      GROUP BY a.firmid) dd
       ON aa.firm_id = dd.firmid
     LEFT JOIN
     (SELECT
        A.firm_id,
        sum(a.net_zcmoney) AS 上月底净资产
      FROM info_silver.ods_order_zcmoney A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(add_months(sysdate - 1, -1), 'mm'),
                                                                'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firm_id = b.firm_id
      WHERE A.fdate = (SELECT CASE WHEN to_char(trunc(sysdate - 1, 'mm') - 1, 'day') = '星期日'
        THEN to_char(trunc(sysdate - 1, 'mm') - 1 - 2, 'yyyymmdd')
                              WHEN to_char(trunc(sysdate - 1, 'mm') - 1, 'day') = '星期六'
                                THEN to_char(trunc(sysdate - 1, 'mm') - 1 - 1, 'yyyymmdd')
                              ELSE to_char(trunc(sysdate - 1, 'mm') - 1, 'yyyymmdd') END
                       FROM dual)
      GROUP BY a.firm_id) ee
       ON aa.firm_id = ee.firm_id
   WHERE bb.group_id IN (1, 7, 8, 111)
   GROUP BY bb.group_id
  ) a6
    ON a1.group_id  = a6.group_id
  LEFT JOIN

  (SELECT
     c.group_id,
     sum(CASE WHEN A.inorout = 'A'
       THEN A.inoutmoney
         WHEN A.inorout = 'B'
           THEN -A.inoutmoney END) AS 本周流转后净入金
   FROM silver_njs.history_transfer@silver_std A
     JOIN info_silver.dw_user_account c
       ON a.firmid = c.firm_id
     JOIN
     (SELECT *
      FROM info_silver.ods_crm_transfer_record
      WHERE PROCESS IN (5, 6) AND valid = 1) b
       ON A.firmid = b.firm_id
   WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
         A.realdate > b.submit_time
   GROUP BY c.group_id
  ) a7
    ON a1.group_id  = a7.group_id

GROUP BY
  a1.group_id,
  a1.总接手资金,
  a2.本周激活资金,
  a3.当日流转当日达标率,
  a4.当日流转当日开仓率,
  a5.本周资金利用率,
  a6.本月资金利用率,
  a7.本周流转后净入金