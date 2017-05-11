/*新增有效新入金*/
SELECT
  to_char(ccc.date1) AS 日期,
  bbb.id             AS 用户
FROM
  (SELECT DISTINCT to_char(open_account_time, 'yyyymmdd') AS date1
   FROM tb_silver_user_stat@silver_std
   WHERE to_char(open_account_time, 'yyyymm') = to_char(sysdate, 'yyyymm')
   ORDER BY to_char(open_account_time, 'yyyymmdd')) ccc
  LEFT JOIN
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
    ON to_char(bbb.date1, 'yyyymmdd') = ccc.date1
--WHERE (to_char(bbb.date1, 'yyyymm')=to_char( SYSDATE, 'yyyymm')
ORDER BY ccc.date1



/*每日HHT交易额*/
SELECT
  round(sum(trade_price * weight) / 100000000, 2) AS 交易额,
  to_char(trade_date, 'yyyymmdd')                 AS 日期
FROM info_silver.tb_nsip_t_filled_order
WHERE to_char(trade_Date, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
      AND to_char(trade_date, 'yyyymmdd') < to_char(sysdate, 'yyyymmdd')
      AND status = 1
GROUP BY to_char(trade_date, 'yyyymmdd')
ORDER BY to_char(trade_date, 'yyyymmdd')

/*每日HHT净资产*/
SELECT
  to_char(a.trade_date, 'yyyymmdd'),
  round(sum(a.last_capital) / 100000000, 2)
FROM NSIP_ACCOUNT.TB_NSIP_A_FUNDS_AFTER_SETTLE@LINK_NSIP_ACCOUNT a
  JOIN info_silver.dw_user_account b
    ON a.trader_id = b.firm_id
WHERE to_char(a.trade_date, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
      AND to_char(a.trade_date, 'yyyymmdd') < to_char(sysdate, 'yyyymmdd')
GROUP BY to_char(a.trade_date, 'yyyymmdd')
ORDER BY to_char(a.trade_date, 'yyyymmdd')


/*每日HHT净入金*/
SELECT
  to_char(a.trade_date, 'yyyymmdd') AS 日期,
  round(sum(charge_amount) / 10000, 2)
FROM NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT a
  JOIN info_silver.dw_user_account b
    ON a.fund_id = b.firm_id
WHERE to_char(a.trade_date, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
      AND to_char(a.trade_date, 'yyyymmdd') < to_char(sysdate, 'yyyymmdd')
      AND ORDER_STATUS = 3 AND RECONC_STATUS = 2
GROUP BY to_char(a.trade_date, 'yyyymmdd')
ORDER BY to_char(a.trade_date, 'yyyymmdd')



/*激活资金*/
SELECT *
FROM
  (
    SELECT
      a.group_id                               组别,
      a.name                                   投顾姓名,
      b.fia_id                                 投顾id,
      b.user_id                                用户id,
      b.user_name                              用户姓名,
      b.submit_time                            流转时间,
      nvl(b.net_zcmoney, 0) + nvl(b.net_in, 0) 激活资金
    FROM silver_consult.tb_crm_ia@consul_std a
      JOIN
      (
        SELECT
          a.fia_id,
          a.user_id,
          a.submit_time,
          e.user_name,
          b.net_zcmoney,
          sum(charge_amount) net_in
        FROM (SELECT *
              FROM silver_consult.tb_crm_transfer_record@consul_std
              WHERE trunc(submit_time) = trunc(sysdate) AND process IN (5, 6)) a
          LEFT JOIN silver_consult.v_tb_crm_user@consul_std e ON a.user_id = e.id
          LEFT JOIN (SELECT *
                     FROM tb_silver_user_stat@silver_std
                     WHERE partner_id = 'hht') d ON e.fa_id = d.user_id
          LEFT JOIN (SELECT
                       firm_id,
                       net_zcmoney
                     FROM info_silver.ods_order_zcmoney
                     WHERE to_char(fdate) = (SELECT CASE WHEN to_char(trunc(sysdate), 'day') = '星期一'
                       THEN (to_char(trunc(sysdate) - 3, 'yyyymmdd'))
                                                    ELSE (to_char(trunc(sysdate) - 1, 'yyyymmdd')) END
                                             FROM dual)) b ON d.firm_id = b.firm_id
          LEFT JOIN (SELECT *
                     FROM TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT
                     WHERE ORDER_STATUS = 3 AND RECONC_STATUS = 2) c
            ON d.firm_id = c.fund_id AND c.create_time < a.submit_time AND c.create_time > trunc(sysdate)
        GROUP BY a.fia_id, a.user_id, a.submit_time, e.user_name, b.net_zcmoney
      ) b ON a.id = b.fia_id
    UNION

    SELECT
      a.group_id                               组别,
      a.name                                   投顾姓名,
      b.fia_id                                 投顾id,
      b.user_id                                用户id,
      b.user_name                              用户姓名,
      b.submit_time                            流转时间,
      nvl(b.net_zcmoney, 0) + nvl(b.net_in, 0) 激活资金
    FROM silver_consult.tb_crm_ia@consul_std a
      JOIN
      (
        SELECT
          a.fia_id,
          a.user_id,
          a.submit_time,
          e.user_name,
          b.net_zcmoney,
          sum(charge_amount) net_in
        FROM (SELECT *
              FROM silver_consult.tb_crm_transfer_record@consul_std
              WHERE trunc(submit_time) = trunc(sysdate - 1) AND process IN (5, 6)) a
          LEFT JOIN silver_consult.v_tb_crm_user@consul_std e ON a.user_id = e.id
          LEFT JOIN (SELECT *
                     FROM tb_silver_user_stat@silver_std
                     WHERE partner_id = 'hht') d ON e.fa_id = d.user_id
          LEFT JOIN (SELECT
                       firm_id,
                       net_zcmoney
                     FROM info_silver.ods_order_zcmoney
                     WHERE to_char(fdate) = (SELECT CASE WHEN to_char(trunc(sysdate - 1), 'day') = '星期一'
                       THEN (to_char(trunc(sysdate) - 4, 'yyyymmdd'))
                                                    ELSE (to_char(trunc(sysdate) - 2, 'yyyymmdd')) END
                                             FROM dual)) b ON d.firm_id = b.firm_id
          LEFT JOIN (SELECT *
                     FROM TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT
                     WHERE ORDER_STATUS = 3 AND RECONC_STATUS = 2) c
            ON d.firm_id = c.fund_id AND c.create_time < a.submit_time AND c.create_time > trunc(sysdate - 1)
        GROUP BY a.fia_id, a.user_id, a.submit_time, e.user_name, b.net_zcmoney
      ) b ON a.id = b.fia_id


    UNION
    SELECT
      a.group_id                         组别,
      a.name                             投顾姓名,
      b.fia_id                           投顾id,
      b.user_id                          用户id,
      c.user_name                        用户姓名,
      b.submit_time                      流转时间,
      HHT_NET_VALUE_SUB + HHT_NET_IN_SUB 激活资金
    FROM silver_consult.tb_crm_ia@consul_std a
      JOIN info_silver.ods_crm_transfer_record b ON a.id = b.fia_id
      JOIN silver_consult.v_tb_crm_user@consul_std c ON b.user_id = c.id
    WHERE b.process IN (5, 6) AND b.valid = 1 AND
          substr(to_char(b.submit_time, 'yyyymmdd'), 1, 6) = substr(to_char(sysdate, 'yyyymmdd'), 1, 6)
  )
ORDER BY 流转时间 ASC


/*每日头寸*/

SELECT
  round(sum(CASE WHEN a.position_direction = 1
    THEN a.weight_open
            WHEN a.position_direction = 2
              THEN a.weight_open END) / 10000, 2) AS 头寸值,
  CASE WHEN a.position_direction = 1
    THEN '多单'
  WHEN a.position_direction = 2
    THEN '空单' END                                 AS 多单空单,
  to_char(a.trade_date, 'yyyymmdd')               AS 日期
FROM NSIP_TRADE.TB_NSIP_T_POSITION_DETAIL_H@LINK_NSIP_TRADE a
  JOIN info_silver.dw_user_account b ON a.trader_id = b.firm_id
WHERE to_char(a.trade_date, 'yyyymm') = to_char(sysdate, 'yyyymm') AND
      to_char(a.trade_date, 'yyyymmdd') < to_char(sysdate, 'yyyymmdd') AND a.status = 1
GROUP BY to_char(a.trade_date, 'yyyymmdd'), CASE WHEN a.position_direction = 1
  THEN '多单'
                                            WHEN a.position_direction = 2
                                              THEN '空单' END
ORDER BY to_char(a.trade_date, 'yyyymmdd')


/*每日头寸收入*/
SELECT
  sub4.fdate                        AS 日期,
  (sub6.money - sub4.money) / 10000 AS 头寸收入
FROM
  (SELECT
     deal.fdate,
     sum(CASE
         WHEN deal.wareid = 'LSAG100g'
           THEN deal.contnum * 8 END) AS money -- 点差
   FROM info_silver.ods_history_deal deal
   WHERE deal.partner_id = 'hht'
         AND ((deal.operation_src = 'open' AND buyorsal = 'B') OR (deal.operation_src = 'close' AND buyorsal = 'S'))
         AND substr(deal.fdate, 1, 6) = to_char(SYSDATE - 1, 'yyyymm')
         AND deal.fdate < to_char(SYSDATE, 'yyyymmdd')
   GROUP BY deal.fdate) sub4
  LEFT JOIN
  (SELECT
     to_char(create_time - 0.25, 'yyyymmdd') AS date1,
     sum(CASE WHEN flow.type IN (5, 6)
       THEN (-1) * flow.amount END)          AS money -- 头寸+点差
   FROM NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT flow
   WHERE to_char(create_time - 0.25, 'yyyymm') = to_char(SYSDATE - 1, 'yyyymm')
         AND to_char(create_time - 0.25, 'yyyymmdd') < to_char(SYSDATE, 'yyyymmdd')
   GROUP BY to_char(create_time - 0.25, 'yyyymmdd')) sub6
    ON sub4.fdate = sub6.date1
ORDER BY sub4.fdate



/*每日收入分解*/

SELECT
      '点差' as type1,
     deal.fdate as date1,
     round(sum(CASE
         WHEN deal.wareid = 'LSAG100g'
           THEN deal.contnum * 8 END)/10000,2) AS money -- 点差
   FROM info_silver.ods_history_deal deal
   WHERE deal.partner_id = 'hht'
         AND ((deal.operation_src = 'open' AND buyorsal = 'B') OR (deal.operation_src = 'close' AND buyorsal = 'S'))
         AND substr(deal.fdate, 1, 6) = to_char(SYSDATE - 1, 'yyyymm')
         AND deal.fdate < to_char(SYSDATE, 'yyyymmdd')
   GROUP BY deal.fdate

  union all

( SELECT
    '滞纳金' as type1,
     to_char(create_time - 0.25, 'yyyymmdd') AS date1,
     round(sum(CASE WHEN flow.type = 7
       THEN flow.AMOUNT END)/10000 ,2)              AS money -- 滞纳金
   FROM NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT flow
   WHERE to_char(create_time - 0.25, 'yyyymm') = to_char(SYSDATE - 1, 'yyyymm')
         AND to_char(create_time - 0.25, 'yyyymmdd') < to_char(SYSDATE, 'yyyymmdd')
   GROUP BY to_char(create_time - 0.25, 'yyyymmdd'))

union ALL

(SELECT
    '头寸收入' as type1,
  sub4.fdate                        AS date1,
  round((sub6.money - sub4.money) / 10000,2) AS money
FROM
  (SELECT
     deal.fdate,
     sum(CASE
         WHEN deal.wareid = 'LSAG100g'
           THEN deal.contnum * 8 END) AS money -- 点差
   FROM info_silver.ods_history_deal deal
   WHERE deal.partner_id = 'hht'
         AND ((deal.operation_src = 'open' AND buyorsal = 'B') OR (deal.operation_src = 'close' AND buyorsal = 'S'))
         AND substr(deal.fdate, 1, 6) = to_char(SYSDATE - 1, 'yyyymm')
         AND deal.fdate < to_char(SYSDATE, 'yyyymmdd')
   GROUP BY deal.fdate) sub4
  LEFT JOIN
  (SELECT
     to_char(create_time - 0.25, 'yyyymmdd') AS date1,
     sum(CASE WHEN flow.type IN (5, 6)
       THEN (-1) * flow.amount END)          AS money -- 头寸+点差
   FROM NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT flow
   WHERE to_char(create_time - 0.25, 'yyyymm') = to_char(SYSDATE - 1, 'yyyymm')
         AND to_char(create_time - 0.25, 'yyyymmdd') < to_char(SYSDATE, 'yyyymmdd')
   GROUP BY to_char(create_time - 0.25, 'yyyymmdd')) sub6
on sub4.fdate = sub6.date1)

union all

(SELECT
    '手续费' as type1,
     to_char(create_time - 0.25, 'yyyymmdd') AS date1,
     round(sum(CASE WHEN flow.type IN (3, 4)
       THEN (-1) * flow.amount END)/10000,2)          AS money
   FROM NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT flow
   WHERE to_char(create_time - 0.25, 'yyyymm') = to_char(SYSDATE - 1, 'yyyymm')
         AND to_char(create_time - 0.25, 'yyyymmdd') < to_char(SYSDATE, 'yyyymmdd')
   GROUP BY to_char(create_time - 0.25, 'yyyymmdd'))
order by date1

