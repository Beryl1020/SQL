SELECT *
FROM NSIP_TRADE.TB_NSIP_T_POSITION_DETAIL_H@LINK_NSIP_TRADE
WHERE status = 1

SELECT *
FROM NSIP_TRADE.TB_NSIP_T_POSITION_DETAIL_H@LINK_NSIP_TRADE
WHERE status = 1



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

/*每日仓位*/
SELECT
  d.fdate,
  round(sum(e.margin) / sum(d.net_zcmoney),2) 每日仓位,
  sum(e.margin) as 准备金,
  sum(d.net_zcmoney) as 净资产
FROM (SELECT *
      FROM info_silver.dw_user_account
      WHERE partner_id = 'hht') a
  JOIN (SELECT
          firm_id,
          fdate,
          sum(net_zcmoney) net_zcmoney
        FROM info_silver.ods_order_zcmoney
        WHERE substr(fdate, 1, 6) = to_char(sysdate-1,'yyyymm') AND partner_id = 'hht'
        GROUP BY firm_id,fdate) d
    ON a.firm_id = d.firm_id
  left JOIN (SELECT
          trader_id,
          to_char(trade_date,'yyyymmdd') as fdate,
          sum(margin) margin
        FROM NSIP_TRADE.TB_NSIP_T_POSITION_DETAIL_H@LINK_NSIP_TRADE
        WHERE status = 1 AND substr(to_char(trade_date, 'yyyymmdd'), 1, 6) = to_char(sysdate-1,'yyyymm')
        GROUP BY trader_id,to_char(trade_date,'yyyymmdd')) e
    ON a.firm_id = e.trader_id AND e.fdate = d.fdate
GROUP BY d.fdate
order by d.fdate


select * from
  (SELECT
     trader_id,
     to_char(trade_date, 'yyyymmdd') AS fdate,
     sum(margin)                        margin
   FROM NSIP_TRADE.TB_NSIP_T_POSITION_DETAIL_H@LINK_NSIP_TRADE
   WHERE status = 1 AND substr(to_char(trade_date, 'yyyymmdd'), 1, 6) = to_char(sysdate - 1, 'yyyymm')
   GROUP BY trader_id, to_char(trade_date, 'yyyymmdd')
  ) a where a.margin = 0

select * from NSIP_TRADE.TB_NSIP_T_POSITION_DETAIL_H@LINK_NSIP_TRADE

