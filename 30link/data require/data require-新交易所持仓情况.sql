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
            when a.position_direction = 2
    THEN a.weight_open END)/10000,2) AS 头寸值,
  CASE WHEN a.position_direction = 1
    THEN '多单'
  when a.position_direction = 2
    THEN '空单' END AS 多单空单,
  to_char(a.trade_date,'yyyymmdd') as 日期
FROM NSIP_TRADE.TB_NSIP_T_POSITION_DETAIL_H@LINK_NSIP_TRADE a
  JOIN info_silver.dw_user_account b ON a.trader_id = b.firm_id
WHERE to_char(a.trade_date, 'yyyymm') = to_char(sysdate, 'yyyymm') AND
      to_char(a.trade_date, 'yyyymmdd') < to_char(sysdate, 'yyyymmdd') AND a.status = 1
group by to_char(a.trade_date,'yyyymmdd'),CASE WHEN a.position_direction = 1
    THEN '多单'
  when a.position_direction = 2
    THEN '空单' END
order by to_char(a.trade_date,'yyyymmdd')



