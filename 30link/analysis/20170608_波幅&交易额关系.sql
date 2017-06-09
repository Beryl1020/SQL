SELECT
  to_char(trade_date, 'yyyymmdd'),
  sum((high_price - low_price) / close_market_price)
FROM info_silver.ods_nsip_q_quote_common
WHERE to_char(trade_date, 'yyyymmdd') <= to_char(sysdate - 1, 'yyyymmdd')
GROUP BY to_char(trade_date, 'yyyymmdd')

SELECT
  sum(contqty),
  fdate
FROM info_silver.ods_history_deal
WHERE fdate between '20170424' and  to_char(sysdate - 1, 'yyyymmdd') AND partner_id = 'hht'
GROUP BY fdate


SELECT *
FROM info_silver.ods_nsip_q_quote_common


select record_day,
sum((highprice-lowprice)/closeprice)
from info_silver.tb_info_klineday
where record_day between '20170301' and '20170415'
group by record_day