/*累计出入金净值计算*/

SELECT
  b.crm_user_id,
  sum(CASE WHEN a.inorout = 'A'
    THEN a.inoutmoney
      WHEN a.inorout = 'B'
        THEN a.inoutmoney*(-1) END)
FROM silver_njs.history_transfer@silver_std a
  JOIN info_silver.dw_user_account b
    ON a.firmid = b.firm_id
WHERE a.fdate <= '20170531'
      AND b.partner_id IN ('pmec', 'hht')
      AND b.crm_user_id IN
          ('34362650', '201729530', '153893045', '129897889', '8941540', '1000124484', '61298879', '1000163390', '1000092669', '1000215692', '1000103012', '178052334', '1000116344', '48168507', '1000295332', '1000311739', '1000186216', '1000062862', '1000227878', '1000308837', '1000148339', '1000072926', '1000380683', '1000091911', '1000262719', '1000399831', '159922918', '1000325087', '1000231746', '1000024450', '1000436469', '1000262426', '1000364356', '1000504280', '1000307933', '1000522097', '1000468834', '1000372786', '1000036607'
          )
GROUP BY b.crm_user_id




/*日均累计出入金净值计算*/
select aaa.crm_user_id, avg(aaa.daynetin)
  from
(SELECT
 b.crm_user_id, c.fdate,
 sum(CASE WHEN a.inorout = 'A'
   THEN a.inoutmoney
     WHEN a.inorout = 'B'
       THEN a.inoutmoney*(-1) END) as daynetin
 FROM silver_njs.history_transfer@silver_std A
 JOIN info_silver.dw_user_account b
 ON A.firmid = b.firm_id
 JOIN
 (SELECT
 DISTINCT to_char(suba.trade_date, 'yyyymmdd') AS fdate
 FROM NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE suba
 WHERE to_char(suba.trade_date, 'yyyymm') = '201705') C
 ON A.fdate <= C.fdate
 WHERE b.partner_id IN ('pmec', 'hht')
 AND b.crm_user_id IN
 ('34362650', '201729530', '153893045', '129897889', '8941540', '1000124484', '61298879', '1000163390', '1000092669', '1000215692', '1000103012', '178052334', '1000116344', '48168507', '1000295332', '1000311739', '1000186216', '1000062862', '1000227878', '1000308837', '1000148339', '1000072926', '1000380683', '1000091911', '1000262719', '1000399831', '159922918', '1000325087', '1000231746', '1000024450', '1000436469', '1000262426', '1000364356', '1000504280', '1000307933', '1000522097', '1000468834', '1000372786', '1000036607'
 )
 GROUP BY b.crm_user_id, c.fdate
) aaa
group by aaa.crm_user_id



/*当月交易额*/

select b.crm_user_id, sum(a.weight*a.trade_price) from NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE a
join info_silver.dw_user_account b
  on a.trader_id = b.firm_id
where to_char(a.trade_date, 'yyyymm')= '201705'
and b.crm_user_id in ('34362650', '201729530', '153893045', '129897889', '8941540', '1000124484', '61298879', '1000163390', '1000092669', '1000215692', '1000103012', '178052334', '1000116344', '48168507', '1000295332', '1000311739', '1000186216', '1000062862', '1000227878', '1000308837', '1000148339', '1000072926', '1000380683', '1000091911', '1000262719', '1000399831', '159922918', '1000325087', '1000231746', '1000024450', '1000436469', '1000262426', '1000364356', '1000504280', '1000307933', '1000522097', '1000468834', '1000372786', '1000036607')
group by b.crm_user_id


select * from NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE









SELECT *
FROM silver_njs.history_transfer@silver_std
