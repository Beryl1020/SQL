SELECT
  a.sub_refer_type,
  count(DISTINCT CASE WHEN a.pmec_firmid IS NOT NULL
    THEN a.pmec_firmid END) AS 广贵用户数,
  count(DISTINCT CASE WHEN a.hht_firmid IS NOT NULL
    THEN hht_firmid END)    AS 龙商开户数,
  sum(a.pmec_netvalue_21)   AS 广贵净资产总,
  sum(a.pmec_netvalue)      AS 广贵现资产,
  sum(a.hht_netvalue)       AS 龙商现资产
FROM info_silver.DM_CRM_TRANSFER_STAT a
WHERE sale_type NOT IN ('all')
GROUP BY a.sub_refer_type

/* 分天数据 */
SELECT
  a.refer_1_type as 内外推,
  a.sub_refer as 渠道号,
  count(distinct a.firm_id) as 广贵21日用户数,
  count(case when b.firm_id is not null then b.firm_id end) as hht开户用户数,
  sum(c.net_assets) as pmec21日净资产,
  sum(case when b.firm_id is not null then d.hhtasset end) as hht昨日净资产,
  sum(e.net_assets) as pmec昨日净资产
FROM
  (SELECT
     /* 4.21前pmec开户用户全 */
     DISTINCT
     user_id,
     firm_id,
     refer_1_type,
     sub_refer
   FROM info_silver.dw_user_account
   WHERE to_char(open_account_time, 'yyyymmdd') <= 20170421 AND partner_id = 'pmec') a
  LEFT JOIN /* hht开户用户 */
  (SELECT DISTINCT
     user_id,
     firm_id
   FROM info_silver.dw_user_account
   WHERE partner_id = 'hht'
  and to_char(open_account_time, 'yyyymmdd') <= to_char(sysdate - 1, 'yyyymmdd')) b
    ON a.user_id = b.user_id
  LEFT JOIN /* 4.21 pmec净资产 */
  (SELECT
     firmid,
     net_assets
   FROM silver_njs.tb_silver_data_center@silver_std
   WHERE hdate = 20170421) c
    ON a.firm_id = c.firmid
  LEFT JOIN /* hht昨日净资产 */
  (SELECT
     fund_id,
     sum(last_capital) AS hhtasset
   FROM NSIP_ACCOUNT.TB_NSIP_A_FUNDS_AFTER_SETTLE@LINK_NSIP_ACCOUNT
   WHERE to_char(trade_date, 'yyyymmdd') = to_char(SYSDATE - 1, 'yyyymmdd')
   GROUP BY fund_id) d
    ON b.firm_id = d.fund_id
  LEFT JOIN /* pmec昨日净资产 */
  (SELECT
     firmid,
     net_assets
   FROM silver_njs.tb_silver_data_center@silver_std
   WHERE hdate = to_char(sysdate - 1, 'yyyymmdd')) e
    on a.firm_id = e.firmid
group by a.sub_refer,a.refer_1_type

