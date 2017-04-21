SELECT
  a.user_name           客户姓名,
  a.id                  客户ID,
  a.group_id            投顾组别,
  d.name                投顾姓名,
  max(c.fdate)          日期,
  sum(CASE WHEN c.commoditycode = 'GDAG'
    THEN c.holdqty END) 白银公斤数,
  sum(CASE WHEN c.commoditycode = 'GDPT'
    THEN c.holdqty END) 铂金公斤数,
  sum(CASE WHEN c.commoditycode = 'GDPD'
    THEN c.holdqty END) 钯金公斤数,
  min(e.net_assets)     净资产,
  min(f.um)             交易准备金,
  min(f.cw)             仓位
FROM
  info_silver.tb_crm_user a
  JOIN info_silver.ods_history_user b
    ON a.fa_id = b.user_id
  LEFT JOIN (SELECT *
             FROM info_silver.pmec_hold_position_order
             WHERE fdate = trunc(sysdate) - 1
             ORDER BY loginaccount) c
    ON b.firm_id = c.loginaccount
  LEFT JOIN info_silver.tb_crm_ia d
    ON a.ia_id = d.id
  LEFT JOIN (SELECT *
             FROM silver_njs.tb_silver_data_center@silver_std
             WHERE hdate = 20170419 AND partner_id = 'pmec') e
    ON b.firm_id = e.firmid
  LEFT JOIN (SELECT
               loginaccount,
               used_margin / netvalue cw,
               used_margin            um
             FROM info_silver.pmec_account_info
             WHERE fdate = trunc(sysdate) - 1 AND netvalue > 0) f
    ON b.firm_id = f.loginaccount
--where c.fdate=trunc(sysdate)-1 and
WHERE a.group_id IN (1, 7, 8)
GROUP BY a.user_name, a.id, a.group_id, d.name

select * from info_silver.pmec_account_info where loginaccount=163000000119188
and  to_char(fdate,'yyyymmdd')=20170330
select * from info_silver.pmec_hold_position_order where loginaccount=163000000119188
and  to_char(fdate,'yyyymmdd')=20170330