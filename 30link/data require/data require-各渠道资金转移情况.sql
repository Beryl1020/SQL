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