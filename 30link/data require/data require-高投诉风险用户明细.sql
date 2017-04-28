SELECT
  aaa.loginaccount as firmid,
  aaa.partner_id,
  aaa.user_id as 主站id,
  aaa.crm_user_id as CRM系统id,
  aaa.real_name as 用户姓名,
  aaa.ia_id as 投顾id,
  aaa.ia_name as 投顾姓名,
  aaa.group_id as 投顾组别,
  aaa.amount AS 亏损额,
  bbb.assets AS 历史最高净资产
FROM
  (SELECT
     a.loginaccount,
     a.partner_id,
     b.user_id,
     b.crm_user_id,
     b.real_name,
     b.ia_id,
     b.ia_name,
     b.group_id,
     sum(a.amount) AS amount
   FROM info_silver.pmec_zj_flow a
     JOIN info_silver.dw_user_account b
       ON a.loginaccount = b.firm_id
   WHERE a.changetype IN (1, 3, 8, 9, 10) AND a.partner_id = 'pmec'
   GROUP BY a.loginaccount, a.partner_id, b.user_id, b.crm_user_id, b.real_name, b.ia_id, b.ia_name,b.group_id) aaa
  JOIN
  (SELECT
     firmid,
     max(net_assets) AS assets
   FROM silver_njs.tb_silver_data_center@silver_std
   WHERE partner_id = 'pmec'
   GROUP BY firmid) bbb
    ON aaa.loginaccount = bbb.firmid
WHERE aaa.amount <= -10000 AND
      aaa.amount / bbb.assets <= -0.5

select * from info_silver.pmec_zj_flow where loginaccount=163000000000761
and changetype in (9,10)


select a. from info_silver.pmec_zj_flow a

