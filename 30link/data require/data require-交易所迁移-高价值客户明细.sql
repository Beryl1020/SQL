SELECT
  user_id,
  firm_id
FROM info_silver.dw_user_account



/*  前端用户  */
SELECT
  b.crm_user_id AS CRM系统ID,
  b.real_name as 客户姓名,
  a.partner_id,
  a.firm_id,
  b.ia_id,
  e.name,
  e.group_id,
  max(d.net_assets)
FROM tb_silver_user_stat@silver_std a
  JOIN info_silver.dw_user_account b
    ON a.firm_id = b.firm_id
  JOIN info_silver.tb_crm_tag_user_rel c
    ON b.crm_user_id = c.user_id
  LEFT JOIN silver_njs.tb_silver_data_center@silver_std d
    ON a.firm_id = d.firmid
  join info_silver.tb_crm_ia e
  on b.ia_id = e.id
WHERE (c.tag_id = 38 or d.net_assets >= 2000)
      AND a.partner_id = 'pmec'
      AND d.hdate = to_char(sysdate - 1, 'yyyymmdd')
and b.ia_id is not null
and e.group_id in (2,3,4,5,6,9,10,11,12,105,106,112,113,114)
group by b.crm_user_id AS CRM系统ID,
  b.real_name as 客户姓名,
  a.partner_id,
  a.firm_id,
  b.ia_i,
  e.name,
  e.group_id







select * from info_silver.tb_crm_tag_user_rel where user_id=1000608343







select * from info_silver.dw_user_account


SELECT
  c.firm_id,
  b.id,
  b.name,
  b.group_id,
  d.net_assets
FROM info_silver.tb_crm_user a
  JOIN tb_silver_user_stat@silver_std c
    ON a.fa_id = c.user_id
  JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id
  JOIN silver_njs.tb_silver_data_center@silver_std d
    ON c.firm_id = d.firmid
WHERE b.group_id IN (1, 7, 8, 111)
      AND c.partner_id = 'pmec'
and d.hdate = 20170420;


select * from silver_njs.tb_silver_data_center@silver_std where partner_id='pmec' and hdate = 20170419 and net_Assets>0 and net_assets is not null
