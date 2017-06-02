SELECT
  a.ia_id,
  b.name,
  b.group_id,
  a.group_id,
  count(DISTINCT a.user_id),
  to_char(a.create_time, 'yyyymmdd')
FROM silver_consult.tb_crm_dispatch_his@consul_std a
  LEFT JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id
WHERE a.type = 11 AND to_char(a.create_time, 'yyyymmdd') = 20170531
GROUP BY to_char(a.create_time, 'yyyymmdd'), a.ia_id, b.name, a.group_id,b.group_id


SELECT
*
FROM silver_consult.tb_crm_dispatch_his@consul_std a
WHERE a.type = 11 AND to_char(a.create_time, 'yyyymmdd') = 20170531 and ia_id is null



