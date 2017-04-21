/*组别维度用户数*/
SELECT
  count(DISTInct a.ia_id),
  b.name,
  b.group_id,
  count(DISTINCT a.user_id),
  a.partner_id
FROM info_silver.dw_user_account a
  JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id

WHERE b.group_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106)
  --and b.name  LIKE '%资源%'
      and b.group_id=5
GROUP BY a.partner_id,  b.group_id,b.name


/*组别维度用户数——组内共享池资源*/
SELECT
  c.group_id,
  count(DISTINCT a.user_id),
  a.partner_id
FROM info_silver.dw_user_account a

  join info_silver.tb_crm_user c
  on a.user_id = c.id
WHERE c.group_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106)
and c.status = 3
GROUP BY a.partner_id, c.group_id



/*组别维度用户数——公司共享池资源*/
SELECT
  count(DISTINCT a.user_id),
  a.partner_id
FROM info_silver.dw_user_account a

  join info_silver.tb_crm_user c
  on a.user_id = c.id
WHERE c.status = 5
GROUP BY a.partner_id

select * from info_silver.tb_crm_user where status=5


select count(1) from info_silver.dw_user_account a
left  join info_silver.tb_crm_user b
  on a.user_id=b.id
where a.IA_ID is null  and a.partner_id='pmec' and b.status not in (3,5)


select count(1) from  info_silver.dw_user_account a join info_silver.tb_crm_user b  on  a.user_id=b.id



SELECT *
FROM info_silver.tb_crm_ia
WHERE id = 927


select * from info_silver.tb_crm_user where fa_id=55505247


SELECT
  user_id,
  real_name,
  partner_id
FROM info_silver.dw_user_account
WHERE ia_id IS NULL
--GROUP BY partner_id

SELECT a.user_id,a.ia_name,b.group_id --,partner_id
FROM info_silver.dw_user_account a
  JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id
WHERE  a.ia_name LIKE '%资源%'
-- group by a.ia_name,b.group_id
