SELECT DISTINCT
  c.name                        AS 投顾,
  c.group_id                    AS 组别,
  count(a.worksec)              AS 通话次数,
  round(sum(a.billsec) / 60, 1) AS 拨打时长分钟,
  round(sum(a.worksec) / 60, 1) AS 实际通话时长分钟
FROM silver_consult.tb_crm_tel_record@consul_std a, silver_consult.v_tb_crm_user@consul_std b,
  silver_consult.tb_crm_ia@consul_std c
WHERE a.user_id = b.id
      AND a.ia_id = c.id
      AND to_char(a.create_time, 'yyyymmdd') = to_char(sysdate - 1, 'yyyymmdd')
GROUP BY c.name, c.group_id
ORDER BY c.group_id



SELECT
  b.name                        AS 投顾姓名,
  b.group_id                    AS 投顾组别,
  count(a.id)                   AS 拨打次数,
  count(case when a.worksec>0 and a.worksec is not null then a.id end) as 拨打并接通次数,
  round(sum(a.billsec) / 60, 1) AS 拨打总时长分钟,
  round(sum(a.worksec) / 60, 1) AS 实际通话时长分钟
FROM info_silver.tb_crm_tel_record a
  JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id and b.status=1
WHERE to_char(a.create_time, 'yyyymmdd') = to_char(sysdate - 1, 'yyyymmdd')
  and b.group_id in (1,2,3,4,5,6,7,8,9,10,11,12,105,106,112,113,114)
group by b.name,b.group_id
order by b.group_id,b.name