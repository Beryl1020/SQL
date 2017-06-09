SELECT
  fgroup_id,
  to_char(submit_time, 'yyyymm'),
  count(user_id),
  count(DISTINCT fia_id),
  sum(CASE WHEN partner_id = 'hht'
    THEN hht_net_value_sub + hht_net_in_sub
      WHEN partner_id = 'pmec'
        THEN pmec_net_value_sub + pmec_net_in_sub
      END) AS activemoney
FROM info_silver.ods_crm_transfer_record
WHERE process IN (5, 6) AND valid = 1 AND fgroup_id IS NOT NULL
      AND to_char(submit_time, 'yyyymm') BETWEEN 201610 AND 201705
GROUP BY fgroup_id, to_char(submit_time, 'yyyymm')


SELECT
  fia_id,
  fname,
  fgroup_id,
  to_char(submit_time, 'yyyymm')
FROM info_silver.ods_crm_transfer_record
WHERE process IN (5, 6) AND valid = 1 AND fgroup_id IS NOT NULL
      AND to_char(submit_time, 'yyyymm') BETWEEN 201610 AND 201705
      AND fgroup_id = 9


SELECT
  id,
  name,
  to_char(create_time, 'yyyymm'),
  to_char(update_time, 'yyyymm'),
  status
FROM info_silver.tb_crm_ia
WHERE group_id = 9 AND role = 0 AND name NOT LIKE '%资源%'
--AND to_char(update_time, 'yyyymm')='201705'


SELECT *
FROM info_silver.tb_crm_ia
WHERE name = '曹羽晞'


SELECT
  b.name,
  to_char(a.create_time, 'yyyymm'),
  sum(CASE WHEN a.worksec >= 0
    THEN a.worksec END)     AS 总接通时长,
  count(a.id)               AS 总拨打次数,
  count(CASE WHEN a.worksec > 0
    THEN a.id END)          AS 总接通次数,
  count(DISTINCT a.user_id) AS 总拨打用户,
  count(DISTINCT CASE WHEN a.worksec > 0
    THEN a.user_id END)     AS 总接通用户
FROM info_silver.ods_crm_tel_record a
  JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id
WHERE --b.name IN ('陈帅', '谷月', '刘晓', '王秋红', '徐飞')
  b.name IN ('曹羽晞', '樊宗啸', '李俊霖', '刘月', '倪佳丽', '谭博文', '王平平', '于婷')
  AND to_char(a.create_time, 'yyyymm') <= '201705'
GROUP BY b.name,
  to_char(a.create_time, 'yyyymm')


SELECT *
FROM silver_consult.tb_crm_tel_record@consul_std
WHERE worksec < 0



----type = 11 新资源
SELECT
  b.name,
  CASE WHEN b.group_id IS NULL
    THEN a.group_id
  ELSE b.group_id END,
  count(DISTINCT a.user_id),
  count(DISTINCT CASE WHEN c.worksec > 0 AND c.worksec IS NOT NULL
    THEN c.user_id END),
  count(DISTINCT CASE WHEN c.worksec >= 0 AND c.worksec IS NOT NULL
    THEN c.useR_id END),
  to_char(a.create_time, 'yyyymm'),
  sum(DISTINCT CASE WHEN c.worksec >= 0 AND c.worksec IS NOT NULL
    THEN c.worksec END)
FROM silver_consult.tb_crm_dispatch_his@consul_std a
  LEFT JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id
  LEFT JOIN info_silver.ods_crm_tel_record c
    ON a.user_id = c.user_id
WHERE a.type = 11 AND to_char(a.create_time, 'yyyymm') BETWEEN 201703 AND 201705
      AND (b.group_id IN (4, 9) OR a.group_id IN (4, 9))
      AND to_char(c.create_time, 'yyyymm') = to_char(a.create_time, 'yyyymm')
      AND b.name IN ('陈帅', '谷月', '刘晓', '王秋红', '徐飞', '曹羽晞', '樊宗啸', '李俊霖', '刘月', '倪佳丽', '谭博文', '王平平', '于婷')
GROUP BY to_char(a.create_time, 'yyyymm'),
  CASE WHEN b.group_id IS NULL
    THEN a.group_id
  ELSE b.group_id END, b.name




----ia_id is not null 新资源
SELECT
  ia.name,
  ia.group_id,
  count(DISTINCT b1.user_id),
  to_char(b1.firsttime, 'yyyymm'),
  count(DISTINCT CASE WHEN tel.worksec >= 0
    THEN tel.user_id END) AS 拨打用户数,
  count(DISTINCT CASE WHEN tel.worksec > 0
    THEN tel.user_id END) AS 接通用户数,
  sum(DISTINCT CASE WHEN tel.worksec >= 0
    THEN tel.worksec END) AS 接通时长
FROM
  (SELECT
     a1.user_id,
     a1.firsttime
   FROM
     (SELECT
        dis.user_id,
        min(dis.create_time) AS firsttime
      FROM silver_consult.tb_crm_dispatch_his@consul_std dis
      WHERE dis.ia_id IS NOT NULL
      GROUP BY dis.user_id) a1
   WHERE to_char(a1.firsttime, 'yyyymm') BETWEEN 201703 AND 201705) b1

  JOIN silver_consult.tb_crm_dispatch_his@consul_std dis ON b1.user_id = dis.user_id AND b1.firsttime = dis.create_time
                                                            AND dis.ia_id IS NOT NULL
  JOIN silver_consult.tb_crm_ia@consul_std ia ON dis.ia_id = ia.id
  JOIN info_silver.ods_crm_tel_record tel ON b1.user_id = tel.user_id
WHERE ia.group_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106, 116, 117) -- 新增资源数
      AND ia.name IN ('陈帅', '谷月', '刘晓', '王秋红', '徐飞', '曹羽晞', '樊宗啸', '李俊霖', '刘月', '倪佳丽', '谭博文', '王平平', '于婷')
GROUP BY ia.name, to_char(b1.firsttime, 'yyyymm'), ia.group_id






---- 拨打人数crm创建时间分布

SELECT
  b.name,c.grade,
  to_char(c.create_time, 'yyyymm') AS 创建时间,
  to_char(a.create_time, 'yyyymm') AS 拨打时间,
  to_char(d.firsttime, 'yyyymm')     AS 分配时间,
  sum(CASE WHEN a.worksec >= 0
    THEN a.worksec END)            AS 总接通时长,
  count(a.id)                      AS 总拨打次数,
  count(CASE WHEN a.worksec > 0
    THEN a.id END)                 AS 总接通次数,
  count(DISTINCT a.user_id)        AS 总拨打用户,
  count(DISTINCT CASE WHEN a.worksec > 0
    THEN a.user_id END)            AS 总接通用户
FROM info_silver.ods_crm_tel_record a
  JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id
  JOIN info_silver.tb_crm_user c ON a.user_id = c.id
  left JOIN (SELECT
          dis.user_id,
          dis.ia_id,
          min(dis.create_time) AS firsttime
        FROM silver_consult.tb_crm_dispatch_his@consul_std dis
        WHERE dis.ia_id IS NOT NULL
        GROUP BY dis.user_id,dis.ia_id) d
    ON a.user_id = d.user_id AND d.ia_id = b.id
WHERE --b.name IN ('陈帅', '谷月', '刘晓', '王秋红', '徐飞')
  b.name IN ('曹羽晞', '樊宗啸', '李俊霖', '刘月', '倪佳丽', '谭博文', '王平平', '于婷', '陈帅', '谷月', '刘晓', '王秋红', '徐飞')
  AND to_char(a.create_time, 'yyyymm') <= '201705'
GROUP BY b.name,
  to_char(a.create_time, 'yyyymm'),to_char(c.create_time, 'yyyymm'),to_char(d.firsttime, 'yyyymm'),c.grade



select * from info_silver.tb_crm_user


