SELECT *
FROM info_silver.dw_user_account


SELECT *
FROM
  (
    SELECT
      a.user_id,
      max(b.id)       AS crm_user_id,
      max(c.id)       AS ia_id,
      max(c.name)     AS ia_name,
      max(c.group_id) AS group_id
    FROM tb_silver_user_stat@silver_std a
      JOIN silver_consult.v_tb_crm_user@consul_std b
        ON a.user_id = b.fa_id
      JOIN silver_consult.tb_crm_ia@consul_std c
        ON b.ia_id = c.id
    GROUP BY a.user_id
  ) b
  LEFT JOIN (SELECT
               user_id,
               max(connect_time) AS connect_time
             FROM silver_consult.tb_crm_tel_record@consul_std
             WHERE worksec > 0
             GROUP BY user_id) a
    ON a.user_id = b.crm_user_id
  LEFT JOIN
  (SELECT
     useR_id,
     tag_id
   FROM silver_consult.tb_crm_tag_user_rel@consul_std
   WHERE tag_id IN (38, 39, 40)) c
    ON b.crm_user_id = c.user_id
WHERE b.crm_user_id IS NOT NULL
      AND
      (a.connect_time IS NULL OR to_char(a.connect_time, 'yyyymmdd') <= '20170424')
      AND b.group_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106, 117, 116)
      AND b.ia_id IS NOT NULL


SELECT *
FROM info_silver.tb_crm_tel_record

SELECT *
FROM info_silver.rpt_crm_transfer_user_stat
WHERE to_char(stat_date, 'yyyymmdd') = '20170524'

SELECT *
FROM info_silver.rpt_crm_transfer_user_stat
WHERE to_char(stat_date, 'yyyymmdd') = 20170523 AND group_id NOT IN (100, 115)

SELECT to_char(trunc(add_months(sysdate - 1, -1), 'mm'))
FROM dual


SELECT sum(CASE WHEN b.inorout = 'A'
  THEN b.inoutmoney
           WHEN b.inorout = 'B'
             THEN b.inoutmoney * (-1) END)
FROM info_silver.ods_crm_transfer_record a
  JOIN info_silver.dw_user_account c ON a.user_id = c.crm_user_id AND c.partner_id = 'hht'
  LEFT JOIN silver_njs.history_transfer@silver_std b
    ON c.firm_id = b.firmid

WHERE to_char(a.submit_time, 'yyyymm') = '201705'
      AND b.realdate > a.submit_time
      AND substr(b.fdate, 1, 6) = '201705' AND b.fdate <= to_char(sysdate - 1, 'yyyymmdd')
      AND a.process IN (5, 6) AND a.valid = 1
      AND c.group_id = 1


SELECT
  a.user_id,
  max(b.id)   AS crm_user_id,
  max(c.id)   AS ia_id,
  max(c.name) AS ia_name,
  max(c.group_id)
FROM tb_silver_user_stat@silver_std a
  JOIN silver_consult.v_tb_crm_user@consul_std b
    ON a.user_id = b.fa_id
  JOIN silver_consult.tb_crm_ia@consul_std c
    ON b.ia_id = c.id
GROUP BY a.user_id


SELECT
  a.user_id,
  max(b.id),
  max(b.name),
  max(b.group_id),
  max(b.empid),
  max(c.mintime)
FROM silver_consult.tb_crm_dispatch_his@consul_std a
  JOIN (SELECT
          user_id,
          min(create_time) AS mintime
        FROM silver_consult.tb_crm_dispatch_his@consul_std
        WHERE ia_id IS NOT NULL
        GROUP BY user_id) c
    ON a.user_id = c.user_id AND a.create_time = c.mintime
  JOIN silver_consult.tb_crm_ia@consul_std b
    ON a.ia_id = b.id
  JOIN silver_consult.v_tb_crm_user@consul_std d
    ON a.user_id = d.id
  JOIN (SELECT
          user_id,
          min(open_account_time) AS open_account_time
        FROM tb_silver_user_stat@silver_std
        WHERE partneR_id IN ('pmec', 'hht')
        GROUP BY user_id) e
    ON d.fa_id = e.user_id
WHERE to_char(c.mintime, 'yyyymmdd') = 20170525
      AND to_char(open_account_time, 'yyyymmdd') = 20170525
      AND b.group_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106, 117, 116)
GROUP BY a.user_id


SELECT *
FROM silver_consult.v_tb_crm_user@consul_std


SELECT *
FROM
  (SELECT *
   FROM (SELECT
           user_id,
           min(open_account_time) AS open_account_time
         FROM tb_silver_user_stat@silver_std
         GROUP BY user_id)
   WHERE to_char(open_account_time, 'yyyymmdd') = 20170525) a
  JOIN silver_consult.v_tb_crm_user@consul_std b
    ON a.user_id = b.fa_id
  JOIN
  (SELECT
     user_id,
     min(create_time) AS mintime
   FROM silver_consult.tb_crm_dispatch_his@consul_std
   GROUP BY user_id) c
    ON b.id = c.user_id

SELECT *
FROM tb_silver_user_stat@silver_std


SELECT *
FROM info_silver.ods_crm_transfer_record
WHERE to_char(submit_time, 'yyyymmdd') BETWEEN 20170520 AND 20170526
      AND firm_id IS NULL

SELECT *
FROM silver_consult.tb_crm_transfer_record@consul_std
WHERE user_id = 1000655993


SELECT *
FROM info_silver.dw_user_account
WHERE crm_user_id = '1000655993'

SELECT *
FROM silver_njs.history_transfer@silver_std
WHERE firmid = 163170526070742


SELECT *
FROM tb_silver_user_stat@silver_std
WHERE user_id = 92344558

SELECT *
FROM silver_njs.history_transfer@silver_std
WHERE fdate = 20170526

SELECT *
FROM info_silver.dw_user_account





-- 资源数
SELECT count(DISTINCT b1.user_id)
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
   WHERE to_char(a1.firsttime, 'yyyymmdd') BETWEEN 20170422 AND 20170428) b1--首次分配时间

  JOIN silver_consult.tb_crm_dispatch_his@consul_std dis ON b1.user_id = dis.user_id AND b1.firsttime = dis.create_time
  JOIN silver_consult.tb_crm_ia@consul_std ia ON dis.ia_id = ia.id
  JOIN silver_consult.v_tb_crm_user@consul_std user1 ON b1.user_id = user1.id AND user1.id IS NOT NULL

WHERE ia.group_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106, 116, 117)


--开单资源数
SELECT count(DISTINCT b1.user_id) from
( SELECT
a1.user_id,
a1.firsttime
FROM
( SELECT
dis.user_id,
MIN (dis.create_time) AS firsttime
FROM silver_consult.tb_crm_dispatch_his@consul_std dis
WHERE dis.ia_id IS NOT NULL
GROUP BY dis.user_id) a1
WHERE to_char(a1.firsttime, 'yyyymmdd') BETWEEN 20170422 AND 20170428) b1  --首次分配时间

JOIN silver_consult.tb_crm_dispatch_his@consul_std dis ON b1.user_id = dis.user_id AND b1.firsttime = dis.create_time
JOIN silver_consult.tb_crm_ia@consul_std ia ON dis.ia_id = ia.id
JOIN silver_consult.v_tb_crm_user@consul_std user1 ON b1.user_id = user1.id AND user1.id IS NOT NULL
JOIN info_silver.ods_crm_transfer_record trans ON b1.user_id = trans.user_id

WHERE ia.group_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106, 116, 117)
AND trans.submit_time BETWEEN b1.firsttime+7 AND b1.firsttime+14       --开单时间
      and trans.process in (5,6) and trans.valid =1




