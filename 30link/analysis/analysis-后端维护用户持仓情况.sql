SELECT *
FROM silver_njs.tb_silver_data_center@silver_std a

SELECT *
FROM silver_njs.history_transfer@silver_std b

SELECT *
FROM info_silver.ods_crm_transfer_record c


SELECT
  a.firmid,
  a.fdate,
  c.bia_id,
  c.cur_bgroup_id,
  sum(CASE WHEN a.inorout = 'A'
    THEN a.inoutmoney
      WHEN a.inorout = 'B'
        THEN a.inoutmoney * (-1) END)
FROM silver_njs.history_transfer@silver_std a
  JOIN
  info_silver.ods_crm_transfer_record c
    ON a.firmid = c.firm_id
WHERE c.cur_bgroup_id IN (1, 7, 8, 111)
      AND c.process IN (5, 6) AND c.valid = 1
      AND a.fdate BETWEEN 20170401 AND 20170419
      AND a.realdate > c.submit_time
GROUP BY a.firmid, a.fdate,c.bia_id,
  c.cur_bgroup_id;





