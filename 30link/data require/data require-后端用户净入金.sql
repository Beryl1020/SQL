SELECT
  trans.user_id,
  trans.firm_id,
  user1.user_name,
  trans.submit_time,
  trans.cur_bgroup_id,
  trans.cur_bname,
  sum(CASE WHEN io.inorout = 'A'
    THEN inoutmoney
      WHEN io.inorout = 'B'
        THEN (-1) * inoutmoney END) AS money
FROM info_silver.ods_crm_transfer_record trans
  JOIN silver_njs.history_transfer@silver_std io
    ON trans.firm_id = io.firmid
  LEFT JOIN info_silver.tb_crm_user user1
    ON trans.user_id = user1.id
WHERE io.fdate BETWEEN 20170329 AND 20170331
      AND trans.process IN (5, 6) AND trans.valid = 1
      AND io.realdate > trans.submit_time
      AND trans.cur_bgroup_id IN (1, 7, 8, 111)
GROUP BY trans.user_id, trans.firm_id, user1.user_name, trans.submit_time, trans.cur_bgroup_id,trans.cur_bname




select * from info_silver.ods_crm_transfer_record