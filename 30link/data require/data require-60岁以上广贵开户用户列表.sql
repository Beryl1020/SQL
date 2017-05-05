SELECT
  b.firmid,
  max(c.acct_real_name),
  max(substr(a.birth_day, 1, 4)),
  max(c.open_account_time)
FROM info_silver.src_certno_info a
  JOIN info_silver.tb_silver_account b
    ON a.certno = b.certno
  JOIN tb_silver_user_stat@silver_std c
    ON b.firmid = c.firm_id

WHERE (a.birth_day <= 19621231 OR a.birth_day >= 19990101) AND c.partner_id = 'hht'
GROUP BY b.firmid
/*   and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
   and substr(a.birth_day)
 group by substr(a.birth_day,1,4)
order by substr(a.birth_day,1,4) DESC     */


SELECT *
FROM tb_silver_user_stat@silver_std
SELECT *
FROM info_silver.ods_history_user

SELECT *
FROM info_silver.src_certno_info


/*18岁以下&55岁以上*/
SELECT
  user_id,
  user_name,
  real_name,
  firm_id,
  birth_day
FROM info_silver.dw_user_account
where (birth_day <= 19621231 OR birth_day >= 19990101)
and partner_id = 'hht'