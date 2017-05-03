select distinct user_name,
  --useR_id,firm_id,real_name,
  last_login_time from info_silver.dw_user_account a
where user_id not IN
      (select user_id from info_silver.dw_user_account where partner_id = 'hht')
and to_char(last_login_time,'yyyymmdd') >=to_char(sysdate-30,'yyyymmdd')
and last_login_time is not NULL
