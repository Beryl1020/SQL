select
  b.firmid,max(c.acct_real_name),max(substr(a.birth_day,1,4)),max(c.open_account_time)
  from info_silver.src_certno_info a
  join info_silver.tb_silver_account b
    on a.certno=b.certno
    join tb_silver_user_stat@silver_std   c
    on b.firmid=c.firm_id
    join

  where a.birth_day<=19571231 and c.partner_id='pmec'
group by b.firmid
 /*   and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
    and substr(a.birth_day)
  group by substr(a.birth_day,1,4)
order by substr(a.birth_day,1,4) DESC     */


select * from tb_silver_user_stat@silver_std
select * from info_silver.ods_history_user