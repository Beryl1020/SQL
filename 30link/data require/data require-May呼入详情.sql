select * from info_silver.tb_crm_tel_record where calltype =0



  select * from
(SELECT user_id,ia_id,create_time,billsec,worksec
FROM silver_consult.tb_crm_tel_record@consul_std where calltype =0 and to_char(create_time,'yyyymm')>201702
and user_id >1) aa
    left join
(select user_id,submit_time,fgroup_id,fia_id from info_silver.ods_crm_transfer_record  ) bb
    on aa.user_id = bb.user_id


