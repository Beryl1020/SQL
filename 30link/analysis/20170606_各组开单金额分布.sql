SELECT
  user_id,
  crm_name,
  fia_id,
  fname,
  fgroup_id,
  bia_id,
  bname,
  bgroup_id,
  submit_time,
  dispatch_time,
  pmec_net_value_sub,
  pmec_net_in_sub,
  hht_net_value_sub,
  hht_net_in_sub,
  cur_bgroup_id,
  partner_id
FROM info_silver.ods_crm_transfer_record
WHERE process IN (5, 6) AND valid = 1


select * from info_silver.ods_crm_transfer_record



select a.user_id,
sum(case when b.inorout = 'A' then b.inoutmoney when b.inorout = 'B' then b.inoutmoney*(-1 ) end) as netin
from info_silver.ods_crm_transfer_record a
  join info_silver.dw_user_account dw
  on a.user_id = dw.crm_user_id
join silver_njs.history_transfer@silver_std b
  on dw.firm_id = b.firmid
where a.process in (5,6) and a.valid = 1
and b.realdate between a.submit_time and a.submit_time + 30
group by a.useR_id
