select a.bia_id,a.bname,
sum(case when to_char(b.fdate,'yyyymm') =201609 then  b.amount end),
  sum(case when to_char(b.fdate,'yyyymm') =201610 then  b.amount end),
  sum(case when to_char(b.fdate,'yyyymm') =201611 then  b.amount end),
  sum(case when to_char(b.fdate,'yyyymm') =201612 then  b.amount end)
from info_silver.ods_crm_transfer_record a
  left join silver_njs.pmec_zj_flow@silver_std b
  on a.firm_id=b.loginaccount
  and a.submit_time < b.createdate
where
a.bia_id is not NULL
and a.process in (5,6) and a.valid = 1
and b.changetype in (9,10)
group by a.bia_id,a.bname




select * from silver_njs.pmec_zj_flow@silver_std



select * from info_silver.ods_crm_transfer_record