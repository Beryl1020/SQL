select sum(case when inorout='A' then inoutmoney when inorout='B' then inoutmoney*(-1) end ) from
silver_njs.history_transfer@silver_std
where fdate between 20170408 and 20170414 and partnerid='pmec'

select * from silver_njs.history_transfer@silver_std



select a.user_id,a.submit_time,a.bgroup_id,a.bia_id,a.bname,a.firm_id,sum(b.contqty)
from info_silver.ods_crm_transfer_record a
  join info_silver.ods_history_deal b
  on a.firm_id=b.firmid
where a.process in(5,6) and a.valid=1 and a.bgroup_id is not null
and a.submit_time < b.trade_time
and b.fdate between '20170101' and '20170131'
group by a.user_id,a.submit_time,a.bgroup_id,a.bia_id,a.bname,a.firm_id;

select distinct bia_id,bname,bgroup_id from info_silver.ods_crm_transfer_record  where bgroup_id in (1,7,8,111) and process in (5,6) and to_char(submit_time,'yyyymm')=201701