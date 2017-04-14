


select a.firmid,
  sum(case when a.inorout='A' then inoutmoney end) as moneyin,
  sum(case when a.inorout='B' then inoutmoney*(-1) end) as moneyout

from silver_njs.history_transfer@silver_std a
  join  info_silver.ods_crm_transfer_record   b
  on a.firmid=b.firm_id

where a.fdate between 20170325 and 20170331

and a.partnerid='pmec'
and a.realdate>b.submit_time
group by a.firmid

-- 后端通话情况各月份对比

select b.group_id,
  count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170304 and 20170310 then a.id end) as a,
  count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170311 and 20170317 then a.id end) as a,
  count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170318 and 20170324 then a.id end) as a,
  count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170325 and 20170331 then a.id end) as b,
count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170401 and 20170407 then a.id end) as c,
  count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170408 and 20170414 then a.id end) as d
from info_silver.tb_crm_tel_record a
join info_silver.tb_crm_ia  b
  on a.ia_id=b.id and b.group_id in(1,7,8,111)
where to_char(a.create_time,'yyyymmdd') between 20170301 and 20170415
  and a.worksec>=240
group by b.group_id


