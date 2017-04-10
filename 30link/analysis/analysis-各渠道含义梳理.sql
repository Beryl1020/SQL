select distinct sub_refer from info_silver.ods_history_user
select distinct refer_1_type from info_silver.ods_history_user
select distinct refer_2_type from info_silver.ods_history_user


select refer_1_type,refer_2_type--,sub_refer
  from info_silver.ods_history_user
group by refer_1_type,refer_2_type--,sub_refer
order by refer_1_type,refer_2_type








select a.refer_1_type,a.refer_2_type,                                                                        --每日开户用户来源
  count(distinct case when to_char(b.open_account_time,'yyyymmdd') =to_char(sysdate-7,'yyyymmdd') then b.firm_id end),
  count(distinct case when to_char(b.open_account_time,'yyyymmdd') =to_char(sysdate-6,'yyyymmdd') then b.firm_id end),
  count(distinct case when to_char(b.open_account_time,'yyyymmdd') =to_char(sysdate-5,'yyyymmdd') then b.firm_id end),
  count(distinct case when to_char(b.open_account_time,'yyyymmdd') =to_char(sysdate-4,'yyyymmdd') then b.firm_id end),
  count(distinct case when to_char(b.open_account_time,'yyyymmdd') =to_char(sysdate-3,'yyyymmdd') then b.firm_id end),
  count(distinct case when to_char(b.open_account_time,'yyyymmdd') =to_char(sysdate-2,'yyyymmdd') then b.firm_id end),
  count(distinct case when to_char(b.open_account_time,'yyyymmdd') =to_char(sysdate-1,'yyyymmdd') then b.firm_id end)
from info_silver.ods_history_user a
join tb_silver_user_stat@silver_std b
  on a.firm_id=b.firm_id
  and a.refer_1_type is not null
where to_char(b.open_account_time,'yyyymmdd')
between to_char(sysdate-7,'yyyymmdd') and to_char(sysdate-1,'yyyymmdd')
group by a.refer_1_type,a.refer_2_type
order by count(distinct case when to_char(b.open_account_time,'yyyymmdd') =to_char(sysdate-7,'yyyymmdd') then b.firm_id end) desc




-- 每日有效新入金用户来源
select his.refer_1_type,his.refer_2_type,count(bbb.id)
from
  (select aaa.firm_id id, min(aaa.realdate) as date1
  from
  (select suba.firmid as firm_id, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer@silver_std where partnerid='pmec') suba
  left JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate
  from silver_njs.history_transfer@silver_std where partnerid='pmec') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate

  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firm_id) bbb
  join info_silver.ods_history_user his
  on bbb.id=his.firm_id
where to_char(bbb.date1,'yyyymmdd') between 20170318 and 20170324
  and his.refer_1_type is not null
group by his.refer_1_type,his.refer_2_type





select * from tb_silver_user_stat@silver_std where to_char(open_account_time,'yyyymmdd')=20170329

select * from info_silver.ods_history_user where firm_id='163000000477528'


 where to_char(open_account_time,'yyyymmdd')=20170330