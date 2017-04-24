select distinct sub_refer from info_silver.ods_history_user
select distinct refer_1_type from info_silver.ods_history_user
select distinct refer_2_type from info_silver.ods_history_user


select refer_1_type,refer_2_type--,sub_refer
  from info_silver.ods_history_user
group by refer_1_type,refer_2_type--,sub_refer
order by refer_1_type,refer_2_type




-- 每日广贵开户数

select a.refer_1_type,a.refer_2_type, to_char(b.open_account_time,'yyyymmdd'),                                           --一周新开户用户来源                                                            --7天内每日开户用户来源
  count(distinct b.firm_id) as 用户数
from info_silver.dw_user_account a
join tb_silver_user_stat@silver_std b
  on a.firm_id=b.firm_id
    and a.partner_id='pmec'
 -- and a.refer_1_type is not null
where to_char(b.open_account_time,'yyyymmdd')
between 20170401 and 20170421
group by a.refer_1_type,a.refer_2_type, to_char(b.open_account_time,'yyyymmdd')
order by to_char(b.open_account_time,'yyyymmdd')







--每日广贵有效新入金


select to_char(bbb.date1,'yyyymmdd'),his.refer_1_type,his.refer_2_type,count(bbb.id)
from
  (select aaa.firm_id id, min(aaa.realdate) as date1
  from
  (select suba.firmid as firm_id, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer@silver_std where partnerid='pmec') suba
  left JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate   -- 一周有效新入金用户来源
  from silver_njs.history_transfer@silver_std where partnerid='pmec') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate

  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firm_id) bbb
  join info_silver.dw_user_account his
  on bbb.id=his.firm_id
where to_char(bbb.date1,'yyyymmdd') between 20170401 and 20170421
  and his.refer_1_type is not null
group by to_char(bbb.date1,'yyyymmdd'),his.refer_1_type,his.refer_2_type








