-- 练习 其中有误 BIEE 投顾表 前两周单量 --
select sum(sub_a.num),sub_b.group_id,sub_b.dt
FROM


(select trans.BGROUP_ID as group_id,to_char(trans.submit_time,'yyyymmdd') as dt, count (distinct trans.id) as num
from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
where to_char(trans.submit_time,'yyyymmdd') between 20170210 and 20170308
  and trans.process in (5,6)
  and trans.bgroup_id in (1,7,8,111)
group by trans.BGROUP_ID, to_char(trans.submit_time,'yyyymmdd')) sub_a

left join
(select trans.BGROUP_ID as group_id,to_char(trans.submit_time,'yyyymmdd') as dt,max(trans.submit_time) as dttime, count (distinct trans.id) as num
from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
where to_char(trans.submit_time,'yyyymmdd') between 20170210 and 20170308
  and trans.process in (5,6)
  and trans.bgroup_id in (1,7,8,111)
group by trans.BGROUP_ID, to_char(trans.submit_time,'yyyymmdd')) sub_b

on sub_a.GROUP_ID=sub_b.GROUP_ID

where sub_a.dt between to_char(sub_b.dttime-13,'yyyymmdd') and to_char(sub_b.dttime,'yyyymmdd')
  and sub_b.dt between 20170301 and 20170308
group BY sub_b.group_id,sub_b.dt














select
  trans.BGROUP_ID as group_id,
  to_char(trans.submit_time,'yyyymmdd') as dt,
  count (distinct trans.id) as num,
  sum(trans.PMEC_NET_VALUE_SUB),
  sum(trans.PMEC_NET_IN_SUB)
from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
where to_char(trans.submit_time,'yyyymmdd') between 20170210 and 20170308
  and trans.process in (5,6)
  and trans.bgroup_id in (1,7,8,111)
group by trans.BGROUP_ID, to_char(trans.submit_time,'yyyymmdd')





SELECT
trans.BGROUP_ID as group_id,
to_char(trans.submit_time,'yyyymmdd') as dt,
sum (case when ab.inorout = 'A' then ab.inoutmoney end),
sum (case when ab.inorout = 'B' then ab.inoutmoney end)

from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
left join silver_njs.history_transfer ab
  on trans.firm_id = ab.firmid
where to_char(trans.submit_time,'yyyymmdd') between 20170223 and 20170308
  and trans.process in (5,6)
  and trans.bgroup_id in (1,7,8,111)
  and ab.realdate between trans.submit_time and to_date('2017/03/09 04:00:00','yyyy/mm/dd hh24:mi:ss')

group by trans.BGROUP_ID, to_char(trans.submit_time,'yyyymmdd')


-- from 程序 教学版 left join on的条件可以不是等于 --
select a.t,count(b.submit_time)
from
(select distinct trunc(open_account_time) t from
tb_silver_user_stat where to_char(open_account_time,'yyyymmdd') between 20170301 and 20170308 )
a
left join info_silver.ods_crm_transfer_record@silver_stat_urs_30_link b
on trunc(b.submit_time) between t-14 and t   and b.valid=1 and b.process in (5,6)
group by t



-- BIEE 10 投顾表 column3 净入金SQL--
select
trans.BGROUP_ID as group_id,
stat.date1 as dt,
sum (case when ab.inorout = 'A' then ab.inoutmoney end),
sum (case when ab.inorout = 'B' then ab.inoutmoney end)
FROM
(select distinct to_char(open_account_time,'yyyymmdd') as date1,
  max(open_account_time) as datetime
from tb_silver_user_stat
where to_char(open_account_time,'yyyymmdd') between 20170301 and 20170308
group by to_char(open_account_time,'yyyymmdd')) stat

left join info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
  on to_char(trans.submit_time,'yyyymmdd') between to_char(stat.datetime-13,'yyyymmdd') and to_char(stat.datetime,'yyyymmdd')
     and trans.process in (5,6)
     and trans.bgroup_id in (1,7,8,111)

left join silver_njs.history_transfer ab
  on trans.firm_id = ab.firmid

where ab.realdate >= trans.submit_time and ab.fdate <= stat.date1
  group by trans.bgroup_id,stat.date1









