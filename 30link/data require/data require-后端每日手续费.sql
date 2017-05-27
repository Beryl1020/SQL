select aa.主站id,aa.用户姓名,aa.组别,aa.投顾姓名,aa.流转时间,aa.激活资金,aa.昨日净资产,bb.本月流转后净入金,cc.本月流转后手续费
from
(select b.user_id as 主站id,
  b.real_name as 用户姓名,
  b.ia_name as 投顾姓名,
  b.group_id as 组别,
  max(case when a.partner_id = 'pmec' then a.pmec_net_value_sub+a.pmec_net_in_sub
    when a.partner_id = 'hht' then a.hht_net_value_sub + a.hht_net_in_sub end ) as 激活资金,
  max(a.submit_time) as 流转时间,
  sum(c.net_assets) as 昨日净资产
from info_silver.dw_user_account b
  join silver_njs.tb_silver_data_center@silver_std  c
  on b.firm_id = c.firmid
    and b.partner_id = 'hht'
  join info_silver.ods_crm_transfer_record a
  on a.firm_id = b.firm_id
where b.group_id in (1,7,8,111,118)
and c.hdate = to_char(sysdate-1,'yyyymmdd') and a.process in (5,6) and a.valid =1
group by b.user_id,b.real_name,b.ia_name,b.group_id

) aa

left join


(
select b.user_id as 主站id,
  b.real_name as 用户姓名,
  b.ia_name as 投顾姓名,
  b.group_id as 组别,
  sum(case when c.inorout='A' then inoutmoney when c.inorout='B' then -inoutmoney end) as 本月流转后净入金
from silver_njs.history_transfer@silver_std c
join info_silver.dw_user_account b
  on c.firmid=b.firm_id
  and b.partner_id = 'hht'
  join info_silver.ods_crm_transfer_record a
  on a.firm_id = b.firm_id
where  substr(c.fdate,1,6) =to_char(sysdate-1,'yyyymm')
  and c.fdate <= to_char(sysdate-1,'yyyymmdd')
and b.group_id in (1,7,8,111,118)
  and a.submit_time < c.realdate and a.process in (5,6) and a.valid =1
group by b.user_id,b.real_name,b.ia_name,b.group_id
) bb
on aa.主站id = bb.主站id
left join

(
SELECT b.user_id as 主站id,
  b.real_name as 用户姓名,
  b.ia_name as 投顾姓名,
  b.group_id as 组别,
  sum(a.trade_price*a.weight)*0.00065 as 本月流转后手续费
FROM info_silver.dw_user_account b
  left join info_silver.tb_nsip_t_filled_order a
  on a.trader_id = b.firm_id
  join info_silver.ods_crm_transfer_record c
  on b.firm_id = c.firm_id
where to_char(a.trade_Date,'yyyymm') = to_char(sysdate-1,'yyyymm')
  and to_char(a.trade_date,'yyyymmdd') <= to_char(sysdate-1,'yyyymmdd')
  and group_id in (1,7,8,111,118)
  and a.trade_time > c.submit_time and c.process in (5,6) and c.valid =1
  group by b.user_id,b.real_name,b.ia_name,b.group_id
) cc
on aa.主站id=cc.主站id





select * from info_silver.ods_order_zcmoney where fdate ='20170526' and partner_id = 'hht'







select aa.主站id,aa.用户姓名,aa.组别,aa.投顾姓名,aa.昨日净资产,bb.昨日流转后净入金,cc.昨日流转后手续费
from
(select b.user_id as 主站id,
  b.real_name as 用户姓名,
  b.ia_name as 投顾姓名,
  b.group_id as 组别,
  sum(c.net_zcmoney) as 昨日净资产
from info_silver.dw_user_account b
  join info_silver.ods_order_zcmoney c
  on b.firm_id = c.firm_id
    and b.partner_id = 'hht'
  join info_silver.ods_crm_transfer_record a
  on a.firm_id = b.firm_id
where b.group_id in (1,7,8,111,118)
and c.fdate = to_char(sysdate-1,'yyyymmdd') and a.process in (5,6) and a.valid =1
group by b.user_id,b.real_name,b.ia_name,b.group_id

) aa

left join


(
select b.user_id as 主站id,
  b.real_name as 用户姓名,
  b.ia_name as 投顾姓名,
  b.group_id as 组别,
  sum(case when c.inorout='A' then inoutmoney when c.inorout='B' then -inoutmoney end) as 昨日流转后净入金
from silver_njs.history_transfer@silver_std c
join info_silver.dw_user_account b
  on c.firmid=b.firm_id
  and b.partner_id = 'hht'
  join info_silver.ods_crm_transfer_record a
  on a.firm_id = b.firm_id
where  c.fdate =to_char(sysdate-1,'yyyymmdd')
and b.group_id in (1,7,8,111,118)
  and a.submit_time < c.realdate and a.process in (5,6) and a.valid =1
group by b.user_id,b.real_name,b.ia_name,b.group_id
) bb
on aa.主站id = bb.主站id
left join

(
SELECT b.user_id as 主站id,
  b.real_name as 用户姓名,
  b.ia_name as 投顾姓名,
  b.group_id as 组别,
  sum(a.trade_price*a.weight)*0.00065 as 昨日流转后手续费
FROM info_silver.dw_user_account b
  left join info_silver.tb_nsip_t_filled_order a
  on a.trader_id = b.firm_id
  join info_silver.ods_crm_transfer_record c
  on b.firm_id = c.firm_id
where to_char(a.trade_Date,'yyyymmdd') = to_char(sysdate-1,'yyyymmdd')
  and group_id in (1,7,8,111,118)
  and a.trade_time > c.submit_time and c.process in (5,6) and c.valid =1
  group by b.user_id,b.real_name,b.ia_name,b.group_id
) cc
on aa.主站id=cc.主站id





select to_char(a.stat_date,'yyyymmdd') as 日期,
b.crm_userid as crm_id,
max(b.real_name) as 用户姓名,
max(b.ia_name) as 投顾姓名,
max(b.group_id) as 组别,
sum(nvl(a.inmoney,0)-nvl(a.outmoney,0)) as 当日净入金,
sum(a.netzcmoney) as 当日计算净资产,
sum(a.contqty) as 当日交易额,
sum(a.tmpmoney) as 当日手续费
from info_silver.rpt_crm_transfer_user_stat a
  join(select * from info_silver.rpt_crm_transfer_user_stat where stat_date= trunc(sysdate-1)) b
  on a.crm_userid = b.crm_userid
where ((a.submit_time is null ) or to_char(a.stat_date,'yyyymmdd')>=to_char(a.submit_time,'yyyymmdd'))
and a.stat_date>=trunc(sysdate-1)
  and a.group_id in (1,7,8,111,118)
group by to_char(a.stat_date,'yyyymmdd'),
b.crm_userid

