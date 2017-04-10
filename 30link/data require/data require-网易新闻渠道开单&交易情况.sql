select a.firm_id as firm_id,
  b.acct_real_name as 用户姓名,
  b.user_id as user_id,
  c.pmec_net_in_sub+c.pmec_net_value_sub as 激活资金,
  e.moneyin as 总净入金,
  f.moneydeal as 开单后交易额,
  d.net_assets as 昨日净资产,
  a.open_account_time as 开户时间,
  to_char(c.submit_time,'yyyymmdd') as 流转时间,
  a.sub_refer,
  a.refer_1_type,
  a.refer_2_type

from info_silver.ods_history_user a
  join info_silver.ods_history_user b
  on a.firm_id=b.firm_id
  join info_silver.ods_crm_transfer_record c
  on a.firm_id=c.firm_id
  left join silver_njs.tb_silver_data_center@silver_std d
  on a.firm_id=d.firmid
  and d.hdate=to_char(sysdate-1,'yyyymmdd')
  LEFT join
  (select io.firmid as firmid,sum(case when io.inorout='A' then io.inoutmoney when io.inorout='B' then io.inoutmoney*(-1) END ) as moneyin
  from silver_njs.history_transfer@silver_std io
    join info_silver.ods_crm_transfer_record trans
    on io.firmid=trans.firm_id and trans.submit_time<io.realdate
  group by io.firmid) e
  on a.firm_id=e.firmid
  left join
  (select deal.firmid as firmid,sum(deal.contqty) as moneydeal
    from info_silver.ods_history_deal deal
    join info_silver.ods_crm_transfer_record trans
    on deal.firmid=trans.firm_id and trans.submit_time<deal.trade_time
    group by deal.firmid) f
  on a.firm_id=f.firmid
where a.sub_refer like '%pzlpwwyxw%'

















select * from info_silver.ods_history_user
select * from  info_silver.ods_crm_transfer_record
select * from silver_njs.history_transfer@silver_std
select * from silver_njs.tb_silver_data_center@silver_std
select * from info_silver.ods_history_deal