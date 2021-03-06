select distinct
  fa_id as 主站ID,
  user_name as 用户姓名,
  fia_id as 投顾ID, name as 投顾姓名,
  (case when group_id =106 then 14 else group_id end) as 投顾组别,
  submit_time as 流转时间,
  net_assets as 激活资金
from
  (select *
   from
    (select distinct fa_id, b.user_name, a.fia_id, c.name, c.group_id, submit_time, net_assets
     from
      (select user_id, fia_id, submit_time, pmec_net_value_sub+pmec_net_in_sub as net_assets
       from silver_consult.silver_consult.tb_crm_transfer_record@consul_std
       where valid = '1'
       and to_char(submit_time, 'yyyymmdd') = to_char(sysdate,'yyyymmdd')
       order by net_assets desc) a
     left join
       (select distinct fa_id, id, user_name from silver_consult.v_tb_crm_user@consul_std) b
     on a.user_id = b.id
     left join
       (select distinct id, name, group_id from silver_consult.tb_crm_ia@consul_std) c
     on a.fia_id = c.id) t1
   left join
     (select user_id, firm_id from silver.tb_silver_user_stat@silver_std
      where partner_id = 'pmec')t2
      on t1.fa_id = t2.user_id) a
  left join
 (select distinct firmid, net_assets as assets
  from silver_njs.tb_silver_data_center@silver_std
  where partner_id = 'pmec'
  and hdate=20160909) b

  on a.firm_id = b.firmid
  where (case when group_id =106 then 14 else group_id end)=2
  order by submit_time
