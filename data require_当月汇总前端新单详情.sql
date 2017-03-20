select
  distinct fa_id as 主站ID,
  user_name as 用户姓名,
  fia_id as 投顾ID,
  name as 投顾姓名,
  group_id as 投顾组别,
  submit_time as 流转时间,
  net_assets as 激活资金
from
  (select * from
    (select distinct fa_id, b.user_name, a.fia_id, c.name, c.group_id, submit_time, net_assets
     from
      (select user_id, fia_id, submit_time, pmec_net_value_sub+pmec_net_in_sub as net_assets
       from silver_consult.TB_CRM_TRANSFER_RECORD@consult_std
       where valid = '1'
       and submit_time > trunc (sysdate-1,'mm')
       order by net_assets desc) a
     left join
      (select distinct fa_id, id, user_name
       from silver_consult.v_tb_crm_user@consult_std) b
     on a.user_id = b.id
     left join
      (select distinct id, name, group_id
       from silver_consult.tb_crm_ia@consult_std) c
     on a.fia_id = c.id)t1

    left join

    (select user_id, firm_id
     from silver.tb_silver_user_stat
     where partner_id = 'pmec')t2

    on t1.fa_id = t2.user_id) a

    left join

    (select distinct firmid, net_assets as assets
     from silver_njs.TB_SILVER_DATA_CENTER
     where partner_id = 'pmec'
     and hdate=20160909)b

    on a.firm_id = b.firmid

where group_id = 2
order by submit_time