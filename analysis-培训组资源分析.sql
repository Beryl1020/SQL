select distinct a.ia_id,b.group_id,a.id,c.firm_id,c.partner_id,d.net_assets,
sum(case when e.inorout='A' and e.fdate>=20170301 then e.inoutmoney end),
max(f.create_time)
from silver_consult.v_tb_crm_user@consult_std a
  left join
  silver_consult.tb_crm_ia@consult_std b
  on a.ia_id=b.id
  left join tb_silver_user_stat c
  on a.fa_id=c.user_id
  left join silver_njs.tb_silver_data_center d
  on c.firm_id=d.firmid
  left join silver_njs.history_transfer e
  on c.firm_id=e.firmid
  left join silver_consult.tb_crm_tel_record@consult_std f
  on a.ia_id=f.ia_id and c.user_id=f.user_id
where b.group_id is not null
and b.group_id=103
and d.hdate ='20170320'

group by a.ia_id,b.group_id,a.id,c.firm_id,c.partner_id,d.net_assets --现测试组资源情况


select a.user_id,d.user_id,
sum(case when e.realdate between a.connect_time1 and a.connect_time1+7 and e.inorout='A' then e.inoutmoney end),
  sum(case when e.realdate between a.connect_time2 and a.connect_time2+7 and e.inorout='A' then e.inoutmoney end),
  sum(case when e.realdate between a.connect_time1 and a.connect_time1+15 and e.inorout='A' then e.inoutmoney end),
  sum(case when e.realdate between a.connect_time2 and a.connect_time2+15 and e.inorout='A' then e.inoutmoney end)
from (select ia_id as ia_id, user_id as user_id, max(connect_time) as connect_time1, min(connect_time) as connect_time2
     from silver_consult.tb_crm_tel_record@consult_std
     group by ia_id,user_id) a
left join silver_consult.tb_crm_ia@consult_std b
  on a.ia_id=b.id
  left join silver_consult.v_tb_crm_user@consult_std c
  on a.user_id=c.id
  left join tb_silver_user_stat d
  on c.fa_id=d.user_id
  left join silver_njs.history_transfer e
  on d.firm_id=e.firmid    
where b.group_id=103 and a.user_id<>-1
group by a.user_id,d.user_id  -- 测试组拨打过的用户入金情况












select * from silver_consult.tb_crm_tel_record@consult_std
select * from silver_consult.tb_crm_ia@consult_std where name='董学雷'
select * from silver_consult.v_tb_crm_user@consult_std
select * from silver_njs.history_transfer
