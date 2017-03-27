select a.user_id,count(distinct c.id), sum(c.worksec)             --直播间资源拨打情况
from silver_consult.tb_crm_dispatch_his@consult_std a
  join silver_consult.tb_crm_tel_record@consult_std c
  on a.user_id=c.user_id
  where c.create_time>a.create_time
and a.type=24
group by a.user_id



select * from silver_consult.tb_crm_dispatch_his@consult_std a                --个别用户检验

--join silver_consult.tb_crm_ia@consult_std b
--on a.ia_id=b.id
where a.user_id =1000513962


select id,name,group_id from silver_consult.tb_crm_ia@consult_std





select ia.group_id, count(distinct b1.user_id) from              --Q1各组新资源数量

(select a1.user_id,a1.firsttime from
(select dis.user_id,min(dis.create_time) as firsttime from silver_consult.tb_crm_dispatch_his@consult_std dis where dis.ia_id is not null group by dis.user_id ) a1
where to_char(a1.firsttime,'yyyymmdd') > 20170101) b1

join silver_consult.tb_crm_dispatch_his@consult_std dis on b1.user_id= dis.user_id and b1.firsttime=dis.create_time
join silver_consult.tb_crm_ia@consult_std ia on dis.ia_id=ia.id
  join silver_consult.v_tb_crm_user@consult_std user1 on b1.user_id=user1.id and user1.id is not null

  join tb_crm_transfer_record@consult_std d on dis.user_id=d.user_id

where ia.group_id in (2,3,4,5,6,9,10,11,12,105,112,113,106,114)
group by ia.group_id


select count(distinct a.user_id),b.group_id from tb_crm_transfer_record@consult_std a --Q1各组开单总量
  join silver_consult.tb_crm_ia@consult_std b
  on a.fia_id=b.id
where a.process in (5,6) and a.valid=1
and to_char(a.submit_time,'yyyymmdd')>=20170101
group by b.group_id



select a.user_id,sum(case when d.inorout='A' then inoutmoney end),   --直播间资源入金量
sum(case when d.inorout='B' then inoutmoney*(-1) end)
from
  silver_consult.tb_crm_dispatch_his@consult_std a
  join silver_consult.v_tb_crm_user@consult_std b
  on a.user_id = b.id
  join tb_silver_user_stat c  on b.fa_id=c.user_id
join silver_njs.history_transfer d
    on c.firm_id=d.firmid
where a.type=24
group by a.user_id

select a.user_id,sum(case when d.inorout='A' then inoutmoney end),   --直播间资源入金量
sum(case when d.inorout='B' then inoutmoney*(-1) end)
from
  silver_consult.tb_crm_dispatch_his@consult_std a
  join silver_consult.v_tb_crm_user@consult_std b
  on a.user_id = b.id
  join tb_silver_user_stat c  on b.fa_id=c.user_id
join silver_njs.history_transfer d
    on c.firm_id=d.firmid
where a.type=24
group by a.user_id


select distinct a.user_id,c.open_account_time from silver_consult.tb_crm_dispatch_his@consult_std a
  join silver_consult.v_tb_crm_user@consult_std b
  on a.user_id = b.id
  join tb_silver_user_stat c  on b.fa_id=c.user_id
where a.type=24


select * from silver_consult.tb_Crm_living_room_apply




