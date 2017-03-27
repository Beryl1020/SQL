select a.user_id,a.room_name,a.create_time
from silver_consult.tb_crm_living_room_apply@consult_std a
where a.apply_type=1
and to_char(a.create_time,'yyyymmdd')>=20170101                              ----所有Q1申请过直播间的资源(约8000个）




select distinct a.user_id,a.room_name,a.create_time,b.fia_id,                ----所有有过申请记录且流转的资源
  b.submit_time,b.pmec_net_value_sub+b.pmec_net_in_sub
from silver_consult.tb_crm_living_room_apply@consult_std a
join tb_crm_transfer_record@consult_std b
  on a.user_id=b.user_id
where a.apply_type=1
and to_char(a.create_time,'yyyymmdd')>=20170101
AND B.PROCESS IN(5,6) and b.valid=1



select a.user_id,sum(b.worksec),count(b.id)
from silver_consult.tb_crm_living_room_apply@consult_std a
  join silver_consult.tb_crm_tel_record@consult_std b
  on a.user_id=b.user_id
  join silver_consult.tb_crm_ia@consult_std c
  on c.id=b.ia_id
where a.apply_type=1
and to_char(a.create_time,'yyyymmdd')>=20170101
and c.group_id in(2,3,4,5,6,9,10,11,12,105,106,112,113,114)
group by a.user_id

select * from silver_consult.tb_crm_tel_record@consult_std
where user_id=76301923









select * from silver_consult.tb_crm_dispatch_his@consult_std where user_id=76301923




select * from tb_crm_transfer_record@consult_std