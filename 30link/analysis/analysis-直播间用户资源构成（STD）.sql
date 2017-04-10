select a.user_id,a.room_name,a.create_time
from silver_consult.tb_crm_living_room_apply@consul_std a
where a.apply_type=1
and to_char(a.create_time,'yyyymmdd')>=20170101                              ----所有Q1申请过直播间的资源(约8000个）




select distinct a.user_id,a.room_name,a.create_time,b.fia_id,                ----所有有过申请记录且流转的资源
  b.submit_time,b.pmec_net_value_sub+b.pmec_net_in_sub
from silver_consult.tb_crm_living_room_apply@consult_std a
join silver_consult.tb_crm_transfer_record@consul_std b
  on a.user_id=b.user_id
where a.apply_type=1
and to_char(a.create_time,'yyyymmdd')>=20170101
AND B.PROCESS IN(5,6) and b.valid=1



select a.user_id,sum(b.worksec),count(b.id)
from silver_consult.tb_crm_living_room_apply@consult_std a
  join silver_consult.tb_crm_tel_record@consul_std b
  on a.user_id=b.user_id
  join silver_consult.tb_crm_ia@consul_std c
  on c.id=b.ia_id
where a.apply_type=1
and to_char(a.create_time,'yyyymmdd')>=20170101
and c.group_id in(2,3,4,5,6,9,10,11,12,105,106,112,113,114)
group by a.user_id

select * from silver_consult.tb_crm_tel_record@consul_std
where user_id=76301923




select a.user_id,a.room_name,a.create_time,b.ia_id,c.name                                        ----所有申请过直播间且在投顾名下的资源数统计
from silver_consult.tb_crm_living_room_apply@consul_std a
  join info_silver.tb_crm_user b
  on a.user_id=b.id
  join info_silver.tb_crm_ia c
  on b.ia_id=c.id and c.status=1 and c.group_id not in (1,7,8,111)
where a.apply_type=1
and to_char(a.create_time,'yyyymmdd')>=20170101


select aa.user_id,aa.group_id,sum(bb.worksec),count(bb.id)
  from
(select distinct a.user_id,c.name,c.group_id FROM
  info_silver.tb_crm_tag_user_rel a                                                              ----所有Q1分配、在前端且申请过直播间的资源数统计
join info_silver.tb_crm_user b
on a.user_id=b.id
  join info_silver.tb_crm_ia c
  on b.ia_id=c.id and c.status=1 and c.group_id in (2,3,4,5,6,9,10,11,12,105,106)
  join silver_consult.tb_crm_dispatch_his@consul_std d
    on a.user_id=d.user_id
  and to_char(d.create_time,'yyyymmdd')>=20170105
where a.tag_id=41 and c.name not in ('程序') and c.name not like '%资源%') aa
  left join info_silver.tb_crm_tel_record bb
    on aa.user_id=bb.user_id
group by aa.user_id,aa.group_id

select a.user_id,a.mintime,b.create_time from                                                          --申请直播间-分配时间>90天的人数
  (select user_id,min(create_time) as mintime
   from silver_consult.tb_crm_dispatch_his@consul_std
  where ia_id is not NULL
  group by user_id) a
join silver_consult.tb_crm_living_room_apply@consul_std b
  on a.user_id=b.user_id
where b.create_time>a.mintime+90






select distinct user_id from silver_consult.tb_crm_living_room_apply@consul_std where user_id=1000330952

select * from info_silver.tb_crm_tag_user_rel
where user_id not in
      (select user_id from silver_consult.tb_crm_living_room_apply@consul_std)
and tag_id=41

select * from info_silver.tb_crm_tag_user_rel where user_id=1000330952 and tag_id=41

select distinct user_id from info_silver.tb_crm_tag_user_rel where tag_id=41



select * from silver_consult.tb_crm_dispatch_his@consul_std where user_id=76301923

select * from info_silver.tb_crm_tel_record


select * from silver_consult.tb_crm_transfer_record@consul_std

select * from silver_consult.tb_crm_living_room_apply@consul_std where to_char(create_time,'yyyymmdd')=20170329