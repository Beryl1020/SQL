

select '新增资源数', count(distinct b1.user_id) from

(select a1.user_id,a1.firsttime from
(select dis.user_id,min(dis.create_time) as firsttime from silver_consult.tb_crm_dispatch_his@consult_std dis where dis.ia_id is not null group by dis.user_id ) a1
where to_char(a1.firsttime,'yyyymmdd') between 20170318 and 20170324) b1

join silver_consult.tb_crm_dispatch_his@consult_std dis on b1.user_id= dis.user_id and b1.firsttime=dis.create_time
join silver_consult.tb_crm_ia@consult_std ia on dis.ia_id=ia.id
  join silver_consult.v_tb_crm_user@consult_std user1 on b1.user_id=user1.id and user1.id is not null

where ia.group_id in (2,3,4,5,6,9,10,11,12,105) -- 新增资源数

union all

select '新增A/B资源数', count(distinct b1.user_id) from

(select a1.user_id,a1.firsttime from
(select dis.user_id,min(dis.create_time) as firsttime from silver_consult.tb_crm_dispatch_his@consult_std dis where dis.ia_id is not null group by dis.user_id ) a1
where to_char(a1.firsttime,'yyyymmdd') between 20170318 and 20170324) b1

join silver_consult.tb_crm_dispatch_his@consult_std dis on b1.user_id= dis.user_id and b1.firsttime=dis.create_time
join silver_consult.tb_crm_ia@consult_std ia on dis.ia_id=ia.id
  join silver_consult.v_tb_crm_user@consult_std user1 on b1.user_id=user1.id and user1.id is not null

where ia.group_id in (2,3,4,5,6,9,10,11,12,105) and user1.grade in ('A','A紧急','A暂缓','B') -- 新增A/B类资源数






