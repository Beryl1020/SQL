

select '新增资源数', count(distinct b1.user_id) from

(select a1.user_id,a1.firsttime from
(select dis.user_id,min(dis.create_time) as firsttime from silver_consult.tb_crm_dispatch_his@consult_std dis where dis.ia_id is not null group by dis.user_id ) a1
where to_char(a1.firsttime,'yyyymmdd') between 20170225 and 20170303) b1

join silver_consult.tb_crm_dispatch_his@consult_std dis on b1.user_id= dis.user_id and b1.firsttime=dis.create_time
join silver_consult.tb_crm_ia@consult_std ia on dis.ia_id=ia.id
  join silver_consult.v_tb_crm_user@consult_std user1 on b1.user_id=user1.id and user1.id is not null

where ia.group_id in (2,3,4,5,6,9,10,11,12,105) -- 新增资源数

union all

select '新增A/B资源数', count(distinct b1.user_id) from

(select a1.user_id,a1.firsttime from
(select dis.user_id,min(dis.create_time) as firsttime from silver_consult.tb_crm_dispatch_his@consult_std dis where dis.ia_id is not null group by dis.user_id ) a1
where to_char(a1.firsttime,'yyyymmdd') between 20170225 and 20170303) b1

join silver_consult.tb_crm_dispatch_his@consult_std dis on b1.user_id= dis.user_id and b1.firsttime=dis.create_time
join silver_consult.tb_crm_ia@consult_std ia on dis.ia_id=ia.id
  join silver_consult.v_tb_crm_user@consult_std user1 on b1.user_id=user1.id and user1.id is not null

where ia.group_id in (2,3,4,5,6,9,10,11,12,105) and user1.grade in ('A','A紧急','A暂缓','B') -- 新增A/B类资源数








select * from silver_consult.tb_crm_ia@consult_std where group_id=108
select * from silver_consult.v_tb_crm_user@consult_std where IA_ID in(895,682)







select count(distinct trans.user_id)
from tb_crm_transfer_record@consult_std trans
  left join silver_consult.tb_crm_ia@consult_std ia
  on trans.fia_id=ia.id
where trans.process in (5,6) and trans.valid=1
      and to_char(trans.submit_time,'yyyymmdd') between 20170225 and 20170303
and ia.group_id in (2,3,4,5,6,9,10,11,12,105)












select to_char(b1.firsttime,'yyyymmdd'), count(distinct b1.user_id) from

(select a1.user_id,a1.firsttime from
(select dis.user_id,min(dis.create_time) as firsttime from silver_consult.tb_crm_dispatch_his@consult_std dis where dis.ia_id is not null group by dis.user_id ) a1
where to_char(a1.firsttime,'yyyymmdd') between 20170320 and 20170320) b1

join silver_consult.tb_crm_dispatch_his@consult_std dis on b1.user_id= dis.user_id and b1.firsttime=dis.create_time
join silver_consult.tb_crm_ia@consult_std ia on dis.ia_id=ia.id
  join silver_consult.v_tb_crm_user@consult_std user1 on b1.user_id=user1.id and user1.id is not null

where ia.group_id in (2,3,4,5,6,9,10,11,12,105) -- 新增资源数
group by to_char(b1.firsttime,'yyyymmdd')



