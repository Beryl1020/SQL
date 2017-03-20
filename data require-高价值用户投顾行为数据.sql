
select tel.user_id,
  sum(case when tel.create_time<=tr.submit_time then tel.worksec end) as worksec,
      sum(case when tel.create_time<=tr.submit_time then tel.billsec end) as billsec,
          avg(tr.pmec_net_value_sub+tr.pmec_net_in_sub)
FROM tb_crm_transfer_record@consult_std tr
  JOIN
  silver_consult.tb_crm_tag_user_rel@consult_std tag
  on tr.user_id=tag.user_id
  left join silver_consult.tb_crm_tel_record@consult_std tel
  on tr.user_id=tel.user_id
where tag.tag_id=39
and tr.process in (5,6)
and tr.valid=1
  and tel.user_id is not null
group by tel.user_id  --高价值&流转客户拨打数据



select tel.user_id,
  count(distinct case
       when trunc(tel.create_time) between trunc(dis.create_time) and trunc(dis.create_time+3) then tel.user_id end)
from
   silver_consult.tb_crm_tel_record@consult_std tel
join silver_consult.tb_crm_tag_user_rel@consult_std tag
  on tel.user_id=tag.user_id
  join
     (select min(create_time) as create_time,user_id from silver_consult.tb_crm_dispatch_his@consult_std dis
     where ia_id is not null group by user_id ) dis
  on tel.user_id=dis.user_id
where tag.tag_id=39
group by tel.user_id



select * from silver_consult.tb_crm_dispatch_his@consult_std -- 分配记录表
select * from silver_consult.tb_crm_tel_record@consult_std --电话表
select * from silver_consult.tb_crm_tag_user_rel@consult_std -- 高价值用户标签表
select * from tb_crm_transfer_record@consult_std -- 流转单表